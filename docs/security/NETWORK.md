# Reference network architecture

For the network and platform administrators who place a pipeline in the customer's network, and the security
official who reviews it. It covers where the pipeline sits, what listens, what each stage reaches and over what, the
network policies that hold it to that, private endpoints and DNS, the operator's path in, and the network map to
keep. It is HIPAA.md §11.2's document D1. What each connection carries is [DATA-FLOWS.md](DATA-FLOWS.md); how the
control API is exposed is [CONTROL-API-DEPLOYMENT.md](CONTROL-API-DEPLOYMENT.md) §2; who may reach the cluster
is [IDENTITY.md](IDENTITY.md).

Where this falls in setting up an environment, with the guards around it (egress firewall, WAF, flow logs, threat
detection), is [ENVIRONMENT-SETUP.md](ENVIRONMENT-SETUP.md) §1.

It describes a pipeline deployed from `deploy/reference/` and the kits' READMEs, with the `hipaa` compliance profile
on, one pipeline to a namespace (HIPAA-OPERATIONS.md §2.8). The network policies of §4 are a starting point written
from the ones the AWS PoC and the sandbox run (`deploy/aws/k8s/`, `deploy/sandbox/`); the sandbox's harness pipeline
runs under them, but no cloud deployment has yet (§9).

## 1. The zones

```
 producers' networks ─────────── TLS or mTLS ───────────┐ (HTTP push only)
 producers' sources (SFTP, SMB, buckets, queues, tables)│
        ▲ the landing node polls them, over TLS / SSH    │
        │                                                ▼
 ┌──── the customer's network: private subnets, no public addresses ─────────────────────────────────┐
 │                                                                                                   │
 │  push load balancer ──────────────► namespace sdh-<pipeline>                                       │
 │  (private, or the one public                 landing workers (push port)                           │
 │   entry, TLS passthrough for mTLS)           split, convert, transform workers (no listener but    │
 │                                                 the kubelet's probe port)                         │
 │  control ingress ─────────────────►          sdh-control-<stage> pods (8443)                       │
 │  (inside the network only)                       │                                                 │
 │        ▲                                         │ egress, by policy, to:                          │
 │        │                                         ▼                                                 │
 │  operator's path in                 platform services: Postgres or PgBouncer, the broker,          │
 │  (VPN, bastion or                     the collector, DNS                                          │
 │   zero-trust access)                cloud services over private endpoints: the KMS, the object     │
 │        │                              store, the workload identity service, the identity provider │
 │        └──► the Kubernetes API (private endpoint)                                                 │
 │                                                                                                   │
 │  collector ──► the audit store (over its private endpoint)                                        │
 └───────────────────────────────────────────────────────────────────────────────────────────────────┘
```

- **Private subnets, no public endpoints.** The nodes, the database, the broker and the collector have no public
  address. The Kubernetes API endpoint is private, or open only to the operator's path in (§6).
- **One way in for data,** and only where an intake is HTTP push: the landing node's push port, behind a load
  balancer. Every other intake is polled by the landing node, so nothing reaches the pipeline from outside for it.
  A push intake for producers outside the customer's network is the only public entry the product needs; put it
  behind the customer's edge (WAF, rate limits, request size), with TLS passing through to the pod for mTLS
  (DESIGN.md §5.3.1).
- **The control API inside the network only,** reached from the operator's path in, never the internet
  (CONTROL-API-DEPLOYMENT.md §2).
- **Egress only to what each stage needs** (§3), over private endpoints where the service is the cloud's. No route to
  the internet from the pipeline's subnets: no NAT gateway, or one whose egress firewall allows only the producers'
  sources the landing node polls.

## 2. What listens

A worker opens no port but the ones below. The control API is a Deployment of its own, so the workers gain none for
it (CONTROL-API-DEPLOYMENT.md §1). No stage serves metrics to be scraped: telemetry is pushed to the collector.

| Setting | Default | On | Reached by |
|---|---|---|---|
| `sdh.landing.http.port` | 8443 | the landing node, HTTP push intakes only; open only on the node holding the lease | producers, through the push load balancer. TLS always; OAuth2 or mTLS on every request (DESIGN.md §5.3.15) |
| `sdh.stage.ready-port` | 8081 | split, convert and transform workers | the kubelet only: a plain TCP port, opened once the pod's release is let in, that carries no data (STAGES.md §14.4a) |
| `sdh.control.port` | 8443 | the `sdh-control-<stage>` pods | the control ingress's namespace only (each kit's NetworkPolicy). TLS always |
| `sdh.control.probe-port` | 8081 | the `sdh-control-<stage>` pods | the kubelet only: plain HTTP, `/livez` and `/readyz`, no data |

The landing node of a polled intake has no listener at all: its health is the process (its kit's README).

**Each listens on every interface** of its pod or host: there is no setting to bind one address. In a pod that is
the pod's own address, and the policies of §4 decide who reaches it. **On a host** (a kit's `bin/`), nothing does
that for you: a host firewall must (each kit's README, "Securing a host"). The probe ports are plain TCP or HTTP,
for the host's own health checks only; the push and control ports for their load balancer or ingress only.

### 2.1 The platform services' listeners

The services the pipeline uses are the customer's, and so are their ports. These are the usual defaults, for the
firewall and the policies: let in only what §3 says reaches each, and close every other listener to the network.

| Service | Port (default) | Who needs it | Close or restrict |
|---|---|---|---|
| Postgres | 5432 | the stages and their control pods, or PgBouncer only | `hostssl` lines only (HARDENING.md §4.1); `listen_addresses` the pipeline network's, never `*` on a public interface |
| PgBouncer | 6432 (5432 on the PoC) | the stages and their control pods | its admin console (`admin_users`) to named staff only |
| Kafka, clients | the TLS listener (9093 or 9094 commonly) | the stages and their control pods | no `PLAINTEXT` listener on the network (HIPAA-OPERATIONS.md §2.6) |
| Kafka, between brokers and the controllers (KRaft) | the inter-broker and controller listeners (9093, 9091 and others, as configured) | the brokers and controllers only | TLS and principals of their own; never reachable from the pipeline's namespaces |
| Vault | 8200 (API), 8201 (cluster) | the stages, for Transit; 8201 Vault's own nodes only | TLS on 8200 (`tls_disable` never true in production); the UI off or to named staff |
| SeaweedFS | S3 gateway 8333; master 9333, volume 8080, filer 8888, and each one's gRPC port (the HTTP port plus 10000) | the S3 gateway: the stages; the rest: SeaweedFS's own servers only | only the S3 gateway, with TLS, reachable from the stages; the master, volume and filer ports (which serve objects without the S3 gateway's identities) never from outside the store's own servers |
| The OpenTelemetry collector | 4318 (OTLP/HTTP), 4317 (OTLP/gRPC) | the stages and their control pods | the pipeline's namespaces only; 4317 closed if unused |
| Kubernetes | the API server (443 or 6443), the kubelet (10250) | the platform's administrators; the API server to the kubelet | never public; `exec`, `port-forward` and `debug` by named staff only (HIPAA-OPERATIONS.md §2) |

Check against the service's own documentation for its version: defaults change, and a chart or operator may set
others. The sandbox's Vault (`deploy/sandbox/infra/vault.yaml`) listens on plain HTTP inside the sandbox; it is not
an example for production.

The kits' policies don't admit the kubelet's probes: on EKS's VPC CNI and on Calico (the sandbox) traffic from a
pod's own node is let through regardless. On a CNI that applies policies to it, let the nodes' addresses in on 8081.

## 3. What each stage reaches

Every connection that carries PHI, or keys to it, is TLS, and the profile refuses a stage that would make one
without it (DATA-FLOWS.md §1, CONFIGURATION.md). The ports are those of the reference settings and the PoC; use
the customer's own.

| To | From | Port | Protocol | Settings |
|---|---|---|---|---|
| DNS | every pod | 53, UDP and TCP | DNS | the cluster's |
| Postgres, or PgBouncer in front of it | every stage and its control pods | 5432 | TLS, `sslmode=verify-full` | `sdh.storage.catalog.uri`, `sdh.lease.jdbc.url`, and the source and sink catalogs |
| The broker | every stage and its control pods | the broker's TLS listener (9094 on the PoC) | Kafka `SASL_SSL` (or `SSL` with client certificates) | the stage's queue URIs (`deploy/reference/`) |
| The collector | every stage and its control pods | 4318 | OTLP/HTTP; plain inside the cluster (no PHI), or `https://` with a token outside it (HIPAA-OPERATIONS.md §2.7) | `sdh.telemetry.otlp.endpoint` |
| The KMS | every stage and its control pods | 443 | HTTPS | `sdh.crypto.*` (`wrap-provider`, `sign-provider`) |
| The object store | every stage and its control pods | 443 | HTTPS; a bucket policy refusing plaintext | the catalogs' `warehouse` and client settings |
| The workload identity service | every pod with a cloud identity | the provider's (EKS Pod Identity: its agent on the node, `169.254.170.23` port 80; STS on 443 for IRSA) | the provider's | the service account (IDENTITY.md §2) |
| The identity provider | the control pods in `jwt` mode; the landing node of an OAuth2 push intake | 443 | HTTPS: the issuer's discovery document and key set | `sdh.control.auth.jwt.issuer-uri` or `jwk-set-uri`; `sdh.landing.http.oauth2.issuer` or `jwk-set-uri` |
| The audit store | the collector only | 443 | HTTPS, through the store's private endpoint | the collector's exporter (HIPAA-OPERATIONS.md §2.7) |

The landing node also reaches its producer's source, by its intake (DESIGN.md §5.3.1):

| Setting | Default | Intake | Protocol |
|---|---|---|---|
| `sdh.landing.source.sftp.port` | 22 | SFTP | SSH, the host key checked against `known-hosts` |
| `sdh.landing.source.smb.port` | 445 | SMB share | SMB 3 with encryption required; a share that doesn't encrypt is refused |
| `sdh.landing.source.bucket.*` | 443 | S3, Azure Blob, GCS | HTTPS |
| `sdh.landing.broker.uri` | the broker's | Kafka, SQS, RabbitMQ, Artemis, Pub/Sub, Service Bus, Event Hubs | the broker's TLS |
| `sdh.landing.source.table.*` | the catalog's and store's | Iceberg table | as its catalog and store |

A mounted directory intake reaches its share through the node's volume mount, not the pod's network: secure that
share as the storage it is (NFS with Kerberos, or the CSI driver's encryption in transit).

Results to producers, where on, go out on the broker like every other message (DESIGN.md §5.3.8). Gold readers
(`connector-queue-reader`) reach the broker, the database, the KMS and the gold bucket as the stages do: give each
reader's namespace the same egress and nothing more.

## 4. Network policies

`deploy/reference/network-policies.yaml` holds them, for each pipeline's namespace, `sdh-<pipeline>`. Label that
namespace `sdh.smiledigitalhealth.com/infra-client: "true"`, which the platform services' own policies let in
(below). The policies default to nothing, then let in and out only what §2 and §3 name. Fill in its `${...}` values, or apply it through `envsubst` as the other reference files are.

| Policy | Pods | Lets |
|---|---|---|
| `default-deny` | all | nothing in or out, unless another policy allows it |
| `to-dns` | all | out to the cluster's DNS, 53 UDP and TCP |
| `to-platform` | all | out to the platform services' namespace: Postgres or PgBouncer (5432), the broker's TLS listener (9094 here), the collector (4318) |
| `to-private-endpoints` | all | out to the private endpoints' subnets on 443: the KMS, STS, the object store, the identity provider |
| `to-pod-identity` | all | out to EKS Pod Identity's agent on the node, `169.254.170.23` port 80; left out elsewhere |
| `landing-to-source` | the landing node | out to a polled intake's source (22 for SFTP here); one rule per source |
| `push-to-landing` | the landing node | in from the push load balancer's namespace on 8443, for an HTTP push intake only |
| `control-from-ingress` | the control pods | in from the control ingress's namespace on 8443, as each kit's own policy (CONTROL-API-DEPLOYMENT.md §2.4) |

The sandbox's harness pipeline runs under this file (`scripts/sandbox.sh harness-up`), with the sandbox's own
services in place of the cloud's: SeaweedFS (8333) and Vault (8200) in its platform namespace, the broker on 9092,
no private endpoints and no Pod Identity, its landing node `app: landing`, and pushes from `sdh-producers`. Checked
there on 2026-11-25, from a throwaway pod in each namespace: from the pipeline's namespace Postgres, the broker,
SeaweedFS, Vault and the collector were open, and the broker's other listener (9093), Grafana (3000), the internet
(1.1.1.1:443) and the namespace's own landing and control ports closed; from `sdh-producers` the landing node's 8443
was open and the control API's closed; from another pipeline's namespace both were closed. Under them, ten of the
control harness's eleven scenarios (`scripts/harness.sh sandbox`) passed, the kubelet's probes and the kits' control
policies included; `erasure` failed once on a race in `audit-verify`, not the network. With that fixed (DESIGN.md
revision (fm)), all eleven passed.

- **With an S3 gateway endpoint** instead of an interface one, S3's addresses are public ones, which a
  NetworkPolicy can't name by prefix list. The PoC allows 443 to every address outside the private ranges
  (`deploy/aws/k8s/pipeline/namespace.yaml`); where you do the same, it is the subnets' route tables, with no NAT,
  that keep that traffic to the endpoint, and the bucket policy's `aws:SourceVpce` condition that refuses it from
  anywhere else. Prefer interface endpoints, so the policy names them.
- **Another pipeline's namespace is not in any rule.** Stages of different pipelines can't reach each other, and the
  platform services tell them apart by their credentials, not their addresses (HIPAA-OPERATIONS.md §2.8).
- **The platform services' own policies** let in only the pipelines' namespaces, and KEDA's operator for the broker
  where the stages autoscale, as `deploy/aws/k8s/infra/namespace.yaml` does. The collector's egress goes only to the
  audit store and DNS (HIPAA-OPERATIONS.md §2.7).
- **A policy is no wall against the cluster's own administrators.** `kubectl port-forward`, `exec` and `debug` reach a
  pod through the API server and the kubelet, which no NetworkPolicy applies to. Grant them per namespace to named
  staff only, audited (HIPAA-OPERATIONS.md §2.4, §2.9).

## 5. Private endpoints and DNS

- **Private endpoints for each cloud service a stage reaches:** on AWS, interface endpoints for KMS, STS and S3 (or
  S3's gateway endpoint), and for CloudWatch Logs where that is the audit store; on GCP, Private Service Connect or
  Private Google Access; on Azure, Private Link for Key Vault and Storage. The identity provider through its own
  private endpoint, or a key set the pods can reach (`jwk-set-uri`, CONTROL-API-DEPLOYMENT.md §2.4).
- **Key and bucket policies that accept the stages' identities only from the pipeline's network** (on AWS, the
  `aws:SourceVpce` or `aws:SourceVpc` condition), so a credential copied out of a pod can't be used from elsewhere
  (HIPAA-OPERATIONS.md §2.9).
- **DNS:** the endpoints' private DNS names on, so the stages' default service names (`kms.<region>.amazonaws.com`,
  the bucket's) resolve to them, with no endpoint URL in the settings. Log the resolver's queries to the SIEM where
  the provider can (Route 53 Resolver query logging), and refuse resolution of names the pipeline has no reason to
  look up where it offers a DNS firewall.
- **Certificates are checked by name:** Postgres with `verify-full`, the broker and the cloud services by their
  hostnames, so a resolver answering wrongly sends a stage to a host it refuses, not one it trusts.

## 6. The operator's path in

- **One path in:** a VPN, a bastion, or zero-trust access (an identity-aware proxy), with SSO and MFA
  (IDENTITY.md §3). Nothing else reaches the control ingress or the Kubernetes API.
- **The control API** through its ingress, inside the network only (CONTROL-API-DEPLOYMENT.md §2).
- **The Kubernetes API** on a private endpoint, or a public one open only to the path's addresses. `kubectl exec`
  reaches the stages' CLI (HIPAA-OPERATIONS.md §2.4).
- **No interactive access to the nodes:** no SSH, the provider's session manager only for named staff, audited
  (HIPAA-OPERATIONS.md §2.9).
- **Producers** are not on this path: they reach the push load balancer, or are polled.

## 7. The network map

Keep a current map of each pipeline's network, for the risk analysis and the penetration test
(HIPAA-OPERATIONS.md §11). Review it at least once a year, and with every change to a zone, an endpoint, an intake
or a producer. It records:
- the subnets and their route tables, and that none routes to the internet but through the egress firewall;
- each listener of §2 and what may reach it, and each connection of §3 with its endpoint, port and protocol;
- the load balancers and ingresses, their certificates and when they expire;
- the private endpoints, and the key and bucket policy conditions that name them;
- each producer: its network, its intake, its credentials and who issues them;
- the operator's path in, and who holds access to it;
- the network policies applied to each namespace (`kubectl get networkpolicy -A -o yaml`).

## 8. Checking a deployment

- [ ] No node, database, broker or collector has a public address; the subnets have no route to the internet but
      through the egress firewall.
- [ ] Every pipeline namespace has `default-deny`, and the other policies of §4 with its own values filled in.
- [ ] From a pod in the namespace, a connection to an address outside §3 times out (`kubectl debug` with the
      restricted profile, or a test pod with the namespace's labels).
- [ ] From another pipeline's namespace, the stages' ports can't be reached.
- [ ] The KMS, STS and the object store resolve to private endpoint addresses from the pods.
- [ ] The key and bucket policies refuse the stages' identities from outside the pipeline's network.
- [ ] The control ingress answers from the operator's path only; the push load balancer only on the push port.
- [ ] The broker has no plaintext listener on the network (HIPAA-OPERATIONS.md §2.6, "Securing the broker").
- [ ] Of the platform services (§2.1), only the listeners in §3 are reachable from the pipeline; SeaweedFS's
      master, volume and filer ports, Kafka's controller listeners and Vault's cluster port aren't.
- [ ] On a host, the firewall lets in only the push or control port, from its load balancer, and the probe port
      from the host itself.
- [ ] The network map is current, and its review dated.

## 9. Findings

- **§4's policies have run only on the sandbox,** on its harness pipeline (§4), not on a cloud deployment: the
  private endpoints, Pod Identity and the broker's TLS listener rules are untried, and the sandbox's other pipelines
  still run under their own policies (`deploy/sandbox/*/namespace.yaml`).
- **The AWS PoC is not this architecture:** it runs in the default VPC's public subnets, reaches the KMS by its
  public endpoint, and lets the stages reach 443 on every public address (AWS-POC.md; HIPAA.md §8.5). It holds no
  PHI and needs none of it until it does.
- **The kubelet's probes rely on the CNI** letting a pod's own node through (§2): true of the two CNIs tried, EKS's VPC
  CNI and Calico, not checked on others.
- **The listeners bind every interface** (§2), with no setting to choose one: a gap on a host with more than one
  network, where only the firewall keeps them in (KNOWN-GAPS.md §5).
- **No egress proxy has been tried.** The stages' clients (the AWS SDK, the JDBC and Kafka clients, the token key set
  fetch) were never run through one. Where egress must go through a proxy, test it first, with synthetic data.
- **The push intake has no rate limit or connection limit of its own** beyond `max-item-bytes` and `max-wait`
  (DESIGN.md §5.3.15): the load balancer in front of it must give them (HIPAA-OPERATIONS.md §11).

## Sources

| Statement | Source |
|---|---|
| The listeners, their settings and defaults | `landing-connector` `config/LandingProperties` (`Http`, `Sftp`, `Smb`); `connector-stage` `StageProperties` (`readyPort`), `control/ControlSettings` |
| The push port opens only on the lease holder | DESIGN.md §5.3.15; `landing-connector` `web/PushWebConfiguration` |
| No listener on a worker, the control API a Deployment of its own, telemetry pushed | each kit's README, "Deploy on Kubernetes" and "Control API"; CONTROL-API-DESIGN.md §1; `connector-runtime` `telemetry/TelemetryConfiguration` |
| The connections and their TLS | `deploy/reference/` and its README; DEPLOYMENT.md, "Transports: what has been tested"; DATA-FLOWS.md §1 |
| The intakes and their protocols | DESIGN.md §5.3.1, §5.3.14–§5.3.18 |
| The policies that have run | `deploy/reference/network-policies.yaml` (on the sandbox's harness pipeline: `scripts/sandbox.sh`, `reference_policies`); `deploy/aws/k8s/pipeline/namespace.yaml`, `deploy/aws/k8s/infra/namespace.yaml`, `deploy/sandbox/*/namespace.yaml`; the kits' control policies |
| The CNIs | `deploy/aws/terraform/compute/main.tf` (`enableNetworkPolicy`); `scripts/sandbox.sh` (`--cni=calico`) |
| Port-forwards past the policies | `scripts/sandbox.sh` (the harness's port-forwards); CONTROL-API-DESIGN.md §15.11, §15.12 |
| The PoC's network | AWS-POC.md (networking, §8.2) |
