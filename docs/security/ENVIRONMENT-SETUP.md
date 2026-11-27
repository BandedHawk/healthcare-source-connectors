# Setting up the environment: from an empty account to the first PHI

For the customer's platform administrator, who does the work; the security official, who signs each gate; and the
project lead, who keeps the record. It puts the setting up of a pipeline's environment in order: nine phases, each
ending in a **gate**, a check with a named approver and a dated result, kept with the risk analysis. Each step points
to the reference document that has the detail, so this document is short on how and long on what, in which order,
and what must be true before going on.

It is HIPAA.md §11.2's document D17. It assumes a pipeline deployed from `deploy/reference/` and the kits' READMEs,
on Kubernetes in a public cloud, with the `hipaa` compliance profile on. On hosts (the distribution kit), the phases
are the same, and the cluster steps become the host's own (each kit's README, "Securing a host").

**What has been tried.** No customer environment has been set up this way yet. Its pieces have run on our sandbox
and the AWS PoC, which are not this architecture (NETWORK.md §9, AWS-POC.md). Each phase's "Not tried" line says
what hasn't run anywhere.

## 0. Before you start

Decide, and write down in the project's change record:

- **Who approves each gate.** The security official for gates 1 to 8, with the privacy official for gate 9.
  Nobody approves a gate for work they did themselves.
- **The environments.** At least a **test** environment and **production**, each in its own cloud account,
  subscription or project, with its own keys, identity provider groups and network. Test holds **synthetic data
  only**; nothing is ever copied from production into it, not even "de-identified" extracts, unless the privacy
  official has approved the method in writing (§ 164.514). Build test first, all the way to gate 7, then
  production.
- **The pipelines:** how many, which intakes and formats each takes, and what each feeds (HIPAA-OPERATIONS.md §2.8:
  one pipeline to a namespace and a database).
- **The cloud provider's BAA** is signed and covers every service the pipeline uses (the KMS, the object store, the
  managed database and broker, the logging services). Check the provider's list of covered services: not every
  service is.
- **Sizes and targets:** expected volumes, the initial load, the RPO and RTO (HIPAA-OPERATIONS.md §7, "Choosing
  the RPO and RTO"), and the latency the feeds need.

## 1. Accounts, network and guards

**Steps:**
1. **The account baseline:** the cloud's audit trail on for every region, sent to a log archive in another account
   that the pipeline's staff can't change; the root or global administrator locked away with MFA; an organisation
   policy (SCP or equivalent) denying the regions and services the pipeline doesn't use, and denying anyone the
   right to turn the audit trail off.
2. **Quotas:** request the node, IP address, KMS request, private endpoint and load balancer quotas the sizing of
   phase 0 needs, before they block a deploy (DEPLOYMENT.md, "Sizing Postgres and Kafka for an initial load").
3. **The network,** as NETWORK.md (D1) §1 to §6: private subnets only; no public address on any node, database,
   broker or collector; private endpoints for the KMS, STS, the object store and the identity provider where it
   offers one; internal DNS; the operator's path in (VPN, bastion or zero-trust access).
4. **Egress through one guard:** the subnets' only route out is an egress firewall (or proxy), allowing by name
   only what NETWORK.md §3 lists that isn't on a private endpoint: typically the identity provider's issuer and the
   container registry. Log every denied connection. A proxy hasn't been tried with the stages' clients (D1 §9):
   test it with synthetic data first.
5. **The push ingress,** if a pipeline takes pushes: an internal or partner-facing load balancer on the push port
   only, with TLS or mTLS to the landing node, **per-client rate and connection limits**, and a request size limit
   at `sdh.landing.push.max-item-bytes` or below. The landing node has no rate limit of its own (D1 §9). Where
   partners reach it over the internet, put a WAF and the provider's DDoS protection in front, with its rule set
   tuned to allow only the push API's paths and methods.
6. **Network logs:** flow logs on every subnet in the pipeline's network, DNS query logs from its resolvers, and the
   load balancers' access logs **without** request bodies or the `Authorization` header, all to the SIEM.
7. **Threat detection:** the provider's own service (Amazon GuardDuty, Microsoft Defender for Cloud, Google Security
   Command Center, or equivalent) on the account, the cluster and the object store, with its findings sent to the
   SIEM.
8. **The network map:** drawn from what was built, not what was planned, and dated (D1 §7). The proposed Security
   Rule asks for it, with an asset inventory, reviewed every 12 months and on change (UPGRADES.md §3.5).

**Gate 1, the network:** D1 §8's checklist passes; a connection from inside the pipeline's subnets to an address
not on the allow-list is refused and logged; the audit trail, flow logs and threat detection findings reach the
SIEM. *Not tried:* the egress firewall and proxy with the stages, private endpoints on a cloud deployment.

## 2. Time, certificates and secrets

**Steps:**
1. **Time:** every node, database and broker synchronised to the provider's time service (or the customer's NTP
   servers), with an alert on drift over a second. Tokens are checked to 60 seconds of skew; leases, the audit
   journal's timestamps and the SIEM's correlation all assume clocks that agree.
2. **The internal CA,** or cert-manager with one, for every TLS certificate the pipeline uses. None is from a
   public CA, as nothing is on the internet but, at most, the push load balancer.
3. **The certificate register:** each certificate, its name, its issuer, who renews it, and when it expires:

   | Certificate | For | Notes |
   |---|---|---|
   | The push endpoint's | the landing node, or the load balancer | with mTLS, also the producers' client certificates and their CA |
   | Each control API's | `sdh-control-<stage>` | read at start: restart after renewal (CONTROL-API-DEPLOYMENT.md §2.1) |
   | Automation's client certificates | the control API in `mtls` mode | not checked for revocation: issue them short-lived (CONTROL-API-DEPLOYMENT.md §3.2) |
   | The database's | Postgres or PgBouncer | the stages connect with `verify-full` |
   | The broker's | Kafka's TLS listener | |
   | The collector's, if outside the cluster | OTLP over `https://` | untried (OBSERVABILITY.md §9) |
   | The object store's, if self-hosted | SeaweedFS's S3 gateway, MinIO | |

   Alert 30 days before each expires, and again at 7. Renew well ahead: a pod that reads its certificate at start
   is restarted one replica at a time.
4. **Secrets:** Secrets encrypted at rest with a KMS key, or an external secret store (HARDENING.md (D4) §2.2).
   Nothing secret in a ConfigMap, a URI or a manifest in Git (the profile refuses a secret in a URI).

**Gate 2:** every node reports synchronised time; the certificate register is complete, with its expiry alerts
firing in a test; no secret is found in the manifests or ConfigMaps (a secrets scan of the deployment repository).

## 3. The cluster and the platform services

**Steps:**
1. **The cluster,** as HARDENING.md (D4) §2 and §3: a private API endpoint; the restricted Pod Security Standard on
   every pipeline namespace; the API server's audit log to the SIEM; admission of **signed images by digest only**,
   checked against the vendor's `cosign.pub` (each kit's README, "Verify what you received"); nodes to a CIS
   benchmark, patched on a schedule, with swap off (HIPAA-OPERATIONS.md §2.9).
2. **Placement:** nodes in at least two zones; each stage's replicas and its control pods spread across them, with
   PodDisruptionBudgets (CONTROL-API-DEPLOYMENT.md §1).
3. **Postgres,** **Kafka** and the **object store**, as D4 §4: TLS, encryption at rest with the customer's key, no
   public address, a database per pipeline (`deploy/reference/postgres.sql`), ACLs per stage
   (`deploy/reference/kafka-acls.sh`), bucket policies refusing plaintext and public access, and versioning or
   object lock on bronze at least (DEPLOYMENT.md, "Object store versions").
4. **Sizing:** Postgres and Kafka for the initial load, not the steady state (DEPLOYMENT.md, "Sizing Postgres and
   Kafka for an initial load" and "Postgres under load").

**Gate 3:** D4 §5's checklist passes; an unsigned image, and one by tag rather than digest, are refused admission.

## 4. Identity and keys

**Steps:**
1. **Workload identities:** one per stage, per pipeline, with no static keys (IDENTITY.md (D2) §2).
2. **People:** the identity provider connected to the control API, with MFA and the approved permission mapping
   (IDENTITY-PROVIDER.md (D15), CONTROL-API-DEPLOYMENT.md (D13) §3 to §5). `kubectl exec` granted per namespace,
   through SSO with MFA, to named staff only (D2 §3.2).
3. **Keys,** as KEYS.md (D3): the KEKs per pipeline and layer in the KMS, key policies naming each stage's
   identity, key administrators other than the pipeline's operators, rotation on. Operators' sealing keys
   registered by someone other than their owner (CONTROL-API-DEPLOYMENT.md §7).
4. **Break-glass accounts:** created, their credentials sealed, their use alerting in the SIEM (D2 §6); test one
   once, then reseal it.

**Gate 4:** CONTROL-API-DEPLOYMENT.md §8's checklist passes; a stage's identity can't use another stage's or
another pipeline's key, bucket or topic (try it from a debug pod with the restricted profile).

## 5. The pipeline

**Steps:**
1. **The settings,** from `deploy/reference/` with the profile at `hipaa` and nothing waived (CONFIGURATION.md
   (D6)). Every value that differs from the reference is in the change record, with why.
2. **Deploy** the stages and their control Deployments from the kits' READMEs, by image digest. Fill in
   `<pipeline>` in each Deployment's `OTEL_RESOURCE_ATTRIBUTES` (OBSERVABILITY.md §2.2).
3. **The network policies** of NETWORK.md §4 in every pipeline namespace, with its own values.
4. **The WSTL set** for the transform stage, in an immutable ConfigMap named for its digest, pinned in
   `sdh.transform.wstl-digest` (UPGRADES.md (D12) §3.4).
5. **Record the baseline:** the manifests and settings in a Git repository the deployers can't rewrite (protected
   branches, reviewed merges); each stage's `GET /api/v1/settings` and `GET /api/v1/version` saved with the change
   record. Changes from here on go through that repository (UPGRADES.md §3.5).

**Gate 5:** `GET /api/v1/compliance` on every stage shows no finding and no waiver; CONFIGURATION.md §10's
checklist passes; the saved settings match the repository.

## 6. Observability and the audit trail

**Steps:**
1. **The collector,** with its three pipelines, as OBSERVABILITY.md (D16) §3; the audit events to a store the
   pipeline's staff can't change (HIPAA-OPERATIONS.md §2.7).
2. **Alerts:** D16 §6.1's operational alerts, and the security reviews and alerts of HIPAA-OPERATIONS.md §3.1, in
   the SIEM; who answers each, and how fast.
3. **Drift:** an alert when the deployed manifests or a stage's settings differ from the baseline of phase 5 (a
   GitOps controller's drift report, or a scheduled comparison of `GET /api/v1/settings` with the saved copy); and
   on any `compliance.waived`, `compliance.off` or `compliance.refused` event.
4. **The weekly journal check:** `audit-verify` scheduled, and its result reviewed (HIPAA-OPERATIONS.md §3.2).

**Gate 6:** D16 §7 and HIPAA-OPERATIONS.md §2.7's "Checking it works" pass, the outage test included; a change to a
setting outside the repository raises the drift alert.

## 7. Acceptance, with synthetic data

**Steps:**
1. **Synthetic documents only:** the vendor's samples, or the customer's own synthetic set; never production data,
   even on production before gate 9.
2. **End to end:** land documents through each intake and format the pipeline will take; check they reach gold,
   and that a malformed one is held as a failure, not dropped (HIPAA-OPERATIONS.md §1).
3. **Operations:** pause and drain a stage (§7.1 there); export, correct and retry a failure; look a synthetic
   patient up (§4.1); run a purge with four eyes; a break-glass restore of a synthetic item (§5).
4. **Resilience:** delete a stage's pod, and a node, while documents land; nothing is lost or duplicated.
5. **The backup and restore drill:** restore the database to a point in time, and the object store's versions, into
   a scratch environment, following HIPAA-OPERATIONS.md §7, and time it. It must meet the RTO set in phase 0.
6. **Capacity:** run a load at the expected peak, and at the initial load's rate if there is one, and watch
   Postgres and Kafka (HIPAA-OPERATIONS.md §7.2).

**Gate 7:** each step's result recorded, with the restore drill's time against the RTO. *Not tried:* the vendor's
control harness runs these scenarios (access, break-glass, failover, failures, keys, outages, pause, restore,
retention, support) only against its own targets, Testcontainers, the sandbox and the AWS PoC, not a customer's
cluster; until it has a target for one, these steps are by hand.

## 8. The penetration test, the fixes and the retest

**Steps:**
1. **Commission it** from a third party of the customer's choosing (HIPAA-OPERATIONS.md §11). Test the environment
   as built through gate 7, with synthetic data, the profile on and nothing waived. Follow the cloud provider's
   policy on testing.
2. **Agree the rules of engagement in writing:** the scope (below), the testers' source addresses, the dates and
   hours, who may stop the test, whether the SIEM's watchers are told, what may not be done (denial of service
   beyond agreed limits, social engineering of staff unless agreed), and how findings are reported (encrypted, to
   named people).
3. **The scope,** at least:
   - **From outside:** the push ingress, and whatever else answers on the internet (nothing else should).
   - **From the operator's network:** the control API on every stage, its authentication and permissions, reaching
     another pipeline's or another item's data, sealed results, replayed requests, the OpenAPI contract
     (`/api/v1/openapi.yaml`) as the map.
   - **From a compromised pod** (an assumed-breach test in a pipeline namespace): what it can reach on the network,
     what its identity can read, whether it can read another stage's keys or messages, or forge a message.
   - **The cloud and cluster configuration:** IAM, key and bucket policies, network rules, the cluster's RBAC and
     admission, against the provider's benchmark.
   - **Out of scope** unless agreed: the identity provider's own service, the cloud provider's infrastructure.
4. **Secure before it,** as HIPAA-OPERATIONS.md §11's table, area by area; a tester will look there first.
5. **Fix what it finds,** in order of severity. A finding in the product goes to the vendor (VENDOR-COMMITMENTS.md
   (D11)); a finding in the environment is the customer's. Record each with its fix, or the risk accepted and by
   whom.
6. **Retest:** the testers confirm each Critical and High fix, in a retest of their own, before gate 9. Medium and
   below may be retested at the next test.
7. **Keep** the report, the rules of engagement, the fixes and the retest's result with the risk analysis.
8. **Repeat** at least every 12 months, and after a major change to the environment: a new intake type open to
   partners, a new region, a change of identity provider, a new network path in.

**Gate 8:** no open Critical or High finding; each Medium or Low fixed or accepted in writing by the security
official.

## 9. Go-live

**Steps:**
1. **The risk analysis** updated with this environment: the data flows (DATA-FLOWS.md (D5) §6), the gates' results,
   the pen test, the accepted risks.
2. **The asset inventory and network map** current and dated.
3. **The runbooks in place** for the operators: HIPAA-OPERATIONS.md (daily, weekly, incidents, requests from
   individuals), UPGRADES.md for changes, RETENTION.md (D9) for removals.
4. **Incident readiness:** who is called, how, and in what order, written down and tried once (a tabletop
   exercise); the vendor's security contact; the cloud provider's support plan.
5. **Access reviewed:** the permission mapping, the break-glass accounts sealed, the deployers' build-time rights
   removed or reduced (D2 §7).
6. **Sign-off** by the security official and the privacy official, dated, naming the pipelines and intakes it
   covers. Only then is the first real PHI sent.

## 10. Afterwards

- **On every change,** UPGRADES.md: a release, a WSTL set, a pipeline's configuration (§3.5), with the gates that
  change touches run again (a new intake: gates 5 to 7, and a pen test if it opens a new path in).
- **Every 12 months:** the network map and asset inventory reviewed, the access reviewed, the pen test repeated,
  the restore drill repeated.
- **At the end:** RETENTION.md §6 for the end of a contract; decommissioning the environment removes its keys last,
  as the erasure certificate names them (RETENTION.md §4).

## 11. Findings

- **No customer environment has been set up this way.** The phases come from the reference documents and the PoC's
  experience, not from a run.
- **The control harness has no target for a customer's cluster** (`TestcontainersTarget`, `SandboxTarget`,
  `AwsTarget` only), so gate 7's scenarios are by hand. A target configured by URL, issuer and namespace would let
  a customer run the harness's scenarios on their own test environment. Recorded in KNOWN-GAPS.md §5.
- **The push intake has no rate or connection limit of its own;** the load balancer or WAF must give them (§1 step
  5; D1 §9).
- **Drift detection is the customer's:** the product exposes its settings and version (`GET /settings`,
  `/version`) but doesn't report a change itself; a settings digest and an event on change are a gap already
  recorded (KNOWN-GAPS.md §5, the configuration digest).
- **The product has had no penetration test or dynamic scan by the vendor** (HIPAA.md §11.1 P13; KNOWN-GAPS.md §3),
  so the customer's test is, today, the first.

## Sources

| Statement | Source |
|---|---|
| Zones, listeners, egress, policies, private endpoints, the operator's path, the network map | NETWORK.md §1–§8 |
| Cluster, node and platform service hardening | HARDENING.md §2–§5 |
| Identity, permissions, break-glass, access reviews | IDENTITY.md; IDENTITY-PROVIDER.md; CONTROL-API-DEPLOYMENT.md |
| Keys and their policies | KEYS.md |
| Settings and the compliance profile | CONFIGURATION.md; `deploy/reference/` |
| Telemetry, the collector, the audit store | OBSERVABILITY.md; HIPAA-OPERATIONS.md §2.7, §3 |
| Backups, restore, RPO and RTO, capacity | HIPAA-OPERATIONS.md §7, §7.2; DEPLOYMENT.md |
| The pen test's areas, and its repetition | HIPAA-OPERATIONS.md §11; HIPAA.md §7 |
| Changes after go-live | UPGRADES.md §3.4, §3.5 |
| Token clock skew of 60 seconds | `connector-stage` `control/ControlConfiguration` (`createDefaultWithIssuer`); CONTROL-API-DEPLOYMENT.md §3.1 |
| The harness's targets and scenarios | `control-harness` `Target` implementations; `scenario/` |
| No rate limit on push | NETWORK.md §9; DESIGN.md §5.3.15 |
