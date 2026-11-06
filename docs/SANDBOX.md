# The demo sandbox

A Kubernetes cluster on one workstation, laid out as a customer would run the pipeline, for demonstrating it under
realistic conditions: several nodes, failover, a replicated broker, a key service, network policies, the
restricted Pod Security Standard and telemetry. It isn't for load: the cluster is sized to show how the pipeline
behaves, not how fast it goes (for that, see §13 of [DESIGN.md](DESIGN.md)). Written 2026-11-05.

Everything runs through `scripts/sandbox.sh`. See [DEPLOYMENT.md](DEPLOYMENT.md) for why the stages are deployed
this way.

## Where it lives

All of it is on the sandbox disk named by `SDH_SANDBOX`, set in `~/.config/sdh/sandbox.env` (so that NetBeans and
Maven see it as well as the shell):

```sh
SDH_SANDBOX=/mnt/sandbox
```

| Path | What |
|---|---|
| `$SDH_SANDBOX/minikube/.minikube` | minikube's home for this project's profiles: `sdh-it` (the Docker VM for builds and integration tests), `sdh-k8s` (the single-node Kubernetes test, §15.2) and `sdh-demo` (this cluster). VM disks, ISO and caches. |
| `$SDH_SANDBOX/sdh-demo/state` | The sandbox's secrets (mode 700): database and S3 credentials, Vault's unseal key and root token, the stages' Vault tokens, the inbox and FHIR-stage key pairs, the sandbox CA. Made once and reused. **Never delete it while the cluster holds data**: what is stored can't be read without it. |
| `$SDH_SANDBOX/minikube/kube` | The profiles' kubeconfigs, one file each (`sdh-demo`, `sdh-k8s`). |

The scripts set `MINIKUBE_HOME` themselves (`scripts/minikube-home.sh`). For minikube used by hand, and for
anything started from the desktop, `~/.profile` sets the same home, with a default kubeconfig beside it, so
nothing minikube makes is kept in the home directory:

```sh
export MINIKUBE_HOME=/mnt/sandbox/minikube
export KUBECONFIG=/mnt/sandbox/minikube/kube/config
```

To reach a profile by hand, use its own kubeconfig:

```sh
KUBECONFIG=/mnt/sandbox/minikube/kube/sdh-demo minikube -p sdh-demo kubectl -- get pods -A
```

If the disk isn't mounted, the scripts refuse to run rather than fill the root filesystem.

## Size

The workstation has 32 threads and 30 GB of memory; 4 threads and about 4 GB are left for the host.

| VM | Nodes | CPUs | Memory | Disk |
|---|---|---|---|---|
| `sdh-demo` | 3 | 6 each | 6 GB each | 120 GB each |
| `sdh-it` | 1 | 8 | 8 GB | 100 GB |

Both fit at once (26 GB), so images can be built while the cluster runs. `sdh-k8s` (4 CPUs, 8 GB) is for its own
test, with the others stopped. The cluster's settings apply when it is first made: `SDH_DEMO_NODES`,
`SDH_DEMO_CPUS`, `SDH_DEMO_MEMORY`, `SDH_DEMO_DISK`.

## Using it

```sh
scripts/sandbox.sh up        # make or start the cluster and the shared services (about 10 minutes the first time)
scripts/sandbox.sh images    # build both customer images in sdh-it, push them to the cluster's registry
scripts/sandbox.sh deploy    # deploy each pipeline's landing node and splitter
scripts/sandbox.sh check     # push the sample C-CDA documents through, and check the result
scripts/sandbox.sh push ccda-push some.xml   # push your own files (synthetic or de-identified only)
scripts/sandbox.sh status    # nodes, pods, topic counts
scripts/sandbox.sh grafana   # http://localhost:3000, user admin, password in state/secrets/grafana
scripts/sandbox.sh stop      # stop the VMs; `up` starts them again with everything kept
scripts/sandbox.sh delete --yes   # delete the cluster and its volumes (state/ is kept)
```

The cluster stays running after `up` and `deploy`; stop it when the demo is over. `images` stops `sdh-it`
afterwards unless `SDH_IT_STOP=0`.

## Layout

```
 sdh-producers           sdh-ccda-push (one namespace per pipeline)            sdh-fhir (reserved)
 ┌──────────┐  mTLS   ┌──────────────────────┐ item.landed ┌──────────┐ records ┌──────────────────┐
 │ producer ├────────►│ landing (active)     ├────────────►│ splitter ├─────────►│ FHIR stage, not  │
 └──────────┘  push   │ landing (standby)    │  Kafka      └────┬─────┘  Kafka   │ yet built        │
                      └──────────┬───────────┘                  │                └────────┬─────────┘
                                 │ bronze                       │ silver                  │ gold
 sdh-infra   ┌───────────────────▼──────────────────────────────▼─────────────────────────▼─────────┐
             │ SeaweedFS (S3)   Postgres (catalog, leases)   Kafka ×3   Vault (Transit)              │
             │ OpenTelemetry collector → Grafana LGTM                                                │
             └──────────────────────────────────────────────────────────────────────────────────────┘
```

**Nodes and storage.** Three nodes, Calico for networking (minikube's default network doesn't enforce network
policies). Volumes are node-local (`local-path`, `deploy/sandbox/cluster/`), on each node's disk, which is on the
sandbox disk; a pod with a volume runs on its volume's node, as with local disks in production. Volumes are kept
when their claim is deleted (`Retain`).

**Shared services** (`sdh-infra`, `deploy/sandbox/infra/`):
- **Postgres:** the Iceberg JDBC catalog and the leases. Each stage logs in as its own role, but all act as
  `sdh_catalog`, because a JDBC catalog keeps every namespace in the same tables.
- **Kafka:** three nodes (KRaft), one per cluster node; every topic has three replicas and needs two for a write.
  Topics are made by the script, as a customer's broker team would; brokers don't create them.
- **SeaweedFS:** S3 object storage, a bucket per medallion layer (below).
- **Vault:** the Transit engine holds every KEK and signing key (`vault-transit`, design §7.4); none leaves it.
  Each stage has a token whose policy allows only its own keys.
- **OpenTelemetry collector and Grafana LGTM:** the stages export to the collector, which passes it to Grafana's
  all-in-one backend (Tempo, Prometheus, Loki). Only allow-listed attributes and audit events leave the stages.

**A pipeline** (`sdh-<pipeline>`, `deploy/sandbox/pipeline/`): each stage is deployed from the Deployment in its
README, as customers get it, with the image by digest from the cluster's registry, and with what a production
cluster adds around it (kustomize patches): requests equal to limits, the push port and its Service, a TCP
readiness probe, and the two landing replicas on different nodes, so that losing a node fails over. The default
pipeline is `ccda-push`: C-CDA documents pushed over HTTPS with mTLS.

**Producers** (`sdh-producers`): a pod with its own client certificate from the sandbox CA, which pushes files.

**Network policies:** each pipeline's namespace denies everything but DNS, the shared services' ports and, for the
landing node, pushes from `sdh-producers`. The shared services accept only namespaces labelled
`sdh.smiledigitalhealth.com/infra-client`, each on its own port. Grafana is reached by port-forward only.

## Medallion layers

Each layer has its own bucket and Iceberg namespace, and each stage's S3 key may use only the buckets it needs:

| Layer | Bucket | Iceberg namespace | Written by | Read by |
|---|---|---|---|---|
| Bronze: items as landed | `sdh-bronze` | `<pipeline>_landing` | landing node | splitter |
| Silver: records, split and converted | `sdh-silver` | `<pipeline>` | splitter | FHIR stage |
| Gold: FHIR resources | `sdh-gold` | `<pipeline>_fhir` | FHIR stage | consumers |

Everything is Iceberg tables of Parquet files. Items and records are encrypted under the pipeline's KEKs in
Vault; the splitter seals its messages to the FHIR stage's key.

## The FHIR stage

Not built yet: it is the planned `healthcare-data-transformer` project, which runs the Whistle mapping (the
`mapping` repository, US Core 6.1) on each record, fail-fast, writes the resources to the gold layer, and sends
what fails to a dead-letter topic for its report mode. The sandbox has its place ready:
- the namespace `sdh-fhir`, with a quota (3 CPUs, 4 GB) the pipelines can't take, and the same network policies;
- per pipeline: the topics `<pipeline>.records` (its input) and `<pipeline>.fhir.dlq`; the gold bucket and an S3
  key that reads silver and writes gold; a database role; a Vault token that unwraps the records KEK and uses
  its own gold KEK (`<pipeline>-fhir`); the private key the splitter seals records to;
- the secret and ConfigMap `fhir-<pipeline>` in `sdh-fhir`, naming all of these.

Deploying it is then one more Deployment in `sdh-fhir`.

## Not yet as in production

- **TLS inside the cluster:** Postgres, Kafka, S3 and Vault are reached without TLS, and Kafka without
  authentication; the network policies limit who can connect. Pushes from producers are TLS with mTLS.
- **Vault:** one unseal key, kept in `state/`, rather than shares or auto-unseal; periodic tokens rather than
  Kubernetes auth through Vault Agent; the root token isn't revoked. `up` unseals Vault after a restart, and
  renews the tokens (a token unused for 32 days expires).
- **Catalog:** a JDBC catalog can't limit a stage to its own namespaces; the buckets do that for the data. A REST
  catalog (Polaris, tested in §15.1) could.
- **Buckets:** a stage's key may use its whole bucket, not only its own pipeline's prefix.
- **One object store and one database**, each a single pod: losing their node stops the pipeline until it is back
  (nothing is lost; the volumes are on the node's disk).
- **Landing updates:** both landing replicas are replaced together (`Recreate`). Only the active node is ready,
  so a rolling update can't make a new pod ready while the old active node lives: tried, it waited until it timed
  out. Producers are refused for the restart's length, and retry. The Deployment always shows one of two replicas
  ready, so `kubectl rollout status` never completes for it; the script waits for one ready pod instead. A
  readiness signal that a standby could pass, with pushes still sent only to the active node, would allow rolling
  updates; it would need a change to the landing node.

## Checked

On 2026-11-05, with the `ccda-push` pipeline:
- `check`: five synthetic and de-identified C-CDA documents pushed over mTLS, each landed with a receipt, five
  `item.landed` messages, all split (`file.completed`), 15 messages on `ccda-push.records`, and no document
  content or filenames in any pod's log.
- **Layers:** data in `sdh-bronze` and `sdh-silver`, none in `sdh-gold`. Each stage's S3 key, tried from a pod:
  the splitter's lists bronze but can't write it; the landing node's can't list silver; the FHIR stage's writes
  and deletes in gold and can't write silver.
- **Telemetry:** both stages' traces in Tempo and their `sdh_*` metrics in Prometheus, through the collector.
- **Losing a node:** the node of the active landing node stopped (taking a Kafka broker with it). The standby
  took over in about 10 seconds, and the next five documents were landed and split with two brokers. The node
  restarted, the broker caught up (no under-replicated partitions), and the lost landing pod, rescheduled, waited
  as the new standby. The producer, which was on the stopped node, had to be deleted to move sooner than
  Kubernetes' five minutes.
- **Restarting:** `stop`, then `up`: Vault came back sealed and was unsealed, the stages restarted by themselves
  (one landing node active, one standby), and five more documents went through.
- **Rollouts:** see Landing updates, above.
