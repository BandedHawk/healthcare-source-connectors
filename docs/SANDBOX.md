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
| `$SDH_SANDBOX/sdh-demo/state` | The sandbox's secrets (mode 700): database and S3 credentials, Vault's unseal key and root token, the stages' Vault tokens, the stages' inbox key pairs and the FHIR stage's, the sandbox CA. Made once and reused. **Never delete it while the cluster holds data**: what is stored can't be read without it. |
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

The workstation has 32 threads and 30.6 GiB of memory, and runs Linux Mint's desktop, NetBeans and a browser.

| VM | Nodes | CPUs | Memory | Disk |
|---|---|---|---|---|
| `sdh-demo` | 3 | 6 each | 7.5 GiB (control plane), 6.75 GiB (each other) | 120 GB each |
| `sdh-it` | 1 | 8 | 8 GiB | 100 GB |
| `sdh-k8s` | 1 | 4 | 8 GiB | 40 GB |

Only one of them runs at a time: the cluster alone holds 21 GiB, and with `sdh-it` it would leave the desktop
too little (Memory, below). `images` therefore stops the cluster while `sdh-it` builds, and starts it again to
load them. The CPU count and disk apply when the cluster is first made (`SDH_DEMO_NODES`, `SDH_DEMO_CPUS`,
`SDH_DEMO_DISK`); the memory (`SDH_DEMO_CP_MEMORY`, `SDH_DEMO_MEMORY`, in MiB) is applied to the stopped VMs at
every `up`, with everything on them kept.

## Memory

Sized on 2026-11-07, after the control-plane node ran out of memory under load (Checked, below), so that neither
the workstation nor a node can be starved. Three rules, each enforced rather than hoped for:

**1. The VMs never take more than the host can spare.** A KVM guest takes memory from the host as it uses it and
doesn't give it back, so a VM ends up holding all it was given. Every script that starts a VM (`sandbox.sh up`,
`it-minikube.sh`, `build-image.sh`, `k8s-minikube.sh`) first checks, in `minikube-home.sh`, that the VMs already
running plus the new ones, each with 192 MiB of QEMU's own, stay within the host's memory less
`SDH_HOST_RESERVE` (9 GiB: the desktop with NetBeans and a browser open used 8.6 GiB), and that the host has
that much available now. If not, it refuses and names the VMs running.

| | MiB |
|---|---|
| Host memory | 31,370 |
| Kept for the desktop (`SDH_HOST_RESERVE`) | 9,216 |
| What the VMs may hold | 22,154 |
| The cluster: 7,680 + 2 × 6,912, plus 3 × 192 for QEMU | 22,080 |
| Left over | 74 |

`sdh-it` (8,192 + 192) doesn't fit beside it, nor does `sdh-k8s`.

**2. Each node offers pods only what it really has.** minikube tells each node's kubelet that the whole VM is
for pods, and turns off eviction; but a node's own use (the kernel, Docker, the kubelet, and minikube's root
filesystem, which is in memory) is about 1.5 GiB, and the control-plane node also runs Kubernetes' own pods,
which reserve nothing (the API server alone used 430 MiB). So every node was offered pods it couldn't hold, and
the kernel's out-of-memory killer, not Kubernetes, decided what to stop. `up` now sets each kubelet's
`systemReserved` to what was measured, and evicts pods when less than 256 MiB is free (minikube rewrites the
kubelet's configuration at each start, so `up` sets it each time). A pod that doesn't fit then waits, unscheduled,
rather than starving the node. Every pod's request equals its limit, so the scheduler's sums are what the pods
can use.

| Node | VM | Seen by Kubernetes | Kept from pods | Eviction | For pods |
|---|---|---|---|---|---|
| Control plane | 7,680 | 7,429 | 2,304 | 256 | 4,869 |
| Each other | 6,912 | 6,673 | 1,664 | 256 | 4,753 |

(The guest's kernel keeps about 250 MiB of each VM before Kubernetes sees it.)

**3. The control-plane node is for the shared services whose data is on it,** and KEDA. Their volumes are
node-local, so Postgres, SeaweedFS, Vault, Grafana LGTM and Kafka's first broker can only run there. `up` taints
it (`node-role.kubernetes.io/control-plane:NoSchedule`; those five tolerate it, and KEDA is pinned there) and
moves anything else off it, so that a restarting service always finds its memory free.

| On the control-plane node | MiB |
|---|---|
| Kafka (first broker; 384 MiB heap) | 768 |
| Grafana LGTM | 1,536 |
| SeaweedFS | 768 |
| Postgres | 512 |
| Vault (was 256, which it exceeded) | 384 |
| Kubernetes' pods that reserve some (etcd, CoreDNS, metrics-server) | 370 |
| KEDA's three pods (held to 128 MiB each; they ask 100 and may grow to 1,000; they use under 50) | 3 × 128 |
| Total, of 4,869 | 4,722 |

| On the other two nodes | MiB |
|---|---|
| Kafka (two brokers) | 2 × 768 |
| Landing node (one pod, no standby) | 1,024 |
| Split stage, one to two pods (768 MiB heap; its README says 2 GiB, the samples are small) | 2 × 1,536 |
| Convert stage, one to two pods (its README says 2 GiB) | 2 × 1,024 |
| OpenTelemetry collector | 256 |
| Producer | 64 |
| Total at most, of 2 × 4,753 = 9,506 | 8,000 |

A total isn't enough: a pod needs its memory on one node. In the second test run, with both convert pods already
up, one on each node, neither node had the 1,536 MiB a second split pod needs, and it waited for minutes. So each
stage's two pods must run on different nodes (a required anti-affinity in `splitter-patch.yaml` and
`convert-patch.yaml`), and each node is sized for one pod of each beside the rest, however they arrive. The busier
node holds a Kafka broker (768), a split pod (1,536), a convert pod (1,024), and at worst the landing pod, the
collector and the producer too (1,344): 4,672 of its 4,753. A third pod of either stage doesn't fit, so KEDA's
maximum is two. The pods do use their memory: under load the convert pods used about 940 MiB of their 1,024, the
landing pod 896 of its 1,024, and the split pods 1,170 of their 1,536.

## Using it

```sh
scripts/sandbox.sh up        # make or start the cluster and the shared services (about 10 minutes the first time)
scripts/sandbox.sh images    # build the three customer images in sdh-it (the cluster stopped), push them to its registry
scripts/sandbox.sh deploy    # deploy each pipeline's landing node, split stage and convert stage
scripts/sandbox.sh check     # push the sample C-CDA documents through, and check the result
scripts/sandbox.sh push ccda-push some.xml   # push your own files (synthetic or de-identified only)
scripts/sandbox.sh scale-demo [count]   # push count copies of the samples (300), and watch KEDA scale out
scripts/sandbox.sh status    # nodes, pods, topic counts, scaling
scripts/sandbox.sh grafana   # http://localhost:3000, user admin, password in state/secrets/grafana
scripts/sandbox.sh stop      # stop the VMs; `up` starts them again with everything kept
scripts/sandbox.sh delete --yes   # delete the cluster and its volumes (state/ is kept)
```

The cluster stays running after `up` and `deploy`; stop it when the demo is over. `images` stops `sdh-it`
afterwards unless `SDH_IT_STOP=0`.

## Layout

```
 sdh-producers         sdh-ccda-push (one namespace per pipeline)                         sdh-fhir (reserved)
 ┌──────────┐ mTLS  ┌───────────────────┐ item.landed ┌──────────┐ split ┌─────────┐ records ┌──────────────┐
 │ producer ├──────►│ landing (one pod) ├────────────►│ splitter ├──────►│ convert ├────────►│ FHIR stage,  │
 └──────────┘ push  │                   │  Kafka      │ 1–2 pods │ Kafka │1–2 pods │  Kafka  │ not yet built│
                    └─────────┬─────────┘                  │ silver           │ silver       └──────┬───────┘
                              │ bronze                     │                  │                     │ gold
 sdh-infra   ┌────────────────▼────────────────────────────▼──────────────────▼─────────────────────▼───────┐
             │ SeaweedFS (S3)   Postgres (catalog, leases, stage registries)   Kafka ×3   Vault (Transit)   │
             │ OpenTelemetry collector → Grafana LGTM                                                       │
             └──────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Nodes and storage.** Three nodes, Calico for networking (minikube's default network doesn't enforce network
policies). Volumes are node-local (`local-path`, `deploy/sandbox/cluster/`), on each node's disk, which is on the
sandbox disk; a pod with a volume runs on its volume's node, as with local disks in production. Volumes are kept
when their claim is deleted (`Retain`).

**Shared services** (`sdh-infra`, `deploy/sandbox/infra/`):
- **Postgres:** the Iceberg JDBC catalog, the leases, and each stage's registry of its items and records (a schema
  per stage and pipeline: `split_<pipeline>`, `convert_<pipeline>`). Each stage logs in as its own role, but all
  act as `sdh_catalog`, because a JDBC catalog keeps every namespace in the same tables.
- **Kafka:** three nodes (KRaft), one per cluster node; every topic has three replicas and needs two for a write.
  Topics are made by the script, as a customer's broker team would; brokers don't create them. The stages' inputs
  (`<pipeline>.landed`, `<pipeline>.split`) have six partitions, the most pods KEDA could give a stage; the rest
  have one. A topic made with fewer is given more by `up` (partitions are only ever added; do it drained).
- **KEDA** (namespace `keda`, version pinned in the script, its manifest checked against a pinned SHA-256 and kept
  in `$SDH_SANDBOX/sdh-demo/downloads/`): scales the split and convert stages on their Kafka consumer lag (below).
- **SeaweedFS:** S3 object storage, a bucket per medallion layer (below).
- **Vault:** the Transit engine holds every KEK and signing key (`vault-transit`, design §7.4); none leaves it.
  Each stage has a token whose policy allows only its own keys.
- **OpenTelemetry collector and Grafana LGTM:** the stages export to the collector, which passes it to Grafana's
  all-in-one backend (Tempo, Prometheus, Loki). Only allow-listed attributes and audit events leave the stages.

**A pipeline** (`sdh-<pipeline>`, `deploy/sandbox/pipeline/`): each stage is deployed from the Deployment in its
README, as customers get it, with the image by digest from the cluster's registry, and with what a production
cluster adds around it (kustomize patches): requests equal to limits, the push port and its Service, and a TCP
readiness probe. The landing node runs as one pod, with no standby (decided 2026-11-07, for memory): if it dies,
Kubernetes starts another, on another node if its node is gone, and it lands once the old pod's lease has
expired. The split stage (Deployment `splitter`) and the convert stage (`convert`) are scaled by KEDA
(`deploy/sandbox/pipeline/scaling.yaml`), from one pod to two each (what the memory is sized for, above), on
their consumer groups' lag: another pod for
each 5 items waiting to be split, and for each 5 records waiting to be converted. Never fewer than one: the pod
holding a stage's duties runs its reconciler, and a lost message adds no lag, so a stage scaled to nothing would
never be woken to recover it. Back to one pod two minutes after the backlog is gone. The script leaves their
replica count to KEDA, so a `deploy` doesn't reset it. The landing node is never scaled: it lands serially, and
never more than one pod is in service, since landing in parallel could land a file twice. The split and convert
stages have less memory than their READMEs say (above). The default pipeline is `ccda-push`: C-CDA documents
pushed over HTTPS with mTLS.

**Producers** (`sdh-producers`): a pod with its own client certificate from the sandbox CA, which pushes files.

**Network policies:** each pipeline's namespace denies everything but DNS, the shared services' ports and, for the
landing node, pushes from `sdh-producers`. The shared services accept only namespaces labelled
`sdh.smiledigitalhealth.com/infra-client`, each on its own port, and KEDA's operator, on Kafka's. Grafana is
reached by port-forward only.

## Medallion layers

Each layer has its own bucket and Iceberg namespace, and each stage's S3 key may use only the buckets it needs:

| Layer | Bucket | Iceberg namespace | Written by | Read by |
|---|---|---|---|---|
| Bronze: items as landed | `sdh-bronze` | `<pipeline>_landing` | landing node | split stage |
| Silver: records as split (`split_records`) | `sdh-silver` | `<pipeline>` | split stage | convert stage |
| Silver: records converted (`records`) | `sdh-silver` | `<pipeline>` | convert stage | FHIR stage |
| Gold: FHIR resources | `sdh-gold` | `<pipeline>_fhir` | FHIR stage | consumers |

Everything is Iceberg tables of Parquet files. Items and records are encrypted under the pipeline's KEKs in
Vault. Each stage seals its messages to the next one's key: the landing node to the split stage's inbox, the split
stage to the convert stage's, and the convert stage to the FHIR stage's.

## The FHIR stage

Not built yet: it is the planned `healthcare-data-transformer` project, which runs the Whistle mapping (the
`mapping` repository, US Core 6.1) on each record, fail-fast, writes the resources to the gold layer, and sends
what fails to a dead-letter topic for its report mode. The sandbox has its place ready:
- the namespace `sdh-fhir`, with a quota (3 CPUs, 4 GB) the pipelines can't take, and the same network policies;
- per pipeline: the topics `<pipeline>.records` (its input) and `<pipeline>.fhir.dlq`; the gold bucket and an S3
  key that reads silver and writes gold; a database role; a Vault token that unwraps the records KEK and uses
  its own gold KEK (`<pipeline>-fhir`); the private key the convert stage seals records to, and the convert
  stage's signing public key (`convert-signer.pem`), which its messages are checked against;
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
- **No landing standby:** one landing pod, so losing it, or its node, stops landing until Kubernetes has started
  another and the old pod's lease has expired; producers are refused meanwhile, and retry. A production cluster
  can run the README's two replicas, an active node and a standby, for a takeover in seconds.
- **Landing updates:** the landing pod is replaced by stopping it first (`Recreate`): it is ready only once it
  holds the lease, so a rolling update can't make a new pod ready while the old one lives: tried, it waited
  until it timed out. Producers are refused for the restart's length, and retry.
- **One workstation:** the cluster and the build VM don't run at once (Memory, above).
- **The registry** (minikube's addon) keeps images only in its container, so a restart empties it; `up` pushes
  them again from the first node's Docker, where they are kept on disk.
- **Object storage can lose its last writes in a host crash.** SeaweedFS runs with its defaults: it doesn't
  sync each write to disk, and keeps its file index in memory. Postgres does sync, so after a crash the Iceberg
  catalog can name files SeaweedFS no longer has, and the stages fail at startup (`Location does not exist`).
  S3 and the other object stores a customer would use don't lose an acknowledged write. Repaired by hand once
  (Checked, below); stop the cluster (`stop`) rather than the host while it runs.

## Checked

On 2026-11-05, with the `ccda-push` pipeline, before the stages (STAGES.md): a splitter that also converted.
Checked again since: see the end of this section.
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
- **Scaling (2026-11-07, KEDA):** `scale-demo 300`, three runs. KEDA scaled the split stage to its two pods and
  the convert stage to its three, within 15 to 50 seconds, and each pod did a share of the work; back to one pod
  each a few minutes after. Found on the way, and fixed: three split pods and three convert pods don't fit on the
  nodes at once (the split stage is now limited to two); copies that differ only before the root element share
  their record IDs, so their records all went to three of the six partitions (the demo now varies the content);
  the demo's counts from pods' logs missed pods scaled away (it now counts in the registries). A convert pod
  scaled away doesn't release its claims, so 2 of 300 records were converted a minute late (STAGES.md §10).
- **Running out of memory (2026-11-07, unplanned):** in the third run, the control-plane node, which holds every
  shared service and one landing pod, ran out of memory: the kernel killed SeaweedFS, Vault exceeded its 256 MiB
  and restarted sealed, and the active landing node exceeded its 1 GiB. Its pods' limits come to about 90% of
  the node, before the Kubernetes control plane's own memory. With Vault sealed, landing couldn't start and the
  split stage held its items, retrying every five minutes (its consumers left the group, since a batch retried in
  place didn't poll: fixed since, revision (bz), below). `up` unsealed Vault, and every item finished by itself: 300 converted, none
  twice. But one valid document was failed, `RESUME_NO_PROGRESS`: the attempts that failed while Vault was down
  were counted as restarts without progress, the guard meant for a document that crashes the splitter. Fixed
  since: an attempt that ends because a service is down isn't counted (STAGES.md §12, DESIGN.md §9.2), and the
  memory is sized (Memory, above). Measured before the fix: the control-plane node's pods had asked for 95% of
  it, their limits came to 158%, the third node held only a Kafka broker, and at idle the control-plane node had
  1.2 GB available. After a stop and an `up`, the registry was empty, so the standby landing pod couldn't start
  on another node.
- **Memory (2026-11-07, after sizing it; Memory, above):** `stop` and `up` resized the VMs with their data kept,
  and `scale-demo 300` ran three times while each node's available memory and the kernel's out-of-memory kills
  were sampled every 20 seconds. First run: the second convert pod waited, unschedulable, as designed rather than
  starving a node; but it could never have fitted, since the two nodes' free memory was split 469 and 981 MiB.
  KEDA's pods moved to the control-plane node. Second run: the same for a second split pod, behind two convert
  pods. Each stage's pods were then spread over the nodes, and the nodes given 256 MiB more. Third run: both
  stages at two pods, one of each per node, the work shared about evenly (149 and 151 records converted), all
  300 done; no pod waited, none was evicted or killed for memory. Lowest available memory: 1.4 GiB on a worker,
  2.8 GiB on the control-plane node, 4.9 GiB on the host. The landing pod restarted twice at startup while Vault
  was still sealed, as it should.
- **A host crash (2026-11-07), repaired by hand.** The workstation went down mid-demo. After it, the split and
  convert stages failed at startup: the catalog in Postgres named metadata files SeaweedFS had lost, its last
  seven seconds or so (`split_records` commits 02748 and 02749, `records` 00995; every other table intact,
  landing's too). Matching Postgres against the last snapshots kept showed four items affected: two whose split
  records were lost (split, by Postgres) and two whose converted records were lost (stored and sent, by
  Postgres); their documents were all in landing. Repaired, with both stages paused (KEDA's
  `paused-replicas: 0`) and every row changed first saved, with the SQL, to `$SDH_SANDBOX/sdh-demo/repair-2026-11-07`:
  in one transaction, each guarded on the values found, the two tables' catalog rows were pointed back at their
  last files kept, the two lost split items' `split_item` rows deleted, and the two lost records' convert rows
  deleted with their items set back to not complete. Then the stages ran: the split stage's reconciler split the
  two items again from landing (and recovered, as `split.recovered`, items the crash had landed but not
  announced), their queued `record.split` messages found the new rows, and the convert stage's reconciler
  rebuilt the two lost records (`convert.records_recovered`). After: every item split was converted (3,014),
  no claim left, no lag on the split stage's group.
- **An outage held through, keeping the consumer groups (2026-11-07, revision (bz)).** `scale-demo 150`, Vault
  sealed for 8 minutes once 51 items had landed, then unsealed. The split stage held 50 items and the convert
  stage one item's records through it, each retrying at most a minute apart and polling between tries: no
  consumer left its group, no commit failed, and no restart was counted (`item.restart_not_counted` 7). After
  it, all 110 items landed were split and converted, none twice. The landing node can't run while Vault is
  sealed: it restarted 7 times, the 40 pushes meanwhile failed at the producer, and it took pushes again about
  three minutes after the unseal, Kubernetes' restart delay. The stages' inputs use `groupProtocol=consumer`.
  Fixed since: revision (ca), below.
- **The landing node through a key service outage (2026-11-07, revision (ca)).** The same run, `scale-demo 150`
  with Vault sealed for 8 minutes once 51 items had landed: the landing node stayed up, with no restart and no
  `intake.fatal`. It answered each push while sealed unavailable (503, `push.failed`); the producer doesn't
  retry, so the other 96 pushes failed within 5 seconds and the run ended short at its 30 minutes. Every item
  that landed (54) was split and converted, none twice, 53 of them finished after the unseal; no consumer left
  its group. Then, with Vault sealed again, the landing pod was deleted: the new pod waited for the key service
  (`crypto.kms_waiting`, ten tries, the last 30 seconds apart) with no restart, holding no lease, and was ready
  39 seconds after the unseal (`crypto.kms_ready attempts=10 waited_ms=201229`, then `lease.acquired`). The
  check after it landed, split and converted all 5 samples. Last, recovery: a producer retrying each push every
  5 seconds, with Vault sealed for 3 minutes, against a running landing node. It was refused 36 times (503),
  its first push landed 2 seconds after the unseal, and all 20 landed, were split and converted, none twice; no
  restart. `push.failed` and `crypto.kms_waiting` are console log lines, so only the pod's log has them; the
  collector gets the refusals as `sdh.push.requests` with `sdh.result=unavailable` (36 here), since only audit
  events leave the host (DESIGN.md §12).
- **Resubmit and the stages' replays (2026-11-08, revisions (cb) and (cc)).** The one item the earlier outage had
  failed `RESUME_NO_PROGRESS` was landed again with `resubmit` on the landing node, in 26 seconds: a new item
  naming it in `replaces_item_id` (`item.resubmitted`), split and converted (1 record, none failed) 21 seconds
  later. The failed item stays `FAILED`; it had split no records, so nothing reached the convert stage twice. Run
  again, the command was refused, naming the new item. `replay` on the split stage sent the new item's
  `record.split` and `item.split`, and the convert stage dropped both (nothing new on `ccda-push.records`);
  `replay` on the convert stage sent its `record.stored` and `item.converted` again, with the same keys and
  partition, under `<messageId>~replay-<millis>`; landing's `replay` was announced again too. No pod restarted.
  Found on the way, and fixed: no `sdh-admin` command could start on an HTTP push node, `replay` included, since
  `WebServerSwitch` gave its context a web server (revision (cb)); and `--with-result`, with results off as here,
  sent the records before failing (it now refuses first).
- **Versions at every stage (2026-11-08, revision (cd)).** Deployed over the existing data: on load, both
  `split_records` and `records` moved from schema 0 to 1, with `stage_version` and `input_version` as optional
  columns (ids 26 and 27); nothing failed at startup. The 5 samples landed, split and converted. `replay` on the
  split stage of an item converted before the deploy: the convert stage audited `record.version_differs` (the
  message under the new build, the stored row under the old) and `record.duplicate`, and processed nothing again;
  the item's counts and completion stayed as they were. Replayed, a new item was dropped quietly as the worker's
  own. No pod restarted. Not tried here: a split item held under another version (none was part way through at the
  deploy; `IngestPipelineTest` covers it), or a duplicate across two convert pods under the same version.
- **The fail store and item progress (2026-11-08, revision (ce)).** Deployed over the existing data: both stages
  made `stage_failure` in their registry schemas, and the convert stage's `stage_item` gained `upstream_open` and
  `progress_key`; nothing failed at startup. The 5 samples landed, split and converted as before. A synthetic C-CDA
  of 176 bytes, broken off part way (no data in it): the split stage failed it as a whole
  (`PARSE_UNBALANCED_ELEMENT`), recorded it once in its fail store (seq -1, `FAILED`, with its version, audited
  `failure.failed`), and sent `item.progress` (0 passed, 1 open) instead of `item.split`; the convert stage took that
  as a total still open upstream (`upstream_open`), sent its own `item.progress` (`UPSTREAM_OPEN`), recorded nothing
  in its own fail store, and didn't complete the item. `replay` on the split stage sent the `item.progress` again
  (0 `record.split`) and wrote nothing; the convert stage dropped it as a repeat. `sdh_stage_failures_open` read 1
  for split and 0 for convert, and `sdh_stage_failures_total` 1. No pod restarted. Not tried here: a record that
  fails conversion, a piece failed inside an item that otherwise splits (neither is easy to make from C-CDA; both
  are tested), and the producer's `incomplete` result (results are off here).
- **Declining and reopening a failure (2026-11-08, revision (cf)).** Deployed over the existing data: both stages
  made `stage_failure_event`. `failures` on the split stage listed the malformed item's failure (seq -1, `FAILED`,
  `PARSE_UNBALANCED_ELEMENT`, offset, version) and nothing of its content. `decline` with no reason on standard
  input was refused (exit 2) and changed nothing. With a synthetic reason through `kubectl exec -i`, it moved the
  failure to `DECLINED` (audited `failure.declined`, without the reason), kept the reason encrypted in
  `stage_failure_event` (98 bytes, under the records KEK), and sent the item's `item.split` again, alone; the
  convert stage completed the item (`stage.item_completed result=COMPLETED`, 0 records). `reopen` moved it back to
  `FAILED` and sent `item.progress`; the convert stage left the item complete, as documented. Declined again, and
  `failures --item-id` showed the three changes, by whom and when, without the reasons. `failures` on the convert
  stage: 0 open. Found here: `admin.decline` was audited with the item checkpoint's state in `sdh.state`; fixed
  and tested in `AdminCommandsTest`, not redeployed.
- **Exporting and correcting a record that failed conversion (2026-11-08, revision (cg)).** Deployed over the
  existing data: the convert stage made `stage_correction`. A synthetic C-CDA of 151 bytes with an undeclared entity
  (`&nbsp;`; no data in it) split cleanly and failed conversion (`CONVERT_XML_MALFORMED`, `FAILED` in the convert
  stage's fail store, item incomplete). `export` with an EC P-256 public key on standard input printed one
  `sdh-export: ` line, a JWE (`ECDH-ES+A256KW`, `A256GCM`) that opened locally with the private key and held the
  failed record (150 bytes); audited `admin.export` with the key's SHA-256. The entity was replaced by `&#160;` and
  submitted with `correct`: the failure moved to `CORRECTION_PENDING` (`failure.correction_submitted`), and within a
  renewal interval a worker converted it, the correction `PASSED` and the failure `CORRECTED` with `resolved_by` and
  `resolved_version` (`failure.corrected`), and the item completed (`stage.item_completed result=COMPLETED`,
  `failed_count=0`). `failures --item-id` showed both changes and the correction, without content. No pod
  restarted. The `admin.decline` audit fix of (cf) was deployed with it.
- **Exporting and correcting an item the split stage failed (2026-11-09, revision (ch)).** Deployed over the
  existing data: the split stage made `stage_correction` (with `notified_at`). A synthetic C-CDA of 162 bytes
  broken off part way (no data in it) failed at the split stage as a whole (seq -1, `PARSE_UNBALANCED_ELEMENT`, 0
  records). `export --seq -1` with an EC P-256 public key printed a JWE (`ECDH-ES+A256KW`, `A256GCM`) that opened
  locally to the item as pushed (same SHA-256). The completed document (190 bytes) submitted with `correct`
  (`failure.correction_submitted`) was split by the pod within a renewal interval: the correction `PASSED`, the
  failure `CORRECTED` with `resolved_by` and `resolved_version` (`failure.corrected`), the item's end sent again
  (`stage.item_completed result=COMPLETED`) and noted (`notified_at`); the convert stage converted the one record
  and completed the item (`count=1`, `failed_count=0`), with nothing in its fail store. No pod restarted. Found
  here: the split stage's `stage.item_completed` after the correction said `count=0`, leaving out the records the
  correction added; fixed and tested in `ItemProcessorTest`, not redeployed. Not tried here: a piece's correction
  (no C-CDA makes one; `ItemProcessorTest` and `AdminCommandsTest` cover both kinds).
- **Retry, release and completing again (2026-11-09, revision (ci)).** Deployed over the existing data: the split
  stage made `split_release` and `split_item.completion` and `completed`, the convert stage `stage_item.completion`
  and `upstream_completion`, both `stage_correction.kind`; nothing failed at startup. A synthetic C-CDA of 188 bytes
  with an undeclared entity (no data in it) failed conversion; `retry` submitted it unchanged (187 bytes, kept
  encrypted), the worker failed it again and the failure went back to `FAILED` (`failure.retry_failed`), listed as
  `retry 1 FAILED ... CONVERT_XML_MALFORMED`. Declined, the item completed as `ic-convert-<id>`; reopened, the stage
  audited `stage.item_reopened reason=failure_reopened` and sent `item.progress` ending `~c1`; declined again, it
  completed as `ic-convert-<id>~c2`, once. A synthetic C-CDA of 172 bytes broken off part way failed at the split
  stage as a whole; `retry --seq -1` failed the same way and left it `FAILED`. Declined, reopened and declined
  there: the split stage sent `is-<id>`, `ip-split-...~c1` and `is-<id>~c2` (each under a `~replay-` broker ID);
  the convert stage completed the item, reopened it from upstream (`reason=upstream`), and completed it again as
  `~c2`. `release` on an item not `HELD` was refused (exit 1, "not HELD") and with neither or both flags (exit 2),
  and wrote no request; `status --item` shows `releases: none`. No item could be held here (one C-CDA splits faster
  than a deploy interrupts it); `IngestPipelineTest` covers resume and close. No pod restarted. Found here: at
  startup the landing node announced its last item again, as designed; that item, completed before this build,
  had no completion number at the convert stage (0), so the split stage's repeated `item.split` was taken as newer
  and the item was audited `stage.item_reopened` and completed again under its old ID (dropped downstream as a
  repeat). Harmless here, but it shows the gap in STAGES.md §15.6f's known limit: 3,547 convert rows and every split
  row from before (ci) carry completion 0, so reopening a failure of one would reuse message IDs already sent.
  Fixed in revision (cj): each start numbers such rows 1 (STAGES.md §15.6f).
- 2026-11-09, revision (cj): the first start numbered the earlier rows, auditing
  `split.completions_numbered count=3547`, `stage.completions_numbered` and `stage.totals_numbered` (each
  `count=3547`); no row at either stage is left at completion 0, and no item was audited `stage.item_reopened`
  at startup. A second start numbered nothing (no audit). `check` passes (5 items split and converted).
- 2026-11-09, revision (ck), items with no records (STAGES.md §16): pushed to `ccda-push` an empty item, a blank
  one (spaces, CR LF, a tab) and one line of plain text. The empty and the blank items each failed as a whole
  (`item.failing` and `failure.failed seq=-1`, `SPLIT_EMPTY`), with nothing sent to the convert stage. The text was
  split as one record of the whole item (`batch.committed count=1`, `item.split count=1`) and failed at the convert
  stage (`record.failed`, `CONVERT_XML_MALFORMED`), as designed. `decline --seq -1` of the empty item (a synthetic
  reason) sent its `item.split` again, and the convert stage completed it with 0 records. `correct --seq -1` of the
  blank item with a synthetic C-CDA document (`google-synthea-sample1.xml`) was `CORRECTED`, and both stages
  completed it with 1 record. The C-CDA splitter's version stays 1 (`ccda/1` in the version), since its cut is
  unchanged. `check` passes (5 items split and converted). No pod restarted.
- **Corrections and retries as revisions (2026-11-09, revision (cn); STAGES.md §15.8.5).** Deployed over the
  existing data: at startup each stage moved its fail store's rows out of the old states once
  (`failure.states_moved`, split `count=2`, convert `count=1`): the three `CORRECTED` failures became `SUPERSEDED`
  (`REVISION`), keeping `superseded_by` from `resolved_by`, each with an event; the `DECLINED` and `FAILED` ones
  were left as they were, and earlier events keep the state names they were written with. At the convert stage, a
  synthetic C-CDA of 128 bytes with an undeclared entity failed (`g1 r1 FAILED`). Corrected with another undeclared
  entity, r1 became `SUPERSEDED` by `g1 r1.2`, and r1.2 failed as a failure of its own, `FAILED`
  (`failure.correction_failed lineage=g1 r1.2`, then `item.progress`). Corrected again with `&#160;`, r1.2 became
  `SUPERSEDED` by `g1 r1.3`, which passed: stored as `<record>~r1.3`, sent as `record.stored` under a message ID
  ending `~r1.3`, and the item completed (`failed_count=0`). A second such record, retried, failed as `g1 r1.2`
  (`failure.retry_failed`); declined, the item completed (`failed_count=1`). At the split stage, a synthetic C-CDA
  of 114 bytes broken off part way, corrected as a whole, became `SUPERSEDED` by `g1 r2`, and both stages completed
  it with 1 record (its record still `g1 r1` at the convert stage: whole-item corrections keep the old rules until
  step 4). `check` passes (5 items split and converted). No pod restarted. The producer's results are off here, so
  the rule that operator actions send it nothing (§15.8.7) wasn't seen. Seen here, not changed: `failures
  --item-id` prints the seq's events and corrections under each of its revisions; a failure superseded by a
  revision says `changed ... by <operator>` where its event says `by pipeline`; `superseded_by` holds a lineage
  (`g1 r1.3`) for new rows and a record ID for rows moved from `CORRECTED`; and the split stage's whole-item
  correction is noted as `g1 r2`, a revision no record carries.
