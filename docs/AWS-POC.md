# AWS proof of concept: configuration, sizing and lifecycle (2026-11-12)

**Status:**
- IAM is applied (`deploy/aws/iam`).
- The Terraform state bucket and the persistent layer were created on 2026-11-12 (81 resources; a re-plan shows
  no drift).
- A trial `up`/`down` of the cluster alone passed on 2026-11-12:
  - 2 base nodes; Postgres and Kafka on the kept volumes; KEDA; the autoscaler, with Pod Identity credentials;
    the topics (4/6/8/1/1/1 partitions) and database roles;
  - then a drain, a clean stop, snapshots, the cluster destroyed, the schedule removed, and nothing left over.
  - The guard ran once with no cluster and succeeded. On 2026-11-12 it also deleted a live cluster (fired by
    mistake, see the load test below): node groups and cluster gone in 11 minutes; `poc.sh down` afterwards found
    no cluster, snapshotted both volumes and left nothing running.
  - Postgres holds a table `sdh_poc_marker` with one row: the next `up` should find it, which proves the data
    survives a new cluster.
- The pipeline ran on AWS on 2026-11-12 (`poc.sh up --hours 2`, then `down`):
  - `apply.sh` deployed the four stages; every stage got its credentials from Pod Identity, with no API token
    mounted, and used KMS and S3 with no errors;
  - the two landing pods ran on different nodes, exactly one holding the lease;
  - the smoke test, `deploy/aws/k8s/pipeline/check.sh`, put the 12 sample documents in the intake bucket: all
    12 were landed (and deleted from the intake), split, converted and transformed (12 records, 7 mapped to a
    bundle, 5 kept as they came), and no log held document content or an object key;
  - KEDA scaled split to 2, convert to 2 and transform to 4 on the backlog, and back;
  - the marker table `sdh_poc_marker` was found on the new cluster, so the data survives one.
  - The Spot burst nodes couldn't be launched (`UnfulfillableCapacity`), so 3 transform pods stayed Pending
    until the backlog cleared. Not the quota, as first thought: see the load test.
- A load test ran on 2026-11-12 (`deploy/aws/k8s/pipeline/load-test.sh`, 2,450 objects, 312 MB: 80 copies of
  each of mapping's 30 test documents and 10 of each of the file connector's 5 error inputs). It was stopped
  part-way, as below. What it showed:
  - **The split stage can't keep up, and slows as bronze grows.** It reads each landed item with a scan of
    `ccda_push_landing.landed_items`, which is partitioned by day, so a lookup by item ID reads every manifest of
    the day, and every commit adds one (corrected 2026-11-12: it isn't unpartitioned): about 800 manifests
    and 10 s a lookup by the time 800 items had landed, rising with each item. Split (and so convert and
    transform) went from about 6 items a minute to about 1. `split_records` has the same shape (85, then 324
    manifests; 1–4 s a lookup), and so does any reader of it: the gold check read only about 6 bundles a minute.
    From 22:45 UTC throughput rose about tenfold (75–90 items per 5 minutes) for reasons not found: the deployer
    can't read bronze's metadata, by design. **Needs a fix in the connectors** before a larger load: lookups by
    key that prune manifests (partitioning or sorting the tables by item ID) and regular manifest rewriting.
  - Landing kept up: all 2,450 objects were in the intake 22 s after the server-side copy began, and landed at
    about 80 a minute. When the stages were paused, about 475 items had been split, 470 converted and 225
    transformed (245 records `TRANSFORM_CHECK_FAILED`, the set's expected failures), about 715 bundles were in
    gold, and about 2,000 landed items were still queued for the split stage (kept; they resume next session).
    The split and convert error inputs were all still queued, so their handling wasn't seen at volume.
  - **Landing failover:** the lease holder was deleted with 1,576 objects still in the intake. The lease moved in
    84 s, never two landing at once, but to the pod that replaced it, not to the standby that had waited 19
    minutes. The new holder's reconcile then scanned `landed_items` (12–18 s a scan) before landing resumed.
  - **Unopenable messages** (junk put on `.split` and `.records`) are rejected and dropped by the convert and
    transform stages (`stage.message_rejected reason=MALFORMED`), not dead-lettered: a dead-letter queue is for
    messages that open. Both DLQs stayed empty.
  - **Spot:** one burst node launched and got a Spot interruption notice 80 s later; every launch after that was
    `UnfulfillableCapacity`. So the quota is not the cause (the node launched): it is Spot capacity for
    `t4g`/`m7g`/`m6g.large` in us-east-2a. Burst capacity here needs more instance types or zones, or on-demand.
  - **The gold check didn't complete.** It needs about 1 CPU and 2.5 GiB, more than the base nodes had free
    while the stages ran, and it read too slowly (above) to finish before the guard fired. Locally, against
    mapping's baseline, it gave the expected results (30 of 30 documents' resource counts; validator errors
    matched for the three bundles tried).
  - **A mistake:** `poc.sh extend --hours 2` (it takes `extend 2`) scheduled the shutdown for that moment.
    Rescheduling 20 s later didn't stop the run the scheduler had already taken, and the guard deleted the
    cluster mid-check. `extend` now refuses anything but a positive number of hours.
- **Fixed after the load test, 2026-11-12, and run on AWS, 2026-11-13** (results below):
  - **Landing failover:** the 84 s wasn't the standby's fault. A listing is one Camel exchange, so the deleted
    holder couldn't stop its route within Kubernetes' default 30 s, was killed, and left its lease to run out
    (60 s TTL). The node now stops listing at the next item when it starts to stop, and the landing pods have
    `terminationGracePeriodSeconds: 90` to finish that item and release the lease (DESIGN.md revision cy). The
    standby should then take over within its 2 s poll, before the replacement pod is even scheduled (the
    anti-affinity keeps it off the node until the old pod is gone). After a crash, with no release, either pod
    may still win once the TTL passes; both are equally ready, since neither loads anything before its lease.
  - **Spot:** the burst group now has six arm64 types of 2 vCPUs and 8 GiB (`t4g`, `m6g`, `m6gd`, `m7g`, `m7gd`,
    `m8g` `.large`, all offered in us-east-2a, 2b and 2c) in all three zones. `poc.sh up --on-demand-burst` puts
    the burst nodes on demand for a performance test (about $0.07/hour each; the on-demand vCPU quota must cover
    8, iam/spot-quota.sh).
  - **The gold check** tolerates the burst taint, so with burst nodes it gets the room it lacked. Its reading is
    as slow as split's (the same lookups), so until the performance work it is run after a small load.
  - **`poc.sh reset`** puts the pipeline back to nothing: the database `sdh`, each pipeline's topics and every
    consumer group, the bronze, silver and gold buckets and the intake prefixes are deleted, then the pipeline is
    deployed again. The keys, the state directory and the tools bucket are kept.
- **Run on AWS, 2026-11-13** (`poc.sh reset`, then `load-test.sh 5 2`: 160 objects, 26 MB; report
  `load/load-20261113T030404Z`):
  - **Landing failover:** the holder was deleted at 30% landed; the standby took the lease **2 s** later (was 84 s).
  - **Spot:** one burst node launched on Spot and stayed; KEDA ran transform at 4 pods and split at 2.
  - **Errors as expected:** split `PARSE_UNBALANCED_ELEMENT` 2 and `PARSE_XML_UNSUPPORTED_ENCODING` 2, convert
    `CONVERT_XML_MALFORMED` 2, transform `TRANSFORM_CHECK_FAILED` 84 (the US Core set's); junk rejected 3 each at
    convert and transform; both DLQs empty.
  - **Gold is readable and right:** the gold check read 238 messages and 78 bundles, 76 of them matched to
    mapping's test documents; every per-document check passed, and the HL7 validator found nothing beyond
    mapping's baselines in the 32 bundles validated.
  - **Throughput** (small, not a performance figure): 868 s end to end, landing 20.6 items/min, split
    11.4 items/min. Split is still the bottleneck (the lookups above).
  - **One duplicate:** 161 items landed for 160 objects, and one document has 6 bundles for 5 copies. Probably
    the item the holder was landing when it was deleted, landed again by the standby; not yet confirmed. Nothing
    was lost. To look at in the landing node's stop path.
  - **Fixed on the way:** `poc.sh reset` left the stages at 0 replicas (they leave replicas to KEDA, and their
    scaling is made last), so it now deletes the stages' workloads and `apply.sh` makes them as on a new cluster.
    The gold check named no bundle, so its per-document checks ran on nothing: it hashed whole files, but a split
    record is only the root element with the XML declaration before it. It now hashes the root element on both
    sides (GoldCheck in the transformer, and gold-check.sh).
  - The session started on images built from the committed connectors plus these fixes. An `up` on a kept
    database whose last load left items part way stays held at split's drain gate (`stage.version_blocked`) until
    those items drain, or `reset`.
- The prices are approximate us-east-2 on-demand list prices, not checked against the Pricing API.

**Decided with the user, 2026-11-12:**

- Synthetic data only (so §9 does not apply).
- Kafka as one broker pod in the cluster.
- The deployer gets a scoped policy, and Claude writes and applies the Terraform.
- arm64 Graviton nodes.
- **The PoC runs only while in use, at most 8 hours a day**, except for a resilience or performance test. When
  not in use it is shut down, at the lowest cost that keeps its data (§3).
- Postgres runs as a pod on a kept EBS volume, not on RDS. A stopped RDS instance restarts itself after 7 days,
  and it is one more thing to start and stop.

## 1. Goal and constraints

- Run the whole C-CDA → FHIR pipeline on AWS, with every part exercised: landing, split, convert and
  transform; Kafka, Postgres leases and registries, Iceberg on S3, AWS KMS, KEDA and failover.
- Cost money only while it runs, and as little as possible then.
- Expect about 2 GB of source C-CDA (§8 on going bigger).
- Account 493469777520, region us-east-2, identity `claude-poc-deployer` (scoped policies in `deploy/aws/iam`).
- The account's default VPC 172.31.0.0/16, with public subnets in us-east-2a, 2b and 2c.

## 2. Architecture

```
 S3 intake bucket ──► landing (1 active + 1 warm standby, Postgres lease)
                          │  bronze: landed_items, landing_ledger
                          ▼  Kafka <p>.landed  (4 partitions)
                       split (KEDA 1..2)
                          │  silver: split_records
                          ▼  Kafka <p>.split   (6 partitions)
                       convert (KEDA 1..3)
                          │  silver: records
                          ▼  Kafka <p>.records (8 partitions)
                       transform (KEDA 1..4, concurrency 2)
                          │  gold: sdh_fhir.fhir
                          ▼  Kafka <p>.fhir (1)   + DLQs, results
 Iceberg catalog: JDBC on the Postgres pod  ·  data: S3 (SSE-KMS)  ·  keys: AWS KMS
```

## 3. Two layers, so it can be shut down

| Layer | What | Lifetime | Cost |
|---|---|---|---|
| **Persistent** (`deploy/aws/terraform/persistent`) | KMS keys, S3 buckets, ECR, the Postgres and Kafka EBS volumes and their snapshots, IAM roles, the S3 gateway endpoint, the auto-down state machine | Created once by `poc.sh bootstrap`, never destroyed (keys, buckets and volumes have `prevent_destroy`) | **About $11/month** (§3.2) |
| **Compute** (`deploy/aws/terraform/compute`, plus the Kubernetes objects `poc.sh` applies) | The EKS cluster, its node groups, add-ons and Pod Identity associations, and everything running in it | Created by `poc.sh up`, destroyed by `poc.sh down` or by the guard | **About $0.25/hour** while it exists, $0 after |

Why it works this way:
- The EKS control plane costs $0.10/hour even with no nodes, so scaling the nodes to zero isn't a shutdown.
  Destroying the cluster is.
- Postgres (catalog, leases, registries, fail store) and Kafka keep their data on two 10 GiB EBS volumes from the
  persistent layer. Each new cluster mounts them as static PersistentVolumes, so the Iceberg tables, the leases
  and the consumer offsets carry on across sessions.

### 3.1 The session lifecycle (`deploy/aws/poc.sh`)

1. **`up`:**
   - first schedules the automatic shutdown on AWS;
   - then creates the cluster (about 15 minutes);
   - starts Postgres and Kafka on the kept volumes;
   - installs KEDA and the Cluster Autoscaler;
   - makes the topics and database roles;
   - deploys the pipeline.
2. **`extend N`:** moves the shutdown to N hours from now, within the day's remaining hours.
3. **`down`:**
   - pauses intake and waits for every consumer group's lag to reach 0 (up to 20 minutes);
   - scales the stages to 0 (KEDA paused at 0);
   - stops Kafka and Postgres cleanly;
   - snapshots both volumes and keeps the last 3;
   - destroys the cluster and removes the schedule;
   - warns about anything the cluster left behind.
4. **The guard:**
   - An EventBridge Scheduler one-off schedule, `sdh-poc-auto-down`, starts the Step Functions state machine
     `sdh-poc-auto-down` at the session's end.
   - It deletes the node groups and then the cluster.
   - So a session ends on time even if the laptop is off or asleep, or `up` failed half way.
   - Postgres and Kafka then stop as in a crash, which the pipeline is built to recover from.
   - It never touches the persistent layer.
5. **The 8-hour day:**
   - `poc.sh` keeps a session log in its state directory and counts every session in the local calendar day.
   - `up` refuses once 8 hours are used, and otherwise sets the deadline to what is left.
   - `--long-test "reason" --hours N` (N ≤ 24) lifts the limit for one session, for resilience or performance
     tests; the reason is logged.
   - A session the guard ended counts up to its planned end.

### 3.2 Costs

| | Item | Approx. cost |
|---|---|---|
| Per running hour | EKS control plane | $0.100 |
| | 2 × `t4g.large` on demand | $0.134 |
| | 2 public IPv4 addresses | $0.010 |
| | Node root volumes | $0.005 |
| | **Total** | **≈ $0.25/hour, ≈ $2 per 8-hour session** |
| | Spot burst nodes, only when KEDA scales out | ≈ $0.02–0.03/hour each |
| Per month, always | 8 KMS keys (3 KEKs, 4 signing keys, 1 infra key) | $8 |
| | 2 × 10 GiB gp3 state volumes | $1.60 |
| | Snapshots (incremental, last 3 each) | < $0.50 |
| | S3, ECR and Terraform state at this volume | < $1 |
| | **Total** | **≈ $11/month** |

| Usage | Monthly total |
|---|---|
| 40 hours a month | ≈ $21 |
| 8 hours every working day (160 h) | ≈ $51 |
| Plus, per long test | ≈ $0.25 an hour |

The budget alarm `sdh-poc-monthly` stays as a backstop: $60/month (8 hours every weekday is about $55), with
emails at 50% and 100% of actual spend and at 100% of forecast. A cluster left running would cost about $190 a
month, so the forecast alert catches it within days. Lowered from $250 on 2026-11-12.

## 4. Choices, and why they are cheap

| Concern | Choice | Rejected, and why |
|---|---|---|
| Kubernetes | EKS 1.37, standard support only (`upgrade_policy`: a version in extended support costs $0.60/hour) | k3s on EC2: no control-plane fee, but no second node for landing's standby |
| Nodes | Managed node groups: `base-a` and `base-b`, one on-demand `t4g.large` each, plus `burst`, 0–2 Spot (six arm64 `.large` types, three zones; on demand with `--on-demand-burst`), with Cluster Autoscaler | Karpenter or Auto Mode: more to set up, or a fee on every instance |
| Postgres | `postgres:18.4` StatefulSet on a kept 10 GiB gp3 volume, snapshotted at each `down` | RDS: restarts itself after 7 days stopped, and is a second start/stop. Aurora Serverless v2: dearer while running. |
| Kafka | 1 KRaft broker (`apache/kafka:4.2.2`) on a kept 10 GiB volume, RF 1 | MSK: ≥ $100/month, or about $540/month serverless |
| Iceberg catalog | JDBC on that Postgres (tested) | Glue: not supported by the code. S3 Tables, Polaris: untested, or one more pod |
| Object store | S3: `sdh-poc-{intake,bronze,silver,gold}-493469777520`. Intake uses SSE-S3, so the deployer can upload samples; the others SSE-KMS with a bucket key. TLS only, versioning off, unfinished uploads removed after a day | — |
| Keys | AWS KMS (`wrap-provider: aws-kms`), the sandbox's Vault keys one for one; rotation off (it adds $1/month per key) | `local`: free, but wouldn't exercise KMS |
| Intake | S3 bucket intake (§5.3.17, claimed by a conditional write) | EFS, Transfer Family SFTP: cost |
| Networking | Default VPC, public subnets, inbound closed; the API endpoint open only to the operator's address, private access for the nodes; S3 gateway endpoint (free) | NAT gateway ($32/month and up) |
| Images | ECR `sdh-poc/<image>`, immutable tags, last 5 kept | — |
| Observability | No control-plane logs (they cost $0.50/GB); pod logs through `kubectl`; OTel collector later if wanted | Container Insights: billed per metric |

## 5. Per-stage configuration

The heap is `MaxRAMPercentage=50` of the limit in every stage. Memory request = limit (Guaranteed QoS).

| Pod | Replicas | CPU req | Mem req = limit | Notes |
|---|---|---|---|---|
| landing | 2 (1 active, 1 standby); never KEDA | 250m | 1 Gi | `SDH_LANDING_STANDBY=true`; required anti-affinity on hostname, so one on each base node; PDB `maxUnavailable: 1`; `Recreate` |
| split | KEDA 1–2 | 500m | 2 Gi | `<p>.landed`, 4 partitions, lag 5 |
| convert | KEDA 1–3 | 250m | 1.25 Gi | `<p>.split`, 6 partitions, lag 5, results off |
| transform | KEDA 1–4 | 500m | 1.5 Gi | `<p>.records`, 8 partitions (4 × concurrency 2), lag 5, results off (below) |
| kafka | 1 | 200m | 768 Mi | heap 384 MiB, as in the sandbox |
| postgres | 1 | 100m | 512 Mi | as in the sandbox |
| KEDA (3 pods), Cluster Autoscaler, CoreDNS, CNI, EBS CSI, Pod Identity agent | — | ~0.5 | ~1.5 Gi | — |

- The scaled stages tolerate the `sdh-poc/burst` taint and prefer the base nodes; everything else runs on the base
  nodes only.
- Producer results are off at every stage, changed from "on at transform" on 2026-11-12: with a bucket intake no
  producer listens for them, and they would need a producer key pair and a results topic. Turn them on at the
  transform stage only if a test needs them (its README).
- The landing pods have no readiness port (a bucket intake opens none), so `apply.sh` tells the active node from
  the standby by its `lease.acquired` audit event.
- Postgres and Kafka run on the base node in us-east-2a, where their volumes are.

**Totals:**
- At rest, about 9.5 GiB and 2.6 vCPU requested, which fits the two base nodes (about 14 GiB allocatable).
- At the maximum, about 18.5 GiB, which needs one or two Spot burst nodes.

## 6. Landing exclusivity on AWS

- What exists already:
  - the Postgres lease `landing/<intake>` (TTL 60 s, expiry by the DB clock);
  - the fencing epoch on the bronze tables;
  - the bucket claim.
- Together they let only one landing node land, even if Kubernetes briefly runs two.
- The standby takes over within 60 s after a crash.
- PoC tests (as long tests):
  - kill the active pod;
  - drain its node;
  - terminate its instance;
  - isolate it with a NetworkPolicy (the VPC CNI's policy agent is on).
  - Each time, check the landed items for duplicates.

## 7. IAM

- The deployer's policies and the permissions boundary are in `deploy/aws/iam`.
- The roles in `terraform/persistent/iam.tf`: cluster, node, EBS CSI, Cluster Autoscaler, one per stage, the guard
  and its scheduler.
- All are under `/sdh-poc/` with the boundary.
- The stages use EKS Pod Identity (not yet tested in this code, DESIGN.md:2634).

| Role | S3 | KMS |
|---|---|---|
| landing | intake, bronze: read and write | landed-items KEK; own signing key |
| splitter | bronze: read; silver: read and write | landed-items and records KEKs; own signing key |
| convert | silver: read and write | records KEK; own signing key |
| transform | silver: read; gold: read and write | records and fhir KEKs; own signing key |

All four may also read every signing key's public half, and use the infra key through S3 only (SSE-KMS).
Database passwords and the stages' key files are Kubernetes Secrets, made by `poc.sh` from its state directory at
each `up`, not Secrets Manager ($0.40 per secret per month).

## 8. Data volume and storage

- **Storage:**
  - At 2 GB in, bronze, silver, gold and Iceberg metadata come to an estimated ≤ 10 GB, under $0.25/month.
  - Storage costs almost nothing at this scale; the hours the compute runs are what cost money.
- **Time to run 2 GB:**
  - Split is the bottleneck: 1.25 items/s for small documents (DESIGN.md:1986-1999).
  - At about 150 KB per document, 2 GB is about 14,000 documents.
  - That takes about 3 hours on one split pod, or 1.5–2 hours on two, so it fits one 8-hour session.
- **A 10–20 GB soak run as a long test** (about $3–5 of compute):
  - It exercises compaction and snapshot expiry under load, the day-partitioned manifest reads, real-S3
    throughput (never measured, DESIGN.md:2067), and a standby taking over a large ledger.
- Include a few large documents (50–400 MB): they set split's memory and transform's `max-bytes`.

## 9. If the data were real PHI

This configuration is for synthetic data only. Real PHI would need:
- a BAA;
- private subnets with a NAT gateway or interface endpoints (about $35–100/month);
- Kafka TLS and ACLs;
- CloudTrail data events.

## 10. Order of deployment

0. **Access:** done (`deploy/aws/iam`).
   - After the 2026-11-12 lifecycle change, the admin re-runs `deploy/aws/iam/apply-admin.sh` once to load the
     new policy versions: EBS volumes and snapshots, the guard, and no more RDS.
1. **Tools on the operator's machine:** `terraform` ≥ 1.10 and `kubectl` (deploy/aws/README.md). Done (1.17.2, 1.37.2).
2. **`poc.sh bootstrap`:** the state bucket and the persistent layer. Done, 2026-11-12.
3. **Images:** `deploy/aws/build-images.sh` builds the four customer images for arm64 in sdh-it, with no
   emulation, and pushes them to ECR.
   - First built 2026-11-12 (tag `0.1.0-SNAPSHOT-20261112T1836Z`).
   - The digests are in the state directory's `images.env`.
   - Not signed yet (`scripts/sign-release.sh`).
4. **Pipeline:** `deploy/aws/k8s/pipeline/apply.sh`, which `poc.sh up` runs (written 2026-11-12, not yet run on
   AWS). It is derived from `deploy/sandbox` and the stages' README Deployments:
   - AWS KMS (`alias/sdh-poc-<p>-*`) in place of Vault, and S3 in place of SeaweedFS, through each stage's role
     (EKS Pod Identity), so no token or access key is in a secret;
   - the bucket intake, `s3://sdh-poc-intake-<account>/<p>/`, in place of HTTP push; objects are deleted once
     landed;
   - the sizes, service accounts and KEDA maxima in §5 and §7;
   - egress only to DNS, Postgres, Kafka, AWS's HTTPS endpoints and the Pod Identity agent.
   - The stages' inbox key pairs and gold's readers' key pair are made once in the state directory's `keys/`,
     beside the KMS signing keys' public halves. Run it again on its own to apply a change.
5. **Tests:**
   - a smoke set (`deploy/aws/k8s/pipeline/check.sh`; passed 2026-11-12), then 2 GB;
   - a load with each stage's error inputs (`deploy/aws/k8s/pipeline/load-test.sh`; tried 2026-11-12, stopped
     part-way: the split stage's lookups, Status), followed by the gold check (`gold-check.sh`): every bundle in
     gold read as a downstream reader would, checked against mapping's baselines, and a few of each document
     run through the HL7 FHIR validator (US Core 6.1.0), in the cluster, as the read-only role gold-reader;
   - failover (§6) and scaling (each stage to its maximum and back), as long tests;
   - the failure and correction commands (STAGES §15);
   - a drain-and-deploy;
   - the soak run.
6. **`poc.sh down`** after every session.
