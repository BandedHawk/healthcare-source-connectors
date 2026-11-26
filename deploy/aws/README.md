# AWS proof of concept: operating it

The design is in docs/AWS-POC.md. This README covers starting and stopping the PoC.

> **Warning: losing a key loses the data, for good.** Everything the pipeline stores, every version and every
> backup, is encrypted under KEKs in your KMS, and neither the product nor the vendor keeps a copy. A KEK
> deleted, locked out by its key policy, or gone with its KMS (a closed cloud account, a lost Vault) leaves
> everything under it unreadable. Before you delete or disable a key, or move or close a store, a database, a
> KMS or a cloud account, read [docs/HIPAA-OPERATIONS.md](../../docs/HIPAA-OPERATIONS.md) §7.3.

## Tools (once)

`poc.sh` needs `terraform` 1.10 or later, `kubectl`, `aws` (already installed), `python3`, `curl`, `envsubst` and
`openssl`. On Linux Mint or Ubuntu:

```bash
wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
```
```bash
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com noble main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
```
```bash
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.37/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
```
```bash
echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.37/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list
```
```bash
sudo apt-get update && sudo apt-get install -y terraform kubectl
```

## Day to day

| Command | What it does |
|---|---|
| `deploy/aws/poc.sh up` | Starts a session that lasts what's left of today's 8 hours. The first run takes about 15 minutes. |
| `deploy/aws/poc.sh up --hours 3` | Starts a shorter session. |
| `deploy/aws/poc.sh extend 2` | Moves the automatic shutdown to 2 hours from now, within today's 8 hours. |
| `deploy/aws/poc.sh down` | Drains the pipeline, snapshots the data and destroys the cluster. **Run it when you're done.** |
| `deploy/aws/poc.sh down --no-drain` | Same, but without waiting for the pipeline to catch up. |
| `deploy/aws/poc.sh status` | Shows the cluster, its node groups, the shutdown time, today's hours and the month's cost. |
| `deploy/aws/poc.sh reset` | With a session up: deletes all pipeline data (database, topics, offsets, buckets, intake) and deploys the pipeline again on nothing. Asks first; `--yes` doesn't. |
| `deploy/aws/poc.sh harness-up` / `harness-down` | With a session up: holds the pipeline and deploys the control harness's own, `harness` (namespace `sdh-harness`), or removes it with everything it stored and starts the pipeline again. `scripts/harness.sh aws` runs both around the scenarios (docs/CONTROL-API-DESIGN.md §15.12). |
| `deploy/aws/poc.sh db-backup` | With a session up: a base backup of Postgres to the backup bucket. `up` takes one too; WAL is archived all the time (docs/AWS-POC.md §8.1). |
| `deploy/aws/poc.sh db-restore --to "2026-11-24 15:00"` | With a session up: stops every stage and takes Postgres back to that time (UTC unless a zone is given) from the backup bucket, keeping what the volume held beside it. The stages stay stopped for their checks and repairs (HIPAA-OPERATIONS.md §7). Asks first; `--yes` doesn't. |
| `deploy/aws/poc.sh db-resume` | After a restore and its repairs: starts the stages again. |
| `deploy/aws/poc.sh up --on-demand-burst` | Burst nodes on demand instead of Spot, for a performance test. |
| `deploy/aws/poc.sh up --long-test "landing failover" --hours 12` | Goes beyond the 8-hour day, for resilience or performance tests only (at most 24 h per session). |
| `deploy/aws/check-config.sh` | Checks, offline, that the deploy settings and the load test still hold the fixes found on AWS (below). Run it before `up` and after changing anything under `deploy/aws`. |

If you forget `down`, the guard on AWS deletes the cluster at the session's deadline, even with this machine off.
Run `down` afterwards anyway: it records the session's end, snapshots the volumes, and tidies the Terraform
state.

`check-config.sh` needs no AWS or cluster. It checks the settings: the stage pods' 90 s to stop, the read-only
catalogs' one connection, one burst node group a zone and the autoscaler's cap, the gold check's tolerance of a
lost node, landing's two pods unscaled, Postgres' memory and connections against KEDA's maxima (and both
PgBouncers' pools under them), the state node, and nothing in plaintext: Postgres and PgBouncer TLS only, Kafka's
plaintext listeners on its loopback, the stages on the hipaa profile, KEDA logged in. And it runs
the load test's own functions against stand-ins for `aws` and `kubectl`: stopping the log follower, its taking up
only running pods, a failed slice copy trying again only what failed, and each copy of a document differing. A
test run's raised maxima pass too, so long as the cap follows `burst_max` (32 with 30). The transformer's `GoldCheckTest` holds the gold check to the copies' marker.

`kubectl` works once a session is up:

```bash
export KUBECONFIG=~/.local/state/sdh-poc/kubeconfig
```

Synthetic documents go into the intake bucket, under the pipeline's prefix; the landing node lands each one and
deletes it:

```bash
AWS_PROFILE=claude-poc aws s3 cp <file> s3://sdh-poc-intake-493469777520/ccda-push/
```

The smoke test puts the 12 sample documents in the intake and follows them to gold:

```bash
deploy/aws/k8s/pipeline/check.sh
```

The load test pushes 2,450 objects (mapping's test documents and the file connector's error inputs), measures
throughput and each stage's failures, then runs the gold check. Its report is kept in the state directory's
`load/<run>/report.txt`. The split stage is too slow for it yet (docs/AWS-POC.md, Status):

```bash
deploy/aws/k8s/pipeline/load-test.sh [copies] [error-copies]
```

At the end it measures the run (`measure.sh`; docs/PERFORMANCE.md §10): seconds per item from landing to gold and
through each stage, where the time went inside the stages, the rate as the tables grew, snapshots per table, KMS
calls and the cost per million documents. Run it again by hand before `poc.sh down`, which loses the stages'
metrics. For the baseline (1,020 documents; up to 5.5 hours on today's code, so `up --hours 6`), without the gold
check, whose reading takes hours on a load this size:

```bash
deploy/aws/poc.sh reset
```

```bash
SDH_LOAD_FAILOVER_AT=0 SDH_LOAD_GOLD_CHECK=0 SDH_LOAD_TIMEOUT_MIN=330 deploy/aws/k8s/pipeline/load-test.sh 34 0
```

```bash
deploy/aws/k8s/pipeline/measure.sh
```

The outage test (docs/PERFORMANCE.md §3.6): the same load, with every stage, Postgres and Kafka killed at once at
40% landed and kept down 10 minutes; then every item must arrive once, with none left behind at any stage:

```bash
SDH_LOAD_FAILOVER_AT=0 SDH_LOAD_OUTAGE_AT=40 SDH_LOAD_OUTAGE_MIN=10 deploy/aws/k8s/pipeline/load-test.sh 5 2
```

Add `SDH_LOAD_OUTAGE_LOSE=landed,split,records` to also delete the messages queued during the outage, as if it had
outlasted the brokers' retention: the reconcilers must rebuild them (it waits 45 minutes more for them). For an
outage across sessions, run `poc.sh down --no-drain` part way through a load and `up` later; the stages resume.
After an outage the "junk rejected" counts may read low: they come from pod logs, and the outage kills the pods.
Run it on a freshly `reset` pipeline: on tables holding earlier runs, split and the gold check slow down
(docs/PERFORMANCE.md §2).

The backlog test (docs/PERFORMANCE.md §10): transform is held at no pods while the load lands, splits and
converts, then released at the given pod count, and the report gives how fast that many pods cleared it (records a
second, a pod's share, CPU, batch). One pod count a run, each on a freshly `reset` pipeline, gives the cleanest
figures; a list such as `1,2,4,6` steps through them in one run:

```bash
SDH_LOAD_FAILOVER_AT=0 SDH_LOAD_GOLD_CHECK=0 SDH_LOAD_BACKLOG=4 deploy/aws/k8s/pipeline/load-test.sh 240 0
```

A record's Kafka key is the hash of its content, so the test makes each copy of a document distinct with an element
the mapping doesn't read (load-inputs.py), and the copies spread over the partitions as real documents do. Runs
before 2026-11-15 marked copies with a comment, which the convert stage drops: their transform pods' shares were
uneven, and the busiest pod's rate is the one to read in them.

The gold check alone (needs about 1 CPU and 2.5 GiB free, a burst node or the stages paused):

```bash
deploy/aws/k8s/pipeline/gold-check.sh [bundles-validated-per-document]
```

To move the automatic shutdown, a number of hours from now (not `--hours`):

```bash
deploy/aws/poc.sh extend 2
```

To apply a change to the stages while a session is up, without a new `up`:

```bash
deploy/aws/k8s/pipeline/apply.sh
```

## Once, at the start

1. The admin re-runs `deploy/aws/iam/apply-admin.sh` to load the current policy versions (see iam/README.md).
2. Run `deploy/aws/poc.sh bootstrap`. It creates the Terraform state bucket and the persistent layer: keys,
   buckets, volumes, roles, ECR and the guard. It shows the plan and asks before applying.

## The state directory

Everything below lives in `~/.local/state/sdh-poc` (or `$SDH_POC_STATE`). **Never delete it.** Back it up
somewhere safe.

| Item | What it holds |
|---|---|
| `secrets/` | Database passwords, Kafka users' passwords (`kafka-<user>`) and the key passphrases, made once. The kept Postgres volume has the passwords set, so without this directory nobody can log in. |
| `keys/` | The stages' inbox key pairs and gold's readers' key pair, made once by `k8s/pipeline/apply.sh`, and the KMS signing keys' public halves. What was sealed to these keys can't be opened without them. |
| `images.env`, `wstl-us-core.tar.gz`, `wstl-us-core.tar.gz.digest` | The images' digests (`build-images.sh`), the transform stage's US Core set, and the set's digest (`set-digest`), which names its ConfigMap and pins the stage. |
| `sessions` | The session log behind the 8-hour day. |
| `kubeconfig` | Access to the current cluster. |
| `pki/` | The PoC's CA, and the certificates it issued to Postgres, PgBouncer and Kafka (docs/AWS-POC.md §8.2). Every pipeline trusts `pki/ca.crt`. |
| `downloads/` | Pinned downloads, checked against their checksums (KEDA). |

## What's never removed automatically

These stay through `down`, and cost about $18 a month in all (the control harness's keys $7 of it):
- the KMS keys;
- the S3 buckets;
- the ECR images;
- the two state volumes and their last 3 snapshots;
- the IAM roles;
- the guard.

The keys, buckets and volumes have Terraform's `prevent_destroy`. Removing them is a deliberate decision: data and
keys lost there are gone for good.

## Not yet done

- Backups (docs/AWS-POC.md §8.1) are applied since 2026-11-24: bucket versioning and its lifecycle, the backup
  bucket, the role `sdh-poc-postgres-backup`, `s3:GetBucketVersioning` for the stages; WAL archived and a base
  backup made at `up`. A restore (`db-restore`) hasn't been tried on AWS yet.
- Nothing in plaintext on the network, and the state node (DESIGN.md revision (fk)), ran on AWS on 2026-11-24
  (runs P and Q, docs/PERFORMANCE.md §10), as did the persistent layer's backups part (applied that day). The
  harness pipeline from `deploy/reference/` hasn't run yet (HIPAA.md §11.4, A1).
- The split stage's lookups in bronze (and readers' in silver) slow with every item: a fix in the connectors
  comes before a larger load (docs/AWS-POC.md, Status).
- Until the lookups are fixed, run the gold check after a small load on a reset pipeline: `poc.sh reset`, then
  `load-test.sh 5 2`. That ran on 2026-11-13: failover in 2 s, Spot burst, gold read and checked (docs/AWS-POC.md,
  Status).
