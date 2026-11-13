# AWS proof of concept: operating it

The design is in docs/AWS-POC.md. This README covers starting and stopping the PoC.

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
| `deploy/aws/poc.sh up --on-demand-burst` | Burst nodes on demand instead of Spot, for a performance test. |
| `deploy/aws/poc.sh up --long-test "landing failover" --hours 12` | Goes beyond the 8-hour day, for resilience or performance tests only (at most 24 h per session). |

If you forget `down`, the guard on AWS deletes the cluster at the session's deadline, even with this machine off.
Run `down` afterwards anyway: it records the session's end, snapshots the volumes, and tidies the Terraform
state.

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
| `secrets/` | Database passwords and the key passphrases, made once. The kept Postgres volume has the passwords set, so without this directory nobody can log in. |
| `keys/` | The stages' inbox key pairs and gold's readers' key pair, made once by `k8s/pipeline/apply.sh`, and the KMS signing keys' public halves. What was sealed to these keys can't be opened without them. |
| `images.env`, `wstl-us-core.tar.gz` | The images' digests (`build-images.sh`) and the transform stage's US Core set. |
| `sessions` | The session log behind the 8-hour day. |
| `kubeconfig` | Access to the current cluster. |
| `downloads/` | Pinned downloads, checked against their checksums (KEDA). |

## What's never removed automatically

These stay through `down`, and cost about $11 a month in all:
- the KMS keys;
- the S3 buckets;
- the ECR images;
- the two state volumes and their last 3 snapshots;
- the IAM roles;
- the guard.

The keys, buckets and volumes have Terraform's `prevent_destroy`. Removing them is a deliberate decision: data and
keys lost there are gone for good.

## Not yet done

- The split stage's lookups in bronze (and readers' in silver) slow with every item: a fix in the connectors
  comes before a larger load (docs/AWS-POC.md, Status).
- Until the lookups are fixed, run the gold check after a small load on a reset pipeline: `poc.sh reset`, then
  `load-test.sh 5 2`. That ran on 2026-11-13: failover in 2 s, Spot burst, gold read and checked (docs/AWS-POC.md,
  Status).
- At that failover one item landed twice (161 for 160 objects); not yet explained.
