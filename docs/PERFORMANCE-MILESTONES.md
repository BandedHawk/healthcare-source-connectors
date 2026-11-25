# Performance milestones

A record of the performance runs that marked real progress: a new baseline, a target met, or a change that moved
throughput, latency or cost clearly. Each entry says what was run and on what, so a later reader can tell whether
a newer result is comparable, and why it differs if it isn't.

This isn't a log of every run. The working detail (each run, what was tried, what was dropped and why) stays in
PERFORMANCE.md §10 and §13, and the design changes in DESIGN.md's revisions. An entry here points to those.

The targets (PERFORMANCE.md §1, revised 2026-11-20 with the user): **≥ 60 C-CDA documents a second sustained**
over an initial load of 15M documents, and **landed → gold p99 under 200 s** while doing so; judged on $ a million
documents and node count as well as on speed (no tuning for the maximum at any price).

## Adding an entry

Add one when a run (or a pair, at 35 and 100 a second) shows important progress. Put the newest entry first,
below this section, and give each one these parts, in this order:

1. **Heading:** the date, a few words on what changed, the runs' names and the DESIGN.md revision.
2. **Why it matters:** one or two sentences: what it shows that the last entry didn't.
3. **Code and images:** the commit (or "working tree at revision (xx)" if uncommitted), the image tag and the
   digest of each stage's image.
4. **Cluster:** region, Kubernetes version, every node group (type, on demand or Spot, count, what runs there),
   the autoscaler's cap.
5. **Stages:** per stage, pods (fixed or the KEDA range), CPU and memory requests, and the settings that tuning
   changed (concurrency, consumers, pools, batches).
6. **Shared services:** Postgres, PgBouncer and Kafka: versions, resources and the settings that matter.
7. **Security:** what's encrypted, how clients log in, the compliance profile.
8. **Load and method:** the test set, copies and objects, the feed rate, what's injected (failovers, junk
   messages, kills), what's reset before, and what's measured and how.
9. **Results:** the table, with the previous comparable runs beside it.
10. **Observations:** what limited the pace, what the shared services did, anything lost, left or restarted.
11. **Caveats:** what isn't comparable, and what carries over to production and what doesn't.

Settings that were changed only for the runs and put back after say so, so nobody reads them as the shipped
defaults. The run reports' paths go in the entry too: they're on the machine that ran them
(`~/.local/state/sdh-poc/load/`), not in the repository.

---

## 2026-11-24: nothing in plaintext, and a node for the shared services (runs P and Q, DESIGN.md revision (fk))

**Why it matters.** The first runs with the security a production deployment needs: every connection on the
network encrypted, every client logging in as itself, the stages on the hipaa profile with nothing waived. They
are the baselines for everything after; no earlier run is comparable to them (PERFORMANCE.md §10). They show TLS
everywhere costs nothing the runs can see, the 60 documents a second target met at 100 a second offered, and the
p99 target met at 35 a second, for $12 a million documents.

### Code and images

- The working tree at revision (fk), uncommitted when run, on top of commit 05e1d8f (revision (fj)); it includes
  P9's DS4P security labels and P10's send-URI secret refusal. The mapping repository at 3f09167, the
  transformer at ac4bcc2.
- Images built 2026-11-24 21:03Z, tag `0.1.0-SNAPSHOT-20261124T2103Z`:

  | Stage | Digest |
  |---|---|
  | landing | `sha256:42bbb3f10ddffb4e15a5457257a8e41f958be33e416ed40df25226e587e6de9e` |
  | split | `sha256:f1bf98ff1cc54c360b3e389d9702a341fa59089e15910ea9bcc3e4a5c5788b40` |
  | convert | `sha256:26681182fe779f7cf2dc8fe8d22d4e2e6383fd6ddac092eb4dd6417ee56b8b31` |
  | transform | `sha256:9cfbc6f520db4fbd0937f0315595e959e9a744e1a45bb3bf75c7a8e585f6b376` |

### Cluster

The AWS PoC (AWS-POC.md), EKS 1.37 in us-east-2, all nodes Graviton (arm64):

| Node group | Type | Pricing | Nodes | What runs there |
|---|---|---|---|---|
| base-a, base-b | `t4g.large` | on demand | 1 each | landing (active and standby), resting stage pods, system pods, the collector |
| duties | `t4g.large` | on demand | 1 | the stages' duties pods (reconcilers, sweeps, commit holders) |
| state | `m7g.xlarge` | on demand | 1 | Postgres, both PgBouncers and Kafka alone (tainted `sdh-poc/state`; new in (fk)) |
| burst | `m7gd.large`, `m7g.large` | Spot | 0 to 30 | the stages' scaled pods |

- Cluster Autoscaler capped at **34 nodes** for the runs (`--max-nodes-total`; 10 as shipped), and the burst
  groups at 30 (`burst_max`). P peaked at 34 nodes (30 burst: 29 `m7gd.large`, 1 `m7g.large`), Q at 30 (26 burst,
  all `m7gd.large`).
- Burst types were limited to the two Graviton3 types for the runs, so that a run's Spot mix doesn't skew it (a
  Graviton2 share does: PERFORMANCE.md §10, runs 60 to 63); the shipped list is wider.

### Stages

KEDA scales split, convert and transform on their inputs' lag; landing never scales (one active pod, one
standby). Maxima are run 49's, set for the runs and put back after (`scaling.yaml`; the shipped maxima are lower).

| Stage | Pods | CPU request | Memory | Settings |
|---|---|---|---|---|
| landing | 2 (1 active) | 750m | 1 GiB | group concurrency 32 |
| split | 1 to 6 | 1100m | 2 GiB | concurrency 8, consumers 8, registry pool 2, registry batch 64 |
| convert | 1 to 12 | 400m | 1280 MiB | |
| transform | 1 to 18 | 1600m | 3 GiB | concurrency 6, consumers 2 |
| duties (split, convert, transform) | 1 each | 250m each | 1536, 1024, 2048 MiB | on the duties node |

- Kafka topics have 48 partitions each.
- JOSE sealing through ACCP (Amazon Corretto Crypto Provider), the audit journal's linger 100 ms, keys in KMS.

### Shared services

All on the state node (about 3.6 of its 3.9 allocatable vCPU requested):

| Service | Image | Pods | CPU | Memory | Settings |
|---|---|---|---|---|---|
| Postgres | `postgres:18.4` | 1 | 1500m | 4 GiB | `max_connections` 300, `shared_buffers` 1 GB, `effective_cache_size` 2560 MB; WAL archived to S3 by a sidecar |
| PgBouncer | `edoburu/pgbouncer:v1.26.1-p0` | 2 | 500m each | 128 MiB, limit 256 MiB | transaction pooling, at most 110 server connections each (220 in all) |
| Kafka | `apache/kafka:4.2.2` | 1 (KRaft, combined) | 1 | 3 GiB | heap 1 GB |

These replaced, on a burstable base node shared with landing: Postgres 500m and 2 GiB, one PgBouncer of 100m,
Kafka 200m and 768 MiB.

### Security

- **Nothing in plaintext on the network** (AWS-POC.md §8.2). Kafka's only network listener is SASL_SSL on 9094,
  SCRAM-SHA-512, with the standard authorizer and ACLs from `deploy/reference/kafka-acls.sh`; its controller and
  admin tools use plaintext listeners on the pod's loopback only.
- A Kafka user per stage per pipeline, plus `keda` (lag only, through a TriggerAuthentication) and the gold
  check's.
- Postgres takes `hostssl` only. The stages reach PgBouncer with `verify-full`, and PgBouncer reaches Postgres
  with `verify-full`; certificates from the PoC's own CA.
- The stages run `compliance.profile: hipaa` with nothing waived.
- Left in plaintext: the stages' metrics to the collector beside them (no PHI), and the Kafka pod's loopback.

### Load and method

- The C-CDA test set: 32 documents, none of them PHI, so 700 copies are 22,400
  objects and 1,000 are 32,000. Earlier runs used 30 documents (21,000 and 30,000 objects).
- `poc.sh reset --yes` before each run: every table, topic and object from the run before removed.
- `SDH_LOAD_RATE=<rate> SDH_LOAD_GOLD_CHECK=0 load-test.sh <copies> 0`: the copies are staged in S3 first, then
  copied server side into the intake bucket in slices of 100, at the rate given.
- Pipeline `ccda-push`. Injected: a graceful landing failover at 30% of the feed, and 3 junk messages a topic.
- Measured by `measure.sh`, from the stages' registry tables: end to end is first landed to last in gold; latency
  is each document's landed → gold. Cost is the compute at on-demand prices for the nodes up, over the run
  (no storage, no S3 requests, no Spot discount).
- Reports: P `~/.local/state/sdh-poc/load/load-20261124T220549Z/report.txt`, Q
  `~/.local/state/sdh-poc/load/load-20261124T224227Z/report.txt`.

### Results

| | P: 35 a second, 22,400 | runs 44, 48 (21,000) | Q: 100 a second, 32,000 | M, O (30,000) |
|---|---|---|---|---|
| **End to end, documents a second** | **32.5** | 33.0 | **62.6** | 59.4, 57.6 |
| Landed → gold, p50 / p99 (s) | 25 / **124** | p99 131–168 | 95 / 332 | 124–138 / 250–257 |
| Landed → split ended, p99 (s) | 60 | | 111 | 103–118 |
| Split ended → convert's first record, p99 (s) | 60 | | 248 | |
| Pods at most (split / convert / transform); nodes | 6 / 12 / 18; 34 | | 6 / 12 / 18; 30 | |
| Postgres cores, avg / max (request 1.5) | 0.33 / 0.56 | | 0.48 / 1.34 | 0.4 / 1.3 (of 0.5) |
| Kafka cores, avg / max (request 1) | 0.38 / 0.71 | | 0.47 / 0.83 | |
| PgBouncers' cores, at their peak (0.5 each) | 0.15 | | 0.39 | |
| Cost, $ a million documents (on-demand prices) | 24 | | 12 | 13 |

Runs 44 and 48 and runs M and O are the last comparable ones before (fk), plaintext, with the shared services on
the base node. Against the targets: Q meets 60 a second (62.6); P meets the p99 (124 s under 200) at 35 a
second; Q's p99 (332 s) doesn't, at 100 offered.

### Observations

- **Exactly once, nothing left.** Both runs: every document in gold once, 0 left behind, no container
  restarted; landing's failover took 3 s (P) and 2 s (Q). The set's own transform check failures, expected,
  were 10,500 (P) and 15,000 (Q).
- **TLS everywhere costs nothing the runs can see:** P is within runs 44 and 48 with a lower p99, and Q is above
  M and O.
- **The shared services aren't the limit.** Postgres peaked at 1.34 cores (Q), Kafka at 0.83, the PgBouncers at
  0.39 between them; the state node at 38% (P) and 67% (Q). Postgres averaged 513 and 668 commits a second, with
  at most 57 and 77 connections.
- **Convert is the pace at 100 a second.** In Q, split ended every item as landing landed it (both done by 353
  s, as the feed finished), then records waited up to 248 s for convert at its 12 pods; converted by 504 s,
  transformed by 535 s.
- P's feed took 670 s and the run ended at 700 s: at 35 a second the pipeline keeps pace with the feed.

### Caveats

- Not comparable with any run before (fk): TLS, the logins, two PgBouncers and the state node all changed at
  once, and the test set grew from 30 to 32 documents.
- Short runs (22,400 and 32,000 documents, 9 to 12 minutes), not the 15M initial load; the sustained rate over
  hours is still to be shown.
- The stages' per-pod rates, CPU a document and $ a million documents carry over to production. Postgres,
  PgBouncer and Kafka are one node's worth, not a production cluster's: where they limit a result, it says what
  that node manages, not what the product can do.
- Cost is at on-demand prices; the burst nodes ran on Spot, so the bill was lower.
- C-CDA only; other formats are untested at scale (DEPLOYMENT.md, "Formats").
