# Pipeline performance: design

Status: **agreed 2026-11-13** (§12). Nothing here is built yet. As each step is built, it goes into DESIGN.md and
STAGES.md as a revision; this file is removed once all are.

## 1. Targets

| | Target | Why |
|---|---|---|
| Initial load | **15M C-CDAs in under 48 hours** (≥ 100 documents/s sustained); headroom to 250/s (17 h) by adding pods and intakes | the current system takes over 10 days (≈ 17/s) |
| Latency, landed → gold | **p50 under 5 s, p99 under 20 s** per document, while sustaining the load above | the user's figure is 20 s per document |
| Stores | S3 and SeaweedFS measured; ADLS Gen2 and GCS equal by construction (§4) | customers choose the store |
| Databases | Postgres measured; SQL that runs unchanged on SQL Server, Oracle and DB2 (§8) | customer policy |
| Documents | typical ≤ 500 KB; large ones (base64 PDFs, images) of 100 MB and more stream, never held whole in memory (§9) | |

100 documents/s means about 660 messages/s and, per stage, 100 items/s. A per-document latency of 20 s
leaves each of the four stages about 5 s, of which waiting for a batch to fill may take at most 1 s (§3.1).

## 2. What limits it now

Measured on AWS (docs/AWS-POC.md, Status) and locally on SeaweedFS (DESIGN.md, 1.25 items/s): the split stage
slows with every item, from about 75 items/minute to a few. The code shows why, and shows three more limits
that would stop us next. **None of them is specific to S3**: S3's higher latency per request only makes them
show sooner.

### 2.1 Point lookups plan the whole table (the split slowdown)

Every stage finds its input by asking Iceberg for rows: `IcebergGenerics.read(table).where(item_id = …)`.
Iceberg answers that by reading the manifest list and then **every manifest whose partition might match**.

- The landing node commits once per item (`newFastAppend`, one new manifest each), and again to the ledger.
  After 15M items, `landed_items` has 15M manifests until maintenance rewrites them.
- Split's lookup of an item's summary row (`LandedItems.summary`) filters on `item_id` and `chunk_index`
  only, not the day, so it reads **every manifest of every day**. Split does it three times per item:
  `ItemSplitter.split`, `LandedItemSource.open`, and the chunk reads.
- Convert and transform (`RecordInputStream`) filter on the day as well, so they read every manifest of that
  day: still O(items landed today).
- Each lookup starts with `table.refresh()`: a read of the table's current `metadata.json` (§2.2).

The message already says where the record is (`StorageRef`: table, day, snapshot), but not which file.
That's the gap: **the writer knows the exact file and row group, and throws that away**.

### 2.2 Every commit rewrites a metadata file that grows with every commit

An Iceberg commit writes a new `metadata.json` listing every snapshot still kept. Snapshots are kept 7 days
(`MaintenanceSettings`). At one commit per item, a day of the initial load is millions of snapshots, so each
commit and each refresh moves a metadata file of hundreds of MB. The per-item commit is the real problem;
retention only sets how bad it gets.

### 2.3 Every message costs a KMS call

`KmsJwsSigner` signs each message with a KMS `Sign` call: 5–15 ms of network per message, and **a quota**.
AWS KMS allows about 300 ECC requests/s per account and region (shared by all stages); Azure Key Vault and
Cloud KMS have similar limits on asymmetric operations. At 6.6 messages per document that caps the whole
pipeline at about **45 documents/s**, however many pods run.

### 2.4 Serialisation where none is needed

- The landing node does one item at a time, end to end: list, GET, encrypt, commit, commit the ledger,
  publish, DELETE. The fixed cost per item is about 100 ms of commits alone on S3, so about 10 items/s: **17
  days for 15M** before any other stage is considered.
- Each split pod splits one item at a time (`ItemSplitter`'s pod-wide lock), though items are leased one by
  one already.
- Topic partitions cap the pods: `.landed` 4, `.split` 6, `.records` 8, and `.fhir` 1.

## 3. The design

Four changes take the storage, KMS and serialisation costs off the per-document path. They keep what the
design guarantees now: exactly-once results, at-rest encryption with per-batch keys, signed and sealed
messages, single-active landing per intake, and no lost or replaced data.

### 3.1 Group commit, in every stage

Each writer collects what it has made into a **commit batch** and commits it in one Iceberg commit, then
publishes the batch's messages. A batch closes at whichever comes first:

- `commit.max-wait` (default **1 s**: the latency it adds);
- `commit.max-items` (default 500);
- `commit.max-bytes` (default 64 MiB, so a data file has a sensible size).

At 100 items/s that is about one commit per second per writer instead of 100, so ~100× fewer manifests and
snapshots. What that changes:

- **Landing:** items are read, encrypted and written concurrently (§3.4), then committed together with their
  ledger rows and the batch's checkpoint, and announced. **Built (revision (de)) without the concurrent reads,
  which are step 4:** small items listed from a source (up to `sdh.storage.group.max-item-bytes`, 8 MB) are
  read one after another into one data file and committed together; larger ones, pushes and message batches
  are landed alone. The intake objects are deleted after the commit, in
  bulk (S3 and GCS batch deletes; ADLS one per object, concurrently). A crash before the commit lands the batch
  again from the same intake objects, as today for one item; the ledger still stops a file being landed twice.
  The batch's one data key wraps once per batch, as now.
- **The ledger, once per batch.** The JDBC catalog commits one table at a time, so the ledger can't share
  the items' commit. **As built (DESIGN.md revision (de)) it is committed after the items, as it was for one
  item:** the items' commit records the whole batch beside the checkpoint, and recovery writes the ledger rows
  of any of its items without one, and announces them all again. Committing the ledger first, as first
  proposed here, would make recovery land items again from the source; this way it needs only the checkpoint.
- **Split, convert, transform:** the stage already writes one data file per poll; the change is to let a
  poll's worth of input fill the batch, and to commit across items. **Built for split (revision (de)):** the
  items split at once on a pod append their files in one commit (`GroupAppender`), taking whatever is waiting
  when the previous commit ends, so it adds no wait when idle; convert and transform already commit a poll's
  records together. Item completion (the registry in the
  database) is updated for the whole batch in one statement (§8).
- **Exactly once** is kept the way it is now: nothing is published before its commit, the checkpoint or
  registry rows commit with it, and a retry first checks whether an earlier attempt committed.

### 3.2 Messages say exactly where the data is

`StorageRef` gains, for the data the message points to:

| Field | Example | |
|---|---|---|
| `file` | `s3://…/silver/records/data/…/00017-….parquet` | the data file, as Iceberg's `DataFile.location()` |
| `rows` | `[1204, 1206)` | the record's rows' positions in the file |
| `fileSize` | 41 943 040 | so the reader can fetch the footer without a HEAD |

A reader opens that file with the table's `FileIO`, reads the footer and the ciphertext column's offset index,
and then **only the pages holding those rows**: a few ranged GETs whatever the table's size, about the
record's own size plus one page. No `refresh()`, no manifest planning. The footer and offset index are cached
per file, since a batch's records share them.

Row groups are 8 MiB (`ROW_GROUP_BYTES`), so reading a whole row group would fetch up to 8 MiB for a 50 KB
record; reading by page is what keeps a read near the record's size. Iceberg's generic reader doesn't read
by page, so this reader is parquet-java's, with row ranges, under the table's `FileIO`: still the same on
every store. Pages are 1 MiB by default; step 1 measures whether a smaller page (256 KiB) is worth its cost
to compression.

- The day and snapshot stay in the message, so a reader can still find the record by scan when `file` is
  missing. That happens only when the data file is gone: compaction rewrote it and the old file has since
  expired (days later, §3.3). The fallback is a planned scan pruned as §3.3 describes, so it's slow but
  bounded.
- Readers check what they read as now: the AAD binds the item ID, seq and chunk to the ciphertext, so a
  wrong `file` or row group can only fail to decrypt, never return the wrong record.
- The landing node's `.landed` message gets the same, for the item's chunks, and the split stage reads the
  summary row from it, not from a lookup. That removes all three of split's lookups.
- Gold readers (and `GoldCheck`) read bundles the same way.

### 3.3 Tables that stay small to plan

With the hot path no longer planning, planning is only for the fallback, the reconcilers, maintenance and
analytic readers. These keep it bounded:

- **Snapshot retention** by count and age in the hot tables: keep **24 hours or 2,000 snapshots**, whichever
  keeps more. With group commit that's about a day of history. A message's snapshot is then only a hint; the
  `file` is what's read.
- **Manifest rewrite** every few minutes (`RewriteManifests`), clustered by partition (the day), so a lookup
  by item opens a few manifests per day, not one per commit. Built: once a table has 48 small manifests.
- ~~**Partition by day and `bucket(16, item_id)`** for new tables, so the fallback prunes by item too.~~
  **Dropped 2026-11-13 (DESIGN.md revision (dc)):** the batch writers put each commit's rows in one file
  under one partition key, so an item bucket would mean a file per bucket (up to 16 per commit: 16 times the
  PUTs and small files) or rows filed under the wrong bucket. With reads by location the hot path doesn't
  plan, so it would only speed the fallback and the reconcilers. Manifests are clustered by day.
- `write.object-storage.enabled=true` for new tables, so data files' paths are spread by hash. That spreads
  requests across S3 and GCS partitions, and keeps ADLS directories small.

### 3.4 Concurrency where it's free

- **Landing stays single-active per intake.** Within the active node, a bounded pool (default 8) GETs,
  hashes and encrypts the next items while the batch fills. The order in which items are committed, and
  therefore landed, is still the listing's: a producer that relies on order (file-based ones) sees no
  difference.
- **Several intakes for a bulk load.** An intake can be configured as `n` shards: prefixes `…/0/` to
  `…/n-1/` (or by the hash of the object name), each with its own lease, so up to `n` landing nodes are
  active, one per shard. The default is 1, as now. The initial load writes its files across the shards; a
  file-based producer that needs one order keeps one shard.
- **Split pods split several items at once** (default 4): the pod-wide lock goes, and the per-item leases
  stay.
- **Partitions:** 24 per topic by default (so 24 pods per stage at most), set per pipeline.
  Built 2026-11-13, with split concurrency (DESIGN.md revision (dc)): on AWS, `STAGE_PARTITIONS` in `poc.sh`
  for every pipeline's stage inputs.

### 3.5 Signing without a KMS call per message

Each pod makes an ES256 key pair in memory at startup, and **KMS signs a short statement about it**: the
public key, the stage, the pipeline, the pod and an expiry (default 1 hour; renewed at half-life). Messages
are signed with the pod's key locally (about 0.1 ms) and carry the statement's ID. A reader verifies a
statement once, against the stage's KMS public key it already trusts, and caches it until it expires.

- KMS calls fall from one per message to **one per pod per 30 minutes**.
- The private key never leaves the pod's memory, and it dies with the pod, so a leaked key lasts an hour at
  most. The KMS key is still the root of trust, and a stage can still only sign as itself.
- The statement goes in the message's protected header the first time and is fetched by ID after that, from
  a small table in the database, so messages don't grow.
- Seal (ECDH-ES to the next stage's inbox key) is unchanged: it's local already.

### 3.6 Recovery: with batches, and after a long outage

Batches must not make recovery harder, and the pipeline must recover from an outage of days with items part
way through it. The rules that make recovery work now (STAGES.md §13) stay the rules:

- **Nothing is acknowledged before it is durable.** A stage acknowledges its input only once its output is
  committed and sent; a landing node deletes an intake object only once its item is committed.
- **Everything durable is enough to resume from:** checkpoints, the registry and the tables, never memory.
- **Doing something twice is harmless:** records are recognised by item and record ID, so a redelivery is a
  duplicate, dropped (§6 of STAGES.md).
- **The reconcilers rebuild what a broker lost** from the tables: split from `landed_items`, convert and
  transform from the table before them, a day at a time from their watermark, after the backlog has drained.

Batches change how much is redone after a crash (one batch: at most 1 s or 500 items), not whether it's
redone correctly. These are where the proposal needs care:

**1. The landing node's batch has a tail: ledger, announcement, intake delete.** With one item, a crash
between them leaves one item to finish; with a batch, up to 500. The rule: **at startup, and on taking the
lease, the node finishes the last committed batch before it lists anything.** The items' commit carries the
batch's checkpoint, which lists its ledger entries and its intake objects. From it the node writes the
ledger rows if they're missing, sends the announcements not yet sent (the split stage drops a repeat), and
deletes the intake objects still there. Only then does it list. Otherwise a new holder lists objects that are
landed but not yet in the ledger or deleted, and lands them again.

Today's node does this for its one item: at startup it repairs the ledger from the last landed item and
announces that item again, before it lists (`IntakeService.ensureRecovered`). The AWS run's apparent
duplicate at failover was that announcement, counted as an item by the load test; no item was landed twice
(docs/AWS-POC.md). Batching extends the same step from one item to the last batch.

**2. A batch that can't commit.** The record stages already fail a bad record, not its batch. A batch whose
commit fails for something other than an outage (too large, say) is split in half and retried, down to one
record, so one cause fails one record, to the fail store, never a batch of good ones.

**3. Stopping with a batch open.** On `SIGTERM` a stage stops taking input, commits and sends the open batch
within the termination grace period, then stops. If it can't (it's killed, or the store is down), the batch
is redone after the restart, from input that was never acknowledged.

**4. Locations in queued messages, after days.** A message queued for days names a data file that must still
be there when it's read. A data file is deleted only once compaction has replaced it and the snapshots that
held it have expired. So **expiry follows the consumers:** a table's maintenance expires a snapshot only once
it's past retention *and* older than the oldest message still unread by the stages that read the table. The
backlog watch the reconcilers use already knows that. Until then, nothing they need is deleted. If a file is
gone anyway (an operator's own maintenance, say), the reader falls back to the scan (§3.2): slow, but
correct. After an outage, maintenance is the last thing to catch up, not the first.

**5. Pod signing statements expire in an hour, and messages may be queued for days.** A reader judges a
statement at the time the message was signed, not when it's read: the message's signing time must fall inside
the statement's validity, and the statement must be signed by the stage's KMS key. A message signed in a
statement's hour verifies a week later. The statements are kept in the database for as long as a message may
be queued or replayed (the brokers' retention, plus the reprocess window). A statement can be revoked there if a
pod's key is thought exposed; messages under it are then refused, as for a rotated key today (STAGES.md
§13.2), and rebuilt by the reconcilers or a replay.

**6. Brokers' retention against the length of an outage.** Nothing configures it now, so Kafka keeps 7 days.
Beyond that the reconcilers rebuild records, but a lost `item.split` needs a replay by hand (STAGES.md §13.2).
Proposed: **30 days on every pipeline topic**, set by the deployment. Messages carry references only, so a
month of 15M documents' messages is a few GB. For an initial load the backlog sits mostly in `.landed`,
since landing is the fastest stage.

**7. Timers that run out during an outage.** Leases, claims and lease TTLs all expire while nothing runs.
That's correct, since their holders are gone, and on restart whichever pod asks first takes them. The intake
keeps what producers put in it, and the landing node resumes listing.

**8. Upgrading at the restart after an outage.** A new build's drain gate holds it until the old version's
items drain (`stage.version_blocked`, as the AWS session showed this week). After an outage, **restart on the
same build, let it drain, then upgrade**, or force the upgrade knowingly (STAGES.md §14.4a). The runbook
says so.

**9. The reconcilers' catch-up at 15M items.** They check a day at a time, with a registry lookup per item.
At 15M a day that's 15M lookups, so the lookups go in batches (§8), and a day's IDs come from the table's
`item_id` column, read alone.

**What an item part way through looks like after an outage**, by where it was:

| Where it was | What's durable | What resumes it |
|---|---|---|
| In the intake, not yet landed | the object | listing, after the last batch's tail (1) |
| Being landed (in an open batch) | the object | landed again in a new batch |
| Landed, its batch's tail unfinished | the items, and the checkpoint | the tail, before listing (1) |
| Landed, announced, not split | `landed_items`, and `.landed` | the queued message; or the split reconciler |
| Part split | the split checkpoint, and the records so far | split resumes at the checkpoint, as now |
| Split, records queued | `split_records`, and `.split` | the queued messages; or the convert reconciler |
| In convert's or transform's open batch | its input, unacknowledged | redelivered and redone |
| Committed, not yet sent | the registry's unsent rows | sent at the restart, as now |
| Sent, input not yet acknowledged | the output | redelivered; the duplicate is dropped |

**Tested, not assumed.** The load test gets an outage mode: stop the whole pipeline part way through a load
(`poc.sh down --no-drain` keeps the volumes, and in the sandbox a stop of every pod), start it again at
least a session later, and check every document arrives in gold exactly once (the audit query of STAGES.md
§13.2). It also runs with the timers shortened (retention, statement lifetime, snapshot retention) so a
multi-day outage can be simulated in an hour. The PoC already did this by accident on 2026-11-12: 2,000
landed items waited a night in `.landed` and resumed the next session.

**Run on today's code, AWS, 2026-11-13** (`load-test.sh 5 2` with `SDH_LOAD_OUTAGE_AT=40`,
`SDH_LOAD_OUTAGE_MIN=10`; report `load/load-20261113T044533Z`). Every stage, Postgres and Kafka were killed at
once at 78 of 160 landed, with no graceful stop, and kept down 10 minutes. Afterwards, 160 objects landed as
160 items (exactly once), and no item was left part way at any stage. The stages recovered 5 records and 236
item completions (`stage.records_recovered`, `stage.item_completion_recovered`); no deferral reached its cap.
Gold passed the gold check. Two points for reading such a report:

- An item ends at a stage when every record has ended, failed or not. `completed_at` is set only when none
  failed, so it isn't the test. An item with a failure still open upstream stays `upstream_open` downstream
  until the failure is retried (STAGES.md, the fail store); that's held, not left behind.
- The run took 2,392 s from the restart to the last item transformed. Split ran at 3 items/min, against 11.4
  in the run before, on tables holding two runs' commits: every lookup read all 312 manifests of
  `split_records` in about 3.9 s (§2). The gold check's reader does the same lookups and took 23 minutes.

Not yet run: the same with the queued messages deleted (`SDH_LOAD_OUTAGE_LOSE`), which leaves the
reconcilers to rebuild them. At today's split rate it needs about 2 hours.

## 4. Why it holds on every store

The hot path now uses only what all four stores do well and the same way:

| Operation | S3 | SeaweedFS (S3 API) | ADLS Gen2 | GCS |
|---|---|---|---|---|
| Ranged GET of a known object | yes | yes | yes | yes |
| PUT of a new object (data files, metadata) | yes | yes | yes | yes |
| List, only for the intake | yes | yes | yes | yes |
| Bulk delete, only for the intake | `DeleteObjects` (1,000) | same | per object, concurrent | batch (100) |

It no longer depends on how quickly a store answers many small requests in a row, which is where the four
differ most. The catalog commit is a database transaction (§7), the same everywhere.

## 5. Contract changes

| Change | Compatibility |
|---|---|
| `StorageRef` gains `file`, `rows`, `fileSize` | optional fields: an old reader ignores them, a new reader falls back to a scan without them |
| `.landed` carries the item's summary fields and its chunks' locations | as above |
| Messages signed by a pod key with a KMS-signed statement | readers accept both kinds during a rollout; the drain gate already holds a new version until the old drains |
| Topics with more partitions | made at deploy; existing topics are increased, not recreated |

## 6. What stays as it is

At-rest encryption per batch, the record envelope and its AAD, sealing to the next stage, single-active
landing per intake, the fail stores, reprocess and corrections, retention, and the reconcilers (they become
cheaper, not different).

## 7. Catalog

**Keep the JDBC catalog** on the customer's database. It is a single table with one row per Iceberg table,
and a commit is a compare-and-swap of that row: nothing extra to run, back up or upgrade, and it's in the
database the customer already looks after. Group commit keeps it to a few commits a second per table, far
inside what any of the four databases does.

A REST catalog (Polaris, Lakekeeper, Gravitino) adds a service to run and keep available, and nothing here
needs what it adds. A cloud's own catalog (S3 Tables, Glue, BigLake) ties the deployment to that cloud. Both
remain possible later, since the code only sees Iceberg's `Catalog`.

## 8. SQL that runs on every database

The stages' SQL is Postgres-only in places: `ON CONFLICT`, `RETURNING`, `clock_timestamp()`, `bytea`,
`timestamptz`, `CREATE … IF NOT EXISTS`. Proposed:

- One small `SqlDialect` per database for what genuinely differs: type names, "now" from the database
  clock, `LIMIT`/`FETCH FIRST`, and creating a table only if it's missing.
- **Upserts as standard `MERGE`**, which Postgres (15+), SQL Server, Oracle and DB2 all run. On Postgres,
  `MERGE` performs as `ON CONFLICT` for one row and better for many.
- No `RETURNING`: read back with the same key, in the same transaction.
- **Batched statements**: the registry is updated once per commit batch (JDBC batch or one `MERGE` from a
  `VALUES` list), not once per record. That's what keeps the database off the hot path at 660 messages/s,
  more than the dialect.
- Retention of finished registry rows (they're 15M × several a load), by a scheduled delete in batches.

Tests run the SQL on Postgres in the build; the other three are checked in CI by the same tests against their
container images (SQL Server and DB2 have free developer images; Oracle Free). Not measured for speed.

## 9. Large documents

The landing node already streams an item into 8 MiB chunks, so size doesn't matter there. After it:

- **Split** reads an item through `LandedItemSource` a window at a time already; with §3.2 that read is
  direct. A single C-CDA record larger than the memory budget is written to `split_records` in chunks as it's
  cut (the record envelope already has chunks), not built in memory first.
- **Convert and transform** read a record as a stream (`RecordInputStream`), and take memory from the
  stage's shared budget by the record's size. A record too big for the budget waits for the whole budget,
  then runs alone; one over it fails as `RECORD_TOO_LARGE` to the fail store, kept, with a limit set per
  stage. The mapping engine's own memory need for a 100 MB document is to be measured; it's the one part
  that can't stream.
- **Kafka carries references only**, whatever the size, so large documents don't need a different path.

## 10. How we'll know

A measurement harness before any change, so every change is measured against the same baseline:

- **Per item, per stage:** time waiting on the topic, reading, transforming, committing, publishing, and the
  KMS, as histograms (the stages export metrics already; this adds the stage-internal timings).
- **End to end:** each message carries the time its item landed, so the last stage can record landed → gold
  per document.
- **Loads:** 1,000, 100,000 and 1M documents, made from mapping's test documents as now, run on SeaweedFS
  in the sandbox and on S3 in the PoC, with a report: documents/s per stage and overall, latency p50/p95/p99,
  commits and snapshots per table, KMS calls, and cost per million documents on AWS.
- **Pass:** ≥ 100 documents/s sustained over the 1M load, p99 landed → gold under 20 s, and documents/s that
  stays flat as the tables grow (the regression we have now).

**Built, 2026-11-13 (step 0; DESIGN.md revision (cz)).** Decided with the user: the time per document comes from
the registries rather than from a time carried in every message, so no contract change and no figure lost to a
restart; and the baseline on today's code is 1,000 documents (1.5–5 hours at today's 3–11 a minute), with 100,000
and 1M once step 1 makes split fast.

- **Per item:** the split stage notes when each item landed (`split_landed`). With `split_item.ended_at` and each
  stage's `stage_record.stored_at`, `measure.sh` gives landed → gold, and each stage's part, as p50/p95/p99/max.
  The gap from one stage's end of an item to the next stage's first record of it is the queue's wait plus one
  record's work, so queue wait needs no broker timestamps.
- **Inside the stages:** `sdh.stage.phase_duration` by stage and phase (split: the landed-table lookup and the
  whole item; the record stages: open, claim, read, transform, commit, seal, register, send, complete, batch),
  beside `sdh.commit.duration` by table. On AWS a collector passes them to a Prometheus with its OTLP receiver
  (`deploy/aws/k8s/infra/telemetry.yaml`, about 0.8 GiB on the base nodes); its data goes at `poc.sh down`.
- **The report** (`deploy/aws/k8s/pipeline/measure.sh`, run by `load-test.sh`): the above, the rate in tenths of
  the run by when items reached gold (flat, or falling as the tables grow), snapshots kept and made per table
  and their metadata size, KMS calls by API from CloudWatch, and the cost per million documents at the run's rate
  (on-demand node price, EKS and KMS; storage and S3 requests not counted).
- **Not yet:** the sandbox (SeaweedFS) run of the same report; it has its own telemetry stack, and `measure.sh`
  reads the AWS PoC's.
- **Changed after the first run:** commits per table come from the metadata version in the catalog, and KMS
  signatures from the messages written to the topics (each is signed once): the deployer may read neither the
  warehouse's metadata nor CloudWatch, by design. Each stage pod reports as its own instance
  (`OTEL_RESOURCE_ATTRIBUTES=service.instance.id=<pod>`): without it two pods' counters interleaved in one
  Prometheus series, read as resets, and counts came out tens of times too high. The pipeline's network policies
  let the stages reach the collector (port 4318).

**Baseline, AWS, 2026-11-13** (`poc.sh reset`, then `load-test.sh 34 0`: 1,020 C-CDA documents, 115 MB, no
failover; on-demand burst nodes; stopped by agreement after 103 minutes with 748 items split, 744 in gold; report
`load/load-20261113T141643Z/measured.txt`). The figures to beat:

| | Baseline |
|---|---|
| Throughput, first landed to last in gold | **0.12 documents/s** (target 100) |
| Landed → gold, p50 / p95 / p99 | **3,592 / 5,320 / 5,526 s** (target p99 20 s); all but ~20 s of it is waiting for split |
| Landing | all 1,020 landed in about 20 minutes (~50/min) |
| Split, per pod | 6.0 s an item fresh, 12.9 s 40 minutes later; 2 pods, so 3–16 items/min |
| Convert per record (1 pod) | read 1.49 s, transform 0.02 s; batch 1.68 s, commit 0.42 s |
| Transform per record (1 pod) | read 0.59 s, transform 0.65 s; batch 1.90 s, commit 0.40 s |
| Seal and sign, per message | 0.016 s |
| Commits | one per item to `split_records` (754 for 748 items), and one per item to each landing table |
| KMS Sign calls | 7.2 per document, so the ~300/s quota caps the pipeline at **~41 documents/s** (§2.3) |
| Cost at this rate | ~$950 per million documents (compute $850, KMS $110), on-demand prices |

What it showed, beyond §2's predictions:

- **Split slows inside a pod as it runs, and a restart restores it** (later found to be the landed table's growth, not the pod: see the next run below). The rate fell from 25 to 3 items/min over
  the first hour, with both split pods on the base nodes beside every other pod. Restarted at 15:18 (for the
  instance fix), both landed on a fresh burst node and ran at 16/min, then fell again to 6/min within 40 minutes.
  Over those 40 minutes the landed-table lookup (0.3 s), the `split_records` commit (0.3 s) and publishing
  (0.03 s) stayed flat; the rest of the item doubled. So it is neither the lookup §2.1 named, nor commit time.
  Candidates, for step 1 to time apart before changing anything: the point lookups on `split_records` within
  an item (a pending batch's resolution, the fail store's), which plan a table growing a manifest per commit;
  something held in the pod that grows with the items split (a cached table's metadata, the heap); and CPU
  shared with the other pods on the base nodes (the restart also moved split to a node of its own).
- **Reading dominates the record stages**: nearly all of convert's work on a record (1.49 of 1.51 s) is
  reading it by point lookup (§2.1), and transform's read is as long as its transform.
- **Not reliable from this run:** split's phase times before 15:18, and convert's and transform's after KEDA
  scaled them to 3 and 4 pods (the instance fix wasn't in their pods); the per-record figures above are from
  the first hour, at one pod each.
- **Split's steps timed apart, AWS, 2026-11-13 (DESIGN.md revision (da)).** Same load as the baseline, on a
  reset pipeline, stopped after 50 minutes once the answer was clear (199 items to gold, 0.07 documents/s;
  report `load-20261113T174625Z/measured.txt`). Split's mean seconds per item, by quarter of the run:

  | Step | Q1 | Q2 | Q3 | Q4 |
  |---|---|---|---|---|
  | `lookup` (landed table: refresh, summary) | 3.52 | 12.56 | 12.80 | 12.54 |
  | `open` (landed table: refresh, item's chunks) | 3.54 | 12.23 | 12.81 | 12.54 |
  | `split` (reads the chunks, writes `split_records`) | 3.83 | 12.70 | 13.29 | 13.11 |
  | `failed_rows` (`split_records` lookup) | 0.74 | 1.49 | 1.97 | 2.29 |
  | `correction_rows` (`split_records` lookup) | 0.69 | 1.44 | 1.92 | 2.24 |
  | everything else | 0.03 | 0.03 | 0.03 | 0.02 |
  | item | 12.24 | 40.25 | 41.39 | 38.59 |

  Landing committed all 1,020 items to `landed_items` in the first 18 minutes, one commit each. In 3-minute
  windows, `lookup`, `open` and `split` rose with those commits (0.8 s at 92 commits, 12.4 s at ~1,000) and
  went flat at ~12.5 s the moment landing stopped. Split was then restarted, and both pods came up on a burst
  node of their own: no change (12.5 s each). CPU on the split pods was 6%, garbage collection 0.2%, Postgres
  idle. So it is **§2.1 on the landed table, three times an item**: each read of it plans every manifest,
  about 12 ms per commit, and nothing held in the pod or shared CPU plays a part. The `split_records`
  lookups are the same effect, smaller so far because that table has had fewer commits (198); they grow
  with it. Together the five steps are 99% of the item. The baseline's reading that `lookup` stayed at
  0.3 s after its restart, and that a restart restores the rate, is not borne out; its split figures were
  taken while two pods' counters were mixed (the instance fix), so this run's figures stand. Convert's
  read (1.76 s per record) and transform's (0.78 s) are point lookups of the same kind (§2.1).
  Step 1's direct reads by location (§3.2) remove all of these; housekeeping (§3.3) alone would only cap them.
- **Reads by location, AWS, 2026-11-13 (step 1, first part; DESIGN.md revision (db)).** Same load, reset
  pipeline, same nodes (report `load-20261113T192923Z/measured.txt`):

  | | Baseline | Split timed | Reads by location |
  |---|---|---|---|
  | items to gold | 744 of 748 in 103 min | 196 (stopped at 50 min) | 1,020 of 1,020 in 18 min |
  | documents/s, first landed to last in gold | 0.12 | 0.07 | **1.23** |
  | landed → gold p50 / p95 / p99 (s) | 3,592 / 5,320 / 5,526 | 551 / 2,434 / 2,667 | **22.5 / 47.5 / 58.5** |
  | split per item (s), first → last quarter | 6 → 13 | 12.2 → 38.6 | **0.93 → 0.77** |
  | convert's read per record (s) | 1.49 | 1.76 | **0.23** |
  | transform's read per record (s) | 0.59 | 0.78 | **0.20** |
  | cost per million documents | ~$950 | $1,436 | **$226** |

  Split's steps by quarter are flat: lookup 0.20 → 0.15 s (the summary row by position), open 0.00, the
  split itself 0.66 → 0.58 s, the end-of-item lookups 0.00. The rate held at 67–101 items/min from the first
  tenth of the run to the last. What now sets the pace: split's own work on an item (0.6 s, one pod at a time
  per item and one item per pod, §3.4), each record stage's commit per batch (0.5 s) and transform's
  transform (0.36 s); and in latency, the gaps between stages (p95 14 s and 22 s: queue waits behind those
  commits). Landing still made one commit per item (1,020 to `landed_items`, and split 1,020 to
  `split_records`), which step 2's group commit takes on. KMS: 6.5 signatures per document, a ceiling of
  ~46 documents/s, which step 3 removes. The p99 is 58.5 s against the 20 s goal, at 1.2 documents/s
  against 100: steps 1 (rest) to 3 are still needed.
  Not checked in this run: the gold check. Its first run was killed by a Spot reclaim of its node after
  13 minutes (`backoffLimit: 0`); the second was stopped at 27 minutes for the session's end. It reads each
  bundle's source C-CDA from `split_records` by `CommittedRecords` lookup, a planned scan of ~13 s at 1,022
  commits, since a gold message doesn't say where its source split record is: so do readers following
  `GoldReaderExample`. Housekeeping's manifest rewrite (§3.3) makes that scan cheap; carrying the source's
  location in the lineage would remove it. Every record convert and transform read was read by position and
  decrypted, and the AAD binds item, seq and chunk, so the reads themselves are checked. Also seen: table
  maintenance runs on AWS, and its orphan sweep logs an ERROR per old metadata file Iceberg has already
  deleted (`ReachableFileUtil`, 100 in 10 minutes); harmless, to be quietened.
- **Step 1 complete, AWS, 2026-11-13 (DESIGN.md revisions (dc), (dd)).** Housekeeping, split 4 items at once
  per pod, 24 partitions per stage topic. Two runs of the same 1,020-document load, each on a reset pipeline
  (reports `load-20261113T214645Z` and `load-20261113T221700Z`; gold check skipped).

  **Run A, as deployed.** 0.98 documents/s first landed to last in gold, landed → gold p50 / p95 / p99
  25.1 / 51.0 / 58.2 s: no better than reads by location alone (1.23, 22.5 / 47.5 / 58.5). **Landing is
  now the limit:** it lands one item at a time, ~1.3 items/s, and split keeps within a few items of it.
  The rate is lower overall because of two stalls of ~2 minutes, at the start and after the failover:
  the AWS landing configuration wrote `ready` and `after` as nested keys (`ready: { mode: stable, settle:
  0s }`), which the flat settings ignore, so landing waited the default `settle`, 2 minutes, after each
  start (in every AWS run so far). Fixed in `landing-application.yml`. Split's item took 1.69 s, against
  0.83 s one at a time: four items share the pod's 0.5 CPU, and `split_records` commits took 1.04 s
  against 0.36 s, more of them at once.

  **Run B, split held back.** With the landing fix, and split paused (KEDA `paused-replicas: 0`) until
  every item had landed, so the stages after landing work through a backlog:
  - **Landing alone:** 1,021 items in 875 s with a 36 s failover, 70 items/min (1.2/s), starting at once.
  - **Split, convert and transform on the backlog:** 234.8 items/min to gold in the run's best tenth
    (3.9 documents/s, two split pods), 165 and 109 in the next: about three times landing's rate. Split's
    item fell from 6.6 s (4 at once, cold) to 1.0 s within the run.
  - **A livelock, found and fixed (revision (dd)).** Paused past the reconcile grace (10 minutes), every
    landed item looked lost, and the reconciler announced each again (`split.recovered`, 536 messages more
    on `<p>.split`). A batch could then hold two messages for one item: split at once, one took the lease
    and the other was refused it, so the batch was retried, and both ran again, for ever. One partition
    stopped at 994 of 1,020 items for 20 minutes. Now a retried batch skips the messages it has already
    split (known by broker ID: a Kafka batch comes again as new objects), so the duplicate finds the item
    done. Deployed during the run: the stuck partition cleared and every item reached gold, exactly once.
    Run B's latency (p50 843 s) is the pause, not the pipeline.

  What sets the pace now: **landing, at ~1.2 items/s**, one commit per item to `landed_items` and the
  ledger: step 2's group commit. After it, split's CPU (0.5 a pod) and the record stages' commits per
  batch. KMS: ~6.5–7 signatures per document, ~42–46 documents/s (step 3).

## 11. Order of work

Each step is measured before the next, and each is useful alone.

| Step | What | Expected effect |
|---|---|---|
| 0 | Measurement harness and a baseline at 1,000 documents (§10; 100,000 after step 1) | the numbers to beat. Harness built and baseline run 2026-11-13 (§10): 0.12 documents/s, p99 5,526 s; split slows as the landed table grows: three planned reads of it per item, 99% of split's time with the `split_records` lookups (§10, 2026-11-13); not the pod, not CPU |
| 0a | The outage mode of the load test (§3.6), run on the current code; find the failover duplicate | recovery checked before anything changes it. Duplicate found (the re-announce, §3.6 point 1); plain outage passed 2026-11-13; message-losing outage not yet run |
| 1 | Direct reads by location (§3.2), housekeeping (§3.3), split concurrency and partitions (§3.4, part) | split no longer slows as tables grow; split and convert at pod count × per-pod rate. Reads by location built and measured 2026-11-13 (DESIGN.md revision (db), §10): 10× the rate, p99 58.5 s, split flat as tables grow. Housekeeping, split concurrency (4 per pod) and 24 partitions built and measured 2026-11-13 (revisions (dc), (dd), §10): landing (~1.2 items/s) is now the limit; on a backlog the stages after it reach ~3.9 documents/s; no item bucket (§3.3); expiry doesn't yet follow the consumers (§3.6, 4) |
| 2 | Group commit in every stage (§3.1) | landing from ~10 to hundreds of items/s; ~100× fewer snapshots. Built 2026-11-13 (DESIGN.md revision (de)): landing groups small items listed from a source (500 a commit, 64 MB, 1 s), split commits concurrent items together; the ledger is written after the items, not before; not yet measured |
| 3 | Pod signing keys (§3.5) | removes the ~45 documents/s KMS ceiling |
| 4 | Concurrent fetch in the landing node and sharded intakes (§3.4) | landing beyond one node's serial rate |
| 5 | SQL dialects and batched registry updates (§8) | database work per batch, not per record; runs on four databases |
| 6 | Large documents (§9) | 100 MB documents through, with bounded memory |
| 7 | 1M documents on S3 and SeaweedFS, against §10's pass mark, with an outage part way (§3.6) | every document in gold once, after the restart |

## 12. Decided with the user, 2026-11-13

1. **Targets (§1):** at least 100 documents/s sustained (15M in under 48 h), and p99 landed → gold under 20 s,
   measured over 1M documents.
2. **Snapshot retention (§3.3):** 24 hours in the hot tables, and never before the consumers have read past
   it (§3.6, 4).
3. **Signing (§3.5):** pod keys, each vouched for by a KMS-signed statement valid for 1 hour, verified at the
   message's signing time, revocable.
4. **Sharded intakes (§3.4):** a fixed number of shards per intake, by prefix (`…/0/` to `…/n-1/`), one
   single-active landing node each; default 1.
5. **Large records (§9):** a record over the stage's limit fails to the fail store as `RECORD_TOO_LARGE`, kept
   and reprocessable with a larger limit.
6. **Broker retention (§3.6, 6):** 30 days on every pipeline topic.
