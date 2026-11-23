# Pipeline performance: design

Status: **agreed 2026-11-13** (§12). Nothing here is built yet. As each step is built, it goes into DESIGN.md and
STAGES.md as a revision; this file is removed once all are.

## 1. Targets

| | Target | Why |
|---|---|---|
| Initial load | **15M C-CDAs at ≥ 60 documents/s sustained** (about 70 hours); more by adding pods and intakes (§3.4) | revised 2026-11-20 with the user (below); the current system takes over 10 days (≈ 17/s) |
| Latency, landed → gold | **p99 under 200 s** per document, while sustaining the load above | revised 2026-11-20 with the user (below) |
| Stores | S3 and SeaweedFS measured; ADLS Gen2 and GCS equal by construction (§4) | customers choose the store |
| Databases | Postgres measured; SQL that runs unchanged on SQL Server, Oracle and DB2 (§8) | customer policy |
| Documents | typical ≤ 500 KB; large ones (base64 PDFs, images) of 100 MB and more stream, never held whole in memory (§9) | |

**Revised 2026-11-20, with the user.** The targets agreed on 2026-11-13 were 15M in under 48 hours (at least
100 documents/s, headroom to 250/s) and p50 under 5 s, p99 under 20 s. Realistically 60 documents/s and 200 s are
enough for C-CDA, if they hold over a 15-million-document initial load. The examples, about 119 KB each, are a
never-before-seen patient's documents: the shape of the initial load. Once the FHIR repository is loaded, a known
patient's later documents are mostly incremental updates, smaller, and large C-CDAs come mainly with new patients;
the steady flow afterwards is a few documents a second for a population of tens of millions (DESIGN.md §5.3.2a).
Where it stands: runs of 90,000 documents sustained 52 to 72 documents/s on run 49's maxima (§10, runs 50 to 54),
p99 131 to 168 s fed at 35/s and 425 s fed at 100/s, faster than split's pace. Not yet shown: the rate and p99 at a
feed of 60/s, over hours rather than 20 minutes, and with the stores sized for 15 million (§11.1).
With the HIPAA work (2026-11-22, runs 62 and 63): unchanged at 35/s (p99 152 s), 46 documents/s at 100/s against
run 59's 55, split slower an item; the audit journal the likeliest cause (§10; measured locally in §13: its
small transactions add 30% to Postgres' CPU an item, which a 100 ms linger removes). With a
shared key agreement for sealing and a native provider (2026-11-23, runs 64 to 67, §13.1): 55.6 and 59.1
documents/s at 100/s, p99 115 s at 35/s, so the HIPAA work's cost at saturation is recovered.

At 60 documents/s that is about 400 messages/s and, per stage, 60 items/s. A per-document p99 of 200 s leaves
each of the four stages about 50 s, waiting included.

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
  are landed alone. The intake objects are deleted after the commit, one at a time (each a HEAD, a
  conditional delete and a HEAD for a `.replaces` marker), not in bulk as first proposed; since step 4, several
  at once (§3.4). A crash before the commit lands the batch
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
  difference. **Built 2026-11-16 (DESIGN.md revision (dp)), step 4 part 1:** a group's items are read up to
  `sdh.storage.group.concurrency` (default 8) at once, each whole on its own virtual thread, ahead of the
  one being written; they are written, committed and announced in the listing's order. After the ledger and
  the messages, the group's items are deleted, moved or renamed at the source as many at once; one that fails
  stays to be tried again, and the others are finished all the same. Only a source whose requests stand
  alone does either (S3, GCS, Azure Blob, a directory: `IntakeSource.concurrent`); SFTP and SMB, which share a
  session, stay one at a time. Why: landing measured ~74 ms an item (~11 items/s), nearly all four S3 round
  trips one after another (the GET, then a HEAD, the delete and a HEAD for a `.replaces` marker); the commits
  are a few ms an item. Landing's steps are now timed (`sdh.stage.phase_duration`, stage `landing`: per
  group, `read_wait`, `write`, `close`, `commit`, `ledger`, `announce`, `finish`, `group`). **Measured
  2026-11-16** (`load-20261116T163159Z`, 7,200 documents, no failover): **landing 30.9 items/s** (7,200 by
  233 s; ~11 before), each landed exactly once, all 7,200 to gold; the gold check passed (0 failed, every
  bundle's source document matched). A group of ~450 items took 12.8 s (28 ms an
  item): waiting for reads 3.1 s, writing 1.2, closing and committing 0.5, the ledger 0.4, **announcing 3.6**
  (still one message at a time) and **finishing at the source 3.9** (8 at once). Landing's pod used 0.63 CPU
  on its 0.25 request, waiting for a CPU 31% of the time. Overall 15.1 documents/s (6.9–8.2 in revision (dm)'s
  runs, which landing paced); split now sets the pace (7,200 by 440 s, 16.4 items/s on 3 pods), transform
  ends 58 s after it; landed → gold p50 / p99 179 / 325 s, as landing now outruns split and the queue waits
  there; ~$18 per million documents (10 nodes at most). Next in landing: the announcements sent as a batch,
  and a CPU request that matches (§11, step 4).
  **The announcements were already sent as a batch** (all of a group's, then the acknowledgements waited
  for); their time was sealing each message, an ES256 signature and an ECDH encryption, one after another on
  a pod that had 0.25 CPU. **Built and measured 2026-11-16 (DESIGN.md revision (dq))**: a group's messages
  sealed up to `sdh.storage.group.concurrency` at once, still sent in order, and landing's CPU request 0.75
  (AWS-POC.md §5). On AWS (`load-20261116T200317Z`, 7,200 documents): **landing 60 items/s** (7,200 by
  120 s; 30.9), while the objects were still arriving until 87 s, so one node's rate is at least that; each
  exactly once; the gold check passed. A group (~550 items) 7.0 s (12.8), 12.7 ms an item (28): reads 1.4,
  writing 0.5, commit and close 0.4, ledger 0.3, sealing 1.2 and sending 0.1 (announcing 3.6 before),
  finishing at the source 2.8 (3.9), now the largest step. Landing's pod used 0.71 CPU, waiting for one 12%
  of the time (31%). Split ended at 295 s (440), 24 items/s on its 3 pods, with landing no longer short of
  CPU beside it; **transform now sets the pace**, ending at 528 s on its 4 pods: 14.4 documents/s overall
  (15.1), landed → gold p50 / p99 215 / 406 s as the queue now waits before transform; ~$17 per million
  documents (9 nodes at most).
- **Several intakes for a bulk load.** An intake can be configured as `n` shards: prefixes `…/0/` to
  `…/n-1/` (or by the hash of the object name), each with its own lease, so up to `n` landing nodes are
  active, one per shard. The default is 1, as now. The initial load writes its files across the shards; a
  file-based producer that needs one order keeps one shard.
- **Split pods split several items at once** (default 4): the pod-wide lock goes, and the per-item leases
  stay.
- **Several batches at once in a split pod** (`sdh.split.consumers`, added 2026-11-14, DESIGN.md revision
  (dh)). Measuring step 2 showed a split pod at under 1 item/s: it took one batch of 4 at a time, and a batch
  is acknowledged only once all its items are split, so each batch waited for its slowest item (2.4 s on
  average per item, 4.9 s at p95, mostly S3, the catalog and Kafka, not CPU). With one Kafka consumer per
  batch, a slow item holds back only its own; on AWS, 8 consumers of 2 items each.
- **Several batches at once in a record stage's pod** (`sdh.stage.consumers`, added 2026-11-15, DESIGN.md
  revision (dn)). Clearing a backlog (§10) showed a transform pod using 1.1–1.3 of its 1.6 cores: the CPU sat
  idle while each batch of ~47 records committed, registered and sent. With two consumers, a batch's records
  are processed while the other's commit, on the same `concurrency` threads; on AWS, transform has 2.
- **Partitions:** 24 per topic by default (so 24 pods per stage at most), set per pipeline.
  Built 2026-11-13, with split concurrency (DESIGN.md revision (dc)): on AWS, `STAGE_PARTITIONS` in `poc.sh`
  for every pipeline's stage inputs.

### 3.5 Signing without a KMS call per message

Each pod makes an ES256 key pair in memory at startup, and **KMS signs a short statement about it**: the
public key, the stage, the pipeline, the pod and an expiry (default 1 hour; renewed at half-life). Messages
are signed with the pod's key locally (about 0.1 ms) and carry the statement and their signing time. A reader
verifies a statement once, against the stage's KMS public key it already trusts, and caches it.

- KMS calls fall from one per message to **one per pod per 30 minutes**.
- The private key never leaves the pod's memory, and it dies with the pod, so a leaked key lasts an hour at
  most. The KMS key is still the root of trust, and a stage can still only sign as itself.
- The statement goes in every message's protected header (`sdh_stmt`, about 500 bytes), with the signing time
  (`sdh_iat`). Decided with the user, 2026-11-13: no table, so a reader needs only the message and the key it
  already trusts, even for a message replayed days later. A reader checks the signing time against the
  statement, not the time now, so a message that waited in a queue still opens.
- **Revocation:** each new statement is logged (`crypto.pod_statement`, its ID, the pod and its expiry; no
  PHI). Listing an ID in `sdh.crypto.pod-keys.revoked` refuses every message signed under it
  (`REVOKED_SIGNER`), including those signed before; the pod's next statement is unaffected. Messages under a
  revoked statement are then redone from the tables by the reconcilers, not lost.
- **Outage:** if KMS can't sign the next statement, the pod keeps its key until it expires, so a KMS outage of
  under half an hour goes unnoticed; after that, sealing fails as it did on every message before.
- **PHI and HIPAA:** nothing changes at rest or in the seal; the statement holds no PHI. The pod's private key
  is in the JVM's heap, so a heap or core dump would hold it: dumps stay off in the images, as now. The JVM's
  ES256 is not a FIPS 140 validated module (KMS is); a deployment that needs one sets
  `sdh.crypto.pod-keys.enabled: false` and signs every message in KMS, at the old ceiling. CloudTrail records
  one `Sign` per statement instead of per message; the log line ties each statement to its pod.
- Results sent to producers stay signed with the stage's KMS key, since producers trust that key alone.
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
month of 15M documents' messages is a few GB. **Measured since (run 53, 2026-11-19):** about 21 KB of messages
a document across the four topics, signed and sealed, so a month of 15M is about 315 GB, not a few. Set
2026-11-20 for the PoC's broker (`KAFKA_LOG_RETENTION_HOURS` 720, `k8s/infra/state.yaml`); its 10 GiB volume holds
about 500,000 documents' messages (§11.1). For an initial load the backlog sits mostly in `.landed`,
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

**With the queued messages deleted, AWS, 2026-11-16** (a long test: `load-test.sh` defaults, 2,450 objects,
with `SDH_LOAD_OUTAGE_AT=40`, `SDH_LOAD_OUTAGE_MIN=10`, `SDH_LOAD_OUTAGE_LOSE=landed,split,records`; report
`load/load-20261116T032724Z`). Killed at 1,222 landed; the queued messages of all three stage topics deleted
while down. **Not recovered in full.** Landing and split were whole: 2,450 objects landed as 2,450 items
(exactly once), split ended them all (2,430 completed, the 20 error inputs failed), its reconciler splitting 22
whose `item.landed` was lost. Convert and transform were not: 145 items split but never ended at convert, and
828 converted but never ended at transform, so 1,602 of 2,430 reached gold. Once the queues drained (~1,085 s,
275 s after the restart), nothing moved until the reconcilers' backlog wait ended (~2,940 s); that one pass
rebuilt 215 records (`stage.records_recovered`) and completed 154 items (`stage.item_completion_recovered`),
and no later pass came before the test's 45 minutes ran out (the interval is 15 minutes).
`stage.item_total_missing` 87: items whose records are all here but whose item end (`item.split`,
`item.converted`) was among the messages lost. That is as designed (STAGES.md §13.2, decided 2026-11-06):
reconcilers rebuild lost record messages, but a lost item end is left for an operator, now by `reprocess`
(STAGES.md §15.8). How many of the 973 items left are lost item ends, and how many a second reconcile pass would
have finished, the run doesn't say: the pods' logs went with the cluster.

**Rerun with lost item ends sent again (2026-11-16, load-20261116T060237Z; DESIGN.md revision (do)).** The
same test, with a 90-minute settle. **Recovered in full:** 0 items left behind at convert and at transform;
all 2,430 completed items reached gold (1,602 before), each exactly once. The 10 that convert failed have no
record for transform, so transform ended 2,420 items. The last item was transformed 2,735 s after the restart:
convert finished at ~1,420 s, and transform caught up in steps, one at each reconcile pass, 15 minutes apart.
The audit counts are from the pods still running at the end, and those undercount after the scale-downs:
`stage.item_completion_recovered` 490, `stage.records_recovered` 0, `stage.item_total_missing` 0, and
`stage.item_end_resent` not yet counted by the report. The load test now counts it, and also counts from the
registries the ends each stage asked for again (`total_wanted_at`) and how many came. Counted afterwards from
the registries kept: transform asked for 231 of convert's item ends again, and all 231 came; convert asked for
none of split's. Recovery is now
automatic, but it is slow: the time is set by the reconcile interval and the backlog wait, not by the work.
The gold check did not run to the end, three times, and none of the failures came from the check itself.
Four minutes into reading, its pod was removed with its node. In a second session, the script stopped
following a reader that prints only when it ends. In a third, the reader ended twelve minutes in, its pod gone
with no failure of its own, most likely a Spot reclaim (not confirmed). The pod is now marked not safe to
evict and prefers base nodes. The script follows a container until it ends, and says which node it ran on. The
content check passed on the fourth try (a burst Spot node; the read took 12.6 minutes): 3,908 messages read,
1,215 bundles, 1,205 items complete, as many as transform mapped; 0 checks failed; 32 bundles through the HL7
validator, 0 with errors mapping's baseline hasn't. The lost and resent messages left nothing wrong in gold.

### 3.7 One committer for a table, across pods (built 2026-11-15)

**Decided with the user, 2026-11-15:** built as below, for `split_records` first (convert's and
transform's tables later). The commit-rate cap is a setting, off by default until measured.

**As built** (DESIGN.md revision (dm)), it differs from the proposal below in three ways. The tables are
`append_lock`, `append_queue` and `append_queue_lease` (a group's leases, one row each). There is no claim
token: the committing pod writes the groups' own IDs into the snapshot's summary (`sdh.appends`), and the
groups stay `waiting` until it marks them, so a pod that dies part way leaves them waiting, and the next
commit finds the IDs in the summaries of the snapshots since they were queued. A commit takes at most 64
groups. Measured in §10: retries from 31% of commits to under 2%, split 1.38 s an item from 1.85.

**The record stages too** (DESIGN.md revision (ds), 2026-11-17): `sdh.stage.commit.shared` sends a convert or
transform batch's data file through the same queue, with no leases, since their registry settles what was
stored (STAGES.md §6). On for transform, whose 18 pods' own commits to gold took 4.1 s (p95 13.8) at 100 a
second and held gold ~80 s and more behind the feed at 35 (§10, runs of 2026-11-17). Checked in the sandbox (2
pods: 11 commits, all through the queue, none refused). Measured on AWS (§10, runs of 2026-11-18): a commit's
turn 0.46 s (p95 0.95), 65.1 documents a second at 100 a second (39.2), landed → gold p99 185 s at 35 (385).

**The problem.** Each split pod commits its own groups to `split_records`. With three pods, 31% of their
commits found another's had gone in first (§10, revision (dl)): Iceberg then reads the table again, rebuilds
the manifest list and tries again, after a 100 ms wait. That took a group commit from 0.38 s to 0.75 s once
all three pods were committing at once. Items wait as long again behind their own pod's last commit. More
pods means more conflicts, so this gets worse just as §1's rate needs more split pods.

**The idea: combine commits through the database.** A pod doesn't commit its own files. It puts its group in
a queue in the stage database, then commits for **every** pod whose group is waiting. Only one pod commits
at a time, so commits never conflict, and each one takes all the work waiting from every pod. No pod is
elected, and nothing new runs: the queue and its lock are rows in the database the stages already use.

1. **Before queueing**, a pod's appender writes its group's files into a manifest itself (`ManifestFiles.write`,
   no snapshot ID: in format 2 the commit's ID and sequence number are inherited). That's the manifest PUT,
   done off the critical path and on many pods at once. It then inserts one row into `pending_appends`:
   the table, the manifest (`ManifestFiles.encode`, a few hundred bytes), each file's item lease name and
   epoch, and `waiting`. This is committed at once.
2. **Taking a turn.** The pod opens a transaction and locks the table's row in `append_locks`
   (`SELECT … FOR UPDATE`). It waits there while another pod commits. Once it holds the lock, it reads its
   own row first. If another pod has already committed it, it takes that snapshot and lets go. Typically
   that is every pod but one, a millisecond each.
3. **Committing for everyone.** Otherwise it locks every `waiting` row for the table (up to 500 files' worth)
   and refuses any whose lease isn't current in the lease table, by epoch and with a margin before expiry.
   It writes them a claim token (`claimed`), in its own committed statement. It then makes one Iceberg fast
   append of all their manifests (`appendManifest`), with the token in the snapshot's summary. Finally it
   marks the rows `committed` with the snapshot ID and releases the lock. One commit is a refresh, the
   parent's manifest list, the manifest list PUT, the metadata PUT and the catalog's compare-and-swap.
   Maintenance's own commits to the table (manifest rewrites, expiry) can still conflict, rarely, and
   Iceberg retries as now.
4. **Each pod then carries on as now:** saving the checkpoint, announcing the records, completing the item.

**Exactly once, and what can go wrong.**

- *A pod dies after queueing.* Its row is still committed while its lease is current. When its item is next
  leased (epoch + 1), the new holder first marks any `waiting` rows of the old epoch `refused`. It then
  runs today's check (`CommittedRecords.has`), which sees anything committed. If a commit holds the row at
  that moment, the new holder's update waits for that commit's row lock, so the check never runs part way
  through it. This is stronger than today, where a pod checks its lease only in its own memory.
- *The committing pod dies part way.* Its transaction rolls back and releases the lock. Its rows are left
  `claimed`, with a token that's either in a snapshot's summary or nowhere. The next pod to take the lock
  looks for the token in the table's recent snapshots. Found: it marks the rows `committed` with that
  snapshot. Not found: it commits them again. Iceberg's own commit-state-unknown case is settled the same
  way.
- *A lease lost while queued:* the row is refused, and the pod is told, as `GroupAppender` tells it now. Its
  manifest is deleted. Refused and committed rows are deleted after a day.
- **One window stays the same as today.** A lease can run out between the check and the compare-and-swap,
  about 0.3 s. The margin covers it, as it covers today's.

**What else was weighed.**

| | Conflicts | With more pods | Cost |
|---|---|---|---|
| Today: a committer per pod | 31% at 3 pods, rising | worse | none |
| A database lock, each pod committing its own group in turn | none | each pod waits for every other's commit: latency grows with pods | none |
| **Combining through the database (this)** | **none** | **one commit takes everyone's: commits per table stay ~1/commit time** | **a few statements a group** |
| One pod elected to commit, the others sending it files on a topic | none | as above | a topic, a reply path, failover of the committer |
| A table per pod or partition | none | readers union many tables, maintenance per table | analytics harder |

**Expected effect** (from run (dl)'s numbers). A group commit drops to about 0.35 s, from 0.61 s: no
conflicts, and no manifest PUT. The wait behind the last commit drops to half a commit, from 0.60 s. Appending
then takes about 0.5 s an item instead of 1.27 s, and an item about 1.1 s in split instead of 1.85. There
are no failed attempts' S3 requests, and no new pods or nodes, so no extra cost. On this load split already
ends about 30 s after landing and transform 60 s after split, so documents/s will rise less than split's
time per item falls. The real gain is that split's commits stay flat as pods are added, which §1's rate
needs. The same appender can later serve convert's and transform's shared tables (0.40–0.47 s a commit,
15% retried).

**A commit-rate cap (separate choice).** §3.1 planned at most one commit a second per writer, for §2.2:
`metadata.json` lists every snapshot kept, ~830 bytes each (~90 gzipped). Combined, a table commits back to
back under load, at most ~3 a second: at 24 hours' retention, up to ~250,000 snapshots and ~22 MB of
gzipped metadata read and written per commit. A minimum interval, say 1 s, bounds that at 86,400 (~8 MB)
for up to 1 s more latency. It's one setting either way.

**Portability.** Plain `SELECT … FOR UPDATE`, `INSERT` and `UPDATE` on two small tables: as written it
runs on Postgres, MySQL and Oracle, and on SQL Server it needs the lock hint `WITH (UPDLOCK, ROWLOCK)` (§8's
dialect). Store-neutral: manifests go through the table's `FileIO`.

**Tests.**
- Embedded Postgres: two appenders' groups in one commit; a lease lost while queued; a new lease holder's
  fence; a committing pod that dies before and after its Iceberg commit, settled both ways.
- AWS: run (dl)'s load, then the same with a failover.

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
- **Pass:** ≥ 60 documents/s sustained over the 15M initial load, p99 landed → gold under 200 s, and
  documents/s that stays flat as the tables grow (revised 2026-11-20, §1; until then ≥ 100/s and 20 s over 1M).

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
- **What the run used** (`deploy/aws/k8s/pipeline/resources.py`, sampled by `load-test.sh` every
  `SDH_LOAD_SAMPLE_SEC`, 60, seconds; 2026-11-19): min / avg / max of each stage's pods and of the nodes; by app,
  a pod's CPU (cores, against its request) and memory (working set, against its limit), the app's total at its
  peak, and pods that came within 10% of their limit; by node pool, the share of allocatable CPU and memory used;
  Postgres' size and growth, its volume, commits a second, cache hits, temp files, deadlocks, backends active and
  waiting on locks or I/O, connections, its sizing settings and the largest schemas; Kafka's volume and bytes by
  topic. From the kubelets' stats summaries, `pg_stat_database`, `pg_stat_activity`, `df` and `du`: names and
  numbers only. Kept in the run's directory (`usage`, `pg-stats`, `kafka-disk`, `pg-schemas`, `pg-settings`).
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

- **Step 2, group commit, AWS, 2026-11-14 (DESIGN.md revision (de)).** The same 1,020-document load on a
  reset pipeline, with the landing failover part-way (report `load-20261114T021156Z`; gold check skipped).
  Every item landed exactly once and reached gold.
  - **Landing is no longer the limit.** 1,013 of 1,020 items landed by 130 s, through a failover whose
    standby took the lease 58 s after the holder was deleted: about 10 items/s while a pod held the lease,
    against 1.2 before. All of it in **2 commits** to `landed_items` and 2 to the ledger (5 metadata
    versions each, with the fencing), against one of each per item. What paces it now is reading the items
    one after another (step 4).
  - **Overall: 1.84 documents/s** first landed to last in gold (0.98 in step 1's run A), 106 items/min end
    to end. Landed → gold p50 / p95 / p99 216 / 328 / 364 s: the items now wait in split's queue instead
    of at the intake, since landing is ~5 times faster than split.
  - **Split is the limit**, at 112.5 items/min (1.9/s) with its 2 pods: an item took 2.4 s (split 1.8 s
    of it, 4 at once on 0.5 CPU). Its batches went to `split_records` in 530 commits for 1,036 batches,
    about 2 a commit (at most 4 a pod at once), at 0.59 s each against 1.04 s in step 1. Convert (3 pods)
    and transform (4) kept up.
  - **A failover now announces a whole group again.** The standby, recovering, sent the last commit's
    `item.landed` again for each of its items: 507 at once (1,520 messages for 1,020 items), where one item
    was sent before. Split found each already split and only announced it again (1,553 lookups for 1,020
    items), but at ~0.5 s a lookup that is about 4 minutes of split pods' time. Worth fixing: a takeover
    could announce only the items the ledger lacks, and leave a message the broker lost to split's
    reconciler, as it already does for earlier items. **Fixed (DESIGN.md revision (df)) another way:** the
    intake records a group as announced once its messages are acknowledged, and recovery sends a group again
    only if that record is missing, so a lost message is still sent at once. Not yet measured.
  - **A slow tail:** the last 7 items took from 407 s to 544 s to split; not yet explained.
  - KMS: 7,676 signatures, 7.5 a document (more with the repeats), a ceiling of 39 documents/s (step 3).

- **Step 3, pod signing keys, and split taking several batches at once, AWS, 2026-11-14 (DESIGN.md
  revisions (dg), (dh)).** Split: 2 pods, each with 8 Kafka consumers of 2 items (16 at once). Two runs on
  a reset pipeline, gold check skipped; every item landed exactly once and reached gold in both.
  - **Run A, the same 1,020 documents with the landing failover** (`load-20261114T053412Z`): 1.87
    documents/s, no better than step 2. Split ran at ~3.5 items/s from 92 s to 231 s, then fell to ~1.2:
    the failover's standby announced again the last group (290 items, 1,310 `item.landed` for 1,020 items),
    whose holder was deleted before it recorded the group as announced, as revision (df) allows. Split took
    each again, and each one read `split_records` for failed pieces (`failed_rows`, p95 23 s, against 0 for
    an item split here): an item split by another pod, or before a restart, wasn't known clean. **Fixed
    (DESIGN.md revision (di)):** the checkpoint notes an item split whole with none failed, so its end sent
    again reads nothing. Not yet measured.
  - **Run B, 2,400 documents, no failover** (`load-20261114T055327Z`): **4.50 documents/s** first landed to
    last in gold (1.84 in step 2), flat from the third tenth of the run (~5.6–6.9 a second to gold). Landed
    → gold p50 / p95 / p99 214 / 370 / 418 s, most of it waiting in split's queue (lag up to 1,718).
  - **KMS is no longer a limit:** 3 pod statements signed in KMS (in the pods still running) for 16,010
    messages, against one Sign per message. Signing each in KMS would have capped this run at 44
    documents/s. Estimated cost $27 per million documents ($181 in step 2), compute only.
  - **Split is still the limit**, at ~2.8 items/s a pod: an item takes 4.5 s (lookup 0.8 s, split 3.2 s of
    which the commit, with its wait, 1.1 s). Its 2,420 batches went to `split_records` in 950 commits, ~2.5
    a commit: one committer a pod at about 1 s a commit gathers only what arrives meanwhile. Next: fewer
    remote calls per item (the lookup, the checkpoint's writes), more items in flight, and a faster commit
    (§2.2: `metadata.json` grows with the snapshots kept). Convert (3 pods) and transform (4) kept up.
  - The report's KMS figure counted messages, which since step 3 aren't KMS calls; `measure.sh` now
    counts the pod statements logged.

- **Split's steps timed, and one read of an item's file, AWS, 2026-11-14 (DESIGN.md revisions (di),
  (dj)).** The same 2,400 documents without a failover, on a reset pipeline, gold check skipped; every
  item landed exactly once and reached gold.
  - **With each step timed** (`load-20261114T153300Z`): 4.13 documents/s, as run B. Of an item's 4.7 s in
    split, 1.6 s went before its first record, a second open of its landed file for the chunk after the
    first for the summary row (`lookup_row`, 0.84 s); the commit 1.2 s (0.46 s queued, 0.70 s a group
    commit, 999 commits for 2,426 items); the rest under 0.2 s a step. The landed table's refresh costs
    nothing. Keeping the unwrapped landing keys (`UnwrapCache`) changed nothing measurable. Every step was
    3–4 times slower in the first quarter than the last, as the load fell: contention, not the code. (A run
    of 1,020 with the failover, before the finer timers: 3.10 documents/s, no repeats read, revision (di)
    working.)
  - **Reading an item of one chunk with its summary row** (`load-20261114T165102Z`): **5.12 documents/s**
    (4.13), ~6.5–10.7 a second to gold from the second tenth to the eighth. Split per item 2.9 s (4.7):
    before the first record 0.005 s (1.6). Landed → split ended p50 107 s (191); landed → gold p50 / p95 /
    p99 187 / 292 / 344 s (246 / 391 / 448). The queues moved on: split ended → convert p50 15 s (11),
    convert → transform 63 s (37).
  - **What the pods got** (cores used / share of the time a thread waited for a CPU / share in GC, by
    quarter): split 1.23 / 0.55 / 0.02 and 1.71 / 0.80 / 0.04 in the first two quarters, falling to 0.84 /
    0.19 as the load fell; transform up to 3.58 cores; convert up to 0.99. **The pods are short of CPU:** split
    requests 0.5 CPU a pod and uses up to ~0.9, transform and convert likewise, so they share the nodes'
    CPUs and wait for them; GC isn't the cause. The steps that slow under load (the summary read, the
    commit) slow with the waiting.
  - **What sets the pace now:** split's commit (1.1 s an item: 0.48 s queued, 0.65 s a commit, 750
    commits for 2,422 items) and summary read (0.85 s), both on CPU-starved pods; then transform. Next:
    CPU requests that match what the stages use, so the scheduler and the cluster autoscaler give them
    the CPUs, then measure again; then a faster commit (§2.2).
  - **With CPU requests that match** (`load-20261114T232226Z`; split 0.9 a pod, transform 0.8, convert
    0.4, up to three burst nodes, all three used): **5.51 documents/s** (5.12). Split's share of time
    waiting for a CPU 0.25–0.32 (0.55–0.80); convert's and transform's under 0.22. Split per item 2.2 s
    (2.9): the summary read 0.56 s (0.85), now flat across the run, so one open's S3 round trips (footer,
    offset index, pages), not contention; a group commit 0.49 s (0.65). Landed → split ended p50 57.7 s
    (107); landed → gold p50 / p95 / p99 166 / 293 / 333 s.
  - **Split still sets the pace**: ~7.6 items/s while landing ran, ~4.4 after, on its 2 pods (KEDA's
    most); transform finished ~30 s after it. In the second quarter split's commits queued 1.4 s an item
    (0.2 s in the others) while transform used 3.5 cores; the catalog's Postgres shares a base node with
    Kafka, not yet measured. Next: a third split pod (3 × 8 consumers fit the 24 partitions), the summary
    read in fewer GETs (the footer and offset index in one ranged read), and the catalog's database
    measured.
  - **The summary read in two requests, a third split pod** (`load-20261115T023814Z`; DESIGN.md
    revision (dk)): **6.81 documents/s** (5.51). A data file is read through `RangedInputFile`: its
    last 128 KB (footer and page indexes) in one closed-range GET, kept, and pages 256 KB at a time from
    where a read starts, so one record is two requests (eleven with each range asked for exactly). The
    summary read (`lookup_row`) 0.19 s (0.56); split per item 2.4 s, of which appending 1.6 s (the
    commit queued 0.85 s, a group commit 0.75 s, 587 commits for 2,365 items). Split, on 3 pods, ended
    at 260 s, landing at 225 s; transform, on its 4, at 379 s: **transform now sets the pace**
    (convert's last → transform's first record p50 74 s; transform 3.6 cores in the second quarter).
    Landed → gold p50 / p95 / p99 125 / 216 / 261 s (166 / 293 / 333). Six nodes, four of them burst.
  - **A deadlock found on the way** (`load-20261115T012749Z`, nothing split): the first page a JVM
    decompresses makes Hadoop initialize `JobConf`, which takes each `Configuration`'s lock. Split's
    items run on virtual threads; one initializing a class, and each waiting for it, holds its carrier,
    and a two-CPU pod has two. Two items reading at once held both while the thread holding a lock one
    needed had none to run on: the pod never moved again, and its lease renewals failed behind it. The
    faster read made it likely (a batch of items reaches the codec together). `DataFileRows` now loads
    the codecs once on a platform thread before any read (`DataFileRowsThreadsTest`: 32 virtual threads
    in a new JVM with two carriers; stuck before, not after). Next: transform's throughput, then split's
    commit (§2.2), then the catalog's database.
  - **Transform tuned, weighed on cost** (2026-11-15). Transform is CPU-bound: ~0.36 CPU-seconds a record
    (877 core-seconds for 2,433 records); at 2 records a pod each pod used ~0.9 of its 2 threads' CPUs,
    idle while a batch committed and while its slowest record ended. Two settings at the same 6.4 CPU,
    one after the other on one stack (B on the nodes A left), burst nodes up to 6. Cost here is
    node-seconds from t0 to the last record in gold, at the on-demand price ($0.0672 an hour), not
    `measure.sh`'s peak nodes for the whole run:

    | Setting | docs/s | split / transform ended | landed → gold p50 / p99 | mean nodes | $ / M docs |
    |---|---|---|---|---|---|
    | 4 pods × 2 records, 0.8 CPU, 1.5 GiB (`load-20261115T023814Z`) | 6.81 | 260 / 379 s | 125 / 261 s | 6.0 | 18 |
    | **A: 4 pods × 4 records, 1.6 CPU, 2 GiB** | **8.37** | 297 / 297 s | 72 / 141 s | 8.8 | 20 |
    | B: 8 pods × 2 records, 0.8 CPU, 1.5 GiB | 7.70 | 281 / 340 s | 92 / 184 s | 8.0 | 21 |

    **A is chosen**: transform ends with split, so it no longer sets the pace, for 23% more throughput at
    ~11% more a document; B is slower and dearer (8 JVMs, each with its own commit and memory). A's pods
    used ~1.1 of their 1.6 CPUs, waiting for one 38% of the time in the second quarter (four threads on a
    two-vCPU node); split waited 57% in the first, sharing nodes with them. Peak sampled nodes 10 (8 at
    the end, six of them burst). At rest transform's one pod (1.6) doesn't fit on the base nodes beside
    the rest, so one burst node stays up with the stack (AWS-POC.md §5). Next: split sets the pace again
    (its commit, §2.2); then a record stage taking several batches at once, as split does (revision (dh)),
    so a transform pod's CPUs work while it commits.
  - **Split's commit as one fast append, metadata gzipped** (`load-20261115T060601Z`; DESIGN.md revision
    (dl)). Locally (a JDBC catalog, 600 commits of 4 files): a commit read the table's metadata file and two
    manifest lists, 4.7 reads with three committers; as a plain fast append 2, and 2.6. The metadata file
    lists every snapshot, ~830 bytes each (499 KB at 600); gzipped 55 KB. A shorter retry wait
    (`commit.retry.min-wait-ms` 10 ms) made more conflicts, so Iceberg's 100 ms stays. On AWS: **no faster.**
    A group commit 0.61 s (0.62 in run A), queued 0.60 s (0.62), split 1.85 s an item (1.87); 563 commits
    for 2,400 items. Iceberg's commit reports (in the pods still running: 309 commits) show why: **31% of
    commits conflicted** with another pod's, 1.48 attempts each, and each attempt is a refresh, the parent's
    manifest list, two PUTs, the metadata PUT and the catalog's compare-and-swap, plus the 100 ms wait;
    0.38 s a commit in the first quarter, 0.75 s in the next two as the three pods' commits overlapped. One
    read fewer and a ninth of the bytes don't move that. 8.13 documents/s (8.37), within the runs' spread:
    split ended at 245 s (297), landing at 214 s; transform at 303 s; landed → gold p50 / p99 94 / 183 s;
    8 nodes at most, ~$18 per million documents by node-seconds. Kept: fewer reads and smaller metadata
    files at no cost, and the metadata file grows with the snapshots kept (§2.2), so gzip matters more
    with time. Next: the conflicts, by one committer for a table rather than one a pod (§3.7).
  - **One queue for split's commits** (`load-20261115T071129Z`, without a failover, and
    `load-20261115T072550Z`, with the landing failover at 30% and a split pod deleted at 800 split, then the
    gold check; DESIGN.md revision (dm), §3.7). **The conflicts are gone:** 5 of 335 commits retried
    (1.02 attempts) and 0 of 140 in the second run, against 97 of 309 (1.48). A group commit 0.49 s (0.61),
    of it 0.28 s the fast append for everyone and 0.06 s the pod's own manifest; queued 0.27 s (0.60), the
    turn at the lock 0.18–0.23 s; 506 and 497 commits (563). Split 1.38 and 1.36 s an item (1.85), p95
    2.46 s (4.75): appending 0.80 s (1.27). Less than §3.7 expected (0.35 s a commit), as a commit for
    everyone still refreshes, and reads the leases and the queue. **No faster end to end**: 6.91 and
    8.23 documents/s (8.13). Split no longer sets the pace: it ended within 30 s of landing in the first
    run, with landing in the second; landing landed its last at 234 s and 287 s (214 s), and transform took
    88 s and 30 s after split (58 s). Landed → split ended p99 72 and 71 s (85). Both runs exactly once,
    none left behind, the gold check passed; the deleted split pod's items were taken up by the others.
    8 nodes at most, no new pods; measure.sh's $34 and $31 per million documents (30) follow the runs'
    length, not the change. (The second run's transform reported 0 junk messages rejected, of 3: the count
    is read from the pods still running, and transform had scaled to one.) Kept, on by default: split's
    commits now stay flat as pods are added, which §1's rate needs. Next: landing (step 4), and transform's
    pace (a record stage taking several batches at once); convert's and transform's tables through the
    same queue once they conflict.
  - **Transform clearing a backlog, by pod count** (`load-20261115T170919Z`, `T175213Z` and `T182821Z`:
    `SDH_LOAD_BACKLOG` at 2, 4 and 6 pods, a run each on a freshly reset pipeline, 7,200 documents, one
    record each, no failover). Transform is held at no pods while the load lands, splits and converts, then
    released; records mapped come from Prometheus' count of transform's process step per pod, as the
    registry counts a record only once its item's total has come too. Every run exactly once, none left
    behind; gold's commits 3 of 131, 6 of 149 and 0 of 42 retried.

    | Pods | Ready (s) | Records/s | A pod's | Least / most a pod mapped | Cores | CPU-s a record | Batch (s) | Clear (s) |
    |---|---|---|---|---|---|---|---|---|
    | 2 | 101 | 8.6 | 4.3 | 453 / 3,385 | 2.67 | 0.31 | 8.5 | 1,094 |
    | 4 | 101 | 14.1 | 3.5 | 802 / 2,220 | 4.56 | 0.32 | 9.3 | 696 |
    | 6 | 5 after 310 | (9.3, the tail) | — | 0 / 1,173 | 3.02 | 0.32 | 12.4 | 729 |

    **A record costs 0.31–0.32 CPU-seconds at every pod count**, and gold's commits barely conflict, so the
    pods don't slow each other: a pod with work maps 4.3–5.0 records a second (the busiest pods: 3,385 in
    780 s, 2,220 in 443 s) on 1.6 CPU, using 1.1–1.3 cores of it; a thread waits for a CPU 0.31–0.39 of the
    time, and the CPU sits idle while each batch of ~47 records commits, registers and sends. **The clear
    time stopped falling at 4 pods, because of the test's data, not transform:** a record's id is the keyed
    hash of its content (`RecordIds`), and is its Kafka key, and the comment that made each copy unique
    is dropped by the convert stage's JSON, so the 240 copies of each of the 30 test documents all went
    to one partition of 24. The pods' shares differed up to 7.5 times, and the last
    tenth took 94–183 s on the few pods holding the heaviest partitions. At 6 pods the sixth waited over
    300 s for a burst node, and its window opened in that tail, so its row isn't a rate. Real documents
    differ, and spread over the partitions evenly. Landed → gold p99 1,337, 1,004 and 1,005 s: the hold, not
    the stages.

    **For 100 documents/s** (§1), at ~4.5 records a second a pod: ~23 transform pods, 25 with headroom, one
    a 2-vCPU node (1.6 CPU), so ~25 nodes for transform alone; `.records`' 24 partitions cap transform at
    24 pods, so it needs more (48). At the on-demand t4g.large's $0.0672 an hour, 25 nodes are $1.68 an hour
    for 360,000 documents, ~$4.70 per million (Spot less); the 15M load, ~42 hours at that rate, ~$70 of
    transform nodes. measure.sh's $38, $33 and $44 per million documents count the whole cluster at its
    peak for these short runs, mostly idle, and aren't comparable. Next: a pod taking its next batch while
    one commits (as split's revision (dh)), to use the idle CPU (up to 5 records a second a pod, 1.6 CPU at
    0.32 CPU-s a record: ~21 pods); 48 partitions; and copies made distinct in the load test (a different comment per copy), so its
    records spread as real ones do.
  - **Transform taking two batches at once** (`load-20261115T222406Z`: `SDH_LOAD_BACKLOG=4`, as `T175213Z`
    above, with `sdh.stage.consumers: 2`, DESIGN.md revision (dn)). Exactly once, none left behind; gold's
    commits 2 of 44 retried.

    | Consumers a pod | Records/s | A pod's | Cores | CPU-s a record | Wait | Record (s) | Batch (s) | Clear (s) |
    |---|---|---|---|---|---|---|---|---|
    | 1 (`T175213Z`) | 14.1 | 3.5 | 4.56 | 0.32 | 0.39 | 0.53 | 9.3 | 696 |
    | 2 | 18.3 | 4.6 | 6.36 | 0.35 | 0.41 | 0.54 | 13.4 | 555 |

    **29% more records a second on the same 4 pods and nodes, and the pods now use all their CPU** (6.36
    cores of 6.4). A record's mapping takes as long as before (0.54 s), so the second batch costs the first
    nothing it can feel; a record costs a little more CPU (0.35 CPU-s, from 0.32), and a batch's commit and
    register a little longer (0.53 and 0.42 s, from 0.44 and 0.27), as two batches share the pod. The clear
    took 20% less time; it still ends in the skewed tail of the test's shared keys. A pod's rate is now
    its CPU's (1.6 ÷ 0.35 ≈ 4.6 a second), so more consumers wouldn't help; a faster mapping or more CPU a
    pod would. Memory wasn't sampled (the cluster has no metrics API), but no transform pod restarted.

    **For 100 documents/s** at 4.6 a pod: ~22 transform pods, 24 with headroom (from 25), ~24 nodes, ~$4.50
    per million documents of transform nodes on-demand (from ~$4.70); with 2 consumers each, 48 partitions.
    measure.sh's $38 per million counts the whole cluster at its peak, as above.
  - **Transform with its records spread evenly** (`load-20261116T004622Z`, a long test: as `T222406Z`, with
    48 partitions a stage input on AWS, `STAGE_PARTITIONS`, and the load test's copies made distinct by an
    element of its own namespace, which the convert stage keeps and the mapping doesn't read: each of the
    30 documents' bundle or failure unchanged, checked locally). Exactly once, none left behind; gold's
    commits 5 of 44 retried.

    | Run | Records/s | A pod's | Least / most a pod mapped | Cores | CPU-s a record | Wait | Batch (s) | Clear (s) |
    |---|---|---|---|---|---|---|---|---|
    | 24 partitions, shared keys (`T222406Z`) | 18.3 | 4.6 | 193 / 2,058 | 6.36 | 0.35 | 0.41 | 13.4 | 555 |
    | 48 partitions, distinct copies | 21.2 | 5.3 | 1,295 / 1,643 | 6.32 | 0.30 | 0.62 | 13.7 | 494 |

    **The pods now share the work evenly** (their counts within 27% of each other, from 10 times), so all
    four are fed to the end, and the backlog cleared in 494 s. A record costs 0.30 CPU-seconds and mapping
    one 0.50 s (0.54 before): the earlier runs' busiest pods mapped the same few heavy documents over and
    over. So a pod maps 5.3 records a second on its 1.6 CPU, all of it used; a thread waits for a CPU 0.62
    of the time, and that, not consumers, is the limit.

    **For 100 documents/s** at 5.3 a pod: ~19 transform pods, 21 with headroom, ~21 nodes, ~$3.90 per million
    documents of transform nodes on-demand (from ~$4.50); 15M documents ~42 hours, ~$59 of transform
    nodes. 21 pods × 2 consumers fit the 48 partitions.
  - **Where a transform record's CPU goes** (2026-11-16, locally: the 30 test documents mapped by
    `MemoryProbe`, 4 at a time, 1 GiB heap, Java Flight Recorder for 60 s, 19,428 samples; a scratch probe
    timing each document alone). On AWS the mapping is ~70% of a record's 0.30 CPU-seconds: 0.54 s of a
    record's wall time, 4 at once on 1.6 CPU, against a pod's 0.19 s a record (§10 above; the read 0.11 s,
    commit, register and seal per batch the rest). Locally it is 0.08–0.10 CPU-s a record, so a Graviton
    core does the same work ~2.5 times slower than this x86 one. The cost **grows linearly with a record's
    size**, 2.5–4 ms of CPU per KB of its JSON for those that map (387 ms for the largest, 154 KB): no
    mapping is quadratic, and records that fail a check stop early (1–200 ms). **95% of the samples are the
    Whistle runtime calling functions**, and no one line takes more than 5%; the runtime's own costs, by
    the samples in each (self time unless said):
    - choosing an overload on every call: `DefaultOverloadSelector.select` and `distance` with the lookup
      in `OverloadLookupCache`, ~17% with what they call, though the result is the same for a call site
      given the same kinds of arguments;
    - `DefaultClosure.create`, 8%: a new `FunctionReference` and argument array for each call;
    - `WhistleFunction.builtinSink` and `shortCircuit`, ~10%: the target registry and the argument
      modifiers looked up again for each call, though the call's proto fixes them;
    - `Container.isNullOrEmpty`, ~5%: it copies a container's field names into a sorted set
      (`DefaultContainer.fields`) to see whether it has any;
    - variables looked up through `HashMap`s in each stack frame, ~5%; reading the record's JSON, ~4%.

    So the runtime, not the US Core set, is where to save. The estimate before building was ~25–30% off the
    mapping (~20% off a record).

    **Built and measured** (2026-11-16, in healthcare-data-harmonization's runtime):
    - emptiness answered without a copy (`DefaultContainer.isNullOrEmpty`);
    - a variadic overload's distance worked out in place, with no streams or copies;
    - a single overload that takes any `Data` of the call's number taken without scoring;
    - a function with no argument modifiers skipping `shortCircuit` and the modifier binding;
    - one package's overloads returned as a read-only view rather than a union.

    Caching each call site's overloads was tried twice, and dropped both times:
    - a hand-written lookup key was slower than the record it replaced;
    - a map keyed by the compiled `FunctionCall` (2026-11-16, after the above) was 3% slower than the
      record. Equal calls at different sites made it compare protos whole. Keyed by identity it was no
      faster, because the map read itself, over thousands of sites, took ~5.6% of samples, as much as the
      lookup it replaced.

    The overload work left (~7% finding, ~7–8% choosing) would only go if each call kept its overload
    itself: the runtime would have to evaluate a compiled tree rather than the protos, a larger change. Locally, one document at a time over the 30 took ~18% less CPU
    (2,322–2,408 ms to 1,939–2,030 ms); `MemoryProbe`, 4 at once, took ~12% less a record (38.5 to
    43.0–44.0 records/s on the same CPU).

    On AWS, the backlog test (load-20261116T221309Z, against load-20261116T004622Z) gave:
    - 23.1 records/s at 4 pods (21.2), 5.78 a pod (5.3);
    - ~0.25 CPU-s a record (0.30): 5.83 cores for 23.1 records/s, ~16% less;
    - the 7,200-record backlog cleared in 403 s (494);
    - exactly once, every item in gold, and the gold check passed (its Spot node was taken three times; the
      Job's replacement pods carried it through).

    The pods still waited for a CPU 54% of the time on the burst nodes. At 5.8 records/s a pod, 100
    documents/s needs ~18 transform pods (21 before). Separately, the burst pool mixes Graviton2 (t4g, m6g),
    3 (m7g) and 4 (m8g) nodes; the later ones are faster a core, and a t4g node at full CPU spends credits
    beyond its baseline, billed on top in unlimited mode (EC2's default for T instances): neither is in
    `measure.sh`'s cost.

  - **Transform's CPU wait, and the node types** (2026-11-16, two backlog runs in one session, 4 pods, Spot
    burst nodes: A load-20261117T004602Z, B load-20261117T011227Z).
    - **The wait figure is not lost CPU.** It is the cgroup's `cpu.pressure` "some": the share of time at
      least one of a pod's threads was ready and waiting. With 4 mapping threads and 2 consumers on a 2-vCPU
      node it is high whenever the pod is busy, so cores used against the node's 2 is the figure to read.
    - **The backlog report now has a line a pod:** its node's pool, instance type and capacity type, its
      records a second, cores, and wait (`load-test.sh`).
    - **Run A, four records at once:** 20.6 records/s (23.1 in the run before, on another mix of nodes). By
      pod: a t4g.large at 6.1 records/s on 1.91 cores; two m7gd.large (Graviton3) at 5.9 on 1.27 cores and
      3.7 on 0.76; one more at 4.9 on 0.98. So four records at once left the faster nodes a third to half
      idle, and a Graviton3 core maps ~1.5 times as many records as a t4g's Graviton2 core (4.6–4.8 records
      a core-second against 3.2).
    - **Run B, six records at once** (3 GiB, the same 1.6 CPU request): **28.6 records/s, 7.15 a pod (39%
      more than A)**; the pods used 1.26–1.94 cores. The backlog cleared in 371 s (426 in A), at $19 per
      million documents (A $23). Every item reached gold exactly once and the gold check passed. All four
      pods were on m7gd.large: records a core-second (4.4–5.0) were as in A, so the gain is the idle CPU put
      to work, not the nodes. At 3 GiB no transform pod fitted on a base node.
    - **Adopted:** six records at once and 3 GiB (`transform-application.yml`, `transform-patch.yaml`). At
      7.15 records/s a pod, 100 documents/s needs ~14 transform pods (18 before).
    - **t4g surplus credits** are now in `measure.sh`'s cost (`node-cpu.sh`: each node's CPU since it started,
      from the kubelet; above 0.6 vCPU a t4g.large, at $0.04 a vCPU-hour), $0.2–0.8 per million documents in
      these runs. The deployer can't read CloudWatch's credit metrics, so this is worked out, not read.
    - **The instance types:** a busy t4g burst node is the dearest per record. At ~1.9 cores it spends ~1.3
      vCPU above its baseline, ~$0.05 an hour on top of its Spot price (~$0.02), for 2/3 of a Graviton3 core's
      work. m6g/m6gd (Graviton2, ~$0.023–0.027 Spot on 2026-11-16) cost less for the same speed; m7gd
      (~$0.027–0.037) does 1.5 times as much. `measure.sh` still prices every node as an on-demand t4g.large.
      **Dropped** from the burst group the same day: five types now (`burst_instance_types`), m6g, m6gd, m7g,
      m7gd and m8g, in three zones. The base nodes stay t4g.large on demand, where Postgres, Kafka and landing
      run below or near the baseline. The first run with the five (C, below) got all four pods Spot m7gd.
    - **Eight at once (run C, load-20261117T030928Z, 4 GiB):** no gain. 28.7 records a second at 4 pods,
      7.18 a pod (six: 28.6, 7.15); 1.19–1.82 cores a pod (six: 1.26–1.94), CPU wait 0.71; backlog clear in
      358 s (371); $20 per million documents (19). Exactly once, nothing left behind, gold check passed.
      Six already fills the node's CPU; two more threads only queue for it, for 1 GiB more. **Six kept.**
- **The first steady feed (2026-11-17, load-20261117T041926Z): it broke down.** 180,000 documents (6,000 copies of
  each of the 30) fed at 100 a second for 30 minutes (`SDH_LOAD_RATE`, staged first with no session up:
  `SDH_LOAD_STAGE_ONLY`), with the maximums raised for the run: 24 burst nodes, splitter 6 (48 partitions, 8
  consumers a pod), convert 12, transform 15. Gold check off. What it showed:
    - **The first five minutes:** landing ~45 items a second, split ~24 with 5–6 pods, as with 3: split doesn't
      scale with pods, so its limit is shared (its commits to `split_records` go through the cross-pod queue, ~1.7
      s each). Convert and transform kept up with split.
    - **From ~11 minutes on:** the landing pods kept dropping to none ready, and the report's registry reads (psql
      in postgres-0) failed again and again. From ~35 minutes split mapped about 1 item a second, ~44 s an item
      against 4 (waiting, not CPU: 6 pods used ~1 core), and landing took nothing in for an hour, then the rest at
      ~45 a second once transform and convert had scaled down. At the 150-minute timeout 14,278 had reached gold;
      the 113 and 122 items "left behind" were still in flight, so the run says nothing about loss.
    - **Most likely cause, not proven:** Postgres. One pod, a 512 MiB limit, 100m CPU requested, max_connections
      200; 35 stage pods with a lease pool (2), Iceberg's JDBC catalogs and the commit pool each come to around
      200 connections, and 200 backends don't fit in 512 MiB. Pod restarts, OOM kills and the connection count
      weren't recorded, and the pods' logs went with the cluster at `down`.
    - **Nodes:** up to 31 burst nodes Ready against a maximum of 24, most likely Spot replacements started before
      the old ones went; not checked (the deployer can't read CloudTrail).
    - **Cost:** ~3.3 hours up. `measure.sh` (every node priced as on-demand t4g) says $5.81 compute; at Spot prices
      nearer $2–3, plus ~$2–3 of S3 requests for 360,000 PUT and copy requests.
- **The steady feed again, Postgres at 2 GiB (2026-11-17, load-20261117T130702Z): Postgres held.** 45,000
  documents at 50 a second for 15 minutes, the same maximums, Postgres given 2 GiB and 500m CPU
  (`k8s/infra/state.yaml`; 512 MiB and 100m before). `load-test.sh` now saves Postgres' connections by role and
  Kubernetes' Warning events every 15 s, and reports containers restarted in the run.
    - **Postgres:** answered all 60 reads, and up to **188 connections of its 200** (transform 74 at 15 pods, ~5 a
      pod; splitter 53 at 6, ~9; convert 46 at 12, ~4; landing 5). With 512 MiB that many backends had nowhere to
      go, which fits the first run's breakdown; no Postgres or landing event this time. 188 of 200 leaves no room
      for more pods: max_connections, or the pools, must change before scaling further.
    - **Rates, to 891 s:** landing kept up with the feed (42,401 by 891 s, ~48 a second, the intake at ~500
      objects); split ~31 items a second with 6 pods, its lag growing (18,809 at the end); convert as split;
      transform ~28. **Split is the limit**, at 6 pods by the partitions (48, 8 consumers a pod).
    - **Nodes:** 24 burst nodes reached; transform, convert and splitter pods then waited for CPU
      (FailedScheduling). A few nodes shut down in the run (NodeShutdown).
    - **The run stopped at 891 s**, without its report: this machine lost its connection to S3 for about a
      minute, and 15 slices' copies failed. `load-test.sh` now tries a slice 3 times before giving up.
- **Split at 4 items a consumer (2026-11-17, load-20261117T145619Z): split no longer the limit.** 36,000
  documents at 60 a second for 10 minutes, the same maximums, split's `concurrency` 4 (32 items at once a pod, 16
  before), Postgres max_connections 300.
    - **Split kept up with landing:** both ended at 640 s; ~70 items a second at 6 pods while it caught up (~31
      with 2 at once). An item 1.7–2.7 s (~4 before): its append 1.1 s (the commit queued 0.35 s, a group commit
      0.7 s). Split now uses 4.0–4.6 cores over its 6 pods, near their 0.9 CPU requests (waiting 0.27–0.33).
      Twice as many at once halved an item's time, rather than doubling it: commits took more groups each.
    - **To gold:** ~67–74 documents a second in the middle tenths (4,000–4,400 a minute), 39.5 overall from
      first landed to last in gold (the tail is transform clearing its queue). Transform was last (930 s):
      some of its 15 pods waited for nodes (FailedScheduling; burst nodes at their 24). Landed → gold p50 / p99
      104 / 653 s.
    - **Postgres:** up to 185 connections of 300, every read answered, no container restarted.
    - **Landing landed 2 objects twice** (36,002 items for 36,000 objects; split noted both as copies, and they
      went on to gold as resends). Most likely the feed's, not landing's: an object written again is a new
      version of it (`ListedItem.versionText`: the time changes even when the bytes don't), and landing keeps
      both, as it must (nothing dropped or replaced). The AWS CLI retries a copy whose answer was lost by
      itself, unseen, and this machine's link to S3 dropped for a minute earlier that day. For one object
      version to be landed twice, landing would need two nodes landing at once, which its fenced commits stop,
      or a restart, and neither pod restarted. The ledger, which would show whether the two items had the same
      source version, can't be read from here: the bronze data's KMS key is denied to the deployer, by design.
      Found on the way: a slice that failed part-way was copied again in full, rewriting objects that may have
      been landed already. A retry now copies only the objects that failed, and the report counts them. The
      first steady feed's 500 extra were messages, not items, and its landing pod restarted, so they were
      probably the last group's announcements sent again at each restart (§3.6 point 1). That run's items
      can't be counted now.
    - **Cost:** `measure.sh` says $17 per million documents (on-demand t4g prices, 29 nodes at most).
  **Adopted:** split `concurrency: 4` (`splitter-application.yml`). Its KEDA maximum stays 3 (96 items at once,
  ~45 a second by this run) until the stages' maximums are set for 100 documents a second.
- **100 documents a second for 15 minutes, every stage scaled out** (2026-11-17, load-20261117T200747Z): 90,000
  documents, split 6 pods, convert 12, transform 18, burst nodes up to 30.
    - **Landing kept nothing twice:** 90,000 items for 90,000 objects, and the feed copied nothing again.
    - **Landing held at ~54 items a second** (one pod), and the intake's backlog grew to 40,000. A group of ~480
      took 9.1 s: reads 3.6 s and deletes 2.6 s, 8 at a time (S3 round trips), messages 1.6 s, sealing 1.3 s.
    - **Transform held at ~37–49 items a second** (~2 records an item), on 14 of its 29 requested cores. Not CPU:
      a batch's commit to the gold table took 4.1 s on average (p95 13.8), with 18 pods taking their turns on one
      table, as split's appends did before its shared commit queue (§10, revision dm).
    - Split ~55–60 items a second, ~40 late in the run (1.7–2.8 s an item). Postgres up to 201 of 300
      connections; no container restarted. Burst nodes counted up to 33 against a maximum of 30 (Spot
      replacements, probably); transform and convert pods waited for nodes at times.
    - 39.2 documents a second first landed to last in gold; landed → gold p50 / p99 170 / 1,071 s, from the
      backlog. 25 items hadn't reached gold when the test stopped waiting: 4 still splitting, 5 not ended at
      convert, 16 not ended at transform. Nodes were shut down in the run, so probably pods lost with them,
      waiting to be taken up again; not checked. $19 per million documents.
- **35 a second for 10 minutes, the latency floor** (2026-11-17, load-20261117T205936Z): stopped at ~590 s, when
  this machine's link to S3 dropped for longer than the feed's retries (3 tries, 10 s apart), so no report.
  Until then landing and split kept up with the feed, and transform ran ~2,500–3,000 items behind all along:
  gold ~80 s behind even with no backlog upstream. The 20 s p99 needs transform's gold commit fixed first.
  **Changed after them:** landing reads, deletes and seals 32 at once (`landing-application.yml`), and the feed
  retries a slice for 2 minutes (7 tries, 20 s apart).
- **100 documents a second again, landing 32 at once** (2026-11-17, load-20261117T230333Z): 90,000 documents,
  the same stage maximums.
    - **Landing kept up:** 92 items a second, all 90,000 landed by 980 s (54 a second before); one item
      announced twice (a failover's re-announce, §3.6 point 1).
    - **Split became the pace, at 19 items a second** (41 before). Its append took 1.2 s an item in the first
      quarter, 4.9 s in the second and 2.0 s in the third; its whole step 1.6, 7.0, 3.4 s (1.4–2.3 s in the run
      before). In the same quarters split resumed items left by other pods (1.9 s an item), splitter pods fell
      from 6 to 4 at ~626 s, nodes were shut down, and the autoscaler failed to add 7 burst nodes
      (`OutOfResources`).
    - **The cause was Spot churn, not split** (the burst group's scaling activities, read after the run):
      in the second quarter 24 burst nodes were replaced, 14 on EC2 rebalance recommendations (EKS managed
      Spot groups rebalance capacity) and 10 to balance the group's zones, while new Spot launches kept
      failing (`UnfulfillableCapacity`); the third quarter had 2. Each took the pods on it, so their items
      were resumed by other pods and the commit queue waited for the lost holder. With the nodes steady
      (442–980 s, all 6 split pods up) split did 43 items a second, as in the run before (41): 6 split pods
      are ~43 items a second, so 100 needs ~14, or a faster pod, whatever the nodes do.
    - For runs that measure the pipeline rather than Spot, `burst_capacity_type = "ON_DEMAND"`
      (`compute/variables.tf`), at about 2.5 times the burst price; or more instance types in the burst
      group, so a replacement finds capacity.
    - 19.4 documents a second first landed to last in gold (39.2), landed → gold p50 / p99 1,040 / 3,973 s.
      14 items not ended at transform when the test stopped waiting. $45 per million documents (19); 38 nodes
      at most.
- **35 a second for 10 minutes, the latency floor, again** (2026-11-17, load-20261117T235454Z): 21,000
  documents; the feed's longer retries got it through.
    - Landing and split kept up with the feed: every item landed exactly once (33 a second, by 631 s), and
      landed → split ended p50 / p99 9 / 89 s.
    - **Transform set the latency:** convert's last record → transform's first p50 / p95 / p99 23 / 254 / 367 s,
      so landed → gold p50 / p99 56 / 385 s (target 20 s p99). 0 left behind; 27.4 documents a second first
      landed to last in gold. Up to 47 nodes for 35 a second, from the stages scaling out on the queue's lag
      and staying out: $39 per million documents.
    - The 20 s p99 needs transform's gold commit fixed first (as above), and the stages' scale-out tuned
      for the cost.
  **Adopted:** landing 32 at once, and the feed's 2-minute retries.
- **100 documents a second, transform's gold commit through the shared queue** (2026-11-18,
  load-20261118T035717Z; DESIGN.md (ds), §3.7): 90,000 documents, the same stage maximums (6 / 12 / 18),
  burst nodes still Spot.
    - **65.1 documents a second** first landed to last in gold (39.2 in run 37, 19.4 in run 39), every item
      landed exactly once and 0 left behind. $11 per million documents (19, 45); 33 nodes at most.
    - Transform's commits waited their turn 0.46 s on average (p95 0.95) and combined in 0.35 s, against
      4.1 s (p95 13.8) for each pod's own commit before. Gold took 2,963 snapshots.
    - Landing 94 items a second (all landed by 957 s); split 65 items a second on 6 pods (41–43 before), its
      step 1.5–1.75 s an item in every quarter.
    - **Split is now the pace:** landed → split ended p50 / p99 249 / 437 s, while convert's last record →
      transform's first was 13 / 36 s. Landed → gold p50 / p99 285 / 517 s.
    - Spot churn happened again: over both runs, ~20 burst nodes were replaced on
      rebalance recommendations and 11 to balance zones, and 5 Spot launches failed (`UnfulfillableCapacity`).
      Split kept its rate through it.
- **35 a second for 10 minutes, the latency floor, with the shared gold commit** (2026-11-18,
  load-20261118T044826Z): 21,000 documents.
    - Every item landed exactly once (35 a second, by 605 s); 0 left behind; 32.1 documents a second first
      landed to last in gold (27.4).
    - **Landed → gold p50 / p99 32 / 185 s** (56 / 385). Convert's last record → transform's first p50 / p99
      11 / 52 s (23 / 367), so transform no longer sets the latency; landed → split ended (10 / 100 s) and split
      ended → convert's first record (9 / 122 s) now do. Target 20 s p99.
    - Up to 36 nodes (47): $26 per million documents (39), still from the stages scaling out on the queue's lag.
  **Adopted:** `sdh.stage.commit.shared: true` for transform. Next: split's rate a pod and the queue wait
  ahead of convert for the latency, and the stages' scale-out for the cost at low rates.
- **Split 8 items a batch** (2026-11-18): in run 41 split's item took 2.16 s, 1.26 s of it the append (its
  turn in the queue 0.45 s, the group's commit 0.79 s), while a pod used ~0.65 of a core and waited for a CPU
  a fifth of the time. So split's `concurrency` goes from 4 to 8 (64 items at once a pod) and its CPU request
  from 900m to 1100m (`splitter-application.yml`, `splitter-patch.yaml`), the 6 pods unchanged. Memory stays
  2 GiB: split streams an item, at most 2 windows of 4 chunks decrypted at a time. Measured the same way as
  runs 41 and 42, the images unchanged:
    - **100 a second** (load-20261118T064205Z): split 70 items a second (65), but its item now takes 3.47 s
      (2.16): the queue wait 0.65 s (0.45), the split step 2.33 s (1.66), a pod ~0.75 of a core, waiting for
      a CPU about a fifth of the time again. 62.9 documents a second first landed to last in gold (65.1);
      landed → gold p50 / p99 291 / 505 s (285 / 517). $12 per million documents (11), 35 nodes at most.
      No container restarted.
    - **16 items not ended at transform** when the test stopped waiting, its lag 0, as transform scaled in
      from 18 pods (14 in run 39, before the shared commit; 0 in run 41). Traced after the run: 8 burst
      nodes had EC2 Spot interruption notices at 1,338 and 1,409 s (08:29:19 and 08:30:30), their transform and
      convert pods went with them, and replacements failed for capacity; the items stopped at 1,462 s. The
      registries were reset for run 44, so which state they were left in can't be read. The code has a way out
      of each state found: a batch is acknowledged only once its records are stored and counted, an item's end
      is sent before it is marked, a lost end is sent again by convert's `ItemEndResender` and lost records are
      rebuilt by the reconciler (STAGES.md §9, §13.2); but those run after a 10-minute grace, every 15 minutes,
      and the test stopped 45 s after nothing moved, so no run has yet waited for them. `load-test.sh` now
      notes where each left item is (`left-items` in the run's directory) and waits up to
      `SDH_LOAD_RECOVER_MIN` (40) minutes for them to end, so the next run that leaves any shows whether they
      recover.
    - **Checked in the sandbox** (2026-11-18, scale demo, the images of runs 41–44): 600 documents with every
      convert and transform pod force-killed (no grace) at 25% and 60% ended at transform, and 1,200 with half
      the transform pods and the convert pod deleted gracefully (as a Spot drain) at 15, 30, 45, 60 and 95%.
      Every item ended at every stage within the demo, with none left for the reconcilers: 42 and 215
      transform records (3 convert records in the second) were taken over once their first pod's claim
      lapsed, no end was asked for again, and no reconciler event was logged. So a pod lost alone, at once or
      drained, doesn't leave an item.
    - **A node lost, too** (the same day): 1,200 documents, and at 40% the VM of the node holding the convert
      pod, a transform pod and a Kafka broker powered off at once (`virsh destroy`), its connections left
      open; it came back 8 minutes later. Convert stood still meanwhile, for the sandbox's own reasons: its one
      pod has nowhere else to go (the control plane is tainted, the other node had no memory for it), and a
      minikube node's kubelet doesn't start with its VM, so the node stayed `NotReady` until `minikube start`
      (which restarted the other node's pods as well). Once convert ran again every item ended at every stage,
      1,200 of 1,200: 15 transform records taken over, none at convert, no end asked for again, no reconciler
      event. So nothing in the sandbox (pods killed, drained, or lost with their node) leaves an item for the
      reconcilers; what it can't reproduce is AWS's pod counts, 18 transform pods and 48 partitions moving at
      once. That is for the next AWS run's `left-items`.
    - **35 a second** (load-20261118T073238Z): every item landed exactly once, 0 left behind. **Landed →
      gold p50 / p99 29 / 131 s** (32 / 185); landed → split ended 9 / 68 s (10 / 100), split ended → convert's
      first record 8 / 61 s (9 / 122). 33.0 documents a second; up to 33 nodes (36), $24 per million (26).
    - At 100 a second the gain is small: split's commits, back to back through the one queue, now bound it
      (2,992 snapshots of split_records in 1,288 s, ~30 items each). More split at that rate needs fewer,
      larger commits or a cheaper commit, not more items at once.
  **Adopted:** split `concurrency: 8` and 1100m, for the latency at 35 a second at about the same cost.
- **A split pod's groups wait for the commit side by side** (2026-11-18, DESIGN.md (dt); measured in runs 45-46,
  not adopted).
  Run 43's timings put split's limit in the pod, not the queue: each pod commits through one thread, a group
  taking 0.84 s from its turn to its commit (7,632 groups on 6 pods in 1,288 s: each pod's thread busy ~83% of
  the time), and its items waiting 0.65 s for that thread. The queue's turns came one after another, 2.3 a
  second, ~0.36 s each to combine, but took ~2.5 groups each of the 64 they can, so ~30 items a commit. With
  `sdh.split.commit.groups-at-once: 4`, a pod's next groups wait for a turn beside its last, and a turn takes
  them all: the same turns, more files each, the turn's fixed cost shared. Checked in the sandbox (one split pod,
  2 at once): 13 of 220 commits took two of the pod's groups together; nothing refused or failed.
  Measured on AWS, 2026-11-18, as runs 43 and 44 (8 items a batch, 1100m, the same loads), 4 at once:
    - **100 a second** (load-20261118T152555Z): the groups did go in together, ~6.7 a commit (2,566 commits
      for 17,245 groups; run 43 2,958 for 7,632, ~2.6). But a pod's four threads each took the files waiting
      when free, so its groups were smaller, more than twice as many, and each still took its own turn of the
      queue, one after another: a turn 1.03 s (0.58), a group 1.19 s (0.84), a commit 0.51 s (0.37). **Split
      61 items a second** (70); the stages done in 1,519 s (1,462), 59.2 documents a second (62.9); up to 37
      nodes (35), $13 per million (12).
    - **35 a second** (load-20261118T161653Z): 7,805 groups (3,103) in 1,765 commits (1,793), a turn 0.75 s
      (0.34): split fell behind at a third of the rate it keeps up with, ending at 1,020 s, 400 s after the
      feed (run 44: 620 s, with it). Landed → gold p50 / p95 / p99 35 / 300 / 415 s (29 / 100 / 131).
    - Both runs had heavy burst-node churn (the Auto Scaling group rebalancing its zones, and Spot), so the
      end-to-end numbers are partly the nodes; the commit counts and timings are not. Both also left items
      claimed at transform by a stopped pod (52 and 5; 16 in run 43): STAGES.md §10, revision (du). Their
      last-in-gold rates (21.6 and 7.4 documents a second) are those items' recovery, not the stages'.
  **Not adopted:** groups-at-once stays in the code at its default, 1, which the AWS and sandbox configs use
  again. More groups means more turns, and the turns are what is serial. What would help is fewer, larger turns:
  a pod's waiting groups going into one turn as one group.
- **Postgres connections, 2026-11-18 (revision (dv), measured in runs 47 and 48: peaks 245 and 238 of 300):** at the stages' scaled-out maxima (split
  6, convert 12, transform 18 pods) run 46 held 272 of 300: transform 144 (8 a pod), split 66 (11), convert 48
  (4). Each pod's own pools are small (the lease pool 2, the commit queue's consumers + 1), so the count follows
  the pods. Each Iceberg JDBC catalog holds up to two (`clients`); the transform stage's source catalog and the
  split stage's landing catalog only read, so now hold one: about 24 fewer at those maxima, ~248 of 300. Convert's
  one catalog also commits, so keeps two. Raising `max_connections` again was the other way, but each backend
  takes memory within Postgres' 2 GiB.
- **Steadiness check of revisions (du) and (dv), 2026-11-18 (runs 47 and 48, the scaled-out maxima as in runs 45
  and 46):** nothing lost or left. Run 47 (100 a second, 90,000 documents): every item landed once, none left
  behind at the end, no `stage.claims_left`; 101 expired claims handed in again by the sweep (38 convert, 63
  transform; a lower bound, below) while Spot capacity came and went: the cluster fell to 17 nodes at 467 s, an
  Auto Scaling group couldn't add 15 Spot nodes in one zone (no capacity), 400-odd pods waited for CPU. 51.8
  documents a second while busy (89,688 in gold by 1,733 s; 62.9 in run 44), then the last 85 items reached gold
  over 30 minutes after the stages scaled down to a pod each: not traced, since about half the pods left no log
  (the log follower took up pods still starting; fixed). Landed → gold p50 / p99 268 / 1,101 s; $31 a million,
  with the tail. Run 48 (35 a second, 21,000): all to gold in 652 s, p50 / p95 / p99 35 / 135 / 168 s (131 in
  run 44), $29 a million (the scaled-out maxima at a third of the load). Both runs' load test ended with 1 from
  its exit trap under `set -e`, leaving the staged copies in S3 (fixed).
- **Revision (dw) at 100 a second, 2026-11-19 (run 49, the maxima of run 47):** nothing lost; every item landed
  once and reached gold; no container restarted; no `stage.claims_left`. 89,990 in gold by 1,631 s, 55.2 documents
  a second by run 47's measure (51.8), 47.7 from first landed to last in gold; landed → gold p50 / p95 / p99
  254 / 863 / 1,032 s (268 / – / 1,101); 37 nodes at most; $15 a million (31). Postgres connections at most 241.
  The tail is down from 30 minutes to about 4.5: the last 10 items, all at transform and all claimed by one pod,
  reached gold at 1,901 s. Five sweeps handed in 146 expired claims (43, 23, 47, 23, 10), each sweep in one batch
  (its 10 to 47 audit lines within 8 ms) with `left=0`; one sweep was cut short when its duties pod stopped at
  scale-in, and the next holder swept 2 minutes later. **Why the last 10 waited:** their pod had two batches
  fail on Iceberg's `CommitFailedException` (05:33 and 05:37), then sat idle from 05:41 while still holding the
  10 claims, until it stopped at 05:44 and released them; the sweep took them 2.7 minutes later. A worker renews
  every claim it owns (`StageRegistry.renew`), not only those of records in a batch it is running, so a record
  left claimed after a failed batch, whose message came back elsewhere or not at all, stays claimed while its pod
  lives, out of the sweep's reach. That is likely run 47's 30-minute tail too. Who acknowledged the 10 messages
  without storing them is still not traced; transform logs no claim per record. **Traced since (2026-11-19,
  DESIGN.md revision (dy)):** after a batch failed and was sought back, Camel's Kafka batching handed on the rest
  of that poll, whose commit moved the group past the records sought back (reproduced in `KafkaRewindIT`); and
  the two `CommitFailedException`s were the queue's lock holder stopping (`RejectedExecutionException`, all 25 of
  the run's `append.failed`), which refused every pod's waiting groups. Both fixed; the burst nodes' churn, 25 shut
  down two at a time, fits managed Spot groups' Capacity Rebalance. Since then the duties run on an on-demand node
  (revision (dz)) and the stages reach Postgres through PgBouncer (revision (ea)); none of the three is measured on
  AWS yet.
- **Revisions (dy), (dz) and (ea) at 100 a second, 2026-11-19 (run 50, the maxima of run 49, the transform duties
  pod deleted part way):** nothing lost; all 90,000 landed once and reached gold, 0 left; no container restarted.
  52.4 documents a second from first landed to last in gold (47.7); landed → gold p50 / p95 / p99 172 / 410 /
  790 s (254 / 863 / 1,032); 39 nodes at most; $16 a million (15). Postgres connections at most 87 (241). No tail:
  the sweep recovered no claims (146), so (dx) and (dy) closed what run 49 traced. The duties pods held the duties
  all run; the deleted one's replacement took them 31 s later and no stage pod fell back. Ten `append.failed_stopping`
  and no other `append.failed`; three `broker.kafka_stale_left`. Of 14 `stage.batch_failed`, all retried, 7 were
  convert pods stopping (`RejectedExecutionException`), 4 transform `CommitFailedException`s in the stopping
  holders' own groups, and 3 `IllegalStateException`s, at least two in pods that weren't stopping: not traced, as
  the log keeps no message. Split slowed in the last quarter (12.1 s an item), so the last tenth ran at 903 a
  minute. The gold check didn't run: it reached Postgres through PgBouncer, whose user list has no `gold_reader`
  (fixed: the gold check connects to Postgres itself).
- **Run 50 again for its gold check, 2026-11-19 (run 51):** nothing lost; 90,001 items for 90,000 objects, all to
  gold, 0 left; no container restarted. 65.9 documents a second end to end (52.4); landed → gold p50 / p95 / p99
  222 / 460 / 551 s (172 / 410 / 790); 36 nodes at most; $12 a million (16); Postgres connections at most 85. Split
  kept the pace to the end this time (4,051 items a minute). **One object landed twice**, 70 s apart, the feed
  having copied nothing again: landing found the second's content the same as the first's (`item.duplicate_content`)
  and the split stage noted it as a copy, so it went on as a linked copy and supplanted nothing. Not traced: landing
  logs no object key, by design, and landing deletes each object it lands, so S3 keeps no second version to show.
  **The gold check failed again,** at its Job's 90-minute deadline, its `read` container still running after a Kafka
  `TimeoutException` (the pod and its log went with the Job). It had passed only on runs of up to 7,200 documents
  (about 12,000 messages); here it read 177,892 one at a time in one pod, which the deadline was never sized for.
- **The gold check at its own size, 2026-11-19 (run 52, 7,200 documents at once, the committed defaults):** passed.
  7,200 landed exactly once, all to gold, 0 left; p50 / p95 / p99 212 / 340 / 370 s; 16.8 documents a second on 9
  nodes; $15 a million. The check's first pod lost its Spot node and the Job started another, which read 13,087
  messages, checked 3,600 bundles with no failure and found no validator error mapping's baseline doesn't expect,
  in 27 minutes. At that pace run 51's 177,892 messages would take about six hours: the gold check belongs on a
  load of this size, and the large runs show every item in gold by their own count.

- **What the run used, at 100 a second, 2026-11-19 (run 53, 90,000 documents, the maxima of run 49 (splitter 6,
  convert 12, transform 18, 30 burst nodes), the first run of `resources.py`):** nothing lost; all 90,000 landed
  once and reached gold, 0 left; no container restarted. **72.4 documents a second** from first landed to last in
  gold (65.9, run 51); landed → gold p50 / p95 / p99 201 / 351 / 425 s; 34 nodes at most, 28.3 on average; **$10
  a million** (12). Split still sets the pace: landing was done at 935 s, split at 1,204 s, gold at 1,277 s.
  What it used, min / avg / max (21 samples):
  - **Pods:** splitter 1 / 4.9 / 6, convert 1 / 10 / 12, transform 1 / 14.9 / 20 (the progress rows: 1 / 15.5 /
    18). A transform pod used 0.92 cores on average of its 1.6 requested (1.94 at most) and 1.2 GiB of its 3 GiB
    (1.6 at most); a splitter 0.74 of 1.1 (1.25), 1.1 GiB of 2; a convert pod 0.24 of 0.4 (0.91), 0.8 GiB of
    1.25. No pod came within 90% of its memory limit.
  - **Nodes:** the burst nodes, 96% m7gd.large Spot, used 44% of their allocatable CPU on average (90% at most) and
    24% of their memory (31%): CPU fills them, so a smaller transform memory limit would save no node. The base
    nodes ran at 61% CPU (82%), the duties node at 13%.
  - **Postgres:** 0.84 cores on average on a 0.5 request (1.35 at most), 0.65 GiB of 2; 1,738 commits a second
    (3,170 at most), 98.6% cache hits, no temporary files; backends active 10.2 on average (31), waiting on a lock
    3.8 (7); connections 19 / 63 / 81 of 300 through PgBouncer; **82 deadlocks**. The database grew 0.85 GiB,
    about 9.6 KB a document (transform's schema 358 MiB, convert's 334, split's 119), 2.1 GiB an hour at this
    pace. **At that rate 15 million documents need about 140 GiB; the PoC's volume is 10 GiB** (§1, the initial
    load): what the registry keeps per record, and for how long, needs deciding before a load of that size.
  - **Kafka:** 0.34 cores on a 0.2 request (0.76), 588 of 768 MiB (681); 1.9 GiB of its 10 GiB volume, about
    550 MiB each for the split, records and FHIR topics and 267 MiB for landed.
  Six `stage.batch_failed`, all `IllegalStateException`s, all retried: four with a `PSQLException` cause, whose
  `thrown_at` (the innermost cause's top three frames, revision (eb)) shows only the driver's frames, so which
  statement failed isn't known (the 82 deadlocks are the likeliest); two from Iceberg adding a shutdown hook in a
  transform pod that was stopping. The report's junk check read 0 rejected at each stage, though the pods' logs
  have all three rejected at each: the counter is read from pods that had gone.
- **Kill checks at AWS pod counts, 2026-11-20 (run 54, run 53's load and maxima, `SDH_LOAD_KILLS`):** at shares
  ended at transform, half of transform's pods stopped gracefully three times (15%, 45%, 95%: 9 of 18 each time),
  one forced kill of a convert and a transform pod at 25% and again at 60%, of a splitter at 35% and 80%, and half
  of convert gracefully at 70% (6 of 12). **Nothing lost or left:** 0 left behind at convert or transform; 69
  expired claims handed in again by the sweep, no batch tried again for a claim left, no end asked for again; no
  container restarted. 59.4 documents a second (72.4 with no kills); p50 / p95 / p99 180 / 556 / 695 s; 34 nodes
  at most; $12 a million. Three findings, fixed since (DESIGN.md revision (ed)):
  - **36 valid records stored unconverted, `CONVERT_SKIPPED`,** by the third-claim rule, at 431 s, before any
    convert pod was killed: KEDA was scaling convert in (00:09:45 to 00:09:54), and each pod that stopped mid-batch
    failed its batch (`RejectedExecutionException`) and released its claims, which counted as an attempt when the
    next pod took them, as an expiry does. Two pods stopping under the same batch made the third pod's claim its
    third attempt. Kept in convert's fail store, not lost, but not in gold: 89,965 of 90,001 reached it. Now a
    claim a stopping worker hands back takes its attempt back (below).
  - **The failover at 30% landed never ran,** and the report didn't say so: `load-test.sh` found the lease holder
    by the `lease.acquired` line in `kubectl logs`, which by then had rotated away (the landing pod logged 180,000
    lines in the run). None of the 90,000-document runs since 2026-11-17 reports a failover, though some asked for
    one; the 7,200-document run 52 had its own. Now it reads the lease table, and says when a failover asked for
    didn't run.
  - **One object landed twice** (90,001 items), and it settles run 51's: the second `item.landed` has the first's
    `sdh.source_ref` and size, a new `sdh.sighting`, 60 s later, and the feed noted no failed copy. So the feed
    wrote the object twice, the AWS CLI sending the copy again after its 60-second read timeout though the first
    had been written; landing rightly took the new version as a new item, found it the same content as the first
    and supplanted nothing. Now the feed's CLI tries once, and a copy it reports failed is sent again only if the
    object isn't in the intake already.
- **Kill checks with (ed), and the revised targets, 2026-11-20 (runs 55 and 56, run 53's maxima, images with (ed) and
  (ef)'s telemetry, read, retention and junk count; not its renewal or stopping fixes):**
  - **Run 55, run 54's kills and the landing failover at 30%, fed at 100 a second:** the failover ran (the standby
    took the lease 3 s after the holder was deleted); 62 expired claims handed in again, 0 left behind; the junk
    count read 3 and 3. 49.9 documents a second; p50 / p95 / p99 364 / 636 / 793 s; 31 nodes; $14 a million.
    **8 records stored unmapped, `TRANSFORM_SKIPPED`,** all in one pod at their third claim: 11 batches failed on
    deadlocks (`sql_state` 40P01, `thrown_from` `StageRegistry.holds`: claim renewal against the batch store), and
    32 transform and 7 convert batches failed with `RejectedExecutionException` about 80 s after each graceful
    kill, their pod's commit pool stopped before the worker's close, so (ed)'s take-back didn't apply. Both fixed
    in (ef), not yet run.
  - **Run 56, fed at 60 a second, no failover or kills, then the gold check:** **41.2 documents a second, p99 854 s,
    $18 a million: neither target met.** p50 / p95 79 / 269 s; 35 nodes at most. Landing kept pace (all landed by
    1,544 s); split stalled at about 87,100 from 1,674 s to 2,027 s, after a splitter pod stopped at 06:12 and the
    split duties recovered its 156 items over 06:14 to 06:16; why it stayed stalled to 06:19 isn't traced yet.
    Postgres counted 147 deadlocks, and **40 records were stored unconverted, `CONVERT_SKIPPED`**, as in run 55.
    **The gold check failed:** it printed none of its lines in 90 minutes and the Job's deadline stopped it, so 16
    read threads weren't enough at 90,000 documents, or the read stalled; nothing says which.
  - **Both runs lost objects at landing: 89,974 and 89,985 of 90,000 landed,** with every stage agreeing and the
    intake empty at the end. Staging held all 90,000; the feed reported no failed copy (`copied-again` and
    `written-anyway` empty, exit 0); landing logged no `item.changed`, `item.abandoned`, `item.duplicate_content`,
    warning or error, and as many `item.landed` as items. Run 56 had no failover, so the hand-over isn't the cause.
    Landing deletes an object only after landing it or finding its sighting in the ledger index, and only if its
    size, time and ETag still match the listing; no path found in the code deletes one it hasn't landed. Runs 53
    (90,000) and 54 (90,001) lost none; the feed's one-try copies (`AWS_MAX_ATTEMPTS=1`) came in with run 55. Not
    known yet whether the 15 and 26 were never written or were deleted unlanded: the next run counts both: the feed's
    acknowledged copies, by name, and landing's takes from the intake by sighting, against those it landed
    (DESIGN.md revision (ef) (7)).
- **The HIPAA work's cost, 2026-11-22 (runs 60 to 63, asked for by the user: images of the H3 commit, so with the
  durable audit journal (P1), the control API's watches (C6, C7) and everything since run 59; run 49's maxima, set
  for the runs and put back after; no gold check).** Compared with run 59 (100 a second, 90,000, 2026-11-20) and
  runs 44 and 48 (35 a second, 21,000, 2026-11-18), the last of each before the HIPAA work:

  | Run | Feed | Burst nodes | Documents/s | Landed → gold p50 / p95 / p99 s | Split item, mean s | $/M |
  |---|---|---|---|---|---|---|
  | 59 (before) | 100/s, reset | m7gd 92% | 54.7 | 519 / 701 / 764 | 4.13 | 14 |
  | 60 | 100/s, **not reset** (run 59's 90,000 still in the tables) | m6gd 37%, m7gd 24%, m7g 21%, m8g 16% | 41.9 | 679 / 1,057 / 1,171 | 5.07 | (31: the end held up, below) |
  | 61 | 35/s, reset | m6g 42%, m6gd 12%, other 46% | 20.7 | 40 / 393 / 450 | 3.06 | 44 |
  | 62 | 100/s, reset | m7gd 93%, m7g 6% | **46.0** | 543 / 974 / 1,025 | 4.86 | 14 |
  | 63 | 35/s, reset | m7gd 53%, m7g 46% | **33.2** | 31 / 121 / **152** | 2.11 | 22 |
  | 44, 48 (before) | 35/s | not recorded | 33.0, 33.0 | 29–35 / 100–135 / 131–168 | 2.16 (48) | 24, 29 |

  - **Below saturation, no change:** at 35 a second (run 63) every document kept pace with the feed, p99 152 s
    (131 and 168 before) and a split item took what it did (2.11 s; 2.16).
  - **At saturation, split is the pace, and slower:** 46.0 documents a second against run 59's 54.7 (−16%), a split
    item 4.86 s against 4.13 (+18%: append +0.23, resume +0.14, commit queue +0.09), while a splitter pod used less
    CPU (0.48 cores on average; 0.62). The committed configuration ran 47 to 72 at 100 a second before (runs 53, 57
    to 59), so one run doesn't settle it, but split waits more for each item rather than working harder. The
    likeliest cause is the audit journal: split now writes seven durable, hash-chained audit events an item (lease
    acquired and released, item started, split and completed, batch committed, stage item completed; 144,478 for
    run 63's 21,000), landing three, transform 2.3, convert one, through Postgres beside the stages' own commits
    (1,518 commits a second on average; 1,441). Not proven: a run with fewer audit events an item, or its journal
    in its own database, would show it. Measured locally since (§13, 2026-11-23): the journal adds about 0.5 ms of
    Postgres CPU an item (+30%), in transactions of about two events; a 100 ms linger takes that away.
  - **Spot's mix moves the result more than the code:** runs 60 and 61 got 37% and 54% Graviton2 (`m6g`, `m6gd`),
    where runs 53 to 59 had 91–96% `m7gd`; split is CPU-bound there, and run 61 fell to 20.7 documents a second, p99
    450 s, at a load run 63 kept pace with. Runs 62 and 63 were made comparable by allowing only `m7gd.large` and
    `m7g.large` for them. A run's report gives its mix (`by node pool`); compare only like with like.
  - **Run 60 measures nothing alone:** it ran on run 59's tables (`poc.sh reset` was missed) and with 37%
    Graviton2, 9% slower than run 62; which of the two cost what isn't known. Every comparison run starts reset.
  - **Found: the split reconciler's list goes stale under a backlog** (runs 60 and 62): once split is more than
    its grace (10 minutes) behind landing, `SplitReconciler` lists every landed item without a checkpoint, then
    splits them one at a time, about four a second, on the duties pod. In run 62 that did real work (1,975 items
    split before their messages arrived); but after the backlog cleared it went on through the rest of its list,
    whose items had been split meanwhile, announcing each again (convert takes them as copies), so the consumer
    lag never reached nothing and the load test couldn't end. Both runs were ended by restarting the duties pod
    once every item was in gold (its new first pass: 0 recovered). Nothing lost or supplanted; every run exactly
    once, 0 left behind. Fixed since (DESIGN.md revision (eu)): each item is looked up again just before it is
    recovered, and left if it was started meanwhile, as is each item left part way; not yet run on AWS.

## 11. Order of work

Each step is measured before the next, and each is useful alone.

| Step | What | Expected effect |
|---|---|---|
| 0 | Measurement harness and a baseline at 1,000 documents (§10; 100,000 after step 1) | the numbers to beat. Harness built and baseline run 2026-11-13 (§10): 0.12 documents/s, p99 5,526 s; split slows as the landed table grows: three planned reads of it per item, 99% of split's time with the `split_records` lookups (§10, 2026-11-13); not the pod, not CPU |
| 0a | The outage mode of the load test (§3.6), run on the current code; find the failover duplicate | recovery checked before anything changes it. Duplicate found (the re-announce, §3.6 point 1); plain outage passed 2026-11-13; message-losing outage run 2026-11-16 (§3.6): landing and split whole, 973 items left at convert and transform; lost item ends now sent again (DESIGN.md (do)), rerun the same day: 0 left, all 2,430 to gold, 2,735 s after the restart; 231 ends resent; gold check passed |
| 1 | Direct reads by location (§3.2), housekeeping (§3.3), split concurrency and partitions (§3.4, part) | split no longer slows as tables grow; split and convert at pod count × per-pod rate. Reads by location built and measured 2026-11-13 (DESIGN.md revision (db), §10): 10× the rate, p99 58.5 s, split flat as tables grow. Housekeeping, split concurrency (4 per pod) and 24 partitions built and measured 2026-11-13 (revisions (dc), (dd), §10): landing (~1.2 items/s) is now the limit; on a backlog the stages after it reach ~3.9 documents/s; no item bucket (§3.3); expiry doesn't yet follow the consumers (§3.6, 4) |
| 2 | Group commit in every stage (§3.1) | landing from ~10 to hundreds of items/s; ~100× fewer snapshots. Built 2026-11-13 (DESIGN.md revision (de)): landing groups small items listed from a source (500 a commit, 64 MB, 1 s), split commits concurrent items together; the ledger is written after the items, not before. Measured 2026-11-14 (§10): landing ~10 items/s (was 1.2) in 2 commits for 1,020 items; 1.84 documents/s overall; split (1.9 items/s on 2 pods) is now the limit; a failover re-announced the whole last group, fixed in revision (df) |
| 3 | Pod signing keys (§3.5) | removes the ~45 documents/s KMS ceiling. Built 2026-11-13 (DESIGN.md revision (dg)): the statement in every message, revocation by statement ID. With split pods taking several batches at once (§3.4, revision (dh)), since split, not KMS, was the limit. Measured 2026-11-14 (§10): 4.50 documents/s without a failover (1.84 before), 3 KMS signatures for 16,010 messages, $27 per million documents; split still the limit (~2.8 items/s a pod); a failover's repeats each read `split_records`, fixed in revision (di). Split's steps timed and an item of one chunk read in one open of its file (revision (dj), §10): 5.12 documents/s, split 2.9 s an item (4.7); the pods are short of CPU (a thread waiting for one up to 80% of the time); with CPU requests that match, 5.51 documents/s, split's CPU wait 0.25–0.32; the summary read in two requests and a third split pod (revision (dk)), 6.81 documents/s, transform now the limit; transform at 4 records a pod on 1.6 CPU, 8.37 documents/s at ~$20 per million documents (from $18), split the limit again; split's commit as one fast append with gzipped metadata (revision (dl)), no faster: 31% of its commits conflict between pods; split's commits through one queue across pods (revision (dm), §3.7): under 2% retried, split 1.38 s an item (1.85), 6.91–8.23 documents/s, landing and transform now set the pace; transform clearing a 7,200-document backlog at 2, 4 and 6 pods (§10): 0.32 CPU-s a record at every count, 4.3–5.0 records a second a pod with work, so ~25 pods (~25 nodes, ~$4.70 per million documents) and 48 partitions for 100 documents/s; the test's identical copies share a Kafka key, so clearing stopped speeding up at 4 pods; two batches at once a transform pod (revision (dn)): 18.3 records a second at 4 pods (14.1), 4.6 a pod, all of its 1.6 CPU, so ~24 pods for 100 documents/s; with 48 partitions and distinct test copies, the pods evenly fed: 21.2 records a second at 4 pods, 5.3 a pod, so ~21 pods (~$3.90 per million documents) for 100 documents/s; the Whistle runtime's call overheads cut (§10, 2026-11-16): 23.1 records a second at 4 pods, ~0.25 CPU-s a record (0.30), so ~18 pods for 100 documents/s; six records at once a pod, 3 GiB (§10, 2026-11-16): 28.6 records a second at 4 pods, 7.15 a pod, so ~14 pods for 100 documents/s; t4g surplus credits in the cost |
| 4 | Concurrent fetch in the landing node and sharded intakes (§3.4) | landing beyond one node's serial rate. Part 1, concurrent reads and source deletes in the one landing node, built 2026-11-16 (DESIGN.md revision (dp)), measured the same day (§3.4): landing 30.9 items/s (~11), gold check passed, 15.1 documents/s overall (6.9–8.2), split the pace again; sealing a group's messages several at once and landing's CPU request 0.75 (revision (dq), §3.4): landing 60 items/s, split 24 items/s on 3 pods, gold check passed; transform (4 pods) now the pace, 14.4 documents/s; finishing at the source landing's largest step. Part 2, sharded intakes, not started |
| 5 | SQL dialects and batched registry updates (§8) | database work per batch, not per record; runs on four databases |
| 6 | Large documents (§9) | 100 MB documents through, with bounded memory |
| 7 | 1M documents on S3 and SeaweedFS, against §10's pass mark, with an outage part way (§3.6) | every document in gold once, after the restart |

### 11.1 Still to measure, as demand arises (noted 2026-11-20, with the user)

Everything above was measured on one pipeline, C-CDA to FHIR, with 30 test documents of about 119 KB each, one
record a document, half of them failing the US Core checks. Each other format runs as its own pipeline (its own
namespace, topics, tables and split commit line), so it neither slows nor is slowed by this one in the code, but its
numbers can't be read off C-CDA's. Each is to be run on AWS when a buyer's feed calls for it, with a realistic mix
of that feed's batch sizes:
- **X12, a transaction a file:** smaller than C-CDA and cheaper to parse, but split's commits cost the same an
  item: expected at or above C-CDA's items a second, for less a million.
- **X12 batches** (an 837 interchange of thousands of claims) **and large CSV files:** few items, many records.
  Records a second, set by convert and transform and by each record's registry rows (Postgres growth goes with
  records, not bytes), is then the measure, not items a second.
- **C-CDA that mostly maps:** run 53's half that failed the checks cost transform little; a mostly mappable feed
  costs more a million, though split, not transform, sets the pace.
- **Hours, not minutes (a soak):** the runs so far last about 20 minutes. A day or two at a steady feed just under
  capacity shows the plateau once a day of snapshots is kept (24 hours, §12), with the commit step's own timings
  (table refresh, metadata write, catalog update) to say whether S3 or Postgres grows. Before it, the stores need
  sizing for it: Kafka's 10 GiB holds about 500,000 documents at 21.9 KB each (30 days' retention is decided,
  §12), Postgres's 10 GiB about 700,000 at 14.8 KB with the audit journal (9.6 KB before it), and the landing node's
  ledger index takes 36 to 72 bytes an item, kept for good. For 15 million C-CDA documents that is at least 250 GiB
  of Postgres volume and 370 GiB of broker disk a copy (DEPLOYMENT.md, "Sizing Postgres and Kafka").
- **Every format other than C-CDA** (`x12`, `edifact`, `csv`, `delimited`, generic `xml`, `fhir`, `parquet`) is
  untested end to end and at load, and has no mapping set: DEPLOYMENT.md, "Formats: what has been tested".

## 12. Decided with the user, 2026-11-13

1. **Targets (§1):** at least 100 documents/s sustained (15M in under 48 h), and p99 landed → gold under 20 s,
   measured over 1M documents. **Revised 2026-11-20:** at least 60 documents/s sustained and p99 under 200 s,
   over a 15M initial load (§1).
2. **Snapshot retention (§3.3):** 24 hours in the hot tables, and never before the consumers have read past
   it (§3.6, 4).
3. **Signing (§3.5):** pod keys, each vouched for by a KMS-signed statement valid for 1 hour, verified at the
   message's signing time, revocable.
4. **Sharded intakes (§3.4):** a fixed number of shards per intake, by prefix (`…/0/` to `…/n-1/`), one
   single-active landing node each; default 1.
5. **Large records (§9):** a record over the stage's limit fails to the fail store as `RECORD_TOO_LARGE`, kept
   and reprocessable with a larger limit.
6. **Broker retention (§3.6, 6):** 30 days on every pipeline topic.

## 13. Split fan-out: from one item to many records (measured locally, 2026-11-23)

**Why:** every load so far had one record an item (C-CDA), so what an item of many records costs to split, and
where the time goes as it writes them, was unmeasured. The formats with many records an item (X12 batches, CSV,
EDIFACT, FHIR NDJSON, Parquet) depend on it.

**How:** `SplitFanOutPerf` (file-connector, `./mvnw -Pperf -pl file-connector -am verify -Dperf.stages=fanout`),
the real split stage in process, as `SplitterPerf`: embedded Postgres, the tables on local disk, a Camel queue in
memory for the broker. Synthetic CSV items (a sequence number and letters, no PHI) of 1 to 100,000 rows, rows of
200 and 2,000 bytes, 64 items at once, batches of 10,000 records (the default), the audit journal off, on, and on
with a linger. For each point: items and records a second, each item's time to its `item.split`, the stage's own
timers summed per item and per record, and Postgres' transactions, rows, write-ahead log and CPU. Three new
timers split a batch's `records` time: `batch_open` (opening its data file), `records_read` (reading and
splitting its records) and `records_store` (encrypting each and writing it to the data file). Then a Java Flight
Recorder profile of one 100,000-row item, and a timing of one message's sealing on one thread. The host: a
16-core (32-thread) Ryzen 9 8945HX, JDK 25.

**The shape** (200-byte rows, audit journal on; 2,000-byte rows were within 10 to 20%):

| Rows an item | Items | Records a second | Item p50 | Postgres CPU an item | Transactions an item | WAL an item |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2,000 | 470 | 115 ms | 2.4 ms | 21 | 9 KB |
| 10 | 2,000 | 4,300 | 139 ms | 2.2 ms | 21 | 10 KB |
| 100 | 200 | 14,800 | 375 ms | 2.7 ms | 18 | 10 KB |
| 1,000 | 20 | 20,600 | 655 ms | 4 ms | 12 | 10 KB |
| 10,000 | 2 | 18,700 | 1.07 s | 15 ms | 45 | 12 KB |
| 100,000 | 1 | 23,000 | 4.4 s | about 10 ms | 133 | 58 KB |

- **Up to about 1,000 records an item, the item's own steps set the pace:** about 20 transactions, 20 rows and
  10 KB of log in Postgres whatever its size, and one Iceberg commit (`append`, grouped with the other items').
  Records a second rise with the records an item, until 1,000.
- **From about 1,000 records an item, records a second level off at 17,000 to 23,000,** with the JVM using 18 to
  25 of the host's 32 threads, and bytes hardly matter (37 MB/s at 2,000 bytes a row). On the item's own thread,
  a record costs about 4 µs to read and split, 22 µs to encrypt and write to the data file, 5 µs to close the
  file and 3 µs to commit it (batches of 10,000). Postgres does almost nothing more a record: its work is per
  item and per batch.
- **What levels it off is sealing the records' messages:** 72% of the CPU of a 100,000-row item, on the
  `sdh-seal` threads, is P-256 arithmetic. Of the sealing, ECDH key agreement is 65% (half of it the JDK
  checking the recipient's public key again for every message), the ephemeral key 10%, the ES256 signature 16%
  and AES-GCM 1%. On one thread, a message's signature takes about 100 µs and its encryption (ECDH-ES+A256KW)
  about 465 µs: **about 0.56 ms of CPU a message**, so a core seals about 1,800 messages a second. Parquet, the
  records' own encryption and Postgres are each 1 to 2%. The next stage pays about as much again to open each
  message (the same key agreement, and a verification).
- **So a record's cost is its message, not its storage.** With one record an item, as C-CDA, sealing is about
  1 ms of CPU an item (its `record.split` and its `item.split`), small beside the item's Postgres steps; at
  10,000 records an item it is about 5.6 CPU-seconds an item at split, and as much again at convert.
- `send_wait` (an item waiting for its last batch's messages) grew with the records an item, to 0.4 s for 10,000;
  it is the messages queuing for the sealing threads. Publishing a batch a thread, in place of the one
  `sdh-publish` thread, was tried and changed nothing, so was not kept; nor did a broker stand-in read by 16
  threads.

**The audit journal, locally:** the first sweep ran audit on before off, and showed every Postgres step of an
item two to four times slower with it on; run again with off first, the difference went, so it was the order (the
registry tables grow over a run), not the journal. With Postgres having CPU to spare, the journal adds no time to
an item. It does add **about 0.5 ms of Postgres CPU an item (+30%), 6 rows and 4 KB of log**: its writer stores
what is waiting as soon as any is, a transaction of about two events each time, so under load it commits
continuously. On AWS, Postgres was short of CPU (§10, runs 60 to 63: up to 1.45 cores on a 0.5-core request), and
every Postgres step of a split item grew by about half while the Iceberg commit didn't, so this is the likeliest
cause of the HIPAA runs' slower split; it is not yet shown on AWS. **A linger** (`sdh.telemetry.journal.linger`,
built, default 0 so unchanged: a batch waits up to that long for more events, up to 500, before it is written;
closing doesn't wait) of 100 ms took the journal's Postgres CPU back to that of no journal (1.6 ms an item,
against 1.7 off and 2.2 on). It changes no event and no chain; it delays an event's being durable by up to the
linger, beyond the in-memory queue it already waits in.

**Not covered:** a real broker (Kafka's acknowledgements), S3 for the data files, Postgres short of CPU, and
fewer cores (a split pod has 1.1). Per-item Postgres counts at a few items (10,000 rows and over) are noisy.
**Next:** the linger on AWS (a 100/s run against run 63), and a CSV fan-out run on AWS (100 and 10,000 rows an
item), to set these against S3, KMS and Kafka.

### 13.1 Sealing and the linger on AWS (runs 64 to 67, 2026-11-23)

**What ran:** images with the native JOSE provider (`sdh.crypto.jose.provider`, ACCP by default here), one ECDH
agreement shared by up to 10,000 messages or 60 s (receivers cache it; each message keeps its own content key, IV
and ES256 signature: DESIGN.md revision (ev)), and the audit journal's linger (`sdh.telemetry.journal.linger`).
Run 49's maxima and only `m7gd.large`/`m7g.large` burst nodes, put back after; reset before each run; no gold
check. The images did not yet have registry retention (off by default, so no effect). Every burst node was
`m7gd.large` Spot in all four runs, so they compare with run 62 (93% `m7gd`) and run 63.

| Run | Feed | Provider, linger | Documents/s | Landed → gold p50 / p95 / p99 s | Split item, mean s | $/M |
|---|---|---|---|---|---|---|
| 62 (before) | 100/s | JDK, a key agreement a message, 0 | 46.0 | 543 / 974 / 1,025 | 4.86 | 14 |
| 64 (A) | 100/s | ACCP, 100 ms | (31.9: a 26-minute landing stall, below; 63 after it) | 242 / 418 / 444 | 3.50 | (21) |
| 66 (C) | 100/s | JDK, 100 ms | **59.1** | 393 / 586 / 619 | 4.12 | 12 |
| 67 (D) | 100/s | ACCP, 0 | **55.6** | 398 / 646 / 712 | 4.13 | 11 |
| 63 (before) | 35/s | JDK, a key agreement a message, 0 | 33.2 | 31 / 121 / 152 | 2.11 | 22 |
| 65 (B) | 35/s | ACCP, 100 ms | **32.9** | 27 / 94 / **115** | 1.70 | 21 |

(Runs: 64 `load-20261123T071829Z`, 65 `load-20261123T065958Z`, 66 `load-20261123T084819Z`, 67
`load-20261123T060527Z`.)

- **The HIPAA runs' loss at saturation is recovered:** 55.6 and 59.1 documents a second against run 62's 46.0, a
  split item 4.1 s against 4.86, back to run 59's (54.7, 4.13), at $11–12 a million. Sealing is now invisible in
  the stages' timers (convert's and transform's `seal` under 0.5 ms a message; landing's about 0.12 s a batch).
- **Provider and linger are not separated by these runs:** the shared agreement was in all of them, and run 64,
  the one with both ACCP and the linger, was spoilt by the stall. JDK with the linger (66) and ACCP without it
  (67) were within run-to-run noise of each other (runs 53 to 59 ranged 47 to 72); with the shared agreement the
  provider's share of the CPU is small (on one core: 231 µs a seal with the JDK, 50 with ACCP, against 886 for the
  JDK with an agreement a message), so most of the gain is the shared agreement.
- **At 35 a second (run 65) every document kept pace,** p99 115 s, the lowest yet (152 in run 63; 131–168 before
  the HIPAA work), a split item 1.70 s (2.11).
- **Nothing lost in any run:** each exactly once, 0 left behind, no container restarted, the landing failover
  test passing in each.
- **Found: landing's startup recovery took 26 minutes (run 64).** The lease holder was deleted during a group of
  500 items it had landed but not yet finished at the source; the standby took the lease after 70 s, then spent
  26 minutes, silent, on its main thread before announcing them again and landing the rest (the 343/min tenth).
  `IntakeService.ensureRecovered` calls `sameAs`, hence `LandingLedger.earliestCopy`, once for each item of the
  last group, and each is a scan of the whole ledger table filtered on the digest, which file statistics can't
  narrow (digests are random): about 3 s each against a ledger of some 30,000 items, so 500 cost about 25 minutes,
  and would cost more on a larger ledger. Landing itself checks the in-memory `LedgerIndex` first; recovery doesn't.
  In the other three runs the holder was deleted between groups, and the standby was landing within 3 s. Fixed
  since (DESIGN.md revision (ew)): one scan for the whole group's digests; shown on AWS in run 68, below.
- **Run 68 (2026-11-23, `load-20261123T133207Z`; asked for by the user): the fix, with failovers part way through
  a group.** 90,000 at 100 a second, the committed maxima (3, 3 and 4 pods; so 27.9 documents a second, not a
  throughput result), the landing holder killed with no grace at 25, 50 and 75% landed
  (`SDH_LOAD_FAILOVER_HOW=midgroup`), and once more by hand at 80%, timed by its log into a group's window between
  its commit and its messages (item.landed with no item.announced in its last second; about 3 s a group). The
  first three, which waited only for a group's lines, all came after the group's messages were acknowledged, so
  their standbys had nothing to announce again and were landing 28 to 41 s after taking the lease. The fourth left
  a group of 500: its standby announced all 500 again in 0.44 s, against a ledger of some 73,000 items, and was
  landing 77 s after taking the lease (22 s to start, 22 s for recovery's repair and its one scan), where run 64's
  took 1,621 s against 30,000. Exactly once (the group taken again from the intake: `in_ledger 500`), 0 left
  behind, no container restarted. `midgroup` now waits for that window itself; the report gives each standby's
  recovery from its log.
- **Not shown: the split reconciler fix (eu).** No pod logged a `split.reconciled` pass in any run: the duties
  pod's first pass after a reset finds no landing table yet, and its next is 15 minutes (`interval`) later, about
  when a run ends. Split never fell more than its grace behind for long, so no run needed it; a run long enough for
  two or more passes under a backlog would show it.
