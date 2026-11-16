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

## 11. Order of work

Each step is measured before the next, and each is useful alone.

| Step | What | Expected effect |
|---|---|---|
| 0 | Measurement harness and a baseline at 1,000 documents (§10; 100,000 after step 1) | the numbers to beat. Harness built and baseline run 2026-11-13 (§10): 0.12 documents/s, p99 5,526 s; split slows as the landed table grows: three planned reads of it per item, 99% of split's time with the `split_records` lookups (§10, 2026-11-13); not the pod, not CPU |
| 0a | The outage mode of the load test (§3.6), run on the current code; find the failover duplicate | recovery checked before anything changes it. Duplicate found (the re-announce, §3.6 point 1); plain outage passed 2026-11-13; message-losing outage run 2026-11-16 (§3.6): landing and split whole, 973 items left at convert and transform; lost item ends now sent again (DESIGN.md (do)), rerun the same day: 0 left, all 2,430 to gold, 2,735 s after the restart; 231 ends resent; gold check passed |
| 1 | Direct reads by location (§3.2), housekeeping (§3.3), split concurrency and partitions (§3.4, part) | split no longer slows as tables grow; split and convert at pod count × per-pod rate. Reads by location built and measured 2026-11-13 (DESIGN.md revision (db), §10): 10× the rate, p99 58.5 s, split flat as tables grow. Housekeeping, split concurrency (4 per pod) and 24 partitions built and measured 2026-11-13 (revisions (dc), (dd), §10): landing (~1.2 items/s) is now the limit; on a backlog the stages after it reach ~3.9 documents/s; no item bucket (§3.3); expiry doesn't yet follow the consumers (§3.6, 4) |
| 2 | Group commit in every stage (§3.1) | landing from ~10 to hundreds of items/s; ~100× fewer snapshots. Built 2026-11-13 (DESIGN.md revision (de)): landing groups small items listed from a source (500 a commit, 64 MB, 1 s), split commits concurrent items together; the ledger is written after the items, not before. Measured 2026-11-14 (§10): landing ~10 items/s (was 1.2) in 2 commits for 1,020 items; 1.84 documents/s overall; split (1.9 items/s on 2 pods) is now the limit; a failover re-announced the whole last group, fixed in revision (df) |
| 3 | Pod signing keys (§3.5) | removes the ~45 documents/s KMS ceiling. Built 2026-11-13 (DESIGN.md revision (dg)): the statement in every message, revocation by statement ID. With split pods taking several batches at once (§3.4, revision (dh)), since split, not KMS, was the limit. Measured 2026-11-14 (§10): 4.50 documents/s without a failover (1.84 before), 3 KMS signatures for 16,010 messages, $27 per million documents; split still the limit (~2.8 items/s a pod); a failover's repeats each read `split_records`, fixed in revision (di). Split's steps timed and an item of one chunk read in one open of its file (revision (dj), §10): 5.12 documents/s, split 2.9 s an item (4.7); the pods are short of CPU (a thread waiting for one up to 80% of the time); with CPU requests that match, 5.51 documents/s, split's CPU wait 0.25–0.32; the summary read in two requests and a third split pod (revision (dk)), 6.81 documents/s, transform now the limit; transform at 4 records a pod on 1.6 CPU, 8.37 documents/s at ~$20 per million documents (from $18), split the limit again; split's commit as one fast append with gzipped metadata (revision (dl)), no faster: 31% of its commits conflict between pods; split's commits through one queue across pods (revision (dm), §3.7): under 2% retried, split 1.38 s an item (1.85), 6.91–8.23 documents/s, landing and transform now set the pace; transform clearing a 7,200-document backlog at 2, 4 and 6 pods (§10): 0.32 CPU-s a record at every count, 4.3–5.0 records a second a pod with work, so ~25 pods (~25 nodes, ~$4.70 per million documents) and 48 partitions for 100 documents/s; the test's identical copies share a Kafka key, so clearing stopped speeding up at 4 pods; two batches at once a transform pod (revision (dn)): 18.3 records a second at 4 pods (14.1), 4.6 a pod, all of its 1.6 CPU, so ~24 pods for 100 documents/s; with 48 partitions and distinct test copies, the pods evenly fed: 21.2 records a second at 4 pods, 5.3 a pod, so ~21 pods (~$3.90 per million documents) for 100 documents/s |
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
