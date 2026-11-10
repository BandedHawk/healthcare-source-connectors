# Parallel stages: design

How the pipeline after the landing node becomes stages that run in parallel under KEDA, on one shared
implementation for every broker. Proposed 2026-11-05, for review before the refactor; decisions already made with
the user are marked **(decided)**. Section numbers (§) refer to [DESIGN.md](DESIGN.md).

## 1. Shape

```
 producers ─► landing node ──item.landed──► split stage ──record.split──► convert stage ──record.stored──► transform stage
              (serial)        (any broker)  (per item,     (any broker)   (per record,     (any broker)    (per record,
              bronze:                       parallel)                     parallel)                        parallel)
              landed_items                  bronze:                       silver:                          gold:
                                            split_records                 records                          FHIR tables
                              ◄───────── item.split ─────────►            ◄── item.converted ──►           ◄── item.transformed
```

- **The landing node stays serial (decided):** one active node per intake and a standby, under its lease (§5.3.5).
  Its output queue is any of the seven brokers, chosen at run time by `sdh.queue.type` and `sdh.queue.uri`. That
  is already so, through `connector-runtime` `publish/`; nothing changes there but the message names.
- **Split, convert and transform are separate stages (decided),** each its own deployment, scaled by KEDA on its
  input queue. Any number of workers, each taking several messages at once.
  - **Split** works per item: it reads a landed item as a stream, cuts it into records and stores each in its
    original form. One item is split by one worker at a time; different items in parallel.
  - **Convert** works per record: C-CDA XML to JSON, X12 to JSON and so on (§6.4), any worker taking any record.
  - **Transform** works per record: it runs the Whistle runtime, fail-fast, over the converted JSON. What it maps
    is set entirely by the WSTL set it is given at run time (decided 2026-11-05): C-CDA JSON to a FHIR bundle
    is the US Core 6.1 WSTL configuration, but any converted format can be mapped to anything a WSTL set
    defines, without a new build. Built in `healthcare-data-transformer`, on the library from this repository
    (decided).
- **No ordering between records (decided).** A large batch spreads across every worker. Each stage sends an
  item-complete message once all of an item's records are done (§5).

## 2. "Section" becomes "record" (decided)

The unit the splitter cuts an item into was called a section, which in C-CDA means something else (a
`<section>` inside a document). It becomes a **record** everywhere: classes (`Section` → `SplitRecord`, since
`Record` would clash with `java.lang.Record` and Iceberg's `Record`; `SectionsTable` → `RecordsTable`), messages
(`section.stored` → `record.stored`), tables (`sections` → `records`), settings, metrics (`sdh.sections.stored`
→ `sdh.records.stored`), audit events and documents. A C-CDA record is one `ClinicalDocument`; an X12 record one
transaction set; a CSV record one row (§6.2). The same goes for "file" where it means an item (`fileId` →
`itemId`, `sdh.files.failed` → `sdh.items.failed`, the `file.failed` audit event → `item.failed`);
`file.started` and `file.completed` stayed until step 4d removed them; the item-complete messages replace them.

To keep one meaning per word, the other uses of "record" were renamed first: where a message batch's messages
start is a **message index** (`MessageIndex`, the `message_offsets` and `message_keys` columns), and a CSV line
is a **row**.

This, and the new messages, make the contract **3.0**. Nothing has been released, so there is no compatibility
with 2.x: every stage and reader moves to 3.0 together.

## 3. Layers

| Layer | Bucket | Iceberg table | Written by | Holds |
|---|---|---|---|---|
| Bronze | `sdh-bronze` | `<pipeline>_landing.landed_items`, `landing_ledger` | landing node | items as received |
| Bronze | `sdh-bronze` | `<pipeline>_split.split_records` | split stage | records as cut, original form |
| Silver | `sdh-silver` | `<pipeline>.records` | convert stage | records converted (JSON), or in original form with the error that stopped conversion |
| Gold | `sdh-gold` | `<pipeline>_fhir.*` | transform stage | FHIR resources |

Split records are bronze: still the data as received, only cut apart. Encryption is as now (§7): each table's
rows are encrypted under its own KEK; the split stage gets the split-records KEK, the convert stage unwraps it.

## 4. Messages (contract 3.0)

| Type | From → to | Payload |
|---|---|---|
| `item.landed` | landing → split | as now, `fileId` renamed `itemId` |
| `record.split` | split → convert | recordId, itemId, seq, sourceOffset, sourceLength, format, storage ref, origin |
| `item.split` | split → convert | itemId, recordCount, failedRecordCount (pieces the splitter couldn't parse, stored raw), itemDigest |
| `record.stored`, `record.failed` | convert → transform and other readers | as `section.stored` and `section.failed` now |
| `item.converted` | convert → transform and readers | itemId, recordCount, failedRecordCount |
| `record.transformed`, `record.untransformed`, `item.transformed` | transform → readers | defined in `connector-contract`, for the transformer; each names the WSTL set (and its digest) that mapped it |
| `record.conflict` | any stage → its dead-letter queue | the record's ID, the stage, both output digests (§6) |
| `item.received`, `item.result` | landing / convert → producer | as now; `item.result` moves to the convert stage, after `item.converted` |

`file.started` and `file.completed` are gone (removed in 4d): the item-complete messages replace them. The
ordering key (Kafka key, SQS group, Service Bus session, Pub/Sub ordering key, Artemis group) becomes the
**record ID** for record messages, so records spread over partitions, and stays the item ID for item messages.

## 5. Item complete

Records arrive at a stage in any order, and an item's `item.split` may come before or after its records. Each
stage keeps a row per item in Postgres, `stage_item(stage, item_id, expected, done, failed, completed_at)`:
- `expected` is set from the upstream item-complete message (`item.split` for convert, `item.converted` for
  transform);
- `done` (and `failed`) go up by one when a record is first stored (§6), never for a duplicate;
- when `expected` is known and `done` reaches it, a conditional update (`completed_at IS NULL`) lets exactly one
  worker send the stage's item-complete message. Sending it again after a crash is harmless: its message ID is
  fixed (`ic-<stage>-<itemId>`), and the next stage drops the repeat (§6); so may the broker, where it
  de-duplicates by message ID. `QueueReader` doesn't (found 2026-11-06, §12).

## 6. Duplicates (decided)

Delivery is at least once on every broker. A record is processed twice when a worker dies before acknowledging,
its lock or visibility times out while it is still working, Kafka moves its partition during a scale-up, or the
stage before sends again after a crash. So each stage keeps a registry in Postgres,
`stage_record(stage, record_id, item_id, state, output_digest, owner, claim_expires, stored_at)`, primary key
`(stage, record_id)`. For each record a worker (as built in step 3):
1. **claims** the record first: insert-if-absent, as `CLAIMED` with its own owner and an expiry, renewed while it
   works. A claim taken over from a worker that let it expire counts one more attempt, which the processor is
   told, so a record that keeps stopping workers can be stored unprocessed (§9);
2. if the claim is its own: does the work, commits it with the rest of the batch, then marks it `STORED` (only if
   it still owns the claim) with the record's sealed message, sends the message, marks it `SENT` (dropping the
   message), and only then acknowledges. A crash between the commit and the send loses nothing: whichever worker
   gets the record next finds it `STORED` and sends the message kept with it;
3. if the record is already stored: compares versions first, then, only if they differ, digests (versions below).
   The same version, or the same digest, is a **duplicate**: dropped, `record.duplicate` audited, acknowledged. A
   different digest (the converter or mapping changed between the two) is a **conflict**: the first stays, the
   second goes to the stage's dead-letter queue as `record.conflict`, audited, and acknowledged. Nothing is replaced
   (decided);
4. if another worker holds a live claim: the rest of the batch is stored and sent first, then the record is
   looked at again until the other has stored it (a duplicate) or let its claim expire (taken over). It is never
   acknowledged before: that would lose the record if the other worker died. Storing the rest first means two
   workers each waiting on the other's records can't hold each other up.

A record ID is a keyed hash of the record's content (§7.3), so the same ID means the same input.

**Versions (decided 2026-11-06).** Processing a stored record again to compare digests went wrong for an output
that depends on more than the record. A record stored `CONVERT_SKIPPED` (§9), because two workers died converting
it, was converted again in full when it was delivered again: the very work that had stopped them. If it stopped
this worker too, the message came back and stopped the next, without end, since a comparison takes no claim and
counts no attempt (on Kafka the partition stalls). If the conversion worked this time, its digest differed from the
skipped one, and the record was reported as a conflict it wasn't. And every ordinary redelivery was processed again
only to learn it was a duplicate. So, as built:
- **A processor states a version** (`StageProcessor.version`), found without doing the work: everything the output
  depends on beyond the record. The registry keeps it with the digest (`stage_record.output_version`). For the
  convert stage it is the build (version and build time, from Spring Boot's `build-info`, `StageBuild`), the
  converter's mapping (`none` without a converter), and a digest of the settings that change output (the format
  options and `max-bytes`): `0.1.0@2026-11-06T14:48:36Z/csv-row/1/3f9a0c1d22b7`. A rebuild counts as a change.
- **Under the same version** the output must be the same: a duplicate, not processed again.
- **Superseded 2026-11-08 (§14.3, built):** a record received again is a duplicate whatever its version, audited
  `record.version_differs` when they differ, and never processed again; the next three points describe the
  behaviour before.
- **Under another version** the record is processed again, at the attempt it was stored at (the registry's
  `attempts`), so an output decided by attempts comes out the same: a skipped record is skipped again, not
  converted. The same digest is then a duplicate, a different one a conflict.
- **No guard against a comparison that stops the worker (decided).** It can happen only when a stage is changed
  while records are in flight, which isn't expected to be done; when it happens by accident, an operator recovers
  by hand (§13). What is built now is the telemetry that recovery needs: before processing again under another
  version the worker audits `record.compare_started`, which reaches the console log at once, and flushes the
  OpenTelemetry exporters (audit events and metrics are otherwise exported in batches, and the last few seconds die
  with the process). It also counts `sdh.stage.version_differs` (by stage).
- **What every event about a record received again carries** (`record.compare_started`, `record.duplicate`,
  `record.conflict`): the stage, the item ID and the record's sequence number (a record is named by these in
  telemetry, never by its ID), the broker's ID of the message (`sdh.broker_message_id`: `topic/partition/offset` on
  Kafka; none for a record the reconciler rebuilt), the version now and the stored one (`sdh.version`,
  `sdh.stored_version`; `none` for a row from before versions were kept), the attempts it was stored at
  (`sdh.attempts`), and the row's state (`sdh.state`: `STORED` while its message is still to be sent, `SENT`
  after). These four attributes are new on the allow-list (§12 of DESIGN.md).

**What remains:** a worker that stalls for longer than its claim's expiry between checking it and committing
could commit a row a successor also commits. The row is identical (same record, same output), every row carries
its record ID, and readers key on it; maintenance reports any such pair. Claims are renewed every few seconds and
expire after a minute, so it takes a stall of that length at that exact point.

## 7. The shared stage library

Two new modules, both published, so `healthcare-data-transformer` can use them:
- **`connector-broker`: receiving from any broker.** The landing node's queue receive layer moves here from
  `landing-connector` (§5.3.16): the per-broker Camel consumers that hold a message until its handler is done,
  the settings checks that refuse options which would acknowledge early, the health watches (`BrokerWatch` and
  the per-broker checks) and the Artemis route. The publishing side (`QueueType`, `Publishers`) moves here too,
  so one module knows each broker both ways. **Built (step 2):** `receive/` and `send/`. Batching moved with the
  consumers, since it is how they hold messages: a receiver is given a `BatchHandler`, which takes a batch of
  `BrokerMessage`s (body, broker message ID, the attributes it asked for) and answers done, retry or fatal.
  `ReceiveSettings.of(properties, prefix, attributes)` checks the settings, naming them by the caller's prefix
  (`sdh.landing.broker`, `sdh.stage.input`). Health is reported to a `BrokerHealth`; the landing node's
  `IntakeHealth` is one. The log events of the watch are now `broker.*` (`broker.consumer_restarted` and the
  rest). `ResultsChannel` stays in `connector-runtime`: it is about the landing node's producers, not a broker.
- **`connector-stage`: a stage worker.** It receives through `connector-broker`, opens and checks messages
  (`connector-contract`), and gives each one to the stage's processor (an interface: split, convert, transform)
  with the claim registry, the item rows, batched Iceberg commits, sending, the dead-letter queue and telemetry
  around it. Settings: `sdh.stage.input` (type and URI, as the landing node's broker settings), `sdh.stage.output`
  (`sdh.queue` as now), `sdh.stage.dlq`, `sdh.stage.concurrency` (records processed at once per pod), and the commit
  batch (`max-records`, `max-wait`).

  **Built (step 3):** `StageProcessor` is a stage's own work: it reads what a message asks (a record, an item's
  total, or nothing), processes a record into an output and its digest (or a failed record, stored as it was),
  commits a batch of outputs in one append and makes each one's message, and makes the item-complete message.
  `StageWorker` is the `BatchHandler` around it (§6): it opens each message, claims, processes up to
  `concurrency` records at once, commits, marks stored, sends, and completes items, and answers retry when anything
  fails on the way, so the batch is tried again in place and what was already stored is recognised. A processor
  returns an error in a record as a failed output; whatever it throws is taken as passing, and the batch is tried
  again. `StageRegistry` holds `stage_record` and `stage_item` in the lease database (schema
  `sdh.stage.registry-schema`, `sdh_stage` by default). The received batch is the commit batch, so its limits
  (`sdh.stage.input.batch.max-messages`, `max-bytes`, `max-wait`) replace `max-records` and `max-wait`. Claims last
  `sdh.stage.claim.ttl` (60 s), renewed every `renew-interval` (15 s). A message that can't be trusted is audited
  (`stage.message_rejected`) and dropped, one the processor doesn't take is audited (`stage.message_unwanted`) and
  acknowledged, and one of a later contract major version holds the batch back until the stage is upgraded
  (`stage.unreadable_version`). One of an earlier major version, which no upgrade reads, is rejected
  (`stage.message_rejected`, reason `EARLIER_VERSION`), so it blocks nothing behind it; its record is found by
  the reconciler, an item total recovered by replay (§13.2; decided 2026-11-07). Other audit events: `record.duplicate`,
  `record.conflict` (`RECORD_CONFLICT`), `record.claim_lost`, `stage.batch_failed` (`STAGE_BATCH_FAILED`) and
  `stage.item_completed`; none carries a record ID. Messages are still keyed by item ID on the broker; record
  messages move to the record ID in step 4 (§4). Tested by `StageRegistryTest` and `StageWorkerTest` (embedded
  Postgres, real sealing), and `StageWorkerIT` on all seven brokers: a worker stops part-way with records claimed,
  two more run at once, wait out its claims and take them over, and store every record exactly once. Its output is
  kept in memory, since sending on each broker is tested by `file-connector`.

The processor interface knows nothing of formats or mappings: a record's bytes and metadata in, its output (or
the error that stopped it) out. The transform stage's processor loads its WSTL set at startup, from a location in
its settings, and the set's digest goes into each record's output digest, so a record mapped again under a
changed set is seen as a conflict (§6), not a duplicate.

Each stage is then a small application on `connector-stage`, with an image of its own (decided):
`sdh/split-stage` and `sdh/convert-stage` here, the transformer's in its own repository. `file-connector` becomes
the split stage; the convert stage is new.

**Commits.** Many workers append to the same table. A worker gathers the records it has claimed into one Iceberg
append (up to `max-records`, or `max-wait`), which Iceberg retries safely when another worker committed first.
These tables aren't fenced by a lease epoch (§9.3): there is no single writer to fence; the claims do that work
per record. Each record is acknowledged only after its batch is committed and marked `STORED`.

**Duties for one pod only:** table maintenance (§8.4) and reconciling run under a JDBC lease (§9.3) held by one
pod of the stage at a time.

## 8. The split stage

- **Claim per item:** the existing lease machinery, one lease per item (`split/<pipeline>/<itemId>`), renewed
  while the item is split, so a redelivered `item.landed` waits while another worker splits that item.
- **Resume:** the item's checkpoint (resume point, next seq, restarts, §9.1–§9.2), MAC'd as now, moves from a
  table property to the item's row, written after each batch commit. A crash between commit and checkpoint
  re-splits that batch; its records have the same IDs, so the registry drops them as duplicates.
- **The inbox table goes:** the broker is the work queue, and the registry replaces its repeat check.
  Reconciling (§5.3.12) stays: the stage finds landed items with no `stage_item` row after the grace, and sends
  their `item.landed` to its own input again.
- **No conversion:** records are stored as cut, and a piece the splitter can't parse is stored with its error
  (§5.3.16), counted in `failedRecordCount`.

**How it is built (step 4b, 2026-11-05):**
- **One item at a time per pod.** Within an item the pod already uses every core (reading, hashing, encrypting
  and sealing run on worker pools); items run in parallel across pods, which KEDA adds. The handler takes the
  messages of a received batch one after the other, so `sdh.stage.input.batch.max-messages` should be 1: a
  long item would otherwise hold the others past their visibility timeout.
- **The item's lease** is a row in the existing lease table, `split/<connector-id>/<itemId>`, taken without
  waiting. If another worker holds it, the batch is answered retry and tried again after the pause, so a
  redelivered `item.landed` waits for the worker splitting it. The lease is renewed in the background while
  the item is split, and released at the end. Losing it stops the item, not the pod: the next commit or
  checkpoint write finds the lease gone, and the batch is retried, and by then waits for whoever took it over.
  The rows stay in the lease table after release: the epoch must only grow, for fencing.
- **The checkpoint row,** `<registry-schema>.split_item(item_id, epoch, checkpoint, pending, state, ended_at,
  updated_at)`, holds the item's checkpoint, MAC'd as now, and its state in plain text, for counting. `ended_at`
  is set once, when the checkpoint first passes `SPLITTING`, and every `item.split` for the item carries it as
  `completedAt`, so one sent again is identical to the first. Each write is fenced by the lease's epoch
  (`WHERE epoch <= ?`), so a worker whose lease was taken over can't write it again. A batch is committed in
  three steps: the checkpoint after it is written as `pending`; the batch is appended to `split_records`;
  `pending` becomes the checkpoint. At resume, a `pending` checkpoint is kept if its last record is in the
  table, and dropped if not. So no batch is split twice, and the plan's re-split (and its duplicate rows) is
  not needed.
- **Appends aren't fenced by a table epoch:** many pods append to `split_records` at once, which Iceberg
  retries on conflict. The lease is checked just before each append. A worker that stalls between that check
  and its commit, for longer than the lease lasts, could append a batch its successor also appends: the rows
  are identical, carry their record IDs, and the next stage drops the repeated messages as duplicates (§6,
  "What remains").
- **The record-ID key** is created once, on `split_records`, by a compare-and-swap of the table's properties:
  of two pods starting together, one wins and the other reads its key.
- **Messages:** a `record.split` for each record (a piece that couldn't be parsed carries its error code),
  sent after its batch commits, then `item.split` once all of them are acknowledged, with `failure` set if the
  item couldn't be split to its end. No `file.started` or `file.completed`. An `item.landed` for an item
  already finished is answered by sending its `item.split` again (its ID is fixed) and acknowledging.
- **Messages it doesn't take** (another intake, other storage, another type, a bad signature) are audited and
  dropped, as the inbox did; one of a contract version it can't read holds the batch back.
- **Errors:** another pod on the item, a lost item lease, storage, the broker or the lease database down: the
  batch is held and retried in place, with a growing pause. A tampered checkpoint, a commit whose outcome isn't
  known, or a JVM `Error` stops the process.
- **Settings:** `sdh.stage.input` and `sdh.stage.output` replace `sdh.landing.queue-uri` and `sdh.queue`;
  `sdh.stage.registry-schema` holds `split_item`. The item lease uses the `sdh.lease` timings and database.
  `sdh.landing.idle-wait`, `sdh.split.convert*` and `sdh.results` are gone. The application's defaults set
  `sdh.stage.input.batch.max-messages` to 1. `sdh.split.autostart: false` leaves the input and duties stopped,
  for tests that hand messages to the handler themselves.
- **Duties for one pod:** reconciling and table maintenance run under a lease of their own,
  `split/<connector-id>/duties`, taken as a standby by every pod, so one pod has it at a time; losing it sends
  that pod back to waiting, and splitting carries on. Maintenance commits through `LeasedCommitter` on that
  lease, while other pods append: Iceberg checks rewrites against appends, and orphan cleanup's minimum age (at
  least an hour, checked at startup) covers appends in flight. The reconciler splits a missing item itself,
  under the item's lease, rather than sending its `item.landed` again; an item another pod has taken meanwhile
  is left to it. Each run also takes on, from its checkpoint, every item still in progress (`SPLITTING`,
  `ARCHIVING`, `FAILING`) whose checkpoint hasn't been written for longer than the grace, whenever it was landed
  (`split.recovered`, reason `CHECKPOINT_STALLED`; built 2026-11-07): its pod stopped part way and its message
  was lost or purged, so nothing else would. One still being split slowly keeps its item lease, and is left.
- **Conversion** is removed from the pipeline; step 4c builds it again in the convert stage, from the code in
  git history before 4b. `item.result` moves to the convert stage in step 4c too: the split stage sends none
  (`ResultReporter` is left in `file-connector`, unwired, for 4c to move).

## 9. The convert stage

Per record: read the split record, convert it (§6.4), store it in `records`, send `record.stored` (or
`record.failed`, stored in its original form, as now). The limits on conversion and the restarts without progress
(§9.2) become per record: a record that has crashed a worker twice (its claim taken over twice) is stored
unconverted, `CONVERT_SKIPPED`. Reconciling: records split more than the grace ago with no row in the convert
registry are sent to its own input again. It sends `item.converted`, then `item.result` to the producer when the
intake has results on.

**How it is built (step 4c, decided 2026-11-06):**
- **A module of its own,** `convert-stage`, image `sdh/convert-stage`: a `StageProcessor` on the shared worker (§7).
  Its input is the split stage's output; it takes `record.split` and `item.split`, and drops the rest as unwanted.
- **Reading:** a record is read from `split_records` with the reader's own code (`QueueReader.readRecord`), so the
  stage holds `records-kek` and the recipient's private key, and nothing of landing's: no landing catalog, no
  `landed-items-kek`.
- **Converting** is the converter registry of §6.4, per the record's format, with the format options of
  `sdh.convert.options` (as `sdh.split.options`; a CSV record already carries its header row). The limits stay:
  a record over `sdh.convert.max-bytes` is stored as split, `CONVERT_TOO_LARGE`, and the conversions running at
  once share a memory budget. A record on its third claim (two workers stopped part-way through it) is stored as
  split, `CONVERT_SKIPPED`. A piece the splitter couldn't parse is stored as split with the splitter's error.
- **Storing:** every pod appends to `records`, shared like `split_records` (§8): more commit retries, the ID key
  created once by compare-and-swap, appends checked against the claims rather than a table epoch. A stored record
  is announced with `record.stored`, or `record.failed` if stored as split with an error.
- **Duplicates and conflicts** are the worker's (§6). The output's digest covers the stored bytes, the error code
  and the converter's mapping version, so a new converter's different output is a conflict, not a duplicate.
- **The item's totals:** `item.split` gives the record count. The registry keeps the `item.split` as received,
  still sealed, with the item's row, and hands it back opened when the item's records are all stored. The convert
  stage then sends `item.converted`, and `item.result` if results are on. Any later stage can keep its item
  message the same way.
- **The producer's references (decided: per record).** The result names the producer's own reference for the
  item, the item it replaces, and for a queue batch the message each problem is in: its number, broker ID and the
  earlier message it replaces. The split stage reads them from `landed_items` (it holds that key already) and
  passes them on:
  - `item.split` gains `producerRef` and `replaces`; its `failure` is placed in its message as the result needs.
  - `record.split` gains `message`, `messageRef` and `messageReplaces` for a record of a queue batch (−1, null,
    null otherwise). Sealed like the rest of the message.
  - The convert stage keeps them only on a failed record's row in `records`: `message_index` in the clear (a
    position, not PHI), and `message_refs_enc`, the two IDs as a JSON array encrypted with the record's own key
    (as landing encrypts the original name). The result is then built from `records` alone.
  - Nothing grows with the batch: putting every message's IDs in `item.split` could pass the 256 KB limit of SQS
    and Service Bus.
- **Results** are sent after `item.converted` and before the item is marked complete, so after a crash they are
  sent again, with the same message IDs. Each problem names its stage: `split` for a piece the splitter couldn't
  parse (a `PARSE_*` code), `convert` otherwise.
- **Duties for one pod,** as in §8 (`convert/<connector-id>/duties`): table maintenance of `records`, and
  reconciling. The reconciler looks at items with records in `split_records` older than the grace whose records
  aren't all in the registry, rebuilds the missing records' `record.split` from their rows, and hands them to the
  worker as if received. A record claimed but never stored, its claim expired, counts as missing: its message may
  have been lost with the batch that claimed it, and the worker takes the claim over (built 2026-11-07, §10). An
  item whose records are all stored and whose total came, but which wasn't completed, the batch that would have
  completed it lost, is completed (`stage.item_completion_recovered`). An item whose `item.split` never arrived
  can't be completed this way (its producer's references are in landing); it is audited
  (`stage.item_total_missing`, once per item), for an operator to recover by hand (§13.2).
- **The reconciler's watermark** (decided 2026-11-07 with the user). Each run looks back `sdh.reconcile.lookback`
  (2 days), so a record split longer ago than that, whose `record.split` was lost while the stage was down (an SQS
  queue keeps messages 4 days by default; a queue purged during an outage), was never looked at again. So the
  registry keeps how far the reconciler has checked, `stage_reconcile.checked_to` (one row per stage, only ever
  moved forward): every record committed before it has been checked at least once, after its grace. A run looks
  from the earlier of `checked_to` and now less the lookback, to now less the grace, a day at a time (each day's
  record IDs are all a run holds in memory, never more), and moves `checked_to` to the end of each day once it is
  done, so a run cut short resumes where it stopped. Normally `checked_to` is minutes old and the lookback
  decides, as before. After an outage longer than the lookback, the first run catches up on the days in between
  (`stage.reconcile_catching_up` at its start, `stage.reconcile_caught_up` at its end, each with
  `sdh.count`, the days to check). With no row yet (a stage that has never reconciled, or one upgraded to this),
  a run starts at the lookback: nothing older is checked then. Full scans were rejected: they hold every record
  ID ever kept.
- **Waiting for a backlog** (decided 2026-11-07 with the user). A record that looks lost (split more than the
  grace ago, unknown to the registry) may only be queued behind a backlog: after an outage, or whenever the queue
  falls further behind than the grace under load. Rebuilt, it is converted serially on the duties pod, slowing the
  backlog, and a failed one loses its placement. So every pod notes when a record older than the grace comes off
  its queue (`BacklogWatch`: the age is from the commit of the snapshot its `record.split` names; a snapshot
  expired from the table is old), in `stage_backlog.seen_at`, at most once a minute per pod, by the database's
  clock. While that is within `sdh.reconcile.backlog-quiet` (15 minutes), a run rebuilds nothing and leaves the
  watermark, audited `stage.reconcile_deferred` (`sdh.count`: minutes waited). Since every pod notes it, a
  partition still draining anywhere holds the run back, and a queue that delivers nothing (one stuck, or empty)
  doesn't. After waiting `sdh.reconcile.max-deferral` (6 hours), a run goes ahead anyway,
  `stage.reconcile_deferral_capped`, so a queue that is always behind can't hide a real loss for long; then it
  waits again. At startup the pods haven't received anything yet, so a pod newly holding the duties waits one
  `backlog-quiet` whatever the queue shows (`sdh.reason` `STARTING`; `BACKLOG` otherwise). The wait is counted
  from when this pod took the duties. `sdh.reconcile.backlog-quiet: 0s` turns waiting off. (The first build of
  this never ended the wait at startup on a stage that had never seen a backlog; fixed 2026-11-07, revision (bu).) A drained-queue signal
  from the brokers was rejected: there is none common to the seven.
- **Settings:** `sdh.stage.*` (§7); `sdh.convert.options`, `.max-bytes`, `.memory`; `sdh.stage.source` and
  `sdh.stage.sink` for `split_records` and `records` (§17.7, replacing `sdh.storage` since §17 step 3);
  `sdh.results` as before; `sdh.lease` for the duties lease.

**As built (2026-11-06):** since §17 step 3 (§17.8c) the classes named here are the shared record stage's
(`RecordStageProcessor`, `RecordStageRuntime`, `RecordReconciler`, `SourceInputs`), with the conversion in
`ConvertTransform`; `sdh.storage.split-namespace` became `sdh.stage.source.namespace`, `sdh.convert.autostart`
became `sdh.stage.autostart`, and the audit events moved from `convert.*` to `stage.*`, with `sdh.stage`.
- `ConvertProcessor` is the stage's `StageProcessor`; `ConvertRuntime` wires it with keys from
  `KeyMaterial.forStage` (the records KEK, signing, recipient and inbox keys; no landing KEK) and a `QueueReader`
  built without message keys, which only reads records (the worker opens the messages). `sdh.storage.split-namespace`
  names the namespace of `split_records` if it differs; `sdh.reconcile.grace`, `.lookback`, `.interval`,
  `.backlog-quiet` and `.max-deferral` tune the reconciler; `sdh.convert.autostart=false` leaves the input and
  duties stopped for tests.
- A record stored as split (too large, skipped, a splitter's error, or a format with no converter) isn't read until
  it is committed: its digest is of its split record ID. A splitter's error code this version doesn't know is
  stored as `PARSE_UNRECOGNISED`.
- The registry keeps the received `item.split` in `total_message` on the item's row; the worker opens it again for
  `itemComplete` and the new `itemCompleted` step, which runs after the item-complete message is sent and before
  the item is marked complete. If it no longer opens (its key retired), `stage.item_total_unreadable` is audited,
  `item.converted` names the intake `unknown`, and no result is sent (`stage.result_not_sent`).
- A failed record's row keeps `message_index` and `message_refs_enc` (`RecordBatchWriter.place`), read back by
  `CommittedRecords.failed(table, item, keys)`.
- **Two copies of a record.** A worker that loses its claim between checking it and committing can append a record
  a successor appends too (§6). The reader now reads a record's chunks from the batch of the first row it meets and
  skips rows of the same record from another batch; `CommittedRecords` reads each record once.
- The duties are the split stage's, moved to `connector-stage` as `StageDuties` (lease `<stage>/<id>/duties`),
  with `StageFailures` deciding what is fatal. The reconciler hands rebuilt messages to
  `StageWorker.handleOpened`, using the registry's `knownRecords` and `completed`. A rebuilt `record.split` has no
  queue message, so a failed record found this way is reported without one, and audited
  `stage.placement_unknown` (stage, item, record number, error code) for the operator (§12). A record of a
  batch of one message, or of a file, loses nothing.
- `ResultReporter` moved here: built from `item.split` and the failed rows; a `CONVERT_*` code is put down to
  `convert`, any other to `split`.
- Left for 4d: the image (`sdh/convert-stage`), distribution kit and integration tests; the conversion settings
  still in `PipelineSettings`.

## 10. KEDA

A `ScaledObject` per stage, on its input: Kafka consumer lag, SQS queue length, Pub/Sub undelivered messages,
Service Bus or RabbitMQ queue length, Event Hubs unprocessed events, Artemis queue depth. KEDA has a scaler for
each of the seven. Kafka topics get as many partitions as the stage's `maxReplicaCount` × `concurrency` needs;
the other brokers have no such limit. A pod shuts down by stopping its consumer, finishing and committing what it
holds, and handing back the rest, so scaling down loses nothing (and what it couldn't finish is delivered again).
This allowed `minReplicaCount` 0, at the cost of a JVM start (about 15 s) on the first message; corrected
2026-11-07, below: never fewer than one.

The sandbox gets the KEDA operator, the stages as scaled deployments, and topics with partitions.

**Decided 2026-11-07 with the user:**
- **At least one pod per stage, always** (`minReplicaCount: 1`). The pod holding a stage's duties runs its
  reconciler and table maintenance, and a lost message adds no lag: a stage scaled to nothing would never be woken
  to recover it.
- **The landing node is never scaled.** It lands serially: parallel landing could land one file twice, through the
  variants of how a file is retrieved. Exactly one node is in service, the one holding its lease; a standby waits
  and takes over if it fails (as built, its README). KEDA never targets it.

**As built in the sandbox (step 5, 2026-11-07):**
- KEDA 2.21.0, installed by `scripts/sandbox.sh up` from its release manifest, checked against a pinned SHA-256.
- A `ScaledObject` per stage (`deploy/sandbox/pipeline/scaling.yaml`) on its Kafka consumer group's lag: the split
  stage (group `splitter-<p>`, topic `<p>.landed`) one pod per 5 items waiting, the convert stage (`convert-<p>`,
  `<p>.split`) one per 5 records (20 until 2026-11-07, which it no longer reached once every partition was
  committed, below); up to 2 pods each, what the sandbox's memory is sized for (decided 2026-11-07
  with the user, after the control-plane node ran out of memory with 2 split and 3 convert pods; docs/SANDBOX.md
  "Memory"); back to one two minutes after the backlog is gone. The Deployments' `replicas` is removed, so a
  deploy doesn't reset KEDA's count.
- The sandbox's landing node is one pod with no standby (decided 2026-11-07 with the user, for memory): if it
  dies, Kubernetes starts another, which lands once the old pod's lease has expired. The README's Deployment
  keeps its active node and standby.
- The stages' input topics have six partitions; the others one, until the FHIR stage scales. Partitions are only
  added, with the topic drained, since a key's queued messages stay on their old partition.
- `scripts/sandbox.sh scale-demo [count]` pushes copies of the sample documents, each its own item, and reports
  how many pods each stage ran and how the work was shared, counted in the stages' registries (docs/SANDBOX.md).
- **Scaling down, as found:** a convert pod that stops doesn't hand back what it holds, as said above: the worker
  stops (`StageWorker.close`, "held claims expire"), and its claimed records wait for their claims to expire
  (`sdh.stage.claim.ttl`, 60 s) before the broker's redelivery is taken by another pod. Safe, but a minute late:
  in the first demo, 2 of 300 records were converted on a second claim, about 50 s after their pod went. Leases
  (the duties, the split stage's items) are released at shutdown. Fixed (bx): `StageWorker.close` stops renewing
  and processing, waits for its threads to stop (at most the claim's time to live), and then releases the claims
  it holds on records it didn't store (`StageRegistry.release`: they expire now; `stage.claims_released` with the
  count). Another pod takes them over at the broker's redelivery, still as one more attempt, as after an expiry,
  so the third-claim rule above is unchanged. If its threads don't stop in time, or Postgres can't be reached
  (`stage.claims_not_released`, with the reason), the claims are left to expire, as after a crash. Measured
  (2026-11-07, scale-demo 300, one pod deleted and three scaled in): each convert pod stopped in 1.6 to 5 s of
  its 30 s grace, Camel's drain of the batch in hand being most of it; the release itself took under 5 ms.
- **A Kafka batch given up was passed by (found and fixed 2026-11-07, revision (by)).** Two of the 300 items
  were never converted. A pod's batch failed as it stopped (`stage.batch_failed`, ExecutionException, cause not
  kept), and the receiver gave it up unhandled, as meant; but Camel's Kafka consumer only logs that, polls on
  from past it, and the next poll's commit moved the partition past the failed batch (`ccda-push.split`
  partition 0, 755 and 756: one item's total, another's record). A FATAL batch would have been passed the same
  way. And Camel commits only the last record's partition of a poll (it copies that record's
  `CamelKafkaManualCommit` to the batch), so the poll's other partitions lagged until a later poll ended on
  them: duplicates at each rebalance, and a lag that never cleared at the end of a run, which the reconciler's
  backlog watch counts. Fixed in the receiver (`KafkaOffsets`): a handled poll commits each partition's last
  record; a poll not handled seeks each of its partitions back to its first record, on the polling thread,
  before giving up, so its records come again (Camel's own `breakOnFirstError` seeks only one partition). And
  in the reconciler, which hadn't repaired either item: a claim expired with nothing stored no longer counts as
  known, and an item stored in full with its total is completed (§9). Checked on the sandbox, 2026-11-07: the
  lag left by the old commits cleared at the first poll after the upgrade; the next reconciler run rebuilt the
  record left claimed and completed the other item (`recovered=1 completed=1`); and two scale-demo runs of 300,
  one with two busy convert pods deleted under load, converted all 300 each, with lag 0 after (4 records on a
  second claim). In those runs the convert stage no longer scaled out: its lag, no longer inflated by
  uncommitted partitions, stayed under KEDA's 20, so one pod kept up, and `scale-demo` reports that as a
  failure; the split stage still scaled to two. Lowered to 5: in the next `scale-demo 300` both stages scaled out
  and the run passed, but the convert lag peaked at 7, so a second convert pod ran only about a seventh of the
  time, two short-lived ones storing 26 records to the first pod's 274 (3 on a second claim, their pod stopped).
  One convert pod keeps up with two split pods at this load.
- **A Kafka consumer left its group while it held a batch (found and fixed 2026-11-07, revision (bz)).** Camel
  handles a poll on the thread that polls, so while a batch was handled the consumer didn't poll, and past
  `max.poll.interval.ms` (Kafka's default 5 minutes) it left its group: its partitions went to another pod, the
  rebalance waited for it, and its commit failed, so its records were handled again. Two ways: a batch retried in
  place through an outage (the memory incident, Vault sealed: the split stage's consumers left their group), and
  a split item taking longer than the interval (one item per message; at §13.1's rates a 10 GiB CSV takes about
  24 minutes on 16 cores). Fixed: on Kafka a batch to be tried again is no longer retried in place, since Kafka
  keeps no delivery count that polling again would raise; after its pause (at most 1 minute,
  `CamelReceiveRoute.KAFKA_LONGEST_PAUSE`) the poll's handled records are committed, the rest sought back
  (`KafkaOffsets`), and the next poll brings them again. The stages' longest pause is 1 minute, not 5, on every
  broker. The split stage sets `maxPollIntervalMs` to 30 minutes unless its URI does; an item taking longer
  still is moved to another pod, which waits for the item's lease, and is announced again: nothing lost or split
  twice, but its partition waits. If that is seen, the fix is a Kafka consumer of our own that pauses its
  partitions and keeps polling while a batch is handled. The sandbox's stages use Kafka 4's group protocol
  (`groupProtocol=consumer`, KIP-848), under which a rebalance (KEDA scaling) doesn't wait for a busy pod's next
  poll; it isn't the default, since a broker before 4.0 refuses it. Checked by `KafkaRewindIT`, under both
  protocols: a batch retried for three poll intervals is handled once and committed, where before the consumer
  left its group (4 warnings) and, under the classic protocol, handled the records twice. Checked on the
  sandbox, 2026-11-07, `scale-demo 150` with Vault sealed for 8 minutes once 51 items had landed: the split
  stage held 50 items and the convert stage one item's records through it (13 tries, `stage.batch_failed`), no
  consumer left its group (no poll timeouts, rejoins or failed commits in the logs), the outage wasn't counted
  as restarts (`item.restart_not_counted` 7, no `RESUME_NO_PROGRESS`), and after it every item landed (110) was
  split and converted, none twice. Long items weren't tried there (its landing node takes 100 MiB at most).

## 11. Order of work

Each step builds and passes its tests before the next.
1. **Rename** section → record and file → item where it means an item; contract 3.0. Mechanical, across about
   170 files. Done 2026-11-05.
2. **`connector-broker`:** move the receive and publish layers; the landing node's queue intake uses it unchanged.
   Done 2026-11-05 (§7).
3. **`connector-stage`:** the worker, registry, item rows, batched commits, dead-letter queue; tested with a test
   processor against every broker's integration test. Done 2026-11-05 (§6, §7).
4. **The split stage** on it (from `file-connector`), and **the convert stage**; the new messages. In four parts
   (decided 2026-11-05), each building and passing its tests:
   - **4a:** the contract: `record.split`, `item.split` and `item.converted`, and record messages keyed by record
     ID on the broker (§4). `record.split` carries the storage fields `record.stored` does, so a split record is
     read with the reader's own code. The transform stage's messages are defined in step 6, with its tables.
     **Done 2026-11-05:** each message names its `orderingKey()` (the item, or the record for record messages and
     `record.conflict`), `OutboundMessage` carries it as `key`, and every adapter keys by it.
   - **4b:** the split stage: `file-connector` splits with conversion off into `split_records`, under a lease per
     item, its checkpoint in the item's row, without the inbox (§8). The module keeps its name; its image becomes
     `sdh/split-stage` (decided). **Done 2026-11-05.** The same content in two items is a record of each (decided:
     "copy per item"): record message IDs and the stage registry's key include the item ID. Left for 4d: the
     admin commands and their test (disabled until then), and the demo, sandbox, image and integration-test
     settings, which still name the inbox's; the integration tests compile but aren't run.
   - **4c:** the convert stage, a new module `convert-stage` on `connector-stage` (§9), image `sdh/convert-stage`.
     **Done 2026-11-06** (as built in §9). The producer's references go per record (decided). Unit tests on an
     in-memory catalog and embedded Postgres; the image and integration tests are left for 4d.
   - **4d:** `file.started` and `file.completed` removed; the demo, sandbox, images, integration tests and docs
     moved to the stages; the admin commands (`restore`, `decrypt-fallback`, `status`) adapted to the per-item
     rows and `split_records` (decided).
     **Built 2026-11-06, in parts:**
     - `file.started` and `file.completed` removed from the contract (still 3.0: nothing released), with the
       "before contract 2.1" notes; a null origin now means the splitter gave none.
     - The admin commands (decided 2026-11-06 with the user): the CLI moved to `connector-stage` (`stage.admin`), so
       every stage has one. The split stage has `restore --item-id` (the unprocessed tail from `split_records`)
       and a `status` of its duties lease, items by state and item leases, with `--item` for a checkpoint; the
       convert stage has the shared `status` of its registry. `decrypt-fallback` and `RecoveryEnvelope` are
       removed: nothing writes fallback files since step 3a, and none exist. The old inbox is removed.
     - The convert stage's images (`sdh/convert-stage`, `sdh/convert-stage-service`) and distribution kit, built
       like the others, with its attribution checked. The split stage's images are renamed `sdh/split-stage`; its
       module and kit keep the name `file-connector`. The kits' key settings keep the names `inbox` and
       `landing-signer`: for each stage, its own key pair and the previous stage's signing key.
     - The integration tests on the stages: the storage tests (S3, GCS, ADLS, Polaris) feed the split stage
       directly and check `split_records`; `FullStackIT` runs the split stage in its JVM and the convert stage from
       its JAR, over Kafka, and reads the records back downstream; the image tests run all three apps as images,
       locked down, through failover. All pass. They found a bug: retention's purge marker was an empty commit,
       which the stages' committer (`LeasedCommitter`) doesn't record, so expired records' files stayed on
       storage until the next snapshot expiry. The marker is now an empty append, which every committer records.
     - The demo runs the convert stage as a third app per pipeline, and follows each item to its
       `stage.item_completed`. It found a second bug: the convert stage failed to start with no format options
       set (the tests always set one); none is now an empty set. The sandbox deploys a convert stage per pipeline
       (its own Vault signing key, inbox key pair, S3 key, database role and registry schema, and the topics
       `<pipeline>.split` and `<pipeline>.convert.dlq`); the FHIR stage's handover names the convert stage's
       signing key. Design §11, §14, §15.3 and SANDBOX.md describe the stages.
5. **KEDA** in the sandbox, with a scaling demonstration (a large batch spread across workers). Done 2026-11-07
   (SANDBOX.md, "Checked").
6. **The transformer** in `healthcare-data-transformer`, on the published library.

## 12. Open for review

- **A lost `item.split`** (§9). Resolved 2026-11-06: found by a query over the stages' `stage.item_completed`, and
  recovered by hand by replaying the item (§13.2). No automatic recovery: a message lost after the broker took it
  points to a larger problem with the infrastructure, which needs an operator anyway (decided).
- **Replay tooling** (§13.2): an admin command per stage that sends all its output for a completed item again,
  with the rules of §13.2. Landing's, `replay --item-id`, built 2026-11-07; the split and convert stages' the
  same day (revision (cc)).
- **`QueueReader` doesn't drop repeated messages.** Resolved 2026-11-07 (revision (cc)): `decodeNew` drops a
  message whose contract ID it has decoded recently (DESIGN.md §10.2); `decode` still returns every one.
- **Tests still to write for the operator commands** (found 2026-11-08, trying revisions (cb) and (cc) on the
  sandbox): the convert stage's `replay` is tested only through `ConvertProcessor.replayMessages`, not as a
  command, so nothing tests that `--with-result` with results off is refused before anything is sent; and no test
  runs an operator command with an HTTP push node's configuration, which is how the web server it was given went
  unnoticed (`WebServerSwitchTest` covers the switch alone). `QueueReader.decodeNew` is tested only as a unit,
  since nothing in the sandbox reads the records yet.
- **Reprocess** (§15.8), proposed 2026-11-09 for review before it is built: runs recorded by the operator and
  done by the stage's pods; outcomes `same`, `different` (stored with `supersedes`) and `failed`; open failures
  matched exactly and `SUPERSEDED`. Four points to decide there: a run's pace beside live data, whether a declined
  failure may be superseded, an item the split stage now cuts differently, and an item-level message downstream.
- **A stage changed with records in flight** (§13.1): proposed 2026-11-08, a version at every stage, kept with the
  data, and a mandatory drain check (§14).
- **Editing bad data**, to revisit later: an item or record whose data won't let it past a stage, and must be edited
  or cleaned. Replay sends data again unchanged; an edit changes it. Under the rule that nothing is lost or
  replaced, an edited item is probably a new item linked to the original, with who changed what recorded, never a
  change in place. Proposed 2026-11-08 (§15): the mechanism and its operator commands; the front end, and who may
  read and change what, still to design.
- **A redelivered record stored as `CONVERT_SKIPPED`** (§9). Resolved 2026-11-06: versions, comparing at the stored
  attempt, and recovery by hand from a stage changed in flight (§6, §13).
- **A reconciled record has no queue message** (§9). Resolved 2026-11-06: its gap is audited
  (`stage.placement_unknown`), and nothing new is stored. Producer results are not guaranteed (DESIGN §5.3.8):
  the operator finds problems from what the pipeline records and resolves them with the producer, by steps both
  agree (§13.3). Rejected for now: keeping `message_index` and `message_refs_enc` on every `split_records` row of a
  batch, until a producer needs results to be complete.
- **The operator's problem report**, to build later: `sdh-admin problems --item <id>`, run as `SDH_OPERATOR`,
  audited and read only, lists an item's failed records with their places, working out a record's message from
  landing (`message_refs_enc`, `message_replaces_enc` and the message index) on request, for the records a result
  couldn't place.
- **The convert reconciler overlapped the queued backlog** after an outage, or whenever the queue fell more than
  the grace behind: it took queued `record.split`s for lost, and converted them serially on the duties pod. Resolved
  2026-11-07: it waits while records older than the grace still come off the queue, up to a limit (§9).
- **An outage can fail a valid item as poison** (found 2026-11-07 in the sandbox, SANDBOX.md "Checked";
  resolved the same day, DESIGN.md §9.2). The split stage counts every resume of an item as a restart without
  progress, before any work, so that a crash that kills the worker still counts
  (`IngestPipeline.FAIL_AFTER_RESTARTS`, 3; `RESUME_NO_PROGRESS`). An attempt that failed cleanly because a service
  was down (Vault sealed, storage unreachable) counted the same, so three retries during an outage failed a valid
  document. Decided 2026-11-07: an attempt whose error says a service was down (`Outage`, an allow-list) takes its
  restart back, and `item.restart_not_counted` is audited; crashes and every other error count as before. Still
  open until 2026-11-07, revision (cb): an item already failed this way couldn't be split again, since a replay
  doesn't re-split a `FAILED` item. Decided with the user: the landing node's `resubmit` lands it again as a new
  item that replaces it, rather than splitting it again in place (§13.5).
- Split records as bronze, in their own namespace and table (§3).
- Postgres for the registry and item rows (§5–§6): the lease database the stages already use. A row per record
  per stage, kept as long as the records are (it is how duplicates are recognised); about 100 bytes each.
- `item.result` to producers sent by the convert stage, after conversion (§4).
- Moving publishing into `connector-broker` with receiving (§7). Done in step 2.

## 13. Recovery by hand

Situations the stages leave to an operator, how to recognise them, and what to do. Tooling for these is to be
built later, from what this section needs; until then a step that needs it says so.

### 13.1 A stage changed while records were in flight

**Superseded 2026-11-08 (§14.3, built):** a record received again under another version is no longer processed
again, so the loop below can't happen; the redelivery is a duplicate, audited `record.version_differs`. Processing
again under a new version is an operator's reprocess (§15.4). What follows describes the behaviour before.

Stages are changed only once their queues are drained (decided 2026-11-06). If one is changed with records still
in flight, a record delivered again may meet a different version than it was stored with (§6). It is then processed
again to compare, and one of three things happens.

**Recognising it:** `sdh.stage.version_differs` above zero for the stage, and a `record.compare_started` event for
each such record, with the fields of §6.

| What follows `record.compare_started` | What it means | What to do |
|---|---|---|
| `record.duplicate` | the change didn't change this record's output | nothing |
| `record.conflict`, and the record on the stage's dead-letter queue | the change gave a different output; the first, stored under the old version, is kept | decide whether the first output is acceptable: usually it is (it is what an undisturbed run of the old version gives). If not, the record is to be processed again under the new version as a resubmission linked to the first, never replacing it (decided); needs tooling |
| nothing, and the pod restarted | processing again stopped the worker; delivered again, it stops the next | below |

**A comparison that stops the worker, step by step:**
1. **Confirm the loop.** The stage's pods restart over and over (on Kafka, one partition's consumer lag grows while
   the others drain). The same `sdh.item_id` and `sdh.seq` appear in `record.compare_started` after each restart,
   with no outcome. The console log (`kubectl logs --previous` on a restarted pod) has the event even when the
   collector didn't receive it.
2. **Identify the record:** stage, item ID and sequence number, `sdh.broker_message_id` for where it sits on the
   queue, `sdh.version` (the build now) and `sdh.stored_version` (the build it was stored with), and `sdh.state`.
3. **Stop the loop by rolling back.** Deploy the build named in `sdh.stored_version` again: the image built from
   it, whose version and build time are those of its jar. The record then meets its own version, is a duplicate
   without being processed, and is acknowledged; anything stored but unsent is sent, as for any duplicate. Needs no
   tooling.
4. **Then let the queues drain,** and redeploy the new build with nothing in flight.
5. **If rolling back isn't possible** (the old build is withdrawn, or other records already need the new one): the
   delivery is to be marked a duplicate without processing it. Needs tooling: an admin command that names the stage,
   item and sequence number. Before it acknowledges, it must check the row's state: a `STORED` row's message has
   not been sent, and must be sent first, or the next stage never gets the record.
6. **Afterwards:** the records audited `record.compare_started` under that build are listed for review, since
   their stored outputs come from the old build.

**To build later, for this section:** the admin command to mark a delivery a duplicate (step 5); a way to process a
conflicting record again under the new version as a linked resubmission (the conflict row of the table); and a
listing of the records compared under a given pair of versions (step 6).

### 13.2 An item lost between stages

**Replaced by §15.8 (decided 2026-11-09):** replay becomes reprocess, at the stage after the one replayed
from (§15.8.9). What follows is kept as the record of what was built.

Delivery is at least once, and a stage acknowledges a message only once it has done all it leads to, so a message
is lost only after a broker took it: it expired before the next stage read it (Kafka and Pub/Sub keep 7 days by
default, SQS 4, Service Bus a time to live), the broker dead-lettered it after its most deliveries (Service Bus and
Artemis default to 10), the next stage dropped it as untrusted (`stage.message_rejected`: a key rotated with the
message still queued), or an operator purged the queue. Each points to a larger problem with the infrastructure,
so recovery is by hand (decided 2026-11-06).

**What is recovered without an operator, and what isn't:**
- A lost `item.landed` is found by the split stage's reconciler, which splits the item from `landed_items` (§8):
  every landed item at startup, then those landed since its last run. So is an item its pod left part way whose
  message was then lost: its checkpoint stalled for longer than the grace (built 2026-11-07).
- A lost `record.split` is rebuilt by the convert stage's reconciler from `split_records`: for items split within
  `sdh.reconcile.lookback` (§9), and, after an outage longer than that, for every day since its watermark
  (§13.4). So is one whose claim a pod left to expire without storing it, and an item whose records are all
  stored, with its total, is completed (built 2026-11-07, §10).
- Nothing else. A lost `item.split` (its producer's references are known only to landing), and anything a stage
  sent on after it, such as `record.stored`, `item.converted` or `item.result`, stay lost: a stage re-sends only
  what it stored and hasn't yet sent, and completes an item once. Corrected 2026-11-07: this said a lost
  `item.landed` stayed lost too, but the split stage's reconciler has always recovered it.

**Recognising it.** Every stage audits the end of an item the same way, once: `stage.item_completed`, with
`sdh.stage`, `sdh.item_id`, `sdh.result` (`COMPLETED` or `FAILED`), `sdh.count` (its records) and
`sdh.failed_count`. Landing's counterpart is `item.landed`. An item lost between two stages is one that ended at
one and not at the next after a grace, found by a query over the audit events: `item.landed` without
`stage.item_completed{stage=split}`, then `stage.item_completed{stage=split}` without `{stage=convert}`, and so on.
This also finds an item with no records (an empty file, or one that failed before its first record), which the
convert stage's reconciler can't see, since it starts from `split_records`. The reconciler adds
`stage.item_total_missing` (item, count of records) once per item for an item whose records are all converted
but whose `item.split` never came.

**Recovering: replay the item from the last stage it ended at.** A replay sends all of a stage's output for one
item again, as it first sent it, whatever its rows say. The next stage drops what it already has (each record is a
duplicate under the same version, not processed again, §6; the total can't change; a completed item isn't
completed twice), processes what it lacks, and completes the item. Then the same at the stage after, if needed.
Per stage, a replay sends:
- **landing:** the item's `item.landed`. The split stage splits an item it hasn't, and sends the `item.split` of
  one it has finished again, unchanged (`ended_at` is fixed). This is all a lost `item.split` needs, so it was
  built first (2026-11-07): `sdh-admin replay --item-id <id>` on the landing node (design §11.4);
- **split:** every `record.split` of the item, rebuilt from `split_records` and placed in its landed message as
  when first sent, then its `item.split`, rebuilt from its checkpoint: `sdh-admin replay --item-id <id>` on the
  split stage (built 2026-11-07, revision (cc));
- **convert:** every `record.stored` and `record.failed`, rebuilt from `records`, then `item.converted`, with the
  counts and completion time the registry kept; the producer's `item.result` only if asked (below), with
  `--with-result`: `sdh-admin replay --item-id <id>` on the convert stage (built 2026-11-07, revision (cc));
- **transform** (step 6): the same, from its own tables.

**Rules, so a replay harms nothing:**
1. **Only a completed item** (its `stage.item_completed` sent): replaying one still in progress would send its
   item-complete message early. A replay of any other is refused.
2. **Its own broker de-duplication ID.** Artemis (`_AMQ_DUPL_ID`, a persistent cache), Service Bus (duplicate
   detection, up to 7 days) and SQS FIFO (5 minutes) drop a message whose ID they have seen, without error, so a
   replay with the original IDs could do nothing at all. A replay sends `<messageId>~replay-<n>` as the broker's ID
   and keeps the contract's `messageId` in the body: the stages recognise records by item and record ID, not by
   broker ID, so nothing downstream changes.
3. **The producer's result only on request.** The contract allows `item.result` again (a resend after a crash has
   the same IDs), but producers are outside organisations, and a second result could set off their own handling.
   It is sent again only when the operator asks, for an item whose result was lost.
4. **Readers outside the stages** must drop the repeats: `QueueReader.decodeNew` does, by the contract message ID,
   which for a record is its item and record IDs (built 2026-11-07, DESIGN.md §10.2).
5. **Under the build that stored it,** where possible: under a newer one each record meets another version
   downstream and is processed again to compare, with §13.1's risk.

**Limits:** only what is still kept can be replayed (records past retention can't be rebuilt), and a rebuilt record
message names no queue message, so if a record that was missing fails at the next stage, the producer's result
names no message for it (audited `stage.placement_unknown`; §13.3).

**Built so far:** landing's replay (2026-11-07), which recovers a lost `item.landed` or `item.split` on every
broker. It rebuilds the message from the item's summary row: the one first sent, naming the table's current
snapshot instead of the one it was landed in (the split stage reads the current table either way). For that, the
landing node keeps each batch's time to the microsecond, as the table does, so the rebuilt `landedAt` matches.
The split and convert stages' replays followed the same day (revision (cc)). Each rebuilds its records' messages
from its table, naming the table's current snapshot, which the reader reads the same, and its item-complete message
from what it kept: the split stage's checkpoint, with when splitting ended, and the convert stage's registry, with
its counts and when it completed. Both refuse an item not finished, and send under broker IDs of their own.

### 13.3 Telling the producer

Producer results are not guaranteed (DESIGN §5.3.8, decided 2026-11-06). The operator finds problems in a
producer's data from what the pipeline records (the audit events, `stage.item_completed`, the failed rows of
`records`), and works out with the producer how to resolve them, by steps both organisations agree. Results help a
producer find its own data, but a producer must not rely on them being complete.

**Where results can have gaps:**
- **A record reached its stage by reconciling** (§9): its result names no queue message. Audited
  `stage.placement_unknown`, with the item and record number.
- **A replay** (§13.2) sends no result unless the operator asks.
- **An item lost between stages** has no result until it is recovered (§13.2).

**Placing a record by hand,** until the problem report exists (§12): the record number of
`stage.placement_unknown` gives its byte offset from its row in `split_records`; the message it is in is the last
of the landed item's message index that starts at or before it, and its broker ID is that entry of the item's
`message_refs_enc`. Only the split stage's key (`landed-items-kek`) opens it, so this is done under the operator's
account, and recorded in the ticket.

### 13.4 Starting again after a long outage

When the convert stage, or its duties pod, has been down longer than the reconciler's grace (10 minutes), or its
queue has fallen further behind than that, the reconciler works differently than usual, and the first start after a
long outage takes longer. That is expected, and safe for the data;
this section is what the operator needs to know to recognise it and follow it.

**Why it takes longer.**
- **The reconciler catches up a day at a time.** It runs once at startup and then every `sdh.reconcile.interval`
  (15 minutes). It checks every record split since its watermark (`stage_reconcile.checked_to`, §9), or since the
  lookback if that is later. Each day costs a scan of that day's `split_records` IDs and a registry lookup per item,
  so a week's outage is seven times a normal run's reading before it finds anything. It saves its progress after
  each day: a pod restarted part-way resumes at the next day, not the start.
- **The reconciler waits for the backlog first.** The `record.split` messages queued during the outage are still on
  the broker, and the reconciler can't tell a queued message from a lost one. So it doesn't rebuild anything at
  startup: it waits one `sdh.reconcile.backlog-quiet` (15 minutes) for the pods to start receiving, and then for as
  long as records older than the grace are still coming off the queue on any pod (§9). Only once the queue has been
  quiet for 15 minutes does it check the days it missed, and by then what it finds is what was really lost. Records
  lost during the outage are therefore converted only after the backlog has drained.
- **Unless the backlog takes longer than `sdh.reconcile.max-deferral`** (6 hours). Then the reconciler goes ahead
  anyway (`stage.reconcile_deferral_capped`), and the old overlap returns for that run: it rebuilds records still
  queued and converts them itself, serially, on the duties pod, while the other pods read the same records off the
  queue. Each record is still converted once (whichever claims it first stores it; the other copy is a duplicate,
  acknowledged and dropped, §6), but the backlog drains more slowly, the duties pod is busy (its CPU, and its memory
  budget, `sdh.convert.memory`) until the run ends, and a failed record it reaches first loses its placement
  (below). After that run it waits again. For an outage whose backlog will take longer than 6 hours to drain, raise
  `max-deferral` for the restart.
- **A rebuilt record that fails loses its placement.** A record that failed conversion, and that the reconciler
  reached before its queue message, has no queue message to name in the producer's result: audited
  `stage.placement_unknown` (§13.3). With the wait, this is mostly a record whose message really was lost, or one
  reached by a run that went ahead after the most wait. The record and its error are stored as for any other; only
  the result's pointer to the producer's message is missing, and it can be placed by hand (§13.3).
- **An upgrade to the watermark starts at the lookback.** A stage with no `stage_reconcile` row yet (first started,
  or upgraded from a build before 2026-11-07) checks only the last `sdh.reconcile.lookback` its first time. A record
  lost longer ago than that, before the upgrade, isn't found; the audit query of §13.2 is what finds its item.

**What to watch.**
- `sdh-admin status` (convert): the `reconciler` line shows how far it has checked and how long ago that is.
  Normally under half an hour. More than an hour behind, it warns: the duties pod is down (the `duties` line says
  who holds the lease, and when it was renewed), it is waiting for a backlog, or it is catching up.
- `stage.reconcile_deferred` at each run that waits, with `sdh.reason` (`STARTING` or `BACKLOG`) and
  `sdh.count`, the minutes waited so far. After a long outage, expect `STARTING` once, then `BACKLOG` every
  `sdh.reconcile.interval` until the queue drains. `stage.reconcile_deferral_capped` means it stopped waiting
  after `sdh.reconcile.max-deferral`: the backlog is draining more slowly than that, or the queue is always behind
  by more than the grace, which on a normal day calls for more pods.
- `stage.reconcile_catching_up` at the start of a run that looks further back than the lookback, and
  `stage.reconcile_caught_up` at its end, each with `sdh.count`, the days checked. A `catching_up` with no
  `caught_up` after it means the run stopped part-way (the pod restarted, or lost the lease): the next run picks
  up from the last day checked. The `convert.reconciled` log line ends every run, with `days=`.
- `stage.records_recovered` per item rebuilt, with `sdh.count`. Any at all means a `record.split` was lost
  (§13.2), unless it follows `stage.reconcile_deferral_capped`: then many are records still on the queue.
- `stage.item_completion_recovered` per item completed by the reconciler, with `sdh.count`, its records: all
  were stored with its total, but the batch that would have completed it was lost (§10). Nothing to do; any at
  all is worth comparing with `stage.batch_failed` at the time.
- `stage.placement_unknown` per failed record rebuilt (§13.3): each needs placing by hand if the producer is to
  be told where it was.
- `stage.item_total_missing`: an item whose records are all converted but whose `item.split` never came; replay
  it from landing (§13.2). During catch-up this can appear for an item whose `item.split` is still queued behind its
  records; it is reported once, and needs action only if the item still hasn't completed (no
  `stage.item_completed{stage=convert}`) once the queue has drained.
- **The outage against the brokers' retention.** Kafka and Pub/Sub keep 7 days by default, SQS 4, Service Bus a
  time to live. An outage longer than retention loses queued messages: the reconcilers rebuild the records, but a
  lost `item.landed` beyond the split stage's own reach, or a lost `item.split`, needs the audit query and replay of
  §13.2. Check the retention configured on each queue before a planned outage, and raise it if the outage may be
  longer.
- **The audit query of §13.2** (each stage's `stage.item_completed` against the one before) is the check that
  nothing was left behind, once the queues have drained and the reconciler has caught up.

**Before a planned outage,** drain the queues first where possible (stop the producers' intake and let each stage
empty its input): then nothing is queued when the stage starts again, and the reconciler waits only the 15 minutes
at startup.

**Its limits.** The wait is judged from what the pods receive, not from the broker, so it is a proxy: a partition
whose consumer is stuck (a message held, §13.1) delivers nothing and doesn't hold the reconciler back, which is as
it should be, since its records won't arrive. A record lost during a long drain is found only after the drain, or
after `max-deferral`. The time waited is counted from when this pod took the duties, so if the duties move to
another pod while waiting, the count starts again.

### 13.5 An item failed by an outage

Before 2026-11-07, a split attempt that failed because a service was down counted towards `RESUME_NO_PROGRESS`
the same as a crash (§12), so an outage could fail a valid item. Since then such an attempt takes its restart back
(DESIGN.md §9.2), but some errors still aren't recognised as an outage (other brokers' clients, Google Cloud
Storage and Azure Blob Storage), and an item failed before the fix stays failed.

**Recognising it:** the item is `FAILED` with `RESUME_NO_PROGRESS` (`sdh-admin status <item>` on the split stage,
or `item.failed` with that code), and around the attempts before it the split stage logged an outage: `retry`
lines, the key service or storage unreachable, a sealed Vault. An item that stops the splitter itself, the case
the guard is for, shows the pod restarting instead (an out-of-memory exit, `OOMKilled`), with no outage around it.
`item.restart_not_counted` means an outage was recognised and didn't count.

**What to do:** nothing has been lost. The item is still in landing, and the records split before the failure are
stored. But a replay from landing doesn't split it again, since the split stage finishes a `FAILED` item by
announcing its failure again (§13.2). Instead, land it again as a new item that replaces it, from what landing
kept (built 2026-11-07, revision (cb)):

```sh
sdh-admin resubmit --item-id <id> --operator alice     # on the landing node of the item's intake
```

The command leaves a request in the lease database, and the node holding the intake's lease lands the item's bytes
again within about 10 seconds, under a new item ID, with `replaces_item_id` naming the failed one, and announces
it; the command waits for it and prints the new ID (`item.resubmitted`). The split stage splits the new item as any
other. Both items are kept: the failed one stays `FAILED`, so the records split from it before it failed reach a
consumer under both items, the new one naming the old. The producer gets a receipt and a result for the new item,
naming the one it replaces, as for its own resubmission; tell it beforehand (§13.3). Run again for the same item,
the command is refused unless `--again`, so a second copy is never landed by mistake.

**Why a new item, not the failed one split again** (decided 2026-11-07): the split stage would resume the failed
item at its checkpoint and repeat nothing, but the convert stage has already completed it as `FAILED`: it keeps the
first total it gets for an item and completes each once, and the item's `item.split` always has the same message ID.
Splitting it again in place would mean undoing those rules; landing it again as a new item changes nothing
downstream, and nothing in place.

## 14. A version at every stage (proposed 2026-11-08, for review)

What a stage's change does to the data passing through it, and how it is kept from mixing versions unnoticed.
Prompted by §13.1: a record delivered again under another version is processed again to compare (§6), which can
stop the worker without end, and redeliveries are no longer rare (the reconciler rebuilds lost records, and an
operator's replay, §13.2, sends a whole item again, at any time after a change). Decisions made with the user are
marked **(decided)**; the rest is proposed.

### 14.1 Decided 2026-11-08

1. **Every stage after landing states a version (decided):** split, convert and transform. Each is expected to
   behave the same for the same input under the same version; a change of build, implementation or settings that
   can change output is a new version. Landing has none: it stores the bytes it receives.
2. **Stored data names the version that made it (decided):** each stage's rows in storage carry its version, so
   output from the wrong implementation (the wrong splitter or converter applied to a pipeline, say) can be found,
   by stage, version and item, after the fact.
3. **Stored data is superseded only by an operator's conscious choice (decided),** never automatically, for
   example because a later version would have made it differently.
4. **A stage checks it is drained before it changes (decided),** and the check is mandatory, not a checklist step.
   Concern (the user): whether it can be effective when something goes wrong and a change is forced. §14.4.

### 14.2 What each stage's version is

The build (version and build time, `StageBuild`, as now), the implementation that made the output, and a digest of
the settings that change output:
- **split:** the splitter (its format and a version the splitter states, new on `RecordSplitter`) and the split
  settings (`SplitConfig`, the limits): `0.1.0@<build time>/ccda/1/<digest>`;
- **convert:** as built (§6): the converter's mapping and the format options and `max-bytes`;
- **transform:** the WSTL set's name and digest, and its settings (defined here, built in
  `healthcare-data-transformer`).

A rebuild stays a new version (the build is what an operator rolls back to), but it no longer costs anything on its
own, since nothing is processed again for it (§14.3).

### 14.3 Stored with the data, and what a redelivery does

- **In storage:** a `stage_version` column in `split_records` (the split stage's version) and `records` (the
  convert stage's), and in the transform stage's tables. The `records` row also keeps the split version of the
  record it was made from, so a record's whole line can be read from one row. Nothing has been released, so the
  columns are added under contract 3.0; rows written before have none (read as `unknown`).
- **In messages:** `record.split` and `record.stored` carry the version that made the record; `item.split`,
  `item.converted` and `item.transformed` the versions an item's records were made under, in the order first used,
  so a consumer sees an item made under two.
- **A record delivered again is a duplicate whatever its version (proposed, in place of §6's comparison):** if a
  row is stored, it stays, the delivery is dropped, and when the versions differ it is audited
  `record.version_differs` (stage, item, sequence number, both versions) and counted (`sdh.stage.version_differs`,
  as now). Nothing is processed again, so a later version can't stop the worker on an old record, and the steps of
  §13.1 for that loop, and its "mark a delivery a duplicate" command, aren't needed. Processing again under the new
  version is the operator's choice (§14.5). `record.conflict` would then come only from two workers making
  different output under the same version, which means the stage isn't deterministic: a bug, to be dead-lettered
  and alerted on.
- **The split stage resumes an item only under the version it began with:** the item's checkpoint keeps its
  version. A pod of another version that finds an item part way (after a restart, or from the reconciler) leaves
  it unfinished and audits `split.version_held` (the item, both versions); it neither fails it nor mixes cuts within
  it, since a changed splitter can cut differently and records are named by their content, so overlaps and gaps
  wouldn't be seen as duplicates. The other items aren't held. Released by §14.5.

**Built 2026-11-08 (§15.7, step 1; DESIGN.md revision (cd)):** the split stage's version
(`<build>/<format>/<splitter version>/<settings digest>`, `RecordSplitter.version`, `SplitConfig.digest`);
`stage_version` and `input_version` on every row of `split_records` and `records`; `version` on `record.split`,
`record.stored` and `record.failed`, and `versions` on `item.split` and `item.converted` (from the registry, in the
order first used); a redelivery a duplicate whatever its version, audited `record.version_differs`; the split
checkpoint's version, and an item begun under another held (`HELD`, `split.version_held`,
`sdh.split.items_held`), neither resumed nor reconciled. Releasing a held item is the operator commands' (step 3).
Tested by `ContractRoundTripTest`, `CheckpointCodecTest`, `IngestPipelineTest.holdsAnItemBegunUnderAnotherVersion`
and `namesItsVersionOnEveryRecord`, `StageWorkerTest.takesARecordUnderAnotherVersionAsADuplicateUnprocessed`,
`StageRegistryTest.aDueItemNamesTheVersionsItWasMadeUnder` and `ConvertStageTest`.

### 14.4 The drain check, and a change that is forced

**Replaced by §14.4a (decided 2026-11-10):** a new version waits only while an item is part way under another,
not for everything to be finished; work waiting on an operator never holds it.

**What "drained" is, for a stage:** nothing of its input that it hasn't finished, judged from storage and the
registries rather than the broker, since seven brokers don't share a way of telling how much is queued:
- **split:** every item landed for its intake has a split row that is `COMPLETED` or `FAILED` (the reconciler's
  own test, §8), and no item lease is held;
- **convert:** every record of `split_records` has a convert row that is `SENT`, no claim is live, and every item
  split is complete (§9's reconciler test);
- **transform:** the same, against `records`.

Upstream stops first (its intake paused, or the stage before scaled to nothing with KEDA's `paused-replicas`), or a
stage is never drained.

**How it is made mandatory:** the registry keeps the stage's current version (a `stage_version` table: the version,
since when, and the operator or `drained` that let it in). A pod starting under another version checks for itself:
- drained: it records its version as current, audits `stage.version_changed`, and starts;
- not drained: it takes no work (not ready), audits `stage.version_blocked` with what is in flight, and checks
  again every minute. The operator rolls back, which lets the old version finish the backlog, then deploys again
  once drained; or forces it (below).
- **A version that is already in flight is always let in,** so rolling back is never blocked: the versions named in
  the live claims, unsent rows and part-way items, and the version current before.

`sdh-admin drain-check` (read only) shows the same test before a deploy: what is in flight and under which version.

**When a change is forced** (the old build is broken, withdrawn, or must not run another record),
`sdh-admin version --accept <version> --reason <text>` on that stage lets the new version in while not drained,
audited with the operator and reason (`stage.version_forced`). The drain check can't prevent this, nor a pod
restarted with changed settings by someone bypassing the procedure; it only makes it deliberate and recorded. What
keeps a forced change from corrupting data is the rest of this section, which holds however the change came:
- records already stored stay as they are, and a redelivery is a duplicate (§14.3): nothing old is processed again;
- an item part way through the split stage is held, not resumed under the new splitter (§14.3);
- every row and message from then on names the version that made it, so what the forced version made, and every
  item made under both, can be listed afterwards (`sdh-admin versions --since`, §14.5) and dealt with by choice.

What it can't recover is the old version's output, if that was the wrong output: it is already stored and sent,
and only §14.5 replaces it.

### 14.4a The drain check, as decided (2026-11-10, with the user)

**Why §14.4 changed.** Since it was written, failures wait for people (§15.3): an open failure, a held split item,
a pending correction or a queued reprocess may wait for days, so "nothing unfinished" could hold a change without
end. What the check protects is that an item is made under one version, and that holds without an empty queue.
Also found: the convert stage stamps a version per format (its build, the format's mapping and the settings
digest, §14.2), and the stages' images have no shell, so a readiness probe can't be a command.

**The rules:**
1. **A pod's release** is the set of versions it stamps: at the split stage its one version, at the convert stage
   one per format it converts, and one for formats with no converter (mapping `none`). It is named by a digest of
   the versions, sorted. The stage keeps each release let in (`stage_release`: the versions, the digest, when, and
   how it was let in), the newest being the stage's current one.
2. **Part way** is an item with output under a version that the stage would still add to without an operator:
   - **split:** an item whose checkpoint is `SPLITTING`, `ARCHIVING` or `FAILING`, under the checkpoint's version;
   - **convert:** an item not complete whose records aren't all handled yet (its total not known, or fewer done
     and failed than expected), under each version its current records were stored with.
   Not part way, so never holding a change (decided): an item that has waited only on landing or the stage before
   (no output yet: it is made wholly under the new version), an open failure, a held split item, a pending
   correction or retry, and a queued reprocess. Each of those is made, when it runs, under the version running
   then, and stamped so (§14.3).
3. **At start, before it takes any work** (its input, duties and sweeps), a pod checks its release:
   - the current release, or none yet (the first start with this check): let in;
   - the release current before it (a rollback, or an old pod restarted during a rollout): let in, and current
     again (decided);
   - each of its versions that the current release hasn't is a version some item is part way under (a version in
     flight): let in (decided);
   - otherwise, if no item is part way under a version it wouldn't stamp: let in, `drained` (decided: a change of
     one format's mapping waits only on that format's items);
   - otherwise, if an operator accepted its release (rule 5): let in, `forced`;
   - otherwise it is **blocked**: it takes no work, audits `stage.version_blocked` (its release, how many items
     are part way and under how many versions, no item IDs), and checks again every
     `sdh.stage.version-check-interval` (a minute), auditing again only when that changes.
   A release let in is recorded, current, and audited `stage.version_changed` (how). Only a starting pod checks:
   one already running goes on, so for the overlap of a rollout both releases may take new items, each item's
   records then naming both (§14.3). The deploy procedure (§14.6 item 4) pauses upstream, which leaves nothing
   for the overlap; a pod can't tell a rollback from a restart, so stopping running pods would only make
   restarts during a rollout flap.
4. **Ready only once let in** (decided): a pod opens a plain TCP port (`sdh.stage.ready-port`, 8081) once let in,
   and its `readinessProbe` is a `tcpSocket` check on it, as landing's push port is. A blocked pod is not ready,
   so a rolling update holds, and the old pods go on finishing the items part way.
5. **Forced** (decided): `sdh-admin version --accept`, run in a blocked pod, accepts that pod's release, with the
   operator and a reason on standard input, kept encrypted (as a failure's, under the stage's KEK, bound to the
   acceptance), audited `stage.version_forced` without the reason. The pod is let in at its next check. What then
   holds the data together is §14.3: the split stage holds an item begun under another version, and every row
   names its version.
6. **`sdh-admin drain-check`** (read only, on any pod) prints the pod's release and whether it would be let in,
   the current and previous releases, the items part way by version (counts, and the first item IDs), and the
   work waiting on an operator by version: open failures, held items, pending corrections, queued reprocess
   objects. `sdh-admin version` lists the releases let in.

**Built in this order, with tests, then tried on the sandbox:** the release store and the gate in the stage
library; the split and convert stages' part-way queries and releases; the ready port and the probes (kits and
sandbox); the commands; the docs (READMEs, DEPLOYMENT.md, DESIGN.md).

**As built (2026-11-10):**
- `DrainGate` (connector-stage) decides in the order of rule 3, waits in `awaitLetIn()` and opens the ready port;
  `StageReleases` keeps `stage_release` and `stage_release_accept` in the stage's registry schema; `PartWay` is
  what each stage finds part way (`StageRegistry.partWay`, `ItemCheckpoints.partWay`). Each runtime opens its gate
  after its tables and calls it first in `start()`; closing the runtime ends the wait, and nothing starts.
- The convert release is `ConvertTransform.versions` (`ConvertProcessor.versions` until §17 step 3): each mapping `ConverterRegistry.mappings()` loads, and
  `none`, with the build and the settings digest, as `version(message)` stamps them.
- **The Deployments' strategy is now a rolling update that adds a pod first** (`maxSurge: 1`,
  `maxUnavailable: 0`), not `Recreate`. Under `Recreate` every old pod stops before a new one starts, so a new pod
  held back would wait with nothing left to finish the items. `Recreate` was kept so that no two builds ran at
  once, because a record received again under another build was checked again; since §14.6 item 2 (2026-11-08) a
  redelivery is a duplicate whatever its version, so that reason is gone.
- Settings: `sdh.stage.ready-port` (8081; below 0 for none, 0 for any free port, as the tests use) and
  `sdh.stage.version-check-interval` (1 minute).
- Commands, at both stages: `drain-check` and `version [--accept]` (`DrainCheckCommand`, `VersionCommand`, over
  `DrainAdmin`). `drain-check` lists open failures by state and version, the split stage's held items, and the
  reprocess objects waiting; pending corrections show as failures in `REVISION_PENDING`.
- Tested by `DrainGateTest`, `DrainCommandsTest`, `StageRegistryTest.anUnfinishedItemIsPartWayUnderItsOutputsVersions`,
  `ItemCheckpointsTest.countsTheItemsPartWayAndHeldByVersion` and
  `ConvertProcessorTest.theReleaseHoldsEveryVersionStamped`.

### 14.5 Processing again under another version: the operator's choice

Until the operator tooling for edits is designed (§12, "Editing bad data", set aside 2026-11-08: who may read and
change what, and how an edit links to the original), the conscious choice is per item, through what exists:
- **land the item again** (`sdh-admin resubmit`, §13.5): a new item linked to the original by `replaces_item_id`,
  made from start to end under the current versions; the original and its outputs are kept;
- **release a held split item** (`sdh-admin release --item-id <id> --resume|--close`, built 2026-11-09, §15.6e):
  resume it under the new version, which then makes it under two (recorded, §14.3); or close it and land it again;
- **list what a version made:** `sdh-admin versions` per stage: items and record counts by version, items made
  under more than one, and the redeliveries audited `record.version_differs`.

### 14.6 For review

1. **What "superseded" means:** answered 2026-11-08 by the user. An operator edits a record a stage failed, and
   submits it to that stage again; the edited record that passes is kept, linked to the failed one, which also
   stays. Proposed in §15.
2. **No comparison on redelivery (§14.3), decided 2026-11-08:** a redelivery is a duplicate whatever its version;
   processing again is only by an operator's forced reprocess or retry (§15.4). This reverses part of §6 (decided
   2026-11-06).
3. **A held split item waits for the operator (§14.3),** rather than failing with a code of its own. Waiting keeps
   it resumable under the old version after a rollback; failing would need a resubmission every time.
   **Held and failed, told apart:** an item is *failed* for something about the item: a piece the splitter can't
   parse, a limit passed, or a document that stops the splitter (`RESUME_NO_PROGRESS`). It is corrected by an
   amendment (§15.1). An item is *held* only for something about the pipeline: the split stage's version changed
   while the item was part way, and nothing is wrong with the item. It needs no correction, only a decision: roll
   back and let the old version finish it, resume it under the new one, or land it again. With the drain check
   (§14.4), no item is part way at a deploy, so an item is held only when a change is forced, or when a pod stops
   part way through an item (a crash, a lost node) just as a forced change lands. Failing it instead would record
   a fault in the item that isn't there, and the item's line would show a failure and a correction that never
   were. A held item is never left unseen: `split.version_held` is audited, the drain check lists it, and
   `sdh.split.items_held` counts the items held.
4. **Deploys follow: pause upstream, wait for `drain-check`, deploy, resume (decided 2026-11-08).**

## 15. Failures, corrections and reprocessing (decided 2026-11-08)

One way of handling what a stage fails, the same at every stage after landing (split, convert, transform), worked
out with the user on 2026-11-08. It replaces the earlier proposals of this section. Every object a stage fails is
kept, every change to it is an operator's, recorded with who and when, and every object that passes can be traced
back to the submission it came from.

### 15.1 Rules (decided)

**Rules 4 and 7 changed by §15.8 (decided 2026-11-09):** a failed correction is a new failure of its own
revision, and a reprocess is always stored and sent.

1. **Landing recognises exact copies,** by the item's keyed digest (`item_digest`), as now (`item.duplicate_content`).
2. **What a stage fails goes to its fail store,** noted as a failed submission. A dead-letter queue and a fail queue
   are the same thing here: storage of the failed objects, per stage. A failure is **not** sent downstream: the
   operator finds a stage's failures from its fail store. The producer's result still reports failures.
3. **An operator may correct a failed submission** with an edit, submitted to the stage that failed it as a
   correction. The failed one is flagged corrected, and the corrected one traced to it.
4. **A correction that fails** goes to the fail store too, marked a failed correction. When a correction passes,
   the original's link is to the object that passed: the line needed is the original and what finally passed. Each
   failed correction names the original it was for, so none is left without an owner.
5. **Only what a stage failed is corrected.** An object that passed a stage can't be corrected at it; one that has
   passed to the end is corrected in the FHIR repository.
6. **An exact copy is noted and discarded, not stored again** (for items, replaced by §15.5a: a copy is processed in
   full, and only recognised): an item or document the same as one stored, as
   proven by its keyed digest, is recorded as having arrived (when, from whom, under what name, the object it is the
   same as) and its bytes aren't stored a second time. Documents are whole: a document (a record: one
   `ClinicalDocument`, one transaction set) is compared whole, never its parts.
7. **A reprocess or retry is forced on purpose,** by an operator, audited: it processes a stored object again even
   though it is an exact copy. If the outcome is the same as the one stored, that is reported and nothing new is
   stored. If it differs, the new outcome is stored as a new object, linked to the one it supersedes, and sent on
   downstream.
8. **An operator may decline to correct a failure,** with a reason. Nothing is removed. A declined failure can be
   **reopened** when new information allows a correction.
9. **An item is complete at a stage** when each of its records has passed or been declined. Until then it is
   incomplete, and the stage sends progress messages instead.
10. **Deduplication of meaning** (the same clinical facts sent twice in different bytes) is done after the last
    stage, not by the pipeline.

### 15.2 What fails at each stage

| Stage | The failed submission | Codes |
|---|---|---|
| split | an item the splitter couldn't cut at all, or one piece of an item it could (the rest passing) | the data: `PARSE_*`, `LIMIT_*`, `RESUME_NO_PROGRESS`; the configuration: the wrong splitter for the pipeline shows as `PARSE_*` on every item |
| convert | a record that couldn't be converted | `CONVERT_*` |
| transform | a record the mapping rejected | the transformer's |

Failures that are the pipeline's, not the data's (`SOURCE_READ_FAILED`, `STORAGE_FAILED`, `PUBLISH_FAILED`, and an
outage), are retried by the stage, and reach the fail store only once its retries are spent. A message a stage
can't open (a bad signature, an unknown contract version) is not a failed submission: it is dead-lettered as now,
and recovered by fixing the configuration and replaying (§13.2).

### 15.3 The fail store

**States changed by §15.8.5 (decided 2026-11-09):** `FAILED`, `REVISION_PENDING`, `DECLINED`, `SUPERSEDED`;
`FAILED_CORRECTION`, `CORRECTED` and `RETRIED` go. The table below is as built.

Per stage: the failed objects stay where the stage stores everything it makes, in its output table (`split_records`,
`records`), encrypted, with their error codes, in the same commit as what passed, and are never announced; and a
state per failure is kept in the stage's registry schema in Postgres (`stage_failure`), since it changes. Decided
with the user on 2026-11-08, in place of an Iceberg table of failures per stage: two tables would need two commits
per batch, and a crash between them a repair at resume. A reprocess from stored data (§15.4) skips the failed rows.

| State | Meaning | Set by |
|---|---|---|
| `FAILED` | failed, not acted on | the stage |
| `CORRECTION_PENDING` | a correction or retry is in flight | `correct`, `retry` |
| `FAILED_CORRECTION` | a correction that failed; names the original | the stage |
| `CORRECTED` | a correction passed; names the object that passed | the stage |
| `RETRIED` | the same input passed on a retry; names the object that passed | the stage |
| `DECLINED` | the operator chose not to correct it, with a reason | `decline` |
| `SUPERSEDED` | resolved by reprocessing an earlier stage (the cause was upstream) | the stage, on a reprocess |

Every change of state is audited (`failure.<state>`, with the stage, item, sequence number, operator, time and,
for `DECLINED`, the reason, encrypted, since it may describe the data). `DECLINED` returns to `FAILED` by `reopen`,
with its own reason. Only one correction or retry is in flight per failure. The fail store is kept by retention
until each failure is `CORRECTED`, `SUPERSEDED` or `DECLINED`, so the line never loses its start.

### 15.4 Corrections, retries and reprocessing

**Replaced by §15.8 (decided 2026-11-09):** a reprocess is always stored and sent, never `outcome=same`; a
retry is a reprocess; a correction is a revision marked `corrected`.

- **`correct`:** the operator exports the failed input (`export`, encrypted to the operator's own key, never
  printed in the clear), edits it, and submits it (`correct`, the edit on standard input). The edited object is
  stored in the fail store's corrections, with who and when, and processed by the stage's own workers as its input,
  its output linked to the original failure. (Decided with the user 2026-11-08, in place of a correction message
  signed with an operator key: see §15.6c.) For a piece the
  splitter couldn't cut, the correction is an item holding that piece alone (one document; one transaction set in
  its envelope; a header and one row), split as any item and linked to the failed piece, so the rest of the item
  isn't sent again.
- **`retry`:** the same input sent again unchanged, for a failure that wasn't the data's (a limit raised, a stage
  fixed). It is a correction without an edit.
- **`reprocess`:** for objects that passed, when a stage has changed (a new mapping): processes stored objects
  again from the earliest stage changed (from `records` for a new transform, `split_records` for a new converter,
  `landed_items` for a new splitter), for an item, an intake, a time range, or what a version made. It isn't a
  correction: nothing failed, and the earlier output stays.
- **Each is forced:** it bypasses the duplicate rule (§14.3), and is audited `stage.reprocessed` (or
  `failure.retried`, `failure.corrected`) with its outcome:
  - **the same outcome** (the output's digest equals the stored one, which the registry keeps): reported
    `outcome=same`, nothing stored, nothing sent;
  - **a different outcome:** stored as a new object with the new version, linked to the one it supersedes
    (`supersedes`), and sent downstream, where it is processed as any new object;
  - **a failure:** to the fail store, as any failure, naming what it was reprocessing.
- **Telemetry:** `sdh.stage.reprocessed` by stage and outcome, so a remap's effect can be counted.
- **Downstream identity:** for a remap to update resources rather than add them, the transformer gives each FHIR
  resource an ID derived from its source record, stable across versions (a requirement on the transformer).

### 15.4a When a later run passes what failed (raised by the user 2026-11-08)

**Replaced by §15.8.3 and §15.8.5 (decided 2026-11-09):** no matching by place across a new cut; a newer
generation or revision supersedes every earlier object, failed or passed, and becomes current even if it fails.

A failure can pass later without anyone correcting it: a reprocess from an earlier stage (a fixed splitter cuts the
piece cleanly), a retry after a limit is raised, or a new converter or mapping. The rule: **the failure is never
rewritten into a success, and the success is never stored over it.** Both stay, and are joined:
- **The failed object is kept as it was,** in the fail store, with its error, version and time. Only its state
  moves on, from `FAILED` (or `DECLINED`, `FAILED_CORRECTION`) to a resolved state, with how it was resolved:
  `CORRECTED` (an operator's edit passed), `RETRIED` (the same input passed on a retry), or `SUPERSEDED` (an
  earlier stage's reprocess made input that passed). Each records `resolved_by` (the object that passed),
  `resolved_at`, `resolved_version` (the version it passed under, beside the version that failed it), and who
  started the run (the operator of the retry, correction or reprocess; `SUPERSEDED` names the reprocess run).
- **The object that passed is a new object,** stored as any output, with `resolves` naming the failure, so it can
  be followed back. It is sent downstream as any new object.
- **Matching a reprocess to a failure** is by the item and the record's place in it (its sequence number and source
  offset and length), never by a guess. When a reprocess cuts differently and no record passing matches an open
  failure exactly, the failure stays open, and the reprocess's report lists it as `unmatched`, for the operator to
  resolve by hand (decline it, naming the reprocess) or correct.
- **Telemetry:** `failure.resolved` (stage, item, sequence number, how, the failed and passing versions, the run,
  the operator; no content), `sdh.stage.failures_resolved` by stage and how, and `sdh.stage.failures_open` falls.
  The run's own events (`stage.reprocessed`, `failure.retried`) carry the same run ID, so the run and every failure
  it resolved are found together.
- **The item:** `item.progress` is sent with the new counts, and the item-complete message once nothing is open
  (§15.6). The producer's result is sent again, naming the failure as resolved.
- **The other way round,** a reprocess that fails what passed before (a new mapping rejects a record): the earlier
  output stays and stays current; the new failure is stored in the fail store as a failure of that reprocess
  (naming the output it was reprocessing), so it is reported and can be acted on, but it doesn't reopen the item.

### 15.5 Exact copies

**Replaced by §15.5a (decided 2026-11-10):** an exact copy is processed in full at every stage; landing only
recognises it and says so. What follows is kept as first decided.

- **At landing:** an item with the same `item_digest` as one stored is recorded as an arrival: a row of its own
  (a new ID for the arrival, `same_as_item_id`, when, the source reference, the producer's reference), audited
  `item.duplicate_content`, with no bytes stored and nothing announced. The producer's receipt and result name the
  item it is the same as. If that item failed and isn't resolved, the failure is flagged `resent` (audited), so an
  operator sees that the producer tried again.
- **At a stage:** a record (a whole document) with the same record ID as one already stored, in any item, is
  recorded as the same as that record (`same_as`, in the item's registry rows), counted as passed for its item, and
  neither stored nor sent again. This replaces "copy per item" (§11, step 4b). The record ID is a keyed
  HMAC-SHA-256 of the record's bytes (§7.3 of DESIGN.md), so equal IDs prove equal content; a document differing in
  a byte (a header timestamp) is a different document.
- **Not discarded:** a forced retry, correction or reprocess (§15.4).

### 15.5a Exact copies, as decided (2026-11-10, with the user)

**Why §15.5 changed.** An output depends on the input's bytes and on the version of the stage that made it (§14.2):
the same bytes under a newer splitter, converter or mapping may come out differently, and an item that failed
under the old version may pass under the new. So a copy can be "the same" as the earlier item only at a stage whose
version hasn't changed since, and that holds stage by stage: landing can't judge it, as it doesn't know the stages'
versions, and a stage that ended a copy unprocessed would leave a changed stage after it nothing to process. Also
found: landing knows an item's digest only once its bytes are stored, so "no bytes stored" means deleting committed
rows; an item not announced is split anyway by the split stage's reconciler, and shows as missing in the audit
check that every item ended at every stage (§13.2); and landing can't reach the stages' fail stores.

**The rules** (replacing §15.1 rule 6 for items, and §15.5):
1. **Every copy is processed in full,** at every stage, under the version running then, as an item of its own: its
   bytes landed and kept, split, converted and transformed like any other. Nothing is skipped because it is a copy,
   as nothing is for a reprocess (§15.8.1 rule 1). Two items with the same content may then have different outputs,
   each made by the version that took it.
2. **Landing recognises a copy and says so.** An item landed with the same `item_digest` as an earlier item of the
   intake is given `sameAs`, the earliest such item still in the ledger (item IDs are UUIDv7, so the lowest), not
   counting the item it `replaces`: a resubmit of an item isn't a copy of it. `item.duplicate_content` is audited
   as now, naming it. `sameAs` goes on the item's `item.landed`, its receipt (`item.received`, and a push's HTTP
   receipt) and, through the stages, its item messages and its producer result.
3. **Records are not matched across items** ("copy per item" stays, §11 step 4b). A record's ID comes from its content
   alone (DESIGN.md §7.3), so a system after the pipeline spots repeated documents by `recordId`, as it handles
   meaning (§15.1 rule 10).
4. **The earlier item's failures are flagged `resent`.** Each stage, when it learns an item is a copy (the split
   stage from `item.landed`, a later stage from `item.split` or its counterpart), flags each open failure
   (`FAILED`, `REVISION_PENDING`) it holds for the item named by `sameAs`: a count and the last time, an event naming
   the copy, audited `failure.resent`. This tells an operator the producer sent it again; the copy's own
   processing is unaffected, and its own failures are its own. Nothing else changes state.
5. **Unchanged:** a forced retry, correction or reprocess (§15.8); landing's `resubmit` (§13.5); the producer's
   result, one per item at its first report (§15.8.7), now naming `sameAs` for a copy.
6. **Not compared:** a stage doesn't say whether a copy's output came out the same as the earlier item's (no
   `same_output` for copies, decided): that would mean reading the earlier item's rows at every stage, and the
   record IDs let anyone compare later.

**The contract** (3.0, unreleased, so added under it; DESIGN.md §10.1, brought up to date with every field 3.0
has gained since the stages were built, decided): `sameAs` (an item ID, left out when unset)
on `item.landed`, `item.received`, `item.split`, `item.progress`, `item.converted` and `item.result`, and on a
push's receipt. The split stage carries it from `item.landed` to its item messages, as it carries `replaces`; the
convert stage from those to its own and the producer's result.

**Built in this order, with tests, then tried on the sandbox:** the contract and its round trips; landing's
`sameAs` (files, pushes and message batches, chosen when the message is built, so a message sent again after a
crash names the same item); the stages carrying it; the fail store's `resent` (`stage_failure` gains `resent_count`
and `resent_at`, shown by `failures`); the docs: DESIGN.md §10.1, §5.3.3 and the READMEs.

**As built (2026-11-10, DESIGN.md revision (cp); tried on the sandbox the same day, SANDBOX.md "Exact copies"):**
- **Landing.** `LandingLedger.earliestCopy` gives the item, from the ledger's items with the same digest: only those
  with a lower ID (compared unsigned, as UUIDv7 time order needs), leaving out the replaced item, so the answer
  doesn't change when later copies land. A newly landed item gets it from the duplicate check, which reads the
  ledger only when the digest index says there may be a match; an item announced again at startup, or a resubmit
  recorded before, is looked up once. The announcer puts it on `item.landed` and `item.received`; a push's receipt
  has it, the receipt replayed for a repeated `Idempotency-Key` too (looked up, with the replaced item from the
  summary row, `ItemLander.replacesOf`). A receipt given while the ledger entry was deferred by a storage error
  leaves it out; the item's messages, sent once the entry is written, still name it.
- **Split stage.** The `sameAs` of an `item.landed` is kept in a table of its own beside the checkpoints,
  `split_copy` (item, earlier item, when noted; the first noted stays), before the item is split; noting it flags the
  earlier item's open failures. Every end sent for the copy reads it from there, so ends sent again after a decline,
  a correction or a reprocess name it too. An `item.reprocess` holds the item as landing kept it, with no `sameAs`,
  and flags nothing. An item the reconciler splits before its `item.landed` arrives names it from when that message
  is taken.
- **Convert stage.** The worker flags the earlier item's open failures when a total of the copy's first generation
  names it (`item.split` or `item.progress`), so a reprocess of the copy flags nothing; a total taken again flags
  nothing more. `item.converted`, `item.progress` and `item.result` carry it from the total.
- **Fail store.** `stage_failure` has `resent_count` and `resent_at`; `stage_failure_event` has `resent_by`, the
  copy, on an event that leaves the state as it was, by `pipeline`. Only `FAILED` and `REVISION_PENDING` failures
  of the earlier item, not superseded, are flagged, each once per copy. Audited `failure.resent`, with
  `sdh.copy_id`. `sdh-admin failures` shows `resent=<n> last=<time>`, and with `--item-id` an event line
  `resent as <copy>`.
- **Tests:** the contract round trips; the ledger's choice (earliest, earlier only, unsigned, not the replaced
  item); pushes (receipt, messages, a resubmit, a repeated key, a restart); the split stage (flagged once, a
  declined failure left alone, the copy's end naming it); the convert stage (flagged once from a copy's total, its
  `item.progress` naming it) and the result.

### 15.6 Items: progress and completion

- **Progress:** each stage sends `item.progress` (the item, the stage, records passed, failed open, declined, the
  total) when it has handled every record once, and again whenever a failure changes state. The producer's result
  is sent at the same time, reporting the failures.
- **Complete:** when no failure is open (each record passed or declined), the stage sends its item-complete message
  (`item.split`, `item.converted`, `item.transformed`), naming the declined records. Downstream, an item's
  completion means "everything that will ever come has come", as now.
- **Reopened after completion:** the item becomes incomplete again (`item.progress`, `reopened`); when it completes
  again, the item-complete message is sent again with a completion number (its message ID carries it), so "complete
  once" (§13.5) becomes "complete once per completion".
- **Never acted on:** an item with an open failure stays incomplete, by design. It is not left unseen: the stages
  count open failures by age (`sdh.stage.failures_open`, with the oldest's age), for alerting, and `sdh-admin
  failures` lists them.

### 15.6a Step 2 as built (2026-11-08)

- **The fail store:** `FailureStore` (connector-core), `<schema>.stage_failure`, keyed by stage, item and sequence
  number (`-1` for the item as a whole), with the error code, the source offset, the version that failed it, the
  state and the columns a later step fills (who changed it, the reason, encrypted; how it was resolved, §15.4a). A
  failure is recorded once; recording it again does nothing, so `failure.failed` is audited once. Counted in
  `sdh.stage.failures`; `sdh.stage.failures_open` and `sdh.stage.failures_oldest_age` are read from it each minute.
- **The split stage** keeps a piece it couldn't parse in `split_records` and doesn't send its `record.split` (nor on
  re-announcing after a restart, nor on `replay`). At the item's end it records the item's failures, then sends
  `item.split` if none is open (its `recordCount` the records sent on), else `item.progress`. An item that couldn't
  be split to the end has its failure as seq `-1`, and ends in `item.progress` with the failure.
- **The convert stage** (the stage library, so the transformer too) records a record that failed in the fail store
  in the same transaction as it marks it stored (`FAILED`, no message), and sends nothing for it. An item whose
  records are all handled completes (`item.converted`) only if none of its failures is open and its total came from
  `item.split`; otherwise it sends `item.progress`, once per change of counts (`progress_key` on `stage_item`). A
  total from the split stage's `item.progress` may grow (`upstream_open`), and is replaced by the final one.
- **The producer's result** is still sent by the convert stage, the last stage that sends results: `incomplete`
  with each `item.progress`, `completed` when the item completes, `failed` for an item that couldn't be split. Since
  failures aren't sent between the stages, it reads the split stage's failed pieces from `split_records` and its
  own from `records`, merged by record number. An `incomplete` result's message ID carries its counts, so a later
  one isn't dropped as a repeat.
- **Recovery:** the convert reconciler doesn't rebuild a failed piece from `split_records` (it is in the split
  stage's fail store, not lost); the convert `replay` doesn't send failed records.
- **Gone:** reprocessing to compare and the conflict report to the dead-letter queue (`record.conflict`), unreachable
  since step 1; the convert stage no longer opens a dead-letter publisher.
- **Not yet:** the states after `FAILED` (step 3's commands), so in step 2 an item with a failure stays incomplete;
  and items from before, whose failures were sent on and not recorded, complete as they did.

### 15.6b Step 3a as built (2026-11-08)

- **Commands**, at every stage that has a fail store (the split stage and the stages of the stage library):
  `failures [--all] [--item-id <id>]` lists them by item, record number, state, error code, offset and version, and
  one item's with each change of state (who, when, whether a reason was given); `decline --item-id <id> --seq <n>`
  moves a failure from `FAILED` or `FAILED_CORRECTION` to `DECLINED`; `reopen` moves a `DECLINED` one back to
  `FAILED`. None prints content or a reason.
- **The reason** is read from standard input (`kubectl exec -i ... < reason.txt`), never from the command line, so it
  stays out of shell histories and the cluster's audit log. It is at most 4 KiB, encrypted with a data key of its
  own wrapped with the stage's records KEK, and bound to its item, record number and event, so it can't be moved to
  another failure. Each change is a row of its own in `<schema>.stage_failure_event`; the failure's row keeps the
  latest. Audited `failure.declined` / `failure.reopened` with both states and the operator, never the reason.
- **Writes:** the commands' database connection stays read-only; only `decline` and `reopen` open a second one that
  may write, and only to the fail store.
- **After a change:** the split stage sends the item's end again, alone (`item.split` once none of its failures is
  open, else `item.progress` with the new counts), if the item has ended; one still being split counts the change
  at its end. A stage of the stage library leaves it to its workers: each renewal interval they look for items not
  yet complete whose failures changed after their last progress (`progress_at`), and complete them or send
  `item.progress` again.
- **Not yet:** `correct`, `retry`, `export`, `reprocess` and releasing a held split item (§14.3), which need edited
  content, its signing, and reprocessing by force; and reopening an item already complete downstream: `reopen` on
  one changes the fail store only.

### 15.6c Corrections as built (2026-11-08, step 3b)

Decided with the user on 2026-11-08: **the stage's workers apply a correction**, not the admin command, so no
output is ever made outside a worker; the corrected content is kept **in Postgres**, encrypted; and **`export`
encrypts to the operator's own public key**, so the failed record is never in the clear outside the pod.
- **`export --item-id <id> --seq <n> < operator-public.pem`** reads an EC P-256 or RSA public key from standard
  input and prints the failed record as a JWE to it (`ECDH-ES+A256KW` or `RSA-OAEP-256`, `A256GCM`) on a line
  starting `sdh-export: `. Audited `admin.export` with the key's SHA-256 (`sdh.recipient_key`) and the size.
- **`correct --item-id <id> --seq <n> < corrected-record`** reads the edited record from standard input (at most the
  stage's largest record), and in one transaction stores it in `<schema>.stage_correction` (numbered per failure,
  with who and when; the content encrypted with a data key of its own wrapped by the records KEK, bound to item,
  record number and correction number) and moves the failure from `FAILED` or `FAILED_CORRECTION` to
  `CORRECTION_PENDING` (audited `failure.correction_submitted`). One correction is in flight per failure. No reason is
  asked: the correction is its own explanation, and who and when are kept.
- **The workers**, each renewal interval, take pending corrections oldest first, each claimed in the registry under
  a name of its own (`correction:<item>:<seq>:<n>`), so one worker processes it and a crash is recovered as for a
  record. The processor processes the corrected content as the record would have been (for the convert stage: the
  failed row of `records` gives its place, origin and message); the output is committed as a new row of the
  stage's table with `correction_of` naming the failed row, which stays as it was. In the same transaction as the
  registry row, the correction is marked `PASSED` or `FAILED` and the failure `CORRECTED` (with `resolved_by`, the
  new record's ID, `resolved_at` and `resolved_version`) or `FAILED_CORRECTION` (audited `failure.corrected` or
  `failure.correction_failed`). A passed one is sent on as any record (`record.stored`, a new record ID); one stored
  but not sent, if a worker stopped between the two, is sent again on a later interval. The item is then looked at
  again: it completes once nothing is open, with the corrected record counted as passed.
- **Counts:** a correction isn't a record of its item (its registry row isn't counted); `failed` still counts what
  failed once, and `resolved` (the fail store's `CORRECTED`, `RETRIED`, `SUPERSEDED`) what passed later, so passed
  is `done - failed + resolved` and the item-complete message's failed count is `failed - resolved`. The producer's
  result no longer lists a failure a correction resolved. A replay sends a passed correction's record too.
- **Readers of the tables** read a record's original rows apart from its corrections (`CommittedRecords.all`,
  `read` and `failed` leave corrections out; `corrections` reads them), so resuming, reconciling and replaying are
  unchanged.
- **Not yet:** corrections at the split stage (§15.4: an item holding the piece alone) and export there (built
  next, §15.6d); `retry`;
  `reprocess`; releasing a held item; a corrected record's message naming what it corrects (`resolves`, for the
  transformer; the link is in the table); and an item already complete when a correction passes (after a
  `reopen`) isn't completed again downstream.

### 15.6d Split corrections as built (2026-11-09, step 3c)

Asked for by the user on 2026-11-08 ("Split corrections"): `export` and `correct` at the split stage, applied by its
pods, as at the convert stage (§15.6c). The split stage has no stage registry, so its pods take the place of the
workers.
- **What is corrected:**
  - **A piece the splitter couldn't parse** (`--seq` from 0): `export` gives the piece as cut from the item (its
    failed row of `split_records`); the correction is that one document. It is split alone and must give exactly
    one record, which passes (else `CORRECTION_RECORD_COUNT`, or the splitter's own code). It is stored under the
    piece's item and number, at the piece's offset and length in the item, with `correction_of` naming the failed
    row.
  - **The item as a whole** (`--seq -1`, an item that couldn't be split to its end): `export` gives the item as
    landed; the correction is the whole item, split from its start. The records already stored must come out of it
    byte for byte (the same record IDs), or it fails (`CORRECTION_CHANGES_PASSED`: only what failed is corrected,
    §15.1); the pieces stored failed may stay as they were. Every record after them must pass. They are stored,
    numbered on from the item's, their places those in the correction, with `correction_of` the item's ID.
  - The largest correction is the stage's largest item.
- **Applying:** every pod looks each renewal interval at the pending corrections, oldest first (at most 10), and
  takes each under its item's lease (another pod's item waits for the next interval), reading again that it is
  still pending. The correction is split and its records committed under the lease, then sent (`record.split`, new
  record IDs), then, in a transaction of its own, the correction is marked `PASSED` or `FAILED` and the failure
  `CORRECTED` or `FAILED_CORRECTION` (audited as at convert). Last the item's end is sent again, as the fail store
  counts it, and the correction noted as sent (`notified_at`). A failed correction stores and sends nothing but the
  item's end.
- **After a stop:** a correction whose records were committed but whose outcome wasn't noted is found by its rows
  (only a pending correction can have any), sent again and noted, not stored twice. An item whose end wasn't sent
  after its correction is sent once a lease's length has passed.
- **Counts:** a corrected piece counts as passed (`item.split`'s failed count leaves it out); the records a
  correction of the whole item added count among the item's records; once that correction passed, the item's end
  names no failure. The item's checkpoint stays as it ended (`FAILED` for an item that couldn't be split): the fail
  store says it was corrected.
- **Downstream:** the convert stage converts a corrected record as any other: it never had the failed one. The
  producer's result no longer lists a piece corrected at the split stage; the convert reconciler rebuilds a
  corrected record whose `record.split` was lost; the split replay sends the corrected records too.
- **Not yet:** as §15.6c, but for the split stage's corrections and export.

### 15.6e Retry, release and completing again (decided 2026-11-09, step 3d)

**Changed by §15.8.5 and §15.8.7 (decided 2026-11-09):** a retry or correction makes a revision (of one record)
or a generation (of an item as a whole), and supersedes its failure whether or not it passes; and no operator
action sends the producer's result again.

Decided with the user on 2026-11-09: a retry that fails leaves its failure `FAILED`; `release` resumes or closes a
held item; `reprocess` is designed first, for review, and built after (§15.8).
- **`retry --item-id <id> --seq <n>`** (every stage with a fail store; no input): a correction without an edit
  (§15.4). The command reads what the failure failed on, as `export` does, and submits that, unchanged, as a
  correction of kind `RETRY` (`stage_correction.kind`; null is an operator's correction), from `FAILED` or
  `FAILED_CORRECTION`, moving the failure to `CORRECTION_PENDING` (audited `failure.retry_submitted`). The stage's
  workers process it exactly as a correction (§15.6c, §15.6d), with the version running now. If it passes, the
  failure is `RETRIED`, naming what passed (`failure.retried`); if it fails, the failure returns to `FAILED`, the
  failed object unchanged, and the retry's error code is kept on its row (`failure.retry_failed`), shown by
  `failures --item-id`. A retry under the version that failed it is allowed (a pipeline failure whose retries were
  spent passes once the service is back); one that passes keeps the version it passed under
  (`resolved_version`), beside the version that failed it. At the split stage, a retry of the item as a whole splits
  the landed item again from its start, under the same rules as a correction of it: the records already stored
  must come out unchanged.
- **`release --item-id <id> --resume|--close`** (the split stage): ends the hold on an item held across versions
  (§14.3). The command records a request (`split_release`: who, when, the action, and for `--close` the reason,
  from standard input, encrypted as a decline's); the split stage's pods apply it within a renewal interval under
  the item's lease, so no output is made outside a pod. `--resume`: the item's checkpoint goes back to
  `SPLITTING` under the version running now, `split.released` audited with both versions, and the pod resumes it
  from its checkpoint; its records keep the version that made each, so the item is made under two, and its
  `item.split` names both. `--close`: the checkpoint goes to `CLOSED` (ended; not resumed, reconciled or counted
  held), `split.item_closed` audited; nothing is sent downstream, where the records already sent stay and the item
  stays incomplete (`stage.item_total_missing`), since it never finished splitting. Closing is for an item the
  operator lands again with landing's `resubmit` (§13.5), whose new item links to it. A request for an item not
  `HELD` is refused by the command, and again when applied if the item moved on in between, saying why in the
  request's row (`status --item`) and audit (`split.release_refused`).
- **Completing again (§15.6, "Reopened after completion"):** an item's completions are numbered, from 1. Each
  stage keeps the number it last sent (`stage_item.completion`; the split stage's `split_item.completion`). When a
  failure of an item complete at a stage is opened again (`reopen`), the stage sends `item.progress` carrying the
  number of completions sent before it (`completion`), which names the completion it withdraws; when nothing is
  open again, it sends the item-complete
  message with the next number. The first completion's message ID is unchanged; a later one's carries `~c<n>`, so
  it isn't dropped as a repeat. A stage after takes an upstream `item.progress` that reopens the completion it
  last took as reopening its own item (its total open again, its completion withdrawn, its own `item.progress`
  sent with its own number); one naming an earlier completion, or not reopening, is stale and changes nothing, so no
  ordering is needed. The producer's result is sent again as `incomplete`, then `completed`.
- **`resolves`:** a record that passed by a correction or retry names, in its `record.split` or `record.stored`,
  the failed record it resolves (`resolves`, the failed row's record ID; for a correction of a whole item, its
  item ID), so the stage after, and later the transformer, can follow it back without reading the table.

### 15.6f Step 3d as built (2026-11-09)

- **Retry:** `retry --item-id <id> --seq <n>` at both stages (`FailureCommands.RetryCommand`, shared by the stage
  library and the split stage's admin). `stage_correction` gains `kind` (`RETRY`; null for a correction), and
  `CorrectionStore.Kind` decides the outcome's state and audit: `RETRIED` or back to `FAILED`
  (`failure.retried`, `failure.retry_failed`), beside `CORRECTED` or `FAILED_CORRECTION`. `FailureStore.State
  .passedLater()` is true for `CORRECTED` and `RETRIED`, wherever a corrected failure counts as passed. The listing
  marks each row `retry` or `correction`.
- **Release:** `release --item-id <id> --resume|--close` in the split stage's admin, `--close` with a reason on
  standard input. Requests are rows of `split_release` (`HoldReleases`: `PENDING`, `DONE` or `REFUSED`, with the
  versions it moved between or the refusal; the reason sealed as a decline's). `SplitCorrections` applies pending
  requests each renewal interval through `ItemSplitter.release`, under the item's lock and lease;
  `IngestPipeline.release` moves a `HELD` checkpoint to `SPLITTING` under the running version
  (`Checkpoint.released`, `split.released`) or to the new state `CLOSED` (`split.item_closed`), and a resumed item
  is split from its checkpoint at once. A pod that finds a `CLOSED` checkpoint skips it (`Result.CLOSED`,
  `split.item_closed_skipped`). `status --item` lists an item's requests, without their reasons.
- **Completing again:** `split_item` and `stage_item` gain `completion` (completions sent) and the split stage
  `completed` (whether the last end sent was complete); `stage_item` gains `upstream_completion`, the number of
  the upstream message its total was taken from. The split stage numbers each end it sends
  (`ItemCheckpoints.announced`, before the send, so the same end sent again keeps its number); the stage library
  counts in `markCompleted`. `item.split`, `item.converted` and a `COMPLETED` `item.result` carry `completion`
  (1 when absent), `item.progress` and an `INCOMPLETE` result the completions before them (0 when absent); the ID
  gains `~c<n>` past the first (`ConnectorMessage.completionSuffix`), and the codec writes the field only past the
  default. `StageRegistry.expect` takes an upstream total only if it is newer by that number (a complete total
  above the one taken; an open one at or above it, unless both are open at the same number, when counts only
  grow); taking one reopens an item completed here (`stage.item_reopened`, reason `upstream`). A failure reopened
  at the stage itself reopens its completed item on the next revisit (`StageRegistry.reopenChanged`,
  `stage.item_reopened`, reason `failure_reopened`). Rows from before this build carried completion 0 (found on
  the sandbox, SANDBOX.md "Checked"), so reopening such an item would have sent messages under IDs already sent,
  and a repeated upstream end of one was taken as newer (a false `stage.item_reopened`). Each start numbers them,
  added 2026-11-09 (revision (cj)): at a stage of the stage library, an item completed with no number gets
  completion 1 and a complete total with no number upstream completion 1 (`stage.completions_numbered`,
  `stage.totals_numbered`); at the split stage, an ended item none of whose failures is open (one from before
  the fail store has none) gets completion 1, marked complete (`ItemCheckpoints.numberEarlierCompletions`,
  `split.completions_numbered`). This build always numbers what it writes, so only earlier rows match, but for a
  split item ended and not yet announced, where the number given is the one announcing it gives. Each changes
  nothing once done.
- **`resolves`:** `record.split` and `record.stored` carry it for a row with `correction_of`; the split stage's
  correction of a whole item names the item ID.
- **Tests:** `StageWorkerTest` (`appliesARetry`, `completesAgainAnItemWhoseFailureIsReopened`),
  `StageRegistryTest.anItemReopenedUpstreamIsReopenedHere`, `FailureCommandsTest.submitsARetry`,
  `ItemProcessorTest` (`leavesAFailedRetryFailed`, `completesAgainAnItemWhoseFailureIsReopened`, `resolves` on
  corrections), `IngestPipelineTest` (release, resume and close), `AdminCommandsTest.releasesOnlyAHeldItem`,
  `ContractRoundTripTest.completionsAndResolvesRoundTrip` and `ConvertStageTest` (`resolves`); the numbering of
  earlier rows by `StageRegistryTest.numbersTheCompletionsOfEarlierRows` and
  `ItemProcessorTest.numbersTheCompletionsOfEarlierItems`. Tried on the sandbox (SANDBOX.md "Checked").

### 15.7 Order of building

Each step builds, passes its tests, and is tried on the sandbox before the next:
1. **Versions with the data** (§14.2, §14.3): the split stage's version; `stage_version` in `split_records` and
   `records` and in the record and item messages; a redelivery a duplicate whatever its version, audited
   `record.version_differs`; a split item held across versions.
2. **The fail store** (§15.3): failures stored apart and no longer sent downstream; states; `item.progress`; item
   completion by §15.6; the producer's result.
3. **Operator commands:** `failures` (list, show, export), `correct`, `retry`, `decline`, `reopen`, and the
   split stage's `release` (built 2026-11-09, §15.6c–§15.6f); then `reprocess`, with generations and revisions,
   decided 2026-11-09 (§15.8, its own order of building in §15.8.10).
4. **Exact copies** (§15.5, replaced by §15.5a) at landing and at the stages.
5. **The drain check** (§14.4, replaced by §14.4a).

### 15.8 Reprocess, generations and revisions (decided 2026-11-09; steps 1 to 5 built)

Worked out with the user on 2026-11-09, in place of the proposal first written here. It replaces `replay`
(§13.2), the outcomes of §15.4 and the matching of §15.4a, and changes how corrections and retries are recorded
(§15.3, §15.6c–§15.6f). The parts it replaces are marked there. Four points first proposed in this write-up were
agreed by the user the same day: a revision per stage (§15.8.2), a failed retry superseding its failure
(§15.8.5), late outputs still sent on, and no producer result for any operator action (§15.8.7).

#### 15.8.1 What was decided

1. **One operation.** An operator may reprocess an object from any stage, for any reason: a stage changed (a new
   splitter, converter or mapping), or a message was lost and an item or record never reached a stage. `replay` is
   the same operation and goes. The object is processed again, under the version running now, and what it makes
   goes on through the rest of the pipeline. Nothing is skipped because it matches what was made before, was made
   under the same version, or never reached the stages after: an object with no earlier output downstream is simply
   processed there as new, flagged as a reprocess.
2. **Only the split stage fans out.** One item becomes many records (documents); convert and transform take one
   record and make one. So a reprocess at the convert or transform stage replaces one output with one output, while
   a reprocess at the split stage makes a new set of records with no assumed relationship to the earlier set, even
   when the counts match. Records of two splits are never lined up.
3. **Generations and revisions.**
   - A reprocess (or a correction) of an **item at the split stage** makes the item's next **generation**. Every
     record it cuts, and everything made from them, belongs to that generation.
   - A reprocess (or a correction, or a retry) of **one record** at any stage makes that record's next
     **revision**. Its siblings, and its item's generation, are unchanged.
   - Both numbers follow the object to every later stage, and logs, telemetry and the fail store name them and the
     stage where the reprocess began, so an operator can see why one record has a higher revision than its
     siblings.
4. **The latest is current; nothing is lost.** The new generation or revision becomes current as soon as it is
   made, even if it later fails downstream: this shows the line of processing, not a judgement of which is better.
   Everything earlier, successes and failures alike, is **superseded**: kept, readable, and comparable with what
   replaced it, but needing no further action. A superseded object can't be reprocessed, corrected, retried or
   declined.
5. **Each stage judges by what has reached it.** An object arriving at a stage where nothing newer has arrived is
   current there and processed as such. One arriving after something newer is processed, stored and marked
   superseded on arrival, never dropped. Queues keep no order, so this marking is a stage's best knowledge; the
   numbers on every output always let a reader work out which is current (§15.8.3).
6. **A retry is a reprocess; a correction is a revision marked `corrected`.** A retry processes the failed object
   again unchanged. A correction processes an operator's edit, links to the failure it was made for, and is
   audited as a change of content.
7. **Interrupted work is recorded.** If a reprocess is asked for while the object is still being processed, the
   new generation or revision takes over, and the interruption is logged and counted, since an interrupted split
   leaves records the output never sees (§15.8.6).
8. **The pipeline doesn't decide the final record.** Outputs of a reprocess are flagged with the run, the cause and
   their generation and revisions. What a system after the transformer does with them (replace, retract, keep
   both, ask an operator) is its own decision. It needn't wait for all of an item's records: it can act on each
   record as it comes, by the rule in §15.8.3.
9. **Producers take no part.** Operator work on the pipeline is independent of the producer, whose only
   responsibility is to deliver data. No operator action (reprocess, retry, correction, decline, reopen) sends
   an `item.result`; other systems direct corrective work to the producer.
10. **No pacing.** A reprocess joins the stage's input queue like any other input, at any time, and is told apart
    only by its flags and run ID in the logs and telemetry.
11. **Selectors:** by item (and one record of it), by intake and landing time, and by the version that made the
    output, so an operator can choose any of them, each traced to the run that chose it.

#### 15.8.2 Identity: where an object stands in its line

Each object a stage stores is named by its **place** and its **line**:
- **place:** the item, the generation, and the record's sequence number in that generation (`-1` for the item as a
  whole). Sequence numbers are only comparable within one generation.
- **line:** a revision number per stage the object has passed, in pipeline order: `split.convert.transform`. Each
  stage numbers its own revisions of a place: 1 when it first processes an input, and one more each time that same
  input is reprocessed, corrected or retried at that stage. The line written out with its generation reads, for
  example, `g2 r1.3.1`: generation 2, the split stage's first cut of it, the convert stage's third revision, the
  transform stage's first.

The user agreed to "two numbers, a generation and a revision", then (agreed the same day) revisions as a
number per stage, not one number, or the arrival rule breaks: the convert stage's revision 2 of record 5 reaches
the transformer, then convert's revision 1, delivered late, arrives after it. With one counter, the transformer
would number the late one 3 and make it current. With the line, the late one is `r1.1.1` against `r1.2.1`, and is
superseded on arrival.

Record IDs stay the keyed digest of the record's content (DESIGN.md §7.3), the proof of what a record holds. They no
longer identify an object alone: an identical document in two generations, or two revisions of a record whose
output is the same, are different objects with the same content. So:
- **registry keys** (`StageRegistry.Key`) and the fail store's key gain the generation and the line;
- **message IDs** gain `~g<n>` past generation 1 and `~r<line>` past `r1.1.1`, as completion numbers do
  (`~c<n>`), so every message of today keeps its ID, and a reprocess is never dropped as a repeat by a stage, a
  broker's de-duplication (Artemis, Service Bus, SQS FIFO) or `QueueReader`;
- **stored rows** (`split_records`, `records`, the transformer's) gain `generation`, `line`, `cause` (`reprocess`,
  `correction`; null for first processing), `run_id`, and `superseded_on_arrival`. Rows written before read as
  generation 1, `r1.1.1`, no cause. Contract 3.0 is unreleased, so the columns are added under it.

Exact copies (§15.5) are unchanged across items and within a generation, but a reprocess never counts as an exact
copy of what it supersedes: it is always stored and sent (§15.1, rule 7, which said "nothing new is stored" when
the outcome was the same, is replaced). A stage may still note that the output's digest equals the one it
supersedes (`same_output=true` on its audit) as information for the operator.

#### 15.8.3 Which object is current

At any stage, and for a system after the transformer:
- **between generations of an item:** the higher generation supersedes every object of the lower one, whatever its
  sequence number;
- **within a generation:** for the same sequence number, the higher line supersedes the lower, compared stage by
  stage from the left.

A stage learns of a generation from any message of it: a record, or the item's own messages (`item.split`,
`item.progress`). Without the item's messages, a new generation that failed as a whole at the split stage (a new
splitter rejects the item) would send nothing downstream, and the stages after would go on treating the older
generation as current.

What a stage records:
- **the latest generation of each item it has seen** (`stage_item` gains `generation`; the split stage's
  `split_item` too, as the generation it is cutting or last cut);
- **the latest line of each place,** from its registry rows;
- **superseding is derived, not written over rows:** a reprocess of a 10,000-record item doesn't rewrite 10,000
  rows. An object is superseded when its stage holds a newer generation or line. Only an object stored after a
  newer one gets `superseded_on_arrival` on its own row, so the late arrivals can be listed.
- **failures:** a failure of a superseded object moves to `SUPERSEDED` (`failure.superseded`, with the run and the
  object that superseded it), at once when the stage learns of the newer generation or line. This holds for every
  open or declined failure: a declined one is still recorded as declined first, its superseding a later event.

#### 15.8.4 The command and the run

`reprocess` on a stage's admin, run as `SDH_OPERATOR`, with a reason on standard input (sealed as a decline's), and
one selector:
- `--item-id <id>` (repeatable): at the split stage, the item; at a later stage, every current record of the
  item there; with `--seq <n>`, the one record;
- `--intake <name> --landed-from <time> --landed-to <time>`;
- `--made-by <version>`: what that version of the stage made (from `stage_version`, as `sdh-admin versions` lists
  it, §14.5).

`--dry-run` lists what the selector chooses, with counts by version and what would be refused, and records
nothing. Otherwise the command records a run, expands the selector once into the objects it chooses (so the run
is a fixed list: items landed later aren't added; a rerun is a new run), and prints the run's ID. It makes no output
and sends nothing itself: the CLI holds no broker credentials. `reprocess --status --run <id>` shows the run object
by object, with no content; `reprocess --cancel --run <id>` withdraws what isn't yet queued.

Tables per stage: `<schema>.reprocess_run` (ID as a UUIDv7; stage; selector; who, when, the reason sealed; the
version it was asked under; `PENDING`, `QUEUED`, `DONE`, `CANCELLED`) and `<schema>.reprocess_object` (run, place,
the line or generation it was given, state, and the outcome once known).

**Queueing.** Each renewal interval, a pod of the stage takes the run's objects not yet queued, each under the
item's lease (split) or the record's claim (later stages), so two pods never queue the same object:
1. it checks the object is still current at the stage (else `refused: superseded`);
2. it gives it its generation or line: the item's latest generation + 1 at the split stage; the place's latest
   revision at this stage + 1 at a later stage; and records that in `reprocess_object` before sending, so a resend
   after a crash keeps the same number;
3. it puts a reprocess message on the stage's own input queue: `item.reprocess` at the split stage (naming the
   landed item and the generation), `record.reprocess` at a later stage (naming the stored input record and the
   line), with the run ID and cause;
4. the stage's workers take it as any input, at the stage's own pace.

Reprocess messages are signed by the stage's own key, and a stage accepts these two types only when signed by
itself (every other type it accepts from the stage before it, as now). Each stage needs leave to send to its own
input queue: a deployment change on each broker (§15.8.9).

**Refused, with the reason in `reprocess_object` and the run's status:**
- the object is superseded at this stage;
- the stored input is past retention, so it can't be read;
- the item has a hold release pending (§15.6e), to be applied first.

Nothing else is refused: an object in progress is interrupted (§15.8.6), and one never processed downstream is
simply processed there. A correction or retry not yet processed is warned about by the command (the reprocess will
supersede it), not refused (§15.8.5).

**What a reprocess reads at each stage:** the split stage, the landed item (`landed_items`); the convert stage, the
input record as the split stage stored it (the current object of its place in `split_records`, failed pieces
excluded, as they are the split stage's own failures); the transform stage, the converted record (`records`). A
stage after the first reads only rows of its input's current generation and line, judged from those rows' own
columns, so it needs nothing from the stage before's registry.

#### 15.8.5 Corrections and retries as revisions

What exists (§15.6c–§15.6f) is kept as the operator's steps (`export`, `correct`, `retry`, with the corrected
content in `stage_correction` and applied by the stage's workers), but what they make is recorded as a revision:
- **A correction or retry of one record** (at the convert or transform stage, or of one piece at the split stage)
  makes the next revision of its place, with cause `correction` (with the edit stored, as now) or `reprocess` (a
  retry). The failure is `SUPERSEDED` by it **whether or not the revision passes**; a revision that fails is a new
  failure of its own (`FAILED`, with its line), open, naming the one it followed. A piece corrected at the split
  stage must still give exactly one record (`CORRECTION_RECORD_COUNT` otherwise); for more, the operator corrects
  the item as a whole.
- **A correction or retry of the item as a whole** (`--seq -1` at the split stage) makes the item's next
  generation, with cause `correction` or `reprocess`. It may cut any number of records. Its earlier rule, that
  records already stored come out byte for byte (`CORRECTION_CHANGES_PASSED`), goes: the earlier generation is
  superseded as a whole rather than extended.
- **A correction waiting when a reprocess supersedes its object** isn't refused or discarded: when its turn comes
  it is processed, stored and superseded on arrival, by §15.8.1 rule 5. The edit is kept, and can be seen.
- **Decline and reopen** are unchanged, for current failures only.

The fail store's states become:

| State | Meaning |
|---|---|
| `FAILED` | failed, not acted on |
| `REVISION_PENDING` | a correction or retry is waiting to be processed (was `CORRECTION_PENDING`) |
| `DECLINED` | the operator chose not to act on it, with a reason |
| `SUPERSEDED` | a newer generation or revision exists; names it, the run or correction that made it, and its cause |

`FAILED_CORRECTION`, `CORRECTED` and `RETRIED` go: whether a newer object passed, and whether it was edited, is
told by that object (its own state, its cause). An item is complete at a stage when every current object of its
current generation there has passed or been declined. Sandbox rows in the states that go are moved once at start,
as completions were numbered (revision (cj)): `CORRECTED` and `RETRIED` to `SUPERSEDED` naming what passed,
`FAILED_CORRECTION` to `SUPERSEDED` with the correction's failure added as a `FAILED` row of its own.

This reverses the decision of earlier on 2026-11-09 that a failed retry returns its failure to `FAILED` (agreed
by the user): under the rules above, the retried object is superseded by the retry, and the retry's own
failure is the open one.

#### 15.8.6 Interrupted processing

- **At the split stage**, splitting an item can take a while, and may be part way, or `HELD` (§14.3), when its next
  generation is asked for. The new generation takes the item's lease; the older one stops where it is and ends as
  `INTERRUPTED`, keeping what it had cut. The split stage audits `split.generation_interrupted` (item, the
  generation stopped and the one that stopped it, the run, records cut so far, the offset reached) and sends the
  older generation's `item.split` with result `INTERRUPTED` and its count, so every stage after knows how many of
  its records to expect, accounts for each, and doesn't report the item's total missing
  (`stage.item_total_missing`). Counted in `sdh.split.generations_interrupted`. A `HELD` item reprocessed this
  way needs no `release`; a `CLOSED` one may be reprocessed too, in place of landing it again.
- **At the convert and transform stages**, a record is processed in one step and can't be cut short. A record in
  progress when its next revision is queued finishes; whichever is stored second is judged by §15.8.3, so the
  older is superseded either way, and the late one is marked on arrival. Nothing is lost, so this isn't counted as
  an interruption, but as a late arrival (`record.superseded_on_arrival`, below).
- **An item's older generation at a stage after the split** can't be interrupted either: its records still arrive
  and are processed and marked superseded. When the stage learns of the newer generation it audits
  `stage.generation_superseded` (item, both generations, the run, counts of the older one's records done, failed
  and still expected), and sends no item-complete message for the older generation thereafter.

#### 15.8.7 Messages and completion

- **Item-complete messages** (`item.split`, `item.converted`, later `item.transformed`) are sent per generation,
  with `generation`; completion numbers (§15.6e) count within a generation. A system after the transformer
  needn't wait for them; they let it tell when a generation is whole, if it wants to.
- **Every output of a reprocess or correction** carries `generation`, `line`, `cause` and `runId` (null for a
  correction, which names its correction instead), `revisionOf` (the record ID of the object it supersedes at the
  same place, when the stage holds one) and `resolves` for a correction or retry (the failure it was made for). An
  output stored superseded on arrival carries `superseded: true`. Such an output is still sent on (agreed),
  so lineage reaches the end of the pipeline; a system after the transformer can ignore it by the flag.
- **Producers:** no operator action sends the producer anything: not a reprocess, retry, correction, decline or
  reopen (decided with the user 2026-11-09). The producer's only responsibility to the pipeline is to deliver data;
  other systems direct any corrective work to the producer. This replaces §15.6e's sending the producer's result
  again (`incomplete`, then `completed`) when a failure changes state. The result sent when an item is first
  processed is unchanged.

#### 15.8.8 Telemetry

Audited, with no content, all carrying `sdh.run_id` (new), `sdh.generation` (new), `sdh.line` (new), `sdh.cause`
(new) and `sdh.began_at` (the stage where the reprocess began; new):
- `stage.reprocess_requested` (run, stage, selector kind, operator, objects chosen, objects refused);
- `stage.reprocess_queued` per object (place, the generation or line given);
- `stage.reprocess_refused` per object (place, reason);
- `stage.generation_started` per item (the generation, cause, stage it began at), at every stage on the
  generation's first message;
- `stage.reprocessed` per object processed (place, line, outcome `stored` or `failed`, `same_output`);
- `split.generation_interrupted`, `stage.generation_superseded` (§15.8.6);
- `record.superseded_on_arrival` (place, its line, the newer line or generation);
- `failure.superseded` (place, the newer object, the run or correction);
- `stage.reprocess_done` per run (totals).

Metrics: `sdh.stage.reprocessed` by stage, cause and outcome; `sdh.stage.superseded_on_arrival` by stage;
`sdh.split.generations_interrupted`; `sdh.stage.failures_superseded` by stage and cause. The fail store's
listing (`failures`) shows each failure's generation and line.

#### 15.8.9 What changes elsewhere

- **Replay goes** (§13.2): landing's `replay --item-id` is a reprocess at the split stage; the split stage's is a
  reprocess at the convert stage of every current record of the item; the convert stage's is a reprocess at the
  transform stage. The replay rules: "only a completed item" goes (§15.8.6 handles in-progress items); the
  broker's own ID per resend stays, now as the generation and line in the message ID; "the producer's result only
  on request" becomes never.
- **The duplicate rule** (§14.3) still holds for a redelivery: the same message, with the same generation and line,
  is a duplicate whatever its version. A reprocess is never a redelivery, since its generation or line is new.
- **A held split item** may be released (§15.6e) or reprocessed (§15.8.6).
- **Landing's `resubmit`** stays, for a new item from the producer's data, linked by `replaces_item_id`; a
  reprocess is for the same item.
- **Deployment:** each stage may send to its own input queue (Kafka ACLs, SQS and Pub/Sub permissions, Service Bus
  and Event Hubs send rights, Artemis and RabbitMQ users); the sandbox and the deployment guide add them.
- **Stable FHIR IDs** (§15.4) leave the pipeline's requirements: what a reprocess's output replaces is decided after
  the transformer.

#### 15.8.10 Order of building, once agreed

1. Generation and line in rows, keys, message IDs and the contract; the fail store's states and the one-time move
   of sandbox rows; the currency rule at every stage (§15.8.3), with `superseded_on_arrival` and
   `failure.superseded`.
2. Corrections and retries as revisions (§15.8.5).
3. The run tables and `reprocess` (`--dry-run` first), the pods' queueing and the self-signed reprocess messages,
   at the convert stage first (one record in, one out).
4. The split stage's reprocess: generations, interruption, `INTERRUPTED` item ends, `stage.generation_superseded`
   downstream.
5. Replay's removal; the brokers' send rights in the sandbox and the deployment guide.

Each with its tests, and tried on the sandbox.

**Step 1 as built (2026-11-09; not yet tried on the sandbox).**

- `Lineage` (connector-contract) holds a generation and a revision per stage, trailing 1s left out, and orders by
  generation, then revision stage by stage. A message ID gains `~g<n>` past generation 1 and `~r<line>` past
  revision 1, so IDs from before are unchanged. Record messages carry `generation`, `line` and, when true,
  `superseded`; item messages carry `generation`. Each is left out of the payload at its first value.
- The records tables gain `generation` and `line` (fields 29 and 30), added by schema evolution; a row without them
  is the first of everything. A registry key is the record ID plus the lineage's suffix.
- The fail store's key is now `(stage, item_id, seq, generation, line)`, moved by a one-time statement on start, and
  it gains `superseded_how` (`REVISION`, `GENERATION`, `ARRIVAL`) and `superseded_by`. Events gain `generation` and
  `line`. Operator commands act on the current failure at a number: not superseded, of the newest lineage.
- The registry applies §15.8.3 in `store` and `expect`, under a lock on the item's row taken once per transaction,
  items in one order. A record whose claim was lost changes nothing (its claim is checked, and its row locked,
  first). A record of a newer generation starts the item again: counts and total dropped, older failures
  superseded (`GENERATION`), `stage.generation_superseded` and `stage.generation_started` audited. A late record is
  stored, not counted, its failure kept as `SUPERSEDED` (`ARRIVAL`), `record.superseded_on_arrival` audited and
  `sdh.stage.superseded_on_arrival` counted. A newer revision at a place marks the older row superseded and its
  failure `SUPERSEDED` (`REVISION`); it adds to the failed count if it failed, never to done. An item's resolved
  count takes `CORRECTED`, `RETRIED` and `REVISION`-superseded failures of its current generation only. A total of
  an older generation is dropped, audited `stage.total_superseded`.
- The worker asks the registry which outputs are superseded already, and sets `superseded` on their messages before
  sealing them; `store` decides again under its locks.

Left from step 1, with why:

- The renames of the fail store's states wait for step 2, since they follow from how corrections become revisions
  there.
- Superseded on arrival is kept in the registry (`stage_record.late`), not in the Iceberg rows, and on the message.
- `RecordBatchWriter` keeps a lineage per item and number within a batch, so one batch can't hold two lineages at
  the same place. No stage writes that today; step 3 must not.
- A pending correction whose failure has since been superseded is refused when it finishes (`CorrectionStore`
  acts on current failures only). Step 2 decides what it becomes. (Done in step 2: it is stored late.)

**Step 2 as built (2026-11-09; tried on the sandbox, SANDBOX.md, and the four things seen there fixed).**

- The fail store's states are `FAILED`, `REVISION_PENDING`, `DECLINED` and `SUPERSEDED`. A correction or retry, when
  submitted, notes the revision it makes (the failed object's line, this stage's revision one more) and the failed
  object's record ID in `stage_correction` (`record_id`, `generation`, `line`). `correct`, `retry` and `decline`
  take a `FAILED` failure only; `failures` shows each failure's lineage.
- **At the convert stage** a correction is claimed under its place's key (record ID and lineage suffix) and stored
  through the currency rule like any object: it supersedes the failure at its place (`REVISION`), whether or not it
  passes; it is never counted as a new record, and adds to the failed count if it failed, its failure a `FAILED` row
  of its own line. One that arrives after something newer is stored late, flagged `superseded` on its message, its
  correction still noted `PASSED` or `FAILED`; nothing is refused. Its row of `records` and its `record.stored` carry
  the revision.
- **At the split stage** a piece's correction is the piece's next split revision (`r2`), on its row of
  `split_records` and its `record.split`. A correction of the item as a whole keeps the rules of §15.6d
  (`CORRECTION_CHANGES_PASSED`, records after the kept ones added in the same generation) until step 4 makes it the
  item's next generation; the records it adds are its split revision (`r2`), on their rows and messages, those
  kept keep theirs, and its failure is superseded, or the failed revision is a failure of its own, as for a
  piece.
- **The producer** gets a result only for an item's first report at the convert stage, in its first generation (no
  `item.progress` or completion of it sent before). The result step runs on no later report, since each follows an
  operator's action.
- **The one-time move**, at every start, changing nothing once done (`CorrectionStore.moveStates`, audited
  `failure.states_moved`): `CORRECTION_PENDING` becomes `REVISION_PENDING`; `CORRECTED` and `RETRIED` become
  `SUPERSEDED` by their correction's revision (the record that passed stays in `resolved_by`); `FAILED_CORRECTION`
  becomes `SUPERSEDED` by the next revision, whose failure (the last failed correction's error code) is added as
  `FAILED`, and at the convert stage counted among the item's failed records. Pending corrections have their
  revision noted.
  Each move adds an event by `pipeline`; earlier events keep the state names they were written with, so the
  history reads as it happened. `superseded_by` always names a revision (or generation), never a record; a failure
  superseded is changed by `pipeline`, and rows the first build of the move named by their record, or left changed
  by the operator, are put right at the next start, keeping when they changed.
- **`failures --item-id`** shows under each failure only its own events (the event's lineage, §15.8.8) and the
  corrections submitted for it (the revision each makes, the next of the failure's; one from before revisions
  were noted, under the number's first failure).

Left from step 2:
- `retry` of a failed revision processes what failed first (the record as split), not the corrected content
  that failed; correct it again to try an edit.
- A reprocess (step 3) must take its revision number past any correction still pending at the place, so the two
  never share a line. Settled in step 3, by the shared counter.

**Step 3 as planned (2026-11-09, with the user).** At the convert stage, in the stage library, so the transformer
gets it as built. Settled with the user:
- **The stage's own keys.** A reprocess message is sealed to the stage itself and signed by it, so the stage is
  given its own inbox public key and signing public key (`sdh.crypto.keys.inbox.public-pem`,
  `sdh.crypto.keys.signing.public-pem`), each pinned by its SHA-256 as the recipient's is; a KMS signing key's
  public half comes from the KMS. The opener trusts the stage's own signing key alongside the stage before's, and
  says which signed: the worker takes `record.reprocess` only when signed by its own stage, and every other type
  only when signed by the stage before.
- **Selectors:** `--item-id <id>` (repeatable), with `--seq <n>` for one record, and `--made-by <version>`. The
  landing-time selector (`--intake --landed-from --landed-to`) waits for the split stage in step 4, which reads
  `landed_items`: `split_records` holds neither intake nor landing time.
- **What `--item-id` chooses:** the item's current objects in the input, `split_records` (the newest generation
  and line at each place, failed pieces left out), whether or not this stage has seen them, so a record whose
  message was lost is processed as new and flagged as a reprocess. `--made-by` chooses this stage's current rows
  made by that version, and reads each one's input at its place in `split_records`.

Worked out in planning, as consequences of what was decided:
- **Revision numbers come from one counter per place** (`<schema>.stage_revision`: stage, item, seq, generation,
  the line before this stage, the last revision given here), taken under its row's lock by `correct` and `retry`
  when submitted and by queueing a reprocess, and never below what is stored there, failed, or noted by a pending
  correction. So a correction and a reprocess never share a line, whichever comes first; this settles the item
  left from step 2.
- **Queueing.** Each renewal interval, every pod takes up to a batch of a run's objects not yet queued,
  `FOR UPDATE SKIP LOCKED`, so no two pods take the same one; checks it (refused `superseded` if its input's place
  now holds something newer than when the run was made, `past_retention` if the input can't be read); gives it its
  line from the counter; and commits that before sending, so a pod that stops between the two resends it, at the
  next interval, under the same line and message ID. The message (`record.reprocess`) holds the `record.split` as
  the reconciler rebuilds it from `split_records` (§9), at the line given, with the run ID and cause `reprocess`.
  It is sent on the stage's own input topic, keyed by record ID as the split stage keys it, so it shares the
  record's partition.
- **Processing.** The worker takes the `record.split` inside as any record, at its line, through the currency rule
  (§15.8.3); its `record.stored` carries `cause`, `runId` and `revisionOf` (the record it supersedes at the place,
  when there is one), and its row of `records` gains `cause` and `run_id` (fields 31 and 32). The object's outcome
  (`stored` or `failed`, the output's record ID, `same_output`) is noted in `reprocess_object` in the registry's
  transaction, and the run is `DONE` once every object is refused, cancelled or has an outcome
  (`stage.reprocess_done`).
- **The command** records nothing with `--dry-run`, which needs no reason; otherwise it records the run and its
  objects in one transaction, prints the run's ID, and warns of corrections still pending at the places chosen.
  `--status --run <id>` lists the run object by object; `--cancel --run <id>` cancels what isn't yet queued.
- **Brokers:** the sandbox's Kafka has no authentication yet (SANDBOX.md), so the convert pods can write their
  own input topic as they are; the send rights of each broker are step 5.

**Step 3 as built (2026-11-09; tried on the sandbox, SANDBOX.md).** As planned, with these choices made in building:

- **Where it lives.** The stage library holds all of it but reading the input: `ReprocessStore` (the run tables),
  `ReprocessInputs` (a stage's input: the current object at each place of an item's newest generation, failed or
  not, with the message it would have been sent as), the worker's queueing (`StageWorker.reprocess`, run each
  renewal interval with the corrections), and the command (`ReprocessCommand`, over `ReprocessAdmin`). The convert
  stage gives `SourceInputs` (`ConvertInputs` until §17 step 3: `split_records`, with the split stage's corrections) and turns it on only when both
  `sdh.stage.reprocess` (its own input, as a queue to send to) and its own public keys are configured; one without
  the other stops it at start.
- **The counter** is `<schema>.stage_revision` (`Revisions`, connector-core), shared by `correct`, `retry` and
  queueing; its floor is what `stage_record`, `stage_failure` and `stage_correction` already hold at the place.
- **Queueing.** A batch of 100 objects per pod per interval, oldest run first. Each object is checked against the
  input as read in that interval: refused `superseded` if the item has a newer generation or its place a newer line,
  `past_retention` if its piece isn't there. Its line is committed with its state, then it is sent and marked sent.
  One queued but unsent after the claim's life is sent again under the same line, so the same message ID
  (`rp-<item>-<recordId><suffix>`); one whose piece is gone by then is refused. A run is `QUEUED` once an object is,
  and `DONE` when none is pending or queued (`stage.reprocess_done`, with counts stored, failed and refused).
- **Signers.** The worker opens each message with the signer's kid: `record.reprocess` is taken only from the
  stage's own kid, any other type only from another (the stage before's); anything else is rejected,
  `stage.message_rejected` `wrong_signer`.
- **Processing.** The `record.split` inside is classified and claimed as any record, at its line, so a reprocess
  delivered twice is a duplicate. The processor is given the cause (`StageProcessor.process` with a `Cause`), which
  convert keeps on the row (`cause`, `run_id`); a correction's row now says `correction`. Before sealing, the worker
  reads the object it would supersede (`StageRegistry.current`): its message key is `revisionOf`, and its digest
  decides `same_output`. The object's outcome is noted in the registry's transaction, and `stage.reprocessed` audited
  and counted (`sdh.stage.reprocessed` by stage, cause and outcome) once committed.
- **The command.** `--item-id` takes one or more IDs, comma-separated (the shared argument parser takes each option
  once). `--made-by` reads this stage's current stored or failed rows of that version, narrowed by `--item-id`, and
  chooses the current piece at each of their places. At most 100,000 objects a run. The run's reason is sealed
  under the run's ID (record number -2), as a failure's is. The command is audited `admin.reprocess`, with the
  selector, the count and the run's ID.
- **Fixed on the way:** the records writer kept a record's lineage, cause and `correction_of` by place, never
  cleared, so a second record of the same place in one batch (a reprocess and a late original) could take the
  first's. Each is now given, or cleared, for every record.
- **The sandbox:** the convert pods get their inbox public key (`inbox-public.pem`, pinned) and
  `sdh.stage.reprocess` on `<pipeline>.split`; their signing key's public half comes from Vault.

Left from step 3:
- The time selector (`--intake --landed-from --landed-to`) is step 4's.
- A `record.reprocess` rebuilt from `split_records` knows no queue message its record is in, so a reprocessed
  record of a message batch that fails names no message (as the reconciler's rebuilt records, §9).
- `RecordStored` of a correction doesn't yet carry a `cause`; its row does.

**Step 4 as planned (2026-11-09, with the user).** The split stage's reprocess, in the split stage
(file-connector, connector-core), reusing the run tables and the command of step 3. Settled with the user:
- **An interrupted generation's end at a later stage** is an open total, as `item.progress` is: its records are
  counted, so no total is reported missing, but the item stays incomplete there, and no item end and no producer
  result is sent for that generation. A newer generation always follows, since the split stage interrupts one
  only to start the next.
- **The producer's result** is sent for an item's first report at the convert stage, whatever its generation, if
  no report of the item was sent before. So an item whose first cut was interrupted still gets one result; an item
  already reported gets nothing for a reprocess (§15.8.7 unchanged otherwise).
- **The landing-time selector** (`--intake --landed-from --landed-to`) is the split stage's only; later stages
  choose by item or version.
- **No `--seq` at the split stage:** a reprocess there is of the whole item, its next generation. One record is
  reprocessed at the convert stage; a failed piece is still retried or corrected as now.

Worked out in planning, as consequences of what was decided:
- **The generation on the checkpoint.** `Checkpoint` gains `generation` (from 1; one written before reads 1), the
  `cause` and run that made it, and, for a whole-item correction, which correction it cuts. `split_item` gains
  `generation` and `version` in plain text, for queries. A new generation replaces the item's checkpoint: cut from
  the start, counts from 0, completion numbers starting again. Its rows of `split_records` carry `generation`,
  `cause` and `run_id`, and its `record.split` messages the generation and cause. Everything that reads an item's
  rows of `split_records` at the split stage reads only the checkpoint's generation: settling a batch at resume,
  sending again what wasn't acknowledged, failed pieces, corrections, the reconciler.
- **Generation numbers** come from the counter of step 3 (`stage_revision`), at the item's whole-item place
  (`seq -1`): one more than the higher of the counter and the checkpoint's generation, taken by queueing a
  reprocess and by submitting a whole-item correction or retry, so the two never share a generation.
- **Queueing,** as at the convert stage, each renewal interval, by the loop that applies corrections and releases
  (`SplitCorrections`). An object is refused `superseded` if the item is now past the generation chosen,
  `past_retention` if the item is no longer in `landed_items`, and `release_pending` if a hold release is waiting
  (§15.8.4). Its message, `item.reprocess`, holds the item's `item.landed` rebuilt from `landed_items`, the
  generation and the run, sealed to the split stage itself and signed by it, and goes to its own input
  (`sdh.stage.reprocess`, the landed topic), keyed by item as landing keys it.
- **Signers:** the split stage takes `item.reprocess` only when signed by itself and `item.landed` only when signed
  by another (landing), as the convert stage does (`stage.message_rejected` `wrong_signer`).
- **Taking an `item.reprocess`,** under the item's lease (another pod holding it means try again later, as now):
  - the item already at that generation or past it: a repeat, resumed if in progress, otherwise done;
  - `SPLITTING` or `HELD` at an older generation: interrupted (below), then the new generation starts;
  - `ARCHIVING` or `FAILING`: its cut is over, so it is finished as now first, then the new generation starts;
  - `COMPLETED`, `FAILED`, `CLOSED` or `INTERRUPTED`: the new generation starts.
  The new generation is cut from the start of the landed item under the version running now; its failures are
  recorded at its generation, those of the generations before superseded (`GENERATION`, `failure.superseded`),
  and `stage.generation_started` audited. When its end is sent, the object's outcome is noted (`stored` if it
  completed, `failed` if it failed as a whole) and `stage.reprocessed` audited and counted.
- **Interruption.** A pod splitting an item looks, after each batch is committed and sent, for a newer generation
  queued for it. If there is one it stops there: the checkpoint becomes `INTERRUPTED` (a new state), audited
  `split.generation_interrupted` (item, generation, the generation and run that stopped it, records cut, offset)
  and counted (`sdh.split.generations_interrupted`), and the generation's end is sent: `item.split` with
  `interrupted: true`, its counts and generation. The lease is let go, and the `item.reprocess`, tried again,
  starts the new generation. If the pod stopped instead, the pod that takes the reprocess finds the item
  `SPLITTING` at the older generation and interrupts it itself, after settling a batch left part way. A `HELD`
  item is interrupted the same way, needing no `release`.
- **Later stages** take `item.split` with `interrupted` as an open total of its generation (above); the newer
  generation's first message supersedes it there (`stage.generation_superseded`, step 1). The contract's
  `ItemSplit` gains `interrupted`, left out when false.
- **The command,** on the split stage's admin: `--item-id` (one or more), `--made-by <version>` (items whose current
  generation was cut under it), and `--landed-from <time> --landed-to <time>` (items landed in that range, from
  `landed_items`; `--intake`, if given, must be the stage's own), each narrowed by `--item-id` if given. `--seq`
  is refused. `--dry-run`, `--status`, `--cancel` and the limit of 100,000 objects as at the convert stage; a
  whole-item correction pending is warned about.
- **Whole-item corrections and retries become generations** (§15.8.5, left from step 2). `correct --seq -1` and
  `retry --seq -1` note the item's next generation when submitted. A pod applies one as it does a reprocess, but
  only to an item whose splitting has ended, as now: the new generation is cut from the start of the
  correction's content (a retry's is the item as landed), with cause `correction` or `reprocess`, its records
  resolving the item. It may cut any number of records, and pieces may fail, each a failure of the new
  generation. The correction is `PASSED` if the cut reached the end, `FAILED` if the item failed as a whole,
  that failure the new generation's own. `CORRECTION_CHANGES_PASSED` goes. A generation cut from a correction is
  resumed from the correction's content, which its checkpoint names. One whose generation is older than the
  item's by the time it is applied (a reprocess came first) isn't cut: it ends `FAILED`
  (`CORRECTION_SUPERSEDED`), its edit kept and readable, and no failure added.
- **The sandbox:** the split pods get their own inbox public key and `sdh.stage.reprocess` on `<pipeline>.landed`.

**Step 4 as built (2026-11-09; tried on the sandbox, SANDBOX.md).** As planned, with these choices made in building:

- **Where it lives.** The split stage's part is in file-connector: `SplitReprocess` (queueing, sending, noting
  outcomes, and telling the pipeline of a newer generation), `ItemSplitter.reprocess` (under the item's lease),
  `SplitHandler` (the signer checks) and the loop that applies corrections and releases (`SplitCorrections`), which
  queues a batch each renewal interval. `ItemProcessor.reprocess` and the pipeline's generations are in
  connector-core. As at the convert stage, the stage reprocesses only once both `sdh.stage.reprocess` and its own
  public keys are configured; one without the other stops it at start.
- **The run tables at the split stage.** The split stage has no stage registry, so `ReprocessStore` makes its own
  tables (`createTablesIfMissing`). An object of the split stage is the item at its whole-item place (`seq -1`); its
  record ID is the item's ID. `reprocess_object` gains `made_generation`, the generation the object was given, read
  in place of the line for a split object.
- **Interruption** is asked of the run tables (`Interruptions`, `ReprocessStore.newerQueued`): an object of the item
  `QUEUED` with a higher generation than the one being split. Its message need not have been sent yet. A whole-item
  correction or retry never interrupts: it is applied only to an item whose splitting has ended.
- **Outcomes** are noted when the generation's end is sent: `stored` (the item split to its end), `failed` (failed as
  a whole), `interrupted` (a newer generation stopped it), or `superseded` (a newer one was there first).
- **Later stages.** The stage registry's item row gains `reported`: whether any report of the item was sent here,
  in any generation. A new generation starts its counts again, but not this. Rows from before are backfilled from
  their counts (`progress_key` set, or a completion). The producer's step runs on the first report only, whatever
  its generation. An interrupted generation's total is noted as looked at, with nothing sent, and audited
  `item.progress` with `result=INTERRUPTED`. The producer's result counts the failures of the report's generation
  only.
- **Reading by generation.** Also reading only one generation: the split stage's `replay` (the checkpoint's), the
  convert stage's replay (the newest), `export` of a failed piece (the newest generation's), and a correction at the
  convert stage (its failure's).
- **The command.** `ReprocessAdmin` gains `wholeItems`, `madeBy` and `landedBetween`, with defaults for the stages
  after. `--landed-from` and `--landed-to` are ISO-8601 times with their offsets, the end excluded, read from the
  summary rows of `landed_items` (`LandedItems.landedBetween`), earliest first. `--made-by` reads `split_item`'s
  plain `version`. A run at the split stage is asked under the stage's full version (build, splitter and settings),
  as its checkpoints keep it.
- **Fixed on the way:** records sent again after a restart (or as a failing item ended) were rebuilt from their rows,
  which don't keep what a correction's generation resolves, so they went without it. They are now rebuilt with the
  checkpoint (`SplitMessages.recordSplit(Checkpoint, …)`), as `replay` does.
- **Tests:** contract round trips; at the split stage, a reprocess as the next generation, a repeat and an older one
  superseded, an interruption, a correction as a generation, a failed retry, and a correction a reprocess
  superseded; at a later stage, an interrupted generation's end; the command's whole-item and landing-time choices;
  and, in the app test, a run queued, sent to the stage's own input, taken and noted, and an `item.landed` signed by
  the stage itself refused. Not tested on its own: a pod interrupting an item another pod left `SPLITTING`.

**Step 5 as planned (2026-11-09, with the user).** Replay's removal, and the brokers' send rights. Settled with
the user:
- **A landed item never split** (its `item.landed` lost, the reconciler not yet past its grace) is chosen by a
  reprocess at the split stage, by `--item-id` or by landing time, and split as any reprocess: under the next
  number of the generation counter, which is 2 for an item with no checkpoint. Its `item.landed`, if it arrives
  late, is then generation 1, a repeat of an item already past it. Until now such items were left out of a run
  without a word. An item ID not in `landed_items` for the stage's intake is still chosen as nothing.
- **All three replays go** (landing, split and convert), with `Replayer`, `ConvertProcessor.replayMessages`,
  `ResultsChannel`'s sending again and their tests. What each did is now:
  - landing's `replay --item-id`: a reprocess at the split stage, or the split stage's reconciler, which already
    splits every landed item whose message was lost;
  - the split stage's `replay --item-id`: a reprocess at the convert stage of the item's current records, which
    reads them from `split_records` whether or not they ever reached it, or the convert stage's reconciler;
  - the convert stage's `replay --item-id [--with-result]`: the transform stage's reprocess, once that stage is
    built. The producer's result is never sent again (§15.8.7).
- **The admin CLIs no longer send.** Without replay, no admin command of landing or the stages publishes, so their
  broker publisher, Camel context and signing go; the admin opens the database, the catalog and the keys it
  unwraps with (landing's none: `resubmit` reads only `landed_items` and the lease database). Broker settings in
  an admin's configuration are ignored, as other settings it doesn't use are.
- **The split stage's pods send an item's end after a decline or reopen** (settled with the user once it was found
  that the split stage's admin sent it, through the replay code). As at the convert stage, the admin only changes
  the fail store. `split_item` gains `announced_at`: the database's time just before an end's failures were
  counted, written once the end is sent (rows from before take the time the column is added). Each renewal
  interval, the loop that applies corrections and releases also looks for items whose splitting has ended
  (`COMPLETED` or `FAILED`) with a failure of their generation changed since, and sends each one's end again under
  its lease, as it does for a correction whose end a pod didn't send. An end that couldn't be sent leaves the time
  as it was, so it is tried again.
- **Send rights are documented, not added to the sandbox.** DEPLOYMENT.md gains, for each of the seven brokers,
  what each stage's identity must be allowed: to read its input, to send to its own input (for reprocess), and to
  send onward. The sandbox's Kafka has no TLS, SASL or ACLs (SANDBOX.md, "Not yet as in production"), so there is
  nothing to grant there; that section says so.

Kept: `PushService`'s replays (a repeated push answered with its first receipt) are another thing and stay, as does
`QueueReader` dropping repeats, which a stage still sends after a crash. Earlier write-ups that name `replay` keep
their history, with a note where they describe what an operator does now.

**Step 5 as built (2026-11-09; tried on the sandbox, SANDBOX.md).** As planned, with these choices made in building:
- **Choosing an item never split.** The split admin's inputs look in `landed_items` for an item with no checkpoint,
  and choose it at generation 1 only if it was landed for the stage's own intake. Queueing gives it the counter's
  next number, 2, and taking its `item.reprocess` finds no checkpoint and cuts it from the start. Its `item.landed`,
  arriving after, finds the item past it and cuts nothing.
- **Sending an end again.** `ItemCheckpoints` gains `now` (the database's clock, which the fail store's
  `changed_at` uses), `markAnnounced` (never moves the time back) and `changedEnds`. The time is read after the
  item's failures are recorded and before they are counted, so recording them doesn't by itself call for another
  end. `SplitCorrections.sweep` sends the ends of both kinds of item, those whose correction's end wasn't sent and
  those whose failures changed, each once.
- **Removed:** the three `ReplayCommand`s and `ReplayCommandTest`, `Replayer` and its test,
  `ConvertProcessor.replayMessages`, `RegistryReport.totalMessage` (the column is still written), the results
  channel's resend under a broker suffix, and the admins' publishers, Camel contexts and, at landing, keys.
- **Tests:** the split admin's decline and reopen have a pod send the end, once each, and the admin nothing;
  `reprocess --dry-run` chooses a landed item never split and nothing for an unknown one; the app test splits such
  an item as generation 2 and its late `item.landed` cuts nothing.

## 16. Items with no records (decided and built 2026-11-09)

Raised by the user on 2026-11-09: an item may hold a single document (one `ClinicalDocument`, one CSV row), and
that is one record, as now. But an item that gives no records at all should never pass unseen, and every splitter
should treat such items by the same rules. Designed, then built the same day (§16.7).

### 16.1 Found (2026-11-09)

Every built-in splitter was run against the same inputs. Today a split with no records completes the item
(`stage.item_completed count=0`) with no warning, and the formats disagree:

| Input | `ccda` / `xml` | `csv` | `csv` with header | `delimited` | `fhir` | `x12` | `edifact` | `parquet` |
|---|---|---|---|---|---|---|---|---|
| 0 bytes | 0 records | 0 | 0 | 0 | 0 | 0 | 0 | — (not Parquet) |
| blank (spaces, tabs, line ends) | 0 | **a record per blank line** | **a record** | 0 | 0 | **fails** `PARSE_X12_INVALID_ENVELOPE` | **fails** `PARSE_EDIFACT_UNEXPECTED_SEGMENT` | — |
| text that isn't the format | **0** | a record per line | rows | a record (lines before the first match) | fails `PARSE_JSON_MALFORMED` | fails | fails | fails `PARSE_PARQUET_MALFORMED` |
| the format, nothing in it | XML without the boundary element: 0 | — | header only: 0 | — | `[]`: 0 | `ISA…IEA`, or `GS…GE`, with no `ST`: 0 | `UNB…UNZ`, or `UNG…UNE`, with no `UNH`: 0 | no rows: 0 |

A single document is one record in every format (`XmlSplitterTest.wholeFileIsOneDocument`, and every sandbox
`check`, whose C-CDA files hold one document each).

### 16.2 Rules (decided with the user 2026-11-09)

1. **Empty fails the item, and an operator decides.** An item of 0 bytes, or blank (rule 2), fails as a whole
   (`--seq -1`, §15.6d), whatever the format, with `SPLIT_EMPTY`: nothing was sent.
2. **Blank is empty, in every text format.** Input of only spaces, tabs, CR and LF (after a byte-order mark, where
   the format allows one) is no data: no splitter cuts a record from it or fails it as malformed. In CSV a line of
   only spaces, tabs and CR is skipped, as an empty line already is, unless it holds the delimiter (with
   `csv.delimiter` a tab, a line of tabs is a row of empty fields). Parquet is binary, so this rule doesn't apply
   to it.
3. **Content the splitter can't cut is one record.** Decided with the user 2026-11-09: an item that isn't empty
   or blank, from which the splitter cut nothing, is taken as a single document: the whole item becomes one record
   (offset 0, the item's length) and goes to the convert stage like any other. The stages after parse it and
   decide whether it is garbage; a splitter mostly can't tell a document it doesn't recognise from garbage, and
   isn't asked to. Examples: plain text, or XML without the boundary element, sent to `ccda` or `xml`.
   The one exception is a splitter that can tell from the item's start that it isn't its format: it fails the item
   as now (`PARSE_*`), and no splitter gains new checks for it. Those today: X12 (no `ISA`), EDIFACT (no `UNA` or
   `UNB`), FHIR (not JSON, or the wrong `resourceType`) and Parquet (not a Parquet file).
4. **A container with nothing in it fails, and an operator decides.** A splitter that read the item as its format
   and found no records in it says so, and the item fails as a whole with `SPLIT_NO_RECORDS`, since it holds no
   data. Taking it as one record would hide that: a header-only CSV would pass as one row of data. Those today: a
   CSV header alone (`csv.header=true`), an X12 interchange or group with no `ST`, an EDIFACT interchange or group
   with no `UNH`, FHIR `[]`, a Parquet file with no rows, a `delimited` file of separator lines alone (`SEPARATOR`
   mode). The XML formats can't tell an empty wrapper from a
   document they don't know, so for them rule 3 applies.
5. **The split stage decides, not each splitter.** A splitter only cuts, skips blank input (rule 2), fails what it
   can tell isn't its format (rule 3), and reports a container it found empty (rule 4). What happens to an item
   with no records is decided once, by the split stage (`WholeItemSession`, which `SplitterRegistry` puts around
   every session it opens), so it holds for every format, a plugin's too: a plugin that reports nothing gets rule
   3.

Two codes rather than one (accepted by the user 2026-11-09), since "nothing was sent" (`SPLIT_EMPTY`) and "a
well-formed file with nothing in it" (`SPLIT_NO_RECORDS`, most often the wrong file or the wrong boundary setting)
point the operator to different causes.

### 16.3 What the split stage does

- **At the item's end,** before it moves to `ARCHIVING`, if the item's record count is 0:
  - 0 bytes or blank: the item fails with `SPLIT_EMPTY`;
  - the splitter's session reports an empty container (new on `SplitSession`, `false` by default): it fails with
    `SPLIT_NO_RECORDS`;
  - otherwise the whole item is stored and sent as record 0 (its origin line 1, column 1; its resume point the
    item's end), then the item completes with 1 record. Nothing new is audited for it beyond `item.split count=1`
    (left open: an audit `split.whole_item` would let an operator see that the splitter found no boundary).

  A failure takes the path of any item that couldn't be split (§15.6a): `item.failing` (with the code), its
  failure in the fail store with `--seq -1`, `item.split` with the failure, `FAILED`, and `item.result` `failed` to
  the producer when results are on. Nothing reaches the convert stage.
- **Resumed items:** the count is the checkpoint's, across restarts, so an item resumed after its last record is
  not mistaken for an empty one. The whole-item record is committed like any batch, so a restart before the commit
  makes it again, the same.
- **Message batches** (§5.3.16): each message is split on its own, and a message that can't be split already fails
  alone. The same rules hold at each message (accepted by the user 2026-11-09). A blank or empty message fails
  alone, as one failed piece (`SPLIT_EMPTY`), and so does an empty container (`SPLIT_NO_RECORDS`); a message the
  splitter cut nothing from is one record; the next message carries on. An Artemis message of a type with no byte
  form is landed empty and reported at landing (`message.unsupported_type`), so it now also shows as a failed piece
  at the split stage. A batch with no messages at all fails as a whole (`SPLIT_EMPTY`).
- **The operator's choices** for a failed item are those of any failed item (§15.4, §15.6e), with nothing new:
  - `decline`, with a reason ("the producer confirms the file is empty"): the item completes with 0 records, and
    the convert stage completes it with 0 records too, as tried on the sandbox (SANDBOX.md "Checked",
    2026-11-09);
  - `correct --seq -1`: the corrected item is split from its start, and a correction that is again empty fails
    the same way (`FAILED_CORRECTION`, with the code);
  - `retry`: the same bytes give the same failure, so it goes back to `FAILED`;
  - `resubmit` (§13.5): the producer's right file lands as a new item linked to this one.
- **Telemetry:** nothing new beyond the codes. `item.failing` and `stage.item_completed result=FAILED` already
  carry the code; `sdh.stage.failures` counts it by code.

### 16.4 What the stages after must catch

Rule 3 relies on the stages after, each judging only what it is for (decided with the user 2026-11-09):

- **The convert stage** judges only whether a record can be turned into well-formed JSON from its format, for
  every format alike. It can't tell whether the content is right. C-CDA: plain text fails (`CONVERT_XML_MALFORMED`,
  no root element), but any well-formed XML converts, a C-CDA document or not. CSV: almost any text is rows of
  fields, so it fails only what can't be read as CSV (`CONVERT_CSV_MALFORMED`, `CONVERT_CSV_FIELD_COUNT`). X12 and
  EDIFACT: it fails segments it can't read (`CONVERT_X12_MALFORMED`, `CONVERT_EDIFACT_MALFORMED`,
  `CONVERT_EDIFACT_CHARSET`), not a transaction that is wrong for its type.
- **The transform stage** judges whether that JSON is what the format must be (a C-CDA document, an 837 claim, the
  columns a CSV feed must have), against its mapping, and fails what isn't.

So until the transformer is built, content that converts passes the pipeline, right or not, in every format. The
split stage doesn't add checks to make up for it.

### 16.5 Versions and items already split

Rules 2 and 4 change what `csv`, `x12`, `edifact`, `fhir`, `parquet` and `delimited` give for some items, so they
raise their splitter version to 2 (§14.2); rules 1 and 3 are the split stage's own, and change its build. The split
stage's version changes with them. An item part way under the old version at the deploy is held, as for any change
(§14.3), and released by `release --resume|--close`; the drain check (§14.4) avoids that once built. Items already
completed with no records stay completed: the rules aren't applied backwards. An operator finds them from the
audits (`stage.item_completed stage=split count=0`) and can reprocess them once reprocess is built (§15.8).

### 16.6 Tests, when built

- Per splitter (`*SplitterTest`): 0 bytes and blank give no records and no error; CSV skips blank lines but keeps
  a tab-delimited line of tabs; each empty container of rule 4 is reported; a single document is one record (kept).
- `IngestPipelineTest`: an empty item and a blank one fail `SPLIT_EMPTY`; a header-only CSV fails
  `SPLIT_NO_RECORDS`; plain text into `ccda` is one record of the whole item, and so is XML without the boundary;
  an item resumed after its last record completes; in a batch, an empty message fails alone and a message with no
  boundary is one record; `emptyFileGoesStraightToArchiving` becomes `emptyFileFails`.
- `ItemProcessorTest`: decline of an empty item completes it with 0 records; a whole-item correction that is
  empty fails.
- The sandbox: push an empty file, a blank one and a file of plain text to the C-CDA intake; see the first two
  fail `SPLIT_EMPTY` and the third pass the split as one record and fail at the convert stage
  (`CONVERT_XML_MALFORMED`); decline one and correct another.

### 16.7 As built (2026-11-09)

- **`WholeItemSession`** (connector-core, `split`): opened by `SplitterRegistry.open` around every splitter
  session, so items, whole-item corrections and each message of a batch all get the rules. At the item's start it
  fails an empty or blank item (`SPLIT_EMPTY`) before the splitter sees it: only spaces, tabs, CR and LF, after a
  UTF-8 byte-order mark, read only as far as the first other byte. After a session from the start that gave no
  record, it fails an empty container (`SPLIT_NO_RECORDS`), or gives the whole item as record 0, which the limits
  then check like any record (`LIMIT_RECORD_SIZE`). A resumed session is passed through, since a resume point
  always follows a record. `SplitterRegistry.openMessages` fails a batch with no messages (`SPLIT_EMPTY`).
- **`SplitSession.emptyContainer()`**, `false` by default: true in `csv` for a header without rows, `x12` once an
  `ISA` was read, `edifact` once an interchange was opened, `fhir` (non-blank with no resource can only be `[]`),
  `parquet`, and `delimited` in `SEPARATOR` mode.
- **CSV:** a line of only spaces, tabs and CR is skipped, unless it holds the delimiter.
- **Batches:** an empty message is no longer passed over: it fails alone as a 0-byte piece. Its resume point names
  the message (`[MessageSession.MESSAGE_DONE, index]` as context, `-1` never being an offset), since several empty
  messages share an offset, and an offset at a message's start now means the first empty message there.
- **Codes:** `SPLIT_EMPTY`, `SPLIT_NO_RECORDS` (`ErrorCode`). Splitter versions 2 as §16.5.
- **Tests:** `NoRecordsTest` (every format: empty and blank, each empty container, content without a boundary as
  one record with its resume point, the size limit on it, a single document kept); `CsvSplitterTest`
  (`headerOnlyFileIsAnEmptyContainer`, `blankLinesAreSkipped`); `FhirSplitterTest`; `MessageSessionTest`
  (`failsEmptyAndBlankMessagesAlone`, resuming after every record, `failsABatchWithoutMessages`);
  `IngestPipelineTest.emptyFileFails`; `ItemProcessorTest.failsAnEmptyItemForAnOperatorToDecide` (failed whole,
  an empty correction failing, declined and complete with 0 records, naming its failure).
- **Tried on the sandbox** 2026-11-09 (SANDBOX.md "Checked"); the `-Pit` suite passed 2026-11-10, once the
  integration tests were brought up to the fail store (§15).

## 17. A shared record stage (decided 2026-11-10, with the user)

Before the transformer is built (§11, step 6), what the convert stage does around its conversion moves into a
library, so the convert and transform stages share one copy of it, and a stage is chosen at run time: its queues,
the table it reads records from and the table it stores them in. Asked for by the user 2026-11-10; the four choices
below were made with the user the same day.

### 17.1 Decided (2026-11-10, with the user)

- **Common concerns go in a library,** used at least by the convert and transform stages: receiving queue
  messages, reading records from an Iceberg table, storing what a stage makes in the next layer's table, and
  announcing it downstream by queue.
- **Chosen at run time:** the input, output and dead-letter queues (any of the seven brokers, as now), the source
  table and the sink table.
- **One of each queue (decided):** one input, one output, one dead-letter queue per stage. Fan-in and fan-out are
  done by running more deployments, not by one stage listening or sending on several.
- **Every source and sink is a records table (decided):** gold has the schema of `split_records` and `records`
  (`RecordsTable.SCHEMA`): a row per record, the content encrypted under the table's KEK, with its metadata. The
  transform stage stores one FHIR bundle per record. Tables per resource type, if wanted, are made from gold
  downstream, outside the pipeline.
- **The producer's result from any stage, by setting (decided):** results become the library's, turned on per
  stage at run time. A pipeline turns them on at its last stage (the transform stage, once built), and the result
  covers failures at every stage before it too.
- **A new module, `connector-record-stage` (decided),** published, on `connector-stage`. `connector-stage` stays
  the per-message worker the split stage runs on too; the new module is what a stage that reads records from one
  table and stores records in another adds to it.

### 17.2 What a record stage is

A stage that, for each record message it receives, reads the record from its source table, makes one output from
it, stores the output (or the record as it came, with the error that stopped it) in its sink table, and announces
it on its output queue; and, once an item's records are all handled, sends the item's progress or completion.
Split is not one: it reads a landed item as a stream and makes many records from it.

```
 input queue ──record message──► [ record stage ] ──record message──► output queue
                                   │  read       ▲ store
                                   ▼             │
                             source table     sink table        (both RecordsTable.SCHEMA, any catalog)
```

The only stage-specific code is a **`RecordTransform`**, the step from one record's bytes to its output:

| | Convert stage | Transform stage |
|---|---|---|
| Stage name (its place in a line, `Lineage`) | `convert` | `transform` |
| Takes | `record.split`, `item.split` | `record.stored`, `item.converted` |
| Makes | `record.stored`, `item.converted` | `record.transformed`, `item.transformed` |
| `RecordTransform` | the converter registry by format, with `sdh.convert.options` | the Whistle runtime, fail-fast, with the WSTL set of its settings |
| Its version (§14.2) | the converter's mapping (or `none`) | the WSTL set's name and digest |
| Output | JSON, `Representation.json(mapping)` | a FHIR bundle, `application/fhir+json`, mapping the WSTL set |
| Its own rules | `CONVERT_TOO_LARGE`, `CONVERT_SKIPPED`, a splitter's error passed on, a format with no converter | the first error's issue (§17.6), from `Checks.failure` |

A `RecordTransform` is given the record's bytes and its metadata (format, content type, mapping, origin, item,
place) and the attempt number, and returns the output's bytes and representation, or an error code with where
in the record it was found. It never sees a table, a key or a queue. It says how much memory a record of a size
takes, so the transforms running at once share a budget (`ConversionBudget`, moved to the library), and gives the
versions it can stamp, for the drain check (§14.4a).

### 17.3 What moves into `connector-record-stage`

Everything in `convert-stage` today but the conversion itself, made general where it names `split_records` or a
message type:

| Now (`convert-stage`) | Becomes | Made general by |
|---|---|---|
| `ConvertProcessor` (all but converting) | `RecordStageProcessor`, the `StageProcessor` of every record stage | the source, the sink and the stage's messages (§17.4), given at start |
| `ConvertProcessor.prepare`/`convert` | the convert stage's `RecordTransform` | stays in `convert-stage` |
| `BacklogWatch` | `BacklogWatch` | the source table, whatever it is |
| `ConvertReconciler` | `RecordReconciler` | the source table and the stage's input messages, rebuilt from a row |
| `ConvertInputs` (reprocess, §15.8.4) | `SourceInputs` | the source table |
| `ResultReporter` | `ResultReporter` | the tables to gather failures from (§17.5) |
| `ConvertRuntime`, `ConvertConfiguration` | `RecordStageRuntime`, an auto-configuration | settings (§17.7); a stage's application gives only its `RecordTransform` |
| `ConvertAdminContext`, `ConvertAdmin` | `RecordStageAdmin` | the source and sink from settings |
| audit events `convert.reconcile_*`, `convert.item_total_missing`, `convert.placement_unknown`, `convert.result_not_sent` | `stage.reconcile_*` and the rest, with `sdh.stage` | nothing released, so renamed without a shim, as in §2 |

`convert-stage` keeps its application class, its `RecordTransform` and its settings `sdh.convert.*`. The
transformer's repository has the same three for the transform stage, and nothing else of its own about queues,
tables, claims, failures, reprocessing, the drain check, results or reconciling.

Two spots in `StageWorker` name the convert stage's messages and become general: a reprocess's `cause` is set
on any record message (`RecordInStorage.withCause`), not only `record.stored`; the check for an interrupted
generation stays on `item.split`, which is right, since only the split stage fans out (§15.8.1).

### 17.4 Messages

Each stage keeps its own message types (`record.stored`, `record.transformed`), so a reader still tells at a
glance what layer a record is in. What the library needs from them is in the contract as interfaces:
- **`RecordInStorage`** (exists) gains `record.transformed`, and `withCause`.
- **An item's end, `ItemEnd`,** for `item.split`, `item.converted` and `item.transformed`: the item, its intake,
  counts, versions, completion and generation, `sameAs`, and the producer's facts (below).
- **`StageMessages`,** per stage name: how to make its record message from a stored row and its item's end from
  an item's progress, and which messages it takes as input (its upstream stage's). The library is given the one
  for its stage; `convert` and `transform` are in the contract, so a reader decodes both.
- **The producer's facts travel with the item's end.** A result names the producer's reference for the item, the
  item it replaces and a whole-item failure; today the convert stage reads them from `item.split`. So that the
  last stage can send results without reading landing, `item.converted` (and `item.transformed`) carry them on
  from the end before it. A record's queue message is carried on its failed row (`message_index`,
  `message_refs_enc`), as now, by every stage.
- **`record.transformed` and `item.transformed`** are added (contract still 3.0: nothing released). Their mapping
  is the WSTL set and its digest (§1).
- **`record.failed` and `record.untransformed` are dropped.** No stage sends a failed record on since the fail
  store (§15.1): the convert stage still builds `record.failed` and the worker discards it.

### 17.5 Results from any stage

`sdh.results.enabled` (as now) turns results on at a stage. The result is sent after the stage's item end, as
now, from what is in the stage's sink and in the tables named by `sdh.results.upstream` (the source by default,
and any table before it: for the transform stage, `split_records` and `records`): the failed rows of every one,
less those a correction resolved. A failure is put down to the stage whose table it is in, not to its code's
prefix. Results on at two stages of a pipeline would tell the producer twice; the stage's start warns if its
upstream item end says results were already sent (`results_sent`, carried on the item's end).

### 17.6 The transform stage, as this design leaves it

Its `RecordTransform` runs the Whistle runtime in fail-fast mode with the WSTL set at `sdh.transform.wstl`, loaded
at start; the set's digest is part of its version, so a record mapped again under a changed set is processed
under a new version (§14.3), never mistaken for a duplicate. A record the mapping rejects is stored in gold as it
came (the silver JSON) with the first error's code and place, in the fail store, open for an operator (§15.1). The
report mode (re-mapping a failed record to list every issue) is an operator command of that stage. This section
decides only what the library needs from it; the stage itself is designed with step 6.

### 17.7 Settings, at run time

As now: `sdh.stage.input`, `sdh.stage.output`, `sdh.stage.dlq`, `sdh.stage.concurrency`, `sdh.lease`,
`sdh.results`, `sdh.reconcile`, `sdh.maintenance`. New, replacing `sdh.storage` in a record stage:

```yaml
sdh:
  stage:
    source:                    # where records are read: the upstream stage's sink
      catalog: { ... }         # Iceberg catalog properties, as sdh.storage.catalog
      catalog-name: sdh
      namespace: sdh_ingest
      table: split_records     # the convert stage's default; records for the transform stage
    sink:                      # where this stage stores what it makes
      catalog: { ... }         # may be another catalog (another bucket, another layer)
      catalog-name: sdh
      namespace: sdh_ingest
      table: records
      kek: records-kek         # the sink's KEK; the source's comes from its rows
```

- **A message naming another table is refused.** A record message's storage reference must name the source's
  catalog, namespace and table; any other is audited `stage.message_rejected` (reason `WRONG_SOURCE`) and dropped,
  as an untrusted message is. So a stage only ever reads where it was told to, whatever a message says.
- **The sink is created if missing,** shared by every pod, as `records` is now (§9); table maintenance and
  retention of the sink are the stage's duties. The source is read only: its maintenance is its own stage's.
- **Keys:** the stage unwraps the source's KEK to read, and holds the sink's to write, by `KeyMaterial.forStage`.
- **The convert stage's old names go in the same change (decided 2026-11-10, §17.9):** `sdh.storage.*` and
  `sdh.storage.split-namespace` are replaced by `sdh.stage.sink` and `sdh.stage.source`, with no aliases. The kit's
  `application.yml.example`, the sandbox's `convert-application.yml`, the integration tests and the demo move with
  it. The split stage and landing keep `sdh.storage`: they are not record stages.

### 17.8 Order of building

Each step builds, passes its tests and, where it says, is tried on the sandbox:
1. **The contract:** `ItemEnd`, `StageMessages`, `withCause` on records, the producer's facts on `item.converted`,
   `record.transformed` and `item.transformed`; `RecordLocation` and `readRecord` on it; `record.failed` dropped.
2. **`connector-record-stage`:** `RecordTransform`, `RecordStageProcessor`, source and sink with the source check,
   `BacklogWatch`, `RecordReconciler`, `SourceInputs`, `ResultReporter`, the runtime and admin; tested with a test
   transform against an in-memory catalog and embedded Postgres, as `connector-stage` is.
3. **`convert-stage` on it:** only its application, `RecordTransform` and `sdh.convert.*` left. Its tests, the
   `-Pit` suite and the sandbox run unchanged in what they check (the audit names of §17.3 aside): the proof
   that nothing was lost in the move.
4. **The transformer** (§11, step 6), on the published module.

### 17.8a Step 1 as built (2026-11-10)

- **`RecordLocation`** (record ID, item ID, seq, plaintext length, chunk count, storage ref), given by every record
  message's `location()`; `QueueReader.openRecord` and `readRecord` take one, and their record-message forms pass
  `location()`. The convert stage's `failures export` reads a failed row through `ConvertProcessor.location`.
- **`record.failed` removed** (`RecordFailed`, `MessageType.RECORD_FAILED`); the convert stage makes only
  `record.stored`. `RecordInStorage` permits `RecordSplit`, `RecordStored` and `RecordTransformed`, and gains
  `resolves()`, `cause()` and `withCause()`; `StageWorker` sets a reprocess's cause on any of them.
- **`ItemEnd`,** sealed, for `item.split`, `item.converted` and `item.transformed`: intake, counts, completedAt,
  versions, completion, generation, sameAs, producerRef, replaces. `item.converted` gains `producerRef` and
  `replaces` (its earlier constructors leave them null), and the convert stage fills them from `item.split`.
- **`record.transformed`** (`tr-<itemId>-<recordId>`, the fields of `record.stored`) and **`item.transformed`**
  (`ic-transform-<itemId>`, the fields of `item.converted`).
- **`StageMessages`** (`CONVERT`, `TRANSFORM`): the stage's name and the stage before's, the types it takes and
  sends, `takes(message)` (the stage before's record, item end and `item.progress`), and factories for its record
  message and item end.
- **Tests:** `StageMessagesTest`; `ContractRoundTripTest` with the new messages and `item.converted`'s references.

### 17.8b Step 2 as built (2026-11-10)

`connector-record-stage` (package `com.sdh.healthcare.connector.record`), on `connector-stage` and
`connector-queue-reader`. The convert stage doesn't use it yet (step 3).
- **`RecordTransform`,** what a stage gives: its `StageMessages`, the version it stamps on a record and every
  version it may stamp (for the drain check, §14.4a), the largest record it takes, and per record a **plan**:
  `AsIs` (store it as it came, with an error or none, such as a piece the splitter couldn't parse) or `Apply`
  (with the memory it needs), then `apply`, which gives `Made` (bytes and representation) or `Failed` (a code
  and the byte it failed at). A bug, any exception from `apply`, fails that record alone with the transform's
  `unexpected()` code and logs `stage.unexpected` (no content).
- **`RecordStageProcessor`,** the `StageProcessor` for any record stage: takes the stage before's record
  messages, its item end and its `item.progress` (§17.4); refuses a record message that names any table but the
  source (`stage.message_rejected`, reason `WRONG_SOURCE`, the source check); stores each record in the sink and
  sends the stage's record message, or keeps a failure on its row and in the fail store; corrects as the convert
  stage did. An item end waits only for the records the stage before sent on (`ItemEnd.passedCount()`:
  `item.split` counts only those, `item.converted` every record it handled, failed ones included).
- **`RecordReconciler`, `BacklogWatch`, `SourceInputs`, `ResultReporter`,** ported from the convert stage onto
  any source. A rebuilt record is named by the version on its row (the convert stage gave none). Audit events
  are named for the library, with the stage as an attribute: `stage.reconcile_catching_up`,
  `stage.reconcile_caught_up`, `stage.reconcile_deferred`, `stage.reconcile_deferral_capped`,
  `stage.item_total_missing`, `stage.records_recovered`, `stage.item_completion_recovered`,
  `stage.backlog_not_noted`, `stage.placement_unknown`.
- **Results (§17.5)** gather an item's failures from `sdh.results.upstream` (a list of stage, namespace and
  table, earliest first, in the source's catalog), then the source and the sink, each put down to the stage
  whose table it is in; a record failed with the same code in a later table keeps the earlier stage. The result
  counts every record of the item: those handled here and those that failed before and never came.
- **Runtime and admin:** `RecordStageRuntime.create(settings, transform, camel, fatalExit)`, an auto-configuration
  that starts one whenever the application has a `RecordTransform` bean (`sdh.stage.autostart`, default true,
  replaces `sdh.convert.autostart`), and `RecordStageAdmin.cli(...)` with `status`, `reprocess`, `drain-check`,
  `version` and the fail store's commands. Settings are `RecordStageSettings`, which a stage's own properties
  implement.
- **Source and sink (§17.7):** `sdh.stage.source` and `sdh.stage.sink`, each `catalog` (Iceberg properties),
  `catalog-name`, `namespace`, `table` and, for the sink, `kek`. The convert stage defaults to `split_records`
  into `records`, the transform stage to `records` into a sink it must name; both to `sdh` and `sdh_ingest`.
  The source's catalog is required; the sink uses it unless it has its own. The sink's KEK defaults to
  `sdh.crypto.keys.records-kek`. Maintenance runs on the sink.

Contract gaps found and filled (decided with the user, still 3.0):
- **`record.stored` and `record.transformed` carry the queue message on** (`message`, `messageRef`,
  `messageReplaces`, as `record.split` does), so a record that fails at a later stage keeps the producer's
  message on its row, and its problem names it.
- **Item ends carry the item's `failure` on,** so a later stage's result reports a whole-item failure.
- **`item.converted` and `item.transformed` carry `resultsSent`,** true when that stage sent the producer's
  result or one before it did. A stage that sends results after one before already did audits
  `stage.results_twice`: results are meant for one stage, the last. The convert stage sets it from step 3.

Tests: `RecordStageProcessorTest` (in memory, as the convert and the transform stage), `RecordStageTest` (the
worker on embedded Postgres as the transform stage: results across three tables, an incomplete result,
`stage.results_twice`, the reconciler), `ResultReporterTest` (ported) and `StageTablesTest`, all with a test
transform, `UpperTransform`.

### 17.8c Step 3 as built (2026-11-10)

`convert-stage` is now its application, `ConvertTransform`, `ConvertConfiguration` (its settings and the
transform as a bean) and `ConvertAdmin` (the library's CLI with its transform); it depends on
`connector-record-stage` alone. `ConvertProcessor`, `ConvertReconciler`, `ConvertInputs`, `BacklogWatch`,
`ResultReporter`, `ConvertRuntime` and `ConvertAdminContext` are gone.
- **`ConvertTransform`** holds what `ConvertProcessor.prepare` and `convert` did: a splitter's error, a format with
  no converter, the third claim (`CONVERT_SKIPPED`, still audited `convert.record_skipped`) and a record over
  `sdh.convert.max-bytes` are stored as split; otherwise the record's converter runs, with the memory its
  `memoryFactor` asks for. Its versions are those `ConvertProcessor` stamped, digest included, so a pod of this
  build and one of the build before stamp the same versions and pass each other's drain check (§14.4a).
- **`ConvertProperties`** implements `RecordStageSettings`; `sdh.convert` keeps `options`, `max-bytes` and
  `memory`. `sdh.storage` and `sdh.convert.autostart` are gone, with no aliases (§17.9): an old configuration
  stops at start (`sdh.stage.source.catalog is required`).
- **Moved with it:** the kit's `application.yml.example`, the sandbox's `convert-application.yml`, `ItSupport`
  (the ITs) and the demo name `sdh.stage.source` and `sdh.stage.sink`; the kit README, DEPLOYMENT and `status`'s
  reconciler hint name the `stage.*` events.
- **Tests:** `ConvertProcessorTest` and `ConvertStageTest` check what they did, through the library
  (`ConvertStages` builds it), with the events' new names. Two checks of package-private parts moved to the
  library's `RecordStageTest` (the backlog watch, and the days a catch-up covers); `ResultReporterTest` moved in
  step 2.

### 17.9 Decided after review (2026-11-10, with the user)

1. **Audit names. Decided 2026-11-10 with the user: renamed `stage.*`,** with `sdh.stage`, as the library's own
   events already are (`stage.item_completed`, `stage.batch_failed`): `stage.reconcile_catching_up`,
   `stage.reconcile_caught_up`, `stage.reconcile_deferred`, `stage.reconcile_deferral_capped`,
   `stage.records_recovered`, `stage.item_completion_recovered`, `stage.item_total_missing`,
   `stage.placement_unknown`, `stage.result_not_sent`, `stage.started`, `stage.not_started`, `stage.stopping`. No
   shim (nothing released); `scripts/sandbox.sh` and SANDBOX.md move in the same change. `convert.record_skipped`
   stays: it is the convert stage's own rule, in its `RecordTransform`.
2. **The producer's facts. Decided 2026-11-10 with the user: carried on each item's end** (§17.4).
   `item.converted` and `item.transformed` gain `producerRef` and `replaces`, copied from the item end the stage
   received, so any stage can send results from what reached it, and none holds landing's KEK or catalog (§9).
   Nothing new is exposed: messages are sealed for the recipient, and `item.progress` carries both already. Two
   fields per item, never per record.
3. **`record.failed`. Decided 2026-11-10 with the user: dropped in step 1,** with `record.untransformed` (never
   built). Its last use is as a carrier: the admin's `failures export` wraps a failed row in a `RecordFailed` to
   read it with `QueueReader.readRecord`. So the contract gains a `RecordLocation` (record ID, storage reference,
   chunk count), which every record message gives; `readRecord` takes one; the export and the demo move to it.
   `RecordInStorage` then permits only what a stage sends: `record.split`, `record.stored`, `record.transformed`.
4. **The convert stage's old `sdh.storage` names. Decided 2026-11-10 with the user: moved in the same change,**
   with no aliases (§17.7). Every place that sets them is in this repository and nothing is released, as for the
   rename of §2; step 3 then proves the stage on the settings the transformer will use.
