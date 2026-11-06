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
`file.started` and `file.completed` stay until step 4 replaces them with the item-complete messages.

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

`file.started` and `file.completed` go: the item-complete messages replace them. The ordering key (Kafka key,
SQS group, Service Bus session, Pub/Sub ordering key, Artemis group) becomes the **record ID** for record
messages, so records spread over partitions, and stays the item ID for item messages.

## 5. Item complete

Records arrive at a stage in any order, and an item's `item.split` may come before or after its records. Each
stage keeps a row per item in Postgres, `stage_item(stage, item_id, expected, done, failed, completed_at)`:
- `expected` is set from the upstream item-complete message (`item.split` for convert, `item.converted` for
  transform);
- `done` (and `failed`) go up by one when a record is first stored (§6), never for a duplicate;
- when `expected` is known and `done` reaches it, a conditional update (`completed_at IS NULL`) lets exactly one
  worker send the stage's item-complete message. Sending it again after a crash is harmless: its message ID is
  fixed (`ic-<stage>-<itemId>`), and the broker or the reader drops the repeat.

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
3. if the record is already stored: does the work again to compare digests. The same digest is a **duplicate**:
   dropped, `record.duplicate` audited, acknowledged. A different digest (the converter or mapping changed between
   the two) is a **conflict**: the first stays, the second goes to the stage's dead-letter queue as
   `record.conflict`, audited, and acknowledged. Nothing is replaced (decided);
4. if another worker holds a live claim: the rest of the batch is stored and sent first, then the record is
   looked at again until the other has stored it (a duplicate) or let its claim expire (taken over). It is never
   acknowledged before: that would lose the record if the other worker died. Storing the rest first means two
   workers each waiting on the other's records can't hold each other up.

A record ID is a keyed hash of the record's content (§7.3), so the same ID means the same input.

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
  acknowledged, and one of a contract major version the stage can't read holds the batch back until the stage is
  upgraded (`stage.unreadable_version`), as for the splitter now. Other audit events: `record.duplicate`,
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
  is left to it.
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
  worker as if received. An item whose `item.split` never arrived can't be completed this way (its producer's
  references are in landing); it is audited (`convert.item_total_missing`), for an operator (§12).
- **Settings:** `sdh.stage.*` (§7); `sdh.convert.options`, `.max-bytes`, `.memory`; `sdh.storage` for `records`
  and the catalog `split_records` is read from; `sdh.results` as before; `sdh.lease` for the duties lease.

**As built (2026-11-06):**
- `ConvertProcessor` is the stage's `StageProcessor`; `ConvertRuntime` wires it with keys from
  `KeyMaterial.forStage` (the records KEK, signing, recipient and inbox keys; no landing KEK) and a `QueueReader`
  built without message keys, which only reads records (the worker opens the messages). `sdh.storage.split-namespace`
  names the namespace of `split_records` if it differs; `sdh.reconcile.grace`, `.lookback` and `.interval` tune
  the reconciler; `sdh.convert.autostart=false` leaves the input and duties stopped for tests.
- A record stored as split (too large, skipped, a splitter's error, or a format with no converter) isn't read until
  it is committed: its digest is of its split record ID. A splitter's error code this version doesn't know is
  stored as `PARSE_UNRECOGNISED`.
- The registry keeps the received `item.split` in `total_message` on the item's row; the worker opens it again for
  `itemComplete` and the new `itemCompleted` step, which runs after the item-complete message is sent and before
  the item is marked complete. If it no longer opens (its key retired), `stage.item_total_unreadable` is audited,
  `item.converted` names the intake `unknown`, and no result is sent (`convert.result_not_sent`).
- A failed record's row keeps `message_index` and `message_refs_enc` (`RecordBatchWriter.place`), read back by
  `CommittedRecords.failed(table, item, keys)`.
- **Two copies of a record.** A worker that loses its claim between checking it and committing can append a record
  a successor appends too (§6). The reader now reads a record's chunks from the batch of the first row it meets and
  skips rows of the same record from another batch; `CommittedRecords` reads each record once.
- The duties are the split stage's, moved to `connector-stage` as `StageDuties` (lease `<stage>/<id>/duties`),
  with `StageFailures` deciding what is fatal. The reconciler hands rebuilt messages to
  `StageWorker.handleOpened`, using the registry's `knownRecords` and `completed`. A rebuilt `record.split` has no
  queue message, so a failed record found this way is reported without one.
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
`minReplicaCount` 0 is allowed, at the cost of a JVM start (about 15 s) on the first message.

The sandbox gets the KEDA operator, the stages as scaled deployments, and topics with partitions.

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
5. **KEDA** in the sandbox, with a scaling demonstration (a large batch spread across workers).
6. **The transformer** in `healthcare-data-transformer`, on the published library.

## 12. Open for review

- **A lost `item.split`** (§9): the convert stage can rebuild lost `record.split` messages from `split_records`,
  but not an `item.split`, whose producer's references come from landing. Options: the split stage's duties send
  each item's `item.split` again once, a grace after it finished; or an operator command does it for the items the
  convert stage audits.
- **A redelivered record stored as `CONVERT_SKIPPED`** (§9): the worker processes it again to compare digests
  (§6), so it is converted again, which may stop the worker as before. Options: the worker compares by attempt
  for such records, or a processor may give the digest of a stored record without redoing the work.
- **A reconciled record has no queue message** (§9): its producer's result names no message for it if it fails.
  The split stage could keep `message_index` and `message_refs_enc` on every `split_records` row of a batch.
- Split records as bronze, in their own namespace and table (§3).
- Postgres for the registry and item rows (§5–§6): the lease database the stages already use. A row per record
  per stage, kept as long as the records are (it is how duplicates are recognised); about 100 bytes each.
- `item.result` to producers sent by the convert stage, after conversion (§4).
- Moving publishing into `connector-broker` with receiving (§7). Done in step 2.
