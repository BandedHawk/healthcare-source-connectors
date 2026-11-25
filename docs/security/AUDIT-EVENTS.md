# Audit event catalogue

Every audit event the pipeline emits and every field it can carry, for the security analyst who sets up the SIEM
and runs the activity reviews (§ 164.312(b), § 164.308(a)(1)(ii)(D)). It is HIPAA.md §11.2's document D7. What to
review and how often is HIPAA-OPERATIONS.md §3; this document says what each event means when it is found.

`AuditEventsCatalogueTest` checks that every event the source code emits is listed here (§8), so a new event
can't be left out.

## 1. How an event is recorded

Every audit event goes through one call, `Telemetry.audit`, `auditWarn` or `auditError` (`connector-core`), which
writes it three ways at once:

| Copy | Where | What it holds |
|---|---|---|
| **The journal** | the stage's Postgres schema: `sdh_audit_event`, chained per process in `sdh_audit_chain`, sealed in `sdh_audit_seal` (DESIGN.md §12.3) | `chain_id`, `seq`, `at`, `name`, `severity`, `attributes` (JSON), `hash`. Each chain row names its `source` and `pod` |
| **The OTLP export** | the operator's collector at `sdh.telemetry.otlp.endpoint`, as OpenTelemetry log records (only audit events: `AuditOnlyLogRecordExporter`) | event name `sdh.audit.<event>`, severity, body `<event>`, and the fields as attributes |
| **The console log** | the pod's standard output, logger `sdh.audit` | `<event> key=value …`, the keys without their `sdh.` prefix |

`audit-export` (HIPAA-OPERATIONS.md §3.3) prints the journal as JSON lines, one event each:

```
{"chain":"<uuid>","seq":42,"at":"2026-11-25T09:14:03.120Z","event":"sdh.audit.item.landed",
 "severity":"INFO","attributes":{"sdh.bytes":"18342","sdh.intake":"claims-sftp","sdh.item_id":"<uuid>",
 "sdh.kind":"file"},"hash":"<hex>"}
```

**Names.** This catalogue names events without their prefix, as the code and the console log do: `item.landed`
here is `sdh.audit.item.landed` in the journal and the export. A SIEM rule on `admin.*` matches
`sdh.audit.admin.*`.

**Severity** is `INFO`, `WARN` or `ERROR`. `INFO` is a normal step of the record (an item landed, an operator's
command that succeeded). `WARN` is something unusual that the product handled: a refusal, a retry, a recovery,
data held back. `ERROR` is something that needs a person: data that couldn't be taken in, a stage that stopped, a
setting refused. An event keeps one severity whatever stage emits it, except operators' commands (§6), whose severity
follows their result: `INFO` when it succeeded, `WARN` when it failed or was refused.

**What fields can hold.** Only the keys of §2 (the closed enum `Attr`; `Fields` accepts no other, and the
exporters drop any other key). Values are IDs, codes, counts, sizes, times, positions, digests, setting names and
principals. No field holds a document's content, a file name or object key, an exception's message, or a
setting's value (DESIGN.md §7.5). The two free-text fields an operator types, `sdh.action_reason` and
`sdh.ticket`, are limited to 200 characters from letters, digits, spaces and `. , : _ # / -`; operators must not
put PHI in them (HIPAA-OPERATIONS.md §1, "Reasons and tickets"; [DATA-FLOWS.md](DATA-FLOWS.md) §3).

**Before the journal opens.** Events from a process's first moments (the compliance check, `connector.started`)
are held in memory, up to 1,000, and written to the journal once it opens. If more come first, the oldest are
dropped from the journal only; the export and console log still have them (§9).

## 2. Fields

All the keys an audit event can carry. A key appears only on the events that set it (§4 to §6).

| Key | What it is | Values |
|---|---|---|
| `sdh.action` | what an operator asked of an item | `RESUME`, `CLOSE` for a held split item (STAGES.md §15.6e) |
| `sdh.action_reason` | why a control API action was asked for, as its caller typed it | checked text, ≤ 200 characters (§1) |
| `sdh.approved_by` | the second principal who approved a four-eyes operation | a principal |
| `sdh.auth` | how the operator's name was proved | `jwt`, `mtls` (control API), `cli` |
| `sdh.attempts` | how many times workers have claimed a record | a number |
| `sdh.broker_message_id` | where a received message is on the broker | such as `topic/partition/offset`; never its content |
| `sdh.bytes` | a size | bytes |
| `sdh.cause` | why an object was made again | `reprocess`, `correction` |
| `sdh.check` | a compliance check | a check ID ([CONFIGURATION.md](CONFIGURATION.md) §1) |
| `sdh.claims` | records or items a stage's pods hold claims on | a number |
| `sdh.control_scope` | what a pause, key or removal applies to | a stage's deployment, `<stage>/<connector ID>` |
| `sdh.copy_id` | an item that is an exact copy of an earlier one | a UUID |
| `sdh.correction` | an operator's correction's number for its failure | a number |
| `sdh.count` | how many of something | a number |
| `sdh.declined_count` | an item's failures declined by an operator | a number |
| `sdh.decision` | whether an operator's call was let through | `allowed`, `approved`, `denied`, `unauthenticated`, `needs-approval`, or a refusal code (§6.3) |
| `sdh.epoch` | the lease epoch, which rises each time an instance takes the lease | a number |
| `sdh.error_code` | what went wrong | an `ErrorCode` name (DESIGN.md §6.4) |
| `sdh.failed_count` | an item's records that failed, stored as they were | a number |
| `sdh.files` | data files rewritten or removed | a number |
| `sdh.format` | the item's format | `ccda`, `x12`, `csv`, … |
| `sdh.from_state` | the state a failure was changed from | a failure state (STAGES.md §15.3) |
| `sdh.generation` | an item's generation | from 1 (STAGES.md §15.8) |
| `sdh.idempotent_replay` | the action was answered from its first answer, a retry with the same idempotency key | `true` |
| `sdh.intake` | a landing intake, by its configured name | such as `claims-sftp`; never a path or host |
| `sdh.item_id` | an item | a UUIDv7 |
| `sdh.kek`, `sdh.kek_from`, `sdh.kek_to` | a KEK, by reference; the KEKs of a re-key | a reference, never key material |
| `sdh.key_principal` | the principal a sealing key is registered for | a principal |
| `sdh.kind` | what kind of item was landed | `file`, `message`, … |
| `sdh.let_in_by` | how a stage's release was let in | such as `drained` |
| `sdh.lineage` | where an object stands in its line of processing | such as `g2 r1.3` (STAGES.md §15.8.2) |
| `sdh.message_type` | a message's type | such as `item.result` |
| `sdh.objects` | stored objects removed | a number |
| `sdh.offset` | a byte offset in the item | a number |
| `sdh.open_count` | an item's failures still open | a number |
| `sdh.operation_id` | a long operation's ID in the control API's record | a UUID |
| `sdh.operator` | who ran an operator command, or submitted a correction | an account name or principal |
| `sdh.outcome` | what came of an operation or an object reprocessed | per event: `stored`, `failed`, `plaintext`, `sealed`, `intact`, `list`, `find`, `awaiting-approval` |
| `sdh.parts` | sealed parts a control API answer sent | a number |
| `sdh.paused` | whether a stage is paused | `true`, `false` |
| `sdh.permission` | the permission a control API call needed | such as `pipeline.view`, `phi.break-glass` |
| `sdh.position` | a byte position within a record | a number |
| `sdh.profile` | the compliance profile a process started under | `hipaa`, `none` |
| `sdh.purpose` | why a reader reads | an HL7 purpose of use, such as `TREAT` |
| `sdh.reader` | who read a stored record outside the stages | the reader's configured name |
| `sdh.reason` | why something happened | a code, a state name, or an exception's class name; never its message |
| `sdh.record_id` | a record | `base32(HMAC-SHA-256)` over its content (DESIGN.md §7.3) |
| `sdh.recipient_key` | the public key an export or lookup was sealed to | SHA-256, hex |
| `sdh.refused_count` | a reprocess run's objects refused | a number |
| `sdh.release` | a stage's release | a digest of its versions (STAGES.md §14.4a) |
| `sdh.request_id` | a control API request, also named in its answer | a UUID |
| `sdh.requested_by` | the principal who asked for a four-eyes operation | a principal |
| `sdh.result` | the outcome of an operation | `ok`, `failed`, `refused`, `replayed`, `not-found`, or a stage's item result |
| `sdh.retention` | how long a stage keeps its rows | an ISO-8601 duration, or `forever` |
| `sdh.rows` | rows changed | a number |
| `sdh.run_id` | a reprocess run | a UUID |
| `sdh.same_output` | whether a reprocess's output matched what it superseded, by digest | `true`, `false` |
| `sdh.sealing_key` | a principal's sealing key, by fingerprint | SHA-256, hex |
| `sdh.selector` | what a reprocess run chose by | `item`, `record`, `made_by` |
| `sdh.seq` | a record's number in its item | from 0 |
| `sdh.setting` | a setting or JVM option, by name | such as `sdh.crypto.wrap-provider`; never its value |
| `sdh.sighting` | which sighting of its source an item was, as the ledger keys it | hex: a hash of the reference hash, size and version. Two items from one source with different ones came from different versions |
| `sdh.snapshot_id` | an Iceberg snapshot of a table an intake reads | a number |
| `sdh.source_ref` | where an item came from, as its keyed hash, which the landing tables hold too | hex; two items with the same one came from the same path, key or message ID. Never the reference itself |
| `sdh.stage` | the stage | `intake`, `split`, `archive`, `convert`, `transform` |
| `sdh.state` | a checkpoint's, failure's, request's or run's state | a state name |
| `sdh.stored_version` | the version a record's stored output was made with | a version digest |
| `sdh.superseded_by` | the lineage or generation that superseded an object | such as `g2` |
| `sdh.table` | an Iceberg table | its name |
| `sdh.tables` | tables dropped | a number |
| `sdh.ticket` | the ticket an operator names for a step that needs one | checked text (§1) |
| `sdh.version` | the version that decides a stage's output beyond the record | build, mapping and a settings digest (STAGES.md §6) |
| `sdh.version_count` | how many versions something names | a number |
| `sdh.waiver_reason` | why a compliance check was waived, as configured | text about the deployment |

`Attr` has keys that only metrics, traces and logs use, never audit events: `sdh.archive`,
`sdh.causes`, `sdh.chain_id`, `sdh.phase`, `sdh.sql_state`, `sdh.thrown_at`, `sdh.thrown_from`.

## 3. Reading this catalogue

§4 lists the pipeline's own events, by area; §5 the landing node's; §6 operators' commands. Each row gives the
event, its severity (I, W, E), the modules that emit it, its fields, and what it means. **Review** marks an event
HIPAA-OPERATIONS.md §3.1 asks the analyst to look at each day, or one that should raise an alert.

## 4. Pipeline events

### 4.1 Starting, stopping and compliance

| Event | Sev | Emitted by | Fields | Meaning |
|---|---|---|---|---|
| `connector.started` | I | landing, split, convert, transform | `stage`, `intake`, `format`, `epoch` | a process has started and taken its place |
| `connector.fatal` | E | all (`FatalExit`) | `reason` | a process stopped itself because it couldn't go on safely, such as a lost lease or a store that can't be written. **Review:** possible attack or fault |
| `compliance.checked` | I | all | `profile`, `count` | the compliance profile passed at startup; `count` checks were run |
| `compliance.waived` | W | all | `profile`, `check`, `setting`, `waiver_reason` | a check was waived, at every start, for each waiver. **Review:** each must match the security official's record |
| `compliance.off` | W | all | `profile` | the process started with no compliance profile (`none`). **Review:** never for real PHI |
| `compliance.refused` | E | all | `profile`, `check`, `setting` | a check failed and the process refused to start; also when a table's bucket has no versioning (`StoreVersioningGuard`). **Review** |

### 4.2 Leases

The landing node and the split stage's duties run under a lease, so only one instance acts at a time (DESIGN.md
§9).

| Event | Sev | Emitted by | Fields | Meaning |
|---|---|---|---|---|
| `lease.acquired` | I | `LeaseKeeper` | `epoch` | this instance took the lease |
| `lease.released` | I | `LeaseKeeper` | `epoch` | this instance gave the lease up, as it stopped |
| `lease.lost` | E | `LeaseKeeper` | `epoch`, `reason` | this instance lost the lease and stops: another took it, or renewing failed. **Review:** expected in a failover; otherwise a fault |
| `lease.fenced_table` | I | `FencedCommitter` | `epoch`, `table` | a table was marked with this instance's epoch, so an older holder's late commits are refused. **Review** when it follows a `lease.lost` elsewhere |

### 4.3 Splitting items

| Event | Sev | Emitted by | Fields | Meaning |
|---|---|---|---|---|
| `item.started` | I | split | `item_id`, `format`, `bytes`, `generation` | the split stage began an item |
| `item.resumed` | I | split, landing | `item_id`, `offset`, `seq`, `count`, `intake` | work on an item went on from its checkpoint after a restart |
| `item.restart_not_counted` | I | split | `item_id`, `count` | an attempt ended because a service it needs was down, so its restart isn't counted toward the limit (DESIGN.md §9.2) |
| `batch.committed` | I | split | `item_id`, `seq`, `offset`, `count` | a batch of an item's records was committed to `split_records` |
| `batch.settled` | I | split | `item_id`, `seq`, `result` | after a restart or failover, the batch an earlier holder left pending was settled: `committed` if it reached the table, `dropped` if it didn't, to be written again |
| `publish.republish` | I | split | `item_id`, `seq`, `count` | records committed before a restart were sent on again |
| `record.failed` | W | split, convert, transform | `stage`, `item_id`, `seq`, `offset`, `position`, `error_code`, `format` | a record couldn't be processed. It is kept as it was, put in the fail store, and not sent on (STAGES.md §15.1) |
| `item.split` | I | split | `item_id`, `count` | an item was split into `count` records |
| `item.failing` | W | split | `stage`, `item_id`, `offset`, `error_code`, `reason` | an item failed as a whole and is being ended as failed |
| `item.failed` | E | split | `stage`, `item_id`, `offset`, `seq`, `error_code` | an item ended failed: its records up to the failure are kept, the rest weren't cut. **Review:** each needs a decision (STAGES.md §15) |
| `item.completed` | I | split | `item_id`, `count`, `bytes` | the split stage finished an item |
| `item.progress` | I | split, convert, transform | `stage`, `item_id`, `generation`, `count`, `open_count`, `declined_count`, `result` | an item's progress was announced |
| `stage.item_completed` | I | split, convert, transform | `stage`, `item_id`, `count`, `failed_count`, `result` | a stage finished an item. The same event at every stage, so one query finds an item lost between two (STAGES.md §13.2) |
| `stage.generation_started` | I | split | `stage`, `item_id`, `generation`, `cause`, `run_id` | a new generation of an item began, from a reprocess or a correction |
| `split.generation_interrupted` | W | split | `item_id`, `generation`, `offset`, `count`, `superseded_by` | a generation was stopped because a newer one began |
| `split.interruption_unknown` | I | split | `item_id`, `reason` | whether a newer generation exists couldn't be read; it is checked again |
| `split.completions_numbered` | I | split | `count` | ended items recorded before completions were numbered were numbered, once, at startup (STAGES.md §15.6f) |
| `split.version_held` | W | split | `item_id`, `seq`, `version`, `stored_version` | an item was held: its stored records were made by another version of the stage. It waits for an operator (`release`) |
| `split.release_requested` | I | split | `item_id`, `action`, `operator` | an operator asked for a held item to be resumed or closed |
| `split.released` | W | split | `item_id`, `seq`, `version`, `stored_version`, `operator` | a held item was resumed under this version |
| `split.release_refused` | W | split | `item_id`, `action`, `state` | a release was refused: the item isn't held |
| `split.item_closed` | W | split | `item_id`, `seq`, `stored_version`, `operator` | a held item was closed by an operator, without its remaining records |
| `split.rejected` | E | split | `intake`, `error_code`, `reason` | a landed-item message was refused, such as one not signed by the landing node. **Review** |
| `split.unreadable_version` | E | split | `intake`, `error_code`, `reason` | a message from a newer version of the landing node, held until this stage is upgraded |
| `split.recovered` | W | split (`SplitReconciler`) | `intake`, `item_id`, `reason` | an item landed but never started at split was found, and started |

### 4.4 Record stages (convert, transform) and the stage runtime

| Event | Sev | Emitted by | Fields | Meaning |
|---|---|---|---|---|
| `stage.message_rejected` | E | split, convert, transform | `stage`, `message_type`, `error_code`, `intake`, `reason` | a message was refused: not signed by the stage it should come from, unreadable, or naming storage this stage doesn't read. **Review:** possible attack or fault |
| `stage.unreadable_version` | E | convert, transform | `stage`, `error_code`, `message_type`, `reason` | a message from a newer version of the stage before; it waits until this stage is upgraded |
| `stage.message_unwanted` | I | convert, transform | `stage`, `message_type`, `reason` | a message this stage doesn't act on, acknowledged |
| `record.duplicate` | I | convert, transform | `stage`, `item_id`, `seq`, `broker_message_id`, `version`, `stored_version`, `attempts`, `state` | a record already processed arrived again; its output is sent again, not made again |
| `record.version_differs` | W | convert, transform | as `record.duplicate` | a record arrived again whose stored output was made by another version of the stage |
| `record.claim_lost` | W | convert, transform | `stage`, `item_id` | a worker's claim expired while it was stalled; the worker that took it over stores and sends the record |
| `stage.batch_failed` | W | convert, transform | `stage`, `error_code`, `message_type`, `reason` | a batch failed and will be tried again |
| `stage.claims_left` | E | convert, transform | `stage`, `error_code`, `count` | a batch was about to be acknowledged with some of its records still claimed by its own pod, so it is tried again. **Review:** report it |
| `stage.placement_unknown` | W | convert | `stage`, `item_id`, `seq`, `error_code` | a failed record reached by reconciling, not by its message, so the producer's result can't name the message it came in (DESIGN.md §5.3.16) |
| `convert.record_skipped` | W | convert | `item_id`, `seq`, `format`, `error_code` | a record failed conversion on every attempt and was stored in its original form with the error |
| `transform.record_skipped` | W | transform (the transformer repository) | `item_id`, `seq`, `format`, `error_code` | a record failed mapping on every attempt, or its format isn't mapped, and was stored as it was with the error |
| `stage.result_not_sent` | W | convert | `stage`, `item_id`, `reason` | the producer's `item.result` couldn't be made for an item |
| `stage.results_twice` | W | convert | `stage`, `item_id` | an item's result was already sent; not sent again |
| `results.sent` | I | landing, convert | `item_id`, `message_type` | a receipt or result was sent to the producer |
| `stage.item_total_missing` | W | convert, transform | `stage`, `item_id`, `count` | an item's end never arrived; the stage before is asked for it again |
| `stage.item_total_unreadable` | W | convert, transform | `stage`, `error_code`, `item_id`, `reason` | an item's end couldn't be opened |
| `stage.item_end_resent` | W | split, convert, transform | `stage`, `item_id` | an item's end was sent again to the next stage, which never had it |
| `stage.total_superseded` | I | convert, transform | `stage`, `item_id`, `generation`, `superseded_by` | an item's end for a superseded generation was ignored |
| `stage.item_reopened` | I | convert, transform | `stage`, `item_id`, `reason` | a completed item was opened again, such as when a declined failure is reopened (`failure_reopened`) |
| `stage.records_recovered` | W | convert, transform (`RecordReconciler`) | `stage`, `item_id`, `count` | records never received from the stage before were found in its table, rebuilt and processed (STAGES.md §9) |
| `stage.claims_recovered` | W | convert, transform (`RecordReconciler`) | `stage`, `item_id`, `count` | records whose claims expired with nothing stored, such as when their pod stopped, were processed again from their stored rows |
| `stage.item_completion_recovered` | W | convert, transform (`RecordReconciler`) | `stage`, `item_id`, `count` | an item whose records were all in, but never completed, was completed |
| `stage.reconcile_catching_up` | W | convert, transform | `stage`, `count` | the reconciler is behind, checking days older than its window |
| `stage.reconcile_caught_up` | I | convert, transform | `stage`, `count` | the reconciler has caught up |
| `stage.reconcile_deferred` | I | convert, transform | `stage`, `reason`, `count` | a reconcile run was put off, such as while a backlog drains |
| `stage.reconcile_deferral_capped` | W | convert, transform | `stage`, `count` | a reconcile run was put off as long as it may be, and runs now |
| `stage.completions_numbered` | I | convert, transform | `stage`, `count` | completions recorded before they were numbered were numbered, once, at startup |
| `stage.totals_numbered` | I | convert, transform | `stage`, `count` | item ends recorded before they were numbered were numbered, once, at startup |
| `stage.registry_pruned` | I | convert, transform | `stage`, `count` | finished entries past their retention were removed from the stage's registry |
| `stage.version_blocked` | W | convert, transform | `stage`, `release`, `count`, `version_count` | a new release is kept out until the stage drains the versions it can't read |
| `stage.version_changed` | I | convert, transform | `stage`, `release`, `let_in_by`, `count`, `version_count` | a new release was let in |
| `stage.version_forced` | W | convert, transform | `operator`, `stage`, `release`, `version_count` | an operator accepted a new release without a drain (`version --accept`); its reason is in the fail store, encrypted |
| `stage.reprocess_queued` | I | convert, transform (`ReprocessStore`) | `stage`, `run_id`, `item_id`, `seq`, `cause`, `lineage` | an object was queued to be made again by a reprocess run (STAGES.md §15.8.4) |
| `stage.reprocess_refused` | I | convert, transform | as `stage.reprocess_queued`, and `reason` | an object a reprocess run chose was refused, such as one already superseded |
| `stage.reprocessed` | I | convert, transform | `stage`, `item_id`, `seq`, `run_id`, `generation`, `cause`, `outcome`, `lineage`, `same_output` | an object was made again: `stored` or `failed`, and whether its output matched what it supersedes |
| `stage.reprocess_done` | I | convert, transform | `stage`, `run_id`, `count`, `failed_count`, `refused_count` | a reprocess run ended |
| `message.unsupported_type` | W | landing (an Artemis intake) | `intake` | a JMS message of a type that isn't read as text or bytes; it is taken as empty |

### 4.5 Failures and corrections

The fail store's events (STAGES.md §15), at every stage. Each names the failure by `stage`, `item_id` and `seq`;
the reasons and corrected content are encrypted in the fail store, never in the event.

| Event | Sev | Fields | Meaning |
|---|---|---|---|
| `failure.failed` | W | `stage`, `item_id`, `seq`, `error_code`, `lineage` | a record's failure was put in the fail store |
| `failure.declined` | I | `stage`, `item_id`, `seq`, `state`, `from_state`, `operator`, `error_code` | an operator declined to correct a failure (`decline`) |
| `failure.reopened` | I | as above | a declined failure was opened again (`reopen`) |
| `failure.retry_submitted` | I | as above | an operator asked for a failure to be tried again as it was (`retry`) |
| `failure.correction_submitted` | I | as above | an operator submitted corrected content for a failure (`correct`) |
| `failure.retried` | I | `stage`, `item_id`, `seq`, `correction`, `operator`, `lineage`, `error_code` | a retry passed |
| `failure.retry_failed` | I | as above | a retry failed again |
| `failure.corrected` | I | as above | a correction passed: the corrected record is stored and sent on |
| `failure.correction_failed` | I | as above | a correction failed; the failure stays open |
| `failure.superseded` | I | `stage`, `item_id`, `seq`, `state`, `lineage`, `superseded_by`, `reason` | a failure was superseded by a correction's revision or a newer generation; kept, linked to what replaced it |
| `failure.resent` | I | `stage`, `item_id`, `seq`, `state`, `copy_id`, `operator`, `error_code` | an exact copy of an item reached the stage; each open failure of the earlier item is flagged, naming the copy (STAGES.md §15.5a). The copy is processed on its own |
| `failure.states_moved` | I | `stage`, `count`, `failed_count` | at startup, failures in states an earlier build used were moved to the current ones, once (STAGES.md §15.8.5) |

### 4.6 Keys, removal and maintenance

| Event | Sev | Emitted by | Fields | Meaning |
|---|---|---|---|---|
| `keys.kek_active` | I | every stage, landing included (`KekWatch`) | `control_scope`, `kek` | the stage now wraps new data with this KEK, after a rotation |
| `keys.kek_refused` | W | every stage | `control_scope`, `kek`, `reason` | a KEK asked for couldn't be used; the stage keeps the one it had. **Review** |
| `keys.rekeyed` | I | every stage (`RekeyPass`) | `control_scope`, `operation_id`, `kek_from`, `kek_to`, `files`, `rows` | stored data was re-wrapped from one KEK to another |
| `keys.rekey_failed` | W | every stage | `control_scope`, `operation_id`, `kek_from`, `kek_to`, `reason` | a re-key failed. **Review** |
| `data.purged` | I | every stage (`PurgePass`) | `control_scope`, `operation_id`, `item_id`, `files`, `rows`, `count`, `ticket` | an item's data was removed from the stage's tables, and its corrections deleted (D9) |
| `data.purge_failed` | W | every stage | `control_scope`, `operation_id`, `item_id`, `reason` | a purge failed. **Review:** the data is still there |
| `item.purged` | I | landing (`GivenUpItems`) | `intake`, `item_id` | the partial rows of an item given up during landing were removed |
| `item.purge_refused` | W | landing | `intake`, `item_id`, `reason` | an item given up wasn't purged, because it had been landed after all (`LANDED`): a landed item is never removed this way |
| `maintenance.retention` | I | every stage (`TableMaintenance`) | `table`, `count`, `reason` | rows older than the retention were deleted: which table, how many files, and the first day kept |
| `maintenance.orphans_deleted` | I | every stage | `table`, `count` | files no snapshot refers to were deleted |

### 4.7 Readers of gold

| Event | Sev | Emitted by | Fields | Meaning |
|---|---|---|---|---|
| `reader.record_read` | I | `connector-queue-reader` (`TelemetryReadAudit`) | `reader`, `purpose`, `table`, `item_id`, `seq`, `record_id` | a gold record was read and decrypted outside the stages (HIPAA.md §4.3). **Review:** a reader not registered, or a purpose not expected |

## 5. Landing events

### 5.1 Intakes

| Event | Sev | Fields | Meaning |
|---|---|---|---|
| `intake.reachable` | I | `intake` | an intake that was unreachable can be reached again |
| `intake.unreachable` | E | `intake`, `error_code`, `reason` | an intake's source can't be reached. **Review** if it lasts: items aren't being taken |
| `intake.not_writable` | E | `intake`, `error_code` | a `read-write` intake can't change its source (delete, move or rename after landing), so it doesn't start |
| `connector.directory_claimed` | E | `error_code`, `reason` | the intake's inbound directory is claimed by another deployment, which `reason` names; this node releases its lease and stops (`INBOUND_DIR_CLAIMED`). **Review:** two deployments configured on one directory |
| `ledger.repaired` | I | `intake` | the landing ledger was repaired from the landed items after a restart |
| `ledger.memory_low` | W | `intake`, `count`, `bytes` | the ledger's index is near the memory set aside for it |
| `ledger.too_large` | E | `intake`, `count`, `bytes`, `error_code` | the ledger's index doesn't fit the memory set aside; the node doesn't start |

### 5.2 Items

| Event | Sev | Fields | Meaning |
|---|---|---|---|
| `item.landed` | I | `intake`, `item_id`, `kind`, `bytes`, `source_ref`, `sighting` | an item was taken in and committed to `landed_items` |
| `batch.landed` | I | `intake`, `item_id`, `count` | a batch of queue messages was landed as one item |
| `batch.skipped` | I | `intake`, `count` | a batch held no messages to land |
| `item.announced` | I | `intake`, `item_id` | the item's `item.landed` message was sent to the split stage |
| `item.finished_at_source` | I | `intake`, `reason`, `source_ref`, `sighting` | a `read-write` intake finished with the item at its source (deleted, moved or renamed), or found it already in the ledger |
| `item.left_at_source` | I | `reason` | an object was left at its source because it had `changed` since it was landed, or was `gone`; a changed one is landed afresh |
| `item.changed` | W | `intake`, `item_id` | an item changed while it was being landed; what was written is discarded and it is landed again |
| `item.abandoned` | W | `intake`, `item_id` | an unfinished item was given up, as its source was gone after a restart; its partial rows are purged |
| `item.cleared` | I | `intake`, `item_id` | an unfinished item's checkpoint was cleared, for its rows to be purged |
| `item.duplicate_content` | W | `intake`, `item_id`, `count`, `reason`, `source_ref`, `sighting` | an item has the same content as one landed before. It is still landed (no data is dropped); the receipt names the earlier item |
| `item.replaces_ignored` | W | `intake`, `reason` | an item named one it replaces that this intake doesn't know, or named it badly; landed as a new item, with no link |
| `item.resubmitted` | I | `intake`, `item_id`, `operator`, `reason` | an operator's resubmission was landed as a new item linked to the one it replaces |
| `resubmit.refused` | W | `intake`, `item_id`, `operator`, `reason` | a resubmission was refused |
| `push.too_large` | W | `intake`, `item_id`, `bytes` | an HTTP push was larger than the intake allows and was refused |
| `push.broken` | W | `intake`, `item_id`, `bytes` | an HTTP push's body ended early or didn't match its declared length; refused |
| `push.busy` | W | `intake` | an HTTP push waited `max-wait` for the one before it and was answered `503` with `Retry-After`; the sender tries again |
| `push.replayed` | I | `intake`, `item_id` | an HTTP push with a key already landed was answered with the first receipt |
| `push.unknown_replaces` | W | `intake` | an HTTP push named an item it replaces that isn't known |

### 5.3 Table intakes

Events of an intake that reads an Iceberg table (DESIGN.md §5.3.18). Each also carries the snapshot it concerns as
`sdh.snapshot_id`.

| Event | Sev | Fields | Meaning |
|---|---|---|---|
| `intake.table_history_lost` | W | `intake`, `snapshot_id` | the snapshot the intake had read up to has expired, or the table was rolled back past it. Every snapshot still kept is followed, but appends between the mark and the oldest kept are lost. **Review:** ask the producer to send them again |
| `intake.table_rewrite` | W | `intake`, `snapshot_id`, `reason` | an `overwrite` or `delete` snapshot, which can't be expressed as new items; nothing is landed for it (a compaction is passed over without an event) |
| `intake.table_file_skipped` | W | `intake`, `snapshot_id`, `reason` | a data file couldn't be read: its format (`FORMAT`) or encryption (`ENCRYPTED`) |
| `intake.table_file_missing` | W | `intake`, `snapshot_id`, `reason` | a data file a snapshot names is missing |

## 6. Operators' commands

Every operator command is audited, through the CLI as `admin.<command>` and through the control API as
`control.<operation>`, with `-` in the command's name written `_`: `drain-check` is `admin.drain_check`.

### 6.1 Commands

| Command | Stages | Needs four eyes | Fields it adds (§6.2 and §6.3 for the rest) |
|---|---|---|---|
| `status` | all | | `stage`, `item_id`, `count`, `state` |
| `stuck` | all | | `stage`, `count` |
| `drain-check`, `drain` | all | | `stage`, `release`, `count`; `control_scope`, `claims` |
| `version` | all | | `stage`, `release`; with `--accept`, also `stage.version_forced` |
| `control-state`, `pause`, `resume` | all | | `control_scope`, `paused` |
| `keys`, `rotate` | all | | `control_scope`, `kek` |
| `rekey` | all | yes | `control_scope`, `operation_id`, `kek_from`, `kek_to`, `files`, `rows` |
| `retention`, `retention-set` | all | | `control_scope`, `retention` |
| `health`, `settings`, `compliance`, `support-bundle` | all | | — |
| `purges`, `purge` | all | `purge`: yes | `control_scope`, `operation_id`, `item_id`, `files`, `rows`, `ticket` |
| `erase` | all | yes | `control_scope`, `tables`, `objects`, `ticket` |
| `restore-check`, `restore-repair`, `restore-release` | all | `restore-repair`, `restore-release`: yes | `control_scope`, `tables`, `files`, `objects`, `ticket`, `result` |
| `failures`, `decline`, `reopen`, `retry`, `correct`, `export` | all | | `stage`, `item_id`, `seq`, `state`, `from_state`, `correction`, `count`, `bytes`; `export`: `recipient_key` |
| `reprocess` | all | | `stage`, `run_id`, `selector`, `state`, `count` |
| `patients` | all | | `stage`, `item_id`, `count`, `outcome` (`list`, `find`), `recipient_key`. **Review:** each run must match a request on the privacy official's log |
| `audit-verify`, `audit-export` | all | | `outcome` (`intact` or changed), `count` |
| `audit-prune` | all | yes | `count` |
| `restore` | split | with `--plaintext`: yes | `item_id`, `table`, `offset`, `bytes`, `outcome` (`plaintext`, `sealed`), `recipient_key`, `ticket`. **Review:** every `plaintext` must have its ticket (HIPAA-OPERATIONS.md §5) |
| `release` | split | | `item_id`, `action`, `correction` |
| `resubmit` | landing | | `intake`, `item_id`, `table`, `state`, `reason` |
| `operator-keys`, `operator-key-register`, `operator-key-revoke` | control API only | | `key_principal`, `sealing_key`, `count` |

### 6.2 Through the CLI: `admin.<command>`

| Event | Sev | Fields | Meaning |
|---|---|---|---|
| `admin.<command>` | I | `operator`, `result` (`ok`), and the command's fields | the command ran and succeeded |
| `admin.<command>` | W | `operator`, `result` (`failed`), `reason` (an exception's class), and the fields gathered so far | the command failed |
| `admin.<command>` | W | `operator`, `decision` (`needs-approval`), `result` (`refused`) | a four-eyes command was refused, as the CLI can't prove two people (HIPAA-OPERATIONS.md §3.3) unless `cli-four-eyes` is waived |

`sdh.operator` is the name given to the CLI, which is self-asserted: match each `admin.*` event to the Kubernetes
audit of `pods/exec` by time and pod (HIPAA-OPERATIONS.md §2.4, §3.1). **Review:** every `admin.*` event.

### 6.3 Through the control API: `control.<operation>`

Each call carries `operator` (the authenticated principal), `auth`, `request_id`, and `permission` where one is
needed. Actions add `action_reason` and, where needed, `ticket`.

| Event | Sev | Fields | Meaning |
|---|---|---|---|
| `control.<operation>` | I | `decision` (`allowed`), `result` (`ok`, `not-found`), the command's fields; `parts`, `sealing_key` for sealed answers; `operation_id` for a long operation | the call was allowed and answered |
| `control.<operation>` | I | `decision` (`allowed`), `idempotent_replay`, `result` (`replayed`) | a retried action, answered from its first answer |
| `control.<operation>` | W | `decision` (`allowed`), `result` (`failed`), `reason` | the call was allowed and failed |
| `control.<operation>` | W | `decision`, `result` (`refused`) | the call was refused. `decision` says why: `denied` (no permission), `needs-approval`, `bad-request`, `not-found`, `in-progress`, `key-reused`, `not-on-this-stage`, `not-your-request`, `result-gone`, `sealing-key-not-yours`, `wrong-state`, or the command's own refusal code. **Review:** repeated `denied` |
| `control.<operation>` | I | `outcome` (`awaiting-approval`), `operation_id` | a four-eyes operation was asked for and waits for a second person |
| `control.<kind>` | I or W | `decision` (`approved`), `operation_id`, `requested_by`, `approved_by`, `action_reason`, `ticket`, `result` | an approved four-eyes operation ran, by the control pod |
| `control.operations` | I or W | `permission`, `decision`, `result` | operations were listed, or one looked up |
| `control.operation_approve`, `control.operation_reject`, `control.operation_cancel` | I or W | `operation_id`, `decision`, `result` | a second person approved or rejected an operation, or its requester cancelled it. The requester can't approve their own |
| `control.operation_result` | I or W | `permission` (`phi.break-glass`), `operation_id`, `decision`, `result`, `bytes` | a break-glass result was fetched, once. **Review:** each must match its approved operation and ticket |
| `control.unauthenticated` | W | `auth`, `request_id`, `decision` (`unauthenticated`), `result`, `reason` | a call whose caller couldn't be authenticated. **Review:** repeated ones |

## 7. Not audit events: the journal's own health

The journal writer reports on itself in the stage's log (logger `JdbcAuditJournal`), not as audit events, since it
can't journal its own failure. HIPAA-OPERATIONS.md §3.1 asks the analyst to watch for them in the stage logs:

| Log event | Level | Meaning |
|---|---|---|
| `audit.journal_started` | INFO | a chain was opened (`chain_id`) |
| `audit.journal_waiting` | WARN | events are waiting to be written: the database is slow or down. While the journal can't be written, operator commands other than those that only read, and read no PHI, are refused |
| `audit.journal_write_failed` | WARN | a write failed and will be tried again |
| `audit.journal_seal_failed` | WARN | a seal couldn't be made; the chain's tail is unsealed until the next |
| `audit.journal_closed` | INFO or WARN | a chain was closed at stop (`entries`); WARN when an event arrived after closing |
| `audit.journal_close_failed` | WARN | a chain couldn't be closed: `audit-verify` will show it `UNCLOSED` |
| `audit.journal_not_closed` | ERROR | events were left unwritten at stop (`unwritten`): they are in the export and console log only |

## 8. Keeping this catalogue complete

`AuditEventsCatalogueTest` (`connector-stage`) reads the main sources of every module, and of the
healthcare-data-transformer checkout when it is beside this repository, and fails if:

- an event named in a call to `Telemetry.audit`, `auditWarn` or `auditError`, or to a helper that audits for
  its caller (`RecordReconciler`'s `Recovery`, `StageRegistry`'s `number`, `IcebergTableSource`'s `report`),
  isn't a row of its own here (`` | `name` | ``);
- a name is built, or given in a variable, at a call site it doesn't know: the families `admin.`, `control.` and
  `failure.` are listed here by hand (§4.5, §6), so a new site must be catalogued, then added to the test;
- a control API operation isn't in §6.1's table of commands.

Without the transformer's checkout beside it, the test can't see the transform stage's one event,
`transform.record_skipped` (§4.4): run it with both checked out before a release.

## 9. Findings

- **The OTLP export carries no chain ID.** HIPAA-OPERATIONS.md §3.2 said to compare the chains `audit-verify`
  lists with the SIEM's copy by `sdh.chain_id`, "which every exported event carries". The OTLP records carry no
  chain ID or sequence number; only `audit-export`'s lines do (`chain`, `seq`). Until the OTLP records carry them,
  compare chains with the SIEM's copy of `audit-export`, which §3.2 now says. Product change owed: add
  `sdh.chain_id` and the entry's sequence number to each exported record.
- **Events before the journal opens can be dropped from it.** HIPAA-OPERATIONS.md §2.2 says no event is ever
  dropped. Once the journal is open that holds: the stage waits rather than lose one. Before it opens, the events
  held in memory are capped at 1,000 (`Telemetry.EARLY_EVENTS`), the oldest dropped from the journal beyond that,
  though not from the export or console log. A process emits a handful at startup, so this isn't expected to
  happen; a fix would be to wait, as the open journal does.
- **Some operator fields are free text** (`sdh.action_reason`, `sdh.ticket`): see §1 and DATA-FLOWS.md §3.

## Sources

| Statement | Source |
|---|---|
| One call path, three copies; event name prefix | `connector-core` `Telemetry` (`audit`), `AuditJournal`; `connector-runtime` `AuditOnlyLogRecordExporter` |
| Journal tables and columns | `connector-core` `AuditTables`, `JdbcAuditJournal` |
| Export lines | `connector-stage` `AuditCommands` (`audit-export`), `connector-core` `AuditTrail.export` |
| Fields | `connector-core` `Attr`, `Fields` |
| Operator events | `connector-stage` `AdminCli`, `AuditRecord`, `ControlService`, `ControlConfiguration`, `ControlRoute` |
| Four-eyes commands | each command's `approval` (`AuditCommands`, `DataCommands`, `KeyCommands`, `RestoreCommands`, `file-connector` `RestoreCommand`) |
| Failure events | `connector-core` `FailureStore`, `CorrectionStore` |
| Journal health | `connector-core` `JdbcAuditJournal` |
