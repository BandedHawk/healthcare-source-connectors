# Retention and destruction

For the security official and the records manager who set how long a pipeline keeps PHI, the data admins who purge
and erase it, the key custodians who destroy its keys, and the platform administrator who disposes of its storage.
It covers what each removal deletes, what it leaves and where, how keys end what is left, and what a restore does to
a removal. It is HIPAA.md §11.2's document D9, for the operator's disposal procedures (§ 164.310(d)(2)(i)) and the
return or destruction of PHI at the end of a contract (§ 164.504(e)(2)(ii)(J)). The procedures, step by step, are
HIPAA-OPERATIONS.md §8 (retention, purge, end of contract) and §3.3 (the audit journal); where PHI is held is
[DATA-FLOWS.md](DATA-FLOWS.md) (D5); the keys are [KEYS.md](KEYS.md) (D3).

It describes a pipeline deployed from `deploy/reference/` and the kits' READMEs, with the `hipaa` compliance profile
on. **The product never removes data on its own:** retention removes only what is older than the period in force,
and a purge or an erasure runs only when two people have asked for it under a ticket (CONTROL-API-DESIGN.md §11).
Out of the box nothing is removed at all: every retention is unset, and so kept for ever (§8).

## 1. What holds what, and what removes it

| Where | What | Removed by | Left after it |
|---|---|---|---|
| Bronze `landed_items` | each item as received, encrypted | retention (§2.1), purge (§2.4), erasure (§2.5); a given-up item's chunks after a grace period (§2.2) | noncurrent object versions; backups |
| Bronze `landing_ledger` | per item: keyed hashes, sizes, times, `item_sha256`; no content | erasure only: it is how a read-only source's files are recognised (DESIGN.md §5.3.2) | backups |
| Bronze `split_records`, silver `records`, gold `fhir` | each record, encrypted | retention, purge, erasure | noncurrent object versions; backups; copies delivered out of the pipeline (§3) |
| Old snapshots and unreferenced files | files compaction replaced; files a failed commit wrote | snapshot expiry and orphan removal (§2.7) | noncurrent object versions |
| Registries (`stage_item`, `stage_record`, the split stage's) | IDs, states, positions; no PHI | registry retention, convert and transform only (§2.3); the schema's deletion (§6) | backups |
| The fail store and corrections (`stage_failure` and its events, `stage_correction`) | failures' codes and positions; operators' reasons and corrected content, encrypted | a purge deletes the item's corrections; erasure deletes both | backups |
| Control tables (`sdh_control_state`, `_operations`, `_purges`, `_requests`, `sdh_operator_keys`) | who asked what, under which ticket; operators' public keys | idempotency keys after their window; operator keys by revocation; otherwise the schema's deletion | backups |
| Held results (`sdh_control_results`) | a sealed lookup, or a break-glass plaintext encrypted under the stage's KEK | at fetch, or at expiry (an hour at most) | backups and WAL, until they expire |
| The control log (`<namespace>/_control/` in the store) | each purge, erasure, rotation, re-key and retention change: IDs, tickets, principals, times; no PHI | the bucket's deletion (§6) | noncurrent object versions |
| The audit journal | IDs, codes, counts, principals, reasons | `audit-prune` (§2.6) | the exported events, in the customer's audit store |
| The broker's topics | sealed messages naming what was stored | the topics' own retention (Kafka 7 days by default) | nothing, once the topic's retention ends |
| The KMS | the KEKs and signing keys | the key custodians (§4) | the KMS's own waiting period |

## 2. Each removal

### 2.1 Retention of the tables

Each stage keeps its own table's rows for its own period: setting one stage's doesn't shorten another's. Set all
four. A retention run deletes **whole UTC days** older than the period, by the table's time column, and the files
that held them **in the same run**: it commits a marker snapshot and expires every snapshot before it, so no
earlier snapshot still points at the deleted files (`TableMaintenance`). The landing node never takes part of an
item: an item landed across midnight keeps all its chunks until its last day goes (DESIGN.md §5.3.13).

| Setting | Default | Stage | What it removes |
|---|---|---|---|
| `sdh.maintenance.retention.landed-items` | unset | landing | `landed_items`, by `landed_at`; at least 7 days, so the split stage has time to read each item |
| `sdh.maintenance.retention.records` | unset | split | `split_records`; at least 1 day |
| `sdh.maintenance.retention` | unset | convert, transform | the stage's sink: silver `records`, gold `fhir`; at least 1 day |
| `sdh.maintenance.retention-bounds.minimum` | unset | every stage | the shortest period the control API may set; unset, the stage's own floor |
| `sdh.maintenance.retention-bounds.maximum` | unset | every stage | the longest; unset, the API may set `forever` |

Unset is for ever. The setting is the starting value: a data admin reads the period in force with `retention`
(`GET /retention`) and sets another with `retention-set` (`PUT /retention`, `days` or `forever`), within the
bounds. A setting outside its bounds refuses startup, and the API refuses a period outside them. The period set
is kept in `sdh_control_state`, read by maintenance at each run; a run that can't read it keeps every row. Each
change is audited (`sdh.retention`) and kept in the control log; each run that deletes is audited as
`maintenance.retention`, with the table, the number of files and the first day kept. Maintenance must be on
(`sdh.maintenance.enabled`, the default) for retention to run.

**What it leaves:** each file it deletes stays in the bucket as a noncurrent version until the lifecycle rule
expires it (§3); the database's backups; the registry's rows of the items (§2.3); failures; the ledger.

**A stage that keeps less than the stage before it** can't rebuild what it lost: a record past retention can't be
reprocessed (`past_retention`), and the landing node's retention must outlast how far the split stage may fall
behind, or an item purged before it is read fails as `SOURCE_READ_FAILED` (DESIGN.md §5.3.13).

### 2.2 Items given up during landing

An item given up part way (its source changed or vanished while it was read, a broken push, a failed batch) leaves
committed chunks with no summary row, which nothing reads. They are removed after a grace period, so an operator has
time to look into the give-up first:

| Setting | Default | Stage | What it removes |
|---|---|---|---|
| `sdh.maintenance.retention.given-up` | 7d | landing | the chunks of an item given up, from when it was given up; at least 1 day |

Each removal is audited as `item.purged`. An item that was landed after all is never removed this way
(`item.purge_refused`).

### 2.3 Registry retention

The convert and transform stages keep a row in their registry for every item and record they handled. The rows
hold no PHI, and are kept for ever unless the customer sets a period (DEPLOYMENT.md, "Registry retention"):

| Setting | Default | Stage | What it removes |
|---|---|---|---|
| `sdh.stage.retention.after` | unset | convert, transform | `stage_record` and `stage_item` rows of an item complete for longer than this, if none of its failures is open |
| `sdh.stage.retention.queue-retention` | 7d | convert, transform | nothing: the input queue's retention, as the broker is configured; `after` must exceed it by a day |
| `sdh.stage.retention.interval` | 1h | convert, transform | nothing: how often the duties pod prunes |
| `sdh.stage.retention.batch` | 100 | convert, transform | nothing: the most items deleted in one transaction |

It never deletes anything stored, the landing ledger, failures, the audit journal, or the split stage's registry.
Each run is audited, `stage.registry_pruned`, with a count. Record the choice with the records-management policy:
after pruning, a message replayed by hand is processed again (HIPAA-OPERATIONS.md §2.9, "Registry retention").

### 2.4 Purging one item

For an individual's request the covered entity has granted, or data sent in error. A data admin asks `purge`
(`POST /items/{itemId}/purge`, a ticket and a reason) **at each stage the item passed through, landing included**,
and a second data admin approves each. The stage's duties pod then, at each maintenance tick (`PurgePass`):

1. rewrites each file holding the item's rows without them, a budget at a time, and expires the snapshots that
   referenced the old files, so they leave storage in the same run;
2. once nothing is left to rewrite, deletes the item's corrections, whose content is a document's;
3. checks no table holds a row of the item, and only then marks the purge `done`, audited as `data.purged` with
   the files and rows. `purges` (`GET /purges`) shows where each stands; a purge that fails twelve runs in a row
   is failed (`data.purge_failed`), and the data is still there.

**What it leaves:** the item's failures, with their codes and positions and any reason an operator gave,
encrypted; the landing ledger's entry, with `item_sha256`; the registries' IDs; the purge's own row and control log
entry, with the ticket; the audit journal; the item's sealed messages on the topics until their retention ends;
noncurrent object versions and backups; and every copy delivered out of the pipeline (§3), which the receiver must
purge. The answer says so.

### 2.5 Erasing a stage

At the end of a contract, or when a pipeline is retired (HIPAA-OPERATIONS.md §8 gives the order). Each stage is
paused, and drained if it drains; then a data admin asks `erase` (`POST /erase`, a ticket and a reason) and a
second approves it. It is refused while the stage is running or holds a live claim, and can't be undone. The
stage:

1. marks itself erased in `sdh_control_state`, so its pods refuse to start from then on, and records the erasure
   in the control log;
2. drops every table it writes, with their data files, and deletes what is left under each table's location where
   the storage can list it (`prefixRemoved`);
3. deletes its rows from the database tables that hold wrapped keys: failures and their events, corrections,
   reprocess runs, release acceptances, and the split stage's held-item releases;
4. answers an **erasure certificate**: a JWS of type `sdh-erasure+jwt`, signed ES256 with the stage's signing key.
   Check it with the key's public half, and keep it with the audit archive.

| Claim | What it holds |
|---|---|
| `scope` | the stage's control scope |
| `operation` | the erase operation's ID |
| `erasure` | the erasure's own ID, in the control state |
| `requestedBy`, `approvedBy` | the two principals |
| `ticket` | the ticket it was asked under |
| `erasedAt`, `at`, `iat` | when the stage was marked erased, and when the certificate was signed |
| `tables` | each table dropped: `name`, `location`, the number of `objects` under it, and whether its prefix was removed (`prefixRemoved`) |
| `rows` | the rows deleted, by database table |
| `keks` | the KEKs the stage's data, wrapped rows and ID key are under now, and both ends of a re-key under way |
| `signingKid` | the signing key's ID |
| `journalHeads` | each audit chain's last `seq` and `hash`, by `chainId` |
| `journal` | that the audit journal is kept |
| `custodian` | that the KEKs and the signing key named must be destroyed in the KMS |

**What it leaves:** the audit journal (export it first, `audit-export`); the registries, leases, control tables and
operator keys in the stage's schema; the control log under `_control/`, so that a restore of the database to before
the erasure erases again (§5); any objects under a location whose storage couldn't list (`prefixRemoved: false`);
noncurrent object versions and backups; the topics. Step 5 of HIPAA-OPERATIONS.md §8 deletes the schemas, buckets
and topics; destroying the keys (§4) makes what is left unreadable.

### 2.6 Pruning the audit journal

HIPAA sets no period for audit records; counsel sets one (six years is the documentation rule's, § 164.316(b)(2)).
`audit-prune` (`POST /audit/prune`, with `before` and a reason) removes whole chains last written before that time,
so what is left still verifies, and refuses a time less than ten minutes ago. An audit admin asks and another
approves (`audit.admin`); the CLI refuses it under the `hipaa` profile. Export first: `audit-export --to <date>`.
What it leaves: the export, and the events the stages sent to the customer's audit store (HIPAA-OPERATIONS.md §2.7,
§3.3).

### 2.7 What maintenance removes as it goes

| Setting | Default | What it removes |
|---|---|---|
| `sdh.maintenance.snapshot-retention` | 24h | snapshots older than this, and the files only they referenced: files compaction replaced, holding the same rows as the current ones |
| `sdh.maintenance.min-snapshots` | 2000 | nothing: the fewest snapshots kept; it and `snapshot-retention` keep whichever is more |
| `sdh.maintenance.orphans.min-age` | 3d | files no snapshot refers to (a commit that failed after writing), once older than this; at least 1 hour; run every `sdh.maintenance.orphans.interval` (24h); audited `maintenance.orphans_deleted` |

The kits' example settings keep snapshots 7 days and at least 10. Old snapshots never hold a row that retention,
a purge or a re-key removed: those runs expire every snapshot before them at once. While a database restore is
under way, unreferenced files are kept until `restore-release` (§5).

The control API removes its own short-lived rows: a held result at fetch or expiry, and the idempotency keys of
its requests after their window (CONTROL-API-DESIGN.md).

## 3. What no removal reaches

The product removes what it stores. These copies are outside it, and the customer's disposal procedures must cover
each:

- **Noncurrent object versions.** The buckets keep versions so the database can be restored (HIPAA-OPERATIONS.md
  §7). Every file a retention run, a purge, an erasure, compaction or a re-key deletes stays as a noncurrent
  version until the bucket's lifecycle rule expires it, or for ever without one. Object Lock keeps it at least its
  retention, whoever asks. Set the rule's period to the database's recovery window, no longer (the AWS PoC keeps
  7 days, `versions_kept_days`). Versions are ciphertext under the same KEKs.
- **Database backups and archived WAL,** until their own retention ends. They hold the deleted rows: encrypted
  columns (failures' reasons, corrections, held results) as ciphertext under the KEKs, and the registries, control
  tables and audit journal in plain, with no PHI.
- **The broker's topics,** until each topic's retention ends. Messages are sealed to the next stage, and name the
  item, its record IDs and where they were stored; the dead-letter topics hold IDs and digests only.
- **Copies delivered out of the pipeline:** what gold's readers wrote elsewhere (a FHIR server, a warehouse), and
  the results sent to a producer (`item.received`, `item.result`). They are the receivers' to remove.
- **The producer's source.** A `read-only` intake never changes it, so the item stays where it was sent from, under
  the producer's controls (`sdh.landing.source.access`).
- **Sealed results on operators' machines,** and anything an operator decrypted from them: the operators'
  procedures (HIPAA-OPERATIONS.md §8, "Operator key pairs").
- **The customer's audit store and SIEM,** which receive the exported events: IDs, codes, principals and reasons,
  no PHI. Their retention is the customer's.
- **Logs, metrics and traces** at the collector hold no PHI, object key or exception message (DESIGN.md §7.5); their
  retention is the customer's. Support bundles likewise.
- **The nodes' disks and memory:** no stage writes plain PHI to disk, and no dump is made (HIPAA-OPERATIONS.md
  §2.9). A volume's disposal is the provider's (§7).

## 4. Destroying the keys: the cryptographic erase

Everything the pipeline stored, everywhere it was copied as stored, is ciphertext under its KEKs: the tables, their
noncurrent versions, the wrapped keys in the database, and every backup of them. Destroying the KEKs ends it all at
once, which is why the end of a contract relies on it (KEYS.md §7). **The product never disables or destroys a
KMS key:** the key custodians do, two people, in the KMS.

What to destroy:

- **Every KEK the pipeline ever used,** not only those the certificates name. A certificate's `keks` are those the
  stage's data is under at its erasure; a KEK retired earlier by a re-key is not among them, yet a backup or a
  noncurrent version made before the re-key is still under it. Take the full list from the key register and the
  control log's rotations and re-keys (`GET /keys` before the erasure shows the rotations).
- **Each stage's signing key**, named by `signingKid`. Keep its public half with the audit archive, so the
  certificates and the journal's seals can still be checked.
- **The inbox key pairs and their passphrases**, wherever they were kept, which open what is left on the topics;
  **the gold readers' key pair**, with the data consumer owner (KEYS.md §7).

How long it takes, by KMS (check the provider's current documentation):

| KMS | Destruction | Until then |
|---|---|---|
| AWS KMS | `ScheduleKeyDeletion`, after a waiting period of 7 to 30 days (30 by default); a multi-Region key's replicas are scheduled each | the key is disabled and can be cancelled; nothing can be decrypted with it |
| Google Cloud KMS | destroying a key version schedules it for the key's `destroy_scheduled_duration` (30 days by default) | the version can be restored |
| Azure Key Vault | deletion, then purge after the soft-delete retention (7 to 90 days); with purge protection, not before | the key can be recovered |
| HashiCorp Vault Transit | deleting a key needs `deletion_allowed`, and is immediate | Vault's own storage snapshots still hold it: destroy those too |

Record each key's destruction, its date and the two custodians, with the certificates. Destroy the KEKs only after
the last backup that someone may need to restore has expired, or as the contract requires: once they are gone,
nothing under them can be restored.

## 5. A restore after a removal

Restoring the database to a point T before a removal brings back what the removal had deleted from the database,
and the restore's repair brings back from the store's versions every file the restored catalog needs
(HIPAA-OPERATIONS.md §7: `restore-check`, then `restore-repair`, four eyes, and `restore-release` once the
pipeline has caught up; each needs `data.admin`). The repair then makes the removals hold again:

- **Purges** made since T are asked again, from the control log, so purged records stay purged. Until they are
  done again, the item's rows are back.
- **An erasure** made since T is marked again, so the stage's pods refuse to start; ask `erase` again to drop its
  tables.
- **A retention change** made since T is reported `to-redo`, to be set again once the stage runs. Rows a retention
  run deleted after T are back until the stage's next maintenance run deletes them again.

So a restore can't undo a removal for good, as long as the control log and the noncurrent versions it needs are
there. A restore point taken before the KEKs were destroyed restores nothing readable.

## 6. At the end of a contract

HIPAA-OPERATIONS.md §8 is the procedure. What each step removes, and what to keep as evidence:

| Step | Removes | Keep |
|---|---|---|
| Return what the covered entity must get back, through its readers | nothing | the covered entity's receipt |
| `audit-export` everything | nothing | the archive, for the covered entity's security official; the signing keys' public halves |
| Pause and drain every stage; `erase` each, four eyes | each stage's tables, files and wrapped rows (§2.5) | each stage's erasure certificate |
| Destroy the keys (§4) | everything left, as readable data: versions, backups, topics | each key's destruction record |
| Delete the buckets, the Postgres schemas and the topics | what was left: the control log, the registries, the control tables, the noncurrent versions | the platform administrator's record |
| Dispose of the media | the provider's | the provider's destruction terms (NIST SP 800-88) |

## 7. Media

The product holds nothing on media of its own. The disks under the buckets, the database, the broker and the nodes
are the provider's or the customer's, and their disposal follows the provider's terms (for a cloud, its media
sanitisation statement) or NIST SP 800-88 for the customer's own hardware. Operators' workstations that held
sealed results, and any removable media an export was written to, are the customer's.

## 8. Findings

- **Nothing is removed unless the customer sets it.** `deploy/reference/` sets no retention and no bounds, and the
  `hipaa` profile doesn't require either: a pipeline deployed from it keeps every row for ever, and its data admins
  may set any period. Set the four retentions and their bounds before real PHI (HIPAA-OPERATIONS.md §8).
- **A purge is asked stage by stage.** One item means up to four requests and four approvals, and nothing checks
  that every stage the item passed through was asked (HIPAA.md P4).
- **A purge leaves the item's failures and its ledger entry.** The failures hold codes, positions and any reason an
  operator gave, encrypted; the ledger holds `item_sha256`, which for a small, guessable item confirms what was
  received (DATA-FLOWS.md §3). Only an erasure, or destroying the KEKs for the reasons, removes them.
- **An erasure certificate names the KEKs in use at the erasure,** not those an earlier re-key retired, which
  older backups and versions still need (§4).
- **Retention deleting old days is tested only below the harness:** `TableMaintenanceTest` deletes whole days and
  their files; the control harness's `erasure` scenario sets retention within its bounds, purges an item at
  three stages and erases every stage, but no scenario waits for a retention run to delete rows.
- **No cryptographic erase has been rehearsed:** no KEK has been destroyed and a restore from an older backup then
  tried.
- **CONFIGURATION.md §8 named the split stage's retention for every stage;** the convert and transform stages'
  is `sdh.maintenance.retention` (corrected 2026-11-25).

## Sources

| Statement | Source |
|---|---|
| Retention, its marker snapshot and expiry in the same run | `TableMaintenance` (connector-core); DESIGN.md §8.4, revision (h) |
| The landing node's retention, its floor, never part of an item; the ledger never purged; items given up | DESIGN.md §5.3.2, §5.3.13, revisions (au), (bf); `LandingProperties.Maintenance`, `GivenUpItems` |
| The retention settings and their floors | `LandingConfiguration`, `ConnectorRuntime` (file-connector), `RecordStageRuntime`; `CommonSettings.RetentionBounds`; `RetentionPolicy` |
| `retention`, `retention-set`, `purge`, `purges`, `erase` | `DataCommands`; CONTROL-API-DESIGN.md §11; HIPAA.md P4 |
| How a purge runs, and what it keeps | `PurgePass`, `ItemPurge` |
| What an erasure deletes, and its certificate | `DataCommands.Erase`, `StageKeys`; each stage's `erasable()` |
| Registry retention | `StageProperties.Retention`, `RegistryPruner`; DEPLOYMENT.md, "Registry retention" |
| `audit-prune` | `AuditCommands`, `AuditTrail`; HIPAA-OPERATIONS.md §3.3 |
| Held results and idempotency keys | `ControlResults`, `ControlStore` |
| The control log, and a restore's repair | `ControlLog`, `RestoreCommands`; HIPAA-OPERATIONS.md §7, DESIGN.md §18 |
| The audit events named | [AUDIT-EVENTS.md](AUDIT-EVENTS.md) |
| The AWS PoC's lifecycle rule | `deploy/aws/terraform/persistent/main.tf` (`aws_s3_bucket_lifecycle_configuration.layer`) |
| Tested | `TableMaintenanceTest`, `DataLifecycleTest`, `RegistryPrunerTest`; the control harness's `erasure` scenario, on every level that has run it |
| Checked by | `RetentionGuideTest` (connector-stage): every retention setting is a row with its default, every `data.admin` and `audit.admin` command is named, and every claim of the erasure certificate is in §2.5's table |
