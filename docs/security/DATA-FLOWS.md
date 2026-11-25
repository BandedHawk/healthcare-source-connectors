# Data flows and where PHI is

For the organisation that operates a pipeline: where protected health information (PHI) goes, where it is held,
in what form, and what protects it. It is written for the risk analysis (§ 164.308(a)(1)(ii)(A)) and the asset
inventory, and is HIPAA.md §11.2's document D5. What each role does about it is HIPAA-OPERATIONS.md; why the
product is built this way is DESIGN.md and STAGES.md, cited below so a reviewer can check each statement.

It describes a pipeline deployed from `deploy/reference/` with the `hipaa` compliance profile on
(HIPAA-OPERATIONS.md §2.1). A deployment that waives a check, or doesn't run the profile, may differ: the
profile refuses at startup what this document says can't happen.

## 1. The flow

```
 producer's source (share, bucket, queue, HTTP push, table)
     │  TLS / SSH / SMB 3 encrypted / mTLS
     ▼
 landing node (one active, one standby) ──► bronze: landed_items
     │  item.landed (sealed)
     ▼
 split stage ──────────────────────────────► bronze: split_records
     │  record.split, item.split (sealed)
     ▼
 convert stage ────────────────────────────► silver: records
     │  record.stored, item.converted (sealed)          └─► item.result to the producer (sealed), if on
     ▼
 transform stage ──────────────────────────► gold: fhir
     │  record.transformed, item.transformed (sealed)
     ▼
 gold readers (connector-queue-reader, each read audited)

 Beside every stage: Postgres (its registry, leases, audit journal, control tables), the KMS, the collector.
 The landing node also sends item.received to the producer (sealed), if results are on.
```

Each stage is its own Deployment, with its own workload identity, database role, buckets, keys and broker user
(HIPAA-OPERATIONS.md §2.8). Every connection that carries PHI, or keys to it, is TLS: to the object store, the
KMS, Postgres (and PgBouncer, with `verify-full`), the broker (SASL_SSL), and the landing node's HTTP intakes
(DEPLOYMENT.md, "Transports: what has been tested"). The stages' telemetry to the collector beside them carries
no PHI, and the profile allows it in plain (AWS-POC.md §8.2).

## 2. Where PHI is held

PHI is in plain text in exactly three kinds of place: **the producer's own systems** before landing, **the memory
of a process working on it** (a stage, a gold reader, an operator's tool opening a sealed result), and **whatever
a reader or operator does with it once decrypted**. Everywhere the product stores or sends it, it is encrypted.

| Where | What | Form | Protected by | Removed by |
|---|---|---|---|---|
| The producer's source (share, bucket, queue, table) | the documents, as sent | plain, under the producer's controls | the producer | `read-write` intakes delete, move or rename an item after landing it; `read-only` intakes never change the source (DESIGN.md §5.3.1) |
| In transit to the landing node | the documents | TLS (SFTP's SSH, SMB 3 with encryption required); HTTP push with mTLS, TLS ending at the pod, or OAuth2 | the transport, the intake's authentication (DESIGN.md §5.3.1) | — |
| A stage's memory while it works | the item or records in hand, a batch waiting to be written, and the stage's working keys | plain | the platform: no attach, no dumps, a read-only root filesystem, the stage's own node and identity (HIPAA-OPERATIONS.md §2.9) | ends with the batch; a pod's memory with the pod |
| Bronze: `<pipeline>_landing.landed_items` | each item as received, in 8 MiB chunks; its original name or path | AES-256-GCM per chunk, under a batch data key wrapped by the landed-items KEK (DESIGN.md §5.3.2, §7.2) | the KMS; the stage roles that may unwrap | retention (`sdh.maintenance.retention.landed-items`), purge, erasure (D9) |
| Bronze: `<pipeline>_split.split_records` | each record as cut, in its original form | encrypted as above, under the records KEK | as above | retention, purge, erasure |
| Silver: `<pipeline>.records` | each record converted to JSON, or in its original form with the error that stopped its conversion | encrypted, under the records KEK | as above | retention, purge, erasure |
| Gold: `<pipeline>_fhir.fhir` | each record mapped to FHIR | encrypted, under the FHIR KEK | as above; readers through `connector-queue-reader`, each read audited | retention, purge, erasure |
| The broker's topics (`<p>.landed`, `.split`, `.records`, `.fhir`, results) | messages naming what was stored and where; producer references in results | JWS signed by the sender, inside JWE to the next stage's (or producer's) public key (DESIGN.md §10.1) | the recipient's private key; broker ACLs per stage user | the topic's retention |
| Dead-letter topics (`<p>.convert.dlq`, `.fhir.dlq`) | `record.conflict` messages | sealed as above; IDs and digests, no content | as above | the topic's retention |
| The fail store and corrections (Postgres, each stage's schema) | an operator's corrected content for a failed record; reasons a failure was declined | encrypted with a data key wrapped by the stage's KEK (`content_enc`, `reason_enc`) | the KMS | the corrections' and failures' lifecycle (STAGES.md §15) |
| Sealed results of the control API and CLI | a restored item, a lookup | JWE to the operator's registered public key (CONTROL-API-DESIGN.md) | the operator's private key | on the operator's machine: the operator's procedures |
| A break-glass plaintext restore | a restored item | held encrypted under the stage's KEK, fetched once within an hour, then deleted (`ControlResults`) | four eyes, a ticket, the KMS | deleted at fetch or expiry |
| Backups | everything above that the database and buckets hold, as stored | as stored: ciphertext stays ciphertext | the backup bucket's own key and policy (HIPAA-OPERATIONS.md §7) | the backups' retention; noncurrent bucket versions by the lifecycle rule (§3) |

**What is never written anywhere:** no plaintext PHI is written to local disk or a temporary file; no heap dump,
core dump or `hs_err` file is made; no log line, metric, trace or audit event holds a document's content, an
exception's message, an object key or a file name (DESIGN.md §7.5). The audit journal and export hold IDs, codes,
counts and positions (HIPAA-OPERATIONS.md §2.2), and the reasons operators give for their actions (§3).

## 3. What is in plain, and what can be learned from it

Not PHI on its own, but what an attacker with the database, the bucket listings or the broker could read without a
key. The risk analysis should weigh each.

| Data | Where | What it reveals |
|---|---|---|
| Item IDs (UUIDv7) | every table, registry, message header and audit event | when an item was landed, to the millisecond |
| Record IDs: `base32(HMAC-SHA-256(idKey, content))` | tables, registries, Kafka message keys, audit events | that two records have the same content (DESIGN.md §7.3); nothing of the content without the ID key, which is stored wrapped by the KMS |
| `source_ref_hash`, message key hashes | `landed_items`, `landing_ledger` | nothing of the name without the ID key: keyed hashes |
| `item_digest` | `landed_items`, `landing_ledger`, item messages | nothing without the ID key: a keyed, chained digest |
| **`item_sha256`: a plain SHA-256 of the item's bytes** | `landed_items` (summary row), `landing_ledger`, the producer's `item.received` | **lets someone who can guess an item's exact bytes confirm the guess.** It is there so a producer can check its receipt. For a document of any size, guessing its exact bytes is not practical; for a short, templated item (a small queue message, a one-row CSV) it could be. Weigh this for intakes of small items |
| Sizes, chunk counts, record counts, times | tables, registries, messages | how large items are, how many records they hold, when they moved |
| Format, content type, the WSTL set's name and digest | tables, messages | what kind of data the pipeline carries |
| Error codes, byte offsets, line and column | silver records, the fail store, problems in `item.result` | where a record failed, never what it held (HIPAA-OPERATIONS.md §2.2) |
| `confidentiality` | gold rows | the item's high-water confidentiality code (such as `R`, restricted), so readers can filter before decrypting (HIPAA.md §8.1 item 10). It tells a reader that a restricted record exists, not what it is |
| `kek_ref`, wrapped data keys | every encrypted row | which KEK protects the row; the wrapped keys are useless without the KMS |
| Message metadata on the broker | each message | `messageId`, `type`, `itemId` and `application/jose`, and the Kafka key (item or record ID) (DESIGN.md §10.1) |
| Operators' reasons and tickets | the control API's operations and the audit journal | what the operator typed. The API takes at most 200 characters from letters, digits, spaces and `. , : _ # / -`, so a document's text can't be pasted in, but a name or record number could still be typed. **Operators must not put PHI in a reason or ticket** (HIPAA-OPERATIONS.md §1, "Reasons and tickets"); the fail store's own reasons, which may describe the data, are encrypted |
| Principals | the audit journal, control tables, operator keys | who did what, as the identity provider names them |

## 4. Keys, and who can open what

The keys are D3's subject; here, only what each can open.

| Key | Opens | Held |
|---|---|---|
| Landed-items KEK | `landed_items` | in the KMS; landing wraps, split unwraps |
| Records KEK | `split_records`, `records` | in the KMS; split and convert wrap, convert and transform unwrap |
| FHIR KEK | gold | in the KMS; transform wraps, gold readers unwrap |
| A stage's inbox private key | messages sent to that stage | in the stage's secret, in its memory |
| Gold readers' private key | `<p>.fhir` messages | with the readers |
| The producer's private key | its results | with the producer |
| An operator's private key | that operator's sealed results | with the operator, never with the product |
| The ID key | nothing: it makes record IDs and keyed hashes | wrapped by a KEK, in a table property; in the stages' memory |

The KEKs never leave the KMS. A copy of a stage's memory holds the data keys it has unwrapped and its working
keys, and its short-lived cloud credentials until they expire (HIPAA-OPERATIONS.md §2.9).

## 5. Copies that outlast a removal

What retention, a purge or an erasure doesn't reach at once, for the disposal procedures (D9):

- **Noncurrent object versions.** The buckets keep versions, as a restore needs (HIPAA-OPERATIONS.md §7). The
  files a purge rewrites or a retention run deletes stay as noncurrent versions until the lifecycle rule expires
  them (7 days on the AWS PoC). They are ciphertext under the same KEKs, so destroying a KEK makes them unreadable
  too.
- **Backups** of the database and buckets, until their own retention ends.
- **Broker topics,** until their retention ends: messages are sealed, and name what was stored.
- **Sealed results on operators' machines,** under the operators' procedures.
- **The landing ledger,** kept for ever by default so a file left on a read-only source isn't landed again: keyed
  hashes, sizes, times and `item_sha256`, no content (DESIGN.md §5.3.2).
- **The audit journal and export:** IDs and codes, kept as the customer's retention says.

## 6. Using this for the risk analysis

- **Assets:** the buckets and tables of §2, the database (registries, audit journal, control tables), the broker,
  the KMS keys of §4, the backups, the stages' nodes (for their memory), the gold readers, and the operators'
  machines that hold their private keys.
- **Where a threat meets plain text:** the producer's systems, the stages' and readers' memory, and decrypted
  results in operators' hands. Everything else needs a KMS permission or a private key as well as access to the
  data.
- **Residual risks to record:** PHI in memory while processed (HIPAA-OPERATIONS.md §2.9); the plain
  `item_sha256` for intakes of small, guessable items; free-text reasons and tickets; noncurrent versions and
  backups after a removal.

## 7. Sources

| Statement | Source |
|---|---|
| Landing table columns, what is encrypted | `connector-core` `LandingTables`; DESIGN.md §5.3.2 |
| Record table columns, used for split records, silver and gold | `connector-contract` `RecordsTable`; STAGES.md §3 |
| Registry and control tables | `StageRegistry`, `FailureStore`, `CorrectionStore`, `ControlStore`, `ControlResults`, `PurgeRequests`, `AuditTables` |
| Envelope encryption, record IDs | DESIGN.md §7.2, §7.3 |
| Message sealing and unencrypted metadata | DESIGN.md §10.1 |
| Reasons and tickets | `ControlService` (the `REASON` and `TICKET` patterns) |
| KEKs per stage | `deploy/reference/*-application.yml` (`sdh.crypto.keys`) |
| Memory | HIPAA-OPERATIONS.md §2.9 |
