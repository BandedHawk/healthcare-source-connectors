# Healthcare Source Connectors — Design

Status: **Draft for review, spike in progress** · Date: 2026-10-28 · Scope: parent project, `connector-contract`, `connector-core`, `connector-queue-reader`, `file-connector`

Revision 2026-10-28 (b): added the `connector-queue-reader` consumer library and `connector-contract` (§3, §10.2). Replaced the Iceberg-property lease with a Postgres lease plus fencing tokens (§9.3). Folded publish marks into batch commits (§9.2). Made host disk capacity an operator responsibility (§17). Recorded the §16 spike results.

Revision 2026-10-28 (c): built the splitters (§6). Resume uses a `ResumePoint` (offset plus PHI-free context offsets) instead of a bare offset, saved in the checkpoint as `resumeAt` (§6.1, §9.1). Sections are lists of literal parts and source ranges. The FHIR splitter uses a byte-level structural scanner instead of Jackson. Added X12 `GS`/`ISA` boundaries and TA1 handling.

Revision 2026-10-28 (d): built the split phase of the pipeline (§9.4). Multi-chunk sections are read twice (hash, then encrypt) so memory stays at one chunk (§7.2). Commit retries check the checkpoint first so a batch is never appended twice (§9.2). The checkpoint gained `format`, `fileSize` and the `ABANDONED` state (§9.1). Fingerprints and checkpoint MACs use labelled HMACs (§5.1, §9.1). Archiving, failure handling and broker adapters are not built yet.

Revision 2026-10-28 (e): built archiving and failure handling (§4.1, §11). Added the `FAILING` state, so the decision to fail a file is durable before anything else happens. Archive tables keep file-level values on a summary row (§8.2). The digest chain, the local fallback format and the completion order are now specified (§5.1, §11.1, §11.2). Broker adapters and the Camel wiring are not built yet.

Revision 2026-10-28 (f): wired the file connector (§5, §5.2, §10.3, §14). Startup order, the Camel route, the broker adapters and the `local` key provider are built. Found and fixed a Camel default that broke oldest-first pickup (§5, §16). The configuration shape changed in a few places (§14). Cloud KMS providers, admin commands and OpenTelemetry export are not built yet.

Revision 2026-10-28 (g): added the Docker integration suite, which runs on minikube (KVM2) (§15). Running it against real services found five problems, all fixed:
- the Camel Service Bus producer can't set per-message sessions or IDs;
- both Azure clients reorder sends that are in flight together;
- `S3FileIO` needs the AWS `kms` module;
- SeaweedFS 4.x needs S3 identities;
- committed sections could go unannounced when a file failed mid-publish.

See §10.3, §8.1, §11.2 and §16. MinIO images are no longer published, so S3 is tested on SeaweedFS only.

Revision 2026-10-28 (h): built table maintenance (§8.4): retention, compaction, snapshot expiry and orphan-file cleanup, scheduled between files under a writer lock. Retention now purges PHI files in the same run (a marker commit makes the delete expirable). Retention settings moved to `sdh.maintenance.retention` (§14).

Revision 2026-10-28 (i): built observability (§12): OpenTelemetry traces, metrics and audit events, exported over OTLP through PHI filters, and checked end to end against a real collector.

Revision 2026-10-28 (j): built the cloud key providers (§7.4) in a new `connector-kms` module: AWS KMS, Google Cloud KMS, Azure Key Vault and Vault Transit, for wrapping and for ES256 signing. Each is tested against an emulator or, for GCP, a fake KMS (§15.1).

Revision 2026-10-28 (k): built the container image (§14.1) and `ContainerImageIT`, which runs it locked down (read-only root filesystem, noexec `/tmp`, no capabilities, secrets as a Kubernetes-style volume). It found that the packaged app exited right after starting: Camel 4 ignores the old `camel.springboot.main-run-controller` setting, now `camel.main.run-controller`. Key files may now be group-readable (0440), as Kubernetes secret volumes with an `fsGroup` always are (§7.4).

Revision 2026-10-28 (l): built the operator commands `restore`, `decrypt-fallback` and `status` (§11.4). They are read-only, need `SDH_OPERATOR` and audit every run. Restores now always verify the digest, including partial ones. The image's JVM options moved to `/app/jvm.options`, shared with an `sdh-admin` wrapper (§14.1). Recording `replaces_file_id` on resubmission is not built yet.

---

## 1. Purpose

Ingest health information files (C-CDA, X12, FHIR, CSV, and proprietary formats) dropped into a directory, split each file into self-contained sections at a run-time-defined boundary, store every section encrypted in Apache Iceberg tables (Parquet data files), and announce each stored section on a run-time-selected message queue with an encrypted, signed message.

The service must:

- Resume after any crash or restart from the exact section where it stopped, including part-way through a file of up to ~1 TB.
- Process every file already in the directory, then keep picking up new ones.
- Never expose PHI in file paths, object names, IDs, queue metadata, logs or telemetry.
- Run as exactly one instance.

`file-connector` is the first of several connectors. Everything that isn't specific to files lives in `connector-core` so later connectors reuse it.

## 2. Technology baseline

All versions were checked against Maven Central on 2026-10-28.

| Component | Version | Notes |
|---|---|---|
| Java | 25 (LTS) | Virtual threads for I/O-bound publishing and KMS calls. HKDF uses the Java 25 `javax.crypto.KDF` API |
| Spring Boot | 4.1.2 | Spring Framework 7.0.x. 4.1 OSS support runs to mid-2027. Only `file-connector` imports the Boot BOM |
| Apache Camel | 4.22.2 (LTS) | Camel pins Spring Boot 4.1.1; the spike confirmed 4.1.2 works (§16) |
| Apache Iceberg | 1.12.0 | No LTS concept; latest stable |
| Apache Parquet | 1.17.1 | The version Iceberg 1.12.0 is built against; pinned to match, not upgraded separately |
| Hadoop (classes only) | 3.4.3 | Six jars, transitive dependencies excluded: `hadoop-common`, `hadoop-mapreduce-client-core`, `hadoop-shaded-guava`, `woodstox-core`, `stax2-api`, `commons-collections4` (§16) |
| Jackson 2 | 2.22.2 | Used by Iceberg only (Spring uses Jackson 3). Pinned in `file-connector`, because Boot 4.1.2 would downgrade it to 2.21.x |
| Nimbus JOSE+JWT | 10.10 | JWS/JWE message encryption and signing |
| PostgreSQL | any supported | Lease store (§9.3). Can be the JDBC catalog's database. Driver 42.7.14, HikariCP (Boot-managed, pool of 2) |
| AWS SDK v2 | 2.50.2 | `s3`, `apache-client` and `kms` for Iceberg's `S3FileIO` (`iceberg-aws`). `kms` is needed even without SSE-KMS: `AwsProperties` loads KMS model classes on startup. Same line as Camel's SQS client, imported as a BOM in the parent POM |
| KMS clients | `google-cloud-kms` 2.98.0, `azure-security-keyvault-keys` 4.10.6, `azure-identity` 1.18.1 | `connector-kms` (§7.4). Chosen to match the versions Camel already brings (gax 2.83, gRPC 1.8x, azure-core 1.57), so nothing shared is upgraded underneath Camel. Vault Transit uses the JDK HTTP client |
| Testcontainers | 2.0.5 | `-Pit` only, run by Failsafe 3.6.1 (§15) |
| OpenTelemetry | 1.62.0 (Boot-managed) | `connector-core` uses the API only; `file-connector` gets the SDK from `spring-boot-starter-opentelemetry` and exports over OTLP/HTTP with its own filtered exporters (§12) |
| Maven | 3.9.16 via Maven Wrapper | The local install is 3.8.7 |
| Spark | **Not used** | |

Where a component has no LTS line (Iceberg, Parquet, Nimbus), we use the latest stable release, with versions managed in the parent POM.

## 3. Project layout

```
healthcare-source-connectors/            groupId com.sdh.healthcare
├── pom.xml                              parent: dependency/plugin management, Java 25, enforcer
├── .gitignore                           excludes build output, IDE files, *.pem, *.key, *.p12, .env
├── mvnw, .mvn/wrapper/                  Maven 3.9.16
├── scripts/                             it-minikube.sh (Docker suite), build-image.sh (image), minikube-env.sh (shared)
├── docs/DESIGN.md                       this document
├── connector-contract/                  library jar: the wire contract shared by writers and readers
│   └── com.sdh.healthcare.connector.contract
│       ├── message/      message records, payload codec, contract version
│       ├── jose/         MessageSealer (sign + encrypt), MessageOpener (decrypt + verify)
│       ├── crypto/       KeyProvider SPI, section key derivation, chunk encryption
│       └── table/        sections table schema, partition spec, sort order
├── connector-queue-reader/              library jar for consumers (§10.2); depends on contract only
├── connector-kms/                       library jar: KMS KeyProviders (§7.4); used by file-connector, optional for readers
│   └── com.sdh.healthcare.connector.reader
├── connector-core/                      library jar (no main class)
│   └── com.sdh.healthcare.connector.core
│       ├── split/        SectionSplitter SPI + built-in splitters
│       ├── crypto/       KMS/local KeyProvider implementations, ID HMAC
│       ├── storage/      Iceberg catalog/FileIO setup, writers, compaction, retention
│       ├── checkpoint/   checkpoint model, lease store, lease keeper, fenced committer
│       ├── publish/      MessagePublisher SPI, OutboundMessage (broker adapters live in the connector app, §10.3)
│       ├── telemetry/    Telemetry facade (spans, audit events, metrics), Attr allow-list, Fields, error codes
│       └── pipeline/     source-agnostic ingest pipeline (read → split → encrypt → write → commit → publish)
└── file-connector/                      Spring Boot application + container image (Dockerfile, §14.1)
    └── com.sdh.healthcare.connector.file
        ├── FileConnectorApplication
        ├── config/       ConnectorProperties (sdh.*), KeyMaterial (local key provider), PEM reading
        ├── runtime/      ConnectorRuntime (startup order), ConnectorConfiguration (Spring wiring), FatalExit,
        │                 MaintenanceScheduler
        ├── route/        InboundRoute (directory discovery)
        ├── ingest/       InboundService (per-file handling, in-progress recovery, backoff)
        ├── publish/      QueueType (broker adapters), CamelMessagePublisher
        ├── telemetry/    OTLP exporters, AllowListSpanExporter, AuditOnlyLogRecordExporter, inbound age gauge
        ├── source/       LocalSourceFile, DirectoryOutcomeRecorder (completed/ and failed/ records, local fallback)
        └── admin/        operator commands: AdminCli (restore, decrypt-fallback, status), §11.4
```

Each module has its own `.gitignore` for module-specific output.

**Why `connector-contract` exists:** the message format, chunk encryption and table layout are written by `connector-core` and read by `connector-queue-reader`. Keeping them in one small module means both sides use the same code, and the reader doesn't pull in Camel, splitters or the pipeline. Consumers depend on `connector-queue-reader` only. They treat `connector-contract` as internal, apart from the message records it defines.

## 4. End-to-end flow

```mermaid
flowchart LR
  A[Inbound dir] -->|Camel file consumer<br/>oldest first, one at a time| B[Fingerprint + file ID]
  B --> C[Resumable reader<br/>seek to checkpoint offset]
  C --> D[Format splitter<br/>byte-offset aware]
  D --> E[Encrypt workers<br/>zstd → AES-256-GCM]
  E --> F[Iceberg writer<br/>sections table]
  F -->|every N sections / T sec / B bytes| G[Fenced atomic commit<br/>data + checkpoint]
  G --> H[Publish section messages<br/>JWS inside JWE]
  H --> I[Published seq recorded<br/>in the next commit]
  D -->|end of file| J[Archive original<br/>source_files table]
  J --> K[file.completed message<br/>delete original<br/>write completed/&lt;fileId&gt;.json]
  D -. error .-> L[Archive to failed_files<br/>or encrypted local fallback]
  L --> M[OTel failure event<br/>write failed/&lt;fileId&gt;.json]
```

### 4.1 File lifecycle (state machine)

```
DISCOVERED → SPLITTING → ARCHIVING → COMPLETED
                │            │
                └────────────┴──→ FAILING → FAILED
   (source gone while SPLITTING, or ARCHIVING with the archive unfinished) → ABANDONED
```

The state lives in the `sections` checkpoint (§9.1). `FileProcessor` (`connector-core` `pipeline/`) runs the whole lifecycle.

| State | Durable record | Restart behaviour |
|---|---|---|
| DISCOVERED | none | The file is still in the inbound directory, so it is simply picked up again |
| SPLITTING | `sections` checkpoint | Resume at `resumeAt` and continue at `nextSeq`. Republish any committed-but-unpublished sections first |
| ARCHIVING | `sections` checkpoint + archive checkpoint on `source_files` | Continue archiving from the last archived block. If the archive is already done, finish without the source (§11.1) |
| COMPLETED | `completed/<fileId>.json`; original deleted | Nothing to do |
| FAILING | `sections` checkpoint with stage, error code and offset; archive checkpoint on `failed_files` | Finish failing the file; never retry splitting it |
| FAILED | `failed/<fileId>.json` (+ `failed/<fileId>.enc` if fallback was used) | Nothing to do; operators take over |
| ABANDONED | `sections` checkpoint | Nothing to do; `file.abandoned` was logged |

**Startup (for the file connector):** if `FileProcessor.inProgress()` returns a checkpoint, fingerprint the inbound files and `process()` the match first. If there is no match, call `recoverWithoutSource()`.

Files are processed **one at a time, oldest first**. This keeps ordering predictable and resume logic simple, and the work within a single file is already parallelised (§7).

## 5. Directory handling (Camel)

- **Consumer:** `camel-file` watches `sdh.file.inbound-dir`, non-recursive, with `sortBy=file:modified`, `maxMessagesPerPoll=1` and **`eagerMaxMessagesPerPoll=false`**. Without the last option Camel applies the one-file limit while scanning, before sorting, so "oldest first" depended on directory order. The spike route had this bug; the application test caught it.
- **Re-offering:** `noop=true` and `idempotent=false`, so a file that couldn't be handled (for example, storage was down) is offered again on the next poll. Successful files are deleted by the processor, so they aren't seen twice.
- **Partially written files:** a configurable read lock (`changed` by default) waits until the file size and modification time stay unchanged for `min-age`. With `read-lock: marker` the route waits for `<name>.done`, and the marker is deleted together with the original. `include` is an Ant-style pattern; hidden files are never picked up.
- **Subdirectories:** `completed/` and `failed/` sit inside the inbound directory and are excluded from pickup.
- **Startup:** if a checkpoint records an in-progress file, that file is processed before anything else. It is matched by fingerprint, not name (size first, so only same-size files are hashed). If the file has disappeared, `FileProcessor.recoverWithoutSource()` finishes it if its archive is complete, or else raises `file.abandoned` and closes the checkpoint. The route is defined with `autoStartup(false)` and is started only after this recovery. The same check runs before every file, so a file left in progress by an earlier error is always finished first.
- **Completion:** Camel's own move/delete options are switched off. The pipeline moves and deletes files itself, because a file is only finished once archiving has been committed.
- **Finished-file records:** `completed/<fileId>.json` and `failed/<fileId>.json` contain no PHI: file ID, timestamps, section count, byte count, keyed file digest, and for failures the stage, offset and error code. The original filename never appears in either directory.

### 5.1 File identity

| Identifier | Definition | Purpose |
|---|---|---|
| `fileId` | UUIDv7, assigned at pickup and saved in the checkpoint | Correlates queue messages, telemetry and tables |
| fingerprint | size + `HMAC(idKey, "sdh-fingerprint" ‖ 0 ‖ first 1 MiB ‖ last 1 MiB)` | Recognises the same file after a restart without reading all of it |
| `fileDigest` | `base32` of a chained HMAC-SHA-256 over fixed 64 MiB blocks, computed during archiving | Integrity value sent in `file.completed` |

The digest is chained, `H_i = HMAC(idKey, "sdh-file-digest" ‖ 0 ‖ H_{i-1} ‖ block_i)` with `H_{-1}` = 32 zero bytes, so a restart only needs the last `H` value and the block number, both of which are saved in the archive checkpoint. An empty file is one empty block. The digest depends only on content, not on `fileId` or how the archive was committed.

### 5.2 Process lifecycle

`ConnectorRuntime` starts in a fixed order:
1. **Validate configuration:** splitter options, queue type and URI, and key material, before touching storage.
2. **Acquire the lease:** if another instance holds it, startup fails with `LEASE_HELD` and a non-zero exit.
3. **Open storage:** open the catalog and create or load the three tables.
4. **Fence:** fence every table with this instance's epoch.
5. **Load keys:** unwrap or create the ID key.
6. **Build the pipeline:** pipeline, archivers and `FileProcessor`.
7. **Start pickup:** run in-progress recovery (§5), then start the route.

Shutdown runs in reverse. The route is stopped first, so no file is in flight when the lease is released.

**Errors per file** (`InboundService`):
- **Fatal errors** (lease loss, unknown commit outcome, tampered checkpoint, broken invariant, any `Error`): `FatalExit` logs a PHI-free reason and the process exits with status 70.
- **Anything else:** the file is left in place and the route thread sleeps with exponential backoff (from `retry.initial-backoff`, capped at 5 min), which pauses the pipeline. The next poll tries again.

Nothing in this path logs a path or filename.

## 6. Splitting

### 6.1 SPI

Package `com.sdh.healthcare.connector.core.split`; built-ins in `split.builtin`.

```java
public interface SectionSplitter {
    String format();                                  // e.g. "ccda"
    default void validate(SplitConfig cfg) {}         // called at startup: bad config fails the service, not a file
    SplitSession open(SeekableByteSource src, ResumePoint from, SplitConfig cfg) throws IOException, SplitException;
}
public interface SplitSession extends AutoCloseable {
    Optional<Section> next() throws IOException, SplitException;
}
record Section(long sourceOffset, long sourceLength, List<Part> parts, ResumePoint resume)  // Part = Literal(bytes) | SourceRange(offset, length)
record ResumePoint(long offset, List<Long> context)
```

- **Byte ranges:** a section is a list of **parts**: source byte ranges plus small synthesised literals (a CSV header, an X12 envelope, an XML declaration or a rewritten root start tag) that make it self-contained. Splitters never hold section bytes in memory; `Section.open(src)` streams the plaintext. Sections are capped by `max-section-bytes`.
- **Resume:** every section carries the `ResumePoint` after it, and `open` must accept `ResumePoint.START` or any of these and produce exactly the sections that followed. `context` holds **source offsets only** of structures the splitter re-reads on resume: the enclosing ISA and GS segments for X12, and the open ancestor start tags for XML (for in-scope namespaces). Offsets carry no PHI, so the resume point can live in the checkpoint (§9.1). File-start context (CSV header, XML declaration, FHIR array-or-stream) is re-read from offset 0.
- **Limits:** `SplitterRegistry.open(format, src, from, nextSeq, cfg)` wraps the session and enforces §6.3, counting sections from `nextSeq` so the per-file count survives a resume. Splitters also stop scanning early once a section passes `max-section-bytes`.
- **Errors:** `SplitException` carries only an `ErrorCode` and a byte offset. Codes: `PARSE_UNTERMINATED_QUOTE`, `PARSE_UNBALANCED_ELEMENT`, `PARSE_XML_MALFORMED`, `PARSE_XML_UNSUPPORTED_ENCODING`, `PARSE_X12_INVALID_ENVELOPE`, `PARSE_X12_UNBALANCED`, `PARSE_X12_CONTROL_MISMATCH`, `PARSE_X12_UNEXPECTED_SEGMENT`, `PARSE_JSON_MALFORMED`, `PARSE_FHIR_UNEXPECTED_RESOURCE`, `RESUME_INVALID`, `LIMIT_*`.
- **Plug-ins:** new formats are discovered via `ServiceLoader` (proprietary formats can be dropped in as a jar).

### 6.2 Built-in splitters

The format and boundary are both set at run time.

| `format` | Default boundary | Behaviour |
|---|---|---|
| `ccda` / `xml` | `ccda`: element `ClinicalDocument`, ns `urn:hl7-org:v3`. `xml`: boundary required, `namespace` defaults to `*` (any) | Byte-level streaming XML tokenizer that ignores comments, CDATA, processing instructions and DOCTYPE. A section runs from the boundary start tag to its matching end tag; a nested boundary element stays inside the outer section. Namespace declarations in scope from ancestor elements are copied onto the section's root element, and the file's XML declaration is prepended, so every section is a valid standalone document. End tags are checked against start tags. Input must be ASCII-compatible (UTF-8, ISO-8859-x); UTF-16 fails with `PARSE_XML_UNSUPPORTED_ENCODING`. Entities from a DOCTYPE internal subset are not carried into sections. |
| `x12` | segment `ST` (also `GS` or `ISA`) | Delimiters are read from each ISA segment. `ST`: each section is `ISA + GS + ST…SE + GE + IEA`, with GE01 and IEA01 rewritten to 1. `GS`: `ISA + GS…GE + IEA`, IEA01 = 1. `ISA`: the interchange unchanged. A TA1 between ISA and GS becomes `ISA + TA1 + IEA` (IEA01 = 0), so nothing is dropped. Control numbers ST02/SE02, GS06/GE02 and ISA13/IEA02 are checked. BIN segments are not supported. |
| `fhir` | `Bundle` (`*` = any `resourceType`) | Byte-level structural JSON scanner (strings, escapes, bracket nesting) that checks each top-level object's `resourceType`. Jackson was dropped because it can't restart mid-array at a byte offset; full JSON validation is left downstream. Accepts a single resource, a JSON array, or NDJSON / whitespace-separated objects. Optional `fhir.ndjson-group-size` = N groups N resources per section (boundary then defaults to `*`); grouped array sections are wrapped in `[ ]`. |
| `csv` | record | RFC 4180 record boundary (newlines inside quotes are respected). `csv.header=true` prepends the first record to every section; `csv.delimiter` (default `,`). Empty lines are skipped. Content is otherwise not interpreted. |
| `delimited` | regex (`delimited.pattern`, or `delimited.literal=true`) | Generic fallback for proprietary formats. `mode = START \| END \| SEPARATOR` controls whether a matching line begins a section, ends one, or is dropped. Each line is decoded (`delimited.charset`, default UTF-8) and tested with `find()` on its first `delimited.match-window` bytes (default 4096). Blank lines never start a section. In START mode, lines before the first match form their own section. |

### 6.3 Limits

These are configurable per format. A violation fails the file with error code `LIMIT_*`.

| Limit | Default |
|---|---|
| `max-file-bytes` | csv 10 GiB; others 1 TiB |
| `max-section-bytes` | 512 MiB (well under Parquet's 2 GiB value limit) |
| `max-sections-per-file` | unlimited |

A file over `max-file-bytes` is rejected **before ingestion**. It is moved to the encrypted local `failed/` fallback and is not stored in Iceberg.

## 7. Encryption and identifiers

### 7.1 Keys

| Key | Type | Held by | Used for |
|---|---|---|---|
| Sections KEK | AES-256 or RSA/EC wrap key | KMS or local keystore | wrapping batch data keys for `sections` |
| Source-files KEK | separate key | KMS or local keystore | `source_files` table |
| Failed-files KEK | separate key | KMS or local keystore | `failed_files` table |
| ID key | 256-bit HMAC secret | generated at first start, stored **wrapped** in a table property, unwrapped into memory at startup | section IDs, fingerprints, digests |
| Signing key | EC P-256 | KMS (sign API) or passphrase-protected PEM | signing queue messages (ES256) |
| Recipient public key(s) | EC P-256 / RSA | PEM files, SHA-256 fingerprint pinned in config | encrypting queue messages |
| Recovery public key | EC P-256 | PEM file | encrypting the local `failed/` fallback when storage or KMS is unreachable |

Using a separate KEK per table lets the retrieving system be given decrypt rights on `sections` only.

### 7.2 Section encryption (envelope)

1. One random 256-bit **batch data key (BDK)** per commit batch, wrapped once by the KEK. This means one KMS call per batch, not per section, which matters for millions of CSV rows.
2. A per-section key is derived as `HKDF-SHA256(BDK, info = "sdh-section" ‖ fileId ‖ seq)`. Exposing one section's key exposes nothing else.
3. The section plaintext is cut into **8 MiB chunks**. The chunk count is known up front, because the splitter reports the section's byte length.
4. Each chunk is **zstd-compressed, then encrypted** with AES-256-GCM, using a random 96-bit nonce and AAD = `fileId ‖ seq ‖ chunkIndex ‖ chunkCount`. The AAD prevents chunks being reordered, swapped between sections, or truncated.
   - The section ID is not part of the AAD, because it isn't known until the whole section has been hashed, yet every chunk row carries it. A single-chunk section (the common case) is read once into memory, hashed and encrypted. A larger section is read **twice**: one pass computes the ID, the second encrypts and writes each chunk, and re-checks the hash so a source that changed in between fails the batch. Memory stays at one 8 MiB chunk per section in flight, whatever `max-section-bytes` is.
   - Compressing before encrypting is required, because ciphertext doesn't compress. It is safe here because no attacker-controlled input is mixed into the plaintext.

### 7.3 Section ID

`sectionId = base32(HMAC-SHA-256(idKey, sectionPlaintextBytes))` (including prefix/suffix bytes).

- The ID is deterministic from content, so a repaired file resubmitted later produces the same IDs for unchanged sections. This is what downstream dedup relies on.
- It is keyed, so it cannot be used to guess low-entropy PHI such as a CSV row.
- Identical content in different files gets the same ID and is treated by downstream as a duplicate. This is accepted, and documented for CSV providers.
- The `idKey` is never rotated: rotating it would break dedup. Losing it breaks dedup only, not decryption.

### 7.4 KeyProvider SPI (built)

```java
public interface KeyProvider {
    WrappedKey wrap(String keyRef, byte[] dataKey);
    byte[] unwrap(WrappedKey wrapped);
    byte[] sign(String keyRef, byte[] digest);        // ES256, raw r‖s
    PublicKey publicKey(String keyRef);
}
```

Wrapping (the three KEKs) and signing are configured independently (`sdh.crypto.wrap-provider`, `sign-provider`), each one of the following. The KMS providers live in `connector-kms`, so downstream readers can use the same class to unwrap (§10.2).

| Provider | Wrap / unwrap | Sign (ES256) | Key ref | Recorded `kek_ref` |
|---|---|---|---|---|
| `local` | AES key wrap with AES-256 keys from a PKCS#12 keystore | encrypted PKCS#8 PEM (EC P-256) | keystore alias | alias |
| `aws-kms` | `Encrypt`/`Decrypt`, symmetric key, encryption context `sdh:purpose=sdh-batch-data-key` | `Sign`, `ECC_NIST_P256`, digest; DER → raw | key ID, ARN or alias | the key ARN KMS used (resolves aliases) |
| `gcp-kms` | `Encrypt`/`Decrypt`, symmetric key, AAD `sdh-batch-data-key`; CRC32C checked both ways | `AsymmetricSign`, `EC_SIGN_P256_SHA256` key version; DER → raw | `projects/…/cryptoKeys/<k>` (sign: `…/cryptoKeyVersions/<n>`) | the key version that encrypted |
| `azure-keyvault` | `wrapKey`/`unwrapKey`, `RSA-OAEP-256` on an RSA key (Key Vault wraps only with RSA; AES on Managed HSM) | `sign` ES256 on an EC P-256 key (already raw) | key identifier URL, versioned or not | versioned key ID |
| `vault-transit` | `encrypt`/`decrypt` on `aes256-gcm96`, associated data `sdh-batch-data-key` | `sign/<k>/sha2-256`, `prehashed`, `marshaling_algorithm=jws` on `ecdsa-p256` | transit key name | key name (`vault:v<n>:` in the value keeps the version) |

**Credentials:**
- **AWS:** the default provider chain; optional `sdh.crypto.aws.region` and `endpoint` (e.g. a VPC endpoint).
- **GCP:** Application Default Credentials; optional `sdh.crypto.gcp.endpoint` (a regional endpoint).
- **Azure:** `DefaultAzureCredential` (managed or workload identity, environment).
- **Vault:** `sdh.crypto.vault.address`, `mount` (default `transit`), optional `namespace`, plus a token, either `token` (from the environment) or `token-file`. The file is re-read on every request, so a Vault Agent sidecar can rotate it. Other Vault auth methods (Kubernetes, AppRole) are expected to run through Vault Agent.

**Startup checks** (`KeyMaterial`):
- **KEKs:** every KEK is probed with a wrap and unwrap of a throwaway key, so a missing key or permission stops startup with a message naming the KEK.
- **Signing key:** a KMS signing key must be EC P-256. A probe JWS is signed through the KMS and verified with the key's public half. Its SHA-256 fingerprint is logged (`crypto.signing_key`) for configuring readers.
- **Separate KEKs:** the sections KEK must differ from the archive KEKs.
- **Client lifecycle:** KMS clients are closed on shutdown.

**Signing through a KMS:** `KmsJwsSigner` (a Nimbus `JWSSigner`) sends the SHA-256 digest of the JWS signing input to `KeyProvider.sign`, so the private key never leaves the KMS. `MessageSealer` uses it like any other signer.

**Failures:** KMS errors carry the KMS's own message, which names keys and requests, never data. During ingest they count as storage failures (§11). If the failed-files KEK is unreachable, the file goes to the local fallback.

**Local mode:**
- **Keystore:** a PKCS#12 keystore whose entries are AES-256 secret keys, with entry aliases as the KEK refs. Plus PEM files for the signing key, the recipient key and the recovery key.
- **Encrypted PEMs:** encrypted PKCS#8 is read with the JDK alone, using PBES2 as written by `openssl pkcs8 -topk8 -v2 aes-256-cbc`; a test checks this against a real OpenSSL-generated key.
- **Protections:**
  - The service refuses to start if the keystore or signing key file is accessible by others, or writable or executable by group. Group read is allowed because Kubernetes secret volumes with an `fsGroup` are always at least 0440. Public key files are not checked.
  - Private PEMs must be encrypted PKCS#8, with the passphrase supplied from an environment variable or mounted secret.
  - Recipient PEM fingerprints must match the pinned values.

**Recipients:** messages are sealed to a single configured recipient. Rotation means switching `kid`; readers accept several keys meanwhile (§10.2). The recipient and recovery keys are always public PEMs: the connector only encrypts to them.

### 7.5 PHI hygiene

- **Logging:** no PHI, filenames or exception messages from parsers are ever logged. Exceptions are mapped to stable error codes (`PARSE_UNBALANCED_ELEMENT`, `LIMIT_SECTION_SIZE`, …), and only the code, exception class, offset and seq are emitted.
- **Camel:** body logging, tracing, message history and dead-letter body dumps are disabled.
- **Temp files:** no plaintext is ever written to temporary files.
- **Container:** runs as non-root with a read-only root filesystem; no heap dumps, core dumps, crash logs or JVM attach; and TLS everywhere (object store, catalog, KMS, brokers). See §14.1.

## 8. Storage

### 8.1 Catalog and object storage

These are selected at run time.

| Setting | Options |
|---|---|
| `sdh.storage.catalog.type` | `rest` (Apache Polaris, Nessie, Unity, AWS S3 Tables, …), `jdbc` (Postgres; simplest for self-hosting) |
| `sdh.storage.io` | `s3` (AWS S3, **SeaweedFS**, MinIO via endpoint override + path-style access), `gcs`, `adls` |

**Storage-side encryption:** server-side encryption (SSE-KMS / CMEK) is enabled where the store supports it, as a second layer beneath application encryption. For SeaweedFS, application encryption is the primary protection.

**SeaweedFS:** S3 identities must be configured (`-s3.config`). SeaweedFS 4.x rejects signed requests otherwise, and the AWS SDK always signs. Use `s3.path-style-access: true`.

**MinIO:** its community images are no longer published on Docker Hub or Quay, so it is no longer tested. It should still work as an S3 endpoint, but that's unverified.

### 8.2 Tables

The namespace is `sdh_ingest` (configurable). There are three tables, each with its own KEK and access policy.

**`sections`** has one row per chunk. A single table keeps commits atomic: Iceberg does not offer multi-table transactions on every catalog, so data and checkpoint must live in one table.

| Column | Type | Notes |
|---|---|---|
| section_id | string | keyed content hash |
| file_id | uuid | |
| seq | long | 0-based section sequence within file |
| chunk_index | int | |
| chunk_count | int | |
| source_offset | long | byte offset of section in source file |
| source_length | long | |
| format | string | |
| plaintext_length | long | before compression |
| ciphertext | binary | nonce ‖ AES-GCM ciphertext ‖ tag |
| bdk_wrapped | binary | wrapped batch data key (repeated per row; ~200 B per row, negligible) |
| kek_ref | string | key ID/version used to wrap |
| batch_id | uuid | |
| ingested_at | timestamptz | |

- **Partitioning:** `day(ingested_at)`. Sort order `file_id, seq, chunk_index`, so readers prune files using min/max statistics.
- **Parquet settings:** zstd level 1 for the whole table. Row group 128 MiB, target file size 512 MiB. The earlier plan to leave `ciphertext` uncompressed isn't possible: Iceberg sets the Parquet codec per table, not per column (spike finding). zstd passes incompressible ciphertext through at a small CPU cost and almost no size overhead.
- **`source_files`** holds the raw original in 8 MiB chunks, one row per chunk: `file_id`, `chunk_index`, `chunk_count`, `chunk_size`, `file_size`, `ciphertext`, `bdk_wrapped`, `kek_ref`, `batch_id`, `archived_at` (partition `day(archived_at)`, sort `file_id, chunk_index`). Chunk key = HKDF(BDK, fileId, seq 0); AAD = fileId ‖ 0 ‖ chunkIndex ‖ chunkCount.
- **Summary row:** values known only at the end go on one row with `chunk_index = -1`, written in the final archive commit: `original_name_enc` (the filename, encrypted with seq −1), `file_digest`, `digest_block_size`, `section_count`, `started_at` and `completed_at`. A file with a summary row is fully archived.
- **`failed_files`** has the same layout. Its summary row adds `failure_stage`, `failure_offset`, `failure_seq`, `error_code`, `failed_at` and `replaces_file_id`. `replaces_file_id` is not filled in yet; it waits for the admin resubmit flow.
- **Archive progress:** commits happen every `archive.commit-bytes` (default 512 MiB, whole blocks). Each one carries the archive checkpoint `sdh.archive.<connectorId>` = {fileId, nextBlock, chainHash, done, digest, finishedAt}, MAC'd like the main checkpoint. `ArchiveReader` restores a file (or its tail from an offset) a few chunks at a time and re-checks the digest.

### 8.3 Commit batching

A commit happens when the first of these thresholds is reached (all configurable):

- `commit.max-sections` (default 10,000)
- `commit.max-bytes` (default 512 MiB)
- `commit.max-interval` (default 30 s)
- end of file

Committing per section would create a snapshot per section and does not scale.

Each commit is **one fenced Iceberg commit** (§9.3) containing:
1. an `append` of the batch's data files;
2. an `updateProperties` that writes the checkpoint (§9), including the `publishedSeq` reached by the previous batch.

The fenced committer checks the lease epoch and commits with a strict compare-and-swap.

### 8.4 Maintenance (built)

Spark is not used. `TableMaintenance` (`connector-core` `maintenance/`) maintains one table without it, and `MaintenanceScheduler` (`file-connector`) runs it on all three tables every `maintenance.interval` (1 h).

**Scheduling:**
- **Between files only:** the scheduler runs only if it can take the writer lock without waiting. The inbound service holds that lock for each file, so maintenance never overlaps with ingest; a busy tick is skipped.
- **Bounded runs:** the compaction budget limits how long intake can be held up.
- **Errors:** a lease loss, unknown commit outcome or tampered checkpoint exits the process (status 70); anything else is logged and retried next interval.
- **Fencing:** every run starts with `FencedCommitter.verifyCurrent` (lease held, table epoch ours). Every maintenance commit goes through the fenced committer except snapshot expiry.

**Each run, per table:**
1. **Retention** (per table, off by default, at least 1 day). It deletes whole UTC days older than `now − retention`, using `deleteFromRowFilter` on the partition source column (`ingested_at`, or `archived_at` for the archive tables). That is metadata-only, because whole partitions match. Nothing to delete means no commit, so there are no empty snapshots. The PHI is then purged in the same run: Iceberg deletes a removed file only once the snapshot that removed it has expired, and the current snapshot never expires. So a fenced marker commit follows the delete, and every snapshot before the marker is expired, which deletes the files. Trade-off: no time travel to before a retention delete.
2. **Compaction (bin-pack):**
   - **What counts as small:** data files below `small-file-fraction` (0.75) of the table's target size (512 MiB), grouped by partition; a partition needs at least `min-input-files` (5) of them.
   - **Packing:** first-fit decreasing into bins of at most the target size.
   - **Rewriting:** each bin is rewritten by copying its rows unchanged (ciphertext included) into one new file, written unsorted because concatenating sorted files doesn't keep the sort order. Row counts are checked, and the swap commits through `RewriteFiles` with `validateFromSnapshot`.
   - **Budget:** it stops after `max-bytes-per-run` (8 GB).
3. **Snapshot expiry:** Iceberg core `expireSnapshots()`, keeping 7 days and at least `min-snapshots` (10).
   - It runs **standalone** through Iceberg's own commit, because only that path deletes the expired files (spike finding).
   - It runs right after `verifyCurrent`. The worst a stale instance could do is expire snapshots that are already past retention.
   - Files replaced by compaction are deleted when the compaction snapshot itself ages out, normally 7 days later.
4. **Orphan-file cleanup** (every `orphans.interval`, 24 h):
   - **Listing:** files under the table location are listed through the FileIO's prefix listing (`S3FileIO` supports it; the in-memory FileIO doesn't, so cleanup is skipped there).
   - **Reachability:** listed files are compared with everything reachable: metadata files and their log, manifest lists, manifests, data and delete files of every snapshot, and statistics files.
   - **Deletion:** unreferenced files older than `orphans.min-age` (3 days, at least 1 hour) are deleted.
   - **Safety stop:** if more than half of what's listed (and more than 20 files) looks orphaned, it refuses and logs `maintenance.orphans_refused`. A path-form mismatch (scheme, bucket alias) would otherwise look like every file is orphaned.

**Readers:** expiry doesn't break `connector-queue-reader` for older messages. When a message's snapshot has been expired, the reader refreshes and reads the current table, where compacted rows are unchanged; after a retention delete it reports the section as not found. One cost: a reader working through a backlog older than the snapshot window refreshes on every such message.

**Tests:** `TableMaintenanceTest` uses a JDBC catalog on embedded Postgres with a local-disk FileIO, so physical deletion is checked. It covers:
- compaction preserves every row byte-for-byte;
- only partitions with enough small files are compacted, within budget;
- replaced files are deleted once their snapshot ages out;
- retention drops only whole old days and removes their files at once, with no empty snapshot when there's nothing to delete;
- retention works on archive tables;
- orphan cleanup deletes old strays but not recent ones or anything referenced;
- a fenced-out instance changes nothing.

`FullStackIT` repeats compaction, orphan cleanup and retention purging against SeaweedFS (§15.1).

## 9. Checkpointing, resume and single instance

### 9.1 Where progress lives

Progress is stored in **Iceberg table properties**, written in the same atomic commit as the data. Data and progress therefore cannot disagree, and they are protected by the same catalog access controls as the PHI. No local database is used. Only the lease's liveness lives outside Iceberg, in Postgres (§9.3).

```json
sdh.checkpoint.file-connector = {
  "fileId": "...", "fingerprint": "...", "state": "SPLITTING",
  "resumeAt": "123456789:1024,5120", "nextSeq": 40211,
  "committedSeq": 40210, "publishedSeq": 39000,
  "format": "ccda", "fileSize": 987654321,
  "startedAt": "...", "mac": "HMAC(idKey, \"sdh-checkpoint\" ‖ 0 ‖ canonical JSON without mac)"
}
```

- **`resumeAt`:** the encoded `ResumePoint` of the last committed section (§6.1): the next offset, then any context offsets. Offsets only, no content.
- **States:** see §4.1. A `FAILING` or `FAILED` checkpoint also carries `failureStage`, `errorCode`, `failureOffset` and `failedAt`. The pipeline refuses a new file while another is `SPLITTING`, `ARCHIVING` or `FAILING`.
- **Labelled MACs:** checkpoint MACs and fingerprints prefix a label, so neither can ever equal a section ID made with the same key.
- **Tamper detection:** the checkpoint is MAC'd with the ID key. If it fails verification, the service stops with `CHECKPOINT_TAMPERED` rather than guessing.
- **Archive phase:** uses an equivalent property on `source_files`, recording `nextBlock`, `chainHash` and `bytesArchived`.

### 9.2 Resume and publish protocol

```
loop per batch N:
  fencedCommit(data_N + checkpoint{resumeAt, nextSeq, committedSeq = last seq of N,
                                   publishedSeq = last seq of N-1 acknowledged by the broker})
  publish messages for seq (publishedSeq, committedSeq], waiting for broker ack
end of file:
  fencedCommit(checkpoint{state = ARCHIVING, publishedSeq = committedSeq})   # no data
```

Publish progress is recorded in the **next** batch commit, not in a separate commit, which halves the number of commits. The end-of-file commit carries no data, so the fenced committer adds an empty marker snapshot to it (§9.3).

On restart:
1. Read the checkpoint.
2. Rebuild messages for any committed-but-unpublished seq range from table metadata columns. Decryption isn't needed for this.
3. Republish those messages, then resume splitting at `resumeAt`.

**Duplicates:** at most one batch can be republished after a crash. That is the same worst case as before, since a crash part-way through publishing a batch already meant republishing it. Broker dedup (§10.3) removes the duplicates where the broker supports it, and downstream dedup handles the rest.

**Unknown commit outcome:** if a commit ends with Iceberg's `CommitStateUnknownException`, the process stops and restarts from the checkpoint. The checkpoint shows whether that commit happened.

**Retrying a commit:** other transient storage errors (I/O, `CommitFailedException`, 5xx, JDBC errors; an allow-list) are retried with backoff. Before each retry the pipeline re-reads the checkpoint, and if it already shows the batch the earlier attempt landed after all, so the batch is never appended twice.

### 9.4 Pipeline (`connector-core` `pipeline/`, built)

`IngestPipeline.split(source)` runs the split phase for one file and returns once every section is committed and published, with the checkpoint in `ARCHIVING`. The connector app supplies the source, the sections table, the `FencedCommitter`, keys, the sealer and a `MessagePublisher`.

1. **Identify:** fingerprint the source. If it matches a `SPLITTING` checkpoint, resume; if it matches an `ARCHIVING` one, return straight away; otherwise assign a UUIDv7 and commit a fresh checkpoint before anything else.
2. **Announce:** publish `file.started` (again on resume; message ID dedup handles the repeat), then republish any committed-but-unpublished range.
3. **Split and encrypt:** single-chunk sections are hashed and encrypted on the worker pool, with at most `4 × workers` in flight; larger sections are processed inline (§7.2). Rows are written in seq order.
4. **Commit:** at the first threshold (§8.3), data file + checkpoint in one fenced commit. The commit first waits for the previous batch's publishing and records its last seq as `publishedSeq`.
5. **Publish:** the batch's messages are sealed on the worker pool and sent in seq order on a background thread while the next batch is split. Failed sends are retried one by one.
6. **End of file:** wait for publishing, then a data-free commit to `ARCHIVING`.

The ID key is created or unwrapped by `IdKeys` (table property `sdh.id-key`). Each audit log line carries only IDs, seqs, offsets and counts.

### 9.3 Single-instance lease and fencing

The design follows the standard pattern for single-writer leases:
- a lease with a TTL in a store that supports compare-and-set and judges expiry by its own clock;
- renewal well before expiry;
- self-fencing on the holder's monotonic clock;
- a **fencing token** that the storage layer checks on every write.

Self-fencing alone isn't enough. A process can pause (GC, VM suspend, container freeze) between checking its lease and committing, so the storage commit must also reject a stale holder.

The earlier draft kept the lease in `sections` table properties, renewed every 30 s. That meant a metadata commit (a new `metadata.json`) every 30 s, around the clock, mixed in with the PHI data. It was replaced as follows.

**Two parts:**

| Part | Where | Job |
|---|---|---|
| Liveness | Postgres row in `sdh_lease` (`JdbcLeaseStore`) | Who holds the lease and until when. Renewals touch only this row and write nothing to Iceberg |
| Fencing token | `sdh.lease.epoch` property on each ingest table (`FencedCommitter`) | Lets a commit from a stale holder be rejected by the catalog itself |

**Lease store (`JdbcLeaseStore`):**
- One row per lease name: `owner`, `epoch`, `expires_at`.
- All expiry checks use the database clock (`clock_timestamp()`), so client clock skew can't create two holders.
- **Acquire** is one atomic upsert. It inserts a new row with epoch 1, or takes over an *expired* row with `epoch + 1`. A live row is left alone, and the caller gets `LEASE_HELD` and exits non-zero. Concurrent acquirers are serialised by the row lock, and the spike test confirms exactly one of 16 wins.
- **Renew** succeeds only while `owner` and `epoch` still match. **Release**, on clean shutdown, sets `expires_at` to now so a restart doesn't wait for the TTL. The row is never deleted, so the epoch only ever increases.
- **Placement:** a JDBC-catalog deployment can use the catalog's own Postgres. A REST-catalog deployment needs a small Postgres for this. The `LeaseStore` SPI leaves room for other stores with the same guarantees (for example a Kubernetes `Lease`); none is planned yet.

**Holder (`LeaseKeeper`):**
- **Timing:** TTL 60 s, renewal every 15 s, safety margin 10 s. These defaults are configurable; the code requires `2 × renew + margin < TTL`, so two renewals can fail in a row before anything happens.
- **Local validity:** `(time the last successful renewal request was sent) + TTL − margin`, measured on the monotonic clock. Counting from when the request was sent, not when the reply arrived, means a slow reply can only shorten the window.
- **Self-fencing:** before every commit, the pipeline calls `checkHeld()`. If the window has passed, or the store reports a new owner, the lease is lost (`lease.lost` audit event, `LEASE_LOST`). The pipeline stops, the process exits non-zero, and the orchestrator restarts it.
- **After a crash:** a successor waits at most the TTL (60 s) before it can take over.

**Fencing (`FencedCommitter`):**
1. **Establish:** after acquiring the lease, the instance commits its epoch to each ingest table. This commit fails if the table already shows a higher epoch. From that point, every older holder is fenced out.
2. **Every commit** (data batches, checkpoint updates, archive progress, compaction):
   - refresh the table and require `sdh.lease.epoch` = our epoch;
   - build the change as a transaction on that exact metadata;
   - commit it with `TableOperations.commit(start, new)`, a strict compare-and-swap;
   - if the commit fails, refresh and check the epoch again, which identifies a stale holder.
3. **Why not `Transaction.commitTransaction()`:** Iceberg's normal commit refreshes the table and quietly re-applies our changes on top of whatever metadata is newest. A stale instance's checkpoint would then land on top of its successor's. The spike includes a control test that shows this happening on both catalog types.
4. **Every commit adds a snapshot:** if a change doesn't add one, for example a properties-only update, the committer adds an empty marker append.
   - A REST catalog only checks a commit against concurrent changes through requirements such as `assert-ref-snapshot-id`, which is only sent when `main` moves. A properties-only commit carries no such requirement, so the server would accept it from a stale instance.
   - JDBC and Hive catalogs compare the whole metadata location, so they are safe either way. The marker keeps behaviour identical across catalogs.
5. **Tested:** a stale holder is rejected when it commits after its successor established, and in the race where the successor establishes between the stale holder's check and its commit. Both are tested on JDBC (embedded Postgres) and on REST (Iceberg's reference REST request handling, run in-process).
6. **Internals:** this relies on Iceberg's public `BaseTransaction.startMetadata()`, `currentMetadata()` and `underlyingOps()`. They are not formally part of the API, so the fencing tests run on every Iceberg upgrade.

## 10. Messaging

### 10.1 Message types

Every message is **JWS (ES256, signed by this service, `typ: sdh-msg+jws`) nested inside JWE (`ECDH-ES+A256KW` for EC recipients or `RSA-OAEP-256` for RSA, always `A256GCM`)** addressed to the recipient's public key. Both headers carry a `kid`, so keys can be rotated. Every payload carries `"v": "<major>.<minor>"`, the contract version (§10.2).

| Type | When | Payload (inside encryption) |
|---|---|---|
| `file.started` | file picked up (or resumed with a new run) | fileId, format, fileSize, startedAt, connectorId |
| `section.stored` | after batch commit | messageId, fileId, sectionId, seq, sourceOffset, sourceLength, format, plaintextLength, chunkCount, storage ref (catalog, namespace, table, ingested date, snapshotId, kekRef) |
| `file.completed` | after archive commit | fileId, sectionCount, fileSize, fileDigest, startedAt, completedAt |

- **No `file.failed` message:** failure is reported through OpenTelemetry only, as agreed. Downstream can detect an unfinished file as a `file.started` with no matching `file.completed`.
- **Change from earlier discussion:** the "duplicates skipped" count is **not** included in `file.completed`. Counting it would require checking every section against all previously stored IDs, which is too expensive at this scale. Duplicate handling stays downstream.
- **Unencrypted broker metadata** is limited to: `messageId`, `type`, `fileId` (used for ordering), and the content type `application/jose`.
- **Message IDs:**
  - `section.stored`: `messageId = sectionId`.
  - `file.started`: `fs-<fileId>`.
  - `file.completed`: `fc-<fileId>`.

### 10.2 Consuming messages: `connector-queue-reader`

Consumers never implement the envelope, encryption or table layout themselves. They use the `connector-queue-reader` library, so those details can change inside the connector without breaking any consumer.

```java
QueueReader reader = QueueReader.builder()
    .recipientKey("downstream-2026", recipientPrivateKey)      // several allowed during rotation
    .trustedSigner("connector-2026", connectorSigningPublicKey)
    .catalog(icebergCatalog)                                     // consumer supplies catalog + FileIO
    .expectedCatalogName("prod")                                 // optional
    .keyProvider(kmsKeyProvider)                                 // unwrap rights on the sections KEK only
    .build();

ConnectorMessage m = reader.decode(brokerMessageBody);           // FileStarted | SectionStored | FileCompleted
if (m instanceof SectionStored s) {
    try (InputStream in = reader.openSection(s)) { ... }         // or reader.readSection(s, maxBytes)
}
```

**What the library does:**
- **Decoding:** decrypts and verifies messages. Only the algorithms listed in §10.1 are accepted, and keys are chosen by `kid` from fixed lists, so a message header can't pick a weaker algorithm or key. Rejections give a `MessageRejectedException.Reason`, never payload content.
- **Section lookup:** queries `sections` for the section's `file_id`, `seq` and `ingested_at` day, which allows partition pruning. It refreshes its cached table if the message names a snapshot it hasn't seen yet.
- **Keys:** unwraps each batch data key once and caches it by `batch_id`, then derives the section key.
- **Streaming:** decrypts chunk by chunk, holding one 8 MiB chunk in memory plus any chunks the scan returns early. Each row is checked against the message (section ID, chunk count, length). The stream fails if a chunk is missing, duplicated, out of range, from another batch, or the wrong length.
- **Broker-neutral:** the consumer passes in the message body from whatever broker client it uses.

**Versioning:** a reader accepts any message with the same contract major version and ignores unknown fields, so minor versions only add optional fields. A new major version is rejected with `UNSUPPORTED_VERSION`. Before the connector starts writing a new major version, consumers must upgrade to a reader that supports it.

**Dependencies:** `connector-contract`, Iceberg core/data/parquet and the six-jar Hadoop set. There is no Camel, no Spring and no pipeline code. Consumers add their catalog and FileIO modules, for example `iceberg-aws`.

### 10.3 Brokers

`sdh.queue.type` selects the adapter, and `sdh.queue.uri` is a full Camel endpoint URI, so any component option can be set at run time. All client libraries are included in the single build.

| `type` | Camel component | Ordering key | Broker-side dedup |
|---|---|---|---|
| `kafka` | camel-kafka | record key = fileId | idempotent + transactional producer |
| `sqs` | camel-aws2-sqs | MessageGroupId = fileId (FIFO) | MessageDeduplicationId (FIFO queues, 5 min) |
| `pubsub` | camel-google-pubsub | orderingKey = fileId | none; `messageId` attribute |
| `servicebus` | camel-azure-servicebus | SessionId = fileId | duplicate detection on MessageId |
| `eventhubs` | camel-azure-eventhubs | partitionKey = fileId | none; `messageId` property |
| `artemis` | camel-jms + Artemis JMS client | JMSXGroupID = fileId | `_AMQ_DUPL_ID` |
| `rabbitmq` | camel-spring-rabbitmq | single queue / consistent-hash exchange | `message_id`; optional `x-deduplication-header` for the dedup plugin |

**What each adapter sets** (`QueueType`):
- **Common metadata, every adapter:** `messageId`, `type`, `fileId` and `contentType` (`application/jose`). These are the only unencrypted values; the body is the JWE compact serialisation, sent as ASCII text.
- **`kafka`:** header `CamelKafkaKey` = fileId.
- **`sqs`:** exchange property `CamelAwsMessageGroupId` = fileId, and the exchange ID = messageId.
- **`pubsub`:** `CamelGooglePubsubOrderingKey` = fileId; the metadata travels as attributes.
- **`servicebus`:** `SessionId`, `MessageId`, `ContentType`, and the metadata as application properties.
- **`eventhubs`:** `PartitionKey` = fileId; the metadata isn't carried.
- **`artemis`:** `JMSXGroupID` = fileId and `_AMQ_DUPL_ID` = messageId.
- **`rabbitmq`:** `MessageId` and `ContentType`.
- **`camel`:** any endpoint with the common metadata only. Used for tests and for components without an adapter.

**Options added when the URI doesn't set them** (an explicit setting always wins):
- **`kafka`:** `requestRequiredAcks=all&enableIdempotence=true`.
- **`sqs`, for `*.fifo` queues:** `messageGroupIdStrategy=usePropertyValue&messageDeduplicationIdStrategy=useExchangeId`.
- **`pubsub`:** `messageOrderingEnabled=true`. Set a regional `pubsubEndpoint` as well.
- **`rabbitmq`:** `confirm=enabled`. Spring AMQP also needs `spring.rabbitmq.publisher-confirm-type=correlated`.

**Broker connections:**
- **Artemis:** a JMS connection factory is created from `sdh.queue.artemis.*`.
- **Other brokers:** connections come from the URI or the usual Camel and Spring component settings.

**How messages are sent:**
- **Order and acks:** `CamelMessagePublisher` calls the endpoint's async producer directly from the pipeline's single publish thread, so messages reach the client library in seq order and are acknowledged asynchronously. `ProducerTemplate.asyncSend` would hand them to a thread pool and lose the order.
- **Service Bus uses the Azure SDK:** Camel 4.22's Service Bus producer ignores per-message SessionId, MessageId and ContentType headers; it only takes a fixed session ID from the endpoint. `ServiceBusPublisher` therefore sends through the Azure SDK sender that Camel already ships. Connection settings are still read from the configured Camel URI, so configuration is unchanged.
- **One send at a time for the Azure brokers:** the Azure AMQP client, used for both Event Hubs and Service Bus, delivered sends that were in flight together out of order. The first message arrived, then the rest reversed. For these two `QueueType.keepsOrderWhenPipelined()` is false, and `Publishers.create` wraps them in `SequentialPublisher`: each send waits for the previous acknowledgement. Throughput on those brokers is bounded by round-trip time.
- **Verified against real brokers:** for every adapter the integration tests check all of the following (§15):
  - every message delivered;
  - each file's messages in order;
  - the ordering key;
  - the allowed metadata;
  - dedup, where the broker has it: Artemis `_AMQ_DUPL_ID`, SQS FIFO and Service Bus MessageId.
- **Done means durable:** a send counts as done only when the producer reports success.
- **Message size:** messages are small, around 2–4 KB after JOSE encoding, well under the smallest limit (SQS 256 KB).

## 11. Failure handling

| Situation | Action |
|---|---|
| Transient errors (KMS, object store, catalog, broker) | Retry with exponential backoff and jitter (`retry.max-attempts`, `retry.max-backoff`). The pipeline pauses; the file stays in its current state |
| Retries exhausted, or a non-retryable error (parse error, limit exceeded) | Fail the file (below) |
| Crash or kill at any point | Resume from the checkpoint (§9) |

**Classifying a failure:**

| Cause | Stage | Error code |
|---|---|---|
| File over `max-file-bytes` (checked before any checkpoint or `file.started`) | `intake` | `LIMIT_FILE_SIZE` |
| Splitter error | `split` | the splitter's `PARSE_*` / `LIMIT_*` code and offset |
| Broker didn't acknowledge after retries | `publish` | `PUBLISH_FAILED` |
| Reading the source failed | `split` or `archive` | `SOURCE_READ_FAILED` |
| Storage, catalog or KMS still failing after retries | `split` or `archive` | `STORAGE_FAILED` |

These never fail a file. They stop the process, which then restarts from the checkpoint: lease loss, `CommitStateUnknownException`, a tampered checkpoint, or a broken invariant (`IllegalStateException`).

### 11.1 Completing a file

After the split phase: archive the original to `source_files`, publish `file.completed`, write `completed/<fileId>.json`, delete the original, then commit `COMPLETED`. The original is deleted only after its archive is committed, and every step can be repeated (message ID dedup, records overwritten, deleting a missing file is fine). So a crash anywhere just repeats the remaining steps, and once the archive is done the source is no longer needed. A failure while archiving fails the file through §11.2, which archives it to `failed_files` instead.

### 11.2 Failing a file
1. Commit the decision: checkpoint `FAILING` with stage, code and offset. If storage is down this can't be recorded; the file still goes to the local fallback.
2. Already-committed and published sections stay where they are, each tied to its `file_id`.
3. The whole original is archived into `failed_files`. It goes to the local fallback `failed/<fileId>.enc` instead (§11.3) when the file is over `max-file-bytes`, when `FAILING` couldn't be recorded, or when the archive fails (storage or KMS unreachable). If even the fallback can't be written, the original is kept and the error propagates.
4. Announce every committed section: publish `section.stored` for any committed section not yet acknowledged. Once a file is `FAILED` no restart will republish it, so it must happen here. This is skipped when the broker itself is the failure, and a `publish.unannounced` range is logged instead. Also, when splitting stops, the in-flight publish of the last committed batch is now awaited rather than cancelled. A test that forces this race shows that, without the fix, the last batch was stored but never announced.
5. Write `failed/<fileId>.json`, delete the original, log `file.failed` (fileId, stage, offset, last committed seq, error code, where it was archived), and commit `FAILED`. No `file.failed` message is published (§10.1); the `sdh.audit.file.failed` event and `sdh.files.failed` metric report it (§12). Attempt count isn't recorded yet.

### 11.3 Local fallback format

`RecoveryEnvelope` (`connector-core` `crypto/`), streaming and any size:
- **Header:** `"SDHFB1"` ‖ fileId ‖ SHA-256(recovery public key) ‖ an ephemeral EC public key.
- **Key:** HKDF-SHA256 over the ECDH of the ephemeral and recovery keys, salted with the header hash.
- **Records:** each is `u32 length ‖ nonce ‖ AES-256-GCM(zstd(plaintext)) ‖ tag`, with AAD = header hash ‖ record index ‖ last-record flag.
- **Contents:** record 0 is JSON metadata (original name, size). The data follows in 8 MiB records.
- **Integrity:** truncation, reordering, splicing and the wrong key are all detected.

`DirectoryOutcomeRecorder` writes it to a hidden temp file and renames it into place only after a successful write and sync. Records are written the same way, with owner-only permissions.

### 11.4 Operator tools (built)

`java -jar file-connector.jar admin <command> …`; in the image, `sdh-admin <command>` (§14.1). The commands use the connector's own configuration (application.yml, environment, `SPRING_APPLICATION_JSON`; `--a.b=c` arguments override it) but start none of the connector: no Camel, no lease, and nothing is written to the tables or the lease store. Tables are loaded, never created, and the ID key is read, never created. So they are safe to run beside the running connector.

| Command | Does |
|---|---|
| `restore --file-id X --to <dir> [--from-offset N \| --unprocessed] [--table failed\|source] [--original-name]` | Decrypts an archived file from `failed_files` (looked up first) or `source_files`. `--unprocessed` starts after the last committed section of a failed file, found from the summary row's `failure_seq` and that section's source range: the part that never reached downstream. A tail repeats no headers or envelopes before that point (an X12 interchange header, a CSV header row), so it may need them added before resubmission. For a failed file, the stage, error code, offset and last committed seq are printed first |
| `decrypt-fallback --file failed/<id>.enc --key <recovery.pem> --to <dir> [--original-name]` | Decrypts a local fallback file (§11.3). The recovery private key must be an encrypted PKCS#8 PEM, with owner-only or group-read permissions as for the signing key (§7.4); its passphrase comes from `$SDH_RECOVERY_PASSPHRASE`, never an argument. Needs no catalog, lease or KMS |
| `status` | Shows the lease (holder, epoch, time left, read with the database clock), the checkpoint (file in progress, state, percentage split, sections committed and published, failure details), each archive table's progress, and the inbound backlog. Each part is shown on its own, so an unreachable KMS still leaves the lease and backlog visible; the exit status is 1 if any part couldn't be read |

**Output safety:**
- Decrypted output goes to a hidden temporary file with owner-only permissions. It is renamed into place only after the digest (restore) or envelope integrity (fallback) has been verified, and deleted otherwise, so no unverified plaintext is left behind.
- An existing file is never overwritten.
- Output is named `<fileId>` (or `<fileId>.from-<offset>`). `--original-name` uses the archived name instead, reduced to its last path element so it can't write outside the directory. The command then doesn't print that name, because command output may end up in logs.
- Every restore checks the digest, including partial ones: all chunks are decrypted either way.

**Audit:**
- `$SDH_OPERATOR` is required (letters, digits, `. _ @ + -`).
- Every run emits `sdh.audit.admin.<command>` with `sdh.operator`, plus the file ID, table, offset and bytes as they become known, and `result=ok|failed` with the exception class.
- Events go to the console and, when `sdh.telemetry.otlp.endpoint` is set, to the collector. Each event is exported straight away and flushed before exit.
- `sdh.operator` was added to the attribute allow-list (§12).

**Exit status:** 0 done, 1 failed, 2 usage error (an unknown option is an error, so a typo can't change what is restored).

**Not built yet:** recording `replaces_file_id` when a repaired file is resubmitted. The connector gives a resubmitted file a new fileId like any other, and nothing yet carries the link from the operator to the intake. One option is a marker file next to the resubmitted file, read at intake. Until then, the operator records the link in the ticket and in the restore's audit event.

## 12. Observability and audit (OpenTelemetry, built)

**Structure:**
- **`connector-core`** emits through `Telemetry` (`telemetry/`), on the OpenTelemetry **API** only. Until an SDK is installed everything is a no-op, so libraries and tests need nothing.
- **`file-connector`** installs the SDK that `spring-boot-starter-opentelemetry` builds and exports over OTLP/HTTP (§12.1).

**Traces:** `ingest.file` (root, one per file) contains:
- **`split.batch`** (file ID, format, last seq, count), which contains:
  - `storage.commit` (table, count);
  - `publish.batch` (count). This runs on the publishing thread, and its trace context is carried across, so it stays a child of its batch.
- **`archive`** (file ID, table).
- **`publish.batch`** for `file.started` and `file.completed`.

Other spans:
- `ingest.recover` is the root when finishing a file without its source.
- `maintenance` (table) runs per table and pass (§8.4).

Root spans carry `sdh.file_id` and `sdh.result`. A failed file's root span has status ERROR with `sdh.error_code` and `sdh.stage`. **Exceptions are never recorded on spans:** their messages could hold PHI, so only the exception class name (`sdh.reason`) is kept. Sampling is 100%: one trace per file is cheap.

**Audit events:** OTel log records with `event.name = sdh.audit.<event>`, also written to the console log (`sdh.audit` logger).
- **File events:**
  - `file.discovered` (bytes);
  - `file.started` (file ID, format, bytes);
  - `file.resumed` (next seq, offset);
  - `batch.committed` (last seq, offset, count);
  - `publish.republish`;
  - `file.split`;
  - `file.archived` (table, bytes);
  - `file.completed` (sections, bytes);
  - `file.failing` and `file.failed` (stage, error code, offset, last seq, where archived);
  - `archive.unavailable`, `publish.unannounced`, `file.failed_unarchived`, `file.abandoned`.
- **Lease and process events:**
  - `lease.acquired`, `lease.fenced_table` (epoch, table), `lease.lost` (reason `EXPIRED`, `TAKEN_OVER` or `FENCED`), `lease.released`;
  - `connector.started`, `connector.fatal` (reason).
- **PHI deletion:** `maintenance.retention` (table, files, first day kept) and `maintenance.orphans_deleted`.
- **Operator events** (`admin.*`) come with the admin commands.

**Metrics:**

| Metric | Type | Attributes |
|---|---|---|
| `sdh.files.in_progress` | gauge | |
| `sdh.file.progress_ratio` | gauge | bytes split / file size of the current file |
| `sdh.inbound.oldest_file_age` | gauge (s) | waiting files only (not hidden files or `.done` markers) |
| `sdh.bytes.read` | counter (By) | stage: `split`, `archive` |
| `sdh.sections.stored` | counter | format |
| `sdh.sections.published` | counter | |
| `sdh.files.completed` | counter | |
| `sdh.files.failed` | counter | error code, stage |
| `sdh.commit.duration` | histogram (s) | table |
| `sdh.publish.duration` | histogram (s) | |

**PHI defence, two layers:**
1. **At the source:** every attribute goes through `Fields`, which only accepts keys from the `Attr` enum (`sdh.file_id`, `sdh.format`, `sdh.state`, `sdh.stage`, `sdh.error_code`, `sdh.result`, `sdh.reason`, `sdh.seq`, `sdh.offset`, `sdh.count`, `sdh.bytes`, `sdh.epoch`, `sdh.table`, `sdh.archive`), with strings or numbers. There is no way to attach a filename or content.
2. **At export:**
   - `AllowListSpanExporter` keeps only those keys, whatever produced the span, and drops span events (exception events) and link attributes.
   - `AuditOnlyLogRecordExporter` exports only `sdh.audit.*` records, with allow-listed attributes, and replaces the body with the event name. Free-text log lines from any library never leave the host.
   - Metrics are created only through `Telemetry`, so their attributes are allow-listed by construction.

### 12.1 Export configuration

- **Endpoint:** `sdh.telemetry.otlp.endpoint` is the collector's OTLP/HTTP base URL; `/v1/traces`, `/v1/metrics` and `/v1/logs` are appended. `sdh.telemetry.otlp.headers` adds request headers (e.g. auth); `timeout` is 10 s; `metric-interval` is 60 s. Unset means no exporters: telemetry stays in-process and audit events still reach the console.
- **What's switched off:** Boot's own OTLP trace and log exporters (`management.tracing.export.otlp.enabled`, `management.logging.export.otlp.enabled`) and the Micrometer OTLP metrics registry (`management.otlp.metrics.export.enabled`). Every export goes through the filters above. JVM metrics via Micrometer aren't exported.
- **Meter provider:** Boot builds no OpenTelemetry meter provider (its metrics go through Micrometer), so `file-connector` defines an `SdkMeterProvider` with a periodic OTLP reader. Without it, metrics recorded through the OTel API would go nowhere.

**Tests:**
- **`FileProcessorTest`:** installs an SDK with in-memory exporters. It checks the span tree (including `publish.batch` parented across threads), the error code on a failed file, the audit events and the metric counts, and that no key outside the allow-list and no filename or record content appears anywhere.
- **`ExportFilterTest`:** covers both export filters.
- **`FullStackIT`:** exports to a real `otel/opentelemetry-collector-contrib:0.162.0`. It checks that traces, metrics and audit events arrive, and that none contains the PHI marker or a filename.

## 13. Performance

| Pipeline stage | Concurrency |
|---|---|
| Reader | 1 thread, sequential; `FileChannel` with 4 MiB direct buffers |
| Splitter | 1 thread |
| Hash, compress, encrypt | worker pool sized to CPU cores, at most 4 × workers sections in flight; sections over one chunk run inline on the writer thread |
| Iceberg writer | 1 thread, writes in seq order |
| Publishing | one background thread per pipeline sends in seq order; sealing runs on the worker pool; the adapter's futures carry concurrency |

- **Reads per file:** each file is read **twice**, once to split and once to archive. This trade keeps both phases simply resumable, and the second read is sequential I/O.
- **Throughput:** expected to be bound by disk read speed or object-store upload speed for large C-CDA and X12 files, and by HMAC/encryption for very many small CSV records.

## 14. Configuration

Bound to `ConnectorProperties`. Secrets come from environment variables, which Spring maps automatically (e.g. `SDH_CRYPTO_KEYSTORE_PASSWORD`, `SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE`, `SDH_LEASE_JDBC_PASSWORD`), or from a `configtree:` mount. Values shown are defaults where they exist.

```yaml
sdh:
  connector-id: file-connector
  encrypt-workers: 0              # 0 = one per CPU core
  file:
    inbound-dir: /data/inbound
    read-lock: changed            # changed | marker (<name>.done)
    min-age: 30s
    poll-interval: 5s
    include: "*"                  # Ant-style
  split:
    format: ccda                  # ccda | xml | x12 | fhir | csv | delimited | <plugin>
    boundary: ClinicalDocument    # optional; format default
    options:                      # format options from §6.2, nested keys become dotted names
      namespace: urn:hl7-org:v3
      csv: { header: true }       # → csv.header
      fhir: { ndjson-group-size: 0 }
      delimited: { pattern: "^MSH\\|", mode: START }
    limits: { max-file-bytes: 1TB, max-section-bytes: 512MB }   # omitted = format defaults
  storage:
    catalog-name: sdh             # also sent in messages
    namespace: sdh_ingest
    catalog:                      # Iceberg catalog properties, passed through unchanged
      type: rest                  # rest | jdbc
      uri: https://polaris:8181/api/catalog
      warehouse: sdh
      io-impl: org.apache.iceberg.aws.s3.S3FileIO
      s3: { endpoint: "https://seaweedfs:8333", path-style-access: true }
      client: { region: us-east-1 }
    commit: { max-sections: 10000, max-bytes: 512MB, max-interval: 30s }
    archive: { commit-bytes: 512MB }
  lease:
    name: file-connector
    jdbc: { url: "jdbc:postgresql://postgres:5432/sdh?sslmode=verify-full", username: sdh, table: sdh_lease }
    ttl: 60s
    renew-interval: 15s
    safety-margin: 10s
  crypto:
    wrap-provider: local          # local | aws-kms | gcp-kms | azure-keyvault | vault-transit (§7.4)
    sign-provider: local          # same choices, independently
    keystore: { path: /run/secrets/keks.p12 }               # local only; password: SDH_CRYPTO_KEYSTORE_PASSWORD
    keys:
      sections-kek: sections-kek/1                          # keystore alias, or the KMS key ref
      source-files-kek: source-files-kek/1
      failed-files-kek: failed-files-kek/1
      signing: { kid: connector-2026, pem: /run/secrets/signing.pem }   # local; passphrase: env
      # signing: { kid: connector-2026, key-ref: "arn:aws:kms:eu-west-1:123456789012:key/…" }   # KMS
    aws: { region: eu-west-1 }                              # aws-kms; credentials: default chain
    gcp: { endpoint: "europe-west1-kms.googleapis.com:443" }  # gcp-kms; credentials: ADC
    vault: { address: "https://vault:8200", token-file: /vault/secrets/token, mount: transit }   # vault-transit
      recipient: { kid: downstream-2026, pem: /run/secrets/recipient.pem, sha256: "ab12..." }
      recovery-public: /run/secrets/recovery-public.pem
  queue:
    type: kafka                   # kafka | sqs | pubsub | servicebus | eventhubs | artemis | rabbitmq | camel
    uri: "kafka:sdh.sections?brokers=kafka:9093&securityProtocol=SSL"
    artemis: { broker-url: "tcp://artemis:61617?sslEnabled=true", username: sdh }
  retry: { max-attempts: 8, initial-backoff: 200ms, max-backoff: 30s }
  telemetry:                      # §12.1; unset endpoint = no export
    otlp: { endpoint: "https://otel-collector:4318", timeout: 10s }   # headers: { Authorization: ... } from env
    metric-interval: 60s
  maintenance:                    # §8.4; runs between files
    enabled: true
    interval: 1h
    snapshot-retention: 7d
    min-snapshots: 10
    compaction: { enabled: true, small-file-fraction: 0.75, min-input-files: 5, max-bytes-per-run: 8GB }
    orphans: { enabled: true, interval: 24h, min-age: 3d }
    retention: { sections: 30d, source-files: 365d, failed-files: 365d }   # unset = keep forever (default)
```

Compared with the earlier sketch:
- split options moved under `split.options`;
- object storage settings are plain Iceberg catalog properties, replacing `storage.io`;
- the signing key and recipient are single objects, not a list;
- retention moved to `maintenance.retention`; `lease.store` isn't read yet.

### 14.1 Container image (built)

`file-connector/Dockerfile` builds the image from the packaged jar. `scripts/build-image.sh` runs Maven and then `docker build` against the minikube VM's Docker daemon, tagging `sdh/file-connector:<version>` (override with `SDH_IMAGE`; extra arguments go to `docker build`). Plain `docker build file-connector` works too.

- **Base:** `eclipse-temurin:25-jre-noble`, with `25-jdk-noble` for the build stage. Production builds should pin both by digest (`--build-arg JRE_IMAGE=eclipse-temurin@sha256:…`).
- **Layers:** Spring Boot's `-Djarmode=tools extract --layers`, copied as four layers with dependencies first, so a code change rebuilds a few hundred KB rather than 200 MB. The image is about 513 MB.
- **User:** uid/gid 10001 (`sdh`), with no home directory or login shell. Everything under `/app` is root-owned and read-only to it; only `/data/inbound` belongs to it.
- **Operator commands:** `/usr/local/bin/sdh-admin` runs `admin` (§11.4) with the same JVM options and `-Xmx512m`, so it fits beside the connector's JVM: `kubectl exec -it <pod> -- env SDH_OPERATOR=alice sdh-admin status`. With a read-only root filesystem, restore into `/tmp` (then `kubectl cp`) or the inbound volume. As a one-off container, arguments after the image name go to the app: `docker run … <image> admin status`.
- **Health:** there is no HTTP endpoint. The process is the health signal: fatal conditions exit with status 70 (`FatalExit`) and the orchestrator restarts it.

**JVM flags.** These are in `/app/jvm.options` (from `file-connector/docker/`), read by `ENTRYPOINT` as `java @/app/jvm.options -jar /app/app.jar`. `JDK_JAVA_OPTIONS` and `JAVA_TOOL_OPTIONS` are read before the command line, so they can add tuning (e.g. `-Xmx`) but can't undo these:

| Flag | Why |
|---|---|
| `-XX:-HeapDumpOnOutOfMemoryError`, `-XX:+ExitOnOutOfMemoryError` | No heap dump; an out-of-memory process exits and is restarted clean |
| `-XX:-CreateCoredumpOnCrash`, `-XX:ErrorFile=/dev/null` | No core dump, and no `hs_err` file (it holds raw memory near the crash) |
| `-XX:+DisableAttachMechanism` | No `jcmd`, so no heap dump on demand |
| `-XX:-UsePerfData` | No `hsperfdata` file in `/tmp` |
| `-XX:MaxRAMPercentage=75` | Heap sized from the container memory limit |
| `--enable-native-access=ALL-UNNAMED` | Java 25 otherwise warns when libraries load native code |
| `-DZstdNativePath=…`, `-Dorg.xerial.snappy.lib.path/name=…` | zstd-jni (section and Parquet compression) and snappy-java (an optional Kafka codec) load from `/app/native`, unpacked at build time, instead of unpacking themselves into `/tmp`. So `/tmp` can be `noexec`. Other native code (Netty, lz4) falls back to Java when it can't load |

**What the container needs at runtime:**

| Path | Mount | Contents |
|---|---|---|
| `/data/inbound` | writable volume, shared with whatever delivers files | Inbound files; the app writes `completed/` and `failed/` (§5) |
| `/tmp` | writable `emptyDir`/tmpfs; `noexec` is fine | `S3FileIO` stages multipart uploads here before sending them: the same bytes as the object store gets, with section payloads already encrypted. Allow about 1 GiB |
| `/run/secrets` | read-only secret volume | Local mode: `keks.p12`, `signing.pem`, `recipient.pem`, `recovery.pem` (paths are configuration, §14). Mode 0400 or 0440 |
| configuration | ConfigMap via `SPRING_CONFIG_ADDITIONAL_LOCATION`, or `SPRING_APPLICATION_JSON` | `sdh.*` (§14). Passwords and passphrases come from environment variables or a `configtree:` mount |

**Kubernetes.** A sketch of the pod settings. It's untested on a cluster (the `sdh-it` profile runs no Kubernetes), but `ContainerImageIT` applies the same constraints in Docker:

```yaml
spec:
  replicas: 1                          # the lease (§9.3) also stops a second instance
  strategy: { type: Recreate }
  template:
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001                 # secret files become root:10001 0440
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: file-connector
          image: sdh/file-connector:0.1.0
          securityContext:
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities: { drop: [ALL] }
          resources: { limits: { memory: 2Gi } }
          volumeMounts:
            - { name: inbound, mountPath: /data/inbound }
            - { name: tmp, mountPath: /tmp }
            - { name: keys, mountPath: /run/secrets, readOnly: true }
      volumes:
        - { name: inbound, persistentVolumeClaim: { claimName: sdh-inbound } }
        - { name: tmp, emptyDir: { sizeLimit: 1Gi } }
        - { name: keys, secret: { secretName: sdh-connector-keys, defaultMode: 0440 } }
```

## 15. Testing

| Level | Coverage |
|---|---|
| Unit | Each splitter, including boundaries spanning buffer edges, resume from every reported offset, malformed input and limits; crypto round-trips and AAD tamper detection; checkpoint MAC; message JOSE round-trip; lease keeper timing with a fake clock |
| Integration without Docker (default build) | Embedded PostgreSQL binaries (`io.zonky.test:embedded-postgres`) for the lease store and JDBC catalog. Iceberg's `RESTCatalogAdapter` for REST catalog commit rules, in-process. `InMemoryFileIO`. Fencing tests run against both catalog types. End-to-end write → message → `connector-queue-reader`. The whole Spring Boot app with a SEDA queue (`FileConnectorApplicationTest`). The admin commands against data the running app wrote, while it holds the lease (`AdminCommandsTest`) |
| Integration with Docker (`-Pit`) | See §15.1 |
| Crash and resume | Fault-injection points at every stage (after write, after commit, mid-publish, before end of file, mid-archive, after delete, while failing), then a restart. The test checks that there are no gaps, no lost or duplicated rows, and duplicate messages only within one batch |
| Throughput | Not built yet: synthetic multi-GB C-CDA, X12 and CSV files, with records of MB/s and sections/s |

### 15.1 Docker integration suite (`-Pit`)

`*IT` classes in `file-connector`, run by Failsafe only under `-Pit`; the default build never needs Docker. Images are pinned to releases at least two weeks old.

| Test | Containers | Verifies |
|---|---|---|
| `FullStackIT` | `postgres:18.4`, `chrislusf/seaweedfs:4.48`, `apache/kafka:4.2.2` | The Spring Boot app as deployed: Postgres for the lease and JDBC catalog, SeaweedFS through `S3FileIO`, Kafka. It drops a good and a bad file. Downstream, `connector-queue-reader` decodes the Kafka messages and reads every section back out of S3. The archive restores byte-for-byte. No object in the bucket (data, manifests or metadata) contains plaintext or a filename. Then maintenance on S3: compacts the 5 batch files into 1 and downstream still reads every section; a planted stray object is found by `S3FileIO` listing and deleted; retention three days on removes every sections data object from the bucket. Telemetry is exported over OTLP to `otel/opentelemetry-collector-contrib:0.162.0` (distroless: its output file is copied in) and must contain the traces, metrics and audit events, with no PHI marker or filename |
| `KafkaIT` | `apache/kafka:4.2.2` (3 partitions) | Record key = fileId, metadata headers, per-file order, `acks=all` + idempotence |
| `RabbitMqIT` | `rabbitmq:4.2.9-management-alpine` | `message_id`, content type, headers, publisher confirms (`CORRELATED`) |
| `ArtemisIT` | `apache/activemq-artemis:2.44.0-alpine` | `JMSXGroupID`, and duplicates dropped by `_AMQ_DUPL_ID` |
| `SqsIT` | `softwaremill/elasticmq-native:1.7.1` | FIFO MessageGroupId, and duplicates dropped by MessageDeduplicationId. ElasticMQ replaces LocalStack: it is SQS-compatible, light, and needs no account token |
| `PubSubIT` | `gcr.io/google.com/cloudsdktool/google-cloud-cli:590.0.0-emulators` | Ordering key with message ordering enabled, attributes |
| `ServiceBusIT` | `mcr.microsoft.com/azure-messaging/servicebus-emulator:2.0.0` + `mssql/server:2022-CU26-ubuntu-22.04` | Sessions per file in order, and duplicates dropped by MessageId. The emulator caps the dedup window at 5 min |
| `EventHubsIT` | `mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.2.0` + `azurite:3.35.0` | Each file in one partition, in order |
| `AwsKmsIT` | `nsmithuk/local-kms:3.11.7` | `aws-kms` through `KeyMaterial.load`: KEK probes; an alias-referenced KEK records the real key ARN; tag tampering refused; a message signed through KMS opens with the KMS public key. local-kms replaces LocalStack (no account token) |
| `AzureKeyVaultIT` | `nagyesta/lowkey-vault:7.3.112` | `azure-keyvault`: RSA-OAEP-256 wrapping records the versioned key ID; ES256 through Key Vault. Emulator workarounds, test-only: (1) versioned key IDs, because Lowkey rejects `wrapKey` on an unversioned ID, which real Key Vault accepts; (2) the KEK also gets `encrypt`/`decrypt`; (3) under minikube the client goes to the VM's IP, which isn't in Lowkey's certificate, so the test client trusts only self-signed certificates and skips hostname verification |
| `ContainerImageIT` | the image built from `Dockerfile` + `postgres:18.4`, `chrislusf/seaweedfs:4.48`, `apache/kafka:4.2.2`, `busybox:1.37.0` | The image as deployed (§14.1): read-only root filesystem, `noexec` tmpfs `/tmp`, all capabilities dropped, `no-new-privileges`, a 1.5 GB memory limit. Secrets are a volume shaped like a Kubernetes secret with an `fsGroup` (root:10001, 0440); the inbound directory is a volume that a second container (standing in for the file delivery) drops files into, because Docker won't copy into a container with a read-only root filesystem. Services are reached by network alias, configured through `SPRING_APPLICATION_JSON`. It checks that the process runs as 10001 and can't write outside its volumes; that a good and a bad file end in `completed/` and `failed/`; that downstream reads every section back from the host; that the console log has no PHI marker or filename; that `sdh-admin status` and `sdh-admin restore` work inside the running container without taking the lease; and that nothing native or `hs_err` was written to `/tmp`. The container log is saved to `target/container-image-it.log` |
| `VaultTransitIT` | `hashicorp/vault:2.1.1` (dev mode) | `vault-transit` built purely from connector settings (address, token file): wrap with associated data, ES256 signing, message round trip; a wrong token fails startup naming the KEK and HTTP 403 |

**Running it on minikube with KVM2:** use `scripts/it-minikube.sh`, which needs no local Docker daemon. The script:
- creates or starts a dedicated single-node profile `sdh-it` (`--driver=kvm2 --container-runtime=docker --no-kubernetes`, 6 CPUs, 12 GB, 60 GB disk; override with `SDH_IT_*`);
- exports `minikube docker-env`;
- runs `./mvnw -Pit verify`, passing any extra arguments through (e.g. `-Dit.test=KafkaIT`).

`SDH_IT_STOP=1` stops the VM afterwards; `minikube delete -p sdh-it` removes it. `scripts/build-image.sh` uses the same VM (§14.1); both share `scripts/minikube-env.sh`.

**What makes minikube work:**
- **Dedicated profile:** `docker-env` doesn't work on multi-node clusters, and the tests shouldn't share a cluster used for other work.
- **Reachability:** containers run inside the VM, and Testcontainers reaches them at the VM's IP through DOCKER_HOST.
- **No host mounts:** host paths don't exist in the VM, so config files (Service Bus, Event Hubs, SeaweedFS identities) are copied in with `Transferable`.
- **Ryuk:** Testcontainers' cleanup container mounts the VM's own Docker socket.
- **Status check:** `minikube status` exits non-zero without Kubernetes, so the script checks the host state instead.

**Failsafe:** it tests `target/classes`, because Spring Boot's repackaged jar keeps classes under `BOOT-INF/`.

**Time:** with images cached in the VM, the whole build (135 unit tests + 13 ITs, including building the image) takes about 2 min 50 s. The first run pulls several GB of images.

**GCP Cloud KMS** has no emulator. `GcpKmsKeyProviderTest` (a unit test in `connector-kms`) runs the provider against an in-process fake of the Cloud KMS gRPC service: real AES-GCM with the AAD enforced, real ECDSA, and the CRC32C integrity fields. It checks our side of the protocol, not Google's service.

**Not covered yet:** an Iceberg REST catalog (Polaris) container; throughput tests; GCP KMS against the real service; the image on a real Kubernetes cluster.

## 16. Risks and verification items

These are checked first, in a build spike before the full implementation. Spike status as of 2026-10-28. All results come from `./mvnw install` on Java 25.0.4 (32 tests, all passing).

| # | Item | Status | Result |
|---|---|---|---|
| 1 | Java 25 with Iceberg/Parquet and Hadoop classes | **Passed** | Parquet write and read through Iceberg work on Java 25. A fully trimmed `hadoop-common` is not enough: `Configuration` needs Woodstox/StAX2, hadoop-shaded-guava and commons-collections4, and Parquet's read path needs `hadoop-mapreduce-client-core`. That is six jars in total, all with transitive dependencies excluded (§2). We don't use `HadoopFileIO`, so Hadoop's security code (`UserGroupInformation`/`Subject`, a known Java 23+ problem area) is never reached. `S3FileIO` against SeaweedFS **passed** in `FullStackIT`, once the AWS `kms` module was added (§2) |
| 2 | Camel 4.22 with Spring Boot 4.1.x | **Passed** | Spring Boot 4.1.2 with Camel 4.22.2: the context starts with all 7 broker components resolved, the file consumer picks up files oldest first, skips `completed/`/`failed/` and never moves or deletes files, and the Iceberg stack works on the same classpath. One fix needed: pin Jackson 2 to 2.22.2 (§2). Fat jar is about 184 MB with every broker client (190 MB once wired, with `iceberg-aws` and the S3 client). **Later finding:** the spike's oldest-first check passed only by directory order; `eagerMaxMessagesPerPoll=false` is needed (§5). **Later finding:** the packaged app exited right after starting, because Camel 4 reads `camel.main.run-controller`, not `camel.springboot.main-run-controller`; tests start the app in-process, so only `ContainerImageIT` saw it (§14.1) |
| 3 | Azure emulators | **Passed** | Service Bus and Event Hubs emulators run under `-Pit` on minikube. They found two adapter bugs (§10.3): Camel's Service Bus producer can't set per-message sessions or IDs, and the Azure client reorders sends in flight together |
| 4 | Custom compaction | **Built** | Bin-pack via `RewriteFiles` + `validateFromSnapshot` through the fenced committer (§8.4). Findings: snapshot expiry must run standalone to delete files physically; and Iceberg deletes a removed file only once the snapshot that removed it has expired, so retention adds a marker commit to purge at once |
| 5 | Metadata growth | **Resolved** | Lease renewals no longer touch Iceberg (§9.3), and publish marks ride along with batch commits (§9.2). Commits are now roughly one per batch. Test: after 130 commits, at most 100 previous metadata files are kept, and the oldest is deleted (JDBC catalog; a REST server applies its own policy) |
| 6 | New: fencing on REST and JDBC catalogs | **Passed** | Plain Iceberg transactions are not fenced, which the control test demonstrates. The compare-and-swap committer with snapshot markers is (§9.3) |
| 7 | New: Testcontainers suite | **Passed (partly)** | §15.1: SeaweedFS + `S3FileIO`, Postgres, and all seven brokers pass on minikube. KMS providers: AWS (local-kms), Azure (Lowkey Vault) and Vault pass; GCP runs against a fake. Not covered: MinIO (images no longer published), a REST catalog container |

**Spike follow-ups:** add a REST-catalog IT. Check the Iceberg internals used by the fenced committer (§9.3) on every Iceberg upgrade.

## 17. Out of scope (for now)

- Multi-instance or high-availability operation. The lease enforces a single writer.
- **Host disk capacity:** space for the inbound directory, `completed/` and `failed/` records, and the encrypted local `failed/<fileId>.enc` fallback is the operators' responsibility, through normal system monitoring. The service doesn't reserve, measure or predict disk space.
- Re-keying existing data after a KEK compromise. KEK rotation for new data is supported through `kek_ref`.
