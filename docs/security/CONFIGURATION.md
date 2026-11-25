# Security configuration guide

Every setting that bears on HIPAA, its safe value, and what it does: HIPAA.md §11.2's document D6. Start from
`deploy/reference/`, which has every safe value below in place for one pipeline on AWS, and change only what this
guide says is the deployment's own. The procedures that use these settings are HIPAA-OPERATIONS.md §2; where PHI
is and what protects it is [DATA-FLOWS.md](DATA-FLOWS.md).

**The rule of thumb:** run the `hipaa` compliance profile with nothing waived. It refuses to start a stage whose
settings would break most of what follows (§1), so a stage that starts under it is already safe on those points.
The rest of this guide is what the profile can't see: the values within a safe setting, and what lies outside the
stages.

## 1. The compliance profile

`sdh.compliance.profile` is `hipaa` unless set otherwise (DESIGN.md §7.6). At startup each stage checks its own
settings, and refuses to start with a message naming the check and the setting, never its value. Each check can be
waived on its own with `sdh.compliance.waive.<id>: <reason>`; every waiver is logged and audited at every start,
and the control API's `compliance` view lists it (HIPAA-OPERATIONS.md §2.1). **Waive nothing for real PHI**
unless the security official has written down why, and what stands in for the check.

| Check (`<id>`) | Refuses | Safe setting | § 164 |
|---|---|---|---|
| `transport-plaintext` | a plaintext endpoint: `http://`, `ws://`, `amqp://`, Artemis without TLS, an Azure connection string over HTTP or to an emulator | `https://` and the TLS form of each URI | .312(e) |
| `transport-database` | a Postgres connection that doesn't verify the server | `sslmode=verify-full` (or `verify-ca`) with `sslrootcert` | .312(d), (e) |
| `transport-broker` | a broker without TLS or authentication: Kafka without `SSL` or `SASL_SSL`, RabbitMQ without `spring.rabbitmq.ssl`, SQS at a plaintext endpoint, Pub/Sub without authentication | Kafka `securityProtocol=SASL_SSL` with SCRAM (or `SSL` with a client certificate) | .312(d), (e) |
| `transport-unchecked` | a connection the profile can't judge: a Camel component or JDBC driver it doesn't know | use a component it knows; waive only with the transport's security shown another way | .312(e) |
| `keys-local` | keys in a local keystore or PEM file | `sdh.crypto.wrap-provider` and `sign-provider`: `aws-kms`, `gcp-kms`, `azure-keyvault` or `vault-transit` | .312(a)(2)(iv) |
| `audit-export` | no audit export | `sdh.telemetry.otlp.endpoint` set to the operator's collector | .312(b) |
| `audit-journal` | the durable audit journal off | `sdh.telemetry.journal.enabled: true` (the default) | .312(b), .414(b) |
| `storage-no-auth` | object storage without authentication | the platform's workload identity, or keys from a secret | .312(d) |
| `store-versioning` | a table whose bucket doesn't keep versions, or whose versioning can't be read | versioning on, and `s3:GetBucketVersioning` (or the store's equivalent) for the stage's role (DEPLOYMENT.md, "Object store versions") | .308(a)(7)(ii)(A) |
| `jvm-dumps` | a JVM that may write memory to disk or let a tool read it | the image's options: `-XX:-HeapDumpOnOutOfMemoryError`, `-XX:-CreateCoredumpOnCrash`, `-XX:ErrorFile=/dev/null`, `-XX:+DisableAttachMechanism` | .310(d), .312(a)(1) |
| `control-tls` | a control API without TLS | `sdh.control.tls.enabled: true` (the default) with a certificate and key | .312(e) |
| `control-token-lifetime` | a control API that accepts tokens issued for longer than an hour | `sdh.control.auth.jwt.max-lifetime` at most `1h` (the default) | .312(a)(2)(iii), (d) |
| `cli-four-eyes` | never refuses a start: under the profile the CLI refuses erasure, plaintext break-glass, re-keying and audit prune, which need two people, unless this is waived | don't waive it: use the control API, which takes a second person's approval | .308(a)(3), (a)(4) |

`ReferenceConfigurationTest` (connector-runtime) runs each file in `deploy/reference/` through the profile with
its image's JVM options and expects nothing refused or waived; `ConfigurationGuideTest` checks that this table
names every check.

## 2. Transport

| Setting | Safe value | What it does |
|---|---|---|
| Kafka URIs (`sdh.stage.input.uri`, `output`, `dlq`, `reprocess`, `sdh.queue.uri`, `sdh.results.queue.uri`) | `securityProtocol=SASL_SSL&saslMechanism=SCRAM-SHA-512&saslJaasConfig=RAW({{env:SDH_KAFKA_JAAS}})&sslTruststoreType=PEM&sslTruststoreLocation=/run/secrets/kafka-ca.crt` | TLS to the broker, verified against its CA; a login per stage, from the environment, never in the file (§3). The broker grants each login only its rights (`deploy/reference/kafka-acls.sh`, DEPLOYMENT.md "Broker rights") |
| JDBC URLs (`sdh.storage.catalog.uri`, `sdh.lease.jdbc.url`, the registries') | `?sslmode=verify-full&sslrootcert=/run/secrets/postgres-ca.crt`, in the pipeline's own database | TLS to Postgres (or PgBouncer), with the server's name checked |
| Object store | `https://` (or the SDK's default), with the bucket policy refusing plaintext requests | TLS to the store; the policy refuses a client that asks without it (AWS-POC.md §4) |
| Iceberg `io-impl` and client | `S3FileIO`, `GCSFileIO` or `ADLSFileIO` with the platform's identity | how the stage reads and writes table files |
| HTTP intakes | `sdh.landing.http.mtls.trusted-cas` and `mtls.subjects` (an allow-list of client certificates), or `oauth2.issuer-uri`, `audience` and `scope`; TLS passed through the ingress to the pod for mTLS | who may push items (DESIGN.md §5.3.1) |
| SFTP and SMB intakes | SFTP: the server's host key in the intake's `known-hosts` file (required; an unknown or changed key is refused); SMB 3 with encryption required | the transport from a share |
| `sdh.telemetry.otlp.endpoint` | the collector beside the stage; TLS from the collector on | telemetry and the audit export carry no PHI, so the profile allows plain HTTP inside the cluster (HIPAA-OPERATIONS.md §2.7) |

## 3. Secrets

- **Never in a settings file or a URI.** Passwords, Kafka logins and tokens come from the stage's Kubernetes
  Secret, as environment variables or mounted files: `SDH_LEASE_JDBC_PASSWORD`, `SDH_KAFKA_JAAS`,
  `SDH_CRYPTO_KEYS_INBOX_PASSPHRASE`, a Vault token file.
- **A written secret is refused.** A URI option holding a password, key, secret, token, JAAS login or connection
  string must be a `{{...}}` placeholder or a `#bean` reference, on every input and output URI; a written value
  stops the stage at startup, naming the setting and option, never the value (DESIGN.md revision (fd)).
- **Files:** a local keystore or private PEM readable by others, or writable by group, stops the stage; private
  PEMs must be encrypted PKCS#8 (DESIGN.md §7.4).
- **The settings view and support bundle** remove every secret by name and by form (HIPAA-OPERATIONS.md §9).

## 4. Keys

What each key is and who holds it is D3. The settings:

| Setting | Safe value | What it does |
|---|---|---|
| `sdh.crypto.wrap-provider`, `sign-provider` | a KMS (§1, `keys-local`) | where KEKs wrap data keys and signing keys sign; the KEKs never leave the KMS |
| `sdh.crypto.keys.landed-items-kek`, `records-kek`, and the transform stage's `sdh.stage.sink.kek` | a KEK of its own for each layer and pipeline (`alias/sdh-<p>-landed-items`, `-records`, `-fhir`) | which KEK protects each table, so each reader is given only its own (DATA-FLOWS.md §4) |
| `sdh.crypto.keys.signing` (`kid`, `key-ref`) | a KMS signing key of the stage's own | signs the stage's messages (ES256) |
| `sdh.crypto.keys.recipient` (`kid`, `pem`, `sha256`) | the next stage's public key, its SHA-256 pinned | whom the stage seals its messages to; a key whose fingerprint doesn't match stops the stage |
| `sdh.crypto.keys.inbox` (`kid`, `pem`, `public-pem`, `sha256`, `passphrase`) | the stage's own key pair, the private half encrypted, its passphrase from the secret | opens the messages sent to the stage |
| `sdh.results.recipient` (`kid`, `pem`, `sha256`) | the producer's public key, pinned | seals results to the producer alone |
| `sdh.crypto.jose.provider` | `jdk`, or `accp` on AWS (`conscrypt` on Google Cloud and Azure) for speed | who does the elliptic-curve work; none is FIPS 140 validated (§9) |
| `sdh.crypto.jose.agreement-messages`, `agreement-lifetime` | `10000` and `60s` (the defaults); `1` for a key agreement a message | how many messages share one ephemeral key: fewer limits what one leaked ephemeral key opens, at a cost in speed (PERFORMANCE.md §13) |

## 5. Audit

| Setting | Safe value | What it does |
|---|---|---|
| `sdh.telemetry.otlp.endpoint` | the operator's collector | the audit export, for the SIEM (HIPAA-OPERATIONS.md §2.7, §3.1) |
| `sdh.telemetry.journal.enabled` | `true` | the durable journal in the stage's schema, hash-chained and sealed (HIPAA.md §8.1 item 1) |
| `sdh.telemetry.journal.seal-interval` | `60s` (the default) | how often the chain's head is signed; a shorter interval narrows what an undetected change could cover |
| `sdh.telemetry.journal.linger` | `0s` (the default) to `100ms` | how long events wait to be written together; never dropped either way |
| A gold reader's `auditAs` and `purpose` | the application's service account and an HL7 purpose of use | names each read of gold in the audit (HIPAA-OPERATIONS.md §2.5) |

## 6. The control API

| Setting | Safe value | What it does |
|---|---|---|
| `sdh.control.mode` | `true` on the control Deployment only | serves the API; the workers don't |
| `sdh.control.tls.enabled`, `certificate`, `key` | on, with a certificate from the operator's CA | TLS for tokens and results (§1, `control-tls`) |
| `sdh.control.auth.jwt.issuer-uri`, `jwk-set-uri`, `audience` | the operator's identity provider, an audience of the API's own | whose tokens are trusted (D2, D13) |
| `sdh.control.auth.jwt.max-lifetime` | `1h` or less | the longest-lived token accepted (§1) |
| `sdh.control.permissions` | each permission mapped to the operator's roles, least privilege; `phi.export`, `phi.break-glass`, `keys.admin`, `data.admin` and `audit.admin` to few | who may do what (CONTROL-API-DESIGN.md §3) |
| `sdh.maintenance.retention-bounds.minimum`, `maximum` | what law and contract allow | the range an operator may set retention to through the API, so a mistake can't remove what must be kept |

The API's network policy and Service come from each stage's README ("Control API"): reachable from the operators'
network only, never from the internet (D1).

## 7. Intakes and producers

| Setting | Safe value | What it does |
|---|---|---|
| `sdh.landing.source.access`, `after` | `read-write` with `after: delete` (or `move`) where the producer allows it; `read-only` otherwise | whether a landed item stays on the source, outside the pipeline's protection (DATA-FLOWS.md §2) |
| `sdh.landing.http.max-item-bytes` | the largest item the producer sends | refuses a push larger than that; unset, a push may be any size |
| `sdh.landing.http.mtls.subjects` or `oauth2.subject` | exactly one sender when results are on | a producer gets results for its own items only; the node refuses to start otherwise (DESIGN.md §5.3.8) |
| `sdh.results.enabled`, `sdh.results.queue.uri` | on only where the producer has a key and a queue of its own | results carry the producer's own references, sealed to it |

## 8. Retention

How each removal works, and what it leaves, is D9; the settings, each unset by default (kept for ever):

| Setting | What it removes |
|---|---|
| `sdh.maintenance.retention.landed-items` | bronze items, whole days, at least 7 days old (DESIGN.md §5.3.13) |
| `sdh.maintenance.retention.records` | a stage's records table |
| `sdh.maintenance.retention.given-up` | the chunks of items given up part way, after a grace period |
| `sdh.stage.retention.after` | registry rows of finished items with no open failure, never data, the ledger, failures or the journal (DEPLOYMENT.md "Registry retention") |

Set them from the customer's records-management policy, and set the control API's retention bounds (§6) to match.
`sdh.maintenance.enabled` (on by default) must stay on for any of them to run.

## 9. Outside the stages

What the profile can't check, and a deployment needs:

- **Buckets:** versioning on, a lifecycle rule expiring noncurrent versions, default encryption, a policy refusing
  plaintext requests, and access only for the stage roles that use them (HIPAA-OPERATIONS.md §2.8, §7).
- **Broker:** no plaintext listener on the network, ACLs from `kafka-acls.sh`, a login per stage
  (HIPAA-OPERATIONS.md §2.6, "Securing the broker").
- **Postgres:** TLS only (`hostssl`, `hostnossl ... reject`), a database per pipeline with the roles of
  `deploy/reference/postgres.sql`, backups (HIPAA-OPERATIONS.md §7).
- **KMS key policies:** each stage may use only its own keys; nobody holds both administration and use (D3).
- **Kubernetes:** the Deployments from each stage's README, unchanged in their security context (non-root, a
  read-only root filesystem, no privilege escalation); network policies that deny by default (D1, D4).
- **FIPS 140:** not supported (DEPLOYMENT.md, "FIPS 140"). A buyer that requires it can't use the product yet.

## 10. Checking a deployment

1. Start each stage with the profile on and nothing waived: a stage that starts has passed §1.
2. Read the control API's `compliance` view on each stage: the profile, and any waiver with its reason.
3. Read the `settings` view (secrets removed) against this guide's §2 to §8.
4. Check §9 with the platform's own tools, and record the result in the risk analysis.

## Sources

| Statement | Source |
|---|---|
| The checks | `connector-runtime` `ComplianceCheck`, `ComplianceRules` |
| The safe values in place | `deploy/reference/*-application.yml`, checked by `ReferenceConfigurationTest` |
| Control API settings and defaults | `connector-stage` `ControlSettings` |
| Journal, sealing and retention settings | `connector-runtime` `CommonSettings` |
| Intake settings | DESIGN.md §5.3.1, §5.3.8, §5.3.15 |
| Registry retention | DEPLOYMENT.md, "Registry retention" |
