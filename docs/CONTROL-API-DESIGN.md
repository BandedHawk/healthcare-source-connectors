# The pipeline control API: design

Status: **Designed 2026-11-20 with the user; steps C1 (2026-11-20) and C2–C7 (2026-11-21) built, the rest not built** · Puts into effect the intent in
[CONTROL-API.md](CONTROL-API.md), and is the design for [HIPAA.md](HIPAA.md) §11.1 P1–P6. The operator commands
it serves are those in DESIGN.md §11.4.

Decided with the user on 2026-11-20:
- **Placement:** a control Deployment per stage (§1).
- **Four eyes:** erasure, plaintext break-glass, re-keying and audit prune each need a second person's approval
  (§6).
- **Sealing:** to keys registered in advance for each principal, never to a key sent with the request (§7).
- **The CLI:** kept, on the same operation layer, as the path for when the API can't be reached (§3).

The rest of this document is the design that follows from those choices. Where it chose something the user hasn't
reviewed, §13 lists it.

## 1. Where it runs

Each stage's image has a **control mode**: `java -jar <stage>.jar control`, or `sdh-control` in the image. It runs
as a small Deployment beside the stage's workers, named `sdh-control-<stage>`.

| | Workers | Control |
|---|---|---|
| Image and version | the stage's | the same |
| Service account, database login, KMS and store rights | the stage's | the same: the admin CLI uses these today, so nothing new is granted |
| Input routes, duties lease, claims | yes | none |
| Inbound port | none (the landing node's push only) | 8443, TLS |
| Replicas | as tuned | 2, for availability; it keeps no state of its own (§8) |
| Memory | as tuned | 384 MiB, heap 256 MiB |

Why a Deployment of its own:
- **It stays up when the workers don't.** Recovery is needed most when workers crash-loop, are paused or are
  scaled to zero.
- **Workers gain no inbound port.**
- **Privilege isn't concentrated.** Each control process holds only its own stage's rights, as the CLI does. A
  single gateway would hold every stage's rights.
- **It works across repositories.** The landing node, split and convert stages are here; the transform stage is in
  `healthcare-data-transformer`. Each gets control mode from shared code in `connector-stage` (`stage.control`) and
  contributes its own operations, as each contributes its admin commands today.

**Routing:** the customer's ingress, inside its network only (CONTROL-API.md, "Exposure"), routes
`/pipelines/{pipeline}/stages/{stage}/` to each stage's control Service. The API has no pipeline-wide endpoint.
Clients (the harness, the console, the customer's tools) gather the stages' answers themselves. Operations that
span the pipeline are recorded once and carried out by each stage (§8.2).

**Network policy:** the control pods accept traffic only from the ingress controller's namespace, on 8443. The
workers' policy is unchanged.

**Sandbox budget:** four control Deployments of two pods add about 3 GiB of limits. The sandbox runs one replica
each, and its per-node sums, which `scripts/sandbox.sh` enforces, are redone before they are added.

## 2. Authentication

Control mode uses Spring Security, as the landing node's push does (DESIGN.md §5.3.15), in one of two modes:

```yaml
sdh:
  control:
    port: 8443
    tls:                      # always on; the profile refuses it off
      certificate: /run/secrets/control.crt
      key: /run/secrets/control.key
    auth:
      mode: jwt               # or mtls
      jwt:
        issuer-uri: https://idp.example.org/realms/health
        audience: sdh-control
        principal-claim: sub  # or preferred_username, email, appid
        roles-claim: groups   # or roles, scp
        max-lifetime: 1h      # tokens issued for longer are refused
      mtls:
        trusted-ca: /run/secrets/control-clients-ca.crt
        principal: subject-dn # or san-email, san-uri
        roles:                # certificate principals have no token claims
          "CN=noc-automation,O=Example": [sdh-operators]
    permissions:              # nothing is granted unless listed here
      pipeline.view: [sdh-viewers, sdh-operators]
      pipeline.operate: [sdh-operators]
      phi.export: [sdh-phi-export]
      phi.break-glass: [sdh-break-glass]
      audit.read: [sdh-auditors]
      audit.admin: [sdh-audit-admins]
      keys.admin: [sdh-key-admins]
      data.admin: [sdh-data-admins]
    approval:
      ttl: 24h                # a request unapproved for this long expires
      break-glass-ttl: 1h
```

- **JWT:** checked for signature (against the issuer's JWKS), issuer, audience, expiry and not-before, with 60 s
  of clock skew. A token whose lifetime is longer than `max-lifetime` is refused, so a long-lived token can't be
  minted for the API.
- **mTLS:** the TLS server requires a client certificate from `trusted-ca`. The principal comes from the
  certificate. Its roles come from the `roles` map, because a certificate carries no role claims.
- **The principal** is the authenticated name, never one given by the caller. It is recorded as `sdh.operator` in
  every audit event (§10). `sdh.auth` records how it was proved: `jwt` or `mtls`.
- **No anonymous access.** Every path needs a credential, even the OpenAPI description and health. Kubernetes
  probes use a separate plain port that serves only liveness and readiness, with no data.

## 3. One operation layer, two ways in

Today each admin command parses its arguments, runs, and prints. The command logic already returns rows apart
from parsing and printing (DESIGN.md §11.4, "A web console, later"). The first build step makes that a type:

```java
interface Operation<C, Q, R>
{
    String name();                 // "failures.retry"
    Permission permission();       // PIPELINE_OPERATE
    Kind kind();                   // QUERY, ACTION, LONG, SEALED
    Approval approval();           // NONE or FOUR_EYES
    Class<Q> request();            // a record, validated
    R run(Q request, C context, Caller caller, AuditRecord audit);
}
```

- **The API** binds JSON to `Q`, checks the permission, and writes `R` as JSON.
- **The CLI** binds options to `Q` and prints `R` as it does now. It keeps its names, its options, its exit codes
  and its `--operator`.
- **Both** write the same audit events. `sdh.access=api|cli` tells them apart, and `sdh.auth=cli` says the CLI's
  operator name is self-asserted (HIPAA.md §8.1 item 3).

**The CLI stays** as the path for when the API can't be reached: the ingress is down, the identity provider is
down, or the control pods won't start. Access to it is what it is today: Kubernetes RBAC on `pods/exec`, with the
platform's audit of `exec` as the proof of who acted. HIPAA-OPERATIONS documents it as an emergency path, to be
granted to few.

**Four-eyes operations through the CLI:** under the `hipaa` profile, the CLI refuses the four operations that
need approval (§6), because it can't prove two people. Recovery needs none of them. A deployment can waive the check
(`sdh.compliance.waive: [CLI_FOUR_EYES]`); the waiver is then shown in the compliance outcome and the support
bundle, as every waiver is.

## 4. Authorization

- Each operation names one permission (§5). Correcting a failure is the only one that needs two:
  `pipeline.operate`, and `phi.export`, because whoever corrects a record has read it.
- The caller's roles (from the token's claim, or from the `roles` map for mTLS) are mapped to permissions by
  `sdh.control.permissions`. A permission no role is mapped to is granted to no one.
- A refusal answers `403` with the permission named, and is audited (`decision=denied`). A missing or invalid
  credential answers `401` and is audited without a principal, with the reason class (`expired`, `audience`,
  `signature`, `untrusted-certificate`).
- **The console later** adds role management on top of the same permissions. It doesn't add permissions of its
  own.

## 5. The resources

All paths are under `/api/v1`, on a stage's control endpoint. **No PHI is ever in a URL:** item IDs, failure
sequence numbers and operation IDs are the product's own identifiers. Patient identifiers and corrected records
travel only in request bodies.

| Permission | Method and path | Does | Kind | CLI today |
|---|---|---|---|---|
| `pipeline.view` | `GET /status` | duties lease, items by state, longest untouched | query | `status` |
| | `GET /items/{itemId}/status` | one item's progress | query | `status --item` |
| | `GET /stuck?idle=` | items not complete that nothing is working on | query | `stuck` |
| | `GET /drain` | what is still claimed or in flight | query | `drain-check` |
| | `GET /version` | the stage's version, and any awaiting acceptance | query | `version` |
| | `GET /health` | each dependency reached or not: database, broker, store, KMS, collector | query | new (P5) |
| | `GET /settings` | effective settings, secrets removed | query | new (P5) |
| | `GET /compliance` | the profile's outcome: passed, refused, waived | query | new (P5) |
| | `GET /support-bundle` | all of the above, plus recent operations and metrics, as one zip; no PHI | query | new (P5) |
| | `GET /failures?state=&code=&from=&to=` | failures: IDs, codes, states, times; no content | query | `failures list` |
| | `GET /reprocess-runs`, `GET /reprocess-runs/{id}` | reprocess runs and their progress | query | `reprocess --status` |
| | `GET /operations`, `GET /operations/{id}` | long operations and requests awaiting approval (§8) | query | new |
| | `GET /control-state` | paused or not, by whom, since when (§9) | query | new (P2) |
| | `GET /openapi.yaml` | the contract | query | |
| `pipeline.operate` | `POST /pause`, `POST /resume` | pause or resume the stage or one intake | action | new (P2) |
| | `POST /drain` | pause, then wait until nothing is claimed | long | new (P2) |
| | `POST /reprocess-runs` | start a reprocess run (`dryRun` allowed) | long | `reprocess` |
| | `DELETE /reprocess-runs/{id}` | cancel it | action | `reprocess --cancel` |
| | `POST /failures/{itemId}/{seq}/retry` | retry a failure | action | `failures retry` |
| | `POST /failures/{itemId}/{seq}/decline` | decline it, with a reason | action | `failures decline` |
| | `POST /failures/{itemId}/{seq}/release` | release it | action | `failures release` |
| | `POST /items/{itemId}/resubmit` | land the item again, as a new item linked to it (landing node) | long | `resubmit` |
| | `POST /version/accept` | accept the running version | action | `version --accept` |
| `pipeline.operate` + `phi.export` | `POST /failures/{itemId}/{seq}/correct` | submit a corrected record (body: the record) | action | `failures correct` |
| `phi.export` | `POST /failures/export` | failures' content, sealed | sealed | `failures export` |
| | `POST /patients/lookup` | the patient lookup (body: identifiers or item IDs), sealed | sealed | `patients` |
| | `POST /items/{itemId}/restore` | a landed item, sealed | sealed | `restore` |
| `phi.break-glass` | `POST /items/{itemId}/restore/plaintext` | a plaintext restore, with ticket and reason; **four eyes** | long | `restore --plaintext` |
| `audit.read` | `GET /audit/verify?from=&to=` | check the journal's chains | query | `audit-verify` |
| | `GET /audit/events?from=&to=` | the journal's events, as JSON lines | query | `audit-export` |
| `audit.admin` | `POST /audit/prune` | delete chains last written before a time; **four eyes** | long | `audit-prune` |
| `keys.admin` | `GET /keys` | the KEKs in use, with how much data each wraps | query | new (P3) |
| | `POST /keys/rotate` | set the KEK for new data | action | new (P3) |
| | `POST /keys/rekey` | re-wrap stored data from one KEK to another; **four eyes** | long | new (P3) |
| | `GET`, `POST`, `DELETE /operator-keys` | register, list and revoke principals' sealing keys (§7) | action | new |
| `data.admin` | `GET`, `PUT /retention` | the retention in force, and set it (§11) | action | new (P4) |
| | `POST /items/{itemId}/purge` | purge an item from the pipeline; **four eyes** | long | new (P4) |
| | `POST /erase` | erase the pipeline's data at the end of a contract; **four eyes** | long | new (P4) |

**Conventions:**
- **Requests** are JSON, validated against the contract. An unknown field is an error, as an unknown CLI option is,
  so a typo can't change what an operation does.
- **Every action needs a `reason`**, which is audited. It is checked against the same character rules as an
  operator's name (no PHI-shaped free text: letters, digits, spaces and `. , : _ - # /`, at most 200 characters).
  Break-glass, purge and erase also need a `ticket`.
- **`Idempotency-Key`** is required on every action and long operation. The pair of principal and key is unique
  for 24 h. A retry with the same key and body gets the first answer back; a retry with a different body answers
  `409`.
- **Lists** are paged with an opaque `cursor` and a `limit` (at most 1000).
- **Errors** are `application/problem+json` (RFC 9457) with a stable `code` (for example `failure-not-found`,
  `wrong-state`, `approval-required`, `kms-unavailable`), a request ID, and never an exception message, a value or
  an object key (HIPAA.md §4.1).
- **Times** are RFC 3339 in UTC, read from the database clock where they decide anything, as the CLI does.

## 6. Four eyes

Erasure (`erase`, `purge`), plaintext break-glass, re-keying and audit prune each need a second person.

1. **Request.** The first principal sends the operation, with its reason and, where needed, its ticket. The answer
   is `202` with an operation in state `awaiting-approval`. The audit event names the requester.
2. **Approve or reject.** A second principal sends `POST /operations/{id}/approve` or `/reject`, with a reason.
   - The approver must hold the same permission, and must not be the requester. Principals are compared as
     authenticated, so the same person through JWT and through mTLS counts once only if the customer maps them to
     one name; HIPAA-OPERATIONS says to.
   - The approver sees the operation's parameters (item IDs, the time range, the KEKs) and its reason and ticket,
     never data.
3. **Run.** Once approved, the operation runs (§8). A request not approved within `approval.ttl` (24 h; 1 h for
   break-glass) expires. The requester may cancel it before it runs.

**Break-glass result:** the plaintext is fetched by the requester only, once, within the break-glass TTL, from
`GET /operations/{id}/result`. It is then deleted. It is held encrypted under the stage's KEK until then, never
in the clear at rest.

**Pipeline-wide operations** (`erase`, `purge`, `rekey`) are approved once for the pipeline, not once per stage
(§8.2).

## 7. Sealing keys, registered per principal

A PHI result is sealed to a public key registered in advance for the caller. A stolen token can't have PHI sealed
to the thief's own key.

- **Register:** `POST /operator-keys` with the principal, a PEM public key (EC P-256 or P-384, or RSA of at least
  3072 bits) and an expiry of at most a year. The answer gives its fingerprint (SHA-256 of the key's
  SubjectPublicKeyInfo).
- **Who may:** `keys.admin`, but **not for themselves**: a principal can't register a key that it will then use,
  so registering and exporting are always two people.
- **Where:** the pipeline's database, in `sdh_operator_keys` (principal, fingerprint, key, registered by, at,
  expires, revoked). All the pipeline's stages share it. Registering and revoking are audited.
- **Use:** a sealed request names `sealTo: <fingerprint>`. It must be one of the caller's own keys, unexpired and
  not revoked, or the answer is `403 sealing-key-not-yours`.
- **The CLI** still reads the key from standard input (`< key.pem`), as now: it is the emergency path, guarded by
  `exec` RBAC (§3).

**The sealed stream:** the answer is `200` with `Content-Type: application/x-ndjson` and chunked transfer, as the
CLI's output is lines today (`ExportSealer`):
- **A header line:** the operation, the fingerprint sealed to, and the request ID.
- **One line per part,** `{"part": n, "jwe": "<compact JWE>"}`. Each part seals at most 1 MiB of plaintext: a
  record, a failure's content, a restored item's chunk, or a page of lookup rows. The JWE is `ECDH-ES+A256KW` (EC
  keys) or `RSA-OAEP-256` (RSA), with `A256GCM`, as the CLI uses.
- **A trailer line:** the count of parts and a SHA-256 over their JWEs, so the client knows the answer was whole.
  A stream cut off has no trailer, and the client discards it. The audit records how many parts were sent.

Nothing is written to disk on the way: each part is sealed as it is read, and the server holds at most a part's
plaintext in memory.

## 8. Long operations

### 8.1 The operation record

Reprocess runs, resubmits, drains, re-keys, purges, erasures, audit prunes and break-glass restores are
operations. `POST` answers `202 Accepted` with `Location: /api/v1/operations/{id}`. The client polls it.

The table `sdh_control_operations` is in the pipeline's database:

| Column | Holds |
|---|---|
| `id` | a UUID |
| `kind`, `scope` | the operation; `stage:<name>` or `pipeline` |
| `parameters` | JSON: item IDs, ranges, KEK references; never PHI |
| `requested_by`, `reason`, `ticket`, `requested_at` | |
| `approved_by`, `approval_reason`, `approved_at` | four eyes only |
| `state` | `awaiting-approval`, `approved`, `running`, `done`, `failed`, `cancelled`, `rejected`, `expired` |
| `parts` | for a pipeline operation, each stage's state and counts |
| `progress`, `result` | counts and row summaries; never PHI |
| `holder`, `holder_until` | the control pod running it, by lease, so two replicas never both run it |

What runs it:
- **Reprocess and resubmit** are run by the workers, as now (the ReprocessStore, the landing node's resubmit poll).
  The operation follows their state.
- **The rest** are run by the stage's control pods. One takes the operation's lease and renews it. If it stops, the
  other takes over where the progress left off. Each of these operations is made resumable for that.

### 8.2 Pipeline-wide operations

`purge`, `erase` and `rekey` touch every stage's data. One request, approved once, is recorded with
`scope=pipeline`. **Each stage's control pods run their own part** when they see it, with their own rights. The
operation is `done` only when every stage's part is. A stage that isn't deployed (whose control pods don't answer
for 10 minutes) holds it at `running`, and that is shown. The request may be sent to any stage's endpoint.

## 9. Pause, resume and drain (P2)

The table `sdh_control_state` holds a row per scope: the stage, or one intake of the landing node. A row gives
`paused`, by whom, since when, and the reason. As built (§15.6), a scope is named as the stage's audit chains are
(`convert/<connector ID>`); a landing node is one intake, so its scope is that intake's.

- **Workers read it** when they start, before starting their routes, and every 5 s after.
- **Pausing** suspends the input routes: Kafka consumers stop polling, landing intakes stop listing, and the HTTP
  push answers `503` with `Retry-After`. Work already claimed finishes and commits. Nothing is dropped. A paused
  stage keeps renewing its leases, so nothing is taken over.
- **Resuming** clears the row's pause. Routes restart within 5 s.
- **Drain** is a long operation: pause, then wait until `GET /drain` shows nothing claimed or in flight. Then it
  is `done`, and the stage can be stopped or upgraded safely.
- **It survives restarts**, because the state is in the database, not in a pod.
- **Autoscaling:** a paused stage's lag grows, and KEDA would add pods that also sit paused. The control pods have
  no Kubernetes rights and don't change the ScaledObject. The operator's runbook pauses KEDA too
  (`autoscaling.keda.sh/paused: "true"`), or accepts pods up to the stage's maximum. §13 lists this.

## 10. Audit

- Each call writes `sdh.audit.control.<operation>`, allowed or denied, with:
  - `sdh.operator` (the principal) and `sdh.auth` (`jwt`, `mtls`; `cli` from the CLI);
  - `sdh.permission`, `sdh.decision` (`allowed`, `denied`, `unauthenticated`);
  - `sdh.operation_id`, `sdh.approved_by` where there is one, `sdh.idempotent_replay` when a retry was answered
    from the first answer;
  - the operation's own attributes, as the CLI's events have them (item ID, counts, state), and `result`;
  - never a value, a patient identifier, a reason's or ticket's text beyond its checked characters, or a token.
- **Sealed:** each control pod writes its events to the stage's journal (DESIGN.md §12.3) in a chain of its own,
  and seals it with the stage's signing key, which it holds as the workers do. That is a change from the CLI,
  whose chains are unsealed. `audit-verify` checks control chains as any other.
- **Requests and approvals** are in both the journal and the operation record. The journal is the proof; the
  record is for the API to answer.

## 11. Keys, retention and erasure: what the operations do

**Rotate (P3):** sets the KEK for new data. The active KEK reference moves from settings to a row in
`sdh_control_state`, read by the workers as pause is. The setting stays as its starting value. New commit batches'
BDKs are wrapped under the new KEK. The old KEK still unwraps old data.

**Re-key (P3, four eyes):** moves stored data from one KEK to another, for each table the stage writes:
1. For each data file holding chunk rows with `kek_ref = from`: unwrap each row's `bdk_wrapped` with the old KEK,
   wrap it with the new, and write the file again as one Iceberg rewrite. The chunks' ciphertext doesn't change:
   only the wrapped BDKs do.
2. Verify: every row now has `kek_ref = to`, and a sample of chunks decrypts with its digest checked.
3. Expire the snapshots that still reference the old files, and remove the orphaned files.
4. Report: rows and files rewritten, and that nothing now references `from`. Only then may the key custodian
   disable the old KEK. **The product never disables or destroys a KEK:** that is the custodian's act, in the KMS.

**Retention (P4):** today a setting. It becomes a row in `sdh_control_state`, so it can be set without a
redeploy. `PUT /retention` sets it per table class (landed items, records, failures, gold), within bounds the
settings file gives (`sdh.retention.minimum`, `sdh.retention.maximum`), so a mistake can't erase what a law or a
contract says to keep. Maintenance applies it as now (DESIGN.md §12, `maintenance.retention`).

**Purge an item (P4, four eyes, pipeline-wide):** each stage deletes the item's rows from its tables, rewrites
the files that held them, expires the snapshots that still reference them, and removes the orphans. Only then is
its part `done`. The landing node also removes it from `landed_items`. The result lists, per stage, tables and row
counts. Copies already delivered out of the pipeline (to a FHIR server, for example) are the customer's to purge;
the result says so.

**Erase a pipeline (P4, four eyes, pipeline-wide):** at the end of a contract.
1. Each stage pauses and drains.
2. The audit journal is exported first (`audit-export`), to the operator's archive, because audit records are
   kept six years (§ 164.316(b)(2)) and hold no PHI.
3. Each stage drops its tables with their data files and removes its store prefixes, within its own rights.
4. The pipeline's control state is set to `erased`. Its stages then refuse to start.
5. The result is an **erasure certificate**: tables dropped, objects removed, the KEK and signing-key references
   for the custodian to destroy, and the journal's last head. It is signed with each stage's signing key. Destroying
   the KEKs makes any copy left anywhere (backups, snapshots) unreadable: that is the custodian's act, again.

None of these runs on its own. Purge and erase run only when asked, approved and reasoned; retention removes only
what is older than the policy in force. That keeps to the rule that the product never drops data on its own.

## 12. The contract

- **OpenAPI 3.1,** hand-written, in `connector-stage/src/main/resources/control/openapi-v1.yaml`. Each stage
  serves the parts it has; a part a stage hasn't (for example `resubmit`, except on the landing node) answers
  `404` with code `not-on-this-stage`.
- **Versioning:** `/api/v1` changes only by addition. A breaking change is `/api/v2`, served beside v1 for at least
  one release.
- **Contract tests:** the harness checks every response it gets against the contract (§14).
- **Compliance profile:** new checks, refused under `hipaa` unless waived:
  - `CONTROL_TLS`: control mode without TLS;
  - `CONTROL_AUTHENTICATION`: no issuer (JWT) or no trusted CA (mTLS);
  - `CONTROL_TOKEN_LIFETIME`: `max-lifetime` over 1 h;
  - `CLI_FOUR_EYES`: the CLI allowed to run four-eyes operations (§3).

## 13. Chosen here, for the user to review

- The CLI refuses four-eyes operations under the `hipaa` profile, unless waived (§3).
- `correct` needs `phi.export` as well as `pipeline.operate` (§4).
- A `keys.admin` can't register a sealing key for themselves (§7).
- The break-glass plaintext is held encrypted for one fetch by the requester, for at most an hour (§6).
- Rotation and retention move from settings to database rows, with settings as starting value and bounds (§11).
- Erasure exports the audit journal and keeps it; the product never destroys KEKs (§11).
- KEDA isn't paused by the API; the runbook does it (§9).
- No pipeline-wide endpoint; pipeline operations go to any stage and are run by each (§1, §8.2).

## 14. The test harness

**Where the code lives:** the client and scenarios are in a new module, `control-harness`, in this repository,
with a Testcontainers target for the stages here. `healthcare-data-transformer` depends on it and adds the
transform stage, to run the whole pipeline. This follows the repositories' dependency direction: this repository
depends on neither of the others.

**Parts:**
- **`ControlClient`:** a typed client of the contract, on `java.net.http`, with its own contract check of each
  answer. It is also what the console and the customer's tools can use as a reference.
- **`TestIssuer`:** signs short-lived JWTs for test principals with chosen roles, and serves their JWKS. Its image,
  `sdh-test-issuer`, is for the sandbox and AWS. It is never part of a customer deployment, and the reference
  settings don't name it.
- **`Unsealer`:** decrypts sealed results with a test principal's private key.
- **`Target`:** what a scenario runs against:

  | Target | Starts or reaches | Injects faults by |
  |---|---|---|
  | `TestcontainersTarget` | Kafka (SASL_SSL), Postgres (TLS), SeaweedFS, Vault, the test issuer, and each stage's image in worker and control modes | pausing and stopping containers |
  | `SandboxTarget` | the sandbox's Deployments, through its ingress | scaling to zero, deleting pods, a deny-all network policy, stopping a node's VM |
  | `AwsTarget` | the PoC stack's Deployments, through its internal ingress | the same, and Spot interruption |

- **Scenarios:** those listed in CONTROL-API.md, each one class, run by any target. Each checks the operation's
  answers, the pipeline's results, and the audit (`audit-verify` clean; every call's event present with its
  principal; the denied calls too).
- **Synthetic data only:** the CCDAtoFHIRSamples documents and generated variants, and malformed ones for the
  failure scenarios.
- **Running it:** `scripts/harness.sh <testcontainers|sandbox|aws> [scenario…]`. The Testcontainers level starts
  the sdh-it VM and stops it after (`SDH_IT_STOP=1`). The sandbox and AWS levels stop or take down what they started,
  as the load tests do.

## 15. Build order

| Step | Builds | Closes |
|---|---|---|
| C1 | The operation layer (§3), with the CLI on it. No change in behaviour: the CLI's tests pass unchanged. **Built 2026-11-20** (§15.1) | |
| C2 | Control mode: TLS, authentication, permissions, audit (§1, §2, §4, §10); the query operations; the OpenAPI file; the compliance checks. **Built 2026-11-21** (§15.2) | part of P1 |
| C3 | Actions, `Idempotency-Key`, the operation record and its leases (§5, §8). **Built 2026-11-21** (§15.3) | P1 |
| C4 | Sealing keys and sealed results (§7). **Built 2026-11-21** (§15.4) | P1 |
| C5 | Four eyes and break-glass (§6). **Built 2026-11-21** (§15.5) | P1 |
| C6 | Pause, resume and drain (§9). **Built 2026-11-21** (§15.6) | P2 |
| C7 | Rotate and re-key (§11). **Built 2026-11-21** (§15.7) | P3 |
| C8 | Retention, purge and erase (§11). **Built 2026-11-21** (§15.8) | P4 |
| C9 | Health, settings, compliance outcome, support bundle | P5 |
| H1 | The harness with its Testcontainers target, growing with C2–C9 | P6 |
| H2 | The sandbox target | P6 |
| H3 | The AWS target | P6 |

### 15.1 C1, as built

- **Types** (`connector-stage`, `stage.admin`):
  - `Operation<C, Q, R>`: `name`, `permission`, `permissions(request)`, `kind` (`QUERY`, `ACTION`, `LONG`, `SEALED`),
    `approval(request)` (`NONE`, `FOUR_EYES`), and `run(request, context, invocation)`;
  - `Permission`: the eight permissions of CONTROL-API.md, with their names in settings;
  - `Invocation`: the `Caller` (principal, and `CLI`, `JWT` or `MTLS`), the audit event, and a `Sink` for streamed
    rows and sealed parts;
  - `Given<T>`: what a request carries that is read only when the operation needs it (a reason, a corrected record,
    a public key), so the CLI reads standard input at the moment it always has;
  - `Partial<T>`: a part of a result read on its own, kept with its failure, as the CLI's status always showed one;
  - `CliOperation<C, Q, R>`: an operation with its CLI binding, `bind(arguments, context)` and
    `print(result, out)`. It is an `AdminCommand`, so `AdminCli` is unchanged.
- **Every command is an operation:** `status` (both), `stuck`, `drain-check`, `version`, `reprocess`, `failures`,
  `decline`, `reopen`, `correct`, `retry`, `export`, `patients`, `audit-verify`, `audit-export`, `audit-prune`,
  `restore`, `release` and `resubmit`. Each keeps its class, its options and its text.
  - Their requests and results are records. A result holds rows (registry rows, leases, failures, runs), not text.
  - What a result can't hold is streamed to the `Sink`: the audit journal's events as rows, and sealed parts.
  - `FailureAdmin.changed` returns its note instead of printing it.
- **What a caller sees differently:** a malformed option is now refused before anything is read or printed. Before,
  `status --item x` printed the duties lease first. A command that fails part way prints nothing of what it had read.
  Before, `drain-check` printed the releases first. Its exit status is the same.
- **Not yet:** nothing checks permissions or approvals; that is C2 and C5. `restore` writes to the CLI's directory
  through a `Destination` in its request; C4 adds a destination that streams. `OperationsTest` checks each
  command's permission, kind and approval against §5.

### 15.2 C2, as built

- **Control mode** (`connector-stage`, `stage.control`): `java -jar <stage>.jar control [--a.b=c …]` on the landing
  node, the split, convert and transform stages. `ControlServer` starts a servlet application of its own from the
  stage's settings class and `ControlConfiguration`, which imports only the web, Jackson and security
  auto-configurations it needs, and `ComplianceConfiguration`. It starts no Camel, lease or consumer.
  - **Workers stay off the network:** every stage now carries Spring MVC and Security. `ControlWebSwitch` sets a
    worker's web type to `none` unless its settings say otherwise, and the control process's to `servlet`. The
    landing node's `WebServerSwitch` leaves the control process alone.
  - **Each request opens what its operation reads** (the stage's admin context, or the audit journal) and closes
    it after, as a CLI run does.
- **Settings** (`ControlSettings`, `sdh.control.*`), as §2 shows them, with these differences:
  - `tls.enabled` (default `true`) and `tls.key-password`; `probe-port` (default 8081), plain HTTP, serving only
    `/livez` and `/readyz`; `auth.jwt.jwk-set-uri`, when the keys aren't found from the issuer;
  - mTLS takes the subject's name as principal, canonical (RFC 2253, lower case), as the landing node's push does.
    `auth.mtls.principal` (SAN e-mail or URI) isn't built.
  - In YAML, a permission's name and a certificate subject are map keys in brackets: `"[pipeline.view]"`,
    `"[CN=noc,O=Example]"`.
  - **Checked at startup:** the API doesn't start without an issuer and audience (JWT) or a trusted CA and TLS
    (mTLS), without a certificate and key when TLS is on, or with a permission name it doesn't know.
- **Authentication** (Spring Security, stateless): only an authenticated `GET` under `/api/v1/` is let through on
  the API's port.
  - **JWT:** signed with an RSA or EC algorithm by the issuer's keys (fetched on the first request), from the
    issuer, for the audience, in date, with issue and expiry times no more than `max-lifetime` apart, and a
    principal claim of at most 256 characters with no control characters. Roles come from the roles claim, a list
    or names separated by spaces.
  - **A refusal** is answered `401` with a problem body that names no detail, and audited as
    `sdh.audit.control.unauthenticated`, with `sdh.auth`, `sdh.decision` and the failure's class as `sdh.reason`.
- **Authorization and audit** (`ControlService`): a request is bound into the operation's request through the
  command's own `bind`, from the path variables and query parameters its route names. A parameter the route doesn't
  name is refused, so a reading path can't be given an option that acts. Then:
  - the permissions `permissions(request)` names must all be granted to one of the caller's roles, or it is `403`
    `permission-required`;
  - an operation needing four eyes is `403` `approval-required` until C5;
  - every request, allowed or refused, is audited as `sdh.audit.control.<operation>` with `sdh.operator`,
    `sdh.auth`, `sdh.permission`, `sdh.decision`, `sdh.request_id` (also the `X-Request-Id` header) and
    `sdh.result`; a failure by its class alone, in the event and the answer.
- **Audit journal** (`ControlJournal`): one chain for the server's life, of `<stage source>/control`, sealed with
  the stage's keys. The API doesn't start if it can't be opened: unlike a CLI query, a request is never served
  unjournaled.
- **Routes** (`ControlRoute`; the ingress strips `/pipelines/{pipeline}/stages/{stage}`):

  | Path (`GET /api/v1/…`) | Operation | Parameters |
  |---|---|---|
  | `status`, `items/{itemId}/status` | `status` | |
  | `stuck` | `stuck` | `idle` |
  | `drain` | `drain-check` | |
  | `version` | `version` (the listing only) | |
  | `failures` | `failures` | `itemId`, `all` |
  | `reprocess-runs/{runId}` | `reprocess --status` | |
  | `audit/verify` | `audit-verify` | `from`, `to`, `stale` |
  | `audit/events` | `audit-export`, as JSON lines ending `{"end": true, "rows": n}` | `from`, `to` |
  | `openapi.yaml` | the contract, to any authenticated caller | |

  Each answer is the operation's `Answering.answer`, written field by field (never by reflection), so a sealed
  reason or a record's content can't reach a caller by being added to a result.
- **Contract:** `connector-stage/src/main/resources/control/openapi-v1.yaml`, OpenAPI 3.1, for these routes.
- **Compliance checks** (connector-runtime), applied only with `sdh.control.mode` on: `control-tls` and
  `control-token-lifetime` (DESIGN.md §7.6). `CONTROL_AUTHENTICATION` isn't a check: the API never starts without a
  way to authenticate (above), so there is nothing to waive. `CLI_FOUR_EYES` comes with C5.
- **Tests:** `ControlServerTest` starts a test stage's API over TLS against a local JWKS and embedded Postgres, and
  checks the refusals (no token, another audience, expired, issued for two hours, another key), `403` without the
  permission, `400` for a parameter the route doesn't take or a malformed one, `404` for an operation the stage
  hasn't, the probes on their port only, client certificates (listed and not), the startup checks, each audit
  event's fields, and the API's chain in the journal. `ControlWebSwitchTest`; `ComplianceRulesTest` for the new
  checks.
- **Not yet:** the control Deployments, their Service, network policy and sandbox budget (§1); an `sdh-control`
  entry in the images; the customer documentation (HIPAA-OPERATIONS); actions, sealed results and four eyes (C3–C5);
  the harness (H1).

### 15.3 C3, as built

- **Actions** (`ControlRoute.actions()`), each the CLI command of the same name, bound through its own `bind`:

  | Path (`/api/v1/…`) | Operation | Body fields (besides `reason`) |
  |---|---|---|
  | `POST reprocess-runs` | `reprocess` | `itemIds` (a list), `seq`, `madeBy`, `landedFrom`, `landedTo`, `intake`, `dryRun` |
  | `DELETE reprocess-runs/{runId}` | `reprocess --cancel` | |
  | `POST failures/{itemId}/{seq}/retry` | `retry` | |
  | `POST failures/{itemId}/{seq}/decline`, `…/reopen` | `decline`, `reopen` | |
  | `POST failures/{itemId}/{seq}/correct` | `correct` | `record`, base64 |
  | `POST version/accept` | `version --accept` | |
  | `POST items/{itemId}/release` | `release` (file connector) | `resume`, `close` |
  | `POST items/{itemId}/resubmit` | `resubmit` (landing node) | `again` |

  §5's failure `release` is `reopen` here, the CLI's name for it.
- **The body** is a JSON object of at most 16 MiB (`413` over that). A field the route doesn't take, or one that
  repeats a path variable, is `400`. What the CLI reads from standard input, the reason or the corrected record,
  is read from the body instead (`AdminArgs.withInput`): the control process's own standard input is never read.
- **Every action needs a `reason`** of 1 to 200 letters, digits, spaces and `. , : _ # / -`. It is audited as
  `sdh.action_reason`, and kept on the operation's record. The commands that kept a reason before (decline,
  reopen, version accept, release) still keep it encrypted.
- **`Idempotency-Key`** (8 to 128 letters, digits and `. _ : -`) is required on every action, and checked after
  the permissions, so a refused caller claims nothing. `ControlStore` keeps each principal's key with the hash of
  the method, path and body, for 24 hours:
  - a retry with the same request gets the first answer back, status, body and `Location`, audited with
    `sdh.idempotent_replay` and result `replayed`, and isn't carried out again;
  - the key with another request is `409` `idempotency-key-reused`; while its first request is still running, `409`
    `in-progress`;
  - every answer is kept, a refusal by the stage (`409` `refused`) or a failure (`500` `failed`) too, each by its
    class alone.
- **Long operations** (`Followed`): `reprocess` (when it records a run) and `resubmit` are answered `202` with
  their record and `Location: /api/v1/operations/{id}`. `resubmit` asks the landing node and returns without
  waiting, as the CLI's waits.
  - **The record** (`sdh_control_operations`, beside the audit journal's tables, §8.1's columns) has the
    operation's identifiers (`runId`, `requestId`) as its parameters, never a value.
  - **It is brought up to date as it is read:** a record still `running` asks the stage where the run or request
    stands (`Followed.progress`). A reprocess run is `done` when its run is; a resubmission is `done` when the new
    item has landed, `failed` when the node refused it. If the stage can't be read, the record is answered as it
    stands, with `progressUnavailable` naming the failure's class. A finished record doesn't change.
  - `GET /api/v1/operations` lists the stage's records newest first, `limit` (1 to 1000, default 100) at a time,
    with an opaque `next` cursor; `GET /api/v1/operations/{id}` reads one. Both need `pipeline.view`, and are
    audited as `sdh.audit.control.operations`.
- **Leases:** `ControlStore.take`, `renew` and `release` let one control pod at a time run an approved or running
  operation, and another take it over once its lease runs out. Nothing uses them yet: every long operation here is
  carried out by the stage's own workers, and the record only follows it. The operations a control pod runs itself
  (C4's sealed exports, C7, C8) will.
- **Contract:** the actions, the operations paths, the `Idempotency-Key` header, the `Reasoned` body, the
  `Accepted` answer and the `Operation` schema are in `openapi-v1.yaml`.
- **Tests:** `ControlActionsTest` serves the fail store's commands and a followed test operation over TLS. It
  checks the refusals (no key, no reason, a reason outside its characters, a field the route doesn't take, no
  permission, an operation the stage hasn't), one decline per key with its replay and audit, a reused key, a
  correction from the body (needing `phi.export`), a `202` with its record brought up to date, the record when the
  stage can't be read, and the pages. `ControlStoreTest` checks the keys, the records and their pages, and the
  leases.
- **Not yet:** four eyes (C5: approve, reject and the `awaiting-approval` state); a runner for operations a
  control pod carries out; removing old operation records (the keys go after 24 hours; the records stay, like the
  audit journal, until C8's retention); everything §15.2 lists as not yet.

### 15.4 C4, as built

- **Sealing keys** (`OperatorKeys`, `sdh_operator_keys` in the pipeline's database beside the audit journal's
  tables, shared by every stage): fingerprint (the key's), principal, the PEM key, registered by and at, expiry,
  revoked by and at. A fingerprint names one key, and so one principal. A revoked key stays listed.
  - `GET /operator-keys[?principal=]` lists them without the keys; `POST /operator-keys` registers one
    (`principal`, `publicKey` PEM, `expiresAt`, `reason`, with an `Idempotency-Key`); `DELETE
    /operator-keys/{fingerprint}` revokes one. All need `keys.admin`, and are audited with `sdh.sealing_key` and
    `sdh.key_principal`.
  - A key is EC P-256 or P-384, or RSA of at least 3072 bits, and expires in the future within a year (366 days).
    A key registered already is `409 refused`.
  - **Never for oneself:** registering a key for the caller's own principal is `403 not-for-yourself`. Revoking
    one's own is allowed.
  - These are the control API's own operations (`OperatorKeyCommands`), not the CLI's: the CLI keeps reading a key
    from standard input.
- **Sealed routes**, each needing `phi.export`, a `reason` and `sealTo` (no `Idempotency-Key`: the parts aren't
  kept, and asking again reads again, audited again):

  | Path (`POST /api/v1/…`) | Operation | Body fields |
  |---|---|---|
  | `failures/{itemId}/{seq}/export` | `export` | |
  | `patients/lookup` | `patients` | `itemIds`, or `from` and `to`; `identifiers` (up to 1000, each up to 512 characters) |
  | `items/{itemId}/restore` | `restore` (split stage) | `fromOffset`, `unprocessed` |

  §5's `POST /failures/export` is per failure here, as the CLI's `export` is. A plaintext restore isn't served:
  it needs four eyes (C5).
- **`sealTo`** must name one of the caller's own keys, unexpired and not revoked, or it is `403
  sealing-key-not-yours`. The check comes after the permissions, so a caller without `phi.export` learns nothing
  of the keys. The server gives the registered key to the command in place of standard input; the command seals
  to it as the CLI does, and audits its fingerprint as `sdh.recipient_key`.
- **The stream** is `application/x-ndjson`: a header (`operation`, `sealedTo`, `requestId`), a line per part
  (`part`, `jwe`), and a trailer (`end: true`, `parts`, `sha256` over the parts' JWEs in order, as ASCII with
  nothing between them). A failure part way ends it with `end: false, incomplete: true` and no trailer, and the
  client discards it. The audit event carries `sdh.parts`. A failure before the first part is answered as any
  other refusal.
- **Part sizes:** `export` seals the failure's content as one part, the patient lookup a page of rows, and
  `restore` up to 1 MiB of the item per part (8 MiB in the CLI's file). The digest of a restored item is checked
  over the whole item. On a mismatch the stream ends without its trailer. The CLI's restore still leaves nothing
  behind.
- **Also changed:**
  - A command's own usage refusal at run time (a key too weak, a span too long) is `400` with its words, not
    `500`.
  - A request is audited before its answer is sent, so a caller holding an answer finds its event.
  - `AdminArgs.withOutputToCaller` tells a command its output goes to the caller. `restore` then takes no `--to`,
    `--original-name` or `--plaintext`.
- **Tests:** `ControlSealedTest` covers:
  - registering for someone else only, keys refused for being weak, expiring too late or already, or registered
    already;
  - listing, and revoking;
  - an export sealed to the caller's own key and opened with its private key, with its trailer's digest checked;
  - another's key, a revoked key and no key refused, and a caller without `phi.export` refused.

  `AdminCommandsTest` checks a restore streamed to the caller in parts.
- **Not yet:** four eyes, approval and plaintext break-glass (C5); a patient lookup tested through the API
  (its command is tested through the CLI); removing expired keys; everything §15.2 and §15.3 list as not yet.

### 15.5 C5, as built

- **Requests:** an action whose operation needs four eyes (`Operation.approval`) isn't carried out when asked.
  After the permission and idempotency checks it is recorded in `sdh_control_operations` as `awaiting-approval`
  and answered `202` with `Location`. Its parameters are what is needed to bind it again (the route, the path's
  variables, the body's fields without the reason), the permissions an approver needs, and `approveBy`. Its
  ticket, if any, goes in `ticket`. A break-glass request needs a ticket (`[A-Za-z0-9._:/-]{1,64}`). Audited as
  the operation with `sdh.outcome=awaiting-approval`.
  - **Routes:** `POST /audit/prune` (`before`), and `POST /items/{itemId}/restore/plaintext` (`ticket`,
    `fromOffset`, `unprocessed`). §5 had the break-glass restore at `…/restore` with `plaintext: true`; it has
    a path of its own here, since it answers `202` where the sealed restore streams.
- **Decisions:** `POST /operations/{id}/approve`, `/reject` and `/cancel` take a body with only a `reason`. They
  need no `Idempotency-Key`: each can be made once, and asking again is `409 wrong-state` with the state.
  - An approver or rejecter holds every permission the operation needs (`403 permission-required`) and isn't its
    requester (`403 own-request`). Principals are compared ignoring case, as for sealing keys.
  - Only the requester cancels (`403 not-your-request`), while the operation awaits approval or is approved and
    not yet taken by a pod.
  - Audited as `sdh.audit.control.operation_approve`, `_reject` and `_cancel`, with `sdh.requested_by`.
  - The record shows `decidedBy`, `decision` and `decidedAt`.
- **Expiry:** a request is expired when it is next read or decided after `approveBy`:
  `sdh.control.approval.ttl` (24 h), or `.break-glass-ttl` (1 h) when it needs `phi.break-glass`.
- **The runner** (`ControlRunner`, one per control pod) runs approved operations:
  - It looks every 10 s, and at once after an approval. It takes an operation's lease (2 min, renewed every
    30 s) through C3's `take` and `renew`, and runs one operation at a time.
  - It binds the operation again from its record and runs it as its requester, audited as the operation with
    `sdh.approved_by` and `sdh.operation_id`. It then finishes the record `done` with the operation's answer as
    its `result`, or `failed` with the failure's class.
  - If the pod stops, the lease runs out and another pod runs the operation again. A prune bears that, since
    pruning again prunes what is left. So does a restore, since its held result is replaced.
  - Only operations a second person approved are run here. The stage's workers still carry out reprocess and
    resubmit, and their records follow them.
- **Break-glass result** (`ControlResults`, `sdh_control_results` and `sdh_control_result_parts`):
  - The restore hands its plaintext to the runner in 1 MiB parts (`Invocation.Sink.plaintext`, which no other
    sink takes). Each result has its own AES-256 data key, wrapped with the stage's records KEK through the keys
    the control API's journal holds. Each part is AES-GCM, bound to its operation and place.
  - It is held for the break-glass TTL from when it finishes, and the record's `result` gives `resultParts`,
    `resultBytes`, `resultUntil` and `resultAt`. A failed run deletes what it held.
  - `GET /operations/{id}/result` needs `phi.break-glass` and the requester. It claims the result atomically, so
    it is fetched once, streams it as `application/octet-stream`, and deletes it whether or not it was sent whole.
    Otherwise it is `410 result-gone`. Audited as `sdh.audit.control.operation_result`, with `sdh.parts` and
    `sdh.bytes`.
  - The runner deletes results fetched, past their time, or left by a run that never finished (after a day).
  - A stage without key settings can't hold one, and the run fails.
- **The CLI** refuses an operation that needs four eyes under the `hipaa` profile (exit 1, audited with
  `sdh.decision=needs-approval`), unless `sdh.compliance.waive.cli-four-eyes` gives a reason.
  - New compliance check `cli-four-eyes` (`ComplianceRules.cliFourEyesAllowed`). It is found only when waived, so
    it never refuses a start, but its waiver is shown and audited at startup with every other.
- **Also:** `audit-prune` answers its counts (`Answering`). `ControlStore` gains `decide`, `cancel`, `expire`,
  `runnable` and `finish`, and its records carry the ticket, the decision and the result.
- **Tests:**
  - `ControlApprovalTest`: a prune approved by a second audit admin only, run by the pod and audited with both,
    with its replay; a rejection, a cancellation by the requester only, and an expiry; and a break-glass request
    needing a ticket, failing on a stage without keys, its result fetched by its requester only.
  - `ControlResultsTest`: a result encrypted at rest, fetched once in order and then gone; one late, unfinished
    or discarded isn't fetched; no KEK, none held.
  - `ControlStoreTest`: the decisions and the runnable queue. `ComplianceRulesTest`: the waiver.
    `StageAdminTest`: the CLI's refusal under the profile.
  - `AdminCommandsTest`: a plaintext restore handed to the runner in order.
- **Not yet:**
  - A break-glass result fetched end to end through the API on a stage with keys (each half is tested: the
    restore's parts, and the held result).
  - Pipeline-wide operations approved once (§8.2, with C7 and C8).
  - Stopping a run whose lease another pod has taken.
  - Everything §15.2–§15.4 list as not yet.

### 15.6 C6, as built

- **State** (`ControlState`, in `stage`): `sdh_control_state`, beside the audit journal, a row per scope that was
  ever paused: `paused`, `paused_by`, `paused_at`, `reason`, `updated_at`. The scope is what the stage's audit
  chains are of, `<stage>/<connector ID>` (`landing/<connector ID>` for a landing node, which is one intake).
  - A pause keeps who, when and the reason, which is checked as the API checks an action's reason (1 to 200
    letters, digits, spaces and `. , : _ # / -`). Pausing a paused scope keeps the first pause.
  - Resuming clears the pause and keeps the row, for the rows C7 and C8 add to it.
- **Pods** (`PauseWatch`): each reads its scope's row when it starts, before it takes work, and every 5 s after. A
  failed read keeps the state last read; a change that fails to apply is applied again at the next read. Each
  change is audited by the pod as `stage.paused` or `stage.resumed`, with `sdh.control_scope`.
  - The input route (`PausableRoute`, used by `StageReceiver` and the landing node) is suspended, which Camel does
    as it stops a route: the batch in hand finishes first, and a consumer that can't be suspended is stopped. A
    route paused before the pod starts isn't started until resumed. The broker watch doesn't restart a paused
    route.
  - The record stages' worker queues no reprocess object and applies no correction. The split stage's
    corrections loop takes no correction, release or reprocess object, but still sends unsent item ends.
  - The landing node lists nothing and takes no queue batch, answers pushes `503` with `Retry-After`, and
    carries out no resubmission.
  - Claims and leases are still renewed, items still looked at again, and the duties pod's reconciler and
    maintenance still run: what was taken finishes, and nothing is taken over.
- **Operations**, on every stage beside the audit commands (`ControlStateCommands`): `pause` (reason on standard
  input, or the body's `reason`), `resume` and `control-state`. Routes `POST /pause`, `POST /resume`,
  `GET /control-state`. Audited with `sdh.control_scope` and `sdh.paused` (whether it was paused before); a
  resume also with `sdh.requested_by`, who had paused it.
- **Drain** (`DrainCommand`, on the stages with a registry: split, convert, transform): pauses, then is drained
  once the stage has been paused for 15 s (three reads by every pod) and no pod holds a live claim. Live claims are
  the registry's records `CLAIMED` with an unexpired claim, or, for the split stage, the item leases
  (`split/<connector ID>/<item>`) unexpired (`JdbcLeaseStore.live`).
  - `POST /drain` is followed (`Followed`): answered `202`, and its record is `done` once drained, `failed` if the
    stage is resumed first. It isn't on the landing node (`404 not-on-this-stage`).
  - The CLI waits for it, for at most `--wait` minutes (10), and exits 1 if it isn't drained in time. The stage
    stays paused either way.
  - `GET /drain` stays the release check, `drain-check`, as C2 built it. §5 had it give what is still claimed;
    a drain's live claims are in its record's `progress` instead.
- **Tests:**
  - `ControlStateTest`: pause, a second pause, resume and scopes apart; a reason refused; the watch reading as it
    starts, telling each part of each change in order, keeping the state when a read fails, and applying a
    failed change again.
  - `PausableRouteTest`: a route paused before it starts, suspended, resumed, not restarted while paused, and
    left alone once stopped.
  - `DrainCommandsTest`: a drain waiting for the pause to settle and the claims to end, the CLI giving up after
    `--wait`, and a resume failing it.
  - `ControlActionsTest`: pause, the state, a second pause, resume, a viewer refused, and no drain on a stage
    without one, through the API.
  - `PushServiceTest`: a resubmission waiting while the intake is paused.
- **Not yet:**
  - Drain on the landing node.
  - A pause tested end to end on a running stage with a broker (the parts are tested: the route switch on a real
    Camel context, and the watch).
  - KEDA isn't paused by the API (§9, §13); the runbook does it.
  - Everything §15.2–§15.5 list as not yet.

### 15.7 C7, as built

- **The KEK in use** (`ControlState`): `sdh_control_state` gains, per scope, `kek`, `kek_by` and `kek_at`. A scope
  never rotated uses its setting's KEK: the records KEK for the split, convert and transform stages (the sink's,
  `sdh.sink.kek` if set), the landed items KEK for a landing node. The setting stays as the starting value.
- **Pods** (`KekWatch`): each reads its scope's KEK as it starts, before its first batch, and every 5 s after, as
  the pause is read. A KEK newly rotated to is probed first (a throwaway key wrapped and unwrapped). One the pod
  can't use is refused and audited once (`keys.kek_refused`, with `sdh.kek` and the failure's class), and the KEK
  in use is kept, so nothing is written the pod couldn't read back. One it can is taken and audited
  (`keys.kek_active`). A batch keeps the KEK it started with: `IngestPipeline.setActiveKek` (split),
  `RecordStageProcessor.Sink`'s `kek` (convert, transform) and `ItemLander.activeKek` (landing) read it as each
  batch starts. Operators' reasons and corrections are wrapped with it too (`reasonKek()`).
- **KEKs a pod may unwrap:** a KMS reaches any key its rights allow. A local provider (`wrap-provider: local`)
  loads only the KEKs it is given, so the new setting `sdh.crypto.keys.other-keks` lists the others, loaded to
  unwrap and to rotate to, and not probed at startup.
- **Operations** (`KeyCommands`, on every stage, the landing node included, all `keys.admin`):
  - `keys` (`GET /keys`): the KEK in use and whether it was rotated to, by whom and when; for each table the
    stage writes, and each database table holding wrapped keys, the rows under each KEK; each table's ID key's
    KEK; and the last re-key with its progress. It reads every table's `kek_ref` column.
  - `rotate --kek` (`POST /keys/rotate`): probes the KEK with the stage's rights, then records it. Rotating to the
    KEK in use changes nothing (`changed` false). Audited with `sdh.kek_from`, `sdh.kek_to`.
  - `rekey --from --to [--wait]` (`POST /keys/rekey`): four eyes (§6). Refused while `from` is the KEK in use
    ("rotate first"), or if the stage can't use `to`. Its run records the re-key under the operation's ID, so a
    run taken over by another control pod finds it rather than asking again, and waits until it is done or
    failed. The CLI waits for at most `--wait` minutes (until it ends if not given; `0` returns at once).
- **The re-key** (`RekeyPass`, on the pod holding the stage's duties, as a `MaintenanceScheduler.Pass`): at each
  maintenance tick (5 min), if a re-key is asked for, maintenance runs over every table at once, with a `Rekey`
  added to each table's purges, under the duties' commit fence.
  1. Per table, the data files the scan finds under `kek_ref = from` are copied, up to 4 GiB a run
     (`Rekey.BUDGET`), each row's `bdk_wrapped` unwrapped with the old KEK and wrapped with the new, checked to
     give the same key back; a batch's rows share one data key, re-wrapped once. The chunks' ciphertext doesn't
     change, and its AAD names no KEK. The files are replaced in one `RewriteFiles`, validated from the snapshot
     the run started at. A file with delete files is refused. The run's snapshot expiry deletes the old files.
  2. Once a run rewrites nothing, the database rows with wrapped keys (`WrappedRows`, `StageKeys`: failures and
     their events, corrections, reprocess runs, release acceptances, and on the split stage held items'
     releases) are re-wrapped 500 to a transaction, each update conditional on the row's old key; then the
     tables' ID keys (`IdKeys.rekey`, the same key under the new KEK).
  3. Only once nothing the stage stores is under `from` (`Rekey.left`, the rows, the ID keys) is the re-key
     `done`, audited by the duties pod as `keys.rekeyed` with the files and rows.
  - Progress is kept in the row, so a duties pod taking over carries on. A run that fails is tried at the next
    tick; after 12 in a row, the re-key is `failed`, with the failure's class (`keys.rekey_failed`).
  - The product never disables or destroys a KEK.
- **Tests:**
  - `RekeyTest` (connector-core): a lander rotated at its next batch; a re-key with a one-byte budget taking a
    file a run until quiet, with nothing left under the old KEK, the old files gone and every item reading back;
    the ID key moved and the same; bad arguments refused.
  - `KeyRotationTest` (connector-stage): the rotation and re-key rows (asked again by the same operation, another
    refused while one runs, progress, ends); the watch refusing a KEK it can't probe, once, taking one it can, and
    keeping it when a read fails; every stage table's rows counted and re-wrapped, another stage's left alone;
    the maintenance pass carrying a re-key out on `landed_items` and the database to `done`; failing after 12
    tries; and the commands (`rotate` probed and once, `keys`, `rekey` four eyes, refused from the KEK in use,
    `--wait 0`), with their routes.
- **Not done, chosen while building:**
  - A re-key is per stage, approved once per stage. §8.2's pipeline-wide approval is not built.
  - The check is a round trip of each data key re-wrapped, not §11's sample of chunks decrypted.
  - The control API's held results (`sdh_control_results`) are left out: each lives an hour at most, and the
    table is shared by every stage writing to the schema. Disable an old KEK an hour after the re-key at the
    earliest.
  - A re-key needs table maintenance enabled (`sdh.maintenance.enabled`); while it is off, the re-key stays
    `requested`.
  - `keys` reads every table's KEK column: slow on a large stage.
  - Re-keying a reader's tables when the KEK is shared across stages: each stage re-keys its own scope.
  - A rotation through a running stage end to end on a broker (the parts are tested: the watch, the writers' KEK
    supplier, the maintenance pass on a real table).
  - Everything §15.2–§15.6 list as not yet.

### 15.8 C8, as built

- **Retention** (`ControlState`, `RetentionPolicy`): `sdh_control_state` gains, per scope, `retention` (an ISO-8601
  duration, or `forever`), `retention_by` and `retention_at`. It is per stage, so per table class: landed items
  (landing), split records (split), records and gold (convert, transform, their sinks). A scope never set uses its
  setting's (`sdh.maintenance.retention`, `.retention.records`, `.retention.landed-items`). The bounds are the new
  settings `sdh.maintenance.retention-bounds.minimum` and `.maximum` (not §11's `sdh.retention.*`, so they sit
  beside the retention they bound), unset by default. The minimum is the longer of the bounds' and the stage's
  floor (1 day; 7 days for landed items); a maximum forbids forever. A setting outside the bounds refuses startup.
  - Maintenance reads the retention in force at each run, through `Target`'s supplier: the operator's if set,
    else the setting's, never below the minimum. One that can't be read keeps every row that run
    (`maintenance.retention_unread`), so a database outage never shortens it.
  - `retention` (`GET /retention`) and `retention-set --days | --forever` (`PUT /retention`; `PUT` added to the
    API's methods), both `data.admin`. Outside the bounds is `400`. Audited with `sdh.retention`.
  - The ledger and the failure tables keep their own rules: the ledger is kept forever, failures until handled.
- **Purge** (`PurgeRequests`, `PurgePass`, `ItemPurge`): `purge --item-id --ticket [--wait]`
  (`POST /items/{itemId}/purge`), four eyes, `data.admin`, the ticket required. Its run records the request in
  the new `sdh_control_purges` table (operation ID, scope, item, who, when, ticket, state, files, rows, failure),
  under the operation's ID so a run taken over finds it, and waits as a re-key's does. `purges [--limit]`
  (`GET /purges`) lists the latest. The row is kept once ended: the record that the item was purged, by item ID
  only.
  - `PurgePass`, a `MaintenanceScheduler.Pass` on the duties pod, takes every active purge of the scope (up to
    100) into one run, an `ItemPurge` added to each table. Per table, the data files whose `item_id` bounds may
    hold an item are copied without its rows, up to 4 GiB a run, counted per item and checked to add up; the
    files are replaced in one `RewriteFiles` validated from the run's start, and the run's snapshot expiry deletes
    the old ones. A file with delete files is refused.
  - Once a run rewrites nothing, each item's corrections (`stage_correction`, which hold a document's content)
    are deleted; then, only if no table holds a row of the item (`ItemPurge.left`), its purge is `done`, audited
    as `data.purged` with the files, rows and corrections deleted. Rows found by a quiet run count as a failed
    run. After 12 failed runs in a row the purges are `failed` (`data.purge_failed`).
  - The landing node purges `landed_items` and leaves its ledger, which holds no PHI.
  - `MaintenanceScheduler.Pass.of(rekey, purge)` takes turns, so a long re-key doesn't hold a purge back.
- **Erase** (`DataCommands.Erase`): `erase --ticket` (`POST /erase`), four eyes, `data.admin`. It runs in the
  control pod, the stage's admin rights. Refused unless the scope is paused; for a stage that drains (split,
  convert, transform), also until the pause is older than the drain's settle time with no live claim. Then:
  1. The KEKs (the setting's, the rotated, a re-key's, each table's rows' and ID key's, the database rows') and the
     journal's heads (`sdh_audit_chain` of the scope: chain, last seq, last hash) are read.
  2. The scope is marked erased (`erase_id`, `erased_by`, `erased_at`; the first erasure kept). Every runtime
     calls `refuseIfErased` at startup, before its first batch.
  3. Each table the stage writes is dropped with its data (`dropTable(id, true)`), then what is left under its
     location removed where the store can list (`SupportsPrefixOperations`, S3's), counted. Landing: `landed_items`
     and the ledger; split: `split_records`; convert and transform: the sink.
  4. The stage's rows in the database tables with wrapped keys (`StageKeys`) are deleted.
  5. The result is the certificate: a JWS (`sdh-erasure+jwt`, ES256) signed with the stage's own key, naming the
     scope, operation, requester, approver, ticket, the tables with their location and objects, rows deleted per
     table, the KEKs and the signing key for the custodian to destroy, and the journal's heads. Audited with
     `sdh.tables`, `sdh.objects`, `sdh.rows`. Asked again, it carries on with what is left.
- **Tests:** `DataLifecycleTest` (connector-stage): the retention and erasure rows; the policy's bounds, floor and
  fallback when the state can't be read; the purge pass on `landed_items` to `done` a file a run, with the other
  items reading back, the correction gone and the failure kept; failing after 12 tries; the commands (bounds,
  ticket, four eyes, `--wait 0`, `purges`) and their routes; and erase refused unpaused and unsettled, then
  dropping the table and only its own stage's rows, its certificate verified with the stage's public key.
  `MaintenanceSchedulerTest`: passes taking turns.
- **Not done, chosen while building:**
  - Purge and erase are per stage, approved once per stage. §8.2's pipeline-wide approval is not built: purge an
    item at each stage it passed through, landing included.
  - Failures are kept on purge: their reasons never quote a document's values (DESIGN.md). The failure tables
    have no retention of their own yet.
  - On erase, the registry's and checkpoints' rows (IDs, offsets, digests, no content) and the purge records are
    kept, as is the audit journal; export it first (`audit-export`), as §11 says. The erase does not export it.
  - A purge needs table maintenance enabled; while it is off, it stays `requested`. A pod retired while a purge
    runs is the next duties holder's to carry on.
  - Erase checks the pause itself; it does not drain the stage (`drain` does).
  - A store that can't list (the in-memory one in tests) leaves only what the table's metadata didn't name.
  - Everything §15.2–§15.7 list as not yet.

Each step adds its scenarios to the harness as it is built, and its customer documentation to HIPAA-OPERATIONS
(HIPAA.md §11.2 D13). C5's is in HIPAA-OPERATIONS §3.3 and §5; C6's in §7.1; C7's in §6.1; C8's in §8.
