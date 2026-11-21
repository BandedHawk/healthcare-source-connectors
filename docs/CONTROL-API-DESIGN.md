# The pipeline control API: design

Status: **Designed 2026-11-20 with the user; steps C1 (2026-11-20) and C2 (2026-11-21) built, the rest not built** · Puts into effect the intent in
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
| `phi.break-glass` | `POST /items/{itemId}/restore` with `plaintext: true` | a plaintext restore, with ticket and reason; **four eyes** | long | `restore --plaintext` |
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
`paused`, by whom, since when, and the reason.

- **Workers read it** when they start, before starting their routes, and every 5 s after.
- **Pausing** suspends the input routes: Kafka consumers stop polling, landing intakes stop listing, and the HTTP
  push answers `503` with `Retry-After`. Work already claimed finishes and commits. Nothing is dropped. A paused
  stage keeps renewing its leases, so nothing is taken over.
- **Resuming** clears the row. Routes restart within 5 s.
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
| C3 | Actions, `Idempotency-Key`, the operation record and its leases (§5, §8) | P1 |
| C4 | Sealing keys and sealed results (§7) | P1 |
| C5 | Four eyes and break-glass (§6) | P1 |
| C6 | Pause, resume and drain (§9) | P2 |
| C7 | Rotate and re-key (§11) | P3 |
| C8 | Retention, purge and erase (§11) | P4 |
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

Each step adds its scenarios to the harness as it is built, and its customer documentation to HIPAA-OPERATIONS
(HIPAA.md §11.2 D13).
