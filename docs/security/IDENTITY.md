# Identity and access

For the organisation that operates a pipeline: every identity that reaches it, how each is authenticated, what each
may do, and how access is granted, reviewed and taken away. It is HIPAA.md §11.2's document D2, for the access
control standard (§ 164.312(a)(1): unique user identification, emergency access, automatic logoff), person or entity
authentication (§ 164.312(d)), and workforce access management (§ 164.308(a)(3), (a)(4)). The roles are
HIPAA-OPERATIONS.md §1; the keys each identity may use are [KEYS.md](KEYS.md); what each audit event records of an
identity is [AUDIT-EVENTS.md](AUDIT-EVENTS.md).

It describes a pipeline deployed from `deploy/reference/` and the kits' READMEs, with the `hipaa` compliance profile
on.

## 1. The identities

| Who | Reaches | Authenticated by | Named in the audit as |
|---|---|---|---|
| **Each stage's workers** (landing, split, convert, transform), one identity per stage per pipeline | the object store, the KMS, Postgres, the broker | the platform's workload identity for the cloud (§2); a login role for Postgres; a SCRAM user or a client certificate for the broker | the stage and pipeline (`sdh.stage`, `sdh.connector`) |
| **Each stage's control API pods** | the same, as the stage's workers do | the same identity as the workers | — (they act for the person calling) |
| **People, through the control API** | one stage's control API, through the customer's ingress | an OIDC bearer token from the customer's identity provider (SSO, with MFA there), or a client certificate | `sdh.operator` (the token's `sub`, or the configured claim), `sdh.auth=jwt` or `mtls`, `sdh.access=api` |
| **People, through the CLI** | the stage's pod, by `kubectl exec` | the customer's Kubernetes authentication (SSO-backed) and RBAC on `pods/exec`; the CLI's own `--operator` is only a claim | `sdh.operator` (self-asserted), `sdh.auth=cli`, `sdh.access=cli`; the Kubernetes audit log names who ran `exec` |
| **Automation** (a NOC tool, a scheduler) | the control API | a client certificate from the configured CA, mapped to roles | its certificate subject, `sdh.auth=mtls` |
| **A producer that pushes** | the landing node's HTTP intake | an OAuth2 bearer token (issuer, audience, scope, subject) or a client certificate with a listed subject | never in plain: hashed with the idempotency key |
| **A producer's source the landing node pulls from** (SFTP, SMB, a bucket, a queue, a table) | — (the landing node reaches it) | the landing node's credentials on the producer's system, in the landing node's Secret, or its workload identity for a bucket | the intake (`sdh.intake`) |
| **Gold readers** (`connector-queue-reader` in each consuming application) | the broker's `<pipeline>.fhir`, the gold bucket, the FHIR KEK | the application's own workload identity, broker user and consumer key pair | the reader name and purpose it audits each read with (`sdh.reader`, `sdh.purpose`) |
| **Platform administrator, key custodians** | the cloud console and APIs, Kubernetes, the KMS, Postgres, the broker | the customer's own SSO and MFA, outside the product | the platform's logs (CloudTrail and its kin, the Kubernetes audit log) |

## 2. Workloads

### 2.1 One identity per stage, per pipeline

Each stage of each pipeline runs as an identity of its own in every system it reaches, granted only what that stage
does. A stage compromised is then that stage of that pipeline, and nothing else (HIPAA-OPERATIONS.md §2.8):

| Stage | Object store | KMS (KEYS.md §4.1) | Postgres (`deploy/reference/postgres.sql`) | Broker (`deploy/reference/kafka-acls.sh`) |
|---|---|---|---|---|
| Landing | write bronze (`landed_items`); read and delete in the intake bucket, for a bucket intake | landed-items KEK; its own signing key | login `landing_<p>`, acting as `catalog_<p>` | send `<p>.landed` |
| Split | read bronze, write silver (`split_records`) | landed-items KEK (unwrap), records KEK; its signing key | `splitter_<p>` | read `<p>.landed` (group `splitter-<p>`); send `<p>.split`, `<p>.landed` |
| Convert | read and write silver | records KEK; its signing key | `convert_<p>` | read `<p>.split`; send `<p>.records`, `<p>.convert.dlq`, `<p>.split` |
| Transform | read silver, write gold | records KEK (unwrap), FHIR KEK; its signing key | `fhir_<p>` | read `<p>.records`; send `<p>.fhir`, `<p>.fhir.dlq`, `<p>.records` |

Each stage sends to its own input as well as onward: a reprocess an operator asks for is queued by the stage's pods
to itself (DEPLOYMENT.md, "Broker rights"). The admin CLI sends nothing and holds no broker credentials.

The control API pods run as their stage's workers do, with the workers' Secret, keys and settings and nothing more
(each kit's README, "Control API"): the API's rights are the stage's, and what it does with them is limited by the
caller's permissions (§4).

### 2.2 No static cloud keys

A stage gets its cloud credentials from the platform: EKS Pod Identity or IRSA on AWS (the PoC uses Pod Identity,
`deploy/aws/terraform/compute/main.tf`), Workload Identity on Google Cloud, a managed or workload identity on Azure.
They are short-lived, renewed by the platform, and never in the stage's settings: the providers read them from the
default chain (DESIGN.md §7.4). Don't give a stage an access key.

**The secrets that remain** are not cloud keys, and are kept in each stage's Kubernetes Secret (or an external
secret store that fills it), never in its settings files (CONFIGURATION.md §3):

| Secret | Protects | Rotate by |
|---|---|---|
| Postgres login password (`SDH_LEASE_JDBC_PASSWORD` and the catalogs') | the stage's database role | `ALTER ROLE … PASSWORD`, then the Secret, then a rolling restart |
| Broker SCRAM password (`SDH_KAFKA_JAAS`), or a client certificate | the stage's broker user | a second SCRAM credential or certificate, the Secret, a restart, then remove the first |
| Inbox key passphrase (`SDH_CRYPTO_KEYS_INBOX_PASSPHRASE`) | the stage's inbox private key | replacing the key pair (KEYS.md §5) |
| Vault token or token file, with Vault Transit | the stage's Vault policy | Vault Agent renews the file; the stage re-reads it on every request |
| A pulled source's credentials (an SFTP key, an SMB password) | the landing node's login on the producer's system | with the producer, then the Secret and a restart |

Postgres can use the cloud identity instead of a password (IAM authentication on RDS and Aurora, Entra ID on Azure),
removing the first of these; the product hasn't been tried with it.

### 2.3 Inside one pipeline's database, the stages share a role

A Postgres JDBC catalog keeps every namespace in the same tables, so the stages of one pipeline each log in as their
own role but act as the pipeline's catalog role, which owns the catalog, the leases, the registries, the audit
journals and the control tables (`postgres.sql`). Pipelines are kept apart by having a database each, which only
their own stages may connect to. Within a pipeline, a stage that is taken over can change another stage's rows,
its audit journal included. The journal's hash chain and seals show such a change (`audit-verify`,
HIPAA-OPERATIONS.md §3.2), but don't prevent it. A REST catalog with grants per principal would separate them
(§9).

## 3. People

### 3.1 Through the control API: SSO, with MFA at the identity provider

The control API is the way people act on a pipeline. It takes, in `sdh.control.auth`:

- **`jwt` (for people):** an OIDC bearer token from the customer's identity provider, checked for signature
  (against the issuer's JWKS), issuer, audience, expiry and not-before. A token issued for longer than
  `max-lifetime` is refused, and the profile refuses a `max-lifetime` over an hour (`control-token-lifetime`). The
  caller is the `principal-claim` (`sub` by default); its roles are the `roles-claim` (`groups` by default).
- **`mtls` (for automation):** a client certificate from `trusted-ca`; the caller is its subject, and its roles
  come from `sdh.control.auth.mtls.roles`, since a certificate carries none.

There is no anonymous access: every path needs a credential, even the API's description and health; Kubernetes'
probes have a port of their own that serves nothing else. A refusal is audited (`control.unauthenticated` with its
reason class, or the operation's event with `decision=denied`).

**The identity provider's part,** which the product can't check:

- **MFA.** Require it for every sign-in to the API's client (a conditional access or authentication policy on its
  audience). The product doesn't read the token's `amr` or `acr` claims (§9).
- **Unique users.** One account per person; no shared accounts. The principal claim must be unique and never
  reused: `sub`, or an immutable ID; not an e-mail address that may be given to someone else later.
- **Automatic logoff.** The provider's session lifetime, and access tokens of at most an hour, which the API
  enforces.
- **Groups** for the roles of §4.2, managed by the platform administrator on the security official's say.

### 3.2 Through the CLI: the emergency path

The CLI (`kubectl exec` into a stage's pod) is for when the API can't be reached: the ingress, the identity provider
or the control pods down. Its operator name is self-asserted, so the proof of who ran a command is the Kubernetes
API server's audit of `pods/exec` (HIPAA-OPERATIONS.md §2.4).

- Grant `pods/exec` only through SSO-backed Kubernetes RBAC with MFA, to named people, per namespace.
- Send the API server's audit log to the SIEM; the analyst checks `admin.*` events against it.
- Under the `hipaa` profile the CLI refuses the operations that need four eyes (`cli-four-eyes`), since it can't
  prove two people. Recovery needs none of them.
- The CLI applies no permissions: whoever may `exec` into a stage's pod may run any of its commands. Grant `exec`
  as you would every permission of that stage.

### 3.3 Outside the product

The platform administrator and the key custodians work in the cloud's and the cluster's own consoles, under the
customer's SSO and MFA, and their actions are in the platform's logs, not the product's. Keep them apart
(KEYS.md §3): an administrator who can change IAM can grant themselves a KEK, so use the cloud's own controls (an
SCP or permissions boundary, a key policy that names its users; the PoC's deployer is denied decrypting and signing
in `deploy/aws/iam/sdh-poc-deployer-iam.json`) to stop
that, and alert on changes to them.

## 4. Permissions

### 4.1 What each permission allows

Nothing is granted unless `sdh.control.permissions` maps a role to it. Each operation needs the permissions below
(each command's `permission()` and `permissions(request)`):

| Permission | Operations | Needs four eyes |
|---|---|---|
| `pipeline.view` | `status`, `stuck`, `drain-check`, `version`, `control-state`, `health`, `settings`, `compliance`, `support-bundle`, `failures` (the list), `reprocess` (a run's status); the operations list | — |
| `pipeline.operate` | `pause`, `resume`, `drain`, `reprocess` (start and cancel), `retry`, `decline`, `reopen`, `correct` (with `phi.export`), `release` (split), `resubmit` (landing), `version` (to accept one) | — |
| `phi.export` | `export` (failures' content), `patients` (the patient lookup), `restore` (sealed, split), `correct` (with `pipeline.operate`, since whoever corrects a record has read it) | — |
| `phi.break-glass` | `restore` in plaintext (split) | yes |
| `audit.read` | `audit-verify`, `audit-export` | — |
| `audit.admin` | `audit-prune` | yes |
| `keys.admin` | `keys`, `rotate`, `rekey`; `operator-keys`, `operator-key-register`, `operator-key-revoke` | `rekey` |
| `data.admin` | `retention`, `retention-set`, `purges`, `purge`, `erase`, `restore-check`, `restore-repair`, `restore-release` | `purge`, `erase`, `restore-repair`, `restore-release` |

- **Approving** an operation needs the same permissions as asking for it, from another principal. Principals are
  compared as authenticated, so one person must map to one principal through JWT and mTLS alike.
- **A sealed result** (`export`, `patients`, `restore`) is sealed to one of the caller's own registered keys
  (`sealTo`); one registered for someone else is refused (`403 sealing-key-not-yours`).
- **A break-glass result** is fetched once, by its requester only, within an hour.
- **Registering a sealing key** for oneself is refused: a `keys.admin` registers keys for others only.

### 4.2 Mapping the customer's roles

A starting point, from the roles of HIPAA-OPERATIONS.md §1; the security official decides, in writing:

| Customer role | Permissions | Group (as the kits' READMEs name them) |
|---|---|---|
| Pipeline operator | `pipeline.view`, `pipeline.operate` | `sdh-operators` |
| Anyone who watches the pipeline | `pipeline.view` | `sdh-viewers` |
| A person the privacy official names to read PHI results (lookups, restores, failures' content) | `phi.export` | `sdh-phi-export` |
| Break-glass requesters and approvers (at least two) | `phi.break-glass` | `sdh-break-glass` |
| Security analyst | `audit.read` | `sdh-auditors` |
| Security official, for audit retention (at least two) | `audit.admin` | `sdh-audit-admins` |
| Key admins (at least two; not the key custodians, §5) | `keys.admin` | `sdh-key-admins` |
| Data admins, for retention, purges and erasure (at least two) | `data.admin` | `sdh-data-admins` |

Each permission that needs four eyes needs at least two holders, or its operations can never be approved.

## 5. Separation of duties

- **Four eyes** for what can't be undone or reads PHI in plaintext: `purge`, `erase`, `rekey`, `audit-prune`,
  `restore-repair`, `restore-release`, and a plaintext `restore`. The requester can't approve their own request;
  the approver sees its parameters, reason and ticket, never data.
- **Two people between anyone and PHI results:** an operator's sealing key is registered by a `keys.admin` who isn't
  that operator.
- **Keys:** the key custodians administer the KMS keys and can't use them; the stages use them and can't administer
  them; a `keys.admin` moves a stage to a KEK but can't create, grant or destroy one (KEYS.md §3). Don't make a key
  custodian a `keys.admin` as well.
- **Don't combine**, in one person: `phi.break-glass` with `audit.admin` (who could read PHI and then prune its
  record); `keys.admin` with `phi.export` (who could register a key for an accomplice and have them export);
  Kubernetes `exec` with `audit.admin`.
- **The CLI is outside these rules** (§3.2): keep `exec` to few, and review every use.

## 6. Emergency access

**Break-glass to PHI** (§ 164.312(a)(2)(ii)): a plaintext restore through the control API, asked by one holder of
`phi.break-glass` with a ticket and a reason, approved by a second, fetched once within an hour, all audited
(HIPAA-OPERATIONS.md §5). Most needs are met by a sealed restore, which needs only `phi.export`.

**Break-glass accounts, for when SSO is down.** The control API needs the identity provider; the CLI path needs
the cluster's authentication. Keep, for each:

- **A cluster credential that doesn't depend on SSO** (a client certificate for a `ClusterRole` that may `exec` into
  the stages' pods, or the cloud's emergency access to the cluster), sealed, split between two people or held in a
  vault that needs two, its use alerted on.
- **Automation's client certificate** for the control API (`mtls`), if the customer runs the API in `mtls` mode for
  automation, does not depend on SSO either: guard it as such an account, or give it only `pipeline.view`.

After any use: record it with its ticket, review the Kubernetes audit log and the `admin.*` events of the session,
and replace the credential.

## 7. Access reviews

At least quarterly, and when the security official asks, the platform administrator lists, and the security
official checks against who should have it:

| What | Where |
|---|---|
| Members of each group mapped to a permission | the identity provider |
| The permission mapping, and mTLS subjects and their roles | each stage's control ConfigMap (`sdh.control.permissions`, `auth.mtls.roles`); `GET /settings` shows it |
| Registered sealing keys: holders, expiries, revocations | `GET /api/v1/operator-keys`, against the security official's fingerprint register |
| Who may `exec` into the stages' pods | Kubernetes RBAC bindings in each pipeline's namespace |
| Each stage's cloud role and what it may reach | IAM (or its kin) and the KMS key policies, against §2.1 and KEYS.md §4.1 |
| Database roles and their grants | `\du` and `\l` on the Postgres server, against `postgres.sql` |
| Broker users and ACLs | Kafka's own `kafka-acls.sh --list`, against `deploy/reference/kafka-acls.sh` |
| Push senders and pulled sources' logins | `sdh.landing.push.oauth2.subject`, `.mtls.subjects`; the landing node's Secret |
| Gold readers: application, reader name, purpose | the privacy official's register (HIPAA-OPERATIONS.md §2.5) |
| Compliance waivers | `GET /compliance` on each stage |

Also compare what was used with what was granted: the principals and permissions in `control.*` events, the
`admin.*` events against the `exec` audit, and the KMS's log of each stage's calls. A grant not used in the period is
a candidate to remove.

## 8. Leavers and role changes

On the day a person leaves or changes role:

1. **Remove them from the identity provider's groups**, or disable the account. Tokens already issued keep working
   until they expire: an hour at most.
2. **Revoke their sealing keys:** `DELETE /api/v1/operator-keys/{fingerprint}` for each, by a `keys.admin`; the
   security official updates the register, and sealed files on their workstation are destroyed (HIPAA-OPERATIONS.md
   §8).
3. **Remove their Kubernetes RBAC bindings** and any cluster credential issued to them; revoke their client
   certificates (and remove their subjects from `auth.mtls.roles`).
4. **Their requests awaiting approval:** list them with `GET /api/v1/operations`, and reject any that shouldn't run
   (`POST /operations/{id}/reject`).
5. **If they could read secrets:** a platform administrator could read the stages' Secrets; rotate the database and
   broker passwords and replace the inbox key pairs (KEYS.md §5) on the security official's judgement. A key
   custodian: hand over the custody, and rotate to new KEKs if the custodian could use them (KEYS.md §5).
6. **Record it:** who, when, and each step done, with the access review's records.

## 9. Findings

- **The product doesn't check that a token came from an MFA sign-in.** It checks the token's signature, issuer,
  audience, lifetime and roles, but not `amr` or `acr`. MFA rests on the identity provider's policy (§3.1). A
  setting naming required `acr` values or `amr` methods would let the profile refuse a stage whose tokens can't
  prove it.
- **Within a pipeline, the stages share the database's catalog role** (§2.3), so one stage's identity can change
  another's registry and audit journal. The journal shows the change but doesn't prevent it.
- **The CLI applies no permissions** (§3.2): anyone who may `exec` into a stage's pod may run every command, four-eyes
  ones aside under the profile.
- **`kafka-acls.sh` grants nothing to gold readers.** A reader needs `Read` and `Describe` on `<p>.fhir` and `Read`
  on its own consumer group, which the broker team adds as the script's `reads` does for a stage.
- **HIPAA-OPERATIONS.md §2.4 predates the control API:** it says the product doesn't authenticate operators, which
  is now true of the CLI only. Corrected there.

## Sources

| Statement | Source |
|---|---|
| Control API authentication and settings | `connector-stage` `ControlSettings`, `ControlConfiguration`; CONTROL-API-DESIGN.md §2 |
| Each operation's permissions and four eyes | each command's `permission()`, `permissions(request)` and `approval(request)` (`connector-stage` `admin/`, `control/OperatorKeyCommands`; `file-connector` `RestoreCommand`, `ReleaseCommand`; `landing-connector` `ResubmitCommand`); `Permission` |
| Approval and sealing rules | `ControlService`; CONTROL-API-DESIGN.md §6, §7 |
| Token lifetime, CLI four eyes | `ComplianceCheck` (`control-token-lifetime`, `cli-four-eyes`) |
| Push authentication | DESIGN.md §5.3.15 |
| Database roles | `deploy/reference/postgres.sql` |
| Broker rights | `deploy/reference/kafka-acls.sh`; DEPLOYMENT.md, "Broker rights" |
| Control pods' rights | each kit's README, "Control API" |
| Workload identity on the PoC | `deploy/aws/terraform/compute/main.tf` (`aws_eks_pod_identity_association`), `persistent/iam.tf` |
