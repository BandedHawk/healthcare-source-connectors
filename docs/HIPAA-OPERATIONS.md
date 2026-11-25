# HIPAA operations: what the customer's staff do, with which tools

Status: **Draft for review** · Date: 2026-11-20 · Companion to [HIPAA.md](HIPAA.md), which maps each rule to the
product. This document is the customer's side: the roles, the procedures, and the product's commands and settings
each procedure uses. **It is not legal advice;** the customer's privacy and compliance counsel adapt it to their
own policies.

"The operator" is the customer, or the agent running the product for it (HIPAA.md §1). Commands are the admin
commands every stage ships (DESIGN.md §11.4), run as
`java @/app/jvm.options @/app/container.options @/app/admin.args <command> --operator <name> ...` through
`kubectl exec`, or from the distribution kit's `bin/` on a host.

## 1. Roles

The product assumes these roles exist. One person may hold several; the security official decides who holds which,
in writing (§ 164.308(a)(2), (a)(3)).

| Role | Does | Product access it needs |
|---|---|---|
| **Security official** | Owns the risk analysis, the policies, the compliance waivers, and this document's procedures. Approves break-glass access. | Reads the audit export; no PHI keys. |
| **Privacy official** | Handles requests for access, amendment and an accounting; leads breach risk assessment and notices. | The results of a patient lookup, decrypted by a person it names; no admin access itself. |
| **Platform administrator** | Runs Kubernetes, cloud IAM, Postgres, the broker and the KMS. Grants and revokes the roles below. | Cluster admin. **Never** KEK decrypt on bronze, silver or gold (the AWS PoC denies even the deployer). |
| **Pipeline operator** | Runs the admin commands: status, failures, restore, the patient lookup, the audit commands. | `kubectl exec` into the admin pod, under a named identity; holds an operator key pair (§2.3). |
| **Key custodian** | Holds the KMS keys: rotation, grants, destruction at end of contract. Two people, so no one can destroy alone. | KMS key administration, not use. |
| **Security analyst** | Reviews audit events in the SIEM, runs `audit-verify`, raises incidents. | The SIEM; `audit-verify` and `audit-export` (read-only). |
| **Data consumer owner** | Owns each application that reads gold with `connector-queue-reader`; sets its reader name and purpose. | The `gold-reader` grant for that application only. |

**Reasons and tickets.** Every action through the control API or the CLI takes a reason, and some a ticket. Both
are kept in plain in the control tables and the audit journal, so that reviewers can read them without a key
(docs/security/DATA-FLOWS.md §3). The API takes only letters, digits, spaces and a few marks, so a document's text
can't be pasted in, but nothing stops a name or a record number being typed. **Never put PHI in a reason or a
ticket:** refer to the item by its ID and to the request by the ticket system's reference.

## 2. Configuration before real PHI

Every setting below, with its safe value, is also in one table per area in
[security/CONFIGURATION.md](security/CONFIGURATION.md).

### 2.1 The compliance profile

Every stage starts under `sdh.compliance.profile: hipaa` unless told otherwise (DESIGN §7.6). It refuses to start on
a plaintext endpoint, unverified Postgres, a broker without TLS, local keys, no audit export, the durable audit
journal turned off, storage without authentication, or a JVM that may dump memory.

- **Do:** leave the profile on. The sandbox and AWS PoC manifests set `none` because they hold synthetic data; a
  customer's manifests must not copy that line.
- **Waivers:** an addressable choice that leaves a check failing is recorded as
  `sdh.compliance.waive.<check>: <one-line reason>`. Each waiver is audited at every start
  (`compliance.waived`), which is the documentation § 164.306(d)(3) asks for. The security official approves each
  one and files it with the risk analysis.

### 2.2 The audit journal and export

Each stage writes every audit event to two places:

1. **The durable journal** (DESIGN §12.3), in the stage's own Postgres schema: tables `sdh_audit_chain`,
   `sdh_audit_event` and `sdh_audit_seal`. Each process writes a hash chain of its own; the pods seal its head with
   the stage's signing key every minute and when they stop. No event is ever dropped: if Postgres is unreachable the
   events queue, and when the queue is full the stage waits rather than loses one. Settings, under
   `sdh.telemetry.journal`: `enabled` (true; the profile refuses `false`), `capacity` (50000 events), `batch-size`
   (500), `seal-interval` (1m), `linger` (0s: how long a batch waits for more events before it is written;
   PERFORMANCE.md §13 measured 100ms taking the journal's extra Postgres CPU away, at up to 100 ms more before an
   event is durable).
2. **The OTLP export** to the operator's collector, for the SIEM (DESIGN §12.1), and from there to a separate
   store (§2.7). It is best effort; the journal is the record of truth, and `audit-export` fills any gap (§3.2).

The events hold no PHI: IDs, codes, counts and positions only (HIPAA.md §4.1). The journal needs no PHI key to read.

### 2.3 Operator key pairs

Commands that write PHI out (`restore`, `patients`, `failures export`) seal it to the public key of the person who
asked for it; only that person's private key reads it.

- Each pipeline operator, and each person the privacy official names to read lookups, makes an EC P-256 key pair on
  a managed workstation, with the private key encrypted, for example
  `openssl ecparam -name prime256v1 -genkey | openssl pkcs8 -topk8 -v2 aes-256-cbc -out operator-private.pem`, then
  `openssl ec -in operator-private.pem -pubout -out operator-public.pem`.
- The security official records each public key's fingerprint against its holder. Every command that seals output
  audits the fingerprint (`sdh.recipient_key`), so the audit names whose key could read it.
- Private keys never go onto the cluster. Decrypting is done on the workstation, with a JOSE tool such as
  `step crypto jwe decrypt --key operator-private.pem`.

### 2.4 Human identity

People act through the control API, which authenticates each caller with the customer's identity provider (SSO,
with MFA required there) and grants by permission: [security/IDENTITY.md](security/IDENTITY.md) has the identities,
the permissions, the role mapping, access reviews and leavers. The CLI is the emergency path, and its `--operator`
name (or `$SDH_OPERATOR`) is self-asserted (HIPAA.md §8.1 item 3). For the CLI:

- Grant `kubectl exec` on admin pods only through SSO-backed Kubernetes RBAC with MFA, to named people.
- Turn on the Kubernetes API server's audit log for `pods/exec`, and send it to the SIEM. That log, not
  `--operator`, is the proof of who ran a command. The analyst checks the two agree (§3.1).
- Session timeouts on the console, `kubectl` credentials and SSO are the automatic logoff (§ 164.312(a)(2)(iii)).

### 2.5 Applications that read gold

Every application that reads records with `connector-queue-reader` audits each record it opens (DESIGN §10.2):

```java
QueueReader.builder()
        .auditAs("claims-service")        // a service account, never a person
        .purpose("HPAYMT")                // HL7 purpose of use: TREAT, HPAYMT, HOPERAT, PUBHLTH, ...
        .audit(new TelemetryReadAudit())  // or leave out: the sdh.audit log
        ...
```

- The data consumer owner chooses the reader name and the purpose, and registers both with the privacy official.
- A reader for anything other than treatment, payment or operations (public health, research, law enforcement) must
  say so in its purpose, because those reads may need an accounting (§4.3).
- By default each read goes to the application's `sdh.audit` log; with `TelemetryReadAudit` it goes through the
  application's own OpenTelemetry SDK and, if it installs one, a `JdbcAuditJournal`. Either way the application's
  owner sends it to the SIEM and keeps it.

### 2.6 Transport and storage settings

See DEPLOYMENT.md for each transport. Start from `deploy/reference/`: each stage's settings with the profile on
and nothing waived, Kafka over `SASL_SSL` and Postgres with `verify-full`, as the product's tests run them. The
profile checks the settings; the platform administrator also confirms:

- Kafka with TLS and SASL, and ACLs giving each stage only its topics;
- Postgres with `sslmode=verify-full`;
- KMS keys, never a local keystore, with rotation on;
- object store versioning on bronze at least, and the buckets refusing plaintext requests;
- CloudTrail (or the provider's equivalent) data events on the buckets and KMS keys, to the SIEM.

#### Securing the broker

ACLs bind only the principals they name. A listener whose principal is a super user, such as a plaintext listener
when `User:ANONYMOUS` is in `super.users`, lets anyone who reaches it read and write every pipeline's topics, and
change the ACLs. The product's broker tests run a broker that takes clients over `SASL_SSL` only (HIPAA.md §8.1,
item 7). Before real PHI:
- **No plaintext listener** for clients. Remove it, rather than relying on the network to keep clients from it.
- **The controller and broker-to-broker listeners over TLS** too, each with a principal of its own: a client
  certificate (`SSL` with `ssl.client.auth=required`) or SCRAM.
- **`super.users` names only those principals,** and a named administrator kept for break-glass, never
  `User:ANONYMOUS` or a stage's principal. Set `allow.everyone.if.no.acl.found=false`.
- **Each stage's principal has only the rights `deploy/reference/kafka-acls.sh` grants,** one per stage and
  pipeline, with passwords or keys from the secret store.
- **Only the pipeline's namespaces reach the broker** (a network policy, or security groups), and the admin
  tools are run by the broker team only.
- **ACL and user changes are audited:** the broker's authorizer log (`kafka.authorizer.logger`) to the SIEM, and
  reviewed with the daily checks (§3.1).
- **On a managed broker** (AWS MSK, Confluent Cloud): unauthenticated access off, client authentication (IAM,
  SCRAM or mTLS) with TLS only, and the provider's own ACLs or policies.

A plaintext listener bound to the broker's loopback (`127.0.0.1`), as the AWS PoC keeps for its controller and
admin tools (AWS-POC.md §8.2), is reached only by whoever can run commands on the broker's host or pod: treat that
access as broker administration, and give those listeners TLS and principals of their own in production.

If a plaintext listener must stay for a while, as on a shared broker being moved over, give its principal no
rights on any pipeline's topics (no super user), keep it on a network no stage's namespace shares, and record it as
a risk in the risk analysis until it is removed.

### 2.7 A separate store for audit events

The journal (§2.2) is durable, but it sits in the same Postgres as the pipeline's own state. NIST SP 800-66r2
asks where audit information resides (HIPAA.md §3.1), and a whole deleted chain shows only against a copy
held elsewhere (§3.2). The product's part ends at the OTLP export: the profile refuses to start a stage with no
`sdh.telemetry.otlp.endpoint` (check `audit-export`). From there on, the **operator's collector** carries the
events to a store the pipeline's own staff can't change. Any OTLP/HTTP receiver will do. The examples below use
the OpenTelemetry Collector (the contrib build, or a provider's distribution of it such as ADOT on AWS).

**What arrives.** Audit events come as OTLP log records, each with `event.name = sdh.audit.<event>` (for
example `sdh.audit.item.landed`, `sdh.audit.reader.record_read`, `sdh.audit.admin.restore`). They carry only
allow-listed attributes, and their body is the event name (DESIGN §12). Traces and metrics come on the same
endpoint. None of it holds PHI, so the profile allows plain HTTP from a stage to a collector inside the cluster.
The collector's onward link to the store must use TLS.

#### Collector settings for every store

1. **A logs pipeline that keeps everything.** `otlp` receiver → `memory_limiter` and `batch` → the store's
   exporter. Never use `nop` there. Never put a sampling or `filter` processor on the logs pipeline, because one
   that drops `sdh.audit.*` records is an audit gap.
2. **Ride out an outage of the store.** On the exporter, give `sending_queue` a `storage: file_storage` extension
   on a persistent volume, so a queue survives a collector restart. Set `retry_on_failure.max_elapsed_time: 0`,
   which means retry without a limit. Size the volume for the longest outage you plan to ride out; at the PoC's
   rates, audit events are a few hundred bytes each. Past that point, `audit-export` fills the gap (§3.3).
3. **More than one collector.** Run two replicas behind the Service, on different nodes. A stage's export is
   best effort: what was in flight when a collector stopped is still in the journal, and the comparison in §3.2
   finds it.
4. **Network.** A NetworkPolicy lets the pipeline namespaces reach the collector on 4318. The collector's egress
   goes only to the store's endpoint (and to DNS).
5. **A collector outside the cluster.** Use `https://` in `sdh.telemetry.otlp.endpoint`, and put a token in
   `sdh.telemetry.otlp.headers` (for example `Authorization`), taken from a Secret. The collector's receiver
   requires the token (the `bearertokenauth` or `basicauth` extension).
6. **At the store:**
   - Retention set to counsel's period (§3.3).
   - Immutability where the store offers it: a retention lock, or object lock.
   - Encryption with the customer's key.
   - Read access for security analysts. Delete rights for nobody who runs the pipeline: the platform
     administrator and the pipeline operators can't remove what records their own actions.
7. **The same store for the platform's own logs:** the Kubernetes API server audit log (§2.4) and the cloud
   provider's audit trail (§2.6). The analyst needs all three side by side.

#### By provider

| Store | Collector exporter | Identity and rights | Network | Retention, key, lock |
|---|---|---|---|---|
| **AWS CloudWatch Logs** | `awscloudwatchlogs` (`log_group_name`, `log_stream_name`) | An IAM role for the collector's service account (IRSA or EKS Pod Identity), limited to `logs:CreateLogStream` and `logs:PutLogEvents` on the group's ARN (`arn:aws:logs:<region>:<account>:log-group:<group>:*`). Create the group in advance, so the role needs no `logs:CreateLogGroup`. | In private subnets, an interface VPC endpoint `com.amazonaws.<region>.logs`; egress on 443 | The group's retention in days; a KMS key whose policy lets `logs.<region>.amazonaws.com` use it; no `logs:DeleteLogGroup` or `logs:PutRetentionPolicy` for pipeline roles. For a locked archive, add a subscription to Firehose and then to an S3 bucket with Object Lock. |
| **Google Cloud Logging** | `googlecloud` | Workload Identity: the collector's Kubernetes service account acts as a Google service account with `roles/logging.logWriter` | Private Google Access on the nodes' subnet | A log bucket of its own, with a sink routing these logs to it; the bucket's retention; a locked bucket; CMEK |
| **Azure Monitor** | `azuremonitor`, to Application Insights on a workspace-based Log Analytics workspace | The connection string, held in a Kubernetes Secret. Use Microsoft Entra authentication instead, where the exporter version supports it. | Azure Monitor Private Link Scope (AMPLS) | The workspace table's retention, and its archive; a customer-managed key through a dedicated cluster |
| **Splunk** | `splunk_hec` | A HEC token in a Secret, for an index of its own | TLS to HEC, with the customer's CA | The index's retention; frozen buckets archived |
| **Elastic** | `elasticsearch`, or `otlphttp` to Elastic's OTLP intake | An API key in a Secret, allowed to write one data stream | TLS, with the customer's CA | An ILM policy; snapshots to a locked repository |
| **Grafana Loki** (as the sandbox does) | `otlphttp` to Loki's OTLP endpoint | A tenant ID and token through `headers` | TLS | Loki's retention; object storage with a lock |
| **Any other SIEM** | `otlphttp` if it accepts OTLP; otherwise `syslog` (RFC 5424 over TLS) | Whatever the SIEM issues, in a Secret | TLS | The SIEM's own |

**Without Kubernetes** (the distribution kit on hosts), run the collector on each host, or on one host nearby,
and point `sdh.telemetry.otlp.endpoint` at it. The settings above apply unchanged, with a file-storage directory
on local disk.

The exporters and their options belong to the collector, not to the product. Check them against the documentation
for the collector version you deploy. **Only the Loki path has been run** (the sandbox); the others have not been
tried in our environments. Example configurations for each store are to come (HIPAA.md §8.1 item 15).

#### Checking it works

Run once after setup, and again after each collector or store change:

1. Run a read-only command on each stage (`status`). Within a minute, find its `sdh.audit.admin.status` event in
   the store.
2. For one day, count each chain's events in the store, and compare with `audit-export --from <day> --to
   <next day>` (§3.2). They should match.
3. Block the collector's egress to the store for ten minutes, then restore it. The events from the outage arrive
   late, with none missing (the comparison of step 2 again). Record the result with the risk analysis.

**Where our own deployments stand.** The sandbox sends audit events to its in-cluster Loki
(`deploy/sandbox/infra/telemetry.yaml`), which shows the path but is not a separate store. The AWS PoC's collector
still exports logs to `nop` (`deploy/aws/k8s/infra/telemetry.yaml`), so its audit events are only in the journal.
Fixing that is the CloudWatch row above, which needs the new IAM role (HIPAA.md §8.5).

### 2.8 Isolating pipelines

Run a pipeline of its own for each covered entity an agent serves, each component of a hybrid entity
(§ 164.105), a clearinghouse function (§ 164.308(a)(4)(ii)(A)), and Part 2 records (HIPAA.md §4.5). Give each
pipeline its own:

| Resource | Its own | Shared server, kept apart by |
|---|---|---|
| Database | `sdh_<pipeline>`: the Iceberg JDBC catalog, the leases, the registries, the audit journal | Only the pipeline's stage roles have `CONNECT` (`deploy/reference/postgres.sql`). Not schemas alone: a JDBC catalog keeps every namespace in the same tables, so any stage of any pipeline in one database could change another's catalog entries. A REST catalog with grants per principal (Polaris) is the other way. |
| Buckets | bronze, silver and gold for the pipeline | Each stage's role may use only the buckets it needs (SANDBOX.md, "Medallion layers"), and only its own pipeline's |
| Keys | the KEKs (landed items, records, gold) and each stage's signing key | KMS key policies, or Vault policies, naming each stage's identity (scripts/sandbox.sh shows the Vault form) |
| Broker | the topics `<pipeline>.*`, and an identity per stage | ACLs per stage (`deploy/reference/kafka-acls.sh`; DEPLOYMENT.md, "Broker rights") |
| Kubernetes | a namespace, with its quota and default-deny network policies | RBAC: the platform administrator grants `exec` per namespace (§2.4) |

Then:
- **Ending a contract** destroys the pipeline's KEKs, which erases its data cryptographically (§8).
- **A reader of gold** is given that pipeline's consumer key only (§2.5).
- **Checked by the product's tests:**
  - `PipelineIsolationIT` lays out two pipelines this way on one Postgres, one object store and one Vault. Each
    pipeline's split stage reaches its own data and is refused the other's database, buckets and keys. A data key
    wrapped under the other's KEK can't be opened with its own.
  - `KafkaSecurityIT` does the same for topics and consumer groups.
- **Checked by the platform administrator,** after setting up each new pipeline: as one of its stages,
  try to read another pipeline's bucket and connect to its database, and record the refusals with the risk
  analysis.

### 2.9 PHI in memory while it is processed

**What is there.** Every stage holds the PHI it is working on in plain text in its process memory: a document
being landed, split or converted, and the records of a batch waiting to be written. A stage gathers records into
batches before it writes them. That changes how many records are in memory at once,
and for how long, by about a second, not whether PHI is there: no program can parse, map or check a document it
cannot read. The same memory holds keys: each data key once unwrapped by the KMS, the stage's inbox private key
(which opens the messages addressed to it), its record-ID key, its pod signing key (DESIGN.md §7, §10.1), and its
short-lived cloud credentials from the workload identity. So **whoever can read a stage's memory can read what it
is processing, the data under the data keys it holds, and the messages sent to it; and, until those credentials
expire, do whatever the stage's own role may**: read its buckets and ask the KMS to unwrap their data keys. The
KEKs themselves never leave the KMS, so a copy of memory taken away is no key to the stored data once the
credentials have expired, and every KMS call made with them is in CloudTrail. This is why each stage's role is
kept to its own buckets and keys (§2.8).

HIPAA asks for encryption of PHI at rest and in transit, not in memory. Memory is covered by the access controls
on the host, the process and the people who can reach them (§ 164.312(a)(1), § 164.310), and by the risk analysis
(§ 164.308(a)(1)(ii)(A)), which should name it.

**What the product does,** as shipped and in `deploy/reference/`:

- Nothing in memory is written to disk: no heap dumps, core dumps or `hs_err` files; an out-of-memory process
  exits and is restarted clean; JVM attach is off, so no tool can join the process (DESIGN.md §7.5, §14.1).
- A read-only root filesystem; the one writable directory, `/tmp`, holds only what is already encrypted.
- Non-root, no capabilities, no privilege escalation, no shell in the image.
- No PHI in logs, metrics, traces or audit events: no document values, object keys or exception messages.
- A batch is acknowledged only once it is stored, so a process killed with records in memory loses none: they
  are delivered again.
- The byte buffers the code holds plaintext in (a decrypted item, a batch being encrypted, an unwrapped data key)
  are overwritten once used. Java gives no such promise for every copy (the garbage collector moves objects, and
  parsed text lives in strings that can't be cleared), so this shortens the window, no more.

**What the customer does to reduce it further.** In rough order of value for effort:

| Action | What it closes |
|---|---|
| **No swap on the nodes,** or encrypted swap (Kubernetes nodes run without swap by default; keep it so) | Memory paged out to a disk |
| **No kernel crash dumps** on the nodes (`kdump` off, or dumps to an encrypted, access-controlled volume), and no node-level memory capture tools | The whole node's memory written to disk on a kernel crash |
| **Encrypted node volumes** (EBS encryption, or the provider's equivalent) | Anything that reaches a node's disk all the same |
| **Restricted Pod Security Standard** enforced on the pipeline namespaces | A privileged or `hostPID` pod on the same node reading another process's memory |
| **`exec`, `attach`, `debug` (ephemeral containers) and `port-forward` on the pipeline namespaces granted to named staff only,** audited through the API server's audit log (§2.4) | The realistic way in: a person with cluster rights joining a running stage |
| **No interactive access to the nodes:** no SSH, Systems Manager sessions only for named staff, audited | Root on a node can read every process on it |
| **Dedicated nodes for the pipeline** (taints and node selectors, or a node group of its own), shared with no other workload | A compromise of another tenant's pod reaching the same kernel |
| **Patched nodes and images,** to a CIS benchmark (§11) | Kernel and runtime flaws that let a container read across processes |
| **Network policies and private endpoints** (§11) | An attacker reaching a stage at all |
| **Key and bucket policies that accept a stage's identity only from the pipeline's network** (on AWS, `aws:SourceVpce` or `aws:SourceVpc` conditions on the KMS key and bucket policies), and SIEM alerts on KMS use from anywhere else | Credentials copied out of memory and used from outside |
| **Short-lived pods:** routine node rotation, which Spot and the autoscaler already give | How long one process's keys and buffers stay readable |
| **Confidential computing,** where the threat model includes the cloud provider's staff or a compromised hypervisor: AMD SEV-SNP or Intel TDX instances (on AWS, SEV-SNP on supported instance types; on Azure and GCP, confidential VMs and node pools), with remote attestation checked before the node joins | The hypervisor and host reading the VM's memory: memory is encrypted by the CPU with a key the host never sees. Not tested with the product; it runs unchanged on such nodes, at a cost in price and some speed. |

AWS Nitro Enclaves are not a fit as they stand: an enclave has no network or disk, so a stage would have to be
rebuilt around a narrow channel to its host. Not offered.

**The limit.** A process can only work on what it can read, so while a document is being processed it is in plain
text in that process's memory, and anything with the process's own rights can read it. No setting removes that:

- **The process itself.** A flaw that lets an attacker run code in a stage (remote code execution in the stage, or
  in a library it uses) reads that stage's memory and keys, on any hardware. Defences here are preventing and
  detecting it: patching, scanning, the signed-message checks that refuse what a stage did not expect, network
  policies, and the audit trail. Confidential computing does not help: the attacker is inside the protected VM.
- **Whoever runs the code.** The operator's own administrators, with node root or `exec`, can read it; the
  controls on them are policy, least privilege, MFA and audit (§1, §2.4), not technology that stops them.
- **Whoever holds the hardware.** Without confidential computing, the cloud provider's hypervisor can; that
  trust is placed in its BAA and its attestations. With it, the trust moves to the CPU vendor and its firmware,
  which has had flaws of its own (side channels, firmware bugs); it is reduced, not removed.
- **Processing without reading** (homomorphic encryption, secure multi-party computation) is not practical for
  parsing and mapping clinical documents at this volume, and is not on the product's roadmap.

So the exposure can be made small, short and watched, but not zero. Record it in the risk analysis as: PHI and
the stages' working keys are in plain text in the stages' memory while processed; mitigated by the controls above;
residual risk accepted, owned by the security official.

### Registry retention

The stages' registries are kept for ever unless the customer sets `sdh.stage.retention.after`
(DEPLOYMENT.md "Registry retention"), which deletes the convert and transform stages' rows of items finished
longer ago, never an item with an open failure, never stored data, the landing ledger, failures or the audit
journal. The registry holds no PHI. Before setting it, record the choice and its reason with the customer's
records-management policy: after pruning, a message replayed by hand would be processed again, and registry
queries say nothing of pruned items. Each run is audited, `stage.registry_pruned`, with a count.

## 3. Audit controls and activity review (§ 164.312(b), § 164.308(a)(1)(ii)(D))

### 3.1 Daily: the SIEM

What each event and field means is in the audit event catalogue,
[security/AUDIT-EVENTS.md](security/AUDIT-EVENTS.md); its rows marked **Review** are the ones below, and others
worth an alert. The security analyst reviews, at least:

- `connector.fatal`, `lease.lost`, `lease.fenced_table`, `stage.message_rejected`: possible attack or fault;
- `compliance.waived`, `compliance.off` and `compliance.refused`;
- every `admin.*` event, matched to the Kubernetes exec audit by time and pod;
- `admin.restore` with `sdh.outcome=plaintext` (§5): each must have its ticket;
- `admin.patients` runs: each must match a request on the privacy official's log;
- `reader.record_read` from an unregistered reader, or with an unexpected purpose;
- `audit.journal_waiting`, `audit.journal_write_failed`, `audit.journal_not_closed` in the stage logs: the journal
  falling behind.

Each review is recorded (who, when, what was found): § 164.316(b)(1)(ii).

### 3.2 Weekly: verify the journal

On each stage's admin:

```
audit-verify --from <last week's date>
```

It checks each chain written in the span from its start: every entry's hash and number, each chain's head, and
every seal against the stage's signing key.

- **Exit 0, "no changes found":** record the result.
- **`CHANGED`** (`GAP`, `BROKEN`, `HEAD_MISSING`, `HEAD_MISMATCH`, `SEAL_INVALID`, `SEAL_MISMATCH`, `SEAL_ORPHAN`;
  exit 1): someone with write access to Postgres changed or deleted audit entries. Treat it as a security incident
  (§6).
- **Warnings:** `UNSEALED_TAIL` (entries after the last seal: a pod killed between seals, or an admin run) and
  `UNCLOSED` (a process that stopped without closing its chain, such as a node lost). Expected after a crash; check
  each against the platform's record of pod terminations.

What it can't show: a whole chain deleted. Compare the chains `audit-verify` lists with a copy kept outside the
database: the archived `audit-export` lines (§3.3), which name each event's `chain` and `seq`. The OTLP export
doesn't carry them yet ([security/AUDIT-EVENTS.md](security/AUDIT-EVENTS.md) §9), so the SIEM's own copy can't be
compared chain by chain. Admin runs' chains are not sealed (the admin has no stage signing key), so they rest on the
hash chain and the copy outside.

### 3.3 Export and retention

`audit-export --from <date> --to <date>` prints the journal's events as JSON lines, with each entry's hash, for the
SIEM or an archive. HIPAA sets no period for audit logs (HIPAA.md §3.1); counsel sets one. To keep the journal at
that period:

```
audit-export --to 2026-01-01 > audit-2025.jsonl     # archive first
audit-prune --before 2026-01-01
```

`audit-prune` removes whole chains last written before the time, so what is left still verifies. It refuses a time
less than 10 minutes ago.

Pruning needs a second person. Through the control API, an audit admin asks (`POST /audit/prune`, with `before`
and a `reason`), and the answer is an operation awaiting approval. Another audit admin approves it
(`POST /operations/{id}/approve`, with a `reason`), and the control pod then runs it. The requester can't approve
their own request. A request not approved within 24 hours expires. Under the `hipaa` profile the CLI refuses
`audit-prune`, because it can't prove two people. If the control API can't be reached and a prune can't wait, the
security official may waive `cli-four-eyes` for that run (`--sdh.compliance.waive.cli-four-eyes="<reason>"`).
The waiver is audited.

## 4. Requests from individuals (Privacy Rule)

### 4.1 Finding a patient's records (§§ 164.524, 164.526, 164.528)

The privacy official logs the request, then asks a pipeline operator to run the lookup on the transform stage:

```
printf '%s\n' "$(cat privacy-reader-public.pem)" 'urn:oid:2.16.840.1.113883.19.5|998991' \
  | kubectl exec -i <admin-pod> -- java ... patients --operator <name> --from 2026-10-01 --to 2026-10-31
```

- Standard input is the public key of the person who will read the result, then one identifier a line
  (`system|value`, or a value alone for any system). Identifiers never go in the arguments, which the platform logs.
- The span is at most 31 days; run it once a month of the period asked for, or by item with `--item-id`.
- The output shows only counts. The result is sealed to the key, on lines starting `sdh-export: `: JSON lines of
  item, seq, record ID and the patients' identifiers.
- Every record read is audited (`reader.record_read`, reader `sdh-admin`), and the run is audited with the key's
  fingerprint and the count, never an identifier.

With the item and record IDs, the privacy official's team reads the records from gold through its usual reader, and:

- **access:** provides them, or the FHIR repository's copy of them;
- **amendment:** amends in the FHIR repository, linking the amendment; nothing in the pipeline is overwritten
  (HIPAA.md §4.2);
- **accounting:** searches the SIEM for `reader.record_read` events on those record IDs with a purpose other than
  `TREAT`, `HPAYMT` or `HOPERAT`, and for gold reads in the provider's data events.

### 4.2 Minimum necessary

The data consumer owner gives each reader the `gold-reader` grant only for the pipeline it needs, and reads only the
records its purpose needs. Each pipeline has its own keys, so a reader of one can't read another (HIPAA.md §8.1
item 9).

## 5. Emergency access: break-glass restore (§ 164.312(a)(2)(ii))

`restore` on the split stage decrypts a landed item from bronze, checks its digest, and writes it out. It is the way
to get a source document back when the pipeline can't process it.

1. **Ticket:** the pipeline operator opens a change or incident ticket. The security official approves it.
2. **Sealed (the default):**

   ```
   kubectl exec -i <admin-pod> -- java ... restore --operator <name> --item-id <id> --to /tmp < operator-public.pem
   ```

   writes `/tmp/<id>.jwe`: one JWE a line, each a part of up to 8 MiB in order, its protected header naming the
   item, the part number, its offset and whether it is the last. Copy it out through a debug container
   (`file-connector` README), decrypt each line on the workstation, join them, and delete `/tmp/<id>.jwe`.
3. **Plaintext, only if needed** (a tool must read it), and only with a second person:
   - **Through the control API:** the requester, who holds `phi.break-glass`, sends
     `POST /items/{itemId}/restore/plaintext` with the `ticket` and a `reason`. The answer is an operation awaiting
     approval. A second person with `phi.break-glass` checks the ticket and approves the operation
     (`POST /operations/{id}/approve`). The control pod restores the item and holds the plaintext, encrypted under
     the stage's records KEK. The requester then fetches it once, within an hour, from
     `GET /operations/{id}/result`, and it is deleted. If the fetch breaks part way, ask again. A request not
     approved within an hour expires.
   - **Through the CLI**, only when the API can't be reached: `restore --plaintext --ticket CHG-1234 ...`. Under
     the `hipaa` profile the CLI refuses this unless the security official waives `cli-four-eyes` for the run,
     because it can't prove two people. The ticket and the waiver are audited. The file is the operator's to
     delete when done; the security official checks it was.
4. **Close the ticket** with the time of the `admin.restore` or `control.restore` audit event. Through the API, use
   the time of `control.operation_result` too.

`status` and the audit commands work when the KMS can't be reached; `restore` needs it.

## 6. Security incidents and breaches (§ 164.308(a)(6), Subpart D)

1. **Detect:** the SIEM reviews (§3.1), `audit-verify` (§3.2), the platform's own alerts.
2. **Scope by ID:** the audit events name the items, tables and records an incident touched; the provider's data
   events show whether a KEK decrypted anything. None of this needs PHI.
3. **Name the individuals** (§ 164.410(c)(1)): run `patients` with `--item-id <the items>` or the incident's span
   and no identifiers. It lists every record with its patients' identifiers, sealed to the privacy official's key.
   For who read them, search `reader.record_read` for those record IDs.
4. **Assess:** the privacy official assesses the four factors of § 164.402(2) with counsel. Encrypted data with an
   unbreached key is secured (HIPAA.md §5).
5. **Notify:** the covered entity's notices (§§ 164.404–408), or, for a business associate, notice to the covered
   entity within 60 days (§ 164.410). Keep the audit export, the `audit-verify` result and the KMS logs: § 164.414(b)
   puts the burden of proof on the operator.

### 6.1 Rotating a KEK and re-keying what is stored (§ 164.308(a)(6), § 164.312(a)(2)(iv))

Rotate when a KEK may be compromised, when a key custodian leaves, or on the covered entity's schedule. A KMS's
own automatic rotation (a new version of the same key) needs none of this: old versions still decrypt. These
steps move a stage to a different key. Each stage is a scope of its own (`<stage>/<connector ID>`, or
`landing/<connector ID>`), with its own KEK: the records KEK for the split and convert stages, the FHIR KEK (its sink's) for the transform
stage, the landed items KEK for a landing node. Every key, who holds it and its key policy are in
[security/KEYS.md](security/KEYS.md); replacing a signing key or an inbox key pair is its §5.

1. **The key custodians** create the new KEK and grant it, to wrap and unwrap, to the stage's own identity, and,
   to unwrap, to everything that reads the stage's tables: the split stage reads `landed_items`, the convert stage
   `split_records`, the transform stage the convert stage's records, and gold's readers (§2.5) the transform
   stage's. With local keys (`wrap-provider: local`), the platform administrator adds the new KEK to
   `sdh.crypto.keys.other-keks` of the stage and of each reader, and restarts them.
2. **Rotate**, with `keys.admin`:

   ```
   POST /api/v1/keys/rotate   {"kek": "alias/sdh-<pipeline>-records-2", "reason": "INC-1234 key rotation"}
   GET  /api/v1/keys
   ```

   The stage checks it can use the KEK first and refuses one it can't. Within seconds each pod checks it again
   and wraps its next batch with it. A pod that can't use it keeps the old KEK and records `keys.kek_refused`:
   fix its grant, and it takes the new KEK at its next read. From now, nothing new is under the old KEK.
3. **Re-key** what is stored, with a second key admin's approval:

   ```
   POST /api/v1/keys/rekey    {"from": "alias/sdh-<pipeline>-records", "to": "alias/sdh-<pipeline>-records-2",
                               "reason": "INC-1234 re-key"}
   POST /api/v1/operations/{id}/approve      (another key admin)
   GET  /api/v1/keys                         (its progress)
   ```

   The stage's maintenance does it, on the pod holding its duties, up to 4 GiB of each table every maintenance
   run (5 minutes), so a large stage takes hours. Table maintenance must be on. The stage keeps working
   meanwhile. The operation is `done` only once nothing the stage stores is under the old KEK: its tables' rows,
   its database rows (failures, corrections, reprocess runs, release acceptances) and its ID keys. After 12
   failed runs in a row it is `failed`, with the failure's class: fix the cause (often a KMS grant) and ask
   again.
4. **Disable the old KEK** once `GET /keys` shows no rows under it on every stage that used it, and at least an
   hour after the last re-key, when the control API's held results have expired. The product never disables or
   destroys a KEK: the custodians do it in the KMS, two people, and keep it disabled, not deleted, for as long as
   backups made before the re-key are kept, since those are still under it.

The same through the CLI, in the stage's pod: `keys`, `rotate --kek <reference>`, and `rekey --from <reference>
--to <reference> [--wait <minutes>]` (refused under the `hipaa` profile, which needs two people). Each is audited
(`sdh.audit.control.rotate`, `.rekey`, with `sdh.kek_from`, `sdh.kek_to`); the pods record `keys.kek_active`,
`keys.kek_refused`, `keys.rekeyed` and `keys.rekey_failed`.

## 7. Backups, recovery and contingency (§ 164.308(a)(7))

How to back up a pipeline and restore it so that nothing landed is lost and nothing is stored twice (DESIGN.md §18;
HIPAA.md D8). The backups are the platform administrator's; the product's part is the order of the restore and the
check after it.

### What to back up

| What | Where | How | Kept for |
|---|---|---|---|
| The catalog, registries, leases, control state, operators' sealing keys and the audit journal | The pipeline's Postgres database | Point-in-time recovery: WAL archiving, or the cloud database's own (RDS, Cloud SQL, Azure Flexible Server) | Your recovery window: 7 to 35 days |
| Every table's data, manifests and metadata: bronze above all, since once an item has landed it is the only copy | The object store | **Versioning on** for every bucket, with noncurrent versions kept at least as long as the database's window; object lock against deletion; replication to another region for a region's loss | At least the database's window |
| KEKs, signing and ID keys | The KMS | Never deleted while any backup needs them; multi-region keys for a region's loss | As long as any backup encrypted under them |
| The stage's settings and secrets, and the deployment's manifests | Your configuration management | Yours | |
| Exported audit events | Your audit store (§2.7) | Yours | Six years |

Not backed up: Kafka's messages, which the stages rebuild from the tables. Iceberg snapshots are not a backup: they
expire after `snapshot-retention` (24 h), in the same bucket.

**Versioning, once on, can't be fully turned off on S3:** a bucket can only be suspended. Its versions are kept,
and from then on an overwrite or a delete replaces the single "null" version, so nothing written later can be
restored. Object Lock needs versioning on for good. GCS and Azure Blob versioning can be turned off, and keep the
versions already made. Plan the noncurrent-version lifecycle rule before you turn it on: until a rule removes
them, every version deleted or overwritten is stored, and paid for, at the bucket's usual price. A pipeline's
maintenance deletes files every day (compaction, snapshot expiry), so noncurrent versions grow with the load,
not only with what was landed.

### Protecting the backups

The backups are a second copy of the PHI and of what protects it: the object store's versions hold every
encrypted object, and the database's backups hold the catalog, the registries, the audit journal and the
operators' sealing keys. They are only as safe as the identities that can change them. **The gap:** the product
neither makes the backups nor can check who may change them. Out of the box, two exposures are likely:
- **The database's backup identity can usually overwrite what it wrote.** A WAL archiver or backup agent needs to
  write; if the same identity may overwrite or delete, an attacker holding it (or a fault in it) can destroy the
  restore points without anyone deleting the database. Where the archiver runs beside the database (a sidecar in
  the database's pod, for instance), every container in that pod usually gets its credentials too.
- **Administrators can delete versions.** Versioning protects against the stages' mistakes and an operator's
  delete, not against an identity allowed `s3:DeleteObjectVersion`, `s3:PutBucketVersioning` or
  `s3:PutLifecycleConfiguration`: such an identity can remove every version at once, or suspend versioning.

To close or narrow them:
- **Write once.** Turn on S3 Object Lock in compliance mode (or GCS bucket retention with a locked policy, or
  Azure immutable storage) on the buckets and the backup store, with a retention at least the recovery window.
  Iceberg's deletes still work, as delete markers; no one can remove a version early, an administrator included.
  At the least, deny overwrites of backup files: a bucket policy that refuses `s3:PutObject` without
  `If-None-Match`, or a versioned backup bucket.
- **Backups out of reach of the production identities:** a backup vault or bucket in another account (AWS Backup
  with Vault Lock, or replication to a bucket the production account can't change), and another region for a
  region's loss.
- **Least privilege:** the database's backup identity may write new backups and read them, never delete them; no
  stage, operator or deployer role in production has `DeleteObjectVersion`, `PutBucketVersioning` or
  `PutLifecycleConfiguration` on the pipeline's buckets. Keep those for a break-glass role, used with a ticket.
- **MFA delete** on the versioned buckets (S3), where Object Lock isn't used.
- **Alerts** on `PutBucketVersioning`, `PutBucketLifecycleConfiguration`, `DeleteObjectVersion`,
  `PutBucketPolicy` and `PutObjectLockConfiguration` (CloudTrail management and data events, to the SIEM), and on a
  backup that didn't arrive (§7.2).
- **A restore test** proves the backups (below); record in the risk analysis which of these are in place.

### Restoring the database to an earlier point

For a corrupt or deleted database, a bad migration, or rows changed in error. **The object store is not rolled
back:** it holds everything landed since, which the intakes no longer have. Open a ticket first, and record in it
the time T restored to; every step below is audited under it.

1. **Stop every stage's workers** (scale their Deployments to 0, and pause KEDA: §7.1), and leave the control API
   Deployments running. Wait for their leases to end: on the way out, or after their TTL (60 s by default).
2. **Restore the database to T,** as a new instance, and point the stages' settings at it. If the catalog is in a
   database apart from the registries, restore both to the same T: the check can't tell a catalog newer than its
   registries.
3. **Check that versioning is still on** for every bucket of the pipeline, and its noncurrent versions kept, and
   leave the store as it is. Under the hipaa profile a stage won't start on a bucket without versioning
   (`store-versioning`); the lifecycle rule is yours to check.
4. **Restart the control APIs** on the restored database, then, on each stage, with `data.admin`:

   ```
   GET  /api/v1/restore/check
   ```

   It changes nothing, and refuses `400` while a lease of the stage is live. It answers, for each table the stage
   writes, the files its metadata needs and whether they are there, the files nothing references, and, for the
   landing node, the newer metadata its tables will be brought forward to; then the control changes made since T
   that the database lacks, each KEK and whether it can be used, and `whole`. Names, counts and times only: no
   record or value. Before repairing:
   - a table `unloadable`: its current metadata file is gone; restore that object to its last version with the
     store's tools, and check again;
   - files `missing` on GCS or Azure Blob: the product brings back deleted files on S3 only; restore those with
     the store's tools, and check again. A file whose versions have expired can't be brought back: restore to a
     later T, or contact support;
   - `rivals` on the landing node: two newest metadata files of one table, both complete; contact support, who
     will help choose;
   - a KEK that can't be used: the key custodian enables it again (a re-key since T had it disabled, §6.1).
5. **Repair each stage,** with four eyes and the ticket:

   ```
   POST /api/v1/restore/repair   {"reason": "INC-1234 restore to T", "ticket": "INC-1234"}
   ```

   A second data admin approves it (`POST /api/v1/operations/{id}/approve`). It brings back from the store's
   versions every file the restored catalog needs that was deleted since; points the landing node's tables at their
   newest metadata, so no item landed since T is lost, and raises its leases' epochs to match; keeps every file
   nothing references, which are the work done since T and still PHI under the pipeline's keys, until step 9; asks
   again the purges made since T, so purged records stay purged, and marks again an erasure made since; and tries
   each KEK. Its result is the report of step 4: go on only when it says `"whole": true`.
6. **Make again what the repair reports `to-redo`:** each key rotation, re-key or retention change made since T
   (only the latest of each kind counts), once the stage is running. A stage erased since T is erased again and its
   pods refuse to start: ask `erase` again (§8) to drop its tables.
7. **Pause or resume** each stage as wanted: the report says whether it is paused as restored (pauses aren't kept in
   the store).
8. **Start the workers,** and resume KEDA. The stages do the work since T again, from the tables: the split
   stage splits each landed item it doesn't know, and each sender sends again the item ends it hadn't announced. A
   record stored in gold since T is stored again with the same record ID and version: tell the owners of gold's
   readers to take a record ID and version they have seen as a repeat. Watch `/status` and `/stuck` until every
   item is complete. With the default settings, this takes a while: the split stage takes an item without its message
   once it landed more than `sdh.landing.reconcile.grace` (10 minutes) ago, at runs `sdh.landing.reconcile.interval`
   (15 minutes) apart, so an item landed just before T may wait 25 minutes; the convert and transform stages first
   wait `sdh.reconcile.backlog-quiet` (15 minutes) for their queues, then use their own grace and interval
   (DEPLOYMENT.md, "Keeping items from being stranded").
9. **Release the kept files** once the pipeline has caught up, with four eyes and the ticket:
   `POST /api/v1/restore/release`. Each stage's maintenance removes, at its next run, the files nothing references.
10. **Record the result** in the ticket: T, the counts each report gave, and how long each step took.

**The audit journal goes back to T with the database:** every event between T and the restore is then in the
exported audit events only (§2.7), so the export must be on before you need it. Keep that export for the review of
the incident, and run `audit-verify` on each stage after the restore: its chains check out as at T.

The same through the CLI, in a pod of the stage's control API: `restore-check`, `restore-repair --ticket <ref>`
and `restore-release --ticket <ref>`; under the hipaa profile the CLI refuses the last two unless `cli-four-eyes` is
waived.

**What it gives:** no item that landed is lost, whatever T is, and work since T is done again exactly once. How
long it takes is the database's restore, then each stage's check (a walk of every table's files), then the
pipeline's catching up; it has not yet been measured at size.

### Other losses

| Loss | What to do |
|---|---|
| Objects deleted from the store, the database whole | Steps 1 and 4 to 9 above, without restoring the database: the repair brings back each deleted file the catalog needs from its versions. An overwritten object is restored to its last good version with the store's tools. |
| A region | The database from its cross-region copy, the store's replica and multi-region keys, then the steps above in the other region. Items landed within the replication's lag whose intake was in the lost region may be lost. |
| Kafka | Make the topics again and start the stages: the reconcilers rebuild what was in flight. |
| A KEK destroyed | Nothing under it can be read again. Keys are never deleted while a backup needs them (§8). |

**Test a restore once a year,** and after each upgrade that changes a table or the registries, and record the
result. `scripts/harness.sh testcontainers restore` runs the steps above on a throwaway pipeline.

### 7.1 Pausing and draining a stage (§ 164.308(a)(7)(ii)(C))

Pause a stage to keep it from taking new work while something is wrong downstream, during maintenance, or before
an upgrade. A pause is kept in the database, so pods that restart or are added stay paused until someone resumes.
What a pod had already taken finishes and is stored, and nothing is dropped. Messages wait on the broker; a landing
node lists nothing and answers pushes `503` with `Retry-After`, so senders try again later.

Through the control API, with `pipeline.operate`:

```
POST /api/v1/pause      {"reason": "INC-1234 FHIR server down"}
GET  /api/v1/control-state
POST /api/v1/resume     {"reason": "INC-1234 resolved"}
```

Each action needs an `Idempotency-Key`. The same through the CLI, run in the stage's pod:

```
pause < reason.txt
control-state
resume
```

Before stopping or upgrading the split, convert or transform stage, drain it: `POST /api/v1/drain` (or `drain`
in the CLI) pauses it, then its operation record is `done` once no pod holds a claim. The CLI waits up to
`--wait` minutes (10) and exits 1 if the stage isn't drained by then. The stage stays paused after a drain: resume
it once the upgrade is done.

KEDA keeps adding pods while a paused stage's backlog grows, and the control API can't stop it. While a stage is
paused for longer than a few minutes, the platform administrator pauses its autoscaling too
(`kubectl annotate scaledobject <stage> autoscaling.keda.sh/paused="true"`), and removes the annotation after
the resume. Otherwise the stage grows to its maximum pods, all idle.

Each pause and resume is audited (`sdh.audit.control.pause`, `.resume`, or `sdh.audit.admin.*` from the CLI),
with who and the reason, and each pod records when it paused and resumed (`stage.paused`, `stage.resumed`).

### 7.2 Watching the database's capacity (§ 164.308(a)(7)(ii)(B), § 164.312(c)(1))

A database whose storage fills stops taking writes: Postgres stops when it can't write its WAL. The stages then
fail and retry, as in a database outage. Nothing landed is lost, but nothing moves until there is room again, and
the audit journal waits too. The product doesn't watch the database's storage: that is the platform team's.
WAL archiving makes it likelier: if the archive can't be written, Postgres keeps every segment until it can, and
they fill the same storage.

Monitor, and alert on:

| What | Where | Alert |
|---|---|---|
| Free storage | The volume's free bytes (the node exporter or kubelet volume stats); RDS `FreeStorageSpace`; Cloud SQL `disk/utilization`; Azure `storage_percent` | Warn at 25% free, page at 10%, and on a fall that would fill it within a day |
| WAL kept back | `pg_wal`'s size (`pg_ls_waldir()`), and WAL kept for replication slots (`pg_replication_slots`) | More than a few times `max_wal_size` |
| The archive | `pg_stat_archiver`: `failed_count` rising, `last_failed_time`, and `last_archived_time` older than `archive_timeout` plus a few minutes | Any failure that lasts; no archive for 15 minutes while the pipeline runs |
| Base backups | The newest base backup's time, in the backup store | Older than your schedule allows |
| Growth | Each pipeline database's size (`pg_database_size`) over weeks: registries and the journal grow with every item until retention trims them (§8) | Its forecast, to size the storage ahead |

Then:
- **Leave room:** size the storage for the database, plus the WAL a day's archive outage would keep, plus a
  base backup if it is made on the same storage. Turn on storage autoscaling on a managed database (RDS storage
  autoscaling, Cloud SQL automatic storage increase, Azure storage autogrow), with a ceiling.
- **Hold writes before it fills,** if you can: making the database read-only (`default_transaction_read_only`)
  below a threshold stops the WAL growing, and the stages wait as in an outage; it is easier to recover from than
  a full volume. The AWS PoC does this with a sidecar (AWS-POC.md §8.1); on a managed database, autoscaling and the
  alerts above take its place.
- **When it alerts:** find what is taking the space (an archive that can't be written, a replication slot no one
  reads, a base backup left behind, or growth), fix that, and only then add storage. Don't delete WAL by hand:
  segments not yet archived are restore points lost.
- **Review** the capacity with the weekly checks (§3), and record each review.

## 8. Retention, disposal and end of contract (§ 164.310(d), § 164.504(e)(2)(ii)(J))

- **Retention** is off by default; each table keeps rows forever until a period is set (DESIGN §8.4, §5.3.13). The
  security official sets it from state law and contracts. Retention deletes whole days and their files, audited.
  - Before real PHI, set the bounds a law or contract allows on every stage:
    `sdh.maintenance.retention-bounds.minimum` and `.maximum` (a maximum forbids keeping rows forever). A setting
    outside them refuses startup.
  - A data admin (`data.admin`) reads it with `GET /retention` and sets it with `PUT /retention` (`days`, or
    `forever`), per stage, within the bounds. It takes effect at the stage's next maintenance run, which removes
    older rows for good: check `days` against the contract before sending. Audited with `sdh.retention`.
- **Purging one item** (an individual's request the covered entity has granted, or data sent in error):
  1. Find the item's ID (§4.1) and open a ticket.
  2. At each stage the item passed through, landing included, a data admin asks
     `POST /items/{itemId}/purge` with the ticket and a reason; a second data admin approves it (§5's flow).
  3. The stage's maintenance rewrites each file holding the item's rows without them and deletes the old files,
     then deletes its corrections. `GET /purges` shows each purge's state; the operation's record ends `done`
     only once no table holds a row of the item, audited as `data.purged`.
  4. Kept: the item's failures (no document values), the landing ledger (hashes and sizes), the registry's IDs and
     the audit journal. Copies already delivered (a FHIR server, a reader's store) are the receiver's to purge.
- **Audit journal:** §3.3.
- **End of contract, or a pipeline retired:**
  1. Export what the covered entity must get back, through its readers.
  2. `audit-export` everything, and give the archive to the covered entity's security official.
  3. Pause every stage (§7.1), and drain those that drain. Then, at each stage, a data admin asks `POST /erase`
     with the ticket and a reason, and a second approves it. It is refused until the stage is paused and settled.
     The stage's tables are dropped with their files, its database rows with wrapped keys are deleted, and its
     pods refuse to start from then on. Keep the result: an erasure certificate, a JWS signed with the stage's
     key (check it with its public key), listing the tables, objects and rows removed, the KEKs and signing key,
     and the journal's last heads.
     It leaves the stage's control log (`_control/` beside its tables: operation IDs, item IDs, tickets,
     principals and times, no PHI), so that a restore of the database to before the erasure erases again (§7);
     step 5 deletes it with the buckets.
  4. The key custodians schedule deletion of the KEKs and signing keys each certificate names (two people).
     Everything the pipeline stored, backups included, becomes unreadable: a cryptographic erase.
     The platform administrator destroys the stages' inbox key pairs and their passphrases, and the data
     consumer owner the gold readers' key pair, which open what is left on the broker's topics
     ([security/KEYS.md](security/KEYS.md) §7).
  5. The platform administrator deletes the buckets, the Postgres schemas and the topics, and records the
     provider's media destruction terms (NIST SP 800-88).
- **Operator key pairs** are revoked when a person leaves: the security official removes the fingerprint from the
  register, and sealed files on that person's workstation are destroyed.

## 9. Support and diagnostics

Support never needs PHI. What a support case needs comes from four queries on each stage's control API, all
`pipeline.view`, also on the CLI (`health`, `settings`, `compliance`, `support-bundle`):

- `GET /health`: whether the control pod reaches the database, the store, the KMS, each broker and the
  collector, each in 10 seconds, with the failure's class when not.
- `GET /settings`: the settings in force, each password, token, key, JAAS configuration and URI user information
  replaced by `<redacted>`.
- `GET /compliance`: the profile's outcome, with each waiver and its reason.
- `GET /support-bundle`: all of these as one JSON document, with the build, the control state, the stage's
  status and its last 50 operations (who and when, no parameters or reasons). It holds IDs, counts, codes and
  settings, and no document content.

Before a bundle leaves the organization:
1. Ask for it per stage, as the person who opens the case; the request is audited with the caller.
2. Read it, or run it through the organization's DLP check: secrets are removed by name and by form, so a secret
   kept under an unusual setting name could remain. Remove anything that should not go.
3. Send it over the channel the support agreement names, and record the case in the ticket.

A support engineer of the vendor is never given a role on the API, nor a sealing key.

## 10. What stays the customer's, with no product feature

BAAs or MOUs with every provider (cloud, KMS, broker, SIEM); the risk analysis and risk management; a named security
official; workforce policies, training and sanctions; MFA and access reviews; the reviews in §3; the incident
response plan and breach notices; contingency plans and their tests; the retention periods; responses to
individuals' requests; the penetration test and its fixes (§11); deciding whether the clearinghouse and Part 2 rules apply (HIPAA.md §1, §4.5); and keeping
this documentation for 6 years from when it was last in effect (§ 164.316(b)(2)(i)).

## 11. Penetration testing: the customer's, and what to secure first

**The penetration test is the customer's to commission,** by a third party of their choosing, against their own
deployment: the proposed Security Rule asks for one every 12 months (HIPAA.md §7), and buyers expect one before
real PHI. The product's vendor does not test a customer's deployment, and has not had the product itself tested by
a third party (HIPAA.md §11.1, P13). Test a deployment built as it will run, from `deploy/reference/` with the
profile on and nothing waived, holding **synthetic data only**; follow the cloud provider's policy on penetration
testing, and tell the SIEM's watchers, or don't, as the rules of engagement say.

**Secure these before the test,** since they are where a tester will look and where the product relies on the
platform. Each names what the product provides and what is the customer's; HIPAA.md §11.2 lists the reference
documents still owed for several of them (D1 to D4, D6, D13).

| Area | Secure before the test | The product provides |
|---|---|---|
| **Network** | Private subnets, no public endpoints. The one ingress is HTTP push behind TLS or mTLS (or the file and table intakes' own sources). The control API reachable from the operator's path only (VPN, bastion or zero-trust access), never the internet. Default-deny network policies in every namespace; egress only to DNS, the KMS, the object store, the broker, the database and the collector, over private endpoints. A current network map. [security/NETWORK.md](security/NETWORK.md) (D1) sets these out. | Example network policies per stage (`deploy/`); the control API's policy in each stage README; no listener on a worker but its health port, and the landing node's push port. |
| **Push ingress** | The TLS certificate and its renewal; with mTLS, the producers' certificates: who issues them, the subjects allowed (`sdh.landing.push.*`), and their revocation. Rate limits and request size at the load balancer. | mTLS with an allowed-subject list; the landing node answers `503` while paused (§7.1). |
| **Control API** | TLS; the identity provider: issuer, audience, signing keys and token lifetime; the mapping of the customer's roles to the API's permissions (§1, §2.4); mTLS client certificates if used; operators' sealing keys registered by someone else (§2.3); approvers distinct from requesters (§5). In scope for the tester: authentication and permission bypass, reaching another pipeline's or another item's data, sealed results, replayed requests. | A permission per operation, checked on every call; every call audited with its principal, the denied ones too; PHI results sealed to the caller's registered key; four eyes for break-glass and audit prune; an OpenAPI contract to test against (`/openapi.yaml`). |
| **Identity and access** | A workload identity per stage, no static keys; each stage's role allowed only its own buckets, keys and topics; human access through SSO with MFA; `kubectl exec` (which reaches the stages' CLI) granted per namespace to named staff only (§2.4); break-glass accounts; leavers removed. | Stages that need no static credentials on AWS, GCP or Azure; the CLI audits its operator (`--operator`). |
| **Kubernetes** | The restricted Pod Security Standard; Secrets encrypted at rest, or an external secret store; the API server's audit log to the SIEM; admission only of signed images by digest; a private API endpoint; nodes to a CIS benchmark, patched. [security/HARDENING.md](security/HARDENING.md) (D4) sets these out, with the database's, broker's and object store's. | Images that run as non-root with a read-only root filesystem and no shell; manifests with those settings; signed images and SBOMs (DESIGN §14.4). |
| **Database** | TLS with `verify-full`; a database per pipeline with only its stage roles connecting (§2.8); encryption at rest; no public address; backups encrypted. | `deploy/reference/postgres.sql` (roles and grants); the profile refuses a connection without TLS. |
| **Broker** | `SASL_SSL`; an identity per stage and ACLs giving each only its topics (`deploy/reference/kafka-acls.sh`); encryption at rest; no public listener. | The profile refuses plaintext; messages signed per stage and checked on receipt, so a forged message is rejected. |
| **Object store** | Bucket policies refusing plaintext and public access; each stage's role on its own buckets only; versioning, or object lock, on bronze at least; KMS encryption; data events to the SIEM (§2.6). | Every object envelope-encrypted under the stage's KEK before it is written. |
| **Keys** | KMS keys, never a local keystore; key policies naming each stage's identity; rotation on; key administrators other than the pipeline's operators. | Rotation and re-keying through the API (§6.1); erase certificates naming the KEKs to destroy (§8). |
| **Audit and monitoring** | The collector to a store the pipeline's staff can't change (§2.7); the SIEM's alerts (§3.1) in force, so the test also shows what was detected. | Every stage's audit events over OTLP; the hash-chained audit journal and `audit-verify` (§3.2). |
| **Settings and supply chain** | The compliance profile at `hipaa`, nothing waived (§2.1); no secrets in URIs; images at the current release, scanned, their fixes taken (HIPAA.md §11.2, D11, D12). | The profile's checks at startup; vulnerability scanning in the build (DESIGN §14.5). |

After the test, keep its report and the fixes made with this documentation (§10), and repeat it after a major
change to the deployment, and at least every 12 months.
