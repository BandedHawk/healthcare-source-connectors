# HIPAA: the rules that apply, and how the system meets them

Status: **Draft for review** · Date: 2026-11-20 · Scope: the connectors (landing, split, convert), the transform
stage, and their deployments (`deploy/sandbox`, `deploy/aws`)

Revision 2026-11-17 (b): checked against the official texts (§9). Corrections:
- **The clearinghouse question (§1):** converting X12 standard transactions into JSON may make the operator a
  health care clearinghouse. That would bring Part 162 and the required § 164.308(a)(4)(ii)(A).
- **Accounting of disclosures (§4.3):** disclosures for treatment, payment and operations are exempt
  (§ 164.528(a)(1)(i)).
- **Breach guidance (§5):** FIPS 140-2 appears only in the criterion for data in motion. The at-rest criterion
  asks for consistency with NIST SP 800-111, so a FIPS-validated module is no longer listed as a gap for the
  safe harbour.
- **The Privacy Rule** runs to § 164.535 (Severability).
- **Preemption:** § 160.203(b) saves only more stringent state *privacy* law, measured against Subpart E.
- **Contract terms:** the § 164.504(e)(2)(ii) letters were corrected.
- **Documentation retention:** the period runs from the later of creation and last in effect.
- **Audit logs:** HIPAA sets no retention period for the logs themselves.
- **Security incidents:** the definition includes *attempted* unauthorized access (§3.4).
- **Recognized security practices** can't raise penalties.
- **Unverified items are now marked as such,** where the official source could not be reached: the 2025 NPRM,
  the Part 2 rule and *Purl*.

Revision 2026-11-17 (c): reframed for the product as sold.
- **Who operates it:** customers operate the product: federal and state government agencies and health insurers,
  or their agents. We sell it.
- **What that changes (§1):**
  - The question for us is whether the product meets HIPAA's technical requirements itself, or gives its
    operators the capabilities to meet them.
  - "The operator" throughout means the customer.
  - The open items are now split into product gaps, documentation we owe customers, and the operator's own duties
    (§8).
  - A section on requirements next to HIPAA for government buyers is added (§8.4).
- **What the AWS PoC is now:** our own test deployment. Its gaps are listed apart (§8.5).

Revision 2026-11-20 (d): checked against the code and both deployments, requirement by requirement; the priority-1
features built (DESIGN.md revision (eg)); and a customer mapping added (§10), with the procedures in
[HIPAA-OPERATIONS.md](HIPAA-OPERATIONS.md).
- **Corrections:**
  - On AWS the collector's logs pipeline exports to `nop`: audit events reached the collector and were dropped
    there, not merely kept on the console (§3.1, §8.5). The sandbox sends them to its in-cluster Loki, which is
    neither separate nor durable.
  - Iceberg snapshots are not a backup: they expire after 24 hours, in the same bucket under the same keys (§3.2).
  - `admin restore` wrote plaintext; it now seals by default (§3.1).
  - Every shipped manifest runs `compliance.profile: none`, which is right for synthetic data but no starting point
    for a customer (§8.1 item 13).
  - The mapping does carry C-CDA `confidentialityCode` into `Composition.confidentiality`, but sets no
    `meta.security`, so readers can't filter on it (§8.1 item 10).
- **Built** (§8.1 items 1, 2 and 4, and restore):
  - a durable, tamper-evident audit journal in each stage's Postgres schema, hash-chained per process and sealed
    with the stage's signing key, with `audit-verify`, `audit-export` and `audit-prune` on every stage, and a
    compliance check that refuses to start without it;
  - an audit of every record `connector-queue-reader` opens, with the reader's name and purpose of use;
  - `patients`, a patient lookup on the record stages, sealed to the operator's key and audited;
  - `restore` sealed to the operator's key by default; plaintext only with `--plaintext --ticket`, audited.

Revision 2026-11-20 (e): §8.1 items 7, 9 and 13 built (DESIGN.md revision (eh)). Kafka over `SASL_SSL` with ACLs
per stage, and Postgres with `verify-full`, are tested through the stages' own clients. Two pipelines on shared
services are tested to be kept apart. Reference settings with the profile on and nothing waived are in
`deploy/reference/`, with the isolation layout in [HIPAA-OPERATIONS.md](HIPAA-OPERATIONS.md) §2.8. Found: pipelines
need a database each, since a JDBC catalog can't keep them apart within one (§8.1 item 9).

Revision 2026-11-20 (f): the outstanding work restated (§11), with the decision to build a supported HTTPS control
API, with a permission per operation, and a test harness, before the console ([CONTROL-API.md](CONTROL-API.md)).
The documents customers need in order to secure a deployment, network architecture first, are listed in §11.2.

**This is an engineering mapping, not legal advice.** It sets out which parts of HIPAA bear on the pipeline, which
controls the code and deployments provide toward each one, and what the code leaves to the organisation that runs
it. Privacy and compliance counsel must review it before any real PHI is processed. The current AWS proof of
concept holds **synthetic data only** ([AWS-POC.md](AWS-POC.md), "Decided with the user", and §9).

Section references such as "DESIGN §7.2" are to [DESIGN.md](DESIGN.md), [STAGES.md](STAGES.md) and
[AWS-POC.md](AWS-POC.md). A citation of the form "§ 164.312(a)(2)(iv)" is to Title 45 of the Code of Federal
Regulations. "(R)" marks a required implementation specification and "(A)" an addressable one; the labels are as
printed in the rule (§ 164.306(d)(1)). Standards themselves carry no label: each one must be met.

## 1. Who HIPAA applies to here, and what we owe as the seller

**The product is sold, and the customer operates it.** Buyers are federal and state government agencies, health
insurers, and the agents of either.

**HIPAA regulates the buyers, not the software.** Each buyer is one of these:
- **A covered entity** (§ 160.103), when it is a health plan or a provider that transacts electronically:
  - an insurer;
  - a state Medicaid agency;
  - federal programs such as Medicare.
- **A hybrid entity** (§§ 164.103, 164.105), when it is a government agency with health care components as well as
  others. It must keep its health care components' PHI apart from its other components (§ 164.105(a)(2)(ii)). The
  product's per-pipeline isolation supports that (§8.1, item 9).
- **A business associate** (§ 160.103), when it is an agent such as a TPA, a PBM, a fiscal agent or a vendor
  running the product for one of the above. The Security Rule applies to it directly (§ 164.302), as do breach
  notice to its covered entity (§ 164.410) and the limits on its uses (§ 164.502(a)(3)).
- **Between government entities,** a memorandum of understanding or a law can take the place of a BAA
  (§ 164.504(e)(3), § 164.314(a)(2)(ii)).

**What we owe, as the seller:**
- **We are not a covered entity or business associate just by selling software.** We become a business associate,
  and a subcontractor where the customer is itself one, only if we create, receive, maintain or transmit their PHI.
  - That happens if we host the product for them (the "service" images, DESIGN §14.1).
  - It also happens if our support staff are given PHI.
- **Our obligations are contractual and practical.** The product must either meet a technical requirement itself,
  or give the operator the capability and the configuration to meet it. We must also tell the operator which of
  the two applies, and how.
- **The product is built so that support never needs PHI.** Logs, telemetry and issue text carry codes, IDs and
  positions, never values (§4.1). `admin status` reads without keys (DESIGN §11.4). This keeps us out of the
  business-associate role. A support bundle that gathers only this data is still to be built (§8.1, item 11).

**The operator's own providers:**
- **The operator's providers are its subcontractors or business associates:** its cloud, object store, KMS and
  broker providers, which maintain its ePHI even when they hold only ciphertext (§ 160.103, "business associate"
  (3)(iii)). The BAAs are the operator's to make (§ 164.308(b)).
- **HHS's cloud computing guidance** says the same, but it couldn't be re-read for this revision (§9).

**Health care clearinghouse: for counsel, and for the customer documentation.**
- **The definition:** a health care clearinghouse (§ 160.103) includes an entity that "receives a standard
  transaction from another entity and processes or facilitates the processing of health information into
  nonstandard format or nonstandard data content for the receiving entity" (paragraph (2)).
- **Why it may apply:** the convert stage turns X12 records into its own JSON (`x12/1`, DESIGN §6.4).
- **Who it affects:** a health plan converting the transactions it receives is a covered entity already. An agent
  that converts other entities' standard transactions for them may itself be a clearinghouse, a covered entity in
  its own right. That brings Part 162 (§ 162.923(c)), the required "isolating health care clearinghouse
  functions" (§ 164.308(a)(4)(ii)(A)), and part of the Privacy Rule (§ 164.500(b)).
- **What the product supports:** a pipeline per function, with its own keys and readers (§8.1, item 9).
- C-CDA, FHIR and CSV are not Part 162 standard transactions.

## 2. The rules, and where each stands (as of 2026-11-17)

| Rule | Citation | Status | Bearing on the system |
|---|---|---|---|
| General administrative requirements, definitions, preemption, enforcement | 45 CFR Part 160 | In force | Definitions (§ 160.103). Penalty tiers (§ 160.404(b)(2)(i)–(iv)), with the amounts adjusted each year at 45 CFR Part 102 (§ 160.404(a)). Penalty factors (§ 160.408). Cornell's copy shows an amendment to § 160.103 at 91 FR 14404 (2026-03-24) that this review has not examined. |
| Transactions, code sets and identifiers | 45 CFR Part 162 | In force | Only if an X12 intake makes the operator a clearinghouse, or the operator conducts transactions for a covered entity (§1; § 162.923(c)) |
| Security Rule | Part 164 Subparts A and C (§§ 164.302–164.318) | In force | The main body of controls (§3) |
| Breach Notification Rule | Part 164 Subpart D (§§ 164.400–164.414) | In force | Encryption to HHS's guidance makes PHI "secured" (§5) |
| Privacy Rule | Part 164 Subpart E (§§ 164.500–164.535) | In force | Minimum necessary, amendment, accounting, and the contract terms (§4) |
| HIPAA (Pub. L. 104-191) and HITECH (Pub. L. 111-5) | Statutes | In force | HITECH § 13402(h)(2) is the basis of the breach guidance (§ 164.402, "unsecured protected health information") |
| Recognized security practices | Pub. L. 116-321 (H.R. 7898, 2021-01-05), adding HITECH § 13412, 42 U.S.C. 17941 | In force | **Considering them is mandatory.** HHS "shall consider" recognized security practices that were in place for at least the previous 12 months when it decides fines, audit results and remedies. These are the NIST Cybersecurity Framework and the other approaches developed under NIST Act § 2(c)(15), the Cybersecurity Act of 2015 § 405(d) practices (HICP), and other recognized programs. Not having them can't raise a fine (§ 13412(b)(2)). § 160.408 doesn't yet list them. See §6. |
| **Proposed Security Rule overhaul** | NPRM of 2025-01-06, cited as 90 FR 898 | **Proposed, not final.** Secondary sources from up to October 2026 report that OCR missed its May 2026 target and that the Unified Agenda lists July 2027. Not verified against the Federal Register (§9). | Not binding. §7 measures the system against it |
| Reproductive health care privacy amendments | Final rule, 89 FR 32976 (2024-04-26) | **Vacated nationwide,** according to secondary sources: *Purl v. HHS* (N.D. Tex., 2025-06-18), appeal dismissed 2025-09-10, leaving only the notice-of-privacy-practices changes made for Part 2. Not verified against a primary source. The eCFR still carries 2024 text in § 164.520 (89 FR 33064), and lists § 164.509 (attestations). | None for the pipeline. It doesn't classify records by subject, and gives no attestation |
| Confidentiality of SUD patient records | 42 CFR Part 2, as revised in 2024; compliance date reported as 2026-02-16 (unverified, §9) | In force | Only if an intake carries records from a Part 2 program (§4.5) |
| Information blocking | 45 CFR Part 171 (21st Century Cures Act) | In force | Applies to "actors", such as providers and developers of certified health IT, not to this pipeline as such (§4.6) |

**State law:**
- § 160.203(b) leaves in place state law that "relates to the privacy of individually identifiable health
  information" and is "more stringent" than **Subpart E**, the Privacy Rule.
- State breach and security laws stand on their own terms.
- They are outside this document, but they often set record retention periods, and breach deadlines shorter than
  HIPAA's.

## 3. Security Rule (Subpart C)

**§ 164.306 (general requirements):**
- **(a) the duty:** ensure the confidentiality, integrity and availability of all ePHI the pipeline creates,
  receives, maintains or transmits. Protect it against reasonably anticipated threats, and against reasonably
  anticipated uses or disclosures that aren't permitted.
- **(b) flexibility:** any measures that reasonably and appropriately implement the standards may be used, chosen
  by size, infrastructure, cost, and the probability and criticality of the risks.
- **(d)(3) addressable specifications:** for each one, the operator must assess whether it is reasonable and
  appropriate. If it is, implement it. If not, document why, and implement an equivalent alternative if that is
  reasonable and appropriate.
- **(e) maintenance:** the measures are reviewed and modified as needed, and their documentation updated
  (§ 164.316(b)(2)(iii)).

The tables below record what the code already does toward each specification.

### 3.1 Technical safeguards (§ 164.312)

| Standard / specification | How the system addresses it | Left to the operator, or open |
|---|---|---|
| **Access control (a)(1).** Unique user identification (a)(2)(i) (R) | Each stage runs as its own identity: an IAM role per stage through EKS Pod Identity, with no shared or static keys (AWS-POC §7). Postgres has a role per stage (`landing_<prefix>`, …). Each stage has its own signing key, so every message names the stage that sent it (DESIGN §10.1). Operator commands require an operator name, which is audited (DESIGN §11.4). | The operator name is self-asserted. NIST SP 800-66r2 (Table 21, activity 3) asks that "system activity can be traced to a specific user", so for now unique human identity must come from the platform (§8.1, item 3): Kubernetes RBAC and cloud IAM tied to a named person, with `kubectl exec` audited. |
| Emergency access procedure (a)(2)(ii) (R) | `admin restore` decrypts a landed item from bronze and verifies its digest (DESIGN §11.4). Its output is sealed to the operator's public key by default; plaintext needs `--plaintext --ticket <ref>`, and the ticket is audited. `status` and the audit commands work even when the KMS can't be reached. | The break-glass procedure: who approves, who holds operator keys ([HIPAA-OPERATIONS.md](HIPAA-OPERATIONS.md) §5). |
| Automatic logoff (a)(2)(iii) (A) | The pipeline has no interactive sessions or UI. Assessed as not applicable to it (§ 164.306(d)(3)). | Session timeouts for the console, `kubectl` and SSO. |
| Encryption and decryption (a)(2)(iv) (A) | **Implemented**, though only addressable. Envelope encryption of every stored record (DESIGN §7.2): AES-256-GCM per 8 MiB chunk, a batch data key wrapped by a KMS key-encryption key, and an HKDF-derived key per record. A separate KEK per table means a reader can be given `records` only (DESIGN §7.1). Storage-side SSE-KMS sits beneath as a second layer (DESIGN §8.1; AWS-POC §4). Decryption needs both the KMS grant and the data. In the AWS PoC, even the deployer is denied decrypt on bronze's key. | KMS key rotation is off in the PoC, to save cost (AWS-POC §4). Re-keying existing data after a KEK compromise is out of scope (DESIGN §17). |
| **Audit controls (b)** (standard; no specifications) | Audit events `sdh.audit.*` for each item's lifecycle, the lease, admin commands, failure state changes and PHI purges (DESIGN §12; STAGES §15.3). Exports are allow-listed, so no PHI can be in them (DESIGN §12). One trace per item, with 100% sampling. **Every event is also written to a durable journal** in the stage's Postgres schema: a hash chain per process, its head sealed with the stage's signing key every minute and at close; never dropped; checked by `audit-verify`, exported by `audit-export`, pruned by `audit-prune` (DESIGN §12.3). Every record `connector-queue-reader` opens is audited with the reader's name and purpose (`reader.record_read`, DESIGN §10.2). | NIST SP 800-66r2 (Table 22) asks "where will audit information reside (e.g., separate server)?". The journal is durable but sits beside the data: the separate copy is the operator's SIEM, fed by the OTLP export and by `audit-export`. **On AWS the collector still drops logs** (`nop`, §8.5). CloudTrail data events on S3 and KMS (AWS-POC §9). A whole chain deleted shows only against the SIEM copy. HIPAA sets no retention period for logs. The 6 years of § 164.316(b)(2)(i) apply to the documentation of required actions and reviews (§3.4). Counsel sets the log retention. |
| **Integrity (c)(1).** Mechanism to authenticate ePHI (c)(2) (A) | **Implemented.** GCM's tag and AAD (item, seq, chunk index, chunk count) detect any change, reordering or truncation (DESIGN §7.2). The record ID is keyed and computed over the content (DESIGN §7.3). Item digests are checked at restore, and no unverified plaintext is left behind (DESIGN §11.4). Checkpoints and the given-up record are MAC'd (DESIGN §5.3.13, §9.1). Commits are fenced, so a stale instance can't write (DESIGN §9.3). Every message is digitally signed (ES256), one of the mechanisms NIST SP 800-66r2 names (Table 23, activity 5). Nothing is dropped or silently replaced: corrections, retries and reprocesses are new, linked objects, and what they supersede is kept (STAGES §15.1, §15.8). A record that arrives again with different output raises `record.conflict` rather than overwriting the first (STAGES §6). | — |
| **Person or entity authentication (d)** (standard; no specifications) | HTTP push uses OAuth2 (a bearer JWT checked for issuer, audience and scope) or mTLS (CA and subject allow-list), with one sender per intake when results are on (DESIGN §5.3.15, §5.3.8). Each message is signed and checked against pinned keys (DESIGN §10.1, §10.2). Cloud services are reached through workload identity. Recipient keys are pinned by fingerprint (DESIGN §7.4). | **Kafka has no authentication or ACLs in the PoC** (AWS-POC §9); SASL_SSL with ACLs per stage is tested (§8.1 item 7) and in the reference settings (§8.1 item 13). Messages are still signed and sealed, so nobody on the broker can read or forge them, but they can be deleted or replayed. Postgres uses passwords from Kubernetes Secrets. MFA for human access is the operator's: NIST SP 800-66r2 (Table 24) advises it where risk is high, for example remote access and privileged functions. |
| **Transmission security (e)(1).** Integrity controls (e)(2)(i) (A); encryption (e)(2)(ii) (A) | Every message between stages is a JWS nested in a JWE (`ECDH-ES+A256KW` or `RSA-OAEP-256`, `A256GCM`), addressed to the next stage's key. Only `messageId`, `type`, `itemId` and the content type are visible on the broker (DESIGN §10.1). TLS is used to the object store, KMS and catalog. The S3 buckets refuse non-TLS requests (AWS-POC §4). HTTP push is TLS only. | Kafka TLS (AWS-POC §9). Postgres TLS: the PoC manifests configure none. Both are tested, and set in the reference settings (§8.1 items 7, 13). Postgres holds the catalog, leases and registries, not record content. Private subnets, in place of public subnets with inbound closed (AWS-POC §9). |

### 3.2 Administrative safeguards (§ 164.308)

| Standard / specification | How the system addresses it | Left to the operator, or open |
|---|---|---|
| Risk analysis (a)(1)(ii)(A) (R); risk management (B) (R) | This document, DESIGN §16 (risks) and §17 (out of scope), and §8 below are inputs to the analysis. | The risk analysis itself, covering the whole environment and kept current. NIST SP 800-66r2 §3–§4 gives a method. |
| Sanction policy (C) (R) | — | Workforce policy. |
| Information system activity review (D) (R) | Audit events, metrics (`sdh.items.failed`, ages, backlogs) and `admin status` for each stage (DESIGN §11.4, §12). `audit-verify` checks the journal for changes and gaps. | Regular review, with alerting, and records of the reviews (§ 164.316(b)(1)(ii)); [HIPAA-OPERATIONS.md](HIPAA-OPERATIONS.md) §3 suggests what to review. |
| Assigned security responsibility (a)(2) (standard) | — | A named security official. |
| Workforce security (a)(3); information access management (a)(4): access authorization (a)(4)(ii)(B) (A), establishment and modification (C) (A) | Least privilege by design. Each stage's role reaches only its own buckets and KEKs (AWS-POC §7). A separate KEK per table means a reader can be granted `records` alone (DESIGN §7.1). Gold is read by a read-only role (`gold-reader`). The deployer can't decrypt bronze. A permissions boundary covers every role (`deploy/aws/iam`). | Granting, reviewing and revoking human access. IAM and quotas are the account administrator's. |
| Isolating health care clearinghouse functions (a)(4)(ii)(A) (R) | — | Only if the operator is a clearinghouse (§1). It would then need a pipeline, keys and readers of its own, kept apart from any other business. |
| Protection from malicious software (a)(5)(ii)(B) (A) | Distroless customer images pinned by digest, a vulnerability scan that passes, signed SBOMs, and base-image updates (DESIGN §14.4, §14.5). Containers run as non-root on a read-only root filesystem, under the restricted Pod Security Standard (DESIGN §14.1, §16 item 9). | Scanning and patching on a schedule. |
| Security reminders (A) (A); log-in monitoring (C) (A); password management (D) (A) | Private keys are encrypted PKCS#8, with passphrases from secrets. The service refuses key files that others can read (DESIGN §7.4). | Training, monitoring of human logins, and password and MFA policy. |
| Security incident procedures (a)(6); response and reporting (a)(6)(ii) (R) | Fatal conditions exit with status 70 and are audited (`connector.fatal`, `lease.lost`). Messages that can't be opened are counted and rejected (`stage.message_rejected`). Audit events let an incident be scoped by item ID without reading PHI. | The incident response plan, and its link to breach assessment (§5). |
| **Contingency plan (a)(7).** Data backup plan (A) (R) | Iceberg snapshots of every table, which are **not a backup**: they expire after 24 hours, in the same bucket under the same keys. On AWS, EBS snapshots of Postgres and Kafka at each `down`, keeping the last 3 (AWS-POC §3.1). Keys, buckets and volumes have `prevent_destroy`. | **Backups of bronze, silver and gold.** The rule asks for "retrievable exact copies" of ePHI, and the PoC has S3 versioning off. Bronze is the only copy once an object has landed, because landing deletes it from the intake. Real PHI needs versioning, replication or AWS Backup, on bronze at least. |
| Disaster recovery plan (B) (R); emergency mode operation plan (C) (R) | Recovery after a crash or outage is built in: resume from checkpoint, fenced leases, standby takeover in about 2 s, and lost item ends resent (DESIGN §9; STAGES §13). The automatic shutdown guard stops the stack without touching the data (AWS-POC §3.1). | Written plans, and a recovery time objective. |
| Testing and revision procedures (D) (A) | Outage and failover tests have been run on AWS, with every item arriving exactly once afterwards (AWS-POC, "Outage test"; PERFORMANCE.md §3.6, §10). Integration suites cover brokers, stores and KMS providers (DESIGN §15). | Loss of a region, and restoring from backup, are untested. |
| Applications and data criticality analysis (E) (A) | — | The operator's analysis. |
| Evaluation (a)(8) (standard) | — | Periodic technical and non-technical evaluation. |
| Business associate contracts (b)(1)–(2); written contract (b)(3) (R) | — | A BAA with the cloud provider, and with any other provider that holds ciphertext, keys or logs (AWS-POC §9). |

### 3.3 Physical safeguards (§ 164.310)

Facility access, workstation use and workstation security ((a)–(c)) belong to the data centre: the cloud
provider's, under its BAA, or the customer's. Device and media controls (§ 164.310(d)):

| Specification | How the system addresses it | Left to the operator, or open |
|---|---|---|
| Disposal (d)(2)(i) (R); media re-use (d)(2)(ii) (R) | **No plaintext PHI is ever written to local disk:** no temporary files, heap dumps, core dumps or `hs_err` files, no JVM attach, and a read-only root filesystem (DESIGN §7.5, §14.1). **In storage:** retention deletes whole days and expires their files in the same run. Items given up are purged after a grace period, each one audited (`item.purged`). Orphan files are removed (DESIGN §8.4, §5.3.13). Everything stored is ciphertext, so destroying a KEK makes its data unreadable: a cryptographic erase. | Retention is **off by default**, and its period is the operator's. HIPAA sets none for records; state law and contracts do. Destruction that meets HHS's guidance is "consistent with NIST Special Publication 800-88" (74 FR 42743): a written procedure for KEK destruction and cloud media at the end of a contract (§4.4). |
| Accountability (d)(2)(iii) (A); data backup and storage (d)(2)(iv) (A) | Where data is stored is fixed by configuration, and recorded for each record (`kek_ref` and the storage refs, DESIGN §10.1). | An inventory of storage and backups. |

### 3.4 Organizational, policy and documentation requirements (§§ 164.304, 164.314, 164.316)

- **Security incident** (§ 164.304): "the attempted or successful unauthorized access, use, disclosure,
  modification, or destruction of information", or interference with system operations.
  - **Attempts count too.** The contract must require the business associate to report any security incident it
    becomes aware of, including breaches (§ 164.314(a)(2)(i)(C)).
  - **What the pipeline records:** messages it rejects (`stage.message_rejected`, for a bad signature or
    contract) and fencing (`lease.fenced_table`, `lease.lost`).
  - **What it doesn't record:** refused pushes (TLS or token failures), and KMS or S3 access denials. These have
    to come from the platform: ingress logs and CloudTrail.
- **§ 164.314(a)(2)(i):** the BAA requires the business associate to:
  - (A) comply with Subpart C;
  - (B) bind its subcontractors;
  - (C) report security incidents.
  - (a)(2)(iii) applies the same terms between a business associate and its subcontractors.
- **§ 164.316(b):** written policies and procedures, and written records of every action, activity or assessment
  the subpart requires to be documented ((b)(1)). They are kept for 6 years from the date of creation or the date
  last in effect, **whichever is later** ((b)(2)(i)). They are made available to those who carry them out
  ((b)(2)(ii)), and reviewed periodically ((b)(2)(iii)).
  - The design documents are in version control, and each records its revisions with dates. That makes them the
    documentation of the technical controls and of the decisions behind them, including the addressable choices
    in §3.1–§3.3.
  - The organisational policies are the operator's.

## 4. Privacy Rule (Subpart E)

### 4.1 Minimum necessary (§ 164.502(b), § 164.514(d))

- **The duty:** a covered entity or business associate must make "reasonable efforts to limit" PHI to the minimum
  necessary for the purpose (§ 164.502(b)(1)).
- **Each stage sees only what it must:**
  - A stage's role decrypts only its own input and output tables (AWS-POC §7).
  - The keys are split by table (DESIGN §7.1).
  - The broker sees only identifiers (DESIGN §10.1).
- **Operational data carries no PHI:**
  - **Logs:** no PHI, filenames or parser messages are logged; errors are reduced to stable codes (DESIGN §7.5).
  - **Telemetry:** attributes are allow-listed at the source and again at export (DESIGN §12).
  - **Issues:** the text of mapping issues and conversion errors never quotes document values. A failure keeps
    the failure and the document only (STAGES §17.10.4, and mapping's commit e1394eb).
  - **Free text an operator writes:** a reason for declining a failure is encrypted, since it may describe the
    data (STAGES §15.3).
- **Ops commands avoid exposure:**
  - `admin restore` prints no original filename, and seals what it writes to the operator's key (DESIGN §11.4).
  - The patient lookup shows counts only; what it finds is sealed (§4.2).
  - Smoke tests check that no log holds document content or an object key (AWS-POC, the 2026-11-12 run).
- **Whole documents:** each record in the pipeline is a whole document, by design. § 164.514(d)(5) allows using or
  disclosing an entire medical record only when it is "specifically justified". The justification is the
  pipeline's purpose, conversion. It belongs in the BAA's description of what the pipeline does, and readers of
  gold should take only what they need.

### 4.2 Right of access and amendment (§§ 164.524, 164.526)

- **Access:** these are covered entities' duties. A business associate must make PHI available for them
  (§ 164.504(e)(2)(ii)(E)–(F)).
  - The pipeline keeps the source document and every stage's output, linked by item and record IDs.
  - Gold is readable with a read-only grant (AWS-POC §7, `gold-reader`).
  - **Finding one individual's records:** `patients` on a record stage takes patient identifiers on standard input
    and lists the records whose FHIR `Patient` resources carry them, sealed to the operator's public key; every
    record read and the run are audited, with no identifier in the audit (DESIGN §11.4).
- **Amendment:** an accepted amendment is made by identifying the records affected and appending or linking the
  amendment (§ 164.526(c)(1)). The original is not erased. A denial's disagreement and rebuttal are appended or
  linked the same way (§ 164.526(d)).
  - **The pipeline's rule matches: data is never dropped or silently replaced.**
  - A resubmission is a new item linked to the earlier one, and both are kept (DESIGN §11.4).
  - A correction or reprocess is a new revision linked to what it supersedes (STAGES §15.8).
  - An object that has passed to the end is corrected in the FHIR repository (STAGES §15.1, rule 5).

### 4.3 Accounting of disclosures (§ 164.528)

- **What must be accounted for:** individuals may ask for an accounting of disclosures made in the 6 years before
  the request (§ 164.528(a)). Disclosures for treatment, payment and health care operations are exempt, among
  others ((a)(1)(i)–(ix)).
- **What that means here:**
  - The pipeline's own processing is the business associate acting for the covered entity.
  - Reads of gold for treatment, payment or operations are exempt.
  - An accounting is needed only for gold reads made for other purposes, such as public health or research
    disclosures under § 164.512.
- **The business associate's part** is to make the information available (§ 164.504(e)(2)(ii)(G)). The audit
  events record what the pipeline did to each item, by ID. **Who read gold is recorded too:** every record
  `connector-queue-reader` opens emits `reader.record_read`, with the reader's name, its purpose of use (an HL7
  code such as `TREAT`, `HPAYMT`, `HOPERAT` or `PUBHLTH`), and the record's IDs and table. A reader names itself
  and its purpose when it is built; one that doesn't reads as `unnamed`. The events go to the reader's `sdh.audit`
  log by default, or to its journal. CloudTrail data events on the gold bucket and its KMS key show reads that
  bypass the library (AWS-POC §9).

### 4.4 Business associate contract terms (§ 164.504(e)(2)(ii))

- **(A) Use and disclose only as the contract allows,** or as required by law (also § 164.502(a)(3)). The
  pipeline only lands, splits, converts and transforms. Nothing in it analyses or shares data for any other
  purpose.
- **(B) Safeguards,** including compliance with Subpart C for ePHI: see §3.
- **(C) Report uses or disclosures the contract doesn't allow,** including breaches of unsecured PHI: see §5.
- **(D) Bind subcontractors** to the same restrictions: the cloud and other providers (§1).
- **(E)–(G) Make PHI available** for access, amendment and accounting: see §4.2 and §4.3.
- **(J) Return or destroy all PHI at termination** if feasible, and otherwise extend the protections to it:
  - Retention purges, given-up purges and orphan cleanup remove data from storage, and are audited (DESIGN §8.4,
    §5.3.13).
  - Destroying a pipeline's KEKs makes everything it stored unreadable, backups included.
  - `poc.sh reset` empties a whole pipeline: database, topics, consumer groups, buckets and intake prefixes.
  - **Open:** a written termination procedure, covering backups and logs as well.

### 4.5 42 CFR Part 2 records

- **Part 2 keeps its own consent and redisclosure rules.**
- **The pipeline doesn't know whether a record is a Part 2 record.** An intake that may carry them should be its
  own pipeline, with its own namespaces, keys and readers (DESIGN §5.3.1, §17). That way access to it can be
  granted and audited apart from other data.
- Readers downstream apply Part 2's redisclosure notice. **This section is not verified against the current Part 2
  text** (§9).

### 4.6 Information blocking (45 CFR Part 171)

The pipeline never discards data, and its failures are visible and recoverable. Every failure is kept in the fail
store and reported to the producer (STAGES §15.1). That supports an actor's duty not to interfere with access,
exchange or use of electronic health information. Latency from landing to gold is a measured target
([PERFORMANCE.md](PERFORMANCE.md)).

## 5. Breach Notification Rule (Subpart D)

**Breach** (§ 164.402):
- **The definition:** an acquisition, access, use or disclosure that Subpart E doesn't permit, and that
  compromises the PHI.
- **Three exclusions** ((1)(i)–(iii)): a good-faith use by a workforce member; an inadvertent disclosure between
  authorised persons; and a recipient who could not reasonably have kept the information.
- **The presumption:** anything else is presumed a breach, unless a risk assessment of at least four factors
  shows a low probability that the PHI was compromised ((2)(i)–(iv)):
  - the nature and extent of the PHI;
  - who obtained it;
  - whether it was actually acquired or viewed;
  - how far the risk was mitigated.

**Only *unsecured* PHI triggers notification.** PHI is secured if a technology or method in the Secretary's
guidance under HITECH § 13402(h)(2) makes it "unusable, unreadable, or indecipherable" to unauthorized persons. The
guidance is at 74 FR 42741–42743, 2009-08-24, re-issuing the version at 74 FR 19006. Its criteria:

| Criterion (74 FR 42742–42743) | Where the system stands |
|---|---|
| **At rest:** encryption "consistent with NIST Special Publication 800-111" | AES-256-GCM through KMS-wrapped data keys (DESIGN §7.2, §7.4), with SSE-KMS beneath. Theft of the buckets, a backup or a volume snapshot alone yields ciphertext. Being "consistent with" SP 800-111 is a judgement for the security official, not something the code can prove. The guidance asks for no FIPS validation for data at rest. A FIPS 140-validated JCE provider would make that judgement easier to defend, and is listed as an option (§8.1, item 5). |
| **In motion:** processes that comply "as appropriate" with NIST SP 800-52 (TLS), 800-77 (IPsec) or 800-113 (SSL VPNs), "or others which are … FIPS 140-2 validated" | TLS to storage, KMS and catalog, and for HTTP push. Between stages, every message is a JWE. That isn't one of the listed protocols, and the JDK's provider isn't FIPS-validated, so the messages' protection rests on the TLS links, which Kafka lacks in the PoC (§3.1). Counsel to decide whether a sealed message on a plaintext link meets the criterion. |
| **Keys:** the key "has not been breached", and decryption tools should be "stored on a device or at a location separate from the data" | Met. The KEKs never leave the KMS. Only wrapped data keys are stored beside the data. Signing keys sign inside the KMS (DESIGN §7.4). |
| **Destruction:** media "cleared, purged, or destroyed consistent with NIST Special Publication 800-88" | See §3.3, disposal. |

**When a breach happens:**
- **Discovery:** a breach is treated as discovered on the first day the business associate knows of it, or would
  have known with reasonable diligence, including through any employee or agent (§ 164.410(a)(2)).
- **The business associate's notice** to the covered entity: without unreasonable delay, and within 60 calendar
  days (§ 164.410(b)). It identifies each individual affected, "to the extent possible" ((c)(1)), and gives what
  else the covered entity needs for its own notices ((c)(2)).
- **What the system gives:**
  - The audit events and storage refs name the items and tables an incident touched, by ID, with no PHI.
  - KMS logs show whether a KEK was used to decrypt.
  - To name the individuals, `patients --item-id <items>` (or a span) lists every record with its patients'
    identifiers, sealed to the privacy official's key and audited (DESIGN §11.4).
  - `reader.record_read` names who read the records, and for what purpose.
  - `audit-verify` shows the trail wasn't changed, for the burden of proof below.
- **The covered entity's notices:**
  - individuals within 60 days (§ 164.404(b));
  - the media, for more than 500 residents of a state or jurisdiction (§ 164.406);
  - HHS: at the same time as individuals for 500 or more, otherwise in a yearly log within 60 days of the year's
    end (§ 164.408).
  - Law enforcement may ask for a delay (§ 164.412).
- **Burden of proof:** the covered entity or business associate must show either that every notice was given or
  that there was no breach (§ 164.414(b)). So the operator keeps the audit trail and the KMS logs.

## 6. Enforcement and recognized security practices

- **Penalty tiers** (§ 160.404(b)(2)):
  - (i) did not know, and with reasonable diligence would not have known;
  - (ii) reasonable cause;
  - (iii) willful neglect, corrected within 30 days;
  - (iv) willful neglect, not corrected within 30 days. This is the highest tier.
  - The amounts are adjusted each year at 45 CFR Part 102.
  - The factors for setting the amount are listed in § 160.408: extent, harm, history and financial condition.
- **Recognized security practices** (HITECH § 13412):
  - HHS must consider practices that were in place for the previous 12 months, in its fines, audits and remedies.
  - NIST SP 800-66r2 notes that HHS 405(d) HICP and the NIST Cybersecurity Framework qualify. Its CPRT dataset
    maps each Security Rule specification to CSF 1.1 subcategories and SP 800-53r5 controls.
  - Mapping the controls above onto the CSF, and keeping evidence that they ran for 12 months, is the operator's
    defence.
  - Not having them can't raise a fine (§ 13412(b)(2)).
- **Evidence the code already produces:**
  - audit events;
  - signed SBOMs and passing scans;
  - test reports for failover, outage and load;
  - the design's revision history.

## 7. The proposed Security Rule, measured against the system

**Not law, and it may change before it is final.** The proposals below are as reported, from memory and secondary
sources. They are **not verified against the NPRM text** (§9).

| Proposal | Where the system stands |
|---|---|
| Addressable specifications become required; encryption of ePHI at rest and in transit is mandatory, with narrow exceptions | Met at rest. In transit: Kafka and Postgres TLS are tested and in the reference settings (§8.1 items 7, 13); the PoC still runs them in plaintext (§8.5). |
| MFA for access to systems holding ePHI | Human access is the operator's. Workloads already use short-lived workload identity, with no passwords for AWS. |
| A technology asset inventory and network map, reviewed every 12 months | Terraform and the manifests describe the whole stack, and SBOMs list the software (DESIGN §14.4). Network policies allow each stage egress only to DNS, Postgres, Kafka, AWS endpoints and the Pod Identity agent (AWS-POC §10). |
| Vulnerability scans every 6 months, and a penetration test every 12 months | Scanning is built (DESIGN §14.5). No penetration test has been done. |
| Network segmentation | Namespaces per pipeline, network policies, and inbound closed. Private subnets are still open (AWS-POC §9). |
| Restore the relevant systems and data within 72 hours | Recovery from a crash or outage takes minutes (§3.2). Restore after data loss is untested, and needs the bucket backups of §3.2. |
| Business associates verify their safeguards in writing every 12 months, and report activating the contingency plan within 24 hours | The operator's. The audit events show when recovery ran. |
| An annual compliance audit, and written incident response plans | The operator's. |

## 8. Gaps and considerations, for a product its customers operate

Each item says whether it is a **product gap** (code or features), something we owe customers in **documentation**,
or the **operator's** own duty that the product can only support.

The outstanding work is restated, with what's decided since, in §11.

### 8.1 Product gaps

1. **Audit delivery and tamper evidence: built 2026-11-20** (DESIGN §12.3, revision (eg)).
   - **What it does:** every audit event is also written to a journal in the stage's own Postgres schema
     (`sdh_audit_chain`, `sdh_audit_event`, `sdh_audit_seal`). Each process writes its own chain: every entry is
     numbered and hashed (SHA-256) over the one before. The pods seal the chain's head with the stage's signing key
     (ES256, through the pod's key and its statement) every `sdh.telemetry.journal.seal-interval` (1 minute) and
     when they stop. Events emitted before the journal opens are held and written first.
   - **Never dropped:** a write that fails is retried with backoff; when the queue (50,000 events) is full, the
     stage waits rather than drop one. The compliance check `audit-journal` refuses to start a stage with the
     journal off.
   - **Commands, on every stage:** `audit-verify` checks a span's chains from their start, and reports gaps,
     changed or rewritten entries, a missing or wrong head and bad seals as changes (exit 1), and entries after the
     last seal or chains never closed as warnings. `audit-export` prints the events as JSON lines, with their
     hashes, for a SIEM. `audit-prune` removes whole chains older than the operator's retention.
   - **Limits:** a whole chain deleted shows only against the SIEM's copy. The admin's own chains are unsealed,
     because the admin holds no stage signing key. The OTLP export is still best effort; the journal is the record.
   - Federal buyers will map this to NIST SP 800-53 AU-9 and AU-10.
2. **Reads of PHI outside the stages: built 2026-11-20** (DESIGN §10.2).
   - `connector-queue-reader` audits every record it opens, before decrypting it: `reader.record_read`, with the
     reader's name (`auditAs`), a purpose of use code (`purpose`), and the record's item, seq, record ID and
     table. To the `sdh.audit` log by default, or to a journal (`audit`, such as `TelemetryReadAudit`).
   - The admin commands that read records (`patients`, `failures export`) read as `sdh-admin`, purpose `HOPERAT`.
     A stage reading its own input says so (`withinStage`) and isn't audited twice.
   - **Limit:** an application that reads the tables without the library isn't seen; CloudTrail data events are the
     check.
3. **The operator's identity on admin commands is self-asserted** (`--operator` or `$SDH_OPERATOR`).
   - **Decided 2026-11-20:** closed by the control API, whose callers are authenticated (§11.1 P1,
     [CONTROL-API.md](CONTROL-API.md)).
   - § 164.312(a)(2)(i) and NIST SP 800-66r2 ask that activity be traceable to a person.
   - **For now:** documentation ties it to the platform's audit of `kubectl exec`.
   - **Later:** the console, as a client of that API.
4. **Scoping a breach to individuals: built 2026-11-20** (DESIGN §11.4).
   - `patients` on a record stage (convert, transform) reads the stage's FHIR records, by item IDs or a span of at
     most 31 days, and finds the identifiers of every `Patient` in each: as a Bundle entry, contained, or at any
     depth.
   - With no identifiers on standard input, it lists every record with its patients (§ 164.410(c)(1)); with some,
     only the records naming them (§§ 164.524, 164.526, 164.528).
   - The result is sealed to the public key given on standard input; the output and the audit show counts only.
   - **Limit:** it finds patients by identifier, as the source gave them. Matching those to the covered entity's
     master patient index is the operator's.
5. **FIPS 140-validated cryptography: test and document a FIPS mode.**
   - HIPAA's breach guidance doesn't require it for data at rest (§5). Federal buyers, however, are bound to
     validated modules (FIPS 140-3, under FISMA), and many state buyers follow them.
   - The code asks for its ciphers through standard JCA names with no provider named (`RecordCrypto`), so a
     validated provider can be installed in the JVM.
   - Untested: the JOSE library, the HKDF code, the AWS SDK and the Iceberg clients under a FIPS-only provider.
   - The KMS and S3 FIPS endpoints can already be set (`sdh.crypto.aws.endpoint`, the catalog's client settings).
6. **Secure by default: built 2026-11-17** (DESIGN §7.6, revision (dr)).
   - **What it does:** every stage starts under `sdh.compliance.profile: hipaa` by default, and refuses to start on
     any of these:
     - a plaintext endpoint;
     - Postgres that doesn't verify the server;
     - a broker without TLS;
     - a connection it can't judge;
     - keys held locally;
     - no audit export;
     - storage without authentication;
     - a JVM that may dump memory.
   - **What it reports:** each check and setting, never a value.
   - **Waivers:** an operator may waive a check with a one-line reason (`sdh.compliance.waive.<check>`). The reason
     is audited at every start, which makes it the operator's documentation of an addressable choice
     (§ 164.306(d)(3)).
   - **Off:** `none` turns the profile off, and that is audited too.
   - **Not covered:** the profile checks the settings, not the far end. A server's certificate is checked by TLS
     itself, and a broker's ACLs aren't visible from the node.
7. **Broker and catalog TLS: tested 2026-11-20** (DESIGN revision (eh)).
   - **Kafka** (connector-broker `KafkaSecurityIT`): a broker that takes clients only over `SASL_SSL` (TLS,
     SCRAM-SHA-512) with the standard authorizer, and each stage's identity given only the ACLs DEPLOYMENT.md
     lists. Through the stages' own publisher, receive route and broker check:
     - a landing node sends and a split stage receives in its group and sends onward;
     - the split stage is refused a topic it only sends to, and another pipeline's topics and group;
     - a broker certificate from an untrusted CA, and a wrong password, are each refused and reported as `AUTH`.
   - **Postgres** (file-connector `PostgresTlsIT`): a server that takes TLS connections only. The lease pool (which
     the registries and the audit journal share) and the Iceberg JDBC catalog (with `jdbc.sslmode` and
     `jdbc.sslrootcert`) connect with `verify-full`. A certificate from another CA, and a connection without
     TLS, are refused.
   - **Still untested:** mTLS to Kafka (a client certificate in place of SCRAM), AWS MSK's IAM login, and TLS
     through PgBouncer. The sandbox and the AWS PoC still run both in plaintext (§8.5).
8. **Re-keying existing data:** built 2026-11-21 (P3, CONTROL-API-DESIGN.md §15.7).
   - `rotate` moves a stage's new data to another KEK; `rekey`, with a second person's approval, re-wraps what
     the stage stores under the old one, and `keys` shows when nothing is left under it. The key custodian then
     disables the old KEK. A KMS's own rotation keeps old versions and needs none of this.
   - The ID key is never rotated, by design (DESIGN §7.3): a re-key re-wraps it, the same key, under the new
     KEK. Customers should be told why.
9. **Isolation per customer, component or function is supported. Document it, and check it holds.**
   - Each pipeline has its own namespaces, KEKs, signing keys, topics and roles. That is the product's answer to:
     - an agent serving several covered entities;
     - a hybrid entity's components (§ 164.105);
     - clearinghouse isolation (§ 164.308(a)(4)(ii)(A));
     - Part 2 records (§4.5).
   - Destroying a pipeline's KEKs is a cryptographic erase of that customer's data: return or destruction at the
     end of a contract (§ 164.504(e)(2)(ii)(J)).
   - **Tested 2026-11-20** (DESIGN revision (eh)): file-connector `PipelineIsolationIT` lays out two pipelines on
     one Postgres, one object store and one Vault, as [HIPAA-OPERATIONS.md](HIPAA-OPERATIONS.md) §2.8 describes.
     Each split stage reaches its own database, buckets and keys, through the stages' own clients. It is refused
     the other pipeline's database, buckets, KEK and signing key, and writing its own input bucket. A data key
     wrapped under the other pipeline's KEK can't be unwrapped under its own. `KafkaSecurityIT` covers topics and
     groups.
   - **Found:** isolation needs a database per pipeline. A JDBC catalog keeps every namespace in the same tables,
     and the sandbox and the AWS PoC put every pipeline in one database, as one catalog role, so there one
     pipeline's stages could change another's catalog entries. They can't read its data, which is under other
     keys and in other buckets. The reference layout (`deploy/reference/postgres.sql`) gives each pipeline a
     database. A REST catalog with grants per principal is the other way.
10. **No security labels on sensitive data.**
    - The mapping doesn't mark Part 2, behavioural health or similar data (for example FHIR `meta.security`), so
      separating it rests on the pipeline alone.
    - **To consider:** labels set from the source document's confidentiality codes, for readers that filter by
      them (minimum necessary, § 164.514(d)).
11. **No support bundle.**
    - Support today relies on the customer copying logs and `status` output by hand.
    - **To consider:** a command that gathers versions, configuration with its secrets removed, `status`, error
      codes and metrics. None of these hold PHI, by design. That keeps support out of PHI, and us out of the
      business-associate role (§1).
12. **Backup and restore are not designed as a whole.**
    - The data spans the object store (Iceberg data and metadata), the catalog and registries in Postgres, and the
      KMS keys. They must be restored to the same point: a catalog pointing at metadata that isn't in the bucket,
      or a registry ahead of its tables, breaks the pipeline.
    - § 164.308(a)(7)(ii)(A)–(B) are the operator's, but only the product can say how to back up and restore
      consistently.
    - **To consider:** a documented, tested procedure, and a check after a restore.
13. **Reference configuration with the profile on: built 2026-11-20** (`deploy/reference/`, DESIGN revision (eh)).
    - Each stage's settings (landing, split, convert, transform), under `hipaa` with nothing waived:
      - Kafka over `SASL_SSL`, with the login from the environment;
      - Postgres with `verify-full`, in the pipeline's own database;
      - AWS KMS;
      - the audit export.
    - Beside them, the pipeline's database layout (`postgres.sql`) and each stage's ACLs (`kafka-acls.sh`).
    - `ReferenceConfigurationTest` runs each file through the profile as the node does at startup, with its
      image's JVM options: nothing refused, nothing waived. The AWS PoC's settings, with the profile on, are
      refused.
    - **Not yet run:** no deployment has been made from these files. The sandbox and the AWS PoC still run
      `profile: none` on synthetic data.
14. **Restore output: sealed by default since 2026-11-20** (DESIGN §11.4). `restore` writes `<id>.jwe`, one JWE a
    line, each a part of up to 8 MiB, its protected header naming the item, part, offset and whether it is the
    last, so parts can't be dropped, reordered or moved unseen. Plaintext needs `--plaintext --ticket <ref>`, with
    the ticket audited.
15. **No ready-made collector configurations for the audit store.** To do before the product is final (deferred
    2026-11-20).
    - [HIPAA-OPERATIONS.md](HIPAA-OPERATIONS.md) §2.7 says what the operator's collector must do and what each
      store needs (exporter, identity and rights, network, retention and lock), but in words only. The one full
      configuration is the sandbox's Loki one, a demonstration without a disk-backed queue or TLS.
    - **To do:** ship in the distribution kit one example collector configuration per store (CloudWatch Logs,
      Cloud Logging, Azure Monitor, Splunk, Elastic, Loki, OTLP or syslog to another SIEM), each keeping every
      event, with a disk-backed queue, unlimited retry and TLS, beside its minimal identity (an IAM policy, a
      role binding). Check each with the collector's `validate` command; run the CloudWatch one end to end on the
      AWS PoC once its collector has its IAM role (§8.5), and the others as customers' clouds allow.

### 8.2 Documentation we owe customers

Items 1, 3 (in part), 4 (the break-glass part) and 6 are now in §10 and
[HIPAA-OPERATIONS.md](HIPAA-OPERATIONS.md); the others are still owed.

1. **A HIPAA shared-responsibility matrix.** This is §3–§5 restated per specification: met by the product,
   capability the operator configures, or the operator's alone.
2. **A security configuration guide.** It covers:
   - each setting that bears on HIPAA: TLS on every transport, broker ACLs (DEPLOYMENT.md), the audit export,
     KMS rather than a local keystore, key rotation, and retention;
   - the safe values, and what each control does.
3. **The audit event catalogue**, with every field, for the operator's SIEM and its activity reviews
   (§ 164.308(a)(1)(ii)(D)).
4. **Backup, restore and disaster recovery procedures** (§8.1, item 12), and an emergency access (break-glass)
   procedure built on `admin restore` (§ 164.312(a)(2)(ii)).
5. **Data flows and storage locations:**
   - where PHI is at each stage, and in what form (ciphertext, keys, identifiers on the broker);
   - for the operator's risk analysis (§ 164.308(a)(1)(ii)(A)) and asset inventory.
6. **Retention and destruction:** how retention, the given-up purge and KEK destruction remove data, and what they
   don't remove (backups, logs). This supports the operator's disposal procedures (§ 164.310(d)(2)(i)) and
   return or destruction at the end of a contract.
7. **The clearinghouse question** for X12 intakes (§1), and Part 2 segregation (§4.5).
8. **The vendor's security commitments:**
   - vulnerability scanning;
   - how security fixes are released, and how quickly;
   - signed images and SBOMs (DESIGN §14.4, §14.5).
   - Operators need this for protection from malicious software (§ 164.308(a)(5)(ii)(B)) and for their own
     evaluations.

### 8.3 The operator's own duties, which the product supports but cannot meet

- BAAs (or MOUs between government bodies) with its providers.
- The risk analysis and risk management.
- Workforce policies, training and sanctions, and a named security official.
- Human access control with MFA, and the review of access.
- Activity review.
- Incident response, and breach assessment and notice.
- Contingency plans and their testing.
- The choice of retention period.
- Accounting of disclosures to individuals.
- The documentation it must keep for 6 years (§ 164.316(b)(2)(i)).

### 8.4 Government buyers: requirements next to HIPAA

These aren't HIPAA. They come with the buyers, and **none was checked against its source for this review**.
- **Federal agencies:**
  - **Controls:** FISMA, with NIST SP 800-53 controls at the system's impact level. Expect the product to be
    mapped onto them (NIST SP 800-66r2's CPRT mapping helps).
  - **Cryptography:** FIPS 140-validated modules (§8.1, item 5).
  - **Hosting:** FedRAMP applies only if we host it.
  - **Secure development:** an attestation of secure software development practices may be required of us as the
    software producer (the CISA form under OMB M-22-18 and M-23-16). Check its current status, which changed in
    2025.
- **State agencies:**
  - Each state's security policy, often based on NIST SP 800-53.
  - **Medicaid and exchange systems:** CMS's security requirements (MARS-E, and its successor).
  - **IRS Publication 1075,** if the data includes federal tax information (eligibility).
  - **Hosting:** state programs similar to FedRAMP (StateRAMP, now GovRAMP) apply only if we host it.
- **Deployment environments:**
  - The product runs on amd64 and arm64 (DESIGN §14.3), and on AWS, Azure and Google storage and KMS.
  - **Open:** GCP Cloud KMS has not been tested against the real service (DESIGN §7.4).
  - **Open:** sovereign clouds (AWS GovCloud, Azure Government) have not been tried. They are configured by
    endpoint and region.

### 8.5 Our own AWS PoC (synthetic data only)

What the AWS PoC would need for real PHI, as listed in AWS-POC §9. These are operator duties, and also where
the product must prove its capabilities:
- a BAA;
- private subnets;
- Kafka TLS and ACLs, and Postgres TLS;
- CloudTrail data events;
- a durable audit sink: the journal now keeps every event in Postgres, which survives `down` on its EBS volume, but
  the collector's logs pipeline still exports to `nop` (`deploy/aws/k8s/infra/telemetry.yaml`), so nothing
  reaches a separate store; sending it to CloudWatch Logs needs an IAM role for the collector (the steps, and
  those for other clouds and self-hosted stores, are in HIPAA-OPERATIONS.md §2.7);
- bucket backups: versioning is off, and bronze is the only copy once an object has landed;
- KMS rotation, which is off.

## 9. Sources, and what could not be checked

The official sources listed for this review were used where they could be reached, on 2026-11-17:

| Source | Used for | Reached? |
|---|---|---|
| eCFR, 45 CFR Parts 160, 162, 164 (`ecfr.gov/current/...`) | All CFR citations | The web pages redirected to a block page. Part 164 Subparts D and E were read through the eCFR API (`ecfr.gov/api/renderer/v1/...`). Part 164 Subpart C and Parts 160 and 162 were read from Cornell LII's copy of the eCFR text (`law.cornell.edu/cfr/text/45/...`), which shows § 160.103 amended through 91 FR 14404 (2026-03-24). |
| Pub. L. 116-321 (govinfo) | §2, §6 | Yes, the full text |
| Pub. L. 104-191, Pub. L. 111-5 (govinfo), H.R. 3103 (congress.gov) | Background only | Not re-read; nothing above rests on them beyond the section numbers cited |
| HHS portals: combined text; Security, Privacy and Breach rules; enforcement; HITECH enforcement rule | Breach guidance, cloud guidance, status of rules | **No:** hhs.gov refused every automated request (HTTP 403). The breach guidance was read instead in its Federal Register publication on govinfo (74 FR 42740, 2009-08-24, pages 42741–42743). HHS's cloud computing guidance, the NPRM's status and the Part 2 compliance date were not checked. |
| NIST SP 800-66 Rev. 2 (February 2024) | §3, §6 | Yes. It is guidance only. In its own words it does not "supplement, replace, modify, or supersede the Security Rule itself". |

Still to be checked against primary sources:
- **The 2025 NPRM:** its citation (90 FR 898) and the proposals in §7.
- ***Purl v. HHS* and its appeal.**
- **The 2024 Part 2 rule:** its citation and its 2026-02-16 compliance date.
- **The 2026-03-24 amendment to § 160.103.**
- **HHS's cloud computing guidance** on providers that hold only ciphertext.

## 10. Requirement by requirement: what the product does, and what the customer does

Each row is one requirement. **Product** is what the code does, or the capability it gives. **Customer** is who on
the customer's staff does what, with which commands and settings; the roles and procedures are in
[HIPAA-OPERATIONS.md](HIPAA-OPERATIONS.md) (§ numbers in this column are to it). A dash means nothing on that side.

### 10.1 Security Rule: technical safeguards (§ 164.312)

| Requirement | Product | Customer |
|---|---|---|
| Unique user identification (a)(2)(i) R | A workload identity per stage; admin commands audited with an operator name | Platform administrator: SSO-backed RBAC for `kubectl exec`, API server audit of `pods/exec` (§2.4). The name is self-asserted until the product authenticates operators (§8.1 item 3) |
| Emergency access (a)(2)(ii) R | `restore`, sealed by default; `status` and the audit commands without the KMS | Security official approves; pipeline operator runs it under a ticket (§5) |
| Automatic logoff (a)(2)(iii) A | No sessions in the product | Platform administrator: console, `kubectl` and SSO timeouts (§2.4) |
| Encryption (a)(2)(iv) A | Envelope encryption, a KEK per table, SSE-KMS beneath; the profile refuses local keys | Key custodian: KMS keys, rotation on (§2.6) |
| Audit controls (b) | Audit events, the durable journal, `audit-verify`/`-export`/`-prune`, reader audit | Security analyst: SIEM, weekly verify (§3); counsel: log retention |
| Integrity (c) | GCM, signed messages, fencing, nothing replaced | — |
| Authentication (d) | OAuth2 or mTLS push, signed messages, workload identity | Platform administrator: Kafka SASL and ACLs, Postgres TLS, MFA for people (§2.6) |
| Transmission security (e) | JWE between stages, TLS elsewhere; the profile refuses plaintext | Platform administrator: TLS on every transport (§2.6) |

### 10.2 Security Rule: administrative safeguards (§ 164.308)

| Requirement | Product | Customer |
|---|---|---|
| Risk analysis and management (a)(1)(ii)(A)–(B) R | This document, DESIGN §16–§17 | Security official: the analysis, kept current |
| Sanctions (C) R; security official (a)(2) | — | The customer's policies and appointment |
| Activity review (D) R | Audit events, metrics, `status`, `audit-verify` | Security analyst: daily SIEM review, weekly verify, recorded (§3.1–§3.2) |
| Workforce security, access management (a)(3)–(4) | Least-privilege roles, a KEK per table, `gold-reader` | Platform administrator grants; security official reviews (§1) |
| Clearinghouse isolation (a)(4)(ii)(A) R | A pipeline per function, with its own keys | Counsel decides whether it applies (§1 above) |
| Awareness, malware, log-in monitoring, passwords (a)(5) | Signed images, SBOMs, scans, restricted pods | Training, patching, login monitoring |
| Incident procedures (a)(6) R | Fatal, rejected and fenced events; `audit-verify`; `patients` by item | Security analyst detects; privacy official assesses (§6) |
| Contingency plan (a)(7) R | Crash recovery, failover. No backup feature yet (§8.1 item 12) | Platform administrator: versioning, Postgres PITR, restore to one point, tested yearly (§7) |
| Evaluation (a)(8) | — | Security official, periodically |
| Business associate contracts (b) R | — | Counsel: BAAs or MOUs with every provider |

### 10.3 Security Rule: physical safeguards (§ 164.310)

| Requirement | Product | Customer |
|---|---|---|
| Facility and workstations (a)–(c) | — | The cloud provider, under its BAA; the customer's own workstations, which hold the operator private keys (§2.3) |
| Disposal, media re-use (d)(2)(i)–(ii) R | No plaintext on disk; retention, purges, cryptographic erase; restore and lookups sealed | Key custodians destroy KEKs; platform administrator records media destruction (§8) |
| Accountability, backup copy (d)(2)(iii)–(iv) A | `kek_ref` and storage refs per record | Platform administrator: the inventory of storage and backups |

### 10.4 Policies and documentation (§ 164.316)

Product: versioned design documents, and compliance waivers audited at every start. Customer: the security
official's policies, the waivers' approvals, the review records of §3, and this documentation, kept 6 years.

### 10.5 Privacy Rule

| Requirement | Product | Customer |
|---|---|---|
| Minimum necessary § 164.502(b), § 164.514(d) | Per-stage keys, PHI-free logs and issues, sealed exports, per-pipeline isolation | Data consumer owners: grants per pipeline, reading only what the purpose needs (§4.2) |
| Access § 164.524; amendment § 164.526 | Everything kept and linked; `patients` finds a person's records | Privacy official logs the request; pipeline operator runs the lookup (§4.1) |
| Accounting § 164.528 | `reader.record_read` with reader and purpose | Data consumer owners name their readers and purposes (§2.5); privacy official produces the accounting (§4.1) |
| Return or destruction § 164.504(e)(2)(ii)(J) | Retention, purges, cryptographic erase, `audit-export` | The end-of-contract procedure (§8) |
| Part 2 (§4.5); labels | A pipeline per intake; no `meta.security` yet (§8.1 item 10) | Counsel decides; an intake of its own for Part 2 records |

### 10.6 Breach Notification Rule

| Requirement | Product | Customer |
|---|---|---|
| Secured PHI (74 FR 42742) | AES-256-GCM under KMS keys; JWE plus TLS in motion | Security official: the judgement against SP 800-111; TLS on every link |
| Discovery and risk assessment § 164.402 | Audit events by ID, the journal, `audit-verify` | Security analyst detects; privacy official assesses with counsel (§6) |
| Identifying individuals § 164.410(c)(1) | `patients --item-id`, sealed and audited | Pipeline operator runs it for the privacy official (§6) |
| Notices §§ 164.404–410; burden of proof § 164.414 | The audit export and verify results as evidence | Privacy official and counsel give the notices and keep the evidence |


## 11. Outstanding work (restated 2026-11-20)

The work still owed before a customer can run the product on real PHI, after the builds of revisions (d) and (e).
**Left out, as later phases already noted:**
- the web console and its role management, a separate project ([CONTROL-API.md](CONTROL-API.md));
- example collector configurations for each audit store (§8.1 item 15, before the product is final).

**Decided 2026-11-20 with the user:**
- the product needs, now, a supported HTTPS control API with a permission per operation, giving full control of a
  pipeline and of error remediation;
- the product needs a test harness that drives that API and the pipeline's operation at three levels:
  Testcontainers, the sandbox, and AWS.

[CONTROL-API.md](CONTROL-API.md) sets out that intent.

### 11.1 Product work

| # | Work | Closes | Notes |
|---|---|---|---|
| P1 | **The control API:** authentication (OIDC or mTLS), a permission per operation, the caller audited as principal, sealed PHI results, an OpenAPI contract; every operator command on every stage, the transform stage included | §8.1 item 3; § 164.312(a)(1), (b), (d) | The CLI's commands are already shaped for it (DESIGN §11.4). Designed: [CONTROL-API-DESIGN.md](CONTROL-API-DESIGN.md), build steps C1–C5. C1–C5 built (2026-11-21): the query and action commands are served over HTTPS with authentication, permissions and audit; actions run once per idempotency key with a checked reason; long operations are followed through a record; PHI results are sealed to keys registered in advance for each principal by someone else; and audit prune and plaintext break-glass need a second person's approval, the break-glass result held encrypted for one fetch. The CLI refuses those two under the profile unless `cli-four-eyes` is waived. |
| P2 | **Pause, resume and drain** of a stage or an intake, through P1 | Contingency operations § 164.308(a)(7)(ii)(C) | Built (2026-11-21, [CONTROL-API-DESIGN.md](CONTROL-API-DESIGN.md) C6): `pause`, `resume` and `control-state` on every stage, and `drain` on the split, convert and transform stages, through the API and the CLI. A pause is kept in the database, so it outlasts every pod; its pods stop taking new work within seconds, and what they took finishes. KEDA is paused by the runbook (HIPAA-OPERATIONS §7.1), not the API. |
| P3 | **Key rotation and re-keying,** through P1 | §8.1 item 8; incident response, the breach safe harbour | Built (2026-11-21, [CONTROL-API-DESIGN.md](CONTROL-API-DESIGN.md) C7): `keys`, `rotate` and `rekey` (four eyes) on every stage, through the API and the CLI. A rotation is kept in the database and taken by each pod at its next batch once it has checked the KEK; a re-key is carried out by the stage's maintenance, file by file, then its database rows and ID keys, and done only when nothing is left under the old KEK. The product never disables a KEK. |
| P4 | **Retention and erasure,** through P1: set and run retention, purge an item, erase a pipeline's data at the end of a contract | § 164.310(d)(2)(i); § 164.504(e)(2)(ii)(J) | Built (2026-11-21, [CONTROL-API-DESIGN.md](CONTROL-API-DESIGN.md) C8): `retention` and `retention-set` (within bounds the settings give), `purge` and `erase` (four eyes and a ticket) on every stage, through the API. Maintenance applies the retention in force at each run; a purge rewrites the files holding an item's rows and deletes the old ones, then its corrections; an erase, once the stage is paused, drops its tables and rows, marks it erased so its pods refuse to start, and answers a certificate signed with the stage's key naming the KEKs for the custodian to destroy. Per stage: no pipeline-wide approval yet. Destroying KEKs stays the KMS's (HIPAA-OPERATIONS §8). |
| P5 | **Config and health view, and the support bundle,** through P1: settings with secrets removed, health, versions, the compliance outcome | §8.1 item 11; keeps support out of PHI (§1) | |
| P6 | **The test harness:** one set of scenarios at three levels (CONTROL-API.md) | Testing and revision § 164.308(a)(7)(ii)(D); evidence for § 164.308(a)(8) | Synthetic data only. |
| P7 | **Backup and restore as a whole:** a consistent backup of buckets, catalog, registries and keys, a tested restore, and a check after it | §8.1 item 12; § 164.308(a)(7)(ii)(A)–(B) | Needed for the 72-hour restore the proposed rule asks for (§7). |
| P8 | **FIPS mode:** tested under a FIPS 140-3 validated provider | §8.1 item 5; federal buyers (§8.4) | The JOSE library, HKDF, the AWS SDK and the Iceberg clients are untested under it. |
| P9 | **Security labels** from the source's confidentiality codes (`meta.security`) | §8.1 item 10; minimum necessary § 164.514(d); Part 2 | |
| P10 | **Secrets in output URIs refused,** as they are on input URIs | § 164.312(a)(1) | Output, reprocess and dead-letter URIs aren't checked (DEPLOYMENT.md, "Broker rights"). |
| P11 | **Transports still untested:** Kafka with client certificates (mTLS) and with AWS MSK's IAM login; Postgres TLS through PgBouncer; GCP Cloud KMS against the real service; AWS GovCloud and Azure Government endpoints | § 164.312(d), (e); §8.4 | Kafka SASL_SSL and Postgres TLS are tested (§8.1 item 7). |
| P12 | **A deployment run from `deploy/reference/`,** with TLS everywhere and a database per pipeline, on the sandbox or AWS | §8.1 items 7, 9 and 13, proved end to end | The sandbox and the AWS PoC share one database across pipelines (§8.1 item 9). |
| P13 | **A penetration test** of a reference deployment and of the control API, by a third party, and its fixes | The proposed rule (§7); buyers' due diligence | After P1 and P12. |

### 11.2 Documents customers need to secure and operate a pipeline

Done: the shared-responsibility mapping (§10), the operating procedures ([HIPAA-OPERATIONS.md](HIPAA-OPERATIONS.md)),
the audit store wiring (§2.7 there), pipeline isolation (§2.8 there), and the reference settings
(`deploy/reference/`). Still owed:

| # | Document | What it tells the customer to secure |
|---|---|---|
| D1 | **Reference network architecture** | The zones: private subnets, no public endpoints. The one ingress: HTTP push behind TLS or mTLS. The control API inside the network only. Egress allowed only to the KMS, object store, broker, database and collector, over private endpoints. Default-deny network policies per namespace (`deploy/` has examples). DNS. The operator's path in (VPN, bastion or zero-trust access). A network map to keep and review. |
| D2 | **Identity and access architecture** | A workload identity per stage, with no static keys. Human access through SSO with MFA. The API's permissions mapped to the customer's roles (P1). Break-glass accounts. Access reviews, and leavers. |
| D3 | **Key management architecture** | The key hierarchy: KEKs per pipeline and layer, signing keys, inbox keys, the ID key, consumer keys, operator keys. Who holds each. Key policies. Rotation, re-keying (P3) and destruction. |
| D4 | **Platform hardening baseline** | Kubernetes: the restricted Pod Security Standard, Secrets encrypted at rest (or an external secret store), the API server's audit log, signed-image admission, a CIS benchmark for the nodes. Encryption at rest and logging for Postgres, Kafka and the object store, and bucket versioning or object lock. |
| D5 | **Data flows and where PHI is** | §8.2 item 5: each stage, what it holds and in what form, for the risk analysis and the asset inventory. |
| D6 | **Security configuration guide** | §8.2 item 2: every setting that bears on HIPAA, its safe value and what it does, starting from `deploy/reference/`. |
| D7 | **Audit event catalogue** | §8.2 item 3: every event and field, for the SIEM and the activity reviews. |
| D8 | **Backup, restore and disaster recovery** | §8.2 item 4, from P7: what to back up, how often, how to restore, the RTO and RPO it gives. |
| D9 | **Retention and destruction** | §8.2 item 6, completed with P4: what each removal deletes and what it leaves (backups, logs). |
| D10 | **The clearinghouse question and Part 2** | §8.2 item 7: when X12 conversion may make the operator a clearinghouse; how to keep Part 2 records apart (§2.8 there, P9). |
| D11 | **The vendor's security commitments** | §8.2 item 8: vulnerability scanning, how quickly fixes are released, signed images and SBOMs, how to report a vulnerability. |
| D12 | **Upgrades and patching** | How to take a release and its fixes without losing data (DEPLOYMENT.md, "Upgrading"), and how quickly. |
| D13 | **Control API deployment and use** | With P1: how to expose it, connect it to the identity provider, map the permissions, and register operators' sealing keys. |

### 11.3 Not product work

These aren't the product's, but no deployment holds real PHI until they are done:
- **Buyers' requirements beside HIPAA (§8.4):** none was checked against its source. Check them when a buyer names
  them: NIST SP 800-53 mapping, MARS-E, IRS Publication 1075, the CISA secure-development attestation.
- **Our AWS PoC (§8.5):** a BAA, private subnets, TLS, CloudTrail data events, bucket versioning, KMS rotation, and
  the collector's IAM role. All are only needed if we put real PHI on it.
- **The operator's own duties (§8.3):** these stay the customer's.
