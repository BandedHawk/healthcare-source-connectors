# Known gaps, and what each waits on

> **Internal.** This is the vendor's register, not a customer document. It gathers in one place every gap the
> security documents (D1 to D13) and HIPAA.md found, and says what each waits on. Many can't be closed until a
> review, a production build pipeline or a service agreement exists, so they are kept here until then rather than
> lost in the documents that found them. Gaps already corrected in a document are not listed.

The product's outstanding work is HIPAA.md §11; this register adds what the security documents found since, sorted
by what has to happen first. Each row names where the gap was found, so the finding there stays the full account.

## 1. Two ways the product is offered

- **The customer operates it.** The customer deploys a release in its own environment and is the covered entity
  or business associate; the vendor never has its data (HIPAA.md §1). The security documents are written for
  this case.
- **The vendor operates it, as a managed service.** The vendor will also offer managed services using this
  pipeline, under separate agreements. There the vendor creates, receives, maintains and transmits the customer's
  PHI, so it is the customer's business associate (or subcontractor), and the vendor's and the customer's
  obligations are reshaped around that relationship.

The documents in this project shape both: the managed service's own runbooks, controls and commitments are to be
drawn from them, and the customer documentation for a managed service from what they say the operator does. Where a
gap below is different under a managed service, its row says so.

## 2. Waiting on review: legal standing and the vendor's commitments

These need counsel or the product's owner before the documents that raise them are published, or the product
changed. The questions are in [CLEARINGHOUSE-PART2.md](CLEARINGHOUSE-PART2.md) §5 (D10) and
[VENDOR-COMMITMENTS.md](VENDOR-COMMITMENTS.md) §4 (D11).

| Gap | Waits on | Found in |
|---|---|---|
| Whether an operator converting X12 is a health care clearinghouse, and what follows | C1–C3, counsel | D10 §1 |
| Whether the vendor is a clearinghouse when it converts X12 for a customer under a managed service | C4, counsel | D10 §1 |
| Whether converting received C-CDA claims attachments into FHIR is a clearinghouse function: a Part 162 standard transaction since 2026, compliance by 2028-05-26 | C5, counsel | D10 §1.1; HIPAA.md §1 |
| Whether D10's reading of the 2024 Part 2 rule is right (its text and dates were checked) | P1, counsel | D10 §2.1 |
| How Part 2 records are kept apart: an intake of their own, or labels in a shared pipeline | P2, counsel and the product's owner | D10 §2.3 |
| Whether the product helps readers attach Part 2's redisclosure notice | P3, the product's owner | D10 §2.3 |
| Confidentiality labels read only from C-CDA, not other formats | P2 and P3 first: whether labels are relied on at all | D10 §2.2, §4 |
| The vendor's role, under this document and under a managed service | V1, the product's owner and counsel | D11 §1.3 |
| Fix times, supported releases, advisories, rescans | V2–V5, the product's owner | D11 §2 |
| A reporting channel and a disclosure policy | V6–V7, the product's owner and counsel | D11 §3 |
| Where releases are published, and built in CI with provenance | V8; also §3 below | D11 §1.4 |
| CISA's secure software development attestation, for federal buyers: no longer required since OMB M-26-05, but agencies may ask for it, or for an SBOM | V9; also §3 below, since it attests the build process | D11 §4; HIPAA.md §8.4 |
| FIPS 140 mode: not supported, untested | V10, the product's owner; then HIPAA.md P8 | D11 §1.4; HARDENING.md |
| The 2026 Privacy Rule changes (91 FR 74848, compliance by 2027-07-19), reviewed in HIPAA.md §4.7: whether a health plan's gold is an "electronic health record" for the right to direct a copy (§ 164.524(d)) | counsel | HIPAA.md §4.7 |
| Business associate contracts to revise for the 2026 Privacy Rule, by renewal on or after 2027-07-19 or by 2028-01-18 at the latest, the managed service's among them; under a managed service, whether the vendor sends copies to individuals directly (§ 164.502(a)(4)(ii)) | the operator's counsel; the managed service agreements | HIPAA.md §4.7 |
| Still unchecked against primary sources: the Security Rule NPRM's proposals beyond patching, *Purl v. HHS*, HHS's cloud computing guidance | a check, then counsel | HIPAA.md §9 |
| Health insurers' standing to convert their own claims, agents in front of or behind the plan, Part 2 in claims, X12 scope, and the vendor's licences (X12 commercial, CPT, NUBC, NCPDP, 3M) | Q1–Q6, counsel and the product's owner | PAYER-DATA-EXCHANGE.md §8 |
| Vendor commitments for payer data: IG versions kept current, mapping defects fixed as High | V11–V12, the product's owner | PAYER-DATA-EXCHANGE.md §4.3 |
| CMS's proposed rule (91 FR 19890): CARIN Blue Button and PDex required from 2027-10-01, FHIR a HIPAA standard for prior authorization | a final rule | PAYER-DATA-EXCHANGE.md §2.3 |
| Buyers' requirements beside HIPAA (NIST SP 800-53, MARS-E, IRS Publication 1075) | a buyer naming them | HIPAA.md §8.4, §11.3 |

## 3. Waiting on the production build pipeline

**The project is a proof of concept, outside the company's standard development process.** Its builds don't run in
the standard CI pipeline, and they don't go through the static and dynamic security scans that process includes.
The build tooling here (`scripts/sign-release.sh`, `scripts/vuln-scan.sh`, `scripts/base-images.sh`) stands in for
it, on a workstation. Once the build is incorporated into the production pipeline, every gap below is to be closed
there, and D11 §1 and each kit's "Verify what you received" revised to describe what the production pipeline does.

| Gap | To do in the production pipeline | Found in |
|---|---|---|
| No static analysis (SAST) of the code in the standard process. **Interim, 2026-11-27:** `./mvnw -Psast verify` runs SpotBugs 4.10.5 with Find Security Bugs 1.14.0, and PMD 7.30.0 with `build/sast/pmd-ruleset.xml`, in both repositories, report only. First run: 302 SpotBugs findings after the exclusions (231 of them `SQL_INJECTION_JDBC` on SQL built from table names), 164 PMD; the transformer 2 and 3. Not triaged; not in any build by default | the standard process's static scans, on every build of every module and of the transform stage's repository; then remove the `sast` profile and each `build/sast/` | D11 §1.4 |
| No dynamic scans (DAST) | the standard process's dynamic scans, of the control API and the push intake on a running pipeline | this register; HIPAA.md P13 is the customer's own pen test, not this |
| No secrets scanning of the code | the standard process's secrets scanning | D11 §1.4 |
| Builds and signing on a workstation, so no build provenance (SLSA) | builds and signing in CI, with a provenance attestation beside each image's SBOM; the signing key stays in Key Vault | D11 §1.4 |
| The transform stage was added to `sign-release.sh` on 2026-11-27 (its tarball, jar SBOM, customer and service images, scanned and signed with the others), but the script hasn't been run since: it stops on stale base image pins | all four stages released by the same pipeline; until then, update the pins (`scripts/base-images.sh update`, then the image tests) and run the script once | D11 §1.4 |
| No release has been published: no release registry, nowhere for tarballs, signatures and `cosign.pub` | the production pipeline's registry and publication | D11 §1.4; DESIGN.md §14.4 |
| No release notes | release notes with a release, under fixed headings: a contract major version, a table, a compliance check, a setting | UPGRADES.md §7 |
| No rescans of released SBOMs | the pipeline's scheduled rescans of supported releases (V5) | D11 §2 |
| No VEX statements for findings the product can't reach | published with the SBOMs (V4); the reasons in `docker/vuln-accepted.yaml` are their start | D11 §2 |

## 4. Waiting on the managed service agreements

The managed service's agreements decide these. The documents here are written for a customer that operates the
product, so each needs a counterpart, or a change, for a managed service.

| Gap | What the agreements must settle | Found in |
|---|---|---|
| The vendor is a business associate under a managed service, and its staff may see PHI | the BAA, its subcontractors (the cloud, KMS, object store and broker providers), and which of the vendor's staff may reach PHI, with access reviewed as IDENTITY.md sets out | HIPAA.md §1; D11 §1.3 |
| The operator's duties become the vendor's, or are shared | which of HIPAA.md §10's "customer" column, and of HIPAA-OPERATIONS.md, the vendor carries: risk analysis, audit review, incident response, breach notice to the customer (§ 164.410), backups and the restore drill, retention and erasure at the end of a contract | HIPAA.md §8.3, §10 |
| The vendor may be a clearinghouse for a customer whose X12 it converts | C4 | D10 §1 |
| Who holds the keys | whether the KEKs are in the customer's KMS or the vendor's; KEYS.md assumes the customer's | KEYS.md; D11 §1.3 |
| FedRAMP, for a managed service offered to federal agencies; and an SBOM of the runtime production environment, which OMB M-26-05 lets agencies require of a cloud platform | the authorisation the service needs, and how runtime SBOMs are produced | HIPAA.md §8.4 |
| The "service" images (Ubuntu, with a shell) run only the vendor's own hosted service | whether the managed service runs the customer images instead, hardened as HARDENING.md sets out | DESIGN.md §14.1 |
| Upgrades and fix times | the vendor applying fixes itself, within the proposed Security Rule's windows, rather than releasing them for a customer to apply | UPGRADES.md §6; D11 V2 |
| A payer data service: in front of the plan or behind it; who runs the FHIR APIs; CMS and state flow-downs; 1-business-day service levels; MA records for 10 years; NCQA's licence and audit | V13 and the service agreements | PAYER-DATA-EXCHANGE.md §5 |
| A pipeline per customer | each customer's pipeline isolated as HIPAA-OPERATIONS.md §2.8 sets out, with keys and readers of its own | D10 §3 |
| Part 2 records | which way of keeping them apart the service offers (P2), and who applies Part 2's notice and limits | D10 §2 |

## 5. Product gaps

Each needs only the product's owner to schedule it. Rows marked "a future project" are the payer claims
work, beyond this project ([PAYER-X12-FUTURE-WORK.md](../PAYER-X12-FUTURE-WORK.md)).

| Gap | Change owed | Found in |
|---|---|---|
| The control Deployments aren't checked for version | refuse a control pod whose release isn't its workers' | UPGRADES.md §7 |
| No check that a token came from an MFA sign-in | a setting naming the required `acr` values or `amr` methods, enforced under the profile | IDENTITY.md §9 |
| A pipeline's stages share one database role | a role per stage, with rights on its own tables only | IDENTITY.md §9 |
| The CLI applies no permissions | permissions in the CLI, or the CLI refused under the profile except for break-glass | IDENTITY.md §9 |
| `kafka-acls.sh` grants nothing to gold readers | a `reads` entry for a reader | IDENTITY.md §9 |
| `audit-verify` checks seals against the current signing key only | earlier keys accepted by `kid` | KEYS.md §8 |
| A stage's signing key or inbox key can be replaced only by draining | a second key accepted for the length of a replacement | KEYS.md §8 |
| The OTLP export carries no chain ID or sequence number | `sdh.chain_id` and the entry's sequence on each exported record | AUDIT-EVENTS.md §9 |
| Events before the journal opens are capped at 1,000 | wait, as the open journal does | AUDIT-EVENTS.md §9 |
| `Location` and `resultAt` ignore the ingress's prefix | a public base path setting, or `X-Forwarded-Prefix` | CONTROL-API-DEPLOYMENT.md §9 |
| Client certificates aren't checked for revocation | CRL or OCSP | CONTROL-API-DEPLOYMENT.md §9 |
| The kits' Deployments mount a service account token | `automountServiceAccountToken: false`, tried with Pod Identity | HARDENING.md §6 |
| The push intake has no rate or connection limit of its own | a limit in the product, or the load balancer's stays required | NETWORK.md §9 |
| `deploy/reference/` sets no retention, and the profile requires none | retention and bounds in the reference settings, and a compliance check | RETENTION.md §8 |
| A purge is asked stage by stage, and leaves the item's failures and ledger entry | a pipeline-wide purge, and what it leaves decided (HIPAA.md P4) | RETENTION.md §8 |
| An erasure certificate names only the KEKs in use at the erasure | the retired KEKs older backups need, named too | RETENTION.md §8 |
| A re-key can't move a stage's data to another KMS provider: one `wrap-provider` a stage | a second, unwrap-only provider during a re-key, so a pipeline can leave a KMS | HIPAA-OPERATIONS.md §7.3 |
| The `store-versioning` check asks for a bucket by its own name, with its own S3 client, so after a region fails over through a Multi-Region Access Point it can't reach the bucket and the profile refuses to start | the check follows Iceberg's `s3.access-points.<bucket>` mapping, as the tables' FileIO does | HIPAA-OPERATIONS.md §7, "Other losses"; §7.3 |
| A pipeline's tables can't be moved to another bucket, store or cloud: Iceberg's catalog and metadata name each file by its full path | a `relocate` command per stage, with four eyes: paused and drained, each table's metadata rewritten with Iceberg's `RewriteTablePathUtil` (in iceberg-core, no Spark), the files copied, the catalog and `warehouse` pointed at the new place, then `restore-check`. No re-encryption: the chunks' AAD binds IDs, not paths | HIPAA-OPERATIONS.md §7.3 |
| A pipeline's stages can't use separate databases: the restore's one point in time, and the resending of lost item ends (a stage reads the next stage's registry), both need one | for when one pipeline outgrows one server: the cross-stage read made a message or a call, and a restore of each server to the same time, tried with crashes | DEPLOYMENT.md, "Postgres under load" |
| Registry retention deletes rows a batch at a time, competing with the load | registry tables partitioned by time, a partition dropped at a time, if a soak shows the deletes cost | DEPLOYMENT.md, "Postgres under load" |
| A stage doesn't check that its memory can't be swapped to disk | a compliance check under the profile (`memory-swap`): refuse to start where the container may swap (cgroup v2 `memory.swap.max` not 0) or the host has swap on (`/proc/swaps`), with a waiver for encrypted swap | HARDENING.md §3 |
| The listeners (push, control, probes) bind every interface, with no setting to choose an address | a bind-address setting for each, for hosts with more than one network | NETWORK.md §2, §9 |
| A configuration change outside the version isn't recorded, and a landing intake's change (a directory to SFTP, say) not at all | at each start, audit a keyed digest of the settings outside the version and of the intake's identity (type, host or bucket, path, access mode, how it authenticates; never the values); `intake.changed`, for review, when it differs from the last start; perhaps refuse a type change under the same name until an operator accepts it | UPGRADES.md §3.5, §7 |
| A WSTL set isn't signed, and the pin (`sdh.transform.wstl-digest`) isn't required | a signed set (a signed OCI artifact, verified by an init container or at start), and a compliance check requiring the pin under the profile; also the way for a set too large for a ConfigMap (1 MiB) | UPGRADES.md §3.4, §7 |
| Buyers and implementation teams have no way to build their own WSTL set (for a proprietary CSV, a delimited or XML format, or any custom mapping): sets are packaged only by the vendor's mapping build | an authoring kit shipped with the transform stage: a template set (`main.wstl`, `wstl-set.json`), the packaging and `set-digest` as a tool that runs without the vendor's build, a local runner that maps sample records and checks determinism, and a guide: the JSON each convert mapping makes (`csv-row/1` and the others) as a set's input, writing and testing a mapping, packaging, pinning and deploying it (UPGRADES.md §3.4) | UPGRADES.md §3.4, §7 |
| The control API refuses a token issued for more than an hour, and Entra ID issues them for 60 to 90 minutes by default | check the time left (`exp`), or allow for the provider's variation, so a provider's default works without a token lifetime policy | IDENTITY-PROVIDER.md §8 |
| The control API's roles claim must be top-level, and a refused token doesn't say why | a claim path for the roles (`realm_access.roles`); a reason class on `control.unauthenticated` (issuer, audience, lifetime, signature, no principal), naming no value | IDENTITY-PROVIDER.md §8 |
| Cognito access tokens have an `aud` only when resource-bound | accept `client_id` as the audience for issuers that use it, or document the resource binding as tried | IDENTITY-PROVIDER.md §4.4, §8 |
| Telemetry names a stage's pipeline only by hand (`service.namespace=<pipeline>` in the kits, whose Deployments set `service.instance.id` since 2026-11-27) | a `sdh.pipeline` resource attribute from the settings | OBSERVABILITY.md §2.2, §9 |
| No dashboards or alert rules are shipped | a Grafana dashboard and a Prometheus rule file in the kits, from OBSERVABILITY.md §6.1 | OBSERVABILITY.md §9 |
| The convert and transform stages send no traces, and traces don't join across stages | spans for the record stages; the trace context carried on the stages' messages | OBSERVABILITY.md §6.2, §9 |
| The control harness runs its scenarios only against its own targets (Testcontainers, the sandbox, the AWS PoC), not a customer's cluster | a target configured by control URL, issuer and namespace, so a customer can run the scenarios on their test environment before go-live | ENVIRONMENT-SETUP.md §7, §11 |
| The US Core 6.1 set gives a `terminology.hl7.org` URI for only 14 HL7 v3 code systems; any other v3 OID (such as RouteOfAdministration, ObservationValue) reaches a terminology server as `urn:oid:` | map every `hl7.terminology.r4` v3 code system's OID to its canonical URI in `common::KnownCodeSystem` | mapping us-core-6.1 dist README, "HL7 v3" |
| The transform stage keeps no warnings from the set, so readers can't see which codings it flagged (a code with no code system) | keep warnings' codes and places with the item, as failures are kept, if readers need them | transform-stage README, "The WSTL set" |
| The product can't tell a Part 2 record | none, unless P2 or P3 asks for one | D10 §4 |
| No X12 transaction is mapped to FHIR (837, 835, 834, 278 to CARIN Blue Button, CARIN Digital Insurance Card, PDex), and NCPDP isn't read | a future project: mappings as WSTL sets, in the order Q5 decides, each validated against its IG | PAYER-X12-FUTURE-WORK.md §3.5 |
| A claim and its payment (837 and 835) can't be joined: one type of content per pipeline | a future project: two pipelines joined after gold, revising the EOB as each part arrives, with a visible held state | PAYER-X12-FUTURE-WORK.md §4 |
| Part 2 programs' claims can't be told apart | a future project: labels or routing from a customer's provider list, if Q4 says so | PAYER-X12-FUTURE-WORK.md §3.6 |
| The patient lookup reads a span of at most 31 days, untimed at size, while access requests must be acted on in 15 calendar days from 2027-07-19 | time a multi-year lookup at size; a longer span or an index of patient identifiers if it doesn't fit | HIPAA.md §4.7; HIPAA-OPERATIONS.md §4.1 |
| The product can't tell a C-CDA claims attachment from another C-CDA document, and has no intake for an X12 275 | depends on C5: an intake or a mark for attachments, if their pipeline must be kept apart | D10 §1.1, §4 |

## 6. Tests owed

| Test | Where it can run | Found in |
|---|---|---|
| No identity provider has been connected to the control API; only the harness's test issuer | the sandbox or a test pipeline, with trial tenants of Entra ID, Okta and Keycloak (IDENTITY-PROVIDER.md §4 to §6) | IDENTITY-PROVIDER.md §8 |
| Only Prometheus's OTLP receiver and Grafana LGTM have received the telemetry; a collector with a private CA is untried | the sandbox, with each back end's trial (OBSERVABILITY.md §5) and an `https://` collector | OBSERVABILITY.md §9 |
| Formats beyond C-CDA and CSV at the pipeline level: X12, EDIFACT, delimited, XML, FHIR, Parquet; X12 at a payer's size, with history since 2016 | locally or the sandbox; size on AWS | D10 §4; DEPLOYMENT.md, "Formats" |
| The release before run against tables a newer release made | locally | UPGRADES.md §7 |
| No terminology server has been run against the pipeline's gold; only the HL7 validator against tx.fhir.org, on the set's public samples | locally, with a terminology server loaded as the US Core set's README says, and synthetic documents | mapping us-core-6.1 dist README, "Setting up your terminology server" |
| An intake's type changed under the same name (a directory to SFTP): what the ledger takes for already landed | locally | UPGRADES.md §3.5, §7 |
| A whole-pipeline upgrade timed at size | the AWS session (HIPAA.md §11.4), with its cost approved | UPGRADES.md §7 |
| The transform stage's control API deployed, and through an ingress | the sandbox | CONTROL-API-DEPLOYMENT.md §9 |
| A control Deployment of each kind on one stage, tokens and certificates | the sandbox | CONTROL-API-DEPLOYMENT.md §9 |
| The network policies on a cloud deployment, and through an egress proxy | the AWS session | NETWORK.md §9 |
| Signed-image admission, a CIS benchmark, an external secret store, Object Lock | the sandbox or the AWS session | HARDENING.md §6 |
| A harness scenario that waits for retention to delete rows | locally | RETENTION.md §8 |
| A cryptographic erase rehearsed: a KEK destroyed, then a restore from an older backup tried | the sandbox | RETENTION.md §8 |
| A self-hosted site restore: SeaweedFS copied by `filer.backup` or `filer.sync` (do versions, delete markers and locks survive?), Postgres from a base backup and WAL archive, Vault from a raft snapshot with its unseal keys, then the restore steps; timed for an RTO | the sandbox, with a second SeaweedFS and Vault | HIPAA-OPERATIONS.md §7, "Self-hosted stores" |
| The restore drill at size, and the other steps of the AWS session | the AWS session | HIPAA.md §11.4 (A1, A3, A4) |
| A region failover through a Multi-Region Access Point with two-way replication (delete markers and replica changes kept): the stages start in the other region, and the restore repair lists versions through the access point | the AWS session, with its cost approved | HIPAA-OPERATIONS.md §7, "Other losses" |
| Postgres under a feed of many records an item (an X12 batch, a large CSV), and over a soak of a day or more: CPU, WAL, vacuum and retention's deletes | the AWS session, with its cost approved | DEPLOYMENT.md, "Postgres under load"; PERFORMANCE.md §11.1 |
| A re-key to a KEK in another account or region of the same KMS | the sandbox (Vault) or the AWS session | HIPAA-OPERATIONS.md §7.3 |
| The transports still untested | as each environment is prepared | HIPAA.md P11 |

## 7. The AWS PoC

The PoC holds synthetic data only. What it would need before real PHI is HIPAA.md §8.5, with what the security
documents found: public subnets and the KMS's public endpoint (NETWORK.md §9), the API server's audit log and
PgBouncer's connection logging off, and node root disk encryption unchecked (HARDENING.md §6).

## Keeping it up

When a document finds a gap, add it here with where it was found; when one is closed, remove it, and say so in the
document that found it. When a review answers a question in §2, or the production pipeline or a managed service
agreement settles a row of §3 or §4, move what is left of it to §5 or §6, or remove it.
