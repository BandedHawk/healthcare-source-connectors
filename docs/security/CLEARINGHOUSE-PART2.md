# The clearinghouse question and Part 2 records

> **DRAFT, for review.** This document is not yet for customers. Sections marked **For review** hold statements
> of law, or choices, that need an answer from counsel or from the product's owner before it is published; each
> has an ID, and the open questions are listed together in §5. Nothing here is legal advice to a customer: it
> tells the customer what the product does, and which questions are theirs to put to their own counsel.

For the security official and the privacy official who decide what a pipeline may carry and for whom, and the
platform administrator who sets up the pipelines that keep it apart. It covers when running the product may make
the operator a health care clearinghouse, and how to keep records covered by 42 CFR Part 2 (substance use
disorder treatment records) apart from other PHI. It is HIPAA.md §11.2's document D10 (§8.2 item 7 there). The
isolation it relies on is HIPAA-OPERATIONS.md §2.8; the labels the product carries are DESIGN.md revisions (fe)
and (ff) (HIPAA.md §11.1, P9); where PHI is held is [DATA-FLOWS.md](DATA-FLOWS.md) (D5).

**The product decides neither question.** It doesn't know who the operator converts data for, and it doesn't know
whether a record came from a Part 2 program. What it gives is a way to keep data apart (a pipeline of its own,
with its own database, buckets, keys, topics and readers) and the source's own confidentiality labels, carried
through as sent.

## 1. The clearinghouse question

### 1.1 What the product does to X12

The convert stage turns each X12 record into JSON of its own shape (`x12/1`, DESIGN.md §6.4): every segment, the
envelopes too, with each element kept as received, positions stable, composites one string. That JSON is not a
standard transaction. No mapping in the product turns X12 into FHIR yet; a customer may write one (the transform
stage runs any WSTL set). EDIFACT is converted the same way (`edifact/1`). C-CDA, FHIR, CSV and the other formats
are not Part 162 standard transactions.

X12 conversion has not been tested beyond the converter's unit tests (DEPLOYMENT.md, "Formats: what has been
tested").

### 1.2 When the operator may be a clearinghouse

> **For review (C1): the definition and its reading.** Counsel to confirm the reading below against the current
> text of § 160.103 and § 164.500(b).

A health care clearinghouse (§ 160.103, paragraph (2)) includes an entity that receives a standard transaction
from another entity and processes or facilitates the processing of health information into nonstandard format or
nonstandard data content for the receiving entity. Paragraph (1) covers the reverse: nonstandard information from
another entity made into standard data elements or a standard transaction. On that reading, it turns on **whose
transactions the operator converts, and for whom:**

| Operator | What it runs | Likely position (for counsel) |
|---|---|---|
| A health plan, or a state Medicaid agency | its own pipeline, converting the transactions it receives | a covered entity already, converting for itself: not a clearinghouse for that |
| An agent (a TPA, a PBM, a fiscal agent) | a pipeline converting a covered entity's received transactions, for that entity | may be a clearinghouse, acting as that entity's business associate |
| An agent serving several covered entities | a pipeline per entity | as above, for each |
| A government agency (a hybrid entity) | a pipeline for a health care component | depends on the component; the component's PHI is kept from the others in any case (§ 164.105) |
| Any operator | only C-CDA, FHIR, CSV or other non-X12 intakes | Part 162 transactions aren't involved |

> **For review (C2): the agent's case.** Is an agent converting a covered entity's received X12 into JSON, or
> into FHIR through a customer's mapping, a clearinghouse? Does it matter that the output is for the covered
> entity's own use, not sent on as a transaction?

### 1.3 What follows if it is

> **For review (C3): the consequences.** Counsel to confirm each consequence, and whether any is missing.

- **The Security Rule's required isolation** (§ 164.308(a)(4)(ii)(A)): a clearinghouse that is part of a larger
  organization must protect its ePHI from access by the larger organization. Run the clearinghouse function as a
  pipeline of its own (§3), with readers of its own.
- **Part 162** (transactions, code sets and identifiers): what it requires of a clearinghouse converting for a
  covered entity (§ 162.923(c), § 162.930).
- **The Privacy Rule, in part** (§ 164.500(b)): a clearinghouse acting as a business associate is held to the
  business associate's terms and only certain Privacy Rule provisions.
- **A business associate agreement** with each covered entity it converts for.

### 1.4 What the product supports

- **A pipeline per function, or per covered entity,** each with its own database, buckets, KEKs, signing keys,
  topics and broker identities, namespace and readers (§3). `PipelineIsolationIT` checks that one pipeline's
  stages are refused another's database, buckets and keys.
- **X12 kept as received.** Each element is kept; nothing is reworded, dropped or coded anew, so the converted
  record can be traced to the transaction it came from (positions in each record's origin).
- **The vendor is not the clearinghouse.** The vendor doesn't operate the pipeline or see its data: support never
  needs PHI (HIPAA.md §1).

## 2. Part 2 records

### 2.1 What Part 2 asks, as the product understands it

> **For review (P1): the 2024 rule.** Counsel to confirm each point against the current text of 42 CFR Part 2, as
> revised in 2024 (89 FR 12472), and its compliance date, reported as 2026-02-16 (HIPAA.md §9: unverified).

- Part 2 protects records of substance use disorder treatment from a federally assisted program, and what is
  derived from them, with consent and redisclosure rules of its own beside HIPAA's.
- Since 2024, a single consent may cover future uses for treatment, payment and health care operations, and a
  covered entity or business associate that receives records under it may redisclose them as HIPAA allows.
- Each disclosure made with consent carries a notice, and a copy of the consent or a summary of it.
- Records may not be used in proceedings against the patient without consent or a court order.
- The 2024 rule doesn't require Part 2 records to be kept apart from other records, but limits what may be done
  with them wherever they are.
- Breaches of Part 2 records are notified as HIPAA's Breach Notification Rule sets out.

### 2.2 What the product knows about a record

**The product doesn't know whether a record is a Part 2 record.** It carries what the source says:
- **Confidentiality labels** (DESIGN.md revisions (fe), (ff)): the C-CDA mapping puts the document's
  `confidentialityCode`, or a section's own, on every resource it makes, in `meta.security`; a statement's DS4P
  Privacy Annotation labels the resource made from it, and adds its obligation, refrain and purpose-of-use codes.
  The Privacy Markings section's codes go on every resource.
- **The high-water mark on the gold row:** the Bundle's most restrictive confidentiality, in plain in the gold
  row's `confidentiality` column, so a reader can leave rows out before decrypting them.
- **No label invented.** A record without a code has none. DS4P R1 for CDA defines no sensitivity codes (SUD, say),
  so none is carried, and none would be put on the gold row: a sensitivity code says what the record holds.
- **Other formats:** the product reads confidentiality coding only in C-CDA. It adds no label to X12, CSV or the
  rest, and what a FHIR resource's own `meta.security` becomes depends on the mapping the transform stage runs.

A `R` (restricted) label doesn't mean a Part 2 record, and a Part 2 record may arrive with no label at all.

### 2.3 Keeping them apart

> **For review (P2): which way the product recommends.** The draft recommends way A. Counsel and the product's
> owner to confirm, or choose B for customers whose sources label every Part 2 record.

| Way | How | What it gives | What it needs |
|---|---|---|---|
| **A. An intake of its own** (recommended) | a Part 2 program's records come in by an intake of their own, into a pipeline of their own (§3) | access to Part 2 records granted, audited, retained and destroyed apart from all other PHI; readers given that pipeline's consumer key only | the source can send Part 2 records separately |
| **B. Labels in a shared pipeline** | records of every kind in one pipeline; readers filter on the labels | one pipeline | every Part 2 record labelled by its source; every reader filtering, and refraining as the labels say. The product doesn't enforce a label |

**The product doesn't enforce Part 2's rules.** It doesn't check consent, attach a redisclosure notice or refuse
a reader. Those belong to whoever reads gold and discloses from it: the operator's readers apply Part 2's notice,
consent and limits (HIPAA.md §4.5).

> **For review (P3): the redisclosure notice.** Should the product carry anything to help a reader attach the
> notice (an obligation code it adds where the source has marked a record as Part 2, say), or does that stay with
> the readers? The draft leaves it with them, since the product invents no label.

### 2.4 What else holds Part 2 data

Part 2 data is held wherever DATA-FLOWS.md says PHI is: the landed items, each layer's records, the fail store's
corrected content, held results, and backups, all encrypted under the pipeline's KEKs. Operators' reasons and
tickets must not hold PHI (HIPAA-OPERATIONS.md §1). The end of a Part 2 pipeline's data is its erasure and the
destruction of its keys ([RETENTION.md](RETENTION.md) §6).

## 3. Setting up a pipeline of its own

For a clearinghouse function, a covered entity an agent serves, a hybrid entity's component, or Part 2 records,
deploy a pipeline as HIPAA-OPERATIONS.md §2.8 sets out:

| Resource | Its own |
|---|---|
| Database | `sdh_<pipeline>`, with only its stage roles connecting (`deploy/reference/postgres.sql`) |
| Buckets | bronze, silver and gold, each stage's role limited to its own |
| Keys | the KEKs and each stage's signing key, with key policies naming its stages |
| Broker | the topics `<pipeline>.*`, an identity and ACLs per stage (`deploy/reference/kafka-acls.sh`) |
| Kubernetes | a namespace, with default-deny network policies (`deploy/reference/network-policies.yaml`) and its own RBAC |
| Readers | a consumer key for that pipeline only; access reviewed apart ([IDENTITY.md](IDENTITY.md)) |
| Control API | its own control Deployments; operators given permissions on that pipeline only |

Then record, in the operator's documentation, which pipeline holds what, for whom, and under which agreement.

## 4. Findings

- **The X12 converter has not been tested at the pipeline level,** only by its unit tests.
- **The product doesn't know a record's Part 2 status,** and carries no sensitivity label: separating Part 2
  records rests on the source (way A) or on its labels (way B).
- **Only C-CDA's labels are read.** No other format's confidentiality coding is.
- **The 2024 Part 2 rule, its compliance date, and § 160.103's clearinghouse paragraphs have not been checked
  against their primary sources** for this draft (HIPAA.md §9).

## 5. Open questions

| ID | Question | For | Answer |
|---|---|---|---|
| C1 | Is §1.2's reading of the clearinghouse definition (§ 160.103, (1) and (2)) right? | counsel | *open* |
| C2 | Is an agent converting a covered entity's received X12 into JSON or FHIR, for that entity's own use, a clearinghouse? | counsel | *open* |
| C3 | Are §1.3's consequences right, and complete (Part 162, § 164.500(b), isolation, the BAA)? | counsel | *open* |
| P1 | Is §2.1's summary of the 2024 Part 2 rule right, and is its compliance date 2026-02-16? | counsel | *open* |
| P2 | Way A (an intake of its own) as the recommendation, with way B allowed where sources label every record? | counsel, product owner | *open* |
| P3 | Should the product add anything to help readers with Part 2's redisclosure notice, or leave it to them? | product owner | *open* |

## Sources

- **Code and design:** the X12 and EDIFACT converters (DESIGN.md §6.4); the C-CDA labels (DESIGN.md revisions (fe),
  (ff); `secondary/security.wstl` in the mapping repository); the gold row's `confidentiality` column;
  `PipelineIsolationIT`.
- **Documents:** HIPAA.md §1 (the clearinghouse question), §4.5 (Part 2), §9 (what wasn't checked), §11.1 P9;
  HIPAA-OPERATIONS.md §2.8; DATA-FLOWS.md; RETENTION.md.
- **Rules, not checked for this draft:** 45 CFR § 160.103 ("health care clearinghouse"), § 162.923, § 162.930,
  § 164.105, § 164.308(a)(4)(ii)(A), § 164.500(b); 42 CFR Part 2 as revised by 89 FR 12472 (2024).
