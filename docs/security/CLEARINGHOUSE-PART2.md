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

**It is written for a customer that operates the product.** The vendor will also offer managed services using this
pipeline, under separate agreements; there the vendor runs the pipeline for the customer, and may itself be in the
agent's position of §1.2 (C4). This document is meant to shape that offering too. The gaps it leaves are kept in
[KNOWN-GAPS.md](KNOWN-GAPS.md).

## 1. The clearinghouse question

### 1.1 What the product does to standard transactions

**X12.** The convert stage turns each X12 record into JSON of its own shape (`x12/1`, DESIGN.md §6.4): every
segment, the envelopes too, with each element kept as received, positions stable, composites one string. That
JSON is not a standard transaction. No mapping in the product turns X12 into FHIR yet; a customer may write one
(the transform stage runs any WSTL set). EDIFACT is converted the same way (`edifact/1`).

**C-CDA, when it is a claims attachment.** Since 2026, a health care claims attachment is a standard transaction
(Part 162 Subpart T, 91 FR 14350, 2026-03-24; effective 2026-05-26, compliance required by 2028-05-26). Its
standards (§ 162.2002) are HL7's C-CDA-based attachment guide (Release 2, version 2.1.0.1) and the C-CDA Templates
for Clinical Notes R2.1, carried in an X12 275 (006020X314) and requested by an X12 277 (006020X313). The
product's main conversion, C-CDA into FHIR through the transform stage, is processing a standard transaction into
nonstandard content when the C-CDA arrives as a claims attachment. The product doesn't know whether a document
arrived as one: that depends on the intake and the source.

FHIR, CSV and the other formats are not Part 162 standard transactions.

X12 conversion has not been tested beyond the converter's unit tests (DEPLOYMENT.md, "Formats: what has been
tested").

### 1.2 When the operator may be a clearinghouse

> **For review (C1): the definition and its reading.** The text below was checked on 2026-11-25 against the
> eCFR (current to 2026-11-23). Counsel to confirm the reading of it.

A health care clearinghouse (§ 160.103) is "a public or private entity, including a billing service, repricing
company, community health management information system or community health information system, and
'value-added' networks and switches", that does either of two functions. Paragraph (2): it "receives a standard
transaction from another entity and processes or facilitates the processing of health information into
nonstandard format or nonstandard data content for the receiving entity". Paragraph (1) covers the reverse:
nonstandard information from another entity made into standard data elements or a standard transaction. A
clearinghouse is a covered entity (§ 160.103, "covered entity" (2)).

§ 162.930 says what a clearinghouse may do as a covered entity's business associate, and (a) reads close to the
agent's case: "receive a standard transaction on behalf of the covered entity and translate it into a nonstandard
transaction (for example, nonstandard format and/or nonstandard data content) for transmission to the covered
entity". On that reading, it turns on **whose transactions the operator converts, and for whom:**

| Operator | What it runs | Likely position (for counsel) |
|---|---|---|
| A health plan, or a state Medicaid agency | its own pipeline, converting the transactions it receives | a covered entity already, converting for itself: not a clearinghouse for that |
| An agent (a TPA, a PBM, a fiscal agent) | a pipeline converting a covered entity's received transactions, for that entity | may be a clearinghouse, acting as that entity's business associate |
| An agent serving several covered entities | a pipeline per entity | as above, for each |
| A government agency (a hybrid entity) | a pipeline for a health care component | depends on the component; the component's PHI is kept from the others in any case (§ 164.105) |
| The vendor, under a managed service | a pipeline per customer, converting that customer's received transactions | as an agent: the customer's business associate, and may be a clearinghouse (C4) |
| Any operator | C-CDA received as claims attachments (from 2028-05-26 at the latest) | as for X12, by the rows above (C5) |
| Any operator | only FHIR, CSV, or C-CDA that isn't a claims attachment | Part 162 transactions aren't involved |

> **For review (C2): the agent's case.** Is an agent converting a covered entity's received X12 into JSON, or
> into FHIR through a customer's mapping, a clearinghouse? Does it matter that the output is for the covered
> entity's own use, not sent on as a transaction? For a health insurer's claims, whether the agent sits in front
> of the plan or behind it is set out in [PAYER-DATA-EXCHANGE.md](PAYER-DATA-EXCHANGE.md) §2.2 (D14, Q2).

> **For review (C4): the vendor's managed service.** Where the vendor runs a pipeline for a customer that
> converts the customer's received X12, is the vendor a clearinghouse, and what must the service's agreements
> and isolation provide for it (§1.3)?

> **For review (C5): claims attachments.** Is an agent (or the vendor, under a managed service) converting a
> health plan's received C-CDA claims attachments into FHIR a clearinghouse, from 2028-05-26 or from when the plan
> first receives attachments under the standard? Does it matter whether the C-CDA is still in its X12 275 when
> the pipeline receives it?

### 1.3 What follows if it is

> **For review (C3): the consequences.** Counsel to confirm each consequence, and whether any is missing.

- **The Security Rule's required isolation** (§ 164.308(a)(4)(ii)(A)): a clearinghouse that is part of a larger
  organization must protect its ePHI from access by the larger organization. Run the clearinghouse function as a
  pipeline of its own (§3), with readers of its own.
- **Part 162** (transactions, code sets and identifiers): what it requires of a clearinghouse converting for a
  covered entity (§ 162.923(c), § 162.930).
- **The Privacy Rule, in part** (§ 164.500(b)(1)): a clearinghouse that creates or receives PHI as a business
  associate of another covered entity must comply with §§ 164.500 to 164.504, 164.512, 164.532 and 164.534 only,
  and may use or disclose PHI only as its business associate contract permits. For PHI it creates or receives
  other than as a business associate, the whole Privacy Rule applies ((b)(2)).
- **A business associate agreement** with each covered entity it converts for.

### 1.4 What the product supports

- **A pipeline per function, or per covered entity,** each with its own database, buckets, KEKs, signing keys,
  topics and broker identities, namespace and readers (§3). `PipelineIsolationIT` checks that one pipeline's
  stages are refused another's database, buckets and keys.
- **X12 kept as received.** Each element is kept; nothing is reworded, dropped or coded anew, so the converted
  record can be traced to the transaction it came from (positions in each record's origin).
- **The vendor is not the clearinghouse when the customer operates the product.** The vendor doesn't operate the
  pipeline or see its data: support never needs PHI (HIPAA.md §1). Under a managed service it does operate it
  (C4).

## 2. Part 2 records

### 2.1 What Part 2 asks, as the product understands it

> **For review (P1): the 2024 rule.** Each point below was checked on 2026-11-25 against 42 CFR Part 2 in the
> eCFR (current to 2026-11-23), and the dates against the 2024 rule as published (89 FR 12472, 2024-02-16):
> effective 2024-04-16, compliance required by **2026-02-16**, so it is in force. Counsel to confirm the reading.

- **What it covers** (§ 2.12(a), (b)): records that would identify a patient as having or having had a substance
  use disorder, and contain SUD information obtained by a federally assisted SUD program (a "part 2 program") for
  treatment, diagnosis or referral. A program is federally assisted if, among others, it is a Medicare provider,
  is authorised to conduct maintenance treatment or withdrawal management, or is registered to dispense controlled
  substances for SUD treatment.
- **A single consent for treatment, payment and health care operations** (§ 2.33(a)(2), (b)(1)): a part 2
  program, covered entity or business associate may use and disclose records under it as the HIPAA regulations
  permit, until the patient revokes it in writing; a covered entity or business associate that receives them may
  redisclose them as HIPAA allows, except for proceedings against the patient.
- **A notice with each disclosure made with consent** (§ 2.32): one of two statements the section gives, and a
  copy of the consent or a clear explanation of its scope.
- **Proceedings against the patient** (§ 2.12(a)(2), § 2.32, §§ 2.64–2.65): records may not be used or
  disclosed in civil, criminal, administrative or legislative proceedings against the patient without the
  patient's consent or a court order under subpart E, and not to bring or substantiate criminal charges.
- **Keeping records apart** (§ 2.12(d)(2)(i)(C)): a part 2 program, covered entity or business associate that
  receives records under a single consent for treatment, payment and health care operations "is not required to
  segregate or segment such records". Part 2 limits what may be done with them wherever they are.
- **Security** (§ 2.16(a)): a part 2 program or other lawful holder must have formal policies and procedures
  against unauthorised use and disclosure, covering for electronic records their creation, receipt, maintenance
  and transmission, destruction (sanitising the media so the information is not retrievable), access, and
  de-identification to § 164.514(b).
- **Breaches** (§ 2.16(b)): the Breach Notification Rule applies to part 2 programs for breaches of unsecured
  records as it applies to covered entities.

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
- **C-CDA claims attachments became a standard transaction in 2026** (Part 162 Subpart T, compliance by
  2028-05-26), so the clearinghouse question reaches the product's main conversion, not only X12 (§1.1, C5). The
  product can't tell an attachment from any other C-CDA document, and no intake receives an X12 275.
- **The rules were checked against their primary sources on 2026-11-25** (Sources); what they mean for a given
  operator is still counsel's (C1–C5, P1).

## 5. Open questions

| ID | Question | For | Answer |
|---|---|---|---|
| C1 | Is §1.2's reading of the clearinghouse definition (§ 160.103, (1) and (2)) right? | counsel | *open* |
| C2 | Is an agent converting a covered entity's received X12 into JSON or FHIR, for that entity's own use, a clearinghouse? | counsel | *open* |
| C3 | Are §1.3's consequences right, and complete (Part 162, § 164.500(b), isolation, the BAA)? | counsel | *open* |
| C4 | Under a managed service converting a customer's received X12, is the vendor a clearinghouse, and what must the agreements and isolation provide? | counsel, product owner | *open* |
| C5 | Is converting a health plan's received C-CDA claims attachments into FHIR a clearinghouse function, and from when? | counsel | *open* |
| P1 | Is §2.1's reading of the 2024 Part 2 rule right? (Its text and dates were checked: compliance required by 2026-02-16.) | counsel | *open* |
| P2 | Way A (an intake of its own) as the recommendation, with way B allowed where sources label every record? | counsel, product owner | *open* |
| P3 | Should the product add anything to help readers with Part 2's redisclosure notice, or leave it to them? | product owner | *open* |

## Sources

- **Code and design:** the X12 and EDIFACT converters (DESIGN.md §6.4); the C-CDA labels (DESIGN.md revisions (fe),
  (ff); `secondary/security.wstl` in the mapping repository); the gold row's `confidentiality` column;
  `PipelineIsolationIT`.
- **Documents:** HIPAA.md §1 (the clearinghouse question), §4.5 (Part 2), §9 (what wasn't checked), §11.1 P9;
  HIPAA-OPERATIONS.md §2.8; DATA-FLOWS.md; RETENTION.md; KNOWN-GAPS.md.
- **Rules, checked on 2026-11-25:** 45 CFR § 160.103 ("health care clearinghouse", "covered entity"), § 162.923,
  § 162.930, Part 162 Subpart T (§§ 162.2001, 162.2002), § 164.105, § 164.308(a)(4)(ii)(A), § 164.500(b), and
  42 CFR Part 2 (§§ 2.12, 2.16, 2.32, 2.33), all in the eCFR through its API, current to 2026-11-23
  (`ecfr.gov/api/versioner/v1/full/2026-11-23/...`); the dates of 89 FR 12472 (2024-02-16) and of 91 FR 14350
  (2026-03-24, claims attachments) in their Federal Register text (`federalregister.gov`).
