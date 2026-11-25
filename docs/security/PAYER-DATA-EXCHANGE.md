# Payer data exchange: CARIN Blue Button, CARIN Digital Insurance Card and the CMS APIs

> **DRAFT, for review.** This document is not yet for customers. It sets out the vendor's reading of the rules
> that apply when a health insurer uses the product to turn its claims and coverage data into FHIR for CARIN
> Blue Button and CARIN Digital Insurance Card, and what follows for the customer, the product, the vendor and the
> managed service. **It is an interpretation, not legal advice.** Sections marked **For review** need an answer
> from counsel or the product's owner; each has an ID, and they are listed together in §8. Rules marked
> "checked" were read in their primary text on 2026-11-25; the rest are marked as not checked.

For the product's owner, counsel, the managed services division, and, once reviewed, the customer's privacy
official, security official and the team building its CMS APIs. It is HIPAA.md §11.2's document D14. It builds on
the clearinghouse question and Part 2 ([CLEARINGHOUSE-PART2.md](CLEARINGHOUSE-PART2.md), D10), the vendor's
commitments ([VENDOR-COMMITMENTS.md](VENDOR-COMMITMENTS.md), D11), and the gaps register
([KNOWN-GAPS.md](KNOWN-GAPS.md)).

## 1. The case

**Health insurers want one vendor to move every input stream to FHIR:** clinical documents (C-CDA, which the
product already converts to US Core), and claims, remittance and enrollment data, mostly X12, for the FHIR
APIs CMS requires and for NCQA's move of HEDIS to digital measures. The FHIR they need is:
- **CARIN Blue Button** (CARIN IG for Blue Button, `hl7.fhir.us.carin-bb`): claims as `ExplanationOfBenefit`,
  with `Coverage`, `Patient`, `Practitioner` and `Organization`, for the Patient Access API. Published versions:
  2.0.0 (STU 2, adopted by ONC in 45 CFR 170.215(k)(1)(i) by the HTI-4 final rule, 90 FR 37167), 2.1.0, and
  2.2.0, the latest (checked).
- **CARIN Digital Insurance Card** (`hl7.fhir.us.insurance-card`): the member's card as `Coverage` and related
  resources, shown through a FHIR API or a SMART Health Link. Latest 1.1.0 (STU 1.1), 2.0.0 in ballot
  (checked). No CMS rule requires it; CMS's Health Technology Ecosystem pledges name it for payers' digital
  insurance cards (not checked against CMS's own pages).
- **Da Vinci PDex** and **US Core**, beside CARIN Blue Button, for the Provider Access and Payer-to-Payer APIs and
  for clinical data.

**What the product has today for X12:** the landing and split stages take X12 by transaction set, functional
group or interchange, and the convert stage turns each into JSON of its own shape (`x12/1`), every segment and
element kept as received (DESIGN.md §6.4). **No mapping turns any X12 transaction into FHIR,** and nothing in the
product knows an 837 from an 835. Pharmacy claims, which come as NCPDP, aren't read at all. A pipeline carries
one type of content (DESIGN.md §5.3), so a claim (837) and its payment (835) arrive in different pipelines.

## 2. The insurer's standing

### 2.1 Converting its own data

> **For review (Q1): the insurer's own conversion.** Counsel to confirm the reading below.

**A health plan is entitled to process its own claims data,** and to convert it to FHIR, as the vendor reads the
rules:
- The claims, remittances and enrollment it receives or creates are its own PHI, used for payment and health care
  operations (§ 164.506), and the conversion is part of both.
- Making them available through the Patient Access API is how it meets its CMS requirement (§2.3) and is a
  disclosure to the individual, or at the individual's direction, under the right of access (§ 164.524, §
  164.502(a)(2)).
- Converting received standard transactions for its own use doesn't make it a health care clearinghouse: the
  definition is of an entity processing them "for the receiving entity" (§ 160.103, paragraph (2); checked),
  and the plan is the receiving entity, already a covered entity (D10 §1.2, first row).

### 2.2 Agents, and where they sit

> **For review (Q2): agents in front of and behind the plan.** Counsel to confirm.

An agent (a TPA, a Medicaid fiscal agent, a PBM, a vendor) running the pipeline for a plan is the plan's business
associate. Whether it is also a clearinghouse turns, on the vendor's reading of § 162.930 (checked), on where it
sits:
- **In front of the plan:** it receives providers' X12 (837s, from them or their clearinghouses) on the plan's
  behalf and translates it into something nonstandard for the plan. That is close to § 162.930(a)'s words, and
  likely a clearinghouse function (D10 C2).
- **Behind the plan:** it receives the plan's own claims data after the plan has received or adjudicated it (from
  its claims system, or the X12 it has already taken in), and converts it for the plan's APIs. That is processing
  as a business associate, and less likely a clearinghouse function.

C-CDA received as claims attachments is a standard transaction from 2028-05-26 at the latest (D10 §1.1, C5), so
the same question reaches C-CDA conversion. **If the April 2026 proposed rule is finalised** (§2.3), FHIR itself
becomes the HIPAA standard for prior authorization and its eligibility transactions, and FHIR in and out would be
standard transactions there.

### 2.3 What CMS requires of the insurer

These apply to **impacted payers**: Medicare Advantage organizations (42 CFR 422.119, 422.121), state Medicaid and
CHIP fee-for-service programs (§ 431.60, § 457.730), Medicaid managed care plans and CHIP managed care entities
(through § 438.242 and § 457.1233), and QHP issuers on the Federally-facilitated Exchanges (45 CFR 156.221). The
text below is § 422.119 and § 422.121's, checked; the others are written to match and were not each checked.
**Commercial and employer plans outside these programs aren't required** to offer the APIs, though many will.

| Requirement | What it asks | Since |
|---|---|---|
| **Patient Access API** (§ 422.119(a)–(b)) | adjudicated claims, with provider remittances and enrollee cost-sharing, within **1 business day** of processing; encounter data within 1 business day of receipt; USCDI data the plan maintains; Part D claims and formulary for MA-PD plans | 2021-01-01, for data with a date of service on or after 2016-01-01 |
| **Prior authorizations in the Patient Access API** (§ 422.119(b)(1)(iv)) | status, dates, items, denial reason and documentation, within 1 business day, kept a year after the last change | 2027-01-01 |
| **Technical** (§ 422.119(c)) | FHIR, SMART and related standards in 45 CFR 170.215; content to USCDI (§ 170.213) and Part 162; **routine testing that the privacy and security features meet HIPAA, 42 CFR Part 2 and Part 3** | 2021-01-01 |
| **Public documentation** (§ 422.119(d)) | the API's syntax, what an app needs, and how to register | 2021-01-01 |
| **Denying an app** (§ 422.119(e)) | only for an unacceptable security risk under its risk analysis, by objective criteria applied alike to all apps | 2021-01-01 |
| **Usage metrics** (§ 422.119(f)) | unique enrollees whose data went to an app, once and more than once, de-identified, by March 31 each year | 2026 |
| **Enrollee resources** (§ 422.119(g)) | plain-language privacy guidance on choosing apps, and how to complain to OCR and the FTC | 2021-01-01 |
| **Provider Access API** (§ 422.121(a)) | the same data less remittances and cost-sharing, to attributed in-network providers within 1 business day of a request; attribution; **opt-out** | 2027-01-01 |
| **Payer-to-Payer API** (§ 422.121(b)) | 5 years of the same data less remittances and cost-sharing, to and from previous and concurrent payers; **opt-in**; request within 1 week, answer within 1 business day | 2027-01-01 |
| **Proposed: required IGs** (91 FR 19890, 2026-04-14) | conformance to CARIN Blue Button, PDex and the other IGs now only recommended, through any unexpired version in 45 CFR 170.215; endpoint reporting; more metrics | proposed for 2027-10-01; **not final** (no final rule in the Federal Register on 2026-11-25) |

**MA organizations stay responsible for what their contractors do** (§ 422.504(i), checked): the plan "maintains
ultimate responsibility" for its contract with CMS whatever it delegates, and keeps books and records for 10
years (§ 422.504(d), checked).

### 2.4 HIPAA beside the CMS rules

- **The Security Rule** covers the pipeline and the APIs alike (§3 of HIPAA.md; the security documents D1–D13).
- **The right of access** (§ 164.524): from 2027-07-19, 15 calendar days, a copy in the form asked for, and no
  fee through a personal health application (HIPAA.md §4.7). The Patient Access API answers most of it at once.
- **An app the member chooses isn't the plan's business associate;** once the plan has sent the data at the
  member's direction, the plan isn't responsible for what the app does with it, as HHS and CMS have said (not
  checked against their text for this draft). The plan's duty is the enrollee resources above.
- **Information blocking** (45 CFR Part 171) applies to health care providers, certified health IT developers and
  health information networks; a plan is not one of them as such (HIPAA.md §4.6).

### 2.5 Part 2 records in claims

> **For review (Q3): Part 2 claims.** Counsel to confirm the reading, and the product's owner to decide Q4.

- **A plan is a "third-party payer" under Part 2,** and Part 2's restrictions follow the records a part 2
  program disclosed to it (42 CFR 2.12(d)(2)(i)(A), checked). A claim from a part 2 program for SUD treatment is
  such a record in the plan's hands.
- **CMS says so too:** the API's privacy features must be tested against 42 CFR Part 2 (§ 422.119(c)(2)).
- **The Patient Access API** discloses to the patient, or at the patient's direction (§ 2.23, not checked for
  this draft).
- **The Provider Access and Payer-to-Payer APIs** disclose to others. Under the 2024 rule a plan that received
  the records under a single consent for treatment, payment and operations may redisclose them as HIPAA allows,
  except for proceedings against the patient (§ 2.33(b)(1), checked); otherwise consent is needed. Each
  disclosure made with consent needs § 2.32's notice and the consent or its scope (checked).
- **So the plan must know which claims came from part 2 programs.** The claim doesn't say so; the plan knows its
  providers. A list of part 2 programs' provider identifiers, kept by the plan, would let a pipeline label or
  route their claims.

> **For review (Q4): labelling Part 2 claims.** Should the product label, or route to a pipeline of their own,
> the claims of providers on a customer's list of part 2 programs (with HL7's privacy law code `42CFRPart2`,
> say)? It would be a label the product applies from the customer's list, not one it invents.

**State law** may be stricter for some kinds of data (mental health, HIV, reproductive health, minors), and may
limit what the Provider Access and Payer-to-Payer APIs may carry. That is the plan's to judge, state by state.

### 2.6 HEDIS and quality reporting

NCQA is moving HEDIS to digital measures: CQL logic over FHIR data, with the hybrid method being retired by
measurement year 2029, on measure-specific timelines (NCQA's notices; not checked against NCQA's current hub).
What that asks of the data, as the vendor understands it:
- **Completeness and provenance:** a measure result is only as good as the data behind it, and an auditor
  (NCQA's HEDIS Compliance Audit) asks where each element came from. The product keeps every source document,
  drops nothing, and records each record's origin and positions in it; a FHIR resource can be traced to the X12
  segment or C-CDA element it came from.
- **NCQA's measure specifications are licensed by NCQA.** The product computes no measure; a customer or a
  managed service that runs them needs NCQA's licence (not checked).

## 3. What the customer must do

Most of what CMS asks is the customer's, and outside the pipeline: the pipeline delivers FHIR to gold, and the
customer's FHIR server and authorisation server expose it.

| Obligation | The pipeline's part | The customer's part |
|---|---|---|
| Data within 1 business day | Landing to gold in minutes (PERFORMANCE.md: p99 under 200 seconds measured at 35 documents a second) | Feeding the pipeline as claims are adjudicated, not in a weekly batch; loading gold into its FHIR server |
| Data back to 2016-01-01 | The initial load: sized for 15 million documents (DEPLOYMENT.md) | Extracting the history; deciding what "maintained" covers |
| Adjudicated claims with remittance and cost-sharing | **Not yet:** needs the claim and its adjudication together (§4.1) | Supplying the adjudication: the 835, or its claims system's own extract |
| Corrections, reversals and adjustments | Each a new revision linked to the one it supersedes; nothing replaced (STAGES.md §15.8) | Showing the current version in its API, keeping the history |
| The FHIR API, SMART, app registration and denial criteria, documentation, metrics, enrollee resources | None | All |
| Provider attribution, opt-out, opt-in, payer-to-payer requests | None | All |
| Security and privacy testing of the API, Part 2 included | The pipeline's own controls (D1–D13) | The API's, and its testing (§ 422.119(c)(2)) |
| Part 2 claims identified and handled | Labels it is given (D10 §2.2); a separate pipeline (D10 way A); Q4 | The list of part 2 programs; consent, notices and limits in its APIs |
| Code system licences (CPT, NUBC, CARC/RARC, CDT) | Carries the codes as received | Its own licences for its use of them (§4.3) |
| HEDIS | Complete, traceable FHIR | The measures, NCQA's licence and audit |
| Right of access in 15 days | Finding a person's records (HIPAA-OPERATIONS.md §4.1) | The copy, and its procedure |

## 4. What the product must meet

### 4.1 Product work

> **For review (Q5): the X12 scope.** The product's owner to decide which transactions, in what order, and
> whether claims-system extracts (CSV, fixed width) come before X12.

- **Mappings, as WSTL sets the transform stage runs** (any set, chosen at run time):
  - 837 professional, institutional and dental, with the 835, to CARIN Blue Button `ExplanationOfBenefit`
    (professional/non-clinician, inpatient and outpatient institutional, oral);
  - 834 enrollment to `Coverage`, `Patient` and the CARIN Digital Insurance Card;
  - pharmacy claims from NCPDP, or the PBM's extract, to CARIN Blue Button's pharmacy profile (NCPDP isn't read
    yet);
  - 278 prior authorization, for the prior authorization data due in the Patient Access API from 2027-01-01.
- **Joining a claim and its adjudication.** An EOB needs both what was billed and what was paid. They arrive
  separately, in different transactions and pipelines, and often days apart. This needs a design: a join keyed
  on the claim's identifiers, with an EOB revised as each part arrives, never replacing one (the no-supplanting
  rule), and a held state, visible, for a claim waiting on its payment.
- **Several IG versions at once.** The proposed rule allows any unexpired version in 45 CFR 170.215; the
  transform stage already runs the WSTL set it is given, so a set per version, with the version recorded on each
  resource (`meta.profile`), fits.
- **Conformance checked:** each mapping's output validated against the IG's profiles, as US Core is for C-CDA.
- **Part 2 labels from a provider list** (Q4).
- **Scale:** a plan's history since 2016 is far larger than the 15 million documents sized so far, and claims
  are small and many; the X12 path hasn't been run at the pipeline level at all (DEPLOYMENT.md, "Formats").

### 4.2 The vendor's licences

> **For review (Q6): licences.** Counsel and the product's owner to confirm what the vendor needs.

- **X12.** X12's standards and implementation guides (TR3s) are copyrighted. A product that embeds X12 material,
  such as maps derived from TR3s, needs a commercial licence from X12; a developer licence covers only
  development (x12.org, "Licensing Program"; not reviewed in full). The maps can't be published openly.
- **Code systems in mappings and value sets:** CARIN Blue Button's terminology page names AMA's CPT, X12's
  claim adjustment reason codes, NUBC's codes (with the AHA as secretariat), NCPDP's standards and 3M's APR-DRGs
  as owned by others, some needing licences; HCPCS Level II is CMS's and free. A product that ships code
  descriptions, or tables of codes, needs licences for them; one that only carries the codes the customer sends
  may not. Recommended for review: the product ships codes, never descriptions, and requires the customer's own
  licences.
- **CARIN and Blue Button marks:** "Blue Button" is a registered mark of HHS, per the IG.

### 4.3 What the vendor commits to

Added to D11's open questions as V11–V13 (§8):
- **IG versions kept current:** a mapping set for each new version ONC adopts in 45 CFR 170.215, released before
  the version it replaces expires.
- **Mapping defects as security-grade defects:** a wrong amount or code in an EOB is shown to members and used
  for quality reporting; fix times as for High (D11 V2).
- **Support without PHI:** mapping faults reproduced with synthetic X12, as for every other format.

## 5. For the managed services division

A managed service turning a plan's claims into FHIR makes the vendor the plan's business associate (KNOWN-GAPS.md
§4). The service agreements need, beyond the business associate agreement:

| Topic | What to settle |
|---|---|
| **Where the service sits** | Behind the plan (its adjudicated data) or in front (providers' X12 on its behalf). In front, the vendor may be a clearinghouse: Part 162, § 164.500(b), and isolation from the vendor's other business (§ 164.308(a)(4)(ii)(A)) apply (Q2; D10 C4) |
| **The 1 business day** | Service levels from the plan's adjudication to gold (or to the plan's FHIR server, if the service runs it), measured in the plan's business days, with backlog recovery after an outage counted against them |
| **Who runs the APIs** | If the service hosts the FHIR and authorisation servers: app registration and denial by objective criteria, public documentation, metrics for CMS, uptime, and the plan's sign-off on each. The plan stays answerable to CMS (§ 422.504(i)) |
| **Flow-downs** | CMS's terms for an MA organization's first tier and downstream entities; state Medicaid contract terms, often with data kept in the US and the state's security standard (MARS-E, or its successor; not checked) |
| **Part 2** | Who keeps the list of part 2 programs, who applies consent and notices, and whether Part 2 claims run in a pipeline of their own (D10 way A) |
| **Licences** | The vendor's X12 licence for operating maps for third parties; who holds CPT, NUBC and others for the service's use (Q6) |
| **HEDIS** | Whether the service runs measures (NCQA's licence; its audit's access to the service's records and audit trail), or only supplies FHIR |
| **Records** | MA records kept 10 years (§ 422.504(d)); retention in the pipeline set to match; return or destruction at the end (RETENTION.md) |
| **Breach and incidents** | Notice to the plan sooner than the Breach Notification Rule's outer limit, and how Part 2 records in a breach are told apart |
| **The 2026 Privacy Rule** | BAAs revised by renewal on or after 2027-07-19, or by 2028-01-18; whether the vendor sends copies to members directly (§ 164.502(a)(4)(ii); HIPAA.md §4.7) |
| **Liability** | For errors in what members see (amounts, cost-sharing), and for quality results built on the data |
| **Federal programs** | FedRAMP if a federal agency buys the service; an SBOM of the runtime environment on request (OMB M-26-05; HIPAA.md §8.4) |

## 6. Findings

- **No X12 transaction is mapped to FHIR,** and NCPDP isn't read: CARIN Blue Button can't be produced from
  X12 today.
- **A claim and its payment can't be joined:** one type of content per pipeline, and no join between pipelines.
- **The product can't tell a part 2 program's claim** without a list from the customer.
- **The X12 path hasn't run at the pipeline level** (DEPLOYMENT.md, "Formats"), and nothing is sized for a payer's
  history since 2016.
- **The vendor holds no X12 licence** for distributing maps, as far as this project records.
- **The proposed rule** (91 FR 19890) would require CARIN Blue Button and PDex from 2027-10-01 and make FHIR a
  HIPAA standard for prior authorization; neither is final.

## 7. Customer guidance, once reviewed

What the customer-facing version will tell an insurer, subject to the answers in §8:
1. Your conversion of your own claims to FHIR is yours to do, as a covered entity (Q1). An agent doing it for you
   is your business associate; where it receives providers' transactions for you, ask your counsel whether it
   is a clearinghouse (Q2).
2. The pipeline gives you FHIR to CARIN Blue Button and US Core in minutes, traceable to its source, with nothing
   dropped or replaced. Your FHIR and authorisation servers, and everything §3 lists as yours, meet CMS's API
   requirements.
3. Give the pipeline your adjudication data, as it happens, and your list of part 2 programs.
4. Hold your own licences for the code systems in your data.
5. Test your APIs' privacy and security, Part 2 included, routinely, and keep the evidence.

## 8. Open questions

| ID | Question | For | Answer |
|---|---|---|---|
| Q1 | Is a plan converting its own claims data to FHIR, for its own APIs and use, outside the clearinghouse definition? | counsel | *open* |
| Q2 | Is an agent in front of the plan (receiving providers' X12 for it) a clearinghouse, and one behind it (converting its adjudicated data) not? | counsel | *open* |
| Q3 | Is §2.5's reading of Part 2 for claims right, including the Patient Access API as a disclosure to the patient (§ 2.23)? | counsel | *open* |
| Q4 | Should the product label or route part 2 programs' claims from a customer's provider list? | product owner, counsel | *open* |
| Q5 | Which transactions are mapped first (837, 835, 834, NCPDP, 278), and do claims-system extracts come before X12? | product owner | *open* |
| Q6 | Which licences does the vendor need (X12 commercial, CPT, NUBC, NCPDP, 3M), for the product and for the managed service? | counsel, product owner | *open* |
| V11 | Will the vendor release a mapping set for each IG version ONC adopts before the one it replaces expires? | product owner | *open* |
| V12 | Are mapping defects in what members see fixed as High (D11 V2)? | product owner | *open* |
| V13 | Is a managed service offered behind the plan only, or also in front of it (Q2)? | product owner, counsel | *open* |

## Sources

- **Checked on 2026-11-25, in the eCFR API (current to 2026-11-23):** 42 CFR 422.119, 422.121, 422.504(d) and
  (i); 42 CFR 2.12, 2.32, 2.33; 45 CFR 160.103, 162.930.
- **Checked in the Federal Register:** the 2024 interoperability and prior authorization final rule
  ([89 FR 8758](https://www.federalregister.gov/documents/2024/02/08/2024-00895)), the 2020 patient access final
  rule ([85 FR 25510](https://www.federalregister.gov/documents/2020/05/01/2020-05050)), and the 2026 proposed rule
  ([91 FR 19890](https://www.federalregister.gov/documents/2026/04/14/2026-07205)), its IG and HIPAA standard
  proposals and their dates.
- **IG versions:** the FHIR package registry (`packages.fhir.org`), for `hl7.fhir.us.carin-bb` and
  `hl7.fhir.us.insurance-card`.
- **Read in secondary form, not checked:** CARIN Blue Button's
  [terminology licensure page](https://hl7.org/fhir/us/carin-bb/STU2/Terminology_Licensure.html); X12's
  [licensing program](https://x12.org/licensing) and [IP use](https://x12.org/products/ip-use); CMS's
  [Interoperability Framework](https://www.cms.gov/interoperability); NCQA's
  [hybrid measure timeline](https://www.ncqa.org/blog/ncqas-proposed-timeline-for-retiring-and-replacing-hedis-hybrid-measures/).
- **Not checked:** 42 CFR 431.60, 457.730, 438.242, 457.1233, 45 CFR 156.221 (assumed to match § 422.119);
  42 CFR 2.23; HHS's and CMS's statements on apps not being business associates; state Medicaid security standards.
- **Project documents:** DESIGN.md §5.3, §6.4; STAGES.md §15.8; DEPLOYMENT.md, "Formats"; PERFORMANCE.md;
  HIPAA.md §4.6, §4.7, §8.4; HIPAA-OPERATIONS.md §4.1; D10, D11, KNOWN-GAPS.md, RETENTION.md.
