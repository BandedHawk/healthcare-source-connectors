# Payer claims to FHIR: the work, for a future project

> **Future work, not part of this project.** This project builds no X12 support beyond what it has (X12 split
> and turned into generic JSON, `x12/1`). This document hands the work to the team that will take it on: every
> stream of effort, technical and not, what each depends on, and what is already decided or found. The reading
> of the rules behind it is [security/PAYER-DATA-EXCHANGE.md](security/PAYER-DATA-EXCHANGE.md) (D14, a draft
> for review); its question IDs (Q1–Q6, V11–V13) and D10's (C2, C4, C5) are used here.

## 1. The goal

Health insurers want one vendor to move every input stream to FHIR. The product already turns C-CDA into US
Core. The new work turns their claims, remittance, enrollment, prior authorization and pharmacy data into:
- **CARIN Blue Button** `ExplanationOfBenefit`, with `Coverage`, `Patient`, `Practitioner` and `Organization`, for
  CMS's Patient Access API;
- **CARIN Digital Insurance Card** for the member's card;
- **Da Vinci PDex** for the Provider Access and Payer-to-Payer APIs;
- data complete and traceable enough for **NCQA's digital HEDIS** measures.

What drives it: CMS's API rules (42 CFR 422.119, 422.121 and their Medicaid, CHIP and Exchange counterparts),
with data due within 1 business day of adjudication; the 2027-01-01 Provider Access and Payer-to-Payer APIs;
CMS's proposal to require CARIN Blue Button and PDex from 2027-10-01 (91 FR 19890, not final); and NCQA retiring
the hybrid HEDIS method by measurement year 2029.

## 2. What exists, and what this project leaves

| Exists | Doesn't |
|---|---|
| Landing intakes (file, object store, push); X12 split by transaction set, group or interchange; `x12/1` JSON keeping every segment and element as received | Any mapping of an X12 transaction to FHIR |
| The transform stage running any WSTL set, chosen at run time, with US Core validation for C-CDA | Validation against CARIN Blue Button, CARIN Digital Insurance Card or PDex profiles |
| Fail store, reprocessing, revisions linked rather than replaced; origin positions on each record | A join of records from different pipelines |
| Confidentiality labels from C-CDA and DS4P | Labels from a list of part 2 programs' providers |
| Security, audit, keys, isolation, retention and operations, documented (D1–D13) | NCPDP pharmacy claims read at all |
| Performance measured for C-CDA (about 60 documents a second, p99 under 200 s) | X12 run end to end through a pipeline; sizing for a payer's history since 2016 |

## 3. The streams of work

Each stream says who leads it, what it needs first, and its rough size (S: weeks, M: one to three months, L: more,
for one team).

### 3.1 Decisions and review (product owner, counsel)

Needed before design is fixed. Size: S of effort, but may take months of elapsed time.

- **Legal standing:** Q1 (a plan's own conversion), Q2 (an agent in front of or behind the plan), Q3 (Part 2 in
  claims), D10's C2, C4 and C5 (clearinghouse, managed service, claims attachments).
- **Scope:** Q5: which transactions and in what order; whether claims-system extracts (CSV, fixed width), which
  carry the adjudication directly, come before X12; which payer types first (Medicare Advantage, Medicaid, the
  Exchanges, commercial).
- **The managed service:** V13: behind the plan only, or in front of it too. In front brings clearinghouse
  duties (§3.8).
- **Part 2:** Q4: labels or routing from a customer's list of part 2 programs.
- **Vendor commitments:** V11 (a mapping set for each new IG version before the old expires), V12 (mapping
  defects in what members see fixed as High), with D11's V1–V10.

### 3.2 Licences and procurement (product owner, counsel, procurement)

Needed before mappings are written from the guides, and before anything ships. Size: S to M elapsed.

- **X12:** a developer licence to build, a commercial licence to embed TR3-derived maps in a product and to run
  them for others (the managed service). The maps can't be published openly; customer documentation of them must
  stay within the licence.
- **Code systems:** CPT (AMA), NUBC codes (AHA as secretariat), X12's claim adjustment and remittance remark codes,
  NCPDP standards, 3M APR-DRGs, CDT (ADA) for dental. Decide (Q6) whether the product ships codes only, never
  descriptions, and requires customers' own licences.
- **NCQA:** only if the managed service runs HEDIS measures.
- **The Blue Button mark** (HHS's): how the product may name it.
- **Synthetic test data:** X12 and NCPDP samples that may be used and kept in the repository, under the licences
  (no real PHI, as for every format).

### 3.3 Standards tracking (product owner, a standards lead)

Ongoing. Size: S, then a standing duty.

- CMS's proposed rule (91 FR 19890): its final form, dates and required IG versions.
- ONC's 45 CFR 170.215 versions and expiry dates (CARIN Blue Button 2.0.0 adopted; 2.2.0 published; the
  Digital Insurance Card 1.1.0, 2.0.0 in ballot; PDex 2.1.0).
- HIPAA transaction standards: claims attachments (Part 162 Subpart T, 2028-05-26); FHIR for prior authorization
  if finalised.
- NCQA's digital measure timelines and data expectations.
- HL7 and CARIN Alliance work groups: whether to take part.

### 3.4 Product design (architect, with the team)

Size: M. The decisions to make, and what is known:

- **Intakes and transport:** how payers send claims data (batch files over SFTP or an object store is likeliest;
  existing intakes cover it). Whether the pipeline answers with X12 acknowledgements (999, TA1): only if it sits in
  front of the plan, which is a clearinghouse question (§3.8).
- **One pipeline per transaction type:** 837P, 837I, 837D, 835, 834, 278, and pharmacy, each its own content
  type, as the product already requires (DESIGN.md §5.3).
- **The claim-to-payment join** (§4): the hardest part.
- **Enrollment as state:** an 834 carries changes (additions, changes, terminations, effective dates), often as
  periodic full files with changes between. `Coverage` and the Digital Insurance Card are the member's current
  and past coverage, so 834s must be accumulated, in order, per member: a second stateful join, smaller than §4
  but of the same kind.
- **Identifiers across streams:** the member ID, subscriber and dependent, and the payer's claim control number,
  linking 834, 837, 835 and C-CDA's patients; how a `Patient` is the same person across them.
- **IG versions side by side:** a WSTL set per IG version, the version on each resource (`meta.profile`), several
  unexpired versions run at once.
- **Part 2 labels** from a provider list (Q4), carried to the gold row's confidentiality as C-CDA's are.
- **Provenance** to the X12 segment and element each FHIR element came from, for HEDIS audits and members'
  questions.

### 3.5 Mappings (mapping engineers, with a claims subject matter expert)

Size: L. As WSTL sets for the transform stage, each validated against its IG's profiles:
- 837P, 837I, 837D to the claim side of CARIN Blue Button's professional, institutional (inpatient, outpatient)
  and oral EOB profiles;
- 835 to the adjudication side (payments, adjustments with reason codes, member cost-sharing);
- 834 to `Coverage`, `Patient` and the Digital Insurance Card;
- pharmacy (NCPDP, or the PBM's extract) to the pharmacy EOB profile;
- 278 to prior authorization data in the Patient Access API (CMS, from 2027-01-01);
- claims-system extracts, if Q5 puts them first: per customer, the CSV and delimited intakes already exist.

A claims subject matter expert is needed throughout: claim frequency codes (originals, replacements, voids),
claim status in an 835 (processed, denied, reversed), coordination of benefits, capitated encounters, and how
each payer's data departs from the guides.

### 3.6 Engineering beyond mappings (the team)

- **The join** (§4). Size: L.
- **Validation** of output against CARIN Blue Button, Digital Insurance Card and PDex, as US Core is for C-CDA.
  Size: S to M.
- **Part 2 labelling** from a provider list. Size: S.
- **NCPDP reading,** if Q5 includes it: a new format at the convert stage. Size: M.
- **Scale:** claims are small and many; a payer's history since 2016 runs to hundreds of millions of claims. The
  pipeline has been sized for 15 million documents. Size and cost runs at that scale. Size: M, with AWS cost.

### 3.7 Testing (the team, QA)

Size: M, alongside 3.5 and 3.6.
- Each transaction end to end through a pipeline, as C-CDA and CSV have been (DEPLOYMENT.md, "Formats").
- The join's cases (§4): late payments, several payments, reversals, replacements, a payment with no claim.
- Conformance of output to each IG version; a FHIR server loaded from gold and read as an app would.
- The control harness scenarios run against the new pipelines (outages, failover, retention, erasure).
- Throughput, latency and cost at a payer's size, with the 1-business-day requirement as the bound.

### 3.8 Compliance and security (security lead, counsel)

Size: M.
- D14 reviewed and turned into customer guidance; D10's questions answered for claims.
- The data flows (D5), configuration (D6), audit events (D7) and retention (D9) documents extended to the new
  pipelines and the join.
- **If any offering sits in front of the plan:** Part 162 compliance as a clearinghouse acting for the plan
  (§ 162.923(c), § 162.930), isolation from the vendor's other business (§ 164.308(a)(4)(ii)(A)), § 164.500(b),
  and acknowledgements.
- Part 2 in claims: the provider list, labels, and what readers must do (D14 §2.5).
- State law limits on what the Provider Access and Payer-to-Payer APIs carry: the customer's, but the product's
  labels must let them act on it.

### 3.9 Documentation (technical writer, with the team)

Size: M.
- Customer guidance from D14 §7, once reviewed: obligations split between pipeline and customer.
- Per-transaction mapping documentation, within the X12 licence.
- Operations: the join's held items, how to see and clear them, how corrections flow (HIPAA-OPERATIONS.md).
- Release notes naming the IG versions each release maps to (UPGRADES.md's finding).

### 3.10 The managed service (managed services division, counsel)

Size: M, elapsed. The agreements must settle what D14 §5 lists: where the service sits; service levels in the
plan's business days; who runs the FHIR and authorisation servers, and if the vendor does, app registration and
refusal, documentation and CMS metrics; CMS and state flow-downs; Part 2; licences; HEDIS and NCQA's audit; MA
records kept 10 years; breach notice; the 2026 Privacy Rule's BAA deadlines; liability for what members see;
FedRAMP for federal buyers.

### 3.11 Going to market (product owner, sales, support)

- A pilot payer with synthetic or de-identified data and a data use agreement.
- Support: mapping faults reproduced with synthetic X12, never PHI (as now); fix times per V12.
- Which offering first: the product, the managed service, or both.

## 4. The claim-to-payment join

**Why it's hard.** An EOB needs what was billed (the 837) and what was decided and paid (the 835), but they
arrive separately, often days or weeks apart, in different files and so different pipelines. One 835 pays many
claims; one claim may be paid across several 835s, partly, or adjusted later; a claim may be replaced or voided
(its frequency code), and a payment reversed and reissued; some 835s pay claims the pipeline never saw (submitted
on paper, or before the history began); and coordination of benefits brings a second payer's adjudication.
CMS's clock (1 business day) runs from adjudication, so an EOB can't wait for every part.

**The starting idea (the user's, 2026-11-25):** keep the two as separate pipelines, each to its own gold, and
join after gold. Each pipeline stays as it is: one content type, its own failures, its own replays. A joining
step reads both golds and writes the EOBs.

What that design will need to decide:
- **The key:** the payer's claim control number (835 CLP07) and the provider's patient control number (837
  CLM01, echoed in 835 CLP01), with the payer's own identifiers where the plan adds them; what to do where they
  don't match.
- **Where the join lives:** a stage of its own after both golds (reading their topics, as the gold readers do),
  writing to a gold of its own for the EOBs; its own registry of pending halves.
- **Revisions, never replacement:** each arrival (a claim, a payment, an adjustment, a reversal) makes a new
  revision of the EOB, linked to the one it supersedes, as the product does everywhere (STAGES.md §15.8).
- **The held state:** a claim with no payment yet, or a payment with no claim, visible to operators and counted,
  with a policy for when an unpaired half is published alone (a claim pending, or a payment for an unknown
  claim) and when it stays held. Nothing dropped.
- **Order and replays:** halves arriving in any order, and either gold being replayed or reprocessed, give the
  same result.
- **Retention and erasure** across the join: a purge or erasure of one side must reach the EOBs made from it.
- **Part 2 and confidentiality:** an EOB's labels are the most restrictive of its halves'.
- **Scale and latency:** pending halves for weeks, at a payer's volume, within the 1-business-day bound once both
  are there.
- **The alternative,** to weigh: payers' claims systems already hold the adjudicated claim, so a claims-system
  extract (Q5) may carry both halves at once and need no join. Many payers may prefer it.

## 5. Order and dependencies

1. **Decisions and licences first** (3.1, 3.2): legal standing, scope, the X12 developer licence and test data.
   Nothing is mapped from the guides before the licence.
2. **Design** (3.4), with the join's design (§4) and the enrollment state, before mappings beyond the simplest.
3. **One end-to-end slice:** 837P and its 835 for one payer type, through two pipelines and the join, to CARIN
   Blue Button in a FHIR server, tested (3.7) at a small size.
4. **Widen:** the other 837s, 834 and the Digital Insurance Card, pharmacy, 278; then scale.
5. **Compliance, documentation, the managed service** (3.8–3.10) alongside, finished before a customer's data.

Standards tracking (3.3) runs throughout; CMS's final rule may change dates and versions.

## 6. Risks

- **The join and the enrollment state** are new kinds of stage for the product, stateful across pipelines.
- **Licences** gate the mapping work, and may limit what can be documented or shipped.
- **The rules are moving:** a final 91 FR 19890 may set versions and dates the plan must meet.
- **Payers' data departs from the guides,** in their own ways; each customer may need its own adjustments.
- **Scale:** a history since 2016 is an order of magnitude beyond what's been sized.
- **The clearinghouse question:** an offering in front of the plan brings duties that one behind it doesn't.
- **Members see the output:** errors in amounts or cost-sharing are visible and may be costly.

## Sources

[security/PAYER-DATA-EXCHANGE.md](security/PAYER-DATA-EXCHANGE.md) (D14) and its sources;
[security/CLEARINGHOUSE-PART2.md](security/CLEARINGHOUSE-PART2.md) (D10);
[security/VENDOR-COMMITMENTS.md](security/VENDOR-COMMITMENTS.md) (D11);
[security/KNOWN-GAPS.md](security/KNOWN-GAPS.md); DESIGN.md §5.3, §6.4; STAGES.md §15.8; DEPLOYMENT.md,
"Formats"; PERFORMANCE.md.
