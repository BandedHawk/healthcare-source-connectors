# HL7 v2 to FHIR: the work, for a future project

> **Future work, not part of this project.** The pipeline reads no HL7 v2 today: there is no v2 converter, no
> MLLP intake and no mapping set for it. This document hands the work to the team that will take it on: every
> stream of effort, what each depends on, and what is already known. It follows the shape of
> [PAYER-X12-FUTURE-WORK.md](PAYER-X12-FUTURE-WORK.md), and much of what that says about pipelines, revisions and
> testing holds here too.

## 1. The goal

Hospitals, labs, public health agencies and HIEs exchange most of their clinical events as HL7 v2 messages: admits,
discharges and transfers (ADT), results (ORU), orders (ORM, OML), immunizations (VXU), documents (MDM) and
schedules (SIU). Buyers who already send C-CDA through the pipeline will want their v2 feeds turned into the same
US Core 6.1 FHIR, landed, stored and protected the same way, so that one gold holds both.

## 2. What exists, and what this project leaves

| Exists | Doesn't |
|---|---|
| Landing intakes: files, object stores, HTTP push (OAuth2 or mTLS, `202` only after the item is committed), and queues (Kafka, SQS, Pub/Sub, Service Bus, Event Hubs and others) | An MLLP intake, and v2 acknowledgements (ACKs) |
| Splitters and converters found by format name through `ServiceLoader` (`RecordSplitter`, `RecordConverter`), with converters for C-CDA, CSV, X12, EDIFACT and Parquet | A v2 splitter or converter: nothing parses ER7 (the pipe-delimited form), batches (`FHS`/`BHS`) or v2 escapes |
| The transform stage, running any WSTL set chosen at run time; the US Core 6.1 set for C-CDA, with its generic helpers (dates, code systems and OIDs, displays, data checks) | A WSTL set that reads v2 |
| Revisions linked, never replaced (STAGES.md §15.8); gold append-only | A way to apply v2's updates, merges and cancellations in order (§4) |
| A guide to the terminology server a buyer runs downstream (the US Core set's README) | v2 tables and local codes covered by that guide |
| Performance measured for C-CDA documents (about 60 a second, p99 under 200 s) | Sizing for many small messages a second over long-lived connections |

## 3. The streams of work

Each stream says who leads it, what it needs first, and its rough size (S: weeks, M: one to three months, L:
more, for one team).

### 3.1 Decisions (product owner, architect)

Needed before design is fixed. Size: S.

- **Scope:** which message types first, decided by buyers' feeds. ADT and ORU are the likeliest; VXU and ORU
  matter to public health.
- **Which v2 versions:** most US feeds are 2.3 to 2.5.1, and public health's guides (immunization, electronic lab
  reporting) are 2.5.1. Older and newer versions differ in fields and tables.
- **How messages arrive:** MLLP straight from an interface engine, or the engine forwarding over HTTP or a queue,
  which the existing intakes already take (§3.4).
- **What gold holds for an event feed:** each message mapped to its own Bundle and left for readers to apply, or
  a stateful step that keeps each patient's current record (§4).

### 3.2 Licences and test data (product owner, counsel)

Size: S.

- **HL7 v2 and the V2-to-FHIR guide:** HL7 makes its standards available free under its IP policy; check the
  terms for a product built on them, and for the guide's ConceptMaps.
- **The parsing library:** HAPI HL7v2, which Camel's HL7 data format uses. Check its licence, as for every
  bundled component (`THIRD-PARTY.txt`).
- **Code systems:** the same as the US Core set's (LOINC, SNOMED CT, RxNorm, CVX and the rest), plus the v2 tables
  in `hl7.terminology.r4`.
- **Synthetic test data:** v2 messages that may be kept in the repository, never real PHI, as for every format.
  Public samples exist in the V2-to-FHIR guide and in public health guides; check each one's terms.

### 3.3 Product design (architect, with the team)

Size: M.

- **One pipeline per message family** (ADT, ORU, and so on), each its own content type, as the product already
  requires (DESIGN.md §5.3).
- **Events, not documents** (§4): the hardest part.
- **Resource ids:** the US Core set hashes a document's content into its ids. A v2 set must give the same patient,
  encounter and order the same id in every message about them: from identifiers (`PID-3` with its assigning
  authority, `PV1-19`, placer and filler order numbers), not from the message.
- **Identifiers across feeds:** how a `Patient` from ADT is the same person as one from ORU, from another sender,
  and from C-CDA.
- **Confidentiality:** v2 rarely carries DS4P. Decide where the gold row's `confidentiality` comes from: the
  `ARV` segment (access restrictions, from v2.6), `PV1` VIP indicators, the sender, or a customer's list.
- **Provenance:** each FHIR element traceable to its segment, field, component and repeat, for operators and for
  data checks' positions (the line and column of today's issues become segment and field).
- **Acknowledgements:** what the pipeline answers, and when (§3.4).

### 3.4 Intake and transport (the team)

Size: M.

- **A first slice with no new intake.** Interface engines (Rhapsody, NextGen Connect, Cloverleaf and the like) can
  usually forward to HTTP or a queue. With a v2 converter alone, the push and queue intakes take v2 now, and push's
  `202` after the commit is the durability a sender needs.
- **An MLLP intake**, for senders that speak only MLLP: Camel's `camel-mllp`, chosen at run time like the other
  intakes, with:
  - TLS, and sender authentication by client certificate, as push's mTLS does; MLLP itself has none;
  - an accept ACK (`CA`, or `AA` in original mode) sent only after the message has landed durably, and a negative
    ACK (`CR`/`AR`) when it can't land, so that the sender resends. An application ACK can't come from landing,
    since mapping happens later: say so to senders, or send it from the producer's result later;
  - one landing pod active at a time, as every intake is (DESIGN.md): long-lived connections must reconnect to the
    standby on failover, and senders must resend what wasn't ACKed;
  - the message's control id (`MSH-10`) for spotting resends, as push uses `Idempotency-Key`.
- **Network:** an MLLP port through the ingress of NETWORK.md (D1), allow-listed per sender, as the push intake is.

### 3.5 The converter (the team)

Size: M.

- A `hl7v2` splitter: a record a message, from MLLP frames, batch files (`FHS`/`BHS`/`MSH`), or one message a file.
  Segments end in a carriage return, not a line feed, so the `delimited` format won't do.
- A converter to JSON (`hl7v2/1`), deterministic and thread-safe as `RecordConverter` requires:
  - every segment, field, repeat, component and subcomponent kept, Z-segments and unknown fields included: nothing
    dropped, as for every format;
  - escapes (`\F\`, `\S\`, `\T\`, `\R\`, `\E\`, hex and character set escapes) decoded, and `MSH-18`'s
    character set honoured;
  - positions (segment number, field, repeat, component) on each element, for data checks;
  - a `ConversionException` with a code and position only, never content, for a message that doesn't parse.
- Built on HAPI's parser (through Camel's HL7 data format, per the project's preference for Camel), with its
  validation off: the pipeline keeps what was sent and checks it in the mapping.

### 3.6 Mappings (mapping engineers, with a v2 subject matter expert)

Size: L. WSTL sets for the transform stage, in a module of the mapping repository beside `us-core-6.1`, each
validated against US Core 6.1 as the C-CDA set is:

- **Follow HL7's guide:** [HL7 Version 2 to FHIR](https://hl7.org/fhir/uv/v2mappings/) (`hl7.fhir.uv.v2mappings`,
  STU 1, 1.0.0, for FHIR R4). It maps messages, segments, data types and tables as ConceptMaps generated from CSV
  files, cumulatively from v2.9, for ADT (A01, A02, A05, A06, A09, A11, A17), ORM, OML, VXU, ORU, MDM and SIU.
  Check for a newer release before starting.
- **It targets base FHIR, not US Core:** each set must add what US Core 6.1 requires (profiles, must-support
  elements, bindings, category codes), as the C-CDA set's `primary/` does.
- **Reuse the C-CDA set's generic helpers** (dates, code systems, displays, data checks); the clinical mapping is
  new.
- **v2 vocabulary:** v2 tables become their `terminology.hl7.org` v2 code systems; local tables and `CWE` codes
  with alternate codings are kept as sent, with the same no-replacement rule. Extend the terminology server guide
  for them.
- **Per-sender variation:** v2 varies far more between sites than C-CDA does: Z-segments, local tables, fields
  used for other things. Plan a base set and a profile per sender, and a v2 subject matter expert throughout.

### 3.7 Testing (the team, QA)

Size: M, alongside 3.5 and 3.6.

- Each message family end to end through a pipeline, as C-CDA and CSV have been (DEPLOYMENT.md, "Formats").
- The event cases (§4): updates out of order, merges, cancellations, a result before its admit, resends.
- MLLP: framing faults, lost connections, failover with unacknowledged messages, slow senders.
- Conformance of output to US Core 6.1, with validator baselines per message type, as the C-CDA set has.
- Throughput, latency and cost with many small messages: landing has been measured at about 30 to 90 items a
  second on AWS (PERFORMANCE.md), and each v2 message is an item.
- The control harness scenarios against the new pipelines.

### 3.8 Compliance, security and documentation (security lead, technical writer)

Size: M.

- The data flows (D5), configuration (D6), audit events (D7), network (D1) and retention (D9) documents extended to
  v2 pipelines and the MLLP intake.
- Customer guidance: what the sender must configure (TLS, certificates, ACK mode, resends), what an ACK from the
  pipeline means, and how events reach a FHIR server (§4).
- The set's README: its v2 handling, as the US Core set's README now sets out its HL7 v3 handling.

## 4. Events, not documents

**Why it's hard.** A C-CDA document stands alone. A v2 message is an event about records that earlier messages
made:
- an update (A08) changes a patient or visit;
- a merge (A40, A34 and similar) joins two patients;
- a cancellation (A11, A12, A13) undoes an admit, transfer or discharge;
- a corrected result (`OBX-11` status `C`) replaces an earlier one.

Messages can arrive out of order, or twice, and the pipeline maps them in parallel. Gold is append-only and the
product never replaces data, so an update arrives as a new record, not a change to an old one.

What the design will need to decide:
- **Who applies events.** Either each Bundle carries enough for a reader to apply it (a transaction Bundle with
  conditional updates keyed by identifiers, applied in event order by whatever loads the FHIR server), or a
  stateful step after gold keeps each patient's current record. The latter is a new kind of stage, like X12's
  claim-to-payment join.
- **Order:** by the message's time (`MSH-7`), the event's (`EVN-2`), the sender's sequence number if it uses one,
  or landing order; what to do with an update older than the record it updates.
- **Merges and cancellations** as revisions linked to what they change, never deletions.
- **Resends:** the same control id (`MSH-10`) from the same sender is the same message.
- **Retention and erasure** across events: a purge of one patient must reach every event about them.

## 5. Order and dependencies

1. **Decisions** (3.1) and test data (3.2).
2. **Design** (3.3), with the event design (§4), before mappings beyond the simplest.
3. **One end-to-end slice:** ORU results (which stand alone most nearly) from one sender, over HTTP push or a
   queue, through the converter and a set, to US Core in a FHIR server, tested at a small size.
4. **ADT** with the event design; then the MLLP intake; then other message families and senders; then scale.
5. **Compliance and documentation** (3.8) alongside, finished before a customer's data.

## 6. Risks

- **Events:** applying updates, merges and cancellations in order is a new kind of work for the product.
- **Site variation:** each sender's v2 is its own dialect; a set that works for one may not for the next.
- **The guide is STU 1 and targets base FHIR:** US Core alignment is the project's own work.
- **Volume:** many small messages may cost more per record than documents do, and may outrun landing's item
  rate.
- **MLLP failover:** senders behave differently when a connection drops; some resend, some queue, some stop.

## Sources

[HL7 Version 2 to FHIR](https://hl7.org/fhir/uv/v2mappings/), its
[message maps](https://hl7.org/fhir/uv/v2mappings/message_maps.html) and
[mappings](https://hl7.org/fhir/uv/v2mappings/mappings.html) (read 2026-11-27);
[PAYER-X12-FUTURE-WORK.md](PAYER-X12-FUTURE-WORK.md); [security/KNOWN-GAPS.md](security/KNOWN-GAPS.md);
DESIGN.md §5.3, §6.4; STAGES.md §15.8; DEPLOYMENT.md, "Formats"; PERFORMANCE.md; the US Core 6.1 set's README in
the mapping repository.
