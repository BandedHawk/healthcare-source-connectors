# ADR-00X: Event-driven KEDA pipeline vs. Apache Spark for pre-Smile CDR C-CDA ingestion

- **Status:** Proposed
- **Date:** 2026-11-25
- **Context:** Clinical data ingestion and FHIR repository architecture

## 1. Context and problem statement

We are building a multi-stage pipeline to ingest raw clinical C-CDA XML documents, transform them into FHIR R4
transaction bundles, and persist them into **Smile CDR** as our operational clinical source of truth.

A counter-proposal suggests standardizing this ingestion layer on **Apache Spark** to align with our downstream
data lake tooling. We must decide between our proposed **event-driven, KEDA-autoscaled Kubernetes microservice
pipeline** and a **Spark-based ingestion pipeline**.

## 2. Decision

We will proceed with the **multi-stage, event-driven KEDA architecture** for C-CDA-to-FHIR conversion and Smile
CDR ingestion. Apache Spark will be strictly reserved for **post-Smile CDR analytical workloads** (consuming Bulk
FHIR `$export` or FHIR Subscription change streams into the data lake).

## 3. Architectural drivers and rationale

### 3.1 Transactional integrity and Smile CDR protection (OLTP vs. OLAP)

- **Smile CDR constraints:** Smile CDR is a stateful, ACID-compliant transactional repository backed by a
  relational database. Writing FHIR transaction bundles triggers real-time Enterprise Master Patient Index
  (EMPI/MDM) matching, referential resolution, and row-level locking.
- **Why KEDA fits:** Queue partitioning (by `patient_id` or `source_id`) preserves clinical document ordering and
  prevents MDM race conditions (e.g., duplicate patient creation from concurrent writes). KEDA `maxReplicaCount`
  caps protect Smile CDR's database connection pool from saturation.
- **Why Spark fails here:** Spark's unordered, partition-parallel execution fires unthrottled HTTP bursts at Smile
  CDR, causing database lock contention, HTTP 429/500 retry storms, and out-of-order clinical state overwrites.

### 3.2 Granular error isolation and clinical auditability

- **Compliance requirement:** Healthcare privacy regulations (HIPAA / PHIPA) require deterministic,
  document-level lineage (`source_ccda_id` → `fhir_bundle_id` → `smile_transaction_id` + `Provenance`).
- **Why KEDA fits:** Message brokers provide native record-level acknowledgments and Dead-Letter Queues (DLQs). If
  one malformed C-CDA fails transformation or FHIR validation, it is isolated and routed to a DLQ with its
  correlation ID without affecting neighboring documents.
- **Why Spark fails here:** Spark tracks lineage at the dataset/partition level. If a single malformed XML
  crashes an executor, Spark retries the *entire partition*, risking duplicate clinical submissions, corrupted
  resource `_history` versions, and muddled audit logs unless custom state-tracking infrastructure is built inside
  Spark.

### 3.3 PHI leakage blast radius and crash containment

- **Security requirement:** Pre-Smile CDR pipelines process raw, unmasked Protected Health Information (PHI).
- **Why KEDA fits:** Each pod processes discrete events in isolated, ephemeral containers. An Out-Of-Memory (OOM)
  crash terminates only that single container and its ephemeral memory, limiting the PHI blast radius to a single
  document.
- **Why Spark fails here:** Spark co-locates dozens of patient records inside shared, long-lived JVM executor
  heaps and writes unencrypted shuffle/spill blocks to local disk. A single bloated C-CDA causing an executor OOM
  crash risks exposing multi-patient PHI via JVM heap dumps, orphaned disk spill files, or verbose parser
  exception traces.

### 3.4 Workload and scaling profile

- **Why KEDA fits:** Clinical feeds arrive continuously in irregular bursts and trickles. KEDA reacts to queue
  depth in seconds and scales to zero during idle periods without JVM driver/executor overhead.
- **Why Spark fails here:** Spark incurs heavy cluster startup and scheduling latency for small/bursty feeds and
  wastes baseline compute when idle.

## 4. Consequences

### 4.1 Positive

- **Clear separation of concerns:** Operational transactional ingestion (KEDA + Smile CDR) is cleanly decoupled
  from analytical lakehouse processing (Spark + Delta/Iceberg).
- **Predictable Smile CDR performance:** Controlled concurrency and ordered delivery prevent MDM race conditions
  and database deadlocks.
- **Reduced compliance risk:** Minimal PHI blast radius on worker crashes and deterministic per-document audit
  trails.

### 4.2 Trade-offs and mitigations

- **Two compute paradigms:** Engineering teams maintain Kubernetes/KEDA workloads for ingestion alongside Spark
  for downstream analytics. *Mitigation:* Standardize container templates, observability (OpenTelemetry
  correlation IDs), and CI/CD pipelines across both layers.
- **Historical backfills:** Massive one-time historical migrations require careful queue throttling so Smile
  CDR's database is not overwhelmed. *Mitigation:* Use dedicated low-priority backfill queues with tuned KEDA
  concurrency caps.

## 5. Summary

| Concern | Data lake / post-Smile CDR (Spark's domain) | Transactional ingestion / pre-Smile CDR (KEDA's domain) |
|---|---|---|
| Workload type | High-throughput batch/micro-batch analytics (OLAP) | Stateful, transactional API writes (OLTP) |
| Target system | S3 / ADLS / GCS (immutable object storage) | Smile CDR + RDBMS (row locks, indexes, MDM matching) |
| Failure recovery | Partition-level task replay | Single-message retry + Dead-Letter Queue (DLQ) |
| Ordering guarantee | Unordered / shuffle-based | Partition-key ordered (prevents patient race conditions) |
| PHI blast radius | Multi-patient shared JVM executor heap | Single-document ephemeral pod isolation |
