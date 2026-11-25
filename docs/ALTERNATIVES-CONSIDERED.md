# Alternatives considered

A record of the platforms and frameworks considered for the pipeline and not chosen, with the reasons, so that
the question needn't be reopened without new facts. Each entry says what was decided, why, and what would make it
worth looking at again. The design itself is in DESIGN.md and STAGES.md.

## 1. Apache Spark (recorded 2026-11-24)

**Decided:** the pipeline's stages are Kubernetes Deployments, each its own Java application (the landing node
on Camel, the split, convert and transform stages on the shared stage runtime), joined by Kafka, writing Iceberg
tables, scaled by KEDA on their inputs' lag, and run through a control API. Spark isn't used, in the stages or in
table maintenance (DESIGN.md §2 and §8.4). This was the design from the start; this entry records why, at the
user's request.

**In short:** Spark is a good engine for computing over data already held: large, uniform datasets and
set-based transforms. This pipeline exists mostly to take custody of documents one at a time, under PHI rules,
with guarantees, operations and an audit trail for each. Spark leaves that work to the application, and gets in
its way in places.

### 1.1 Why not

1. **The unit of work is a document, not a dataset.** Each item has its own registry row, claims and leases, an
   end message, reconcilers that send again what a stage never received, resubmission links and dead-letter
   entries; operators act on one item through the control API (restore, purge, reprocess, release). Spark
   retries a task or a partition: one malformed C-CDA that fails a task runs its whole partition again, and with
   it every side effect of the records already done. The item registry and claims would have to be built beside
   Spark, not taken from it.
2. **Exactly once ends at Spark's sink.** Structured Streaming is exactly once into an idempotent sink, such as
   Iceberg, through its checkpoint. The stages do more than write a table: they seal messages to the next stage,
   write the audit journal, mark items in the next stage's registry and send item ends. A micro-batch replayed
   repeats all of them. The rule that no data is lost or supplanted needs each one done once, or detected, for
   each item.
3. **PHI in what Spark records about itself.** The stages never log exception messages, object keys or document
   values (HIPAA.md). By default, Spark's executor exceptions often carry row contents; its UI, event logs and
   history server keep query plans, which can hold literal values; shuffle and spill files land on executors'
   disks. Each can be locked down (I/O encryption, redaction, the UI and event logs off or encrypted), but it is
   a large surface to audit and keep audited. The stages control every line they log, and the compliance
   profile checks their settings at startup.
4. **Encryption is per record, not per cluster.** Records are sealed with JOSE to the next stage's inbox key,
   with data keys wrapped by KEKs for each pipeline and layer, rotated, re-keyed and destroyed per stage
   (DESIGN.md §7). Spark's own encryption covers its transport and disks, not envelope encryption per record.
   That would live in user-defined functions, which Spark's optimiser can't see into.
5. **Latency is per document.** The target is landed → gold p99 under 200 s while sustaining 60 documents a
   second (PERFORMANCE.md §1). Micro-batches could meet it, but a slow document holds its batch's commit, which
   is why the split stage's batches are no larger than what a pod splits at once; and Spark adds its own
   scheduling and commit cadence. Small consumers that KEDA scales on lag, committing through a shared queue,
   fit a steady feed with bursts.
6. **Buyers operate it** (HIPAA.md §1). Governments, insurers and agents run the product. Spark adds a cluster
   manager or operator, a driver per job that is a single point of failure, executor sizing, checkpoint storage
   and a history server, each to secure, patch and cover in their HIPAA documentation. Plain Deployments and a
   control API are less to take on. The landing node is one active pod with a standby that takes over in seconds
   (2 to 3 s in PERFORMANCE-MILESTONES.md's first entry), which isn't a Spark pattern.
7. **Landing is about connecting to sources.** Intakes are chosen at run time from many transports (HTTP push
   with mTLS, object stores, queues and others), which Camel provides. Spark's sources are files, Kafka and
   JDBC; the rest would be custom sources.
8. **Cost at this scale.** The pipeline runs at about $12 a million documents on small pods that scale down to
   one at rest (PERFORMANCE-MILESTONES.md). A Spark streaming job keeps a driver and executors up, and executors
   sized for shuffles are heavier than the stages need: nothing here shuffles, as each document is mapped to FHIR
   on its own.

### 1.2 Where Spark would fit

- **Bulk work over data already landed:** reprocessing years of silver records through a new WSTL mapping, for
  example, where the input is in Iceberg, encrypted at rest, and no per-item life cycle is needed.
- **Analytics on gold:** queries over the FHIR tables, which are columnar and set-based. A buyer may run Spark
  or another engine there; the tables are plain Iceberg.
- **Perhaps a one-off initial load,** if its guarantees for each item were relaxed for that run; the audit and
  sealing rules would still hold.

### 1.3 What would reopen it

- A requirement for set-based processing across documents (joins, aggregation, deduplication across sources)
  inside the pipeline, rather than after gold.
- A bulk load or reprocessing whose size the stages can't meet at a reasonable cost (tuning-cost balance:
  PERFORMANCE.md), measured, not assumed.
- Buyers who already operate Spark under their own HIPAA controls and ask for it.
