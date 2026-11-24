# Deploying the pipeline

How the pipeline is meant to be deployed, why, and what else was considered. Written 2026-11-05. The design itself is in [DESIGN.md](DESIGN.md); section numbers below (§) refer to it.

A pipeline is one intake, its landing node and its splitter, and carries one type of content (§5.3). The **landing node** (`landing-connector`) takes items in through the intake (a watched directory, HTTP push, a remote source or a queue) and stores each exactly as received, encrypted, in `landed_items` (§5.3). It then sends a signed, encrypted `item.landed` message to its splitter. The **splitter** (`file-connector`) takes that message into its inbox, reads the item from landing, and splits, converts, stores and announces its records to downstream consumers (§5.3.12, §6, §10).

## Recommendation

**Run the landing node and the splitter as separate images and deployments, with the queue between them on an external broker.** Use the broker the customer already runs for the splitter's output, with a queue or topic of its own.

- Each stage has its own image (`sdh/landing-connector`, `sdh/split-stage`, `sdh/convert-stage`), its own deployment, its own active/standby lease (§9) and its own keys (§7.4).
- `item.landed` goes over its own queue or topic on the broker, separate from the output (`record.stored` and the rest).
- For trials and small sites, ship a single bundle (a Compose file or Helm chart) that runs the two images and a small broker. That makes installation simple without supporting a second shape of the product.

The demonstration (`-Pdemo`, §15.3) has the same layout: two processes and an external broker, with separate queues for the two stages. It runs the apps from their JARs rather than their images, and without standbys. The image tests and the Kubernetes run (§15) cover the images and failover.

## Why

**1. Intake stays up when splitting doesn't.** The landing node's job is to accept data and make it durable. Splitting is where the risky work happens: malformed files, unusual encodings, memory pressure from large batches. As separate processes, a splitter crash, out-of-memory error or bad release doesn't stop intake. Producers still get their `202`s, and directories are still picked up. Items wait safely in `landed_items` until the splitter catches up. Losing intake is the worse failure for health data, because a producer may not send again.

**2. They face different threats.** The landing node faces the network: HTTP push with mTLS, and pulls from remote sources. The splitter only needs storage and the brokers. Keeping the parsers and converters out of the image that faces the network makes that image smaller, with fewer libraries to patch. Each also holds only its own keys: the splitter has its own key pair and only trusts the landing node's signing key, so compromising one doesn't hand over the other's keys.

**3. Their resource needs differ.** Landing is mostly I/O and fairly steady. Splitting uses CPU and memory, in bursts. Separate deployments can be sized, limited and scaled separately.

**4. They can be released separately.** A converter fix ships without touching intake, and an intake fix doesn't redeploy the converters. The signed `item.landed` message is the contract between them, and it is versioned (see below).

**5. The broker between them adds little.** The splitter must already publish to the customer's broker, so a broker is required anyway. The link between the stages is one more queue or topic on it. It carries one small signed message per item; the data itself is in Iceberg.

**What it costs:** two deployments to configure, monitor and keep compatible; one more queue to provision, secure and include in disaster-recovery planning; and more setup for a small site. The bundle covers the last.

## What else was considered

| Shape | What it gains | What it loses | Verdict |
|---|---|---|---|
| **A. Separate images, external broker** | All of the above | More moving parts | **Recommended** |
| **B. One image; the role (landing or splitter) chosen at start-up** | One image to build, sign, scan and track in a bill of materials | Point 2: the network-facing landing node carries every parser and converter | Not worth it for health data |
| **C. One process, both stages, an in-memory queue between them** | One container; no broker between the stages | Points 1, 3 and 4 entirely: a splitter failure stops intake | Trials and demos only (the demonstration was like this until revision (bk)) |
| **D. Separate images, no broker between them:** the splitter polls `landed_items`, or a Postgres queue table | One fewer queue | Delay, and more Iceberg metadata reads; a home-made queue is a second way of doing what brokers do, against using Camel for messaging | Not worth it: the output needs a broker anyway |
| **E. Both containers in one Kubernetes pod** | Process isolation; one unit to deploy | Scheduled, scaled and restarted together, so most of points 1 and 3; still needs a broker | Of little benefit compared with A |
| **F. A broker built into the landing node** (embedded Artemis) | No separate broker | The queue's availability is tied to the landing node's, and failing over to the standby means failing over the broker and its stored messages | Not recommended |

## Keeping items from being stranded

With a queue between the stages, a lost `item.landed` message would leave an item landed but never split. Two mechanisms between them make sure every landed item is split.

- **A crash between landing and announcing** (the landing node). At startup, the landing node sends the last landed item's message again (§5.3.11). Items are landed and announced one at a time, in order, so only the last can be unannounced. Tested by `IntakeServiceTest.announcesTheLastItemAgainAfterACrashBeforeItsMessage`, and for pushes and message batches by `PushServiceTest` and `BatchServiceTest`.
- **A message the broker lost after accepting it** (the splitter). This covers a purged queue, a lost broker node, a retention limit passed while the splitter was down, or a dead-letter queue. The split stage reconciles (`SplitReconciler`, [STAGES.md](STAGES.md) §8). At startup, and then every `sdh.landing.reconcile.interval` (15 minutes), it splits every item of its intake that was fully landed more than `sdh.landing.reconcile.grace` (10 minutes) ago and was never started. It also takes on every item left part way, whose checkpoint hasn't been written for longer than the grace: its pod stopped while splitting it, and its message was lost too (built 2026-11-07). Each one is audited as `split.recovered` (reason `MESSAGE_MISSING` or `CHECKPOINT_STALLED`), which flags the broker problem. If the message turns up later, it only announces the item again. Each listed item is looked up again just before it is split, and left if it has been started meanwhile: under a backlog the list can hold thousands of items whose messages are still on their way (2026-11-22, DESIGN.md revision (eu)). Tested by `FileConnectorApplicationTest.recoversLandedItemsWhoseMessagesWereLost` (not before the grace, not twice, not another intake's item, and the late message only announces), `leavesAListedItemSplitMeanwhile`, `resumesAnItemLeftPartWayWhoseMessageWasLost` and `leavesAStalledItemFinishedMeanwhile` (an item left part way is looked up again too, and left if it was finished or taken on meanwhile). What the split stage sent on and the broker then lost is found by the convert stage's reconciler (below), or recovered by an operator with a reprocess at the convert stage (`sdh-admin reprocess --item-id`, STAGES.md §15.8.9), which reads the records from `split_records`. An item can also be split again as a whole, by a reprocess at the split stage.
- **A `record.split` the broker lost** (the convert stage). The convert stage's reconciler, on the pod holding its duties lease, rebuilds every record split more than `sdh.reconcile.grace` (10 minutes) ago that it hasn't converted, at startup and then every `sdh.reconcile.interval` (15 minutes), and audits each item as `stage.records_recovered`. A record a pod claimed and never stored, its claim expired, is rebuilt the same way, and an item whose records are all stored with its total, but which wasn't completed, is completed and audited `stage.item_completion_recovered` (design revision (by)). It remembers how far it has checked (`stage_reconcile`, shown by `sdh-admin status`), so after an outage it checks every day since, not only the last `sdh.reconcile.lookback` (2 days); a catch-up is audited `stage.reconcile_catching_up` and `stage.reconcile_caught_up` (design revision (bs)). Tested by `ConvertStageTest.catchesUpOnAnOutageLongerThanTheLookback`.

- **A record a stopped pod left claimed** (the convert and transform stages, revision (du)). The same pod sweeps, every `sdh.reconcile.claim-sweep` (1 minute), for claims that expired at least `sdh.reconcile.claim-age` (1 minute) ago with nothing stored, and processes their records again from their stored rows, without the reconciler's waits; each item is audited `stage.claims_recovered`. A batch that would be acknowledged with one of its records still claimed by its own pod is tried again instead, audited `stage.claims_left` (an error: report it). Tested by `RecordStageTest.sweepsRecordsWhoseHolderStopped`.

**Starting again after a long outage takes longer** (STAGES.md §13.4). The convert reconciler can't tell a record still queued from a lost one, so it waits for the queue first (design revision (bt)). At startup it waits `sdh.reconcile.backlog-quiet` (15 minutes) for the pods to begin receiving (not when it takes the duties over from a pod that ran lately: STAGES.md §9), then for as long as records older than the grace still come off the queue on any pod (`stage.reconcile_deferred`). Only then does it check the days it missed, so lost records are converted after the backlog. If the backlog takes longer than `sdh.reconcile.max-deferral` (6 hours) to drain, it goes ahead anyway (`stage.reconcile_deferral_capped`): it then converts records still queued, one at a time on the duties pod, alongside the other pods. Each is converted once, but the backlog drains more slowly, and a failed record it reaches first is audited `stage.placement_unknown` (its result can't name the producer's message). Raise `max-deferral` for a restart whose backlog will take longer. On a normal day, `reconcile_deferral_capped` means the queue is always more than the grace behind: add pods. Before a planned outage, drain the queues if you can, and check that each queue's retention (Kafka and Pub/Sub 7 days by default, SQS 4) is longer than the outage. Afterwards, check with the audit query of STAGES.md §13.2 that every item completed at each stage. Tested by `ConvertStageTest.waitsForABacklogToDrainThenGoesAheadAnyway` and `waitsAtStartupForTheQueueToStartDelivering`.

**Watch for:** `inbox.recovered` events. Any at all mean messages are going missing between the stages, and the broker should be looked at.

**One consequence:** a new splitter pointed at existing landing data splits everything already landed for its intake. That is deliberate: nothing landed goes unsplit. To start a new splitter without that, set `sdh.landing.reconcile.enabled: false` for its first run.

## Upgrading: the `item.landed` contract

Every message carries its contract version, `"v": "<major>.<minor>"` (§10.2).
- **Minor versions** only add optional fields. A reader ignores fields it doesn't know, and fields added since an older writer read as unset. So a landing node and a splitter that differ only in minor version work together, in either order.
- **A new major version** is a breaking change. A stage that can't read a later one doesn't drop the message, which would strand its item. Instead it audits `split.unreadable_version` (`stage.unreadable_version` for the convert stage) and leaves the message unacknowledged, so the broker delivers it again, with a growing pause, until the stage is upgraded. The messages behind it wait. The stage keeps running, and the split stage still picks up the items through reconciling once their grace has passed. Nothing is lost.
- **An earlier major version** (a message sent by the previous build, still queued after an upgrade) will never be read, so it isn't held: it is rejected and acknowledged (`split.rejected` or `stage.message_rejected`, reason `EARLIER_VERSION`), and its item is recovered by reconciling, or by a reprocess at the split stage (decided 2026-11-07, design revision (br); replay became reprocess, STAGES.md §15.8.9). Before, it blocked its partition for good: the sandbox found this when the previous build's landing node re-announced an item before it was upgraded.

**Upgrade order:**
1. Upgrade the splitter first, then the landing node, whenever the contract's major version changes. The same rule applies to downstream consumers of the splitter's output: upgrade them before the splitter writes a new major version.
2. Minor versions can be upgraded in either order.

**Tested by:**
- `ContractVersionTest`: a later minor version, with unknown fields, is read; an earlier minor version, without `replaces`, is read with it unset; another major version, or none, is refused with `UNSUPPORTED_VERSION`, and that is distinct from a bad payload.
- `FileConnectorApplicationTest.takesOtherMinorsHoldsALaterMajorAndRejectsAnEarlier`: the split stage takes and splits items sent with a later and an earlier minor version. A later major version is neither acknowledged nor fatal. On a queue it is tried again in place, and holds back the message behind it. An earlier major version is rejected and acknowledged. Reconciling still gets both items split. `StageWorkerTest` checks the same for the convert stage's worker.

## Changing a stage's version

Decided 2026-11-10 ([STAGES.md](STAGES.md) §14.4a). Each record names the version of the stage that made it, and an item is made under one version: a split or convert pod starts work only once the *drain check* lets its release in (each stage's README, "Changing the version"). A pod of a new release waits, not ready, while any item is part way under a version it wouldn't stamp; the old pods go on and finish those items. So:
1. **Deploy with a rolling update that adds a pod first** (`maxSurge: 1`, `maxUnavailable: 0`, as the READMEs' Deployments). Under `Recreate` no old pod would be left to finish the items, and a new pod held back would wait for good.
2. **Pause what feeds the stage, and wait until `drain-check` lists nothing part way.** Pods already running aren't checked, so with input still flowing the old pods may begin new items under the old version during the rollout, and those items would be made under both (recorded, §14.3).
3. **Deploy, and check that the new pods are ready** (`stage.version_changed`, with how). A pod held back audits `stage.version_blocked`; `drain-check` in it says what holds it.
4. **Resume.**

Work waiting on an operator (open failures, held split items, pending corrections, queued reprocessing) never holds a change: it is made, when it runs, under the version running then. A rollback to the release current before is always let in. If a change can't wait, `version --accept` in the held pod lets it in, with a reason, audited `stage.version_forced`; the split stage then holds an item begun under another version for an operator (`release`).

**Tested by:** `DrainGateTest` (each way a release is let in, a held one waiting until accepted, the ready port), `StageRegistryTest.anUnfinishedItemIsPartWayUnderItsOutputsVersions`, `ItemCheckpointsTest.countsTheItemsPartWayAndHeldByVersion` and `DrainCommandsTest`; tried in the sandbox ([SANDBOX.md](SANDBOX.md)).

## Scaling

Decided 2026-11-07 ([STAGES.md](STAGES.md) §10).
- **The landing node: never more than one in service.** It lands serially, and landing in parallel could land one file twice, by the different ways a file can be retrieved. Only the node holding its lease lands; a standby waits and takes over within the lease's time if the active node fails. Run two replicas (active and standby) on different nodes, with `strategy: Recreate`, and never autoscale it.
- **The split and convert stages: at least one pod, scaled with KEDA** on their input's backlog (each stage's README, "Autoscaling"). Never scale a stage to zero: the pod holding its duties lease reconciles, finding work whose messages were lost, and a lost message adds nothing to the backlog that would start a pod. On Kafka, give each stage's input topic at least as many partitions as its most pods; on Kafka 4.0 or later, use its group protocol (`groupProtocol=consumer` in the input's URI), so a rebalance doesn't wait for a busy pod; and set the split stage's `maxPollIntervalMs` (30 minutes by default) above the longest item's split time (revision (bz)). Tried in the sandbox with `scripts/sandbox.sh scale-demo` ([SANDBOX.md](SANDBOX.md)).
- **The split stage's database pool** (`sdh.lease.jdbc.pool-size`, 2 connections a pod): keep 2. The stage gathers its items' registry statements into one of each kind (`sdh.split.registry-batch`, 64), so on the AWS PoC its 64 items in flight waited 0.04 s an item in all for 2 connections, and it kept up with landing at pool 2 as at pool 6 (57.6 and 59.4 documents a second end to end, latency to gold no better at 6; [PERFORMANCE.md](PERFORMANCE.md) §13.3, runs M and O). A larger pool holds more connections, a Postgres backend each without PgBouncer, for every split pod. Raise it (6 is tried) only if the stage's `pool_wait` step grows: a database further away or slower than the PoC's, or a split latency that matters more than connections. With gathering off (`registry-batch` 1), 2 is too few: each of 9 takes an item waited about 75 ms.
- **Keep the convert stage's backlog under its reconciler's grace** (10 minutes) on a normal day: a backlog older than that holds its reconciler back, and `stage.reconcile_deferral_capped` is the sign that it needs more pods (STAGES.md §13.4).

## Sizing Postgres and Kafka for an initial load

**Size both before a large load: the PoC's 10 GiB volumes hold well under a million documents.** The estimate below
is from C-CDA only, measured on the AWS PoC on 2026-11-22 with the HIPAA work in place (the audit journal included;
[PERFORMANCE.md](PERFORMANCE.md) §10, runs 62 and 63), with one record a document of about 119 KB. It is a
**minimum**, for one pipeline: nothing was pruned during the runs, and the growth was linear over them. A soak of
hours has not been run (PERFORMANCE.md §11.1), so the numbers for 15 million are extrapolated, not observed.

| | Measured, per C-CDA document | For 15 million C-CDA documents, at least | Notes |
|---|---|---|---|
| **Postgres** (all of a pipeline's schemas) | 14.7 to 14.8 KB (9.6 KB before the audit journal) | **about 210 GiB** of data, so a volume of **at least 250 GiB** | The PoC's volume ran about 1 GiB above the data (write-ahead log, `max_wal_size` 1 GiB); the rest of the margin is for vacuum and index bloat. By schema, a document added about 5.3 KB to transform's, 4.3 to convert's, 3.6 to split's and 1.4 to the landing node's (`public` on the PoC). About 4.5 KB of the whole is the audit journal (13 events a document across the stages, 7 of them split's). |
| **Kafka** (the pipeline's topics, one copy) | 21.9 KB (landed 3.2, split 6.5, records 6.3, FHIR 5.9) | **about 305 GiB a copy**, so **at least 370 GiB** of broker disk a copy | With the decided 30 days' retention (PERFORMANCE.md §12), a load of 15 million at 50 to 60 documents a second (3 to 4 days) is all still held at its end. Multiply by the replication factor (3 is usual; the PoC runs 1). A shorter retention holds less; it should still outlast the longest outage expected, since messages lost to retention are rebuilt by the reconcilers (STAGES.md §8), more slowly than a backlog is read. |

What changes it:
- **Records, not bytes, drive Postgres:** each record has its registry rows at each stage, so a format with many
  records an item (an X12 batch, a large CSV) grows Postgres by its records, and a smaller document grows it
  about as much as a large one.
- **The audit journal** is kept in each stage's schema until exported and pruned (HIPAA-OPERATIONS.md §3.3); a
  pruning schedule keeps it to the export window.
- **What the registries keep per record, and for how long, is not yet decided** (PERFORMANCE.md §10, run 53): a
  retention on registry rows would bound Postgres; until then it grows for good.
- **The landing node's ledger index** takes 36 to 72 bytes an item, kept for good (in the figures above).
- **The object store** (bronze, silver, gold) is not covered here; it grows with the documents' own size, several
  times over across the layers.

## Formats: what has been tested

**Only C-CDA has been tested end to end and under load with a mapping set.** It has one (US Core 6.1, in the
mapping repository), and nearly every measurement in PERFORMANCE.md is of it. CSV has been run end to end at load
on the AWS PoC, with no mapping set (PERFORMANCE.md §13.2, runs G and H). The product also splits and stores the
other formats below, each with its own unit and integration tests of splitting; **none of those has been run end
to end, at load, or through a mapping set, and each needs that testing before a customer relies on it** (PERFORMANCE.md §11.1 says
what to measure):

| Format (`format:`) | What a record is | Not yet tested |
|---|---|---|
| `x12` | a transaction set (`ST`, by default), a functional group (`GS`) or an interchange (`ISA`), each a valid standalone interchange | end to end, at load (a transaction a file, and batches such as an 837 of thousands of claims); no mapping set |
| `edifact` | a message (`UNH`, by default) or an interchange, each a valid standalone interchange | end to end, at load; no mapping set |
| `csv` | a row (RFC 4180), optionally with the header prepended | Run end to end at load on AWS (2026-11-24): 300,000 records as 3,000 files of 100 rows and as 30 of 10,000, exactly once, about 120 to 130 records a second at 12 convert and 18 transform pods. No mapping set; files of millions of rows not tried |
| `delimited` | a run of lines found by a pattern, for proprietary line formats | end to end, at load; no mapping set |
| `xml` | an element by name, as a standalone document (generic XML other than C-CDA) | end to end, at load; no mapping set |
| `fhir` | a resource (by default a `Bundle`) from a single resource, an array or NDJSON; or groups of N resources, for bulk export | end to end, at load (bulk-export NDJSON especially); the transform stage's mapping is for C-CDA |
| `parquet` | a row, as a JSON object; how an Iceberg table intake is split | end to end, at load; no mapping set |

A plugin splitter (`format: <plugin>`) is the customer's to test. Each format runs as its own pipeline, so it
neither slows nor is slowed by another in the code, but its numbers can't be read off C-CDA's.

## Transports: what has been tested

Tested, and in the reference settings (`deploy/reference/`): Kafka over `SASL_SSL` with per-stage ACLs, and
Postgres with TLS (`verify-full`) direct to the server (HIPAA.md §8.1 items 7 and 13).

Tested, not in the reference settings: **Kafka with client certificates (mutual TLS)** instead of SASL, by every
scenario of the control harness on Testcontainers (`SDH_HARNESS_KAFKA_LOGIN=certificate scripts/harness.sh
testcontainers`, 2026-11-24). Each stage has its own certificate, and its Kafka URIs say `securityProtocol=SSL`,
`sslKeystoreType=PKCS12`, `sslKeystoreLocation=<its key store>` and `sslKeystorePassword` and `sslKeyPassword` as
placeholders (`RAW({{env:SDH_KAFKA_KEYSTORE_PASSWORD}})`: a stage refuses a password written into a URI), beside
the broker's CA as with SASL. The broker's client listener sets `ssl.client.auth=required`, and
`ssl.principal.mapping.rules=RULE:^CN=([^,]+),.*$/$1/,DEFAULT` so that a certificate's CN is the Kafka user: the
same users, and the rights below, as with SCRAM. The ACLs themselves were tested with SCRAM (`KafkaSecurityIT`);
the harness's stage users are super users.

**Supported or expected, but not yet tested,** and planned for later iterations, as the product is prepared for
deployment in each environment (HIPAA.md §11.1, P11):
- **AWS MSK with its IAM login;**
- **Postgres TLS through PgBouncer** (the PoC runs PgBouncer, in plaintext). This may be tested sooner than the
  rest, but only after performance has been reviewed further (PERFORMANCE.md §10, runs 60 to 63);
- **GCP Cloud KMS** against the real service (its client is built and tested against a stand-in only);
- **AWS GovCloud and Azure Government** endpoints.

Until each is tested, a customer using it tests it in their own environment first, with synthetic data. Postgres
TLS through PgBouncer is planned with the other infrastructure tests (HIPAA.md §11.4).

## Audit events: the collector

Every stage exports its audit events over OTLP (`sdh.telemetry.otlp.endpoint`; under the compliance profile, a
stage won't start without it). The operator runs the collector that receives them, and sends them on to a store
the pipeline's staff can't change: CloudWatch Logs, Cloud Logging, Azure Monitor, Splunk, Elastic, Loki or any
SIEM. The collector settings that keep every event, the identity and rights each store needs, and how to check it
works are in [HIPAA-OPERATIONS.md](HIPAA-OPERATIONS.md) §2.7.

## Broker rights

Decided 2026-11-09 ([STAGES.md](STAGES.md) §15.8.9, §15.8.10 step 5). Give each stage an identity of its own on the broker, allowed only what it does. Each stage after landing **sends to its own input** as well as onward: an operator's reprocess is queued by the stage's pods as a message to the stage itself, sealed to it and signed by it, and the stage takes a reprocess message only when signed by its own key.

| Identity | Reads | Sends to |
|---|---|---|
| Landing node | (nothing) | its output (`<pipeline>.landed` in the sandbox) |
| Split stage | `<pipeline>.landed` | `<pipeline>.split`, and `<pipeline>.landed` (`sdh.stage.reprocess`) |
| Convert stage | `<pipeline>.split` | `<pipeline>.records`, `<pipeline>.convert.dlq` (`sdh.stage.dlq`), `<pipeline>.split` (`sdh.stage.reprocess`), and the producer's results queue when `sdh.results.enabled` |
| Transform stage | `<pipeline>.records` | `<pipeline>.fhir`, `<pipeline>.fhir.dlq` (`sdh.stage.dlq`), `<pipeline>.records` (`sdh.stage.reprocess`), and the producer's results queue when `sdh.results.enabled` |
| The admin CLIs (`sdh-admin`) | (nothing) | (nothing) |

The admin CLIs send nothing: what follows from an operator's command (a reprocess, a decline's new item end) is sent by the stage's pods. Give operators no broker credentials.

What each broker calls these rights:
- **Kafka:** ACLs `Read` and `Describe` on the input topic and `Read` on the stage's consumer group; `Write` and `Describe` on each topic it sends to. Brokers before Kafka 3.0 also need `IdempotentWrite` on the cluster, since the producers are idempotent.
- **Amazon SQS:** `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` and `sqs:GetQueueAttributes` on the input queue; `sqs:SendMessage` on each queue it sends to, its own input included.
- **Google Pub/Sub:** `roles/pubsub.subscriber` on the input subscription; `roles/pubsub.publisher` on each topic it sends to, the topic of its own input included.
- **Azure Service Bus:** `Listen` on the input queue or subscription; `Send` on each queue or topic it sends to (a SAS rule, or the Data Receiver and Data Sender roles).
- **Azure Event Hubs:** `Listen` on the input event hub; `Send` on each event hub it sends to (a SAS rule, or the Data Receiver and Data Sender roles).
- **ActiveMQ Artemis:** a role with `consume` on the input address's queue and `send` on each address it sends to, in `security-settings`.
- **RabbitMQ:** a user with `read` on the input queue and `write` on each exchange it publishes to.

On Kafka these rights are tried by connector-broker's `KafkaSecurityIT` (since 2026-11-20): a broker taking only
`SASL_SSL` clients, with the landing node's and the split stage's ACLs exactly as above. Each sends and receives
with them, and is refused a topic it only sends to, and another pipeline's topics and consumer group.
`deploy/reference/kafka-acls.sh` applies them for a pipeline. They aren't tried in the sandbox, whose Kafka has no
TLS, SASL or ACLs ([SANDBOX.md](SANDBOX.md), "Not yet as in production").

**Kafka over TLS:** put `securityProtocol=SASL_SSL`, `saslMechanism`, `saslJaasConfig=RAW({{env:SDH_KAFKA_JAAS}})`,
`sslTruststoreType=PEM` and `sslTruststoreLocation` on every Kafka URI, as `deploy/reference/` does. `RAW(...)`
keeps the login's quotes and semicolon intact, and the placeholder keeps it out of the settings. A written value
is refused on every broker URI, input, output, reprocess, dead-letter and results alike (DESIGN.md §5.3.16): the
node doesn't start, and the message names the setting and option, never the value.

## Registry retention

Each record stage (convert, transform) keeps a row in its Postgres registry for every record it has handled
(`stage_record`) and every item (`stage_item`). By default they are **kept for ever**, so the registry grows by
about one row and a few hundred bytes a record: some 15 GB a stage for 15 million documents of C-CDA, more for
fan-out formats (one row a CSV row or X12 transaction). The customer may instead have finished items' rows
deleted after a time they choose:

| Setting | Default | What it does |
|---|---|---|
| `sdh.stage.retention.after` | unset: for ever | How long after an item is complete its `stage_record` and `stage_item` rows are deleted, if none of its failures is open |
| `sdh.stage.retention.queue-retention` | `7d` | The longest the stage's input queue keeps a message, as the broker is configured (Kafka `retention.ms`; SQS at most 14 days) |
| `sdh.stage.retention.interval` | `1h` | How often the duties pod prunes |
| `sdh.stage.retention.batch` | `100` | The most items deleted in one transaction |

The stage refuses to start with `after` shorter than `queue-retention`, or than its reconciler's look back
(`sdh.reconcile.lookback`), each plus a day. Pruning runs on the duties pod and is audited,
`stage.registry_pruned`, with the number of items and no item's ID.

What is never deleted: anything stored (the records in silver and gold, with their item, record and version), the
landing ledger (`landed_items`: every item received, its digest and its links to items it is a copy of), failures
and their events (open, declined or superseded), the audit journal, and the split stage's own registry, whose
item rows its reconciler compares with the landing ledger at each start (a split item without one would be split
again); the split stage also relies on never deleting an item's row to skip reading failures for an item
it has just made (STAGES.md §18). Retention applies to the convert and transform stages only.

**What the choice means:**
- **Repeated messages.** A record's row is what makes a message delivered a second time a no-op. Once the row is
  gone, the same message would be processed as new, and its output stored again. The minimum above stops the
  queue redelivering one; a message replayed by hand from a backup or an older offset after that time is not
  caught. Kept for ever, every repeat is caught.
- **Reprocessing.** An operator's reprocess of an item (`sdh-admin reprocess`) works after pruning: it is a new
  generation, stored as a new version beside the old, never in place of it. What is lost is the check that a
  record's new output matches what was stored before (`record.conflict`), for that item.
- **Questions the registry answered.** For a pruned item, the stage's `status`, `item` and latency queries
  (time to gold) say nothing; the item's history is still in the stored tables, the ledger and the audit
  journal, by item ID. Reports that count from the registry (PERFORMANCE.md's gold check, the load test's
  report) count only what is kept.
- **Failures.** An item with an open failure is kept whole until it is resolved or declined, then kept for the
  retention again from its completion.
- **Sizing.** With a retention, the registry holds about `after` × the daily record rate; for ever, every record
  since the start. Postgres's own `VACUUM` reclaims the space; a large first prune can be spread by a smaller
  `batch`.
- **HIPAA.** The registry holds no PHI (IDs, states, digests and sealed messages to the next stage), so the
  choice is the customer's records-management decision, not a HIPAA one; keep it at least as long as the
  customer's own policy for operational records of processing. Deletion is audited.

## Object store versions: needed for a restore

Turn on versioning for every bucket of a pipeline, and keep noncurrent versions at least as long as the database's
point-in-time window. A restore of the database to an earlier point (HIPAA-OPERATIONS.md §7, DESIGN.md §18)
references files the stages' maintenance has deleted since, and its repair brings them back from their last
versions; without them it stops. Bronze matters most: once an item has landed, it is the only copy. The repair
brings files back on S3 (and S3-compatible stores with versioning) only; on GCS or Azure Blob it names each missing
file for the store's own tools.

The hipaa profile checks it: each stage asks the store, as it opens each table, whether its bucket keeps versions,
and refuses to start if it doesn't, or if the store won't say (`store-versioning`, DESIGN.md §7.6). On S3 each
stage's role needs `s3:GetBucketVersioning` on its buckets; on GCS, `storage.buckets.get`. Azure's versioning is a
setting of the storage account, which a stage can't read: check it yourself and waive the check with that reason.
The check doesn't see the lifecycle rule for noncurrent versions: check that when you deploy, and at each restore
test.

An erasure (`erase`, `purge`) deletes the current objects; their noncurrent versions stay until the lifecycle rule
expires them. Set that rule to the shortest window your recovery plan allows, and say so in your retention policy.

## FIPS 140

**Not supported** (2026-11-23). No stage has been tested or configured with a FIPS 140-2 or 140-3 validated
module, which US federal buyers (FISMA) and many state and local government buyers require; each distribution's
README says so to customers ("Cryptography and FIPS 140"). Record encryption and message sealing run in the JVM,
under the JDK's providers or the native one chosen with `sdh.crypto.jose.provider` (`accp`, `conscrypt`: their
standard, non-FIPS builds). A KMS's own validation covers only what it does (wrapping data keys, the stages' KMS
signatures). The work needed is HIPAA.md §8.1 item 5 and P8.

## Settings

| Setting | Default | What it does |
|---|---|---|
| `sdh.crypto.jose.provider` | `jdk` | Who does message sealing's elliptic-curve work: `jdk`, `accp` (Amazon Corretto Crypto Provider, native, Linux x86_64 and arm64; for AWS) or `conscrypt` (native, the same platforms; for Google Cloud and Azure). A native one that can't load stops the stage at startup. None is a FIPS 140 validated build ("FIPS 140" above) |
| `sdh.crypto.jose.agreement-messages` | `10000` | The most messages sealed under one key agreement (one ephemeral key) to the next stage's key; 1 for one a message (docs/PERFORMANCE.md §13) |
| `sdh.crypto.jose.agreement-lifetime` | `60s` | The longest one key agreement is used |
| `sdh.landing.reconcile.enabled` | `true` | Whether the splitter looks for landed items whose messages were lost |
| `sdh.landing.reconcile.grace` | `10m` | How long an item may wait for its message before it is added without it. Keep it well above normal delivery time, and above any clock difference between the two nodes |
| `sdh.landing.reconcile.interval` | `15m` | How often to look, after the first time at startup |
| `sdh.lease.jdbc.pool-size` | `2` | The split stage's connections a pod for its registry, leases and checkpoints ("Scaling" above) |
| `sdh.split.registry-batch` | `64` | The most items' registry statements of one kind the split stage gathers into one; 1 turns gathering off ([STAGES.md](STAGES.md) §18) |
