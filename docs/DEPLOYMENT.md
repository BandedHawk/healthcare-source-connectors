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
- **A message the broker lost after accepting it** (the splitter). This covers a purged queue, a lost broker node, a retention limit passed while the splitter was down, or a dead-letter queue. The split stage reconciles (`SplitReconciler`, [STAGES.md](STAGES.md) §8). At startup, and then every `sdh.landing.reconcile.interval` (15 minutes), it splits every item of its intake that was fully landed more than `sdh.landing.reconcile.grace` (10 minutes) ago and was never started. It also takes on every item left part way, whose checkpoint hasn't been written for longer than the grace: its pod stopped while splitting it, and its message was lost too (built 2026-11-07). Each one is audited as `split.recovered` (reason `MESSAGE_MISSING` or `CHECKPOINT_STALLED`), which flags the broker problem. If the message turns up later, it only announces the item again. Tested by `FileConnectorApplicationTest.recoversLandedItemsWhoseMessagesWereLost` (not before the grace, not twice, not another intake's item, and the late message only announces) and `resumesAnItemLeftPartWayWhoseMessageWasLost`. What the split stage sent on and the broker then lost is found by the convert stage's reconciler (below), or recovered by an operator with a reprocess at the convert stage (`sdh-admin reprocess --item-id`, STAGES.md §15.8.9), which reads the records from `split_records`. An item can also be split again as a whole, by a reprocess at the split stage.
- **A `record.split` the broker lost** (the convert stage). The convert stage's reconciler, on the pod holding its duties lease, rebuilds every record split more than `sdh.reconcile.grace` (10 minutes) ago that it hasn't converted, at startup and then every `sdh.reconcile.interval` (15 minutes), and audits each item as `convert.records_recovered`. A record a pod claimed and never stored, its claim expired, is rebuilt the same way, and an item whose records are all stored with its total, but which wasn't completed, is completed and audited `convert.item_completion_recovered` (design revision (by)). It remembers how far it has checked (`stage_reconcile`, shown by `sdh-admin status`), so after an outage it checks every day since, not only the last `sdh.reconcile.lookback` (2 days); a catch-up is audited `convert.reconcile_catching_up` and `convert.reconcile_caught_up` (design revision (bs)). Tested by `ConvertStageTest.catchesUpOnAnOutageLongerThanTheLookback`.

**Starting again after a long outage takes longer** (STAGES.md §13.4). The convert reconciler can't tell a record still queued from a lost one, so it waits for the queue first (design revision (bt)). At startup it waits `sdh.reconcile.backlog-quiet` (15 minutes) for the pods to begin receiving, then for as long as records older than the grace still come off the queue on any pod (`convert.reconcile_deferred`). Only then does it check the days it missed, so lost records are converted after the backlog. If the backlog takes longer than `sdh.reconcile.max-deferral` (6 hours) to drain, it goes ahead anyway (`convert.reconcile_deferral_capped`): it then converts records still queued, one at a time on the duties pod, alongside the other pods. Each is converted once, but the backlog drains more slowly, and a failed record it reaches first is audited `convert.placement_unknown` (its result can't name the producer's message). Raise `max-deferral` for a restart whose backlog will take longer. On a normal day, `reconcile_deferral_capped` means the queue is always more than the grace behind: add pods. Before a planned outage, drain the queues if you can, and check that each queue's retention (Kafka and Pub/Sub 7 days by default, SQS 4) is longer than the outage. Afterwards, check with the audit query of STAGES.md §13.2 that every item completed at each stage. Tested by `ConvertStageTest.waitsForABacklogToDrainThenGoesAheadAnyway` and `waitsAtStartupForTheQueueToStartDelivering`.

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

## Scaling

Decided 2026-11-07 ([STAGES.md](STAGES.md) §10).
- **The landing node: never more than one in service.** It lands serially, and landing in parallel could land one file twice, by the different ways a file can be retrieved. Only the node holding its lease lands; a standby waits and takes over within the lease's time if the active node fails. Run two replicas (active and standby) on different nodes, with `strategy: Recreate`, and never autoscale it.
- **The split and convert stages: at least one pod, scaled with KEDA** on their input's backlog (each stage's README, "Autoscaling"). Never scale a stage to zero: the pod holding its duties lease reconciles, finding work whose messages were lost, and a lost message adds nothing to the backlog that would start a pod. On Kafka, give each stage's input topic at least as many partitions as its most pods; on Kafka 4.0 or later, use its group protocol (`groupProtocol=consumer` in the input's URI), so a rebalance doesn't wait for a busy pod; and set the split stage's `maxPollIntervalMs` (30 minutes by default) above the longest item's split time (revision (bz)). Tried in the sandbox with `scripts/sandbox.sh scale-demo` ([SANDBOX.md](SANDBOX.md)).
- **Keep the convert stage's backlog under its reconciler's grace** (10 minutes) on a normal day: a backlog older than that holds its reconciler back, and `convert.reconcile_deferral_capped` is the sign that it needs more pods (STAGES.md §13.4).

## Broker rights

Decided 2026-11-09 ([STAGES.md](STAGES.md) §15.8.9, §15.8.10 step 5). Give each stage an identity of its own on the broker, allowed only what it does. Each stage after landing **sends to its own input** as well as onward: an operator's reprocess is queued by the stage's pods as a message to the stage itself, sealed to it and signed by it, and the stage takes a reprocess message only when signed by its own key.

| Identity | Reads | Sends to |
|---|---|---|
| Landing node | (nothing) | its output (`<pipeline>.landed` in the sandbox) |
| Split stage | `<pipeline>.landed` | `<pipeline>.split`, and `<pipeline>.landed` (`sdh.stage.reprocess`) |
| Convert stage | `<pipeline>.split` | `<pipeline>.records`, `<pipeline>.split` (`sdh.stage.reprocess`), and the producer's results queue when `sdh.results.enabled` |
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

Not tried in the sandbox, whose Kafka has no TLS, SASL or ACLs ([SANDBOX.md](SANDBOX.md), "Not yet as in production").

## Settings

| Setting | Default | What it does |
|---|---|---|
| `sdh.landing.reconcile.enabled` | `true` | Whether the splitter looks for landed items whose messages were lost |
| `sdh.landing.reconcile.grace` | `10m` | How long an item may wait for its message before it is added without it. Keep it well above normal delivery time, and above any clock difference between the two nodes |
| `sdh.landing.reconcile.interval` | `15m` | How often to look, after the first time at startup |
