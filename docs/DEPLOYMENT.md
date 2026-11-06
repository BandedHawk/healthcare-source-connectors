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
- **A message the broker lost after accepting it** (the splitter). This covers a purged queue, a lost broker node, a retention limit passed while the splitter was down, or a dead-letter queue. The splitter reconciles (`InboxReconciler`, §5.3.12). At startup, and then every `sdh.landing.reconcile.interval` (15 minutes), it adds every item of its intake that was fully landed more than `sdh.landing.reconcile.grace` (10 minutes) ago and isn't in its inbox. Each one is audited as `inbox.recovered` (reason `MESSAGE_MISSING`), which flags the broker problem, and is split after the items already waiting. If the message turns up later, it is a repeat and is ignored. Tested by `FileConnectorApplicationTest.recoversLandedItemsWhoseMessagesWereLost`: not before the grace, not twice, not another intake's item, and the late message is a repeat.

**Watch for:** `inbox.recovered` events. Any at all mean messages are going missing between the stages, and the broker should be looked at.

**One consequence:** a new splitter pointed at existing landing data splits everything already landed for its intake. That is deliberate: nothing landed goes unsplit. To start a new splitter without that, set `sdh.landing.reconcile.enabled: false` for its first run.

## Upgrading: the `item.landed` contract

Every message carries its contract version, `"v": "<major>.<minor>"` (§10.2).
- **Minor versions** only add optional fields. A reader ignores fields it doesn't know, and fields added since an older writer read as unset. So a landing node and a splitter that differ only in minor version work together, in either order.
- **A new major version** is a breaking change. A splitter that can't read it doesn't drop the message, which would strand its item. Instead it audits `inbox.unreadable_version` and leaves the message unacknowledged, so the broker delivers it again, with a growing pause, until the splitter is upgraded. The messages behind it wait. The splitter keeps running, and it still picks up the items through reconciling once their grace has passed. Nothing is lost.

**Upgrade order:**
1. Upgrade the splitter first, then the landing node, whenever the contract's major version changes. The same rule applies to downstream consumers of the splitter's output: upgrade them before the splitter writes a new major version.
2. Minor versions can be upgraded in either order.

**Tested by:**
- `ContractVersionTest`: a later minor version, with unknown fields, is read; an earlier minor version, without `replaces`, is read with it unset; another major version, or none, is refused with `UNSUPPORTED_VERSION`, and that is distinct from a bad payload.
- `FileConnectorApplicationTest.takesOtherMinorVersionsAndWaitsOnAnotherMajor`: the splitter takes and splits items sent with a later and an earlier minor version. A later major version is neither acknowledged nor fatal. On a queue it is tried again in place, and holds back the message behind it. Reconciling still gets its item split.

## Settings

| Setting | Default | What it does |
|---|---|---|
| `sdh.landing.reconcile.enabled` | `true` | Whether the splitter looks for landed items whose messages were lost |
| `sdh.landing.reconcile.grace` | `10m` | How long an item may wait for its message before it is added without it. Keep it well above normal delivery time, and above any clock difference between the two nodes |
| `sdh.landing.reconcile.interval` | `15m` | How often to look, after the first time at startup |
