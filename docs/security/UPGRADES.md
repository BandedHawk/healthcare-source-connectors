# Upgrades and patching

For the platform administrator who deploys a release, the operators who pause and resume the pipeline around it,
and the security official who sets how quickly fixes are taken. It covers what a release changes, what keeps data
whole across it, how to take one, how to go back, and how to take patches to the parts the customer runs. It is
HIPAA.md §11.2's document D12, for the customer's patch management (§ 164.308(a)(1)(ii)(B), (a)(5)(ii)(B)) and
for keeping data whole while the system changes (§ 164.312(c)(1)). The design behind it is DEPLOYMENT.md,
"Upgrading" and "Changing a stage's version", and STAGES.md §14.4a; each kit's README has a section "Changing the
version". It also covers testing a new WSTL set (§3.4) and changing a pipeline's configuration (§3.5), such as
moving an intake from a directory to SFTP, which HIPAA treats as an operational change: evaluate again
(§ 164.308(a)(8)) and update the policies and procedures (§ 164.316(b)(2)(iii)). What the vendor commits to (how
soon fixes are released, how to report a vulnerability) is [VENDOR-COMMITMENTS.md](VENDOR-COMMITMENTS.md) (D11, a
draft).

It describes a pipeline deployed from `deploy/reference/` and the kits' READMEs, with the `hipaa` compliance profile
on. **A release never loses or rewrites data on its own.** Every record is stamped with the version of the stage
that made it, a new release starts work only once nothing is part way under another version, tables only gain
columns, and a message no stage can read yet waits on the broker rather than being dropped.

## 1. What a release changes, and what keeps data whole

Every new build is a new release at every stage, even one that changes nothing a stage does: the build is part of
the version each record is stamped with (STAGES.md §14.2).

| What changes | What keeps data whole | Where |
|---|---|---|
| **The stage's version:** the build, the splitter or the format's mapping, the WSTL set, and a digest of the settings that change the output | the **drain check:** a pod of a new release waits, not ready and taking no work, while any item is part way under a version it wouldn't stamp; the old pods finish those items. An item is made under one version | `DrainGate`; §3 |
| **The messages between the stages:** the contract version, now `3.0` | a reader takes any minor version of its major, and ignores fields it doesn't know. A later major version waits on the broker, unacknowledged, until the stage is upgraded; an earlier one, which no upgrade reads, is rejected and its item recovered by the reconcilers | `ContractVersion`; §3.1 |
| **The database's tables** (registries, fail store, control tables) | each pod makes its tables at startup, and a release only **adds:** a table, a column, an index, or drops a default or a `NOT NULL`. So the old pods, still running through the rollout, and the release before, after a rollback, read them | `SchemaChanges`; §4 |
| **The Iceberg tables** | only optional columns are added, at startup; a reader that doesn't know one ignores it | `IngestTables` |
| **Settings** | a setting that changes the output is in the version's digest, so it is a new release, held by the drain check like a new build. Under the `hipaa` profile a release may add compliance checks: a pod that fails one refuses to start, naming the check (CONFIGURATION.md) | §3.3 |
| **Keys** | a release changes no key, and reads what earlier releases sealed | KEYS.md |

## 2. Before taking a release

1. **Verify it.** Check the tarball's signature, the image's signature and its SBOM attestation, with the release's
   `cosign.pub` (each kit's README, "Verify what you received"). Deploy images by digest, never by tag, and admit
   only signed ones (HARDENING.md).
2. **Scan it yourself,** with your own scanner, from the image's SBOM. The release was scanned before it was signed
   (DESIGN.md §14.5): no High or Critical vulnerability with a fix available, and each accepted finding written down
   with a reason. Vulnerabilities published since then are only found by scanning again.
3. **Read what the release says changed:** a new contract major version, a table change, a new compliance check, a
   setting renamed (§7: there are no release notes yet).
4. **Take a backup point** of the database (HIPAA-OPERATIONS.md §7), and record it. A release that changes a table
   or the registries is followed by a restore test (HIPAA-OPERATIONS.md §7).
5. **Check each queue's retention** against the time the pipeline will be paused (Kafka and Pub/Sub 7 days by
   default, SQS 4). Messages waiting longer than that are lost, and only the reconcilers find their items again.
6. **Try it first** on a pipeline that isn't production, with synthetic data: `scripts/harness.sh` runs the control
   harness's scenarios against a throwaway pipeline (CONTROL-API-DESIGN.md, H1 to H3).

## 3. Upgrading a pipeline

The whole pipeline, with nothing part way anywhere:

1. **Pause the landing node** (`pause`, `pipeline.operate`; HIPAA-OPERATIONS.md §7.1). It lists nothing more, and
   answers pushes `503` with `Retry-After`, so senders try again later. What it has landed goes on through the
   stages.
2. **Wait until each stage is empty:** `drain-check` in a pod of each of the split, convert and transform stages
   lists nothing part way, and each input queue has no lag. Open failures, held split items, pending corrections
   and queued reprocessing never hold a release: each is made, when it runs, under the version running then.
3. **Deploy the stages last first:** transform, convert, split, then the landing node. A stage is always upgraded
   before the one that sends to it, so no stage receives a message newer than it reads (§3.1).
4. **Check each stage's new pods are ready:** each audits `stage.version_changed` with how it was let in
   (`drained`). A pod held back audits `stage.version_blocked` (how many items hold it, never which), and
   `drain-check` in it says what holds it.
5. **Deploy each stage's control Deployment** with its workers, from the same release. The control pods take no
   work, and the drain check doesn't hold them.
6. **Resume the landing node.**

How each stage takes a new release:

| Stage | Deployment strategy | Held by the drain check | During the rollout |
|---|---|---|---|
| Landing | two replicas, active and standby; `strategy: { type: Recreate }` with an HTTP push intake (a standby is never ready) | no: it stamps no version | with Recreate, pushes are refused until the new active node takes the lease, within about a minute |
| Split, convert, transform | `rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }`: a new pod first | yes | the old pods go on; a new pod is ready only once let in. A new pod that fails to start (a refused compliance check, say) never takes an old one down |

**Why the stage before is paused, not the stage itself.** A paused stage takes nothing from its queue. At the
convert and transform stages an item is part way while some of its records are stored and others are still on
the queue; pausing the stage itself (`drain`) leaves them there, so its new pods stay held until it is resumed. A
`drain` is for stopping a stage with nothing in a pod, not for changing its version. Pausing what feeds the stage
lets it empty its queue and finish its items.

**How long it takes:** the time the stages take to empty their queues, plus a rollout per stage. In the sandbox a
held pod was let in at its next check, within the minute (SANDBOX.md, the drain check, 2026-11-10); a held pod
checks every `sdh.stage.version-check-interval`. The time at production size has not been measured (§7).

| Setting | Default | What it does |
|---|---|---|
| `sdh.stage.version-check-interval` | 1m | how often a pod held back by the drain check checks again |

### 3.1 The contract between the stages

Every message carries its contract version, `"v": "<major>.<minor>"`; this release writes `3.0`.

- **A minor version** only adds optional fields. Stages of different minor versions work together, in either order.
- **A later major version** than a stage reads is not dropped: the stage audits `split.unreadable_version` (or
  `stage.unreadable_version`) and leaves the message unacknowledged, so it is delivered again, with a growing pause,
  until the stage is upgraded. The messages behind it wait. The split stage still finds the items by reconciling.
- **An earlier major version,** a message the release before sent and still queued, is never read, so it isn't held:
  it is rejected and acknowledged (`split.rejected` or `stage.message_rejected`, reason `EARLIER_VERSION`), and its
  item is recovered by the reconcilers or by a reprocess at the split stage.

With the pipeline emptied first (§3), no message of another major version is in flight when the stages change.

### 3.2 One stage only

A new WSTL set at the transform stage, a mapping at the convert stage, or a setting in the version's digest changes
that stage's version alone. Pause the stage before it, wait until its `drain-check` lists nothing part way, deploy
it, check `stage.version_changed`, and resume. At the convert stage, a change to one format's mapping waits only on
that format's items.

### 3.3 When it can't wait

If a new pod is held and the change can't wait, an operator accepts its release in that pod (`version --accept`,
with the reason on standard input, kept encrypted): audited `stage.version_forced`, and the pod is let in
(`forced`) at its next check. An item then made under two versions is recorded as such: the convert stage audits
`record.version_differs`, and the split stage holds an item whose stored records another version made
(`split.version_held`) until an operator releases it (`release`). Nothing is lost, but the item is no longer made
under one version: record why in the ticket.

**After an outage, restart on the same release, let it drain, then upgrade** (PERFORMANCE.md §8). A new release
started straight after an outage is held until the old version's items are finished, and no old pod is left to
finish them.

### 3.4 Testing a new WSTL set

A WSTL set decides what goes into gold: a mistake in one maps records wrongly, or leaves a field out, without an
error. The stage checks only that the set loads (a set that won't load stops the stage's start, before it takes any
message, so no record fails) and puts the set's digest in its version. Whether the output is right is the customer's
test, before production:

1. **Review the change as code.** Keep the set in version control, have each change reviewed by someone other than
   its author, and raise the `version` in its `wstl-set.json`. Record the set's name and version in the change
   ticket.
2. **Run it on a pipeline that isn't production,** with synthetic or de-identified documents only, never real PHI.
   Land a corpus under the current set, deploy the new one (§3.2), and reprocess what the current set made:
   `reprocess --made-by <current version>` at the transform stage, with `--dry-run` first (STAGES.md §15.8.4).
   `reprocess --status --run <id>` gives each record's outcome, `same`, `different` or `failed`. Every `different`
   should be one the change meant, and every `failed` explained: each is an open failure with its code.
3. **Check that it is deterministic:** reprocess again what the new set made (`--made-by <new version>`). Any
   `different` means the set doesn't give the same output for the same input (a clock, a random number or a UUID
   in it), which the stage can't otherwise detect. Don't deploy such a set (STAGES.md §17.10.2).
4. **Validate the bundles** against the profile the set says it makes (`makes`; US Core 6.1 for C-CDA), with the
   validator your gold readers rely on.
5. **Pin it, and deploy it in production** as §3.2. `set-digest` prints the set's digest as the stage computes it
   (the transform stage's README, "The WSTL set"); give it to the stage as `sdh.transform.wstl-digest`, and the
   stage refuses to start with any other set (`transform.set_not_loaded`, naming both digests). Put the set in a
   new immutable ConfigMap named for it, and change the Deployment's ConfigMap and pin together: a new set is then
   always a change of the Deployment, which `kubectl rollout undo` takes back with the set. Check that the release
   `stage.version_changed` names, and `version` lists, carries the set's name, version and digest as tested.
   Records already in gold stay as the old set made them; to map them again under the new one, reprocess them
   (`--made-by`), which keeps the old ones, superseded (§4, "Its data").

**Who may change it.** The set is a ConfigMap mounted read only, not part of the signed image (STAGES.md §17.10.2),
so whoever can change it, or the Deployment, can change how PHI is mapped. Give write access to both only to those
who may deploy the stage (Kubernetes RBAC). With the pin, a set can't be swapped without the Deployment being
changed too, which the API server's audit log records; a version change that no ticket accounts for is a security
incident to look into (HIPAA-OPERATIONS.md §6). **Never edit a set's ConfigMap in place:** a mounted ConfigMap
changed in place is changed under the running pods, which go on with the set they loaded, while each new pod (KEDA
scaling out, a restart, a node drained) loads the new one, a new release the drain check holds back, not ready, so
the stage quietly loses the pods it scales to. An immutable ConfigMap refuses the edit. The set isn't signed (§7).

**Why not send a set to running pods.** A set is code that decides what PHI goes into gold, and the stage's
guarantees rest on a pod's set being fixed for its life: an item made under one version, the drain check, and
reprocess outcomes compared by version. A way to load a set into a running pod would break those, and would be a
way into pods that hold decrypted PHI, needing its own permission, signature check, approval and audit: what a
Deployment change already has. A new set needs no new image, only a rollout.

### 3.5 Changing a pipeline's configuration

A change to a pipeline's settings, Secrets or manifests that isn't a new release still changes how PHI is
received, held or sent. What the product notices of one:

| What changes | What the product records | What it doesn't |
|---|---|---|
| A split, convert or transform setting that changes the output (one in the version's digest) | a new version: held by the drain check, audited `stage.version_changed` | |
| A WSTL set | a new version, by the set's digest (§3.4) | |
| A compliance waiver | `compliance.waived` at every start, with its reason | |
| Any other stage setting: a broker, a database, a key, a limit | `connector.started`, as at any start | what changed: `GET /settings` (the control API's `settings`) shows only what is set now |
| The landing node's intake: its type (a directory to SFTP, say), host, bucket, path, access mode or credentials | `connector.started`, naming only the intake and its lease epoch: the landing node stamps no version | that anything changed (§7) |

So the record of a configuration change is the customer's: the change ticket, the manifests in version control,
and the Kubernetes API server's audit log of who changed a ConfigMap, Secret or Deployment. Keep the output of
`GET /settings` (secrets removed) from before and after the change with the ticket.

**What HIPAA asks.** The Security Rule has no standard named change management, but a change of this kind is an
"operational change" under two of them: the evaluation is to be done again in response to it (§ 164.308(a)(8)),
and the documentation reviewed and updated (§ 164.316(b)(2)(iii)). The risk analysis and risk management are
ongoing (§ 164.308(a)(1)(ii)(A)–(B)), and the change must keep the audit controls (§ 164.312(b)), integrity
(§ 164.312(c)(1)) and transmission security (§ 164.312(e)(1)). The proposed Security Rule (90 FR 898, not final;
HIPAA.md §2) would also require a technology asset inventory and a network map, reviewed at least every 12 months
and whenever the environment or operations change.

**Changing an intake** (a directory to SFTP, a bucket to HTTP push, a new sender's server):
1. **Assess it first,** with the security official: the new transport's risks in the risk analysis; the sender's
   agreement (a BAA where the sender's server, or a third party's, now holds the PHI on its way); the network map
   (NETWORK.md §7) and asset inventory; and who now holds which credentials.
2. **Set it up to CONFIGURATION.md's requirements** for the transport: an SFTP server's host key pinned in
   `known-hosts`; SMB 3 with encryption; TLS and the sender's authentication for a push. The compliance profile
   refuses the plaintext endpoints it knows of, and checks nothing else about the new source.
3. **Empty the old source first.** Let the landing node land everything on it (with `access: read-write`, nothing
   is left there), then pause it (§3, step 1).
4. **Prefer a new intake name** (`sdh.landing.intake`) for a new kind of source. The name keys the landing lease,
   the checkpoint and the ledger, and an item's source reference is the intake name with the item's path relative
   to the source. Under a new name the ledger starts empty, so a file found on both sources is landed again, as a
   duplicate (`item.duplicate_content`; nothing is dropped). Under the same name, whether a file at the same
   relative path is taken for one already landed depends on the size and version each source reports, which is
   untested (§7).
5. **Deploy and resume,** then check: `connector.started` from the new pod, no `intake.unreachable`, and an item
   followed through to gold. Record in the ticket the settings before and after, when, and who approved it.
6. **Retire the old source:** revoke its credentials, remove its firewall rules and network policies, and dispose
   of anything left on it under your disposal policy (HIPAA-OPERATIONS.md §8).

## 4. Going back

**The release current before is always let in** (`previous`), whatever is part way: `kubectl rollout undo`, stage
by stage, the reverse of §3 (landing first, then split, convert, transform), with the landing node paused.

- **Its tables:** a release only adds to them, so the release before reads them; `UpgradeGuideTest` checks that no
  stage's code drops, renames or retypes a column or table (§6). One change did more: the fail store's primary key
  was widened on 2026-11-09 (STAGES.md §15.8.2), before any release; a release from before that can't write
  failures to a store changed by it.
- **Its messages:** a release going back to an earlier contract major version holds the later major's messages its
  stages can't read, unacknowledged, until it is upgraded again. Go back only to a release of the same major
  version, or empty the pipeline first.
- **Its data:** records made by the newer release stay, stamped with its version. Going back doesn't remove them.
  To make them again under the release before, reprocess their items.

Going back further than one release is a new change: the drain check holds it until nothing is part way.

## 5. What the audit trail shows

| Event | When |
|---|---|
| `stage.version_changed` | a release was let in, with how: `first` (the stage's first), `previous` (a rollback), `in_flight` (a rollout already begun), `drained` (nothing part way under another version), `forced` (accepted by an operator). A pod of the current release, `current`, audits nothing |
| `stage.version_blocked` | a pod of a new release is held: how many items hold it |
| `stage.version_forced` | an operator accepted a held release |
| `split.unreadable_version`, `stage.unreadable_version` | a message of a later contract major version waits for this stage's upgrade |
| `split.rejected`, `stage.message_rejected` (`EARLIER_VERSION`) | a message of an earlier contract major version was rejected; its item is recovered |
| `record.version_differs`, `split.version_held` | an item was made under two versions (§3.3) |

`version`, in any pod, lists the releases let in, newest first: the stage's record of what ran when. Keep these
events with the change ticket.

## 6. Patching

**What the vendor's releases carry** (DESIGN.md §14.4, §14.5):
- **Base images,** pinned by digest in `docker/base-images.env`; a new build of a pinned tag is taken by one pull
  request (Renovate), and a release isn't signed on a stale base.
- **Java libraries,** with security overrides in the parent POM where a library brings an older version.
- **A vulnerability scan** of the jars' and images' SBOMs before signing: it fails on any High or Critical finding
  with a fix available.

A fix reaches a pipeline only as a new release, taken as §3 sets out. There is no patch applied in place: the images
are read-only and have no shell.

**What the customer patches** (shared responsibility, HIPAA.md §10):

| Component | What a patch does to the pipeline |
|---|---|
| Kubernetes nodes and control plane | a drained node's pods stop gracefully and start elsewhere; a stage's claims are taken over (STAGES.md §10). Keep the landing node's two replicas on different nodes |
| PostgreSQL, PgBouncer | a restart is an outage: pods wait and retry, and the reconcilers find what was in flight. The harness's `outages` scenario pauses each dependency for 30 s with an item on its way, and nothing needs reprocessing after |
| The broker | the same; topics keep their messages across a rolling restart |
| The object store, KMS, Vault | the same, for a managed service's own maintenance |
| KEDA, the ingress, the CNI | while KEDA is down a stage keeps its pods; while the ingress is down the control API and pushes are unreachable, and senders retry |

A major version of PostgreSQL or the broker is a change of its own: back up first, and try it on a pipeline that
isn't production. DEPLOYMENT.md, "Transports: what has been tested", lists what the product has been run against.

**How quickly.** HIPAA leaves the schedule to the covered entity's risk analysis (§ 164.308(a)(1)(ii)(B)). The
proposed Security Rule (90 FR 898, proposed § 164.308(a)(4)(ii)(B)–(C), checked 2026-11-25) would require
patches within 15 calendar days of identifying the need for a critical risk and 30 for a high one (or of a patch
becoming available, where none is yet), and a review and test of the patch policy every 12 months; it is not
final (HIPAA.md §2, §7). Set the windows in your policy, by severity, and measure them from the release that
carries the fix. How quickly the vendor releases a fix is D11's.

## 7. Findings

- **No release notes.** Nothing yet tells a customer what a release changes: a contract major version, a table, a
  compliance check, a setting. The product needs a release-notes document with those headings, kept with each
  release (§2).
- **Going back across a table change is untested.** The tables only gain columns, and `UpgradeGuideTest` holds the
  code to that, but no test runs the release before against tables a newer release made.
- **A whole-pipeline upgrade has not been timed at size.** The sandbox shows a held release let in within a
  minute; no run at the AWS PoC's size has timed a pause, empty, deploy and resume (PERFORMANCE.md §8 saw only
  a release held after an outage).
- **The control Deployments aren't checked for version.** A control pod of one release beside workers of another is
  not refused; deploy them together (§3, step 5).
- **The documents said to drain a stage before changing its version.** HIPAA-OPERATIONS.md §7.1 told operators to
  drain the stage itself before an upgrade, which at the convert and transform stages leaves its new pods held
  while it is paused (§3). Corrected there.
- **The landing node doesn't notice an intake change.** It stamps no version, and `connector.started` names only
  the intake and its lease epoch, so moving an intake from a directory to SFTP leaves no trace in the audit trail
  (§3.5). Nor is any other setting outside the version's digest recorded. The product needs, at each start, a
  keyed digest of those settings and of the intake's identity (type, host or bucket, path, access mode, how it
  authenticates; never the values), and `intake.changed`, for review, when it differs from the last start.
- **Changing an intake's type under the same name is untested:** what the ledger takes for already landed when the
  same relative paths appear on a new source (§3.5, step 4).
- **There is no way for a customer to build its own WSTL set.** §3.4 assumes a set in hand, but sets are packaged
  only by the vendor's mapping build: there is no template, packaging tool, local runner or guide for a
  proprietary CSV or other custom mapping (KNOWN-GAPS.md §5).
- **A WSTL set isn't signed** (§3.4). The pin (`sdh.transform.wstl-digest`) ties what runs to what was tested,
  but an unpinned stage loads any set, and nothing requires the pin under the `hipaa` profile.
- **The vendor's commitments are unanswered** (D11, drafted as [VENDOR-COMMITMENTS.md](VENDOR-COMMITMENTS.md)):
  how soon a fix for a High or Critical vulnerability is released, how long a release is supported, and how to
  report a vulnerability. The release registry and where releases are published are not set up yet (DESIGN.md
  §14.4).

## Sources

- **Code:** `DrainGate` and `StageReleases` (the drain check), `ContractVersion` and `PayloadCodec` (the contract),
  `SchemaChanges`, `FailureStore.ddls` and `IngestTables` (the tables), `StageProperties`
  (`version-check-interval`), `LandingRuntime` (`connector.started`) and `SourceRef` (an item's source reference).
- **Documents:** DEPLOYMENT.md, "Upgrading" and "Changing a stage's version"; STAGES.md §14.2, §14.4a, §15.8.4,
  §17.10.2; CONFIGURATION.md; NETWORK.md §7; DESIGN.md
  §10.2, §14.4, §14.5; each kit's README, "Changing the version" and "Verify what you received"; SANDBOX.md, the
  drain check; PERFORMANCE.md §8; HIPAA-OPERATIONS.md §6, §7, §7.1, §8.
- **The rule:** 45 CFR § 164.308(a)(1), (a)(8), § 164.312(b), (c)(1), (e)(1), § 164.316(b)(2)(iii)
  (https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164/subpart-C); the proposed rule, 90 FR 898
  (https://www.federalregister.gov/documents/2025/01/06/2024-30983).
- **The test:** `UpgradeGuideTest` (connector-stage) checks that this document names every way a release is let in,
  the contract version written, and the drain check's setting with its default; that every stage Deployment in the
  kits' READMEs adds a pod first and takes none away; and that no stage's code drops, renames or retypes a table or
  column.
