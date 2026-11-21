# The pipeline control API and its test harness: intent

Status: **Intent, decided 2026-11-20 with the user; designed in [CONTROL-API-DESIGN.md](CONTROL-API-DESIGN.md); its query operations, actions, sealed results, four eyes, break-glass, pause, resume and drain built (C2–C6, 2026-11-21), the rest not** · Companion to [HIPAA.md](HIPAA.md) §11,
which lists this as outstanding work, and to DESIGN.md §11.4, the operator tools it replaces as the supported
interface.

## What is decided

1. **A web console with role-based access control is a later, separate project.** It isn't part of this work.
2. **What the product must have now is a supported control API:** HTTPS, authenticated, authorized per operation,
   and audited. It gives full control of a pipeline and of error remediation, so that:
   - customers' operators and their own tools can run the pipeline compliantly without the console;
   - the console, when it comes, is a client of the API and adds no control of its own.
3. **A test harness** drives the API and the pipeline's operation, with synthetic data only, at three levels:
   - a Testcontainers suite on the sdh-it VM;
   - scenarios on the minikube sandbox;
   - scenarios on the AWS PoC.

   The same scenarios run at each level.

Today's operator tools are a CLI run through `kubectl exec` (DESIGN.md §11.4). The operator's name is
self-asserted, so the proof of who acted is the platform's audit of `exec` (HIPAA.md §8.1 item 3). The API makes
the caller's identity the product's own record.

## The API, as decided

- **Authentication:** every call carries the caller's credential from the customer's identity provider: an OIDC
  bearer token checked for issuer, audience and expiry, or a client certificate (mTLS). The landing node's HTTP push
  already checks both (DESIGN.md §5.3.15). The authenticated principal, not a name given by the caller, is
  recorded in every audit event (`sdh.operator`).
- **Authorization: a permission per operation.** Each operation names the permission it needs, and the API checks
  it against the roles or scopes in the caller's token, mapped by the customer's settings. The console later adds
  the user interface and role management, on the same permissions. A first set:

  | Permission | Operations |
  |---|---|
  | `pipeline.view` | status, stuck items, drain check, versions, health, settings with secrets removed, the compliance outcome, failures listed (counts and codes, no content) |
  | `pipeline.operate` | pause, resume, drain; reprocess; failures retried, corrected, declined or released; resubmit |
  | `phi.export` | failures exported, patients looked up, items restored: each **sealed** to the caller's registered key |
  | `phi.break-glass` | restore in plaintext, with a ticket (HIPAA-OPERATIONS.md §5) |
  | `audit.read` | audit verify, audit export |
  | `audit.admin` | audit prune |
  | `keys.admin` | rotate a KEK for new data; re-key stored data (HIPAA.md §8.1 item 8) |
  | `data.admin` | set and run retention; purge an item; erase a pipeline's data at the end of a contract |

- **Full control and remediation:** every operator command of DESIGN.md §11.4, on every stage, the transform stage
  included, and also:
  - pause, resume and drain of a stage or an intake;
  - KEK rotation and re-keying;
  - retention and erasure;
  - a view of each stage's settings (secrets removed), its health, versions and compliance outcome, which is also
    the support bundle (HIPAA.md §8.1 item 11).
- **What it never does:**
  - return PHI in the clear, except through `phi.break-glass`;
  - put PHI in a URL, a log, an error or an audit event (HIPAA.md §4.1);
  - drop or replace data: remediation links, as resubmission does (DESIGN.md §5.3.8).
- **Contract:** a versioned OpenAPI description, with results as structured rows (no PHI), as the CLI's commands
  are already shaped (DESIGN.md §11.4, "A web console, later").
- **Exposure:** inside the customer's network only, behind its own ingress with TLS. The customer's network
  architecture (HIPAA.md §11.2) says where.

**Decided in its design** (2026-11-20, [CONTROL-API-DESIGN.md](CONTROL-API-DESIGN.md)):
- the API runs as a control Deployment per stage, from the stage's own image and identity;
- a sealed result is streamed as JSON lines of JWE parts, to a key registered for the caller in advance;
- erasure, plaintext break-glass, re-keying and audit prune need a second person's approval;
- the CLI is kept beside the API, on the same operations, for when the API can't be reached.

## The test harness, as decided

One set of scenarios, written once, run at three levels:

| Level | Where | For |
|---|---|---|
| Testcontainers | the sdh-it VM (stopped after each run) | every scenario, on each change: real Kafka (SASL_SSL), Postgres (TLS), object store and key service, as the security ITs run them |
| Sandbox | minikube (`scripts/sandbox.sh`) | the real Deployments, network policies, failover and node loss |
| AWS | the PoC stack (`deploy/aws`) | scale, Spot loss, and the cloud's KMS, IAM and S3 |

Scenarios, each driven through the API and checked by its results and its audit:
- **Load and complete:** synthetic documents end to end, every item complete exactly once.
- **Failures and remediation:** malformed documents fail; they are listed, exported sealed, corrected or retried,
  and declined; a resubmission links to what it replaces.
- **Outages:** a broker, database, store or key service is cut off and comes back; stuck items are found and
  reprocessed; nothing is lost.
- **Pause, drain and resume.**
- **Keys:** a KEK rotated; stored data re-keyed and still readable; the old key retired.
- **Retention and erasure:** an item purged; a pipeline erased; nothing left readable.
- **Access:** each operation refused without its permission, and refused without a valid credential; every call
  audited with its principal; `audit-verify` clean afterwards.
- **Break-glass:** a plaintext restore needs its permission and a ticket, and both are audited.

The harness also gives the test team a way to operate a pipeline for testing: to load synthetic data, inject
faults and inspect the results without PHI.
