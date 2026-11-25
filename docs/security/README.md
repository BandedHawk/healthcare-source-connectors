# Security reference documents

The documents a customer needs to secure and operate a pipeline, beside the operating procedures in
[HIPAA-OPERATIONS.md](../HIPAA-OPERATIONS.md). The list, and what each must cover, is HIPAA.md §11.2; each is
written for a pipeline deployed from `deploy/reference/` with the `hipaa` compliance profile on, and names the code
and settings each statement comes from, so a reviewer can check it.

| # | Document | Status |
|---|---|---|
| D1 | [Reference network architecture](NETWORK.md) | written 2026-11-25; `NetworkGuideTest` checks it names every port setting and that its policies let in only the listeners |
| D2 | [Identity and access](IDENTITY.md) | written 2026-11-25; `IdentityGuideTest` checks it names every permission and control API operation |
| D3 | [Key management](KEYS.md) | written 2026-11-25; `KeyManagementGuideTest` checks it names every key setting |
| D4 | [Platform hardening baseline](HARDENING.md) | written 2026-11-25; `HardeningGuideTest` checks it names every JVM flag and that every kit Deployment has its pod settings |
| D5 | [Data flows and where PHI is](DATA-FLOWS.md) | written 2026-11-24 |
| D6 | [Security configuration guide](CONFIGURATION.md) | written 2026-11-25; `ConfigurationGuideTest` checks it names every compliance check |
| D7 | [Audit event catalogue](AUDIT-EVENTS.md) | written 2026-11-25; `AuditEventsCatalogueTest` checks it lists every event |
| D8 | Backup, restore and disaster recovery | in [HIPAA-OPERATIONS.md](../HIPAA-OPERATIONS.md) §7 |
| D13 | [Control API deployment and use](CONTROL-API-DEPLOYMENT.md) | written 2026-11-25; `ControlDeploymentGuideTest` checks it describes every control API setting |
| D9 | [Retention and destruction](RETENTION.md) | written 2026-11-25; `RetentionGuideTest` checks it names every retention setting, data and audit admin command, and erasure certificate claim |
| D12 | [Upgrades and patching](UPGRADES.md) | written 2026-11-25; `UpgradeGuideTest` checks it names every way a release is let in, that the kits' stage Deployments add a pod first, and that releases only add to their tables |
| D10 | [The clearinghouse question and Part 2](CLEARINGHOUSE-PART2.md) | **draft** 2026-11-25: open questions C1–C5, P1–P3 for counsel and the product owner (§5 there) |
| D14 | [Payer data exchange: CARIN Blue Button, CARIN Digital Insurance Card and the CMS APIs](PAYER-DATA-EXCHANGE.md) | **draft** 2026-11-25: open questions Q1–Q6 and V11–V13 for counsel and the product owner (§8 there) |
| D11 | [The vendor's security commitments](VENDOR-COMMITMENTS.md) | **draft** 2026-11-25: open questions V1–V10 for the product owner and counsel (§4 there) |

**Internal:** [KNOWN-GAPS.md](KNOWN-GAPS.md) gathers every gap these documents found, by what each waits on:
review of legal standing and the vendor's commitments, the production build pipeline (the project is a PoC,
outside the standard process and its static and dynamic scans), the managed service agreements, product work, or
tests. The documents are written for a customer that operates the product; the vendor will also offer managed
services using this pipeline, under separate agreements, and these documents are meant to shape that offering and
its customer documentation as well.
