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
| D9–D12 | | owed (HIPAA.md §11.2) |
