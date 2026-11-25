# Security reference documents

The documents a customer needs to secure and operate a pipeline, beside the operating procedures in
[HIPAA-OPERATIONS.md](../HIPAA-OPERATIONS.md). The list, and what each must cover, is HIPAA.md §11.2; each is
written for a pipeline deployed from `deploy/reference/` with the `hipaa` compliance profile on, and names the code
and settings each statement comes from, so a reviewer can check it.

| # | Document | Status |
|---|---|---|
| D5 | [Data flows and where PHI is](DATA-FLOWS.md) | written 2026-11-24 |
| D6 | [Security configuration guide](CONFIGURATION.md) | written 2026-11-25; `ConfigurationGuideTest` checks it names every compliance check |
| D7 | [Audit event catalogue](AUDIT-EVENTS.md) | written 2026-11-25; `AuditEventsCatalogueTest` checks it lists every event |
| D8 | Backup, restore and disaster recovery | in [HIPAA-OPERATIONS.md](../HIPAA-OPERATIONS.md) §7 |
| D1–D4, D9–D13 | | owed (HIPAA.md §11.2) |
