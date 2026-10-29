package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.contract.table.SectionsTable;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.storage.ArchiveTables;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.config.KeyMaterial;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * What the admin commands read from, opened only when a command needs it. Strictly read-only: tables are loaded,
 * never created; the ID key is never created; the lease is inspected, never taken (design §11.4).
 */
final class AdminContext implements AutoCloseable {
    private final ConnectorProperties props;
    private KeyMaterial keys;
    private Catalog catalog;
    private SectionIds ids;
    private HikariDataSource leaseDb;

    AdminContext(ConnectorProperties props) {
        this.props = props;
    }

    ConnectorProperties props() {
        return props;
    }

    KeyMaterial keys() throws Exception {
        if (keys == null) {
            keys = KeyMaterial.load(props.crypto());
        }
        return keys;
    }

    Catalog catalog() {
        if (catalog == null) {
            Map<String, String> c = props.storage().catalog();
            if (c == null || c.isEmpty()) {
                throw new IllegalArgumentException("sdh.storage.catalog is required");
            }
            catalog = CatalogUtil.buildIcebergCatalog(props.storage().catalogName(), c, null);
        }
        return catalog;
    }

    Table table(String name) {
        try {
            return catalog().loadTable(TableIdentifier.of(props.storage().namespace(), name));
        } catch (NoSuchTableException e) {
            throw new IllegalStateException("Table " + name + " does not exist: the connector has never run here");
        }
    }

    Table sections() {
        return table(SectionsTable.DEFAULT_NAME);
    }

    Table sourceFiles() {
        return table(ArchiveTables.SOURCE_FILES);
    }

    Table failedFiles() {
        return table(ArchiveTables.FAILED_FILES);
    }

    SectionIds ids() throws Exception {
        if (ids == null) {
            ids = new SectionIds(IdKeys.load(sections(), keys().wrap()));
        }
        return ids;
    }

    JdbcLeaseStore leaseStore() {
        if (leaseDb == null) {
            ConnectorProperties.Jdbc jdbc = props.lease().jdbc();
            if (jdbc.url() == null || jdbc.url().isBlank()) {
                throw new IllegalArgumentException("sdh.lease.jdbc.url is required");
            }
            HikariConfig c = new HikariConfig();
            c.setJdbcUrl(jdbc.url());
            c.setUsername(jdbc.username());
            c.setPassword(jdbc.password());
            c.setMaximumPoolSize(1);
            c.setConnectionTimeout(10_000);
            c.setReadOnly(true);
            c.setPoolName("sdh-admin");
            leaseDb = new HikariDataSource(c);
        }
        return new JdbcLeaseStore(leaseDb, props.lease().jdbc().table(), Duration.ofSeconds(10));
    }

    @Override
    public void close() {
        if (keys != null) {
            keys.close();
        }
        if (catalog instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception ignored) {
                // exiting
            }
        }
        if (leaseDb != null) {
            leaseDb.close();
        }
    }
}
