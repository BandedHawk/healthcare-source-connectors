/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.inbox.InboxTables;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
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
 * What the admin commands read from, opened only when a command needs it.
 * Strictly read-only: tables are loaded, never created; the ID key is never
 * created; the lease is inspected, never taken (design §11.4).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class AdminContext implements AutoCloseable
{

    /** The connector configuration. */
    private final ConnectorProperties properties;

    /** The KEKs, once loaded. */
    private KeyMaterial keys;

    /** The splitter's catalog, once built. */
    private Catalog catalog;

    /** The landing catalog, once built; may be {@link #catalog}. */
    private Catalog landingCatalog;

    /** The splitter's ID key, once loaded. */
    private RecordIds ids;

    /** The landing node's ID key, once loaded. */
    private RecordIds landingIds;

    /** The read-only lease database pool, once opened. */
    private HikariDataSource leaseDb;

    /**
     * Opens nothing yet.
     *
     * @param properties the connector configuration
     */
    AdminContext(ConnectorProperties properties)
    {
        this.properties = properties;
    }

    /**
     * The connector configuration.
     *
     * @return the configuration
     */
    ConnectorProperties properties()
    {
        return properties;
    }

    /**
     * The KEKs, loaded on first use.
     *
     * @return the key material
     * @throws Exception if the keys can't be loaded
     */
    KeyMaterial keys() throws Exception
    {
        if (keys == null)
        {
            keys = KeyMaterial.load(properties.crypto());
        }
        return keys;
    }

    /**
     * The splitter's catalog, built on first use.
     *
     * @return the catalog
     * @throws IllegalArgumentException if no catalog is configured
     */
    Catalog catalog()
    {
        if (catalog == null)
        {
            Map<String, String> settings = properties.storage().catalog();
            if (settings == null || settings.isEmpty())
            {
                throw new IllegalArgumentException(
                        "sdh.storage.catalog is required");
            }
            catalog = CatalogUtil.buildIcebergCatalog(
                    properties.storage().catalogName(), settings, null);
        }
        return catalog;
    }

    /**
     * The landing catalog: {@code sdh.landing.storage.catalog}, or the
     * splitter's own when that is unset.
     *
     * @return the catalog
     */
    Catalog landingCatalog()
    {
        if (landingCatalog == null)
        {
            Map<String, String> settings =
                    properties.landing().storage().catalog();
            landingCatalog = settings == null || settings.isEmpty()
                    ? catalog()
                    : CatalogUtil.buildIcebergCatalog(
                            properties.landing().storage().catalogName(),
                            settings, null);
        }
        return landingCatalog;
    }

    /**
     * A table in the splitter's namespace.
     *
     * @param name the table name
     * @return the table
     * @throws IllegalStateException if the table doesn't exist
     */
    Table table(String name)
    {
        return load(catalog(), properties.storage().namespace(), name);
    }

    /**
     * Loads a table, never creating it.
     *
     * @param catalog   the catalog
     * @param namespace the namespace
     * @param name      the table name
     * @return the table
     * @throws IllegalStateException if the table doesn't exist
     */
    private static Table load(Catalog catalog, String namespace, String name)
    {
        try
        {
            return catalog.loadTable(TableIdentifier.of(namespace, name));
        }
        catch (NoSuchTableException failure)
        {
            throw new IllegalStateException("Table " + name
                    + " does not exist: the connector has never run here");
        }
    }

    /**
     * The records table.
     *
     * @return the table
     */
    Table records()
    {
        return table(RecordsTable.DEFAULT_NAME);
    }

    /**
     * The inbox table.
     *
     * @return the table
     */
    Table inbox()
    {
        return table(InboxTables.INBOX);
    }

    /**
     * The landing node's landed items table.
     *
     * @return the table
     */
    Table landedItems()
    {
        return load(landingCatalog(), properties.landing().storage()
                .namespace(), LandingTables.LANDED_ITEMS);
    }

    /**
     * The landing node's ID key, for checking a landed item's digest;
     * unwrapped with the landed-items KEK.
     *
     * @return the ID key
     * @throws Exception if the key can't be loaded or unwrapped
     */
    RecordIds landingIds() throws Exception
    {
        if (landingIds == null)
        {
            landingIds = new RecordIds(
                    IdKeys.load(landedItems(), keys().wrap()));
        }
        return landingIds;
    }

    /**
     * The splitter's ID key.
     *
     * @return the ID key
     * @throws Exception if the key can't be loaded or unwrapped
     */
    RecordIds ids() throws Exception
    {
        if (ids == null)
        {
            ids = new RecordIds(IdKeys.load(records(), keys().wrap()));
        }
        return ids;
    }

    /**
     * The lease store, through a read-only pool of one connection.
     *
     * @return the lease store
     * @throws IllegalArgumentException if no lease database is configured
     */
    JdbcLeaseStore leaseStore()
    {
        if (leaseDb == null)
        {
            CommonSettings.Jdbc jdbc = properties.lease().jdbc();
            if (jdbc.url() == null || jdbc.url().isBlank())
            {
                throw new IllegalArgumentException(
                        "sdh.lease.jdbc.url is required");
            }
            HikariConfig pool = new HikariConfig();
            pool.setJdbcUrl(jdbc.url());
            pool.setUsername(jdbc.username());
            pool.setPassword(jdbc.password());
            pool.setMaximumPoolSize(1);
            pool.setConnectionTimeout(10_000);
            pool.setReadOnly(true);
            pool.setPoolName("sdh-admin");
            leaseDb = new HikariDataSource(pool);
        }
        return new JdbcLeaseStore(leaseDb, properties.lease().jdbc().table(),
                Duration.ofSeconds(10));
    }

    /** Closes whatever was opened, ignoring catalog close failures. */
    @Override
    public void close()
    {
        if (keys != null)
        {
            keys.close();
        }
        if (landingCatalog != catalog
                && landingCatalog instanceof AutoCloseable closeable)
        {
            try
            {
                closeable.close();
            }
            catch (Exception ignored)
            {
                // exiting
            }
        }
        if (catalog instanceof AutoCloseable closeable)
        {
            try
            {
                closeable.close();
            }
            catch (Exception ignored)
            {
                // exiting
            }
        }
        if (leaseDb != null)
        {
            leaseDb.close();
        }
    }
}
