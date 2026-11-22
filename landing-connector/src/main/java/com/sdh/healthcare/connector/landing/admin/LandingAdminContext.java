/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.admin;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.audit.AuditTables;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.intake.ResubmitRequests;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.ControlState;
import com.sdh.healthcare.connector.stage.PurgeRequests;
import com.sdh.healthcare.connector.stage.RetentionPolicy;
import com.sdh.healthcare.connector.stage.admin.AdminDatabase;
import com.sdh.healthcare.connector.stage.admin.DataAdmin;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * What the landing node's admin commands use, each part opened only when a
 * command needs it. Nothing is written to the tables, and no lease is
 * taken, so the commands are safe beside the running node: tables are
 * loaded, never created. {@code resubmit} leaves a request in the lease
 * database for the node to carry out (STAGES.md §13.5). No command sends
 * to a broker, so the admin needs no broker settings (STAGES.md §15.8.10
 * step 5); only the key commands load the node's keys, to check a KEK
 * rotated to (CONTROL-API-DESIGN.md §11), and the node's maintenance, not
 * the admin, carries out a re-key or a purge. Only {@code erase}
 * (CONTROL-API-DESIGN.md §11), once the node is paused, drops its tables.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class LandingAdminContext implements DataAdmin, AutoCloseable
{

    /** The node's settings. */
    private final LandingProperties properties;

    /** The landing catalog, once built. */
    private Catalog catalog;

    /** The lease database, opened for writing on first use. */
    private final AdminDatabase database;

    /** The node's keys, once loaded. */
    private KeyMaterial keys;

    /**
     * Creates the context; nothing is opened yet.
     *
     * @param properties the node's settings
     */
    LandingAdminContext(LandingProperties properties)
    {
        this.properties = properties;
        this.database = new AdminDatabase(properties.lease(), true);
    }

    /**
     * The node's settings.
     *
     * @return the settings
     */
    LandingProperties properties()
    {
        return properties;
    }

    /**
     * The landing catalog, built on first use.
     *
     * @return it
     * @throws IllegalArgumentException if no catalog is configured
     */
    private Catalog catalog()
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
     * The landing node's {@code landed_items}, which must exist.
     *
     * @return the table
     * @throws IllegalArgumentException if no catalog is configured
     * @throws IllegalStateException    if the table doesn't exist
     */
    Table landedItems()
    {
        try
        {
            return catalog().loadTable(TableIdentifier.of(
                    properties.storage().namespace(),
                    LandingTables.LANDED_ITEMS));
        }
        catch (NoSuchTableException missing)
        {
            throw new IllegalStateException("Table "
                    + LandingTables.LANDED_ITEMS
                    + " does not exist: the node has never run here");
        }
    }

    /**
     * The requests to land an item again, in the lease database: the
     * node creates the table at startup.
     *
     * @return the request store
     */
    ResubmitRequests resubmitRequests()
    {
        return new ResubmitRequests(database.dataSource(),
                properties.lease().jdbc().table(), Duration.ofSeconds(10));
    }

    /** Beside the lease table, under the node's chains' scope. */
    @Override
    public ControlState controlState(boolean change)
    {
        return new ControlState(database.dataSource(),
                AuditTables.besideLeaseTable(properties.lease().jdbc()
                        .table()).schema(),
                "landing/" + properties.connectorId());
    }

    /** The landed items KEK. */
    @Override
    public String settingKek()
    {
        return properties.crypto().keys().landedItemsKek();
    }

    @Override
    public KeyProvider keyProvider() throws Exception
    {
        return signingKeys().wrap();
    }

    /** The node's keys, loaded on first use, as the node loads them. */
    @Override
    public KeyMaterial signingKeys() throws Exception
    {
        if (keys == null)
        {
            keys = KeyMaterial.forLanding(properties.crypto(),
                    KeyMaterial.Providers.DEFAULT);
        }
        return keys;
    }

    /** {@code landed_items}' retention, within its bounds. */
    @Override
    public RetentionPolicy retentionPolicy()
    {
        LandingProperties.Maintenance maintenance = properties.maintenance();
        return RetentionPolicy.of(maintenance.retention().landedItems(),
                LandingProperties.Maintenance.MIN_RETENTION,
                maintenance.retentionBounds());
    }

    @Override
    public PurgeRequests purgeRequests(boolean change)
    {
        return PurgeRequests.of(database.dataSource(),
                AuditTables.besideLeaseTable(properties.lease().jdbc()
                        .table()).schema(), controlState(change));
    }

    /** {@code landed_items} and the ledger. */
    @Override
    public List<DataAdmin.Erasable> erasable()
    {
        String namespace = properties.storage().namespace();
        return List.of(new DataAdmin.Erasable(catalog(), TableIdentifier.of(
                namespace, LandingTables.LANDED_ITEMS)),
                new DataAdmin.Erasable(catalog(), TableIdentifier.of(
                        namespace, LandingTables.LEDGER)));
    }

    @Override
    public DataSource dataToChange()
    {
        return database.dataSource();
    }

    @Override
    public AuditTables auditTables()
    {
        return AuditTables.besideLeaseTable(properties.lease().jdbc()
                .table());
    }

    /** A landing node has no claims: its pause is enough. */
    @Override
    public boolean drains()
    {
        return false;
    }

    /** {@code landed_items}: the ledger holds no keys. */
    @Override
    public List<Table> keyTables()
    {
        try
        {
            return List.of(landedItems());
        }
        catch (IllegalStateException neverRun)
        {
            return List.of();
        }
    }

    @Override
    public DataSource keyDatabase()
    {
        return database.dataSource();
    }

    @Override
    public String stage()
    {
        return "landing";
    }

    @Override
    public void close() throws Exception
    {
        if (keys != null)
        {
            keys.close();
        }
        try
        {
            database.close();
        }
        finally
        {
            if (catalog instanceof AutoCloseable closeable)
            {
                closeable.close();
            }
        }
    }
}
