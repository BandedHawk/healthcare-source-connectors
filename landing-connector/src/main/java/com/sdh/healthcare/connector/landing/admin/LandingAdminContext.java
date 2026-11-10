/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.admin;

import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.intake.ResubmitRequests;
import com.sdh.healthcare.connector.stage.admin.AdminDatabase;
import java.time.Duration;
import java.util.Map;
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
 * to a broker, so the admin needs no broker settings or keys (STAGES.md
 * §15.8.10 step 5).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class LandingAdminContext implements AutoCloseable
{

    /** The node's settings. */
    private final LandingProperties properties;

    /** The landing catalog, once built. */
    private Catalog catalog;

    /** The lease database, opened for writing on first use. */
    private final AdminDatabase database;

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
     * The landing node's {@code landed_items}, which must exist.
     *
     * @return the table
     * @throws IllegalArgumentException if no catalog is configured
     * @throws IllegalStateException    if the table doesn't exist
     */
    Table landedItems()
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
        try
        {
            return catalog.loadTable(TableIdentifier.of(
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

    @Override
    public void close() throws Exception
    {
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
