/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.broker.send.ClosablePublisher;
import com.sdh.healthcare.connector.broker.send.Publishers;
import com.sdh.healthcare.connector.broker.send.QueueType;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointCodec;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.SplitItemReport;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.admin.AdminDatabase;
import java.time.Duration;
import java.util.Map;
import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * What the split stage's admin commands read from, each part opened only
 * when a command needs it. Strictly read-only: tables are loaded, never
 * created; ID keys are never created; leases are inspected, never taken;
 * the database connection is read-only (design §11.4).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class SplitAdminContext implements AutoCloseable
{

    /** The stage's settings. */
    private final ConnectorProperties properties;

    /** The lease database. */
    private final AdminDatabase database;

    /** The keys, once loaded. */
    private KeyMaterial keys;

    /** The stage's catalog, once built. */
    private Catalog catalog;

    /** Landing's catalog, once built; the stage's if it is the same. */
    private Catalog landingCatalog;

    /** The ID keys of {@code split_records}, once loaded. */
    private RecordIds splitIds;

    /** Landing's ID keys, once loaded. */
    private RecordIds landingIds;

    /** The Camel context the publisher sends through, once started. */
    private CamelContext camel;

    /** The publisher to the stage's output, once created. */
    private ClosablePublisher publisher;

    /**
     * Creates the context; nothing is opened yet.
     *
     * @param properties the stage's settings
     */
    SplitAdminContext(ConnectorProperties properties)
    {
        this.properties = properties;
        this.database = new AdminDatabase(properties.lease());
    }

    /**
     * Creates the context with a publisher of the caller's, for tests.
     *
     * @param properties the stage's settings
     * @param publisher  sends the commands' messages
     */
    SplitAdminContext(ConnectorProperties properties,
            ClosablePublisher publisher)
    {
        this(properties);
        this.publisher = publisher;
    }

    /**
     * The publisher to the stage's output ({@code sdh.stage.output}),
     * created on first use, as the stage creates its own: for
     * {@code replay}.
     *
     * @return the publisher
     * @throws Exception if the Camel context can't start
     */
    ClosablePublisher publisher() throws Exception
    {
        if (publisher == null)
        {
            CommonSettings.Queue output = properties.queue();
            if (output == null || output.uri() == null)
            {
                throw new IllegalArgumentException(
                        "sdh.stage.output is required");
            }
            QueueType type = QueueType.of(output.type());
            camel = new DefaultCamelContext();
            Publishers.configureBroker(camel, type,
                    output.artemis().brokerUrl(), output.artemis().username(),
                    output.artemis().password());
            camel.start();
            publisher = Publishers.create(camel, type,
                    type.effectiveUri(output.uri()));
        }
        return publisher;
    }

    /**
     * The stage's settings.
     *
     * @return the settings
     */
    ConnectorProperties properties()
    {
        return properties;
    }

    /**
     * The keys, loaded on first use.
     *
     * @return the keys
     * @throws Exception if they can't be loaded
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
     * The stage's catalog, built on first use.
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
     * Landing's catalog: its own if configured, else the stage's.
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
     * Loads a table, which must exist.
     *
     * @param catalog   the catalog
     * @param namespace the namespace
     * @param name      the table
     * @return the table
     * @throws IllegalStateException if it doesn't exist
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
                    + " does not exist: the stage has never run here");
        }
    }

    /**
     * The stage's {@code split_records}.
     *
     * @return the table
     */
    Table splitRecords()
    {
        return load(catalog(), properties.storage().namespace(),
                IngestTables.SPLIT_RECORDS);
    }

    /**
     * Landing's {@code landed_items}.
     *
     * @return the table
     */
    Table landedItems()
    {
        return load(landingCatalog(), properties.landing().storage()
                .namespace(), LandingTables.LANDED_ITEMS);
    }

    /**
     * Landing's ID keys, which name a landed item's chunks.
     *
     * @return the keys
     * @throws Exception if they can't be loaded
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
     * The checkpoints of the stage's items, to load: they are MAC'd with
     * the ID key of {@code split_records}.
     *
     * @return the checkpoints
     * @throws Exception if the ID key can't be loaded
     */
    ItemCheckpoints checkpoints() throws Exception
    {
        if (splitIds == null)
        {
            splitIds = new RecordIds(
                    IdKeys.load(splitRecords(), keys().wrap()));
        }
        return new ItemCheckpoints(database.dataSource(),
                properties.stage().registrySchema(),
                new CheckpointCodec(splitIds), Duration.ofSeconds(10));
    }

    /**
     * The stage's item rows, without their checkpoints: needs no keys.
     *
     * @return the report
     */
    SplitItemReport items()
    {
        return new SplitItemReport(database.dataSource(),
                properties.stage().registrySchema());
    }

    /**
     * The leases, to inspect.
     *
     * @return the lease store
     */
    JdbcLeaseStore leases()
    {
        return database.leases();
    }

    @Override
    public void close()
    {
        if (publisher != null)
        {
            try
            {
                publisher.close();
            }
            catch (Exception ignored)
            {
                // exiting
            }
        }
        closeQuietly(camel);
        if (keys != null)
        {
            keys.close();
        }
        if (landingCatalog != catalog)
        {
            closeQuietly(landingCatalog);
        }
        closeQuietly(catalog);
        database.close();
    }

    /**
     * Closes a catalog or Camel context, if it holds anything to close.
     *
     * @param catalog the catalog or context, or null
     */
    private static void closeQuietly(Object catalog)
    {
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
    }
}
