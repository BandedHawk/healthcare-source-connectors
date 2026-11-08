/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.admin;

import com.sdh.healthcare.connector.broker.send.ClosablePublisher;
import com.sdh.healthcare.connector.broker.send.Publishers;
import com.sdh.healthcare.connector.broker.send.QueueType;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.intake.ResubmitRequests;
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
 * What the landing node's admin commands use, each part opened only when a
 * command needs it. Nothing is written to the tables, and no lease is
 * taken, so the commands are safe beside the running node: tables are
 * loaded, never created. Unlike the stages' commands, {@code replay} sends
 * to the node's queue, so the publisher is here too (design §11.4), and
 * {@code resubmit} leaves a request in the lease database for the node to
 * carry out (STAGES.md §13.5).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class LandingAdminContext implements AutoCloseable
{

    /** The node's settings. */
    private final LandingProperties properties;

    /** The keys, once loaded. */
    private KeyMaterial keys;

    /** The landing catalog, once built. */
    private Catalog catalog;

    /** The Camel context the publisher sends through, once started. */
    private CamelContext camel;

    /** The publisher to the node's queue, once created. */
    private ClosablePublisher publisher;

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
     * Creates the context with a publisher of the caller's, for tests.
     *
     * @param properties the node's settings
     * @param publisher  sends the commands' messages
     */
    LandingAdminContext(LandingProperties properties,
            ClosablePublisher publisher)
    {
        this.properties = properties;
        this.publisher = publisher;
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
     * The keys the node seals its messages with, loaded on first use.
     *
     * @return the keys
     * @throws Exception if they can't be loaded
     */
    KeyMaterial keys() throws Exception
    {
        if (keys == null)
        {
            keys = KeyMaterial.forLanding(properties.crypto(),
                    KeyMaterial.Providers.DEFAULT);
        }
        return keys;
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
     * The publisher to the node's queue, created on first use, as the node
     * creates its own.
     *
     * @return the publisher
     * @throws Exception if the Camel context can't start
     */
    ClosablePublisher publisher() throws Exception
    {
        if (publisher == null)
        {
            QueueType type = QueueType.of(properties.queue().type());
            camel = new DefaultCamelContext();
            Publishers.configureBroker(camel, type,
                    properties.queue().artemis().brokerUrl(),
                    properties.queue().artemis().username(),
                    properties.queue().artemis().password());
            camel.start();
            publisher = Publishers.create(camel, type,
                    type.effectiveUri(properties.queue().uri()));
        }
        return publisher;
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
        database.close();
        try
        {
            if (publisher != null)
            {
                publisher.close();
            }
        }
        finally
        {
            try
            {
                if (camel != null)
                {
                    camel.close();
                }
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
}
