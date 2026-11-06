/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.split;

import com.sdh.healthcare.connector.core.landing.LandingTables;
import java.io.IOException;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * The landing node's {@value LandingTables#LANDED_ITEMS} table, loaded
 * when it is first needed. The splitter only reads it, and never creates
 * it: it may start before the landing node has, and an item can only
 * arrive once the table exists.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LandedItemsTable
{

    /** The landing catalog. */
    private final Catalog catalog;

    /** The table's name in the catalog. */
    private final TableIdentifier identifier;

    /** The table, once loaded. */
    private Table table;

    /**
     * Creates the holder.
     *
     * @param catalog   the landing catalog
     * @param namespace the landing namespace
     */
    public LandedItemsTable(Catalog catalog, String namespace)
    {
        this.catalog = catalog;
        this.identifier = TableIdentifier.of(Namespace.of(namespace),
                LandingTables.LANDED_ITEMS);
    }

    /**
     * The table, loading it the first time.
     *
     * @return the table
     * @throws IOException if it doesn't exist yet; trying again later may
     *                     succeed
     */
    public synchronized Table get() throws IOException
    {
        if (table == null)
        {
            try
            {
                table = catalog.loadTable(identifier);
            }
            catch (NoSuchTableException missing)
            {
                throw new IOException("Landing table doesn't exist yet",
                        missing);
            }
        }
        return table;
    }
}
