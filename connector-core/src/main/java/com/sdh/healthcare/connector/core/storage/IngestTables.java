/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import com.sdh.healthcare.connector.contract.table.RecordsTable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;

/**
 * Creates the ingest tables with the properties the design relies on, or
 * loads them if they exist, adding any optional columns a later contract
 * brought.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class IngestTables
{

    /**
     * Table properties for every ingest table (design §8.2, §16.5).
     * <p>
     * Iceberg sets the Parquet codec per table, not per column, so ciphertext
     * is zstd-compressed like everything else. On incompressible pages that
     * costs a little CPU and almost no space.
     */
    public static final Map<String, String> PROPERTIES = Map.of(
            TableProperties.FORMAT_VERSION, "2",
            TableProperties.PARQUET_COMPRESSION, "zstd",
            TableProperties.PARQUET_COMPRESSION_LEVEL, "1",
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
            Long.toString(128L * 1024 * 1024),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
            Long.toString(512L * 1024 * 1024),
            TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true",
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "100");

    /**
     * Writer settings applied on top of the table's properties by every
     * Parquet writer here (design §13.1). They are set at write time rather
     * than as table properties, so tables created before them get them too.
     * <p>
     * Check the row group's buffered size after every row, not after
     * Iceberg's default of 100. A row here can be an 8 MiB ciphertext chunk,
     * so 100 rows is about 800 MiB held in memory before the 128 MiB
     * row-group limit is even checked; the archive ran out of heap at
     * 768 MiB. After the first check, Iceberg estimates when the next one is
     * due from the average row size, so small rows are still checked rarely.
     */
    public static final Map<String, String> WRITE_OVERRIDES = Map.of(
            TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1");

    /** The split stage's table of records as cut (STAGES.md §3). */
    public static final String SPLIT_RECORDS = "split_records";

    /**
     * Commit retries for a table many stage workers append to at once
     * (STAGES.md §7): each conflict costs Iceberg one retry, and with many
     * pods committing, the default of four runs out.
     */
    static final String SHARED_COMMIT_RETRIES = "20";

    /** Not instantiated: this class only has static methods. */
    private IngestTables()
    {
    }

    /**
     * Creates or loads the {@code records} table.
     *
     * @param catalog   the Iceberg catalog
     * @param namespace the namespace the table is in, created if missing
     * @return the table
     */
    public static Table records(Catalog catalog, String namespace)
    {
        return create(catalog, namespace, RecordsTable.DEFAULT_NAME,
                RecordsTable.SCHEMA, RecordsTable.SPEC,
                RecordsTable.SORT_ORDER);
    }

    /**
     * Creates or loads the split stage's {@code split_records} table: the
     * {@code records} columns, with more commit retries, since every split
     * worker appends to it (STAGES.md §8).
     *
     * @param catalog   the Iceberg catalog
     * @param namespace the namespace the table is in, created if missing
     * @return the table
     */
    public static Table splitRecords(Catalog catalog, String namespace)
    {
        Map<String, String> properties = new HashMap<>(PROPERTIES);
        properties.put(TableProperties.COMMIT_NUM_RETRIES,
                SHARED_COMMIT_RETRIES);
        return create(catalog, namespace, SPLIT_RECORDS, RecordsTable.SCHEMA,
                RecordsTable.SPEC, RecordsTable.SORT_ORDER, properties);
    }

    /**
     * Creates or loads the {@code records} table that every convert worker
     * appends to (STAGES.md §9): the usual columns, with more commit
     * retries, as for {@code split_records}.
     *
     * @param catalog   the Iceberg catalog
     * @param namespace the namespace the table is in, created if missing
     * @return the table
     */
    public static Table sharedRecords(Catalog catalog, String namespace)
    {
        return shared(catalog, namespace, RecordsTable.DEFAULT_NAME);
    }

    /**
     * Creates or loads a table of records that every worker of a stage
     * appends to, such as a record stage's sink (STAGES.md §17.7): the
     * {@code records} columns, with more commit retries, as for
     * {@code split_records}.
     *
     * @param catalog   the Iceberg catalog
     * @param namespace the namespace the table is in, created if missing
     * @param name      the table's name
     * @return the table
     */
    public static Table shared(Catalog catalog, String namespace,
            String name)
    {
        Map<String, String> properties = new HashMap<>(PROPERTIES);
        properties.put(TableProperties.COMMIT_NUM_RETRIES,
                SHARED_COMMIT_RETRIES);
        return create(catalog, namespace, name, RecordsTable.SCHEMA,
                RecordsTable.SPEC, RecordsTable.SORT_ORDER, properties);
    }

    /**
     * Adds to an existing table the optional columns of a schema it doesn't
     * have yet, so a table made by an earlier version takes the columns a
     * later contract adds (such as the line and column ones of 2.1). Rows
     * written before read them as null. Required columns are never added: a
     * contract adds only optional ones.
     * <p>
     * Two connectors starting together may both try; the one that loses the
     * commit reloads the table and finds the columns there. It tries three
     * times in all.
     *
     * @param table  the existing table
     * @param schema the schema it should have
     * @return the same table, with the columns added
     * @throws CommitFailedException if the third attempt fails too
     */
    static Table addMissingColumns(Table table, Schema schema)
    {
        for (int attempt = 0; ; attempt++)
        {
            List<Types.NestedField> missing = new ArrayList<>();
            for (Types.NestedField field : schema.columns())
            {
                if (field.isOptional()
                        && table.schema().findField(field.name()) == null)
                {
                    missing.add(field);
                }
            }
            if (missing.isEmpty())
            {
                return table;
            }
            UpdateSchema update = table.updateSchema();
            for (Types.NestedField field : missing)
            {
                update.addColumn(field.name(), field.type(), field.doc());
            }
            try
            {
                update.commit();
                return table;
            }
            catch (CommitFailedException conflict)
            {
                if (attempt >= 2)
                {
                    throw conflict;
                }
                table.refresh();
            }
        }
    }

    /**
     * Creates a table, and its namespace if needed, or loads it if it exists
     * and adds any optional columns it lacks. Every table is given the
     * {@link #PROPERTIES} the design relies on. The landing tables are
     * created through this too (design §5.3.2).
     *
     * @param catalog   the Iceberg catalog
     * @param namespace the namespace the table is in
     * @param name      the table's name
     * @param schema    its columns
     * @param spec      its partitioning, for a new table
     * @param order     its sort order, for a new table
     * @return the table
     */
    public static Table create(Catalog catalog, String namespace,
            String name, Schema schema, PartitionSpec spec, SortOrder order)
    {
        return create(catalog, namespace, name, schema, spec, order,
                PROPERTIES);
    }

    /**
     * Creates or loads a table with the given properties for a new one.
     * Of workers creating it at once, the losers load the winner's.
     *
     * @param catalog    the Iceberg catalog
     * @param namespace  the namespace the table is in
     * @param name       the table's name
     * @param schema     its columns
     * @param spec       its partitioning, for a new table
     * @param order      its sort order, for a new table
     * @param properties its properties, for a new table
     * @return the table
     */
    static Table create(Catalog catalog, String namespace, String name,
            Schema schema, PartitionSpec spec, SortOrder order,
            Map<String, String> properties)
    {
        Namespace tableNamespace = Namespace.of(namespace);
        if (catalog instanceof SupportsNamespaces namespaces
                && !namespaces.namespaceExists(tableNamespace))
        {
            try
            {
                namespaces.createNamespace(tableNamespace);
            }
            catch (AlreadyExistsException ignored)
            {
                // Created concurrently.
            }
        }
        TableIdentifier id = TableIdentifier.of(tableNamespace, name);
        if (catalog.tableExists(id))
        {
            return addMissingColumns(catalog.loadTable(id), schema);
        }
        try
        {
            return catalog.buildTable(id, schema)
                    .withPartitionSpec(spec)
                    .withSortOrder(order)
                    .withProperties(properties)
                    .create();
        }
        catch (AlreadyExistsException created)
        {
            // Another worker created it just now.
            return addMissingColumns(catalog.loadTable(id), schema);
        }
    }
}
