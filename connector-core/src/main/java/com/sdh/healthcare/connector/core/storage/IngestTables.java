package com.sdh.healthcare.connector.core.storage;

import com.sdh.healthcare.connector.contract.table.SectionsTable;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;

/** Creates the ingest tables with the properties the design relies on. */
public final class IngestTables {

    /**
     * Table properties for every ingest table (design §8.2, §16.5).
     *
     * <p>Iceberg sets the Parquet codec per table, not per column, so ciphertext is zstd-compressed
     * like everything else. On incompressible pages that costs a little CPU and almost no space.
     */
    public static final Map<String, String> PROPERTIES = Map.of(
            TableProperties.FORMAT_VERSION, "2",
            TableProperties.PARQUET_COMPRESSION, "zstd",
            TableProperties.PARQUET_COMPRESSION_LEVEL, "1",
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(128L * 1024 * 1024),
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, Long.toString(512L * 1024 * 1024),
            TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true",
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "100");

    /**
     * Writer settings applied on top of the table's properties by every Parquet writer here (design §13.1). Set at
     * write time rather than as table properties, so tables created before them get them too.
     *
     * <p>Check the row group's buffered size after every row, not after Iceberg's default of 100. A row here can be
     * an 8 MiB ciphertext chunk, so 100 rows is about 800 MiB held in memory before the 128 MiB row-group limit is
     * even checked; the archive ran out of heap at 768 MiB. After the first check, Iceberg estimates when the next
     * one is due from the average row size, so small rows are still checked rarely.
     */
    public static final Map<String, String> WRITE_OVERRIDES =
            Map.of(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1");

    private IngestTables() {}

    public static Table sections(Catalog catalog, String namespace) {
        return create(catalog, namespace, SectionsTable.DEFAULT_NAME, SectionsTable.SCHEMA, SectionsTable.SPEC,
                SectionsTable.SORT_ORDER);
    }

    public static Table sourceFiles(Catalog catalog, String namespace) {
        Schema s = ArchiveTables.SOURCE_FILES_SCHEMA;
        return create(catalog, namespace, ArchiveTables.SOURCE_FILES, s, ArchiveTables.spec(s),
                ArchiveTables.sortOrder(s));
    }

    public static Table failedFiles(Catalog catalog, String namespace) {
        Schema s = ArchiveTables.FAILED_FILES_SCHEMA;
        return create(catalog, namespace, ArchiveTables.FAILED_FILES, s, ArchiveTables.spec(s),
                ArchiveTables.sortOrder(s));
    }

    private static Table create(Catalog catalog, String namespace, String name, Schema schema, PartitionSpec spec,
            SortOrder order) {
        Namespace ns = Namespace.of(namespace);
        if (catalog instanceof SupportsNamespaces sn && !sn.namespaceExists(ns)) {
            try {
                sn.createNamespace(ns);
            } catch (AlreadyExistsException ignored) {
                // created concurrently
            }
        }
        TableIdentifier id = TableIdentifier.of(ns, name);
        if (catalog.tableExists(id)) {
            return catalog.loadTable(id);
        }
        return catalog.buildTable(id, schema)
                .withPartitionSpec(spec)
                .withSortOrder(order)
                .withProperties(PROPERTIES)
                .create();
    }
}
