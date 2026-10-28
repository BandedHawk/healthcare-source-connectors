package com.sdh.healthcare.connector.core.storage;

import com.sdh.healthcare.connector.contract.table.SectionsTable;
import java.util.Map;
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

    private IngestTables() {}

    public static Table sections(Catalog catalog, String namespace) {
        Namespace ns = Namespace.of(namespace);
        if (catalog instanceof SupportsNamespaces sn && !sn.namespaceExists(ns)) {
            try {
                sn.createNamespace(ns);
            } catch (AlreadyExistsException ignored) {
                // created concurrently
            }
        }
        TableIdentifier id = TableIdentifier.of(ns, SectionsTable.DEFAULT_NAME);
        if (catalog.tableExists(id)) {
            return catalog.loadTable(id);
        }
        return catalog.buildTable(id, SectionsTable.SCHEMA)
                .withPartitionSpec(SectionsTable.SPEC)
                .withSortOrder(SectionsTable.SORT_ORDER)
                .withProperties(PROPERTIES)
                .create();
    }
}
