/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.maintenance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * A restore's look at a table (DESIGN.md §18.4, §18.5): what the restored
 * catalog's metadata needs that the store no longer has, bringing that
 * back from the store's versions, the newer metadata the landing node is
 * brought forward to, and the files nothing references.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class TableRestoreTest
{

    /** The table's columns. */
    static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));

    /** The table. */
    static final TableIdentifier ID = TableIdentifier.of("ns", "records");

    /** Creates the tests; JUnit runs each test on a new instance. */
    TableRestoreTest()
    {
    }

    /**
     * Restored to an earlier point, the catalog points at metadata whose
     * files maintenance has deleted since: a check names them and changes
     * nothing; a repair brings them back from the store's versions; then
     * the table is whole, and the files written since are orphans.
     */
    @Test
    void aRepairBringsBackWhatTheRestoredMetadataNeeds()
    {
        JdbcCatalog catalog = catalog(VersionedFileIO.class);
        Table table = catalog.createTable(ID, SCHEMA,
                PartitionSpec.unpartitioned());
        DataFile first = append(table, "a");
        String restorePoint = current(table);
        String firstList = table.currentSnapshot().manifestListLocation();
        DataFile second = append(table, "b");
        // Maintenance after the point: the first file and list removed.
        table.io().deleteFile(first.location());
        table.io().deleteFile(firstList);

        restore(catalog, restorePoint);
        Table restored = catalog.loadTable(ID);
        TableRestore.Walk checked = TableRestore.walk(restored, false);
        // The list is missing, so its manifests' files can't be looked at.
        assertThat(checked.missing()).containsExactly(firstList);
        assertThat(checked.recovered()).isEmpty();
        assertThat(checked.complete()).isFalse();
        assertThat(checked.whole()).isFalse();
        assertThat(table.io().newInputFile(first.location()).exists())
                .isFalse();

        TableRestore.Walk repaired = TableRestore.walk(restored, true);
        assertThat(repaired.recovered()).containsExactlyInAnyOrder(
                first.location(), firstList);
        assertThat(repaired.missing()).isEmpty();
        assertThat(repaired.whole()).isTrue();
        assertThat(repaired.orphans().listed()).isTrue();
        assertThat(repaired.orphans().count()).isGreaterThanOrEqualTo(1);

        TableRestore.Walk after = TableRestore.walk(restored, false);
        assertThat(after.whole()).isTrue();
        assertThat(after.referenced()).isEqualTo(repaired.referenced());
        assertThat(restored.io().newInputFile(second.location()).exists())
                .isTrue();
    }

    /**
     * Where the store keeps no versions, or a file's versions have expired,
     * a repair can't bring it back: it stays missing, and the table isn't
     * whole.
     */
    @Test
    void whatTheStoreNoLongerHasStaysMissing()
    {
        JdbcCatalog catalog = catalog(VersionedFileIO.class);
        Table table = catalog.createTable(ID, SCHEMA,
                PartitionSpec.unpartitioned());
        DataFile first = append(table, "a");
        String restorePoint = current(table);
        append(table, "b");
        VersionedFileIO.expire(first.location());

        restore(catalog, restorePoint);
        TableRestore.Walk repaired = TableRestore.walk(catalog.loadTable(ID),
                true);
        assertThat(repaired.missing()).containsExactly(first.location());
        assertThat(repaired.whole()).isFalse();

        JdbcCatalog plain = catalog(ListingFileIO.class);
        Table other = plain.createTable(ID, SCHEMA,
                PartitionSpec.unpartitioned());
        DataFile gone = append(other, "a");
        other.io().deleteFile(gone.location());
        assertThat(TableRestore.walk(other, true).missing())
                .containsExactly(gone.location());
    }

    /**
     * A store that can't list still has its referenced files checked, but
     * its orphans aren't counted, and no newer metadata can be found.
     */
    @Test
    void aStoreThatCantListCountsNoOrphans()
    {
        JdbcCatalog catalog = catalog(InMemoryFileIO.class);
        Table table = catalog.createTable(ID, SCHEMA,
                PartitionSpec.unpartitioned());
        append(table, "a");
        TableRestore.Walk walk = TableRestore.walk(table, false);
        assertThat(walk.whole()).isTrue();
        assertThat(walk.orphans().listed()).isFalse();
        assertThatThrownBy(() -> TableRestore.newest(table))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * The landing node's table restored to an earlier point is brought
     * forward to its newest metadata, with the lease epoch it records; a
     * metadata file of another table in the location is not a candidate;
     * two files of the newest version are rivals, left to an operator.
     */
    @Test
    void theNewestMetadataOfTheSameTableIsFound()
    {
        JdbcCatalog catalog = catalog(VersionedFileIO.class);
        Table table = catalog.createTable(ID, SCHEMA,
                PartitionSpec.unpartitioned());
        append(table, "a");
        String restorePoint = current(table);
        append(table, "b");
        table.updateProperties().set(FencedCommitter.EPOCH_PROPERTY, "7")
                .commit();
        String tip = current(table);

        restore(catalog, restorePoint);
        Table restored = catalog.loadTable(ID);
        TableRestore.Forward forward = TableRestore.newest(restored);
        assertThat(forward.from()).isEqualTo(restorePoint);
        assertThat(forward.to()).contains(tip);
        assertThat(forward.rivals()).isEmpty();
        assertThat(forward.epoch()).isEqualTo(7);

        Table moved = TableRestore.pointAt(catalog, ID, tip);
        assertThat(current(moved)).isEqualTo(tip);
        assertThat(TableRestore.newest(moved).to()).isEmpty();

        // Another table's metadata, newer by its name, is passed over.
        String location = moved.location() + "/metadata/";
        long version = TableRestore.version(tip).orElseThrow();
        copy(moved, foreign(), location + String.format("%05d-%s"
                + ".metadata.json", version + 5, UUID.randomUUID()));
        assertThat(TableRestore.newest(moved).to()).isEmpty();

        // Two of the newest version, from the same table: rivals.
        String one = location + String.format("%05d-%s.metadata.json",
                version + 1, UUID.randomUUID());
        String two = location + String.format("%05d-%s.metadata.json",
                version + 1, UUID.randomUUID());
        copy(moved, tip, one);
        copy(moved, tip, two);
        TableRestore.Forward rivals = TableRestore.newest(moved);
        assertThat(rivals.to()).isEmpty();
        assertThat(rivals.rivals()).containsExactlyInAnyOrder(one, two);
    }

    /**
     * A repair's hold on orphans is a table property, removed by a
     * release.
     */
    @Test
    void orphansAreKeptUntilReleased()
    {
        JdbcCatalog catalog = catalog(VersionedFileIO.class);
        Table table = catalog.createTable(ID, SCHEMA,
                PartitionSpec.unpartitioned());
        Instant at = Instant.parse("2026-11-24T10:15:30Z");
        TableRestore.keepOrphans(table, at);
        assertThat(catalog.loadTable(ID).properties())
                .containsEntry(TableRestore.KEEP_ORPHANS, at.toString());
        assertThat(TableRestore.release(catalog.loadTable(ID))).isTrue();
        assertThat(catalog.loadTable(ID).properties())
                .doesNotContainKey(TableRestore.KEEP_ORPHANS);
        assertThat(TableRestore.release(catalog.loadTable(ID))).isFalse();
    }

    /**
     * A metadata file's version is read from its name, compressed or not;
     * other names have none.
     */
    @Test
    void readsAMetadataFilesVersion()
    {
        String uuid = UUID.randomUUID().toString();
        assertThat(TableRestore.version("s3://b/t/metadata/00042-" + uuid
                + ".metadata.json")).contains(42L);
        assertThat(TableRestore.version("s3://b/t/metadata/00007-" + uuid
                + ".gz.metadata.json")).contains(7L);
        assertThat(TableRestore.version("s3://b/t/metadata/v3.metadata"
                + ".json")).isEmpty();
        assertThat(TableRestore.version("s3://b/t/metadata/snap-1-" + uuid
                + ".avro")).isEmpty();
    }

    /**
     * A key written with two slashes is matched to the one a store lists
     * with one; the scheme's are left.
     */
    @Test
    void comparesKeysWithTheirSlashesMadeOne()
    {
        assertThat(TableRestore.canonical("s3://b/t/data/0101//compact-1"
                + ".parquet")).isEqualTo("s3://b/t/data/0101/compact-1"
                        + ".parquet");
        assertThat(TableRestore.canonical("memory://wh/a///b"))
                .isEqualTo("memory://wh/a/b");
        assertThat(TableRestore.canonical("/local//x")).isEqualTo("/local/x");
    }

    /**
     * A catalog of its own over a store, on the shared test Postgres.
     *
     * @param io the store's class
     * @return it
     */
    static JdbcCatalog catalog(Class<?> io)
    {
        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("jdbc_" + UUID.randomUUID().toString()
                .replace("-", ""), Map.of(CatalogProperties.URI,
                        TestPostgres.jdbcUrl(),
                        CatalogProperties.WAREHOUSE_LOCATION,
                        "memory://wh-" + UUID.randomUUID(),
                        CatalogProperties.FILE_IO_IMPL, io.getName(),
                        "jdbc.schema-version", "V1"));
        catalog.createNamespace(Namespace.of("ns"));
        return catalog;
    }

    /**
     * Appends a data file, written to the store.
     *
     * @param table the table
     * @param name  the file's name
     * @return it
     */
    static DataFile append(Table table, String name)
    {
        String location = table.location() + "/data/" + name + "-"
                + UUID.randomUUID() + ".parquet";
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        try (OutputStream out = table.io().newOutputFile(location).create())
        {
            out.write(bytes);
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(location).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(bytes.length).withRecordCount(1)
                .build();
        table.newAppend().appendFile(file).commit();
        return file;
    }

    /**
     * The table's current metadata file.
     *
     * @param table the table
     * @return its location
     */
    static String current(Table table)
    {
        return ((HasTableOperations) table).operations().refresh()
                .metadataFileLocation();
    }

    /**
     * Points the catalog back at earlier metadata, as restoring its
     * database does.
     *
     * @param catalog  the catalog
     * @param metadata the earlier metadata
     */
    static void restore(JdbcCatalog catalog, String metadata)
    {
        catalog.dropTable(ID, false);
        catalog.registerTable(ID, metadata);
    }

    /**
     * Another table's metadata file, in a catalog of its own.
     *
     * @return its location
     */
    static String foreign()
    {
        JdbcCatalog other = catalog(VersionedFileIO.class);
        Table table = other.createTable(ID, SCHEMA,
                PartitionSpec.unpartitioned());
        append(table, "z");
        return current(table);
    }

    /**
     * Copies a file in the store.
     *
     * @param table the table whose store it is
     * @param from  the file
     * @param to    the copy
     */
    static void copy(Table table, String from, String to)
    {
        try (InputStream in = table.io().newInputFile(from).newStream();
                OutputStream out = table.io().newOutputFile(to).create())
        {
            in.transferTo(out);
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
    }
}
