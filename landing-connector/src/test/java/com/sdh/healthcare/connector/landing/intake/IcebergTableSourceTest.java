/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

/**
 * Tests for {@link IcebergTableSource} on a real table: a JDBC catalog on
 * embedded Postgres, with files in memory.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@ExtendWith(OutputCaptureExtension.class)
class IcebergTableSourceTest
{

    /** The table's schema. */
    static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));

    /** The catalog's database. */
    static EmbeddedPostgres postgres;

    /** The catalog's properties. */
    Map<String, String> catalogProperties;

    /** The test's own catalog, to write the table. */
    Catalog catalog;

    /** The table. */
    Table table;

    /** The table's name. */
    TableIdentifier identifier;

    /** Progress, kept in memory. */
    Memory progress = new Memory();

    /** The sources made; closed after each test. */
    final List<IcebergTableSource> sources = new ArrayList<>();

    /** Progress kept in memory, as the ledger would keep it. */
    static final class Memory extends TableProgress
    {

        /** The saved mark. */
        TableProgress.Mark saved;

        /** Creates the store. */
        Memory()
        {
            super(null, null, null, "test");
        }

        @Override
        public Optional<Mark> load()
        {
            return Optional.ofNullable(saved);
        }

        @Override
        public void save(Mark mark)
        {
            saved = mark;
        }
    }

    /**
     * Starts the database.
     *
     * @throws Exception if it can't start
     */
    @BeforeAll
    static void startDatabase() throws Exception
    {
        postgres = EmbeddedPostgres.start();
    }

    /**
     * Stops the database.
     *
     * @throws Exception if it can't stop
     */
    @AfterAll
    static void stopDatabase() throws Exception
    {
        postgres.close();
    }

    /** Creates a table of its own for each test. */
    @BeforeEach
    void createTable()
    {
        catalogProperties = Map.of("type", "jdbc",
                "uri", postgres.getJdbcUrl("postgres", "postgres"),
                "warehouse", "memory://wh-" + UUID.randomUUID(),
                "io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO",
                "jdbc.user", "postgres", "jdbc.password", "postgres",
                "jdbc.schema-version", "V1");
        catalog = CatalogUtil.buildIcebergCatalog("source", catalogProperties,
                null);
        String namespace = "ns" + UUID.randomUUID().toString()
                .replace("-", "");
        ((SupportsNamespaces) catalog).createNamespace(Namespace.of(namespace));
        identifier = TableIdentifier.of(namespace, "claims");
        table = catalog.createTable(identifier, SCHEMA,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"));
    }

    /**
     * Closes the sources and the catalog.
     *
     * @throws Exception if the catalog can't be closed
     */
    @AfterEach
    void close() throws Exception
    {
        sources.forEach(IcebergTableSource::close);
        ((AutoCloseable) catalog).close();
    }

    /**
     * A source on the table.
     *
     * @param start {@code earliest} or {@code latest}
     * @param name  the table's name
     * @return the source, with the test's progress
     */
    IcebergTableSource source(String start, TableIdentifier name)
    {
        SourceSettings settings = SourceSettings.of(
                new LandingProperties.Source(null, "read-only", null, null,
                        ".landed", "settled", Duration.ofMinutes(2), "*",
                        false, Duration.ofSeconds(1), "iceberg",
                        Duration.ofMinutes(5), null, null, null,
                        new LandingProperties.Table(catalogProperties,
                                "source", name.toString(), start)));
        IcebergTableSource source = new IcebergTableSource("claims",
                settings);
        source.attach(progress);
        sources.add(source);
        return source;
    }

    /**
     * Writes a Parquet data file of rows.
     *
     * @param ids the rows' IDs
     * @return the file, not yet in the table
     * @throws Exception if it can't be written
     */
    DataFile file(long... ids) throws Exception
    {
        String path = table.location() + "/data/" + UUID.randomUUID()
                + ".parquet";
        FileAppender<Record> appender = Parquet.write(
                table.io().newOutputFile(path)).schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::create).build();
        try (appender)
        {
            for (long id : ids)
            {
                Record row = GenericRecord.create(SCHEMA);
                row.setField("id", id);
                appender.add(row);
            }
        }
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(appender.length())
                .withMetrics(appender.metrics()).build();
    }

    /**
     * Appends files in one snapshot.
     *
     * @param files the files
     */
    void append(DataFile... files)
    {
        var append = table.newAppend();
        for (DataFile file : files)
        {
            append.appendFile(file);
        }
        append.commit();
    }

    /**
     * Lists the source, and lands every item it gives.
     *
     * @param source the source
     * @return the references listed
     * @throws Exception if listing fails
     */
    static List<String> landAll(IcebergTableSource source) throws Exception
    {
        List<String> refs = new ArrayList<>();
        for (ListedItem item : source.list())
        {
            refs.add(item.ref());
            source.afterLanding(item);
        }
        return refs;
    }

    /**
     * On the first start with {@code earliest}, every file the table holds
     * is landed; then each append, and nothing for a compaction. The mark
     * moves only once a snapshot's files are landed.
     *
     * @throws Exception if the test fails
     */
    @Test
    void backfillsThenFollowsAppends() throws Exception
    {
        DataFile a = file(1, 2);
        DataFile b = file(3);
        append(a);
        append(b);
        IcebergTableSource source = source("earliest", identifier);
        List<ListedItem> items = source.list();
        assertThat(items).extracting(ListedItem::ref)
                .containsExactly(a.location(), b.location());
        assertThat(items.getFirst().tag())
                .isEqualTo(table.uuid().toString());
        assertThat(source.kind()).isEqualTo(ItemKind.TABLE_FILE);
        long current = table.currentSnapshot().snapshotId();
        assertThat(progress.saved)
                .isEqualTo(new TableProgress.Mark(current, true));
        source.afterLanding(items.get(0));
        assertThat(progress.saved.backfill()).isTrue();
        source.afterLanding(items.get(1));
        assertThat(progress.saved)
                .isEqualTo(new TableProgress.Mark(current, false));
        assertThat(source.list()).isEmpty();

        DataFile c = file(4);
        append(c);
        DataFile merged = file(1, 2, 3);
        table.newRewrite().deleteFile(a).deleteFile(b).addFile(merged)
                .commit();
        assertThat(landAll(source)).containsExactly(c.location());
        assertThat(progress.saved.snapshotId())
                .isEqualTo(table.currentSnapshot().snapshotId());
        assertThat(source.list()).isEmpty();
    }

    /**
     * An append's {@code sdh.replaces} property is given for each file it
     * added; a backfill and other appends give none.
     *
     * @throws Exception if the test fails
     */
    @Test
    void givesTheItemAnAppendNamesAsReplaced() throws Exception
    {
        DataFile a = file(1);
        table.newAppend().appendFile(a).set(
                IcebergTableSource.REPLACES_PROPERTY, "backfilled").commit();
        IcebergTableSource source = source("earliest", identifier);
        ListedItem first = source.list().getFirst();
        assertThat(source.replacesValue(first)).isEmpty();
        source.afterLanding(first);

        String earlier = UUID.randomUUID().toString();
        DataFile b = file(2);
        DataFile c = file(3);
        table.newAppend().appendFile(b).appendFile(c).set(
                IcebergTableSource.REPLACES_PROPERTY, earlier).commit();
        DataFile d = file(4);
        append(d);
        List<ListedItem> items = source.list();
        assertThat(items).extracting(source::replacesValue).containsExactly(
                Optional.of(earlier), Optional.of(earlier), Optional.empty());
        items.forEach(source::afterLanding);
        assertThat(source.replacesValue(items.getFirst())).isEmpty();
    }

    /**
     * With {@code latest}, what the table holds at the first start isn't
     * landed; later appends are.
     *
     * @throws Exception if the test fails
     */
    @Test
    void startsFromTheLatestSnapshot() throws Exception
    {
        append(file(1));
        IcebergTableSource source = source("latest", identifier);
        assertThat(source.list()).isEmpty();
        DataFile b = file(2);
        append(b);
        assertThat(landAll(source)).containsExactly(b.location());
    }

    /**
     * On a table empty at the first start, the first append is landed;
     * an overwrite or delete is reported and passed over; a restart
     * carries on from the mark; a rollback lands nothing.
     *
     * @param output what was logged
     * @throws Exception if the test fails
     */
    @Test
    void passesOverRewritesAndCarriesOnAfterARestart(CapturedOutput output)
            throws Exception
    {
        IcebergTableSource source = source("latest", identifier);
        assertThat(source.list()).isEmpty();
        assertThat(progress.saved.snapshotId())
                .isEqualTo(TableProgress.NONE);
        DataFile a = file(1);
        append(a);
        assertThat(landAll(source)).containsExactly(a.location());
        long landed = table.currentSnapshot().snapshotId();

        DataFile b = file(2);
        table.newOverwrite().deleteFile(a).addFile(b).commit();
        DataFile c = file(3);
        append(c);
        DataFile d = file(4);
        append(d);
        List<ListedItem> items = source.list();
        assertThat(items).extracting(ListedItem::ref)
                .containsExactly(c.location(), d.location());
        assertThat(output.getAll()).contains("intake.table_rewrite")
                .doesNotContain(b.location());
        source.afterLanding(items.getFirst());
        long afterC = progress.saved.snapshotId();
        assertThat(afterC).isNotEqualTo(landed);

        IcebergTableSource restarted = source("latest", identifier);
        assertThat(landAll(restarted)).containsExactly(d.location());
        assertThat(restarted.list()).isEmpty();

        table.manageSnapshots().rollbackTo(afterC).commit();
        assertThat(restarted.list()).isEmpty();
    }

    /**
     * An item reads from an offset; a data file deleted before it is
     * landed is reported changed and isn't offered again.
     *
     * @throws Exception if the test fails
     */
    @Test
    void readsFromAnOffsetAndPassesOverAMissingFile() throws Exception
    {
        IcebergTableSource source = source("latest", identifier);
        assertThat(source.list()).isEmpty();
        DataFile a = file(1, 2, 3);
        DataFile b = file(4);
        append(a, b);
        List<ListedItem> items = source.list();
        ListedItem itemA = items.stream()
                .filter(item -> item.ref().equals(a.location()))
                .findFirst().orElseThrow();
        ListedItem itemB = items.stream()
                .filter(item -> item.ref().equals(b.location()))
                .findFirst().orElseThrow();
        byte[] whole;
        try (InputStream input = table.io().newInputFile(a.location())
                .newStream())
        {
            whole = input.readAllBytes();
        }
        try (InputStream input = source.open(itemA, 4))
        {
            assertThat(input.readAllBytes()).isEqualTo(
                    Arrays.copyOfRange(whole, 4, whole.length));
        }
        table.io().deleteFile(b.location());
        assertThatThrownBy(() -> source.open(itemB, 0))
                .isInstanceOf(ItemLander.ItemChangedException.class);
        source.afterLanding(itemA);
        assertThat(source.list()).isEmpty();
        assertThat(progress.saved.snapshotId())
                .isEqualTo(table.currentSnapshot().snapshotId());
    }

    /**
     * A file that isn't Parquet is skipped; a table that doesn't exist is
     * {@code NOT_FOUND}.
     *
     * @throws Exception if the test fails
     */
    @Test
    void skipsOtherFormatsAndReportsAMissingTable() throws Exception
    {
        IcebergTableSource source = source("latest", identifier);
        DataFile avro = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(table.location() + "/data/x.avro")
                .withFormat(FileFormat.AVRO).withFileSizeInBytes(10)
                .withRecordCount(1).build();
        append(avro);
        assertThat(source.list()).isEmpty();

        IcebergTableSource missing = source("latest",
                TableIdentifier.of(identifier.namespace(), "missing"));
        assertThatThrownBy(missing::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.NOT_FOUND));
        assertThat(missing.reachable()).isFalse();
    }
}
