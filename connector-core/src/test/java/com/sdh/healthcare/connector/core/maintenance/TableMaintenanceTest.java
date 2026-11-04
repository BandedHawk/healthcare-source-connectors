/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.maintenance;

import static com.sdh.healthcare.connector.contract.table.SectionsTable.CIPHERTEXT;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.DEFAULT_NAME;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.INGESTED_AT;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.SECTION_ID;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.SEQ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.core.LocalFileIO;
import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.storage.SectionBatchWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link TableMaintenance}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class TableMaintenanceTest
{

    /** The time the tests run. */
    static final Instant NOW = Instant.now();

    /** A day old enough for retention and compaction. */
    static final Instant OLD_DAY = NOW.minus(Duration.ofDays(10));

    /** A day inside every retention window. */
    static final Instant RECENT_DAY = NOW.minus(Duration.ofDays(1));

    /** The warehouse directory. */
    @TempDir
    Path warehouse;

    /** The catalog, on the shared test Postgres. */
    JdbcCatalog catalog;

    /** A namespace of the test's own. */
    String namespace = "ns_" + UUID.randomUUID().toString().replace("-", "");

    /** The data keys' KEK. */
    AesKeyWrapProvider keys;

    /** The epoch of the last committer made. */
    long epoch;

    /** The committer holding the current epoch. */
    FencedCommitter committer;

    /**
     * Creates the catalog, a KEK and the first committer.
     *
     * @throws Exception if the key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        catalog = new JdbcCatalog();
        catalog.initialize(
                "jdbc_" + UUID.randomUUID().toString().replace("-", ""),
                Map.of(CatalogProperties.URI, TestPostgres.jdbcUrl(),
                        CatalogProperties.WAREHOUSE_LOCATION,
                        warehouse.toString(),
                        CatalogProperties.FILE_IO_IMPL,
                        LocalFileIO.class.getName(),
                        "jdbc.schema-version", "V1"));
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of("k", generator.generateKey()));
        committer = newCommitter();
    }

    /**
     * A committer at the next epoch.
     *
     * @return the committer
     */
    FencedCommitter newCommitter()
    {
        long held = ++epoch;
        return new FencedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return held;
            }

            @Override
            public void checkHeld()
            {
            }
        });
    }

    /**
     * The sections table, established for the current committer.
     *
     * @return the table
     */
    Table sections()
    {
        Table table = IngestTables.sections(catalog, namespace);
        committer.establish(table);
        return table;
    }

    /**
     * Commits one small data file of sections ingested on a day.
     *
     * @param table the sections table
     * @param day the ingestion time
     * @param rows the number of sections
     */
    void batch(Table table, Instant day, int rows)
    {
        SectionBatchWriter writer = new SectionBatchWriter(table, keys, "k",
                new SectionIds(new byte[32]), day);
        UUID file = UUID.randomUUID();
        for (int index = 0; index < rows; index++)
        {
            writer.add(new SectionBatchWriter.SectionInput(file, index,
                    index * 10L, "csv",
                    ("row-" + index + "-" + file).getBytes()));
        }
        DataFile dataFile = writer.finish();
        committer.commit(table, transaction -> transaction.newFastAppend()
                .appendFile(dataFile).commit());
    }

    /**
     * Every row of the table as text, for comparing contents.
     *
     * @param table the sections table
     * @return id, sequence, ingestion time and ciphertext of each row
     * @throws IOException if the table can't be read
     */
    static Set<String> rows(Table table) throws IOException
    {
        table.refresh();
        Set<String> out = new TreeSet<>();
        try (CloseableIterable<Record> records =
                IcebergGenerics.read(table).build())
        {
            for (Record record : records)
            {
                ByteBuffer ciphertext =
                        ((ByteBuffer) record.getField(CIPHERTEXT)).duplicate();
                byte[] bytes = new byte[ciphertext.remaining()];
                ciphertext.get(bytes);
                out.add(record.getField(SECTION_ID) + "|"
                        + record.getField(SEQ) + "|"
                        + record.getField(INGESTED_AT) + "|"
                        + HexFormat.of().formatHex(bytes));
            }
        }
        return out;
    }

    /**
     * The table's current data files.
     *
     * @param table the table
     * @return the data files
     * @throws IOException if the files can't be planned
     */
    static List<DataFile> files(Table table) throws IOException
    {
        table.refresh();
        List<DataFile> out = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().planFiles())
        {
            tasks.forEach(task -> out.add(task.file()));
        }
        return out;
    }

    /**
     * The number of snapshots the table keeps.
     *
     * @param table the table
     * @return the count
     */
    static int snapshots(Table table)
    {
        table.refresh();
        int count = 0;
        for (Snapshot ignored : table.snapshots())
        {
            count++;
        }
        return count;
    }

    /**
     * Settings with expiry of the files it replaces turned on and a
     * rewrite threshold of 0.75.
     *
     * @param snapshotRetention how long snapshots are kept
     * @param minSnapshots the snapshots always kept
     * @param minInputFiles the small files a partition needs to compact
     * @param budget the bytes one run may rewrite
     * @param orphanAge how old an unreferenced file must be to delete
     * @return the settings
     */
    static MaintenanceSettings settings(Duration snapshotRetention,
            int minSnapshots, int minInputFiles, long budget,
            Duration orphanAge)
    {
        return new MaintenanceSettings(snapshotRetention, minSnapshots, true,
                0.75, minInputFiles, budget, orphanAge);
    }

    /**
     * Maintenance through the current committer.
     *
     * @param settings the settings
     * @return the maintenance
     */
    TableMaintenance maintenance(MaintenanceSettings settings)
    {
        return new TableMaintenance(committer, settings, Clock.systemUTC());
    }

    /**
     * Compaction merges each partition's small files, keeps every row,
     * and the replaced files go once their snapshot expires.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void compactsSmallFilesPerPartitionKeepingRowsThenExpiresOldFiles()
            throws Exception
    {
        Table table = sections();
        for (int index = 0; index < 4; index++)
        {
            batch(table, OLD_DAY, 3);
        }
        batch(table, RECENT_DAY, 2);
        batch(table, RECENT_DAY, 2);
        Set<String> before = rows(table);
        List<DataFile> oldFiles = files(table);
        assertThat(oldFiles).hasSize(6);

        // Only the old day has enough small files (3+). Everything is
        // expired except the current snapshot.
        TableMaintenance.Report report = maintenance(settings(Duration.ZERO,
                1, 3, 8L << 30, Duration.ofDays(3)))
                .run(table, INGESTED_AT, Optional.empty(), false);

        assertThat(report.compactedFiles()).isEqualTo(4);
        assertThat(report.compactedInto()).isEqualTo(1);
        assertThat(files(table)).hasSize(3);
        assertThat(rows(table)).isEqualTo(before);
        assertThat(snapshots(table)).isEqualTo(1);
        // The compaction snapshot is current, so the files it replaced stay
        // (time travel to it still works)…
        assertThat(oldFiles)
                .allMatch(file -> Files.exists(Path.of(file.location())));

        // …until it ages out: after the next commit and expiry, they are
        // physically gone.
        batch(table, RECENT_DAY, 1);
        maintenance(settings(Duration.ZERO, 1, 3, 8L << 30,
                Duration.ofDays(3)))
                .run(table, INGESTED_AT, Optional.empty(), false);
        long gone = oldFiles.stream()
                .filter(file -> !Files.exists(Path.of(file.location())))
                .count();
        assertThat(gone).isEqualTo(4);
        assertThat(rows(table)).containsAll(before);
    }

    /**
     * Compaction stops once the budget is spent, and young snapshots
     * stay.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void compactionStopsAtTheBudgetAndKeepsRecentSnapshots() throws Exception
    {
        Table table = sections();
        for (int index = 0; index < 4; index++)
        {
            batch(table, OLD_DAY, 1);
            batch(table, RECENT_DAY, 1);
        }
        List<DataFile> all = files(table);
        int firstDay = day(all.getFirst());
        long oneBin = all.stream().filter(file -> day(file) == firstDay)
                .mapToLong(DataFile::fileSizeInBytes).sum();
        int snapshotsBefore = snapshots(table);
        TableMaintenance.Report report = maintenance(settings(
                Duration.ofDays(7), 10, 2, oneBin, Duration.ofDays(3)))
                .run(table, INGESTED_AT, Optional.empty(), false);
        // The second partition waits for the next run.
        assertThat(report.compactedInto()).isEqualTo(1);
        assertThat(files(table)).hasSize(5);
        // All are younger than 7 days.
        assertThat(report.expiredSnapshots()).isZero();
        assertThat(snapshots(table)).isEqualTo(snapshotsBefore + 1);
    }

    /**
     * Retention drops whole days past the window and removes their files
     * at once; with nothing to drop it commits nothing.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void retentionDropsWholeOldDaysAndRemovesTheirFilesAtOnce()
            throws Exception
    {
        Table table = sections();
        batch(table, OLD_DAY, 2);
        batch(table, RECENT_DAY, 2);
        int oldDay = (int) OLD_DAY.atOffset(ZoneOffset.UTC).toLocalDate()
                .toEpochDay();
        String oldFile = files(table).stream()
                .filter(file -> day(file) == oldDay).findFirst().orElseThrow()
                .location();

        MaintenanceSettings settings = settings(Duration.ofDays(7), 10, 5,
                8L << 30, Duration.ofDays(3));
        TableMaintenance.Report report = maintenance(settings).run(table,
                INGESTED_AT, Optional.of(Duration.ofDays(5)), false);

        assertThat(report.retentionDeletedFiles()).isEqualTo(1);
        assertThat(rows(table)).hasSize(2).allMatch(row -> !row.contains(
                OLD_DAY.atOffset(ZoneOffset.UTC).toLocalDate().toString()));
        assertThat(Path.of(oldFile))
                .as("PHI removed physically, not only from metadata")
                .doesNotExist();

        int snapshotCount = snapshots(table);
        maintenance(settings).run(table, INGESTED_AT,
                Optional.of(Duration.ofDays(5)), false);
        assertThat(snapshots(table))
                .as("nothing to delete → no empty snapshot")
                .isEqualTo(snapshotCount);
    }

    /**
     * The day partition value of a data file.
     *
     * @param file the data file
     * @return days since the epoch
     */
    static int day(DataFile file)
    {
        return file.partition().get(0, Integer.class);
    }

    /**
     * Retention works on the landed items table by its landing time.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void retentionWorksOnLandedItemsByLandedAt() throws Exception
    {
        Table table = LandingTables.landedItems(catalog, namespace);
        committer.establish(table);
        SectionIds ids = new SectionIds(new byte[32]);
        RetryPolicy fast = new RetryPolicy(1, Duration.ZERO, Duration.ZERO);
        ItemLander.Settings landerSettings = ItemLander.Settings
                .defaults("c", "k").withSizes(64, 256, 256).withRetry(fast);
        new ItemLander(table, committer, keys, ids, landerSettings,
                Clock.fixed(OLD_DAY, ZoneOffset.UTC))
                .land(new NewItem(UUID.randomUUID(), ItemKind.FILE, null,
                        "old.csv", null),
                        offset -> new ByteArrayInputStream(new byte[300]));
        UUID recent = UUID.randomUUID();
        new ItemLander(table, committer, keys, ids, landerSettings,
                Clock.fixed(RECENT_DAY, ZoneOffset.UTC))
                .land(new NewItem(recent, ItemKind.FILE, null, "new.csv",
                        null),
                        offset -> new ByteArrayInputStream(new byte[100]));

        maintenance(settings(Duration.ofDays(7), 10, 50, 8L << 30,
                Duration.ofDays(3)))
                .run(table, LandingTables.LANDED_AT,
                        Optional.of(Duration.ofDays(5)), false);

        table.refresh();
        try (CloseableIterable<Record> records =
                IcebergGenerics.read(table).build())
        {
            assertThat(records).isNotEmpty().allMatch(record -> record
                    .getField(LandingTables.ITEM_ID).equals(recent));
        }
    }

    /**
     * Orphan cleanup deletes unreferenced files past the age limit and
     * nothing else.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void orphanCleanupDeletesOnlyOldUnreferencedFiles() throws Exception
    {
        Table table = sections();
        batch(table, RECENT_DAY, 2);
        batch(table, RECENT_DAY, 2);
        Set<String> before = rows(table);
        Path data = Path.of(table.location(), "data");
        Path oldStray = Files.writeString(
                data.resolve("crashed-batch.parquet"), "half-written");
        Files.setLastModifiedTime(oldStray,
                FileTime.from(NOW.minus(Duration.ofDays(4))));
        Path freshStray = Files.writeString(
                data.resolve("in-flight.parquet"), "being written");
        Set<String> referenced = TableMaintenance.reachable(table);

        MaintenanceSettings settings = new MaintenanceSettings(
                Duration.ofDays(7), 10, false, 0.75, 5, 8L << 30,
                Duration.ofDays(3));
        TableMaintenance.Report report = maintenance(settings).run(table,
                INGESTED_AT, Optional.empty(), true);

        assertThat(report.orphansDeleted()).isEqualTo(1);
        assertThat(oldStray).doesNotExist();
        assertThat(freshStray).exists();
        assertThat(referenced)
                .allMatch(path -> Files.exists(Path.of(path)));
        assertThat(rows(catalog.loadTable(
                TableIdentifier.of(namespace, DEFAULT_NAME))))
                .isEqualTo(before);
    }

    /** Listed paths are qualified to match the table's own paths. */
    @Test
    void listedPathsAreQualifiedLikeTablePaths()
    {
        String adls = "abfss://fs@acct.dfs.core.windows.net/wh/t/";
        // ADLSFileIO lists paths relative to the container.
        assertThat(TableMaintenance.qualify(adls, "wh/t/data/x.parquet"))
                .isEqualTo("abfss://fs@acct.dfs.core.windows.net"
                        + "/wh/t/data/x.parquet");
        assertThat(TableMaintenance.qualify(adls, "/wh/t/data/x.parquet"))
                .isEqualTo("abfss://fs@acct.dfs.core.windows.net"
                        + "/wh/t/data/x.parquet");
        // S3 and GCS list full locations; local paths have no scheme at all.
        assertThat(TableMaintenance.qualify("s3://b/wh/t/",
                "s3://b/wh/t/data/x.parquet"))
                .isEqualTo("s3://b/wh/t/data/x.parquet");
        assertThat(TableMaintenance.qualify("/tmp/wh/t/",
                "/tmp/wh/t/data/x.parquet"))
                .isEqualTo("/tmp/wh/t/data/x.parquet");
    }

    /**
     * Lists every file under another path than the table uses, as a bucket
     * alias or a FileIO quirk would.
     */
    public static final class AliasListingFileIO extends LocalFileIO
    {

        /** Creates the FileIO; the catalog calls this by reflection. */
        public AliasListingFileIO()
        {
        }

        /**
         * Lists the files under a prefix, each behind {@code /alias}.
         *
         * @param prefix the prefix
         * @return the files, under the alias
         */
        @Override
        public Iterable<FileInfo> listPrefix(String prefix)
        {
            List<FileInfo> out = new ArrayList<>();
            for (FileInfo file : super.listPrefix(prefix))
            {
                out.add(new FileInfo("/alias" + file.location(), file.size(),
                        file.createdAtMillis()));
            }
            return out;
        }
    }

    /**
     * A small table isn't wiped when no listed path matches a referenced
     * one.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aSmallTableIsNotWipedWhenListedPathsDontMatch() throws Exception
    {
        JdbcCatalog aliased = new JdbcCatalog();
        aliased.initialize(
                "jdbc_" + UUID.randomUUID().toString().replace("-", ""),
                Map.of(CatalogProperties.URI, TestPostgres.jdbcUrl(),
                        CatalogProperties.WAREHOUSE_LOCATION,
                        warehouse.resolve("aliased").toString(),
                        CatalogProperties.FILE_IO_IMPL,
                        AliasListingFileIO.class.getName(),
                        "jdbc.schema-version", "V1"));
        Table table = IngestTables.sections(aliased, namespace);
        committer.establish(table);
        batch(table, RECENT_DAY, 2);
        Set<String> referenced = TableMaintenance.reachable(table);
        // Below the old size-based guard.
        assertThat(referenced).hasSizeLessThan(20);
        for (String path : referenced)
        {
            Files.setLastModifiedTime(Path.of(path),
                    FileTime.from(NOW.minus(Duration.ofDays(30))));
        }

        TableMaintenance.Report report = maintenance(new MaintenanceSettings(
                Duration.ofDays(7), 10, false, 0.75, 5, 8L << 30,
                Duration.ofDays(3)))
                .run(table, INGESTED_AT, Optional.empty(), true);

        assertThat(report.orphansDeleted()).isZero();
        assertThat(referenced)
                .allMatch(path -> Files.exists(Path.of(path)));
    }

    /**
     * An instance fenced out by a successor changes nothing.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aFencedOutInstanceChangesNothing() throws Exception
    {
        Table table = sections();
        for (int index = 0; index < 3; index++)
        {
            batch(table, OLD_DAY, 1);
        }
        int snapshotCount = snapshots(table);
        FencedCommitter stale = committer;
        committer = newCommitter();
        // The successor.
        committer.establish(table);
        int afterEstablish = snapshots(table);
        assertThatThrownBy(() -> new TableMaintenance(stale,
                settings(Duration.ZERO, 1, 2, 8L << 30, Duration.ofDays(3)),
                Clock.systemUTC()).run(table, INGESTED_AT,
                        Optional.of(Duration.ofDays(1)), true))
                .isInstanceOf(LeaseLostException.class);
        assertThat(snapshots(table)).isEqualTo(afterEstablish)
                .isGreaterThan(snapshotCount);
        assertThat(files(table)).hasSize(3);
    }
}
