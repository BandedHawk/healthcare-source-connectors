package com.sdh.healthcare.connector.core.maintenance;

import static com.sdh.healthcare.connector.contract.table.SectionsTable.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.core.LocalFileIO;
import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.storage.ArchiveTables;
import com.sdh.healthcare.connector.core.storage.Archiver;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.storage.SectionBatchWriter;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TableMaintenanceTest {
    static final Instant NOW = Instant.now();
    static final Instant OLD_DAY = NOW.minus(Duration.ofDays(10));
    static final Instant RECENT_DAY = NOW.minus(Duration.ofDays(1));

    @TempDir
    Path warehouse;

    JdbcCatalog catalog;
    String namespace = "ns_" + UUID.randomUUID().toString().replace("-", "");
    AesKeyWrapProvider keys;
    long epoch;
    FencedCommitter committer;

    @BeforeEach
    void setUp() throws Exception {
        catalog = new JdbcCatalog();
        catalog.initialize("jdbc_" + UUID.randomUUID().toString().replace("-", ""), Map.of(
                CatalogProperties.URI, TestPostgres.jdbcUrl(),
                CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString(),
                CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName(),
                "jdbc.schema-version", "V1"));
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        keys = new AesKeyWrapProvider(Map.of("k", kg.generateKey()));
        committer = newCommitter();
    }

    FencedCommitter newCommitter() {
        long e = ++epoch;
        return new FencedCommitter(new LeaseToken() {
            public long epoch() { return e; }
            public void checkHeld() {}
        });
    }

    Table sections() {
        Table t = IngestTables.sections(catalog, namespace);
        committer.establish(t);
        return t;
    }

    /** One commit with one small data file holding {@code rows} sections, ingested on {@code day}. */
    void batch(Table t, Instant day, int rows) {
        SectionBatchWriter w = new SectionBatchWriter(t, keys, "k", new SectionIds(new byte[32]), day);
        UUID file = UUID.randomUUID();
        for (int i = 0; i < rows; i++) {
            w.add(new SectionBatchWriter.SectionInput(file, i, i * 10L, "csv", ("row-" + i + "-" + file).getBytes()));
        }
        DataFile df = w.finish();
        committer.commit(t, tx -> tx.newFastAppend().appendFile(df).commit());
    }

    static Set<String> rows(Table t) throws IOException {
        t.refresh();
        Set<String> out = new TreeSet<>();
        try (CloseableIterable<Record> rs = IcebergGenerics.read(t).build()) {
            for (Record r : rs) {
                ByteBuffer ct = ((ByteBuffer) r.getField(CIPHERTEXT)).duplicate();
                byte[] b = new byte[ct.remaining()];
                ct.get(b);
                out.add(r.getField(SECTION_ID) + "|" + r.getField(SEQ) + "|" + r.getField(INGESTED_AT) + "|"
                        + HexFormat.of().formatHex(b));
            }
        }
        return out;
    }

    static List<DataFile> files(Table t) throws IOException {
        t.refresh();
        List<DataFile> out = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = t.newScan().planFiles()) {
            tasks.forEach(task -> out.add(task.file()));
        }
        return out;
    }

    static int snapshots(Table t) {
        t.refresh();
        int n = 0;
        for (var ignored : t.snapshots()) {
            n++;
        }
        return n;
    }

    static MaintenanceSettings settings(Duration snapshotRetention, int minSnapshots, int minInputFiles,
            long budget, Duration orphanAge) {
        return new MaintenanceSettings(snapshotRetention, minSnapshots, true, 0.75, minInputFiles, budget, orphanAge);
    }

    TableMaintenance maintenance(MaintenanceSettings s) {
        return new TableMaintenance(committer, s, Clock.systemUTC());
    }

    @Test
    void compactsSmallFilesPerPartitionKeepingEveryRowAndThenExpiresTheOldFiles() throws Exception {
        Table t = sections();
        for (int i = 0; i < 4; i++) {
            batch(t, OLD_DAY, 3);
        }
        batch(t, RECENT_DAY, 2);
        batch(t, RECENT_DAY, 2);
        Set<String> before = rows(t);
        List<DataFile> oldFiles = files(t);
        assertThat(oldFiles).hasSize(6);

        // Only the old day has enough small files (3+). Everything is expired except the current snapshot.
        TableMaintenance.Report r = maintenance(settings(Duration.ZERO, 1, 3, 8L << 30, Duration.ofDays(3)))
                .run(t, INGESTED_AT, Optional.empty(), false);

        assertThat(r.compactedFiles()).isEqualTo(4);
        assertThat(r.compactedInto()).isEqualTo(1);
        assertThat(files(t)).hasSize(3);
        assertThat(rows(t)).isEqualTo(before);
        assertThat(snapshots(t)).isEqualTo(1);
        // The compaction snapshot is current, so the files it replaced stay (time travel to it still works)…
        assertThat(oldFiles).allMatch(f -> Files.exists(Path.of(f.location())));

        // …until it ages out: after the next commit and expiry, they are physically gone.
        batch(t, RECENT_DAY, 1);
        maintenance(settings(Duration.ZERO, 1, 3, 8L << 30, Duration.ofDays(3)))
                .run(t, INGESTED_AT, Optional.empty(), false);
        long gone = oldFiles.stream().filter(f -> !Files.exists(Path.of(f.location()))).count();
        assertThat(gone).isEqualTo(4);
        assertThat(rows(t)).containsAll(before);
    }

    @Test
    void compactionStopsAtTheBudgetAndKeepsRecentSnapshots() throws Exception {
        Table t = sections();
        for (int i = 0; i < 4; i++) {
            batch(t, OLD_DAY, 1);
            batch(t, RECENT_DAY, 1);
        }
        List<DataFile> all = files(t);
        int firstDay = day(all.getFirst());
        long oneBin = all.stream().filter(f -> day(f) == firstDay).mapToLong(DataFile::fileSizeInBytes).sum();
        int snapshotsBefore = snapshots(t);
        TableMaintenance.Report r = maintenance(settings(Duration.ofDays(7), 10, 2, oneBin, Duration.ofDays(3)))
                .run(t, INGESTED_AT, Optional.empty(), false);
        assertThat(r.compactedInto()).isEqualTo(1); // second partition waits for the next run
        assertThat(files(t)).hasSize(5);
        assertThat(r.expiredSnapshots()).isZero(); // all younger than 7 days
        assertThat(snapshots(t)).isEqualTo(snapshotsBefore + 1);
    }

    @Test
    void retentionDropsWholeOldDaysAndRemovesTheirFilesAtOnce() throws Exception {
        Table t = sections();
        batch(t, OLD_DAY, 2);
        batch(t, RECENT_DAY, 2);
        int oldDay = (int) OLD_DAY.atOffset(ZoneOffset.UTC).toLocalDate().toEpochDay();
        String oldFile = files(t).stream().filter(f -> day(f) == oldDay).findFirst().orElseThrow().location();

        MaintenanceSettings s = settings(Duration.ofDays(7), 10, 5, 8L << 30, Duration.ofDays(3));
        TableMaintenance.Report r = maintenance(s).run(t, INGESTED_AT, Optional.of(Duration.ofDays(5)), false);

        assertThat(r.retentionDeletedFiles()).isEqualTo(1);
        assertThat(rows(t)).hasSize(2).allMatch(row -> !row.contains(
                OLD_DAY.atOffset(ZoneOffset.UTC).toLocalDate().toString()));
        assertThat(Path.of(oldFile)).as("PHI removed physically, not only from metadata").doesNotExist();

        int snaps = snapshots(t);
        maintenance(s).run(t, INGESTED_AT, Optional.of(Duration.ofDays(5)), false);
        assertThat(snapshots(t)).as("nothing to delete → no empty snapshot").isEqualTo(snaps);
    }

    /** The day partition value (days since the epoch). */
    static int day(DataFile f) {
        return f.partition().get(0, Integer.class);
    }

    @Test
    void retentionWorksOnArchiveTablesByArchivedAt() throws Exception {
        Table t = IngestTables.sourceFiles(catalog, namespace);
        committer.establish(t);
        SectionIds ids = new SectionIds(new byte[32]);
        RetryPolicy fast = new RetryPolicy(1, Duration.ZERO, Duration.ZERO);
        Archiver.Settings as = Archiver.Settings.defaults("c", "k").withSizes(64, 256, 256).withRetry(fast);
        new Archiver(t, committer, keys, ids, as, Clock.fixed(OLD_DAY, ZoneOffset.UTC))
                .archive(UUID.randomUUID(), SeekableByteSource.of(new byte[300]), Optional.of("old.csv"), (r, at) -> {});
        UUID recent = UUID.randomUUID();
        new Archiver(t, committer, keys, ids, as, Clock.fixed(RECENT_DAY, ZoneOffset.UTC))
                .archive(recent, SeekableByteSource.of(new byte[100]), Optional.of("new.csv"), (r, at) -> {});

        maintenance(settings(Duration.ofDays(7), 10, 50, 8L << 30, Duration.ofDays(3)))
                .run(t, ArchiveTables.ARCHIVED_AT, Optional.of(Duration.ofDays(5)), false);

        t.refresh();
        try (CloseableIterable<Record> rs = IcebergGenerics.read(t).build()) {
            assertThat(rs).isNotEmpty().allMatch(r -> r.getField(ArchiveTables.FILE_ID).equals(recent));
        }
    }

    @Test
    void orphanCleanupDeletesOnlyOldUnreferencedFiles() throws Exception {
        Table t = sections();
        batch(t, RECENT_DAY, 2);
        batch(t, RECENT_DAY, 2);
        Set<String> before = rows(t);
        Path data = Path.of(t.location(), "data");
        Path oldStray = Files.writeString(data.resolve("crashed-batch.parquet"), "half-written");
        Files.setLastModifiedTime(oldStray, FileTime.from(NOW.minus(Duration.ofDays(4))));
        Path freshStray = Files.writeString(data.resolve("in-flight.parquet"), "being written");
        Set<String> referenced = TableMaintenance.reachable(t);

        MaintenanceSettings s = new MaintenanceSettings(Duration.ofDays(7), 10, false, 0.75, 5, 8L << 30,
                Duration.ofDays(3));
        TableMaintenance.Report r = maintenance(s).run(t, INGESTED_AT, Optional.empty(), true);

        assertThat(r.orphansDeleted()).isEqualTo(1);
        assertThat(oldStray).doesNotExist();
        assertThat(freshStray).exists();
        assertThat(referenced).allMatch(p -> Files.exists(Path.of(p)));
        assertThat(rows(catalog.loadTable(org.apache.iceberg.catalog.TableIdentifier.of(namespace, DEFAULT_NAME))))
                .isEqualTo(before);
    }

    @Test
    void aFencedOutInstanceChangesNothing() throws Exception {
        Table t = sections();
        for (int i = 0; i < 3; i++) {
            batch(t, OLD_DAY, 1);
        }
        int snaps = snapshots(t);
        FencedCommitter stale = committer;
        committer = newCommitter();
        committer.establish(t); // successor
        int afterEstablish = snapshots(t);
        assertThatThrownBy(() -> new TableMaintenance(stale, settings(Duration.ZERO, 1, 2, 8L << 30,
                Duration.ofDays(3)), Clock.systemUTC()).run(t, INGESTED_AT, Optional.of(Duration.ofDays(1)), true))
                .isInstanceOf(LeaseLostException.class);
        assertThat(snapshots(t)).isEqualTo(afterEstablish).isGreaterThan(snaps);
        assertThat(files(t)).hasSize(3);
    }
}
