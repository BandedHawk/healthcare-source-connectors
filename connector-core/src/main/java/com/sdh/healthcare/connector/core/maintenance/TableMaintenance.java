package com.sdh.healthcare.connector.core.maintenance;

import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintenance for one ingest table without Spark (design §8.4): retention, bin-pack compaction, snapshot expiry and
 * orphan-file cleanup. Runs on the single writer, never at the same time as ingest (the caller guarantees that).
 *
 * <p>Every commit goes through the {@link FencedCommitter} except snapshot expiry, which must use Iceberg's own
 * commit path to delete files physically. Iceberg deletes a removed file once the snapshot that removed it expires,
 * so files replaced by compaction disappear when the compaction snapshot ages out ({@code snapshotRetention});
 * retention purges at once (see {@link #run}). It runs straight after {@link FencedCommitter#verifyCurrent}; the worst a
 * stale instance could do with it is expire snapshots that are already past retention.
 */
public final class TableMaintenance {
    private static final Logger LOG = LoggerFactory.getLogger(TableMaintenance.class);

    /** What one run did, for logs and tests. */
    public record Report(String table, int retentionDeletedFiles, int compactedFiles, int compactedInto,
            long compactedBytes, int expiredSnapshots, int orphansDeleted) {}

    private final FencedCommitter committer;
    private final MaintenanceSettings settings;
    private final Clock clock;

    public TableMaintenance(FencedCommitter committer, MaintenanceSettings settings, Clock clock) {
        this.committer = committer;
        this.settings = settings;
        this.clock = clock;
    }

    /**
     * @param timeColumn the partition source column, used for retention ({@code ingested_at} or {@code archived_at})
     * @param retention  delete rows older than this (whole UTC days); empty = keep forever
     * @param orphans    also clean up unreferenced files this run
     */
    public Report run(Table table, String timeColumn, Optional<Duration> retention, boolean orphans) {
        committer.verifyCurrent(table);
        Instant now = clock.instant();

        int retentionDeleted = 0;
        Long purgeBefore = null;
        if (retention.isPresent()) {
            retentionDeleted = applyRetention(table, timeColumn, retention.get(), now);
            if (retentionDeleted > 0) {
                // Iceberg's expiry deletes a file only once the snapshot that removed it has expired, and the current
                // snapshot never expires. A fenced marker commit makes the retention delete non-current, so it (and
                // every snapshot before it) can be expired now and the PHI files deleted in this run.
                committer.commit(table, tx -> { });
                purgeBefore = table.currentSnapshot().timestampMillis() + 1;
            }
        }

        long[] compaction = settings.compaction() ? compact(table) : new long[3];
        int expired = expire(table, now, purgeBefore);
        int orphansDeleted = orphans ? deleteOrphans(table, now) : 0;

        Report r = new Report(table.name(), retentionDeleted, (int) compaction[0], (int) compaction[1], compaction[2],
                expired, orphansDeleted);
        LOG.info("maintenance.done table={} retentionDeletedFiles={} compactedFiles={} into={} expiredSnapshots={}"
                        + " orphansDeleted={}", table.name(), r.retentionDeletedFiles(), r.compactedFiles(),
                r.compactedInto(), r.expiredSnapshots(), r.orphansDeleted());
        return r;
    }

    // ---- retention ----

    /** Deletes whole UTC days older than {@code now - retention}. Returns the number of data files removed. */
    private int applyRetention(Table table, String timeColumn, Duration retention, Instant now) {
        Instant cutoff = now.minus(retention).truncatedTo(ChronoUnit.DAYS).atOffset(ZoneOffset.UTC).toInstant();
        Expression older = Expressions.lessThan(timeColumn,
                TimeUnit.MILLISECONDS.toMicros(cutoff.toEpochMilli()));
        int matching = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(older).planFiles()) {
            for (FileScanTask ignored : tasks) {
                matching++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (matching == 0) {
            return 0; // no empty snapshot
        }
        committer.commit(table, tx -> tx.newDelete().deleteFromRowFilter(older).commit());
        LOG.info("maintenance.retention table={} cutoffDay={} files={}", table.name(),
                cutoff.atOffset(ZoneOffset.UTC).toLocalDate(), matching);
        return matching;
    }

    // ---- compaction ----

    /** Bin-packs small files per partition. Returns {inputFiles, outputFiles, inputBytes}. */
    private long[] compact(Table table) {
        long target = Long.parseLong(table.properties().getOrDefault(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                Long.toString(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT)));
        long small = (long) (target * settings.smallFileFraction());
        Snapshot start = table.currentSnapshot();
        if (start == null) {
            return new long[3];
        }
        Map<String, List<DataFile>> byPartition = new LinkedHashMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask t : tasks) {
                DataFile f = t.file();
                if (f.fileSizeInBytes() < small && t.deletes().isEmpty()) {
                    byPartition.computeIfAbsent(table.spec().partitionToPath(f.partition()), k -> new ArrayList<>())
                            .add(f);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        long in = 0;
        long out = 0;
        long bytes = 0;
        for (List<DataFile> files : byPartition.values()) {
            if (files.size() < settings.minInputFiles()) {
                continue;
            }
            for (List<DataFile> bin : binPack(files, target)) {
                if (bin.size() < 2) {
                    continue;
                }
                long binBytes = bin.stream().mapToLong(DataFile::fileSizeInBytes).sum();
                if (bytes > 0 && bytes + binBytes > settings.maxRewriteBytesPerRun()) {
                    LOG.info("maintenance.compaction_budget_reached table={} bytes={}", table.name(), bytes);
                    return new long[] {in, out, bytes};
                }
                rewrite(table, start.snapshotId(), bin);
                in += bin.size();
                out++;
                bytes += binBytes;
            }
        }
        return new long[] {in, out, bytes};
    }

    static List<List<DataFile>> binPack(List<DataFile> files, long target) {
        List<DataFile> sorted = new ArrayList<>(files);
        sorted.sort((a, b) -> Long.compare(b.fileSizeInBytes(), a.fileSizeInBytes()));
        List<List<DataFile>> bins = new ArrayList<>();
        List<Long> sizes = new ArrayList<>();
        for (DataFile f : sorted) {
            int fit = -1;
            for (int i = 0; i < bins.size(); i++) {
                if (sizes.get(i) + f.fileSizeInBytes() <= target) {
                    fit = i;
                    break;
                }
            }
            if (fit < 0) {
                bins.add(new ArrayList<>());
                sizes.add(0L);
                fit = bins.size() - 1;
            }
            bins.get(fit).add(f);
            sizes.set(fit, sizes.get(fit) + f.fileSizeInBytes());
        }
        return bins;
    }

    /**
     * Copies the rows of {@code bin} unchanged (ciphertext included) into one new file and swaps it in with
     * {@code RewriteFiles}, validated from {@code startSnapshot}. The new file is written unsorted: concatenating
     * sorted files doesn't keep the table's sort order.
     */
    private void rewrite(Table table, long startSnapshot, List<DataFile> bin) {
        DataFile first = bin.getFirst();
        String location = table.locationProvider().newDataLocation(table.spec(), first.partition(),
                FileFormat.PARQUET.addExtension("compact-" + UUID.randomUUID()));
        DataWriter<Record> writer;
        try {
            writer = Parquet.writeData(table.io().newOutputFile(location))
                    .schema(table.schema())
                    .withSpec(table.spec())
                    .withPartition(first.partition())
                    .withSortOrder(SortOrder.unsorted())
                    .setAll(table.properties())
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        long expectedRows = 0;
        try (writer) {
            for (DataFile f : bin) {
                expectedRows += f.recordCount();
                try (CloseableIterable<Record> rows = Parquet.read(table.io().newInputFile(f.location()))
                        .project(table.schema())
                        .createReaderFunc(fs -> GenericParquetReaders.buildReader(table.schema(), fs))
                        .build()) {
                    for (Record r : rows) {
                        writer.write(r);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        DataFile merged = writer.toDataFile();
        if (merged.recordCount() != expectedRows) {
            table.io().deleteFile(location);
            throw new IllegalStateException("Compaction row count mismatch in " + table.name());
        }
        committer.commit(table, tx -> {
            var rw = tx.newRewrite().validateFromSnapshot(startSnapshot);
            bin.forEach(rw::deleteFile);
            rw.addFile(merged);
            rw.commit();
        });
    }

    // ---- expiry ----

    /**
     * Expires snapshots older than the retention window, keeping at least {@code minSnapshots}. After a retention
     * delete, every snapshot before that delete is expired too, so the deleted PHI files are removed now.
     */
    private int expire(Table table, Instant now, Long purgeBefore) {
        committer.verifyCurrent(table);
        int before = size(table.snapshots());
        long olderThan = now.minus(settings.snapshotRetention()).toEpochMilli();
        var expire = table.expireSnapshots();
        if (purgeBefore != null) {
            expire.expireOlderThan(Math.max(olderThan, purgeBefore)).retainLast(1);
        } else {
            expire.expireOlderThan(olderThan).retainLast(settings.minSnapshots());
        }
        expire.commit();
        table.refresh();
        return before - size(table.snapshots());
    }

    private static int size(Iterable<?> it) {
        int n = 0;
        for (Object ignored : it) {
            n++;
        }
        return n;
    }

    // ---- orphans ----

    /**
     * Deletes files under the table location that no snapshot or metadata file references and that are older than
     * {@code orphanMinAge}: leftovers of crashed batches and abandoned compactions. Skipped if the FileIO can't list.
     */
    private int deleteOrphans(Table table, Instant now) {
        if (!(table.io() instanceof SupportsPrefixOperations io)) {
            LOG.info("maintenance.orphans_skipped table={} reason=io_cannot_list", table.name());
            return 0;
        }
        table.refresh();
        Set<String> reachable = reachable(table);
        if (reachable.isEmpty()) {
            return 0;
        }
        long cutoff = now.minus(settings.orphanMinAge()).toEpochMilli();
        String prefix = table.location().endsWith("/") ? table.location() : table.location() + "/";
        List<String> orphans = new ArrayList<>();
        int listed = 0;
        for (FileInfo f : io.listPrefix(prefix)) {
            listed++;
            if (!reachable.contains(f.location()) && f.createdAtMillis() < cutoff) {
                orphans.add(f.location());
            }
        }
        // A path form mismatch (scheme, bucket alias) would make every file look orphaned. Refuse rather than wipe.
        if (orphans.size() > 20 && orphans.size() * 2 > listed) {
            LOG.error("maintenance.orphans_refused table={} orphans={} listed={} (do listed paths match table paths?)",
                    table.name(), orphans.size(), listed);
            return 0;
        }
        for (String o : orphans) {
            io.deleteFile(o);
        }
        return orphans.size();
    }

    static Set<String> reachable(Table table) {
        Set<String> paths = new HashSet<>(ReachableFileUtil.metadataFileLocations(table, true));
        paths.addAll(ReachableFileUtil.manifestListLocations(table));
        paths.addAll(ReachableFileUtil.statisticsFilesLocations(table));
        for (PartitionStatisticsFile p : table.partitionStatisticsFiles()) {
            paths.add(p.path());
        }
        Set<String> manifests = new HashSet<>();
        for (Snapshot s : table.snapshots()) {
            for (ManifestFile m : s.allManifests(table.io())) {
                if (manifests.add(m.path())) {
                    paths.add(m.path());
                    try {
                        if (m.content() == ManifestContent.DATA) {
                            try (CloseableIterable<String> files = ManifestFiles.readPaths(m, table.io(),
                                    table.specs())) {
                                files.forEach(paths::add);
                            }
                        } else {
                            try (var deletes = ManifestFiles.readDeleteManifest(m, table.io(), table.specs())) {
                                deletes.forEach(d -> paths.add(d.location()));
                            }
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }
        }
        return paths;
    }
}
