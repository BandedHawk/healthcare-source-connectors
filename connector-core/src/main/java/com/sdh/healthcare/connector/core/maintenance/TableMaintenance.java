/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.maintenance;

import com.sdh.healthcare.connector.core.checkpoint.TableCommitter;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
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
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.RewriteFiles;
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
 * Maintenance for one ingest table without Spark (design §8.4): retention,
 * bin-pack compaction, snapshot expiry and orphan-file cleanup. On a table
 * with one writer it runs on that writer, never at the same time as ingest
 * (the caller guarantees that). On a table many stage workers append to
 * (STAGES.md §8) it runs on the pod holding the stage's duties lease while
 * the others append: a compaction that conflicts with an append is tried
 * again, and only files older than {@code orphanMinAge} are taken as
 * orphans, so a data file being written isn't one.
 * <p>
 * Every commit goes through the {@link TableCommitter} except snapshot
 * expiry, which must use Iceberg's own commit path to delete files
 * physically. Iceberg deletes a removed file once the snapshot that removed
 * it expires, so files replaced by compaction disappear when the compaction
 * snapshot ages out ({@code snapshotRetention}); retention purges at once
 * (see {@link #run}). It runs straight after
 * {@link TableCommitter#verifyCurrent}; the worst a stale instance could
 * do with it is expire snapshots that are already past retention.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class TableMaintenance
{

    /** Logs what each run did, without PHI. */
    private static final Logger LOG = LoggerFactory.getLogger(
            TableMaintenance.class);

    /**
     * What one run did, for logs and tests.
     *
     * @param table                 the table's name
     * @param retentionDeletedFiles data files removed by retention
     * @param purgedFiles           data files removed or rewritten by the
     *                              caller's purge, such as of given-up
     *                              items
     * @param compactedFiles        small files compacted
     * @param compactedInto         files they were compacted into
     * @param compactedBytes        bytes rewritten by compaction
     * @param expiredSnapshots      snapshots expired
     * @param orphansDeleted        unreferenced files deleted
     */
    public record Report(String table, int retentionDeletedFiles,
            int purgedFiles, int compactedFiles, int compactedInto,
            long compactedBytes, int expiredSnapshots, int orphansDeleted)
    {
    }

    /**
     * Rows a caller removes before compaction and expiry, such as the
     * chunks of items given up, so the files it removes are deleted from
     * storage in the same run, as retention's are.
     */
    @FunctionalInterface
    public interface Purge
    {

        /** Purges nothing. */
        Purge NONE = (table, now) -> 0;

        /**
         * Removes the rows, committing through the table's fence.
         *
         * @param table the table
         * @param now   the time of the run
         * @return how many data files were removed or rewritten
         */
        int purge(Table table, Instant now);
    }

    /**
     * What {@link #copyRows} wrote.
     *
     * @param file the new file; null if no row was kept
     * @param read how many rows were read
     * @param kept how many were kept
     */
    public record Copied(DataFile file, long read, long kept)
    {
    }

    /** Commits under the lease. */
    private final TableCommitter committer;

    /** The maintenance settings. */
    private final MaintenanceSettings settings;

    /** Tells the time, for retention and ages. */
    private final Clock clock;

    /**
     * Creates the maintenance.
     *
     * @param committer commits under the lease
     * @param settings  the maintenance settings
     * @param clock     tells the time
     */
    public TableMaintenance(TableCommitter committer,
            MaintenanceSettings settings, Clock clock)
    {
        this.committer = committer;
        this.settings = settings;
        this.clock = clock;
    }

    /**
     * Runs maintenance on a table, with the retention cutoff as configured.
     *
     * @param table      the table
     * @param timeColumn the partition source column, used for retention
     *                   ({@code ingested_at} or {@code landed_at})
     * @param retention  delete rows older than this (whole UTC days); empty
     *                   = keep forever
     * @param orphans    also clean up unreferenced files this run
     * @return what it did
     */
    public Report run(Table table, String timeColumn,
            Optional<Duration> retention, boolean orphans)
    {
        return run(table, timeColumn, retention, orphans,
                UnaryOperator.identity());
    }

    /**
     * Runs maintenance on a table, letting the caller keep more than the
     * retention would: for a table whose records span several rows written
     * at different times, such as a landed item's chunks, so none is purged
     * in part.
     *
     * @param table      the table
     * @param timeColumn the partition source column, used for retention
     * @param retention  delete rows older than this (whole UTC days); empty
     *                   = keep forever
     * @param orphans    also clean up unreferenced files this run
     * @param keepFrom   given the start of the first day retention keeps,
     *                   returns the instant from which rows must be kept
     *                   instead; an instant after the one given is ignored,
     *                   and the result is taken back to the start of its
     *                   UTC day
     * @return what it did
     */
    public Report run(Table table, String timeColumn,
            Optional<Duration> retention, boolean orphans,
            UnaryOperator<Instant> keepFrom)
    {
        return run(table, timeColumn, retention, orphans, keepFrom,
                Purge.NONE);
    }

    /**
     * Runs maintenance on a table, with a purge of the caller's after
     * retention.
     *
     * @param table      the table
     * @param timeColumn the partition source column, used for retention
     * @param retention  delete rows older than this (whole UTC days); empty
     *                   = keep forever
     * @param orphans    also clean up unreferenced files this run
     * @param keepFrom   where rows must be kept from instead, as for
     *                   {@link #run(Table, String, Optional, boolean,
     *                   UnaryOperator)}
     * @param purge      removes rows the caller no longer keeps
     * @return what it did
     */
    public Report run(Table table, String timeColumn,
            Optional<Duration> retention, boolean orphans,
            UnaryOperator<Instant> keepFrom, Purge purge)
    {
        Span span = Telemetry.get().span("maintenance",
                Fields.of(Attr.TABLE, table.name()));
        try (Scope ignored = span.makeCurrent())
        {
            return runTraced(table, timeColumn, retention, orphans,
                    keepFrom, purge);
        }
        catch (RuntimeException | Error failure)
        {
            Telemetry.fail(span, failure);
            throw failure;
        }
        finally
        {
            span.end();
        }
    }

    /**
     * Runs maintenance inside the run's span.
     *
     * @param table      the table
     * @param timeColumn the partition source column
     * @param retention  how long rows are kept; empty = forever
     * @param orphans    also clean up unreferenced files
     * @param keepFrom   where rows must be kept from instead
     * @param purge      removes rows the caller no longer keeps
     * @return what it did
     */
    private Report runTraced(Table table, String timeColumn,
            Optional<Duration> retention, boolean orphans,
            UnaryOperator<Instant> keepFrom, Purge purge)
    {
        committer.verifyCurrent(table);
        Instant now = clock.instant();

        int retentionDeleted = 0;
        Long purgeBefore = null;
        if (retention.isPresent())
        {
            retentionDeleted = applyRetention(table, timeColumn,
                    retention.get(), now, keepFrom);
        }
        int purged = purge.purge(table, now);
        if (retentionDeleted + purged > 0)
        {
            // Iceberg's expiry deletes a file only once the snapshot that
            // removed it has expired, and the current snapshot never
            // expires. A fenced marker commit makes the delete non-current,
            // so it (and every snapshot before it) can be expired now and
            // the PHI files deleted in this run.
            committer.commit(table, transaction ->
            {
            });
            purgeBefore = table.currentSnapshot().timestampMillis() + 1;
        }

        long[] compaction = settings.compaction()
                ? compact(table) : new long[3];
        int expired = expire(table, now, purgeBefore);
        int orphansDeleted = orphans ? deleteOrphans(table, now) : 0;

        Report report = new Report(table.name(), retentionDeleted, purged,
                (int) compaction[0], (int) compaction[1], compaction[2],
                expired, orphansDeleted);
        LOG.info("maintenance.done table={} retentionDeletedFiles={}"
                + " purgedFiles={} compactedFiles={} into={}"
                + " expiredSnapshots={} orphansDeleted={}", table.name(),
                report.retentionDeletedFiles(), report.purgedFiles(),
                report.compactedFiles(),
                report.compactedInto(), report.expiredSnapshots(),
                report.orphansDeleted());
        return report;
    }

    // ---- retention ----

    /**
     * Deletes whole UTC days older than {@code now - retention}, or than
     * where {@code keepFrom} says rows must be kept from, if that is
     * earlier.
     *
     * @param table      the table
     * @param timeColumn the partition source column
     * @param retention  how long rows are kept
     * @param now        the time now
     * @param keepFrom   where rows must be kept from instead
     * @return the number of data files removed
     */
    private int applyRetention(Table table, String timeColumn,
            Duration retention, Instant now, UnaryOperator<Instant> keepFrom)
    {
        Instant byAge = now.minus(retention).truncatedTo(ChronoUnit.DAYS);
        Instant held = keepFrom.apply(byAge);
        Instant cutoff = held.isBefore(byAge)
                ? held.truncatedTo(ChronoUnit.DAYS) : byAge;
        Expression older = Expressions.lessThan(timeColumn,
                TimeUnit.MILLISECONDS.toMicros(cutoff.toEpochMilli()));
        int matching = 0;
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().filter(older).planFiles())
        {
            for (FileScanTask ignored : tasks)
            {
                matching++;
            }
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        if (matching == 0)
        {
            return 0; // no empty snapshot
        }
        committer.commit(table, transaction -> transaction.newDelete()
                .deleteFromRowFilter(older).commit());
        // PHI deletion is audited: which table, how many files, and the
        // first day kept.
        Telemetry.get().audit("maintenance.retention",
                Fields.of(Attr.TABLE, table.name())
                        .with(Attr.COUNT, matching)
                        .with(Attr.REASON, "before "
                                + cutoff.atOffset(ZoneOffset.UTC)
                                        .toLocalDate()));
        return matching;
    }

    // ---- compaction ----

    /**
     * Bin-packs small files per partition.
     *
     * @param table the table
     * @return {inputFiles, outputFiles, inputBytes}
     */
    private long[] compact(Table table)
    {
        long target = Long.parseLong(table.properties().getOrDefault(
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                Long.toString(
                        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT)));
        long small = (long) (target * settings.smallFileFraction());
        Snapshot start = table.currentSnapshot();
        if (start == null)
        {
            return new long[3];
        }
        Map<String, List<DataFile>> byPartition = new LinkedHashMap<>();
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().planFiles())
        {
            for (FileScanTask task : tasks)
            {
                DataFile file = task.file();
                if (file.fileSizeInBytes() < small
                        && task.deletes().isEmpty())
                {
                    byPartition.computeIfAbsent(
                            table.spec().partitionToPath(file.partition()),
                            partition -> new ArrayList<>())
                            .add(file);
                }
            }
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }

        long inputFiles = 0;
        long outputFiles = 0;
        long bytes = 0;
        for (List<DataFile> files : byPartition.values())
        {
            if (files.size() < settings.minInputFiles())
            {
                continue;
            }
            for (List<DataFile> bin : binPack(files, target))
            {
                if (bin.size() < 2)
                {
                    continue;
                }
                long binBytes = bin.stream()
                        .mapToLong(DataFile::fileSizeInBytes).sum();
                if (bytes > 0 && bytes + binBytes
                        > settings.maxRewriteBytesPerRun())
                {
                    LOG.info("maintenance.compaction_budget_reached"
                            + " table={} bytes={}", table.name(), bytes);
                    return new long[]
                    {
                        inputFiles, outputFiles, bytes
                    };
                }
                rewrite(table, start.snapshotId(), bin);
                inputFiles += bin.size();
                outputFiles++;
                bytes += binBytes;
            }
        }
        return new long[]
        {
            inputFiles, outputFiles, bytes
        };
    }

    /**
     * Packs files into bins of at most the target size, largest first, each
     * into the first bin it fits.
     *
     * @param files  the files
     * @param target the target size of a bin, in bytes
     * @return the bins
     */
    static List<List<DataFile>> binPack(List<DataFile> files, long target)
    {
        List<DataFile> sorted = new ArrayList<>(files);
        sorted.sort((left, right) -> Long.compare(right.fileSizeInBytes(),
                left.fileSizeInBytes()));
        List<List<DataFile>> bins = new ArrayList<>();
        List<Long> sizes = new ArrayList<>();
        for (DataFile file : sorted)
        {
            int fit = -1;
            for (int index = 0; index < bins.size(); index++)
            {
                if (sizes.get(index) + file.fileSizeInBytes() <= target)
                {
                    fit = index;
                    break;
                }
            }
            if (fit < 0)
            {
                bins.add(new ArrayList<>());
                sizes.add(0L);
                fit = bins.size() - 1;
            }
            bins.get(fit).add(file);
            sizes.set(fit, sizes.get(fit) + file.fileSizeInBytes());
        }
        return bins;
    }

    /**
     * Copies the rows of {@code bin} unchanged (ciphertext included) into
     * one new file and swaps it in with {@code RewriteFiles}, validated from
     * {@code startSnapshot}. The new file is written unsorted: concatenating
     * sorted files doesn't keep the table's sort order.
     *
     * @param table         the table
     * @param startSnapshot the snapshot the files were chosen from
     * @param bin           the files to merge, all in one partition
     * @throws IllegalStateException if the merged file's row count is wrong
     */
    private void rewrite(Table table, long startSnapshot, List<DataFile> bin)
    {
        long expectedRows = 0;
        for (DataFile file : bin)
        {
            expectedRows += file.recordCount();
        }
        Copied copied = copyRows(table, bin, row -> true, "compact");
        DataFile merged = copied.file();
        if (merged == null || merged.recordCount() != expectedRows
                || copied.read() != expectedRows)
        {
            if (merged != null)
            {
                table.io().deleteFile(merged.location());
            }
            throw new IllegalStateException(
                    "Compaction row count mismatch in " + table.name());
        }
        committer.commit(table, transaction ->
        {
            RewriteFiles rewriteFiles = transaction.newRewrite()
                    .validateFromSnapshot(startSnapshot);
            bin.forEach(rewriteFiles::deleteFile);
            rewriteFiles.addFile(merged);
            rewriteFiles.commit();
        });
    }

    /**
     * Copies the rows of data files that a filter keeps, unchanged
     * (ciphertext included), into one new file in the first one's
     * partition, written unsorted. Nothing is committed: the caller swaps
     * the file in, or deletes it.
     *
     * @param table  the table
     * @param files  the files, all in one partition
     * @param keep   which rows to keep
     * @param prefix the start of the new file's name
     * @return the new file, null if no row was kept, and the counts
     * @throws UncheckedIOException if a file can't be read or written
     */
    public static Copied copyRows(Table table, List<DataFile> files,
            Predicate<Record> keep, String prefix)
    {
        DataFile first = files.getFirst();
        String location = table.locationProvider().newDataLocation(
                table.spec(), first.partition(),
                FileFormat.PARQUET.addExtension(
                        prefix + "-" + UUID.randomUUID()));
        DataWriter<Record> writer;
        try
        {
            writer = Parquet.writeData(table.io().newOutputFile(location))
                    .schema(table.schema())
                    .withSpec(table.spec())
                    .withPartition(first.partition())
                    .withSortOrder(SortOrder.unsorted())
                    .setAll(table.properties())
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .build();
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        long read = 0;
        long kept = 0;
        try (writer)
        {
            for (DataFile file : files)
            {
                try (CloseableIterable<Record> rows = Parquet.read(
                        table.io().newInputFile(file.location()))
                        .project(table.schema())
                        .createReaderFunc(fileSchema -> GenericParquetReaders
                                .buildReader(table.schema(), fileSchema))
                        .build())
                {
                    for (Record row : rows)
                    {
                        read++;
                        if (keep.test(row))
                        {
                            writer.write(row);
                            kept++;
                        }
                    }
                }
            }
        }
        catch (IOException failure)
        {
            deleteQuietly(table, location);
            throw new UncheckedIOException(failure);
        }
        if (kept == 0)
        {
            deleteQuietly(table, location);
            return new Copied(null, read, 0);
        }
        return new Copied(writer.toDataFile(), read, kept);
    }

    /**
     * Deletes a file that was never committed, if it was written at all.
     * An unreferenced file left behind is removed by orphan cleanup.
     *
     * @param table    the table
     * @param location the file
     */
    private static void deleteQuietly(Table table, String location)
    {
        try
        {
            table.io().deleteFile(location);
        }
        catch (RuntimeException ignored)
        {
            // Not written, or not deletable now; orphan cleanup removes it.
        }
    }

    // ---- expiry ----

    /**
     * Expires snapshots older than the retention window, keeping at least
     * {@code minSnapshots}. After a retention delete, every snapshot before
     * that delete is expired too, so the deleted PHI files are removed now.
     *
     * @param table       the table
     * @param now         the time now
     * @param purgeBefore expire every snapshot before this, in Unix
     *                    milliseconds, keeping only the current one; null
     *                    if retention deleted nothing
     * @return how many snapshots were expired
     */
    private int expire(Table table, Instant now, Long purgeBefore)
    {
        committer.verifyCurrent(table);
        int before = size(table.snapshots());
        long olderThan = now.minus(settings.snapshotRetention())
                .toEpochMilli();
        ExpireSnapshots expire = table.expireSnapshots();
        if (purgeBefore != null)
        {
            expire.expireOlderThan(Math.max(olderThan, purgeBefore))
                    .retainLast(1);
        }
        else
        {
            expire.expireOlderThan(olderThan)
                    .retainLast(settings.minSnapshots());
        }
        expire.commit();
        table.refresh();
        return before - size(table.snapshots());
    }

    /**
     * Counts what an iterable holds.
     *
     * @param items the iterable
     * @return how many items it holds
     */
    private static int size(Iterable<?> items)
    {
        int count = 0;
        for (Object ignored : items)
        {
            count++;
        }
        return count;
    }

    // ---- orphans ----

    /**
     * Deletes files under the table location that no snapshot or metadata
     * file references and that are older than {@code orphanMinAge}:
     * leftovers of crashed batches and abandoned compactions. Skipped if the
     * FileIO can't list.
     *
     * @param table the table
     * @param now   the time now
     * @return how many files were deleted
     */
    private int deleteOrphans(Table table, Instant now)
    {
        if (!(table.io() instanceof SupportsPrefixOperations io))
        {
            LOG.info("maintenance.orphans_skipped table={}"
                    + " reason=io_cannot_list", table.name());
            return 0;
        }
        table.refresh();
        Set<String> reachable = reachable(table);
        if (reachable.isEmpty())
        {
            return 0;
        }
        long cutoff = now.minus(settings.orphanMinAge()).toEpochMilli();
        String prefix = table.location().endsWith("/")
                ? table.location() : table.location() + "/";
        List<String> orphans = new ArrayList<>();
        int listed = 0;
        for (FileInfo file : io.listPrefix(prefix))
        {
            listed++;
            String location = qualify(prefix, file.location());
            // A path form the table doesn't use (another scheme, a bucket
            // alias) would make every file look orphaned, however small the
            // table. Refuse the whole run rather than delete anything.
            if (!location.startsWith(prefix))
            {
                LOG.error("maintenance.orphans_refused table={}"
                        + " reason=listed_path_outside_table (do listed"
                        + " paths match table paths?)", table.name());
                return 0;
            }
            if (!reachable.contains(location)
                    && file.createdAtMillis() < cutoff)
            {
                orphans.add(location);
            }
        }
        // A second line of defence: most of a table looking orphaned means
        // something is wrong, not a crash leftover.
        if (orphans.size() > 20 && orphans.size() * 2 > listed)
        {
            LOG.error("maintenance.orphans_refused table={} orphans={}"
                    + " listed={} (do listed paths match table paths?)",
                    table.name(), orphans.size(), listed);
            return 0;
        }
        for (String orphan : orphans)
        {
            io.deleteFile(orphan);
        }
        if (!orphans.isEmpty())
        {
            Telemetry.get().audit("maintenance.orphans_deleted",
                    Fields.of(Attr.TABLE, table.name())
                            .with(Attr.COUNT, orphans.size()));
        }
        return orphans.size();
    }

    /**
     * The full location of a listed file. {@code ADLSFileIO.listPrefix}
     * (Iceberg 1.12) returns paths relative to the container
     * ({@code wh/t/data/x.parquet}), unlike the S3 and GCS FileIOs and
     * unlike the table's own paths
     * ({@code abfss://fs@account.dfs.core.windows.net/wh/t/data/x.parquet}).
     * A relative path is resolved against the scheme and authority of the
     * prefix it was listed under; anything else is returned unchanged.
     *
     * @param prefix the prefix it was listed under
     * @param listed the path as listed
     * @return the full location
     */
    static String qualify(String prefix, String listed)
    {
        int scheme = prefix.indexOf("://");
        if (scheme < 0 || listed.contains("://"))
        {
            return listed;
        }
        int pathStart = prefix.indexOf('/', scheme + 3);
        String root = pathStart < 0
                ? prefix + "/" : prefix.substring(0, pathStart + 1);
        return root + (listed.startsWith("/") ? listed.substring(1) : listed);
    }

    /**
     * Every file the table's metadata references: metadata files, manifest
     * lists, statistics, manifests, and the data and delete files of every
     * snapshot.
     *
     * @param table the table
     * @return their locations
     * @throws UncheckedIOException if a manifest can't be read
     */
    static Set<String> reachable(Table table)
    {
        Set<String> paths = new HashSet<>(
                ReachableFileUtil.metadataFileLocations(table, true));
        paths.addAll(ReachableFileUtil.manifestListLocations(table));
        paths.addAll(ReachableFileUtil.statisticsFilesLocations(table));
        for (PartitionStatisticsFile statistics
                : table.partitionStatisticsFiles())
        {
            paths.add(statistics.path());
        }
        Set<String> manifests = new HashSet<>();
        for (Snapshot snapshot : table.snapshots())
        {
            for (ManifestFile manifest : snapshot.allManifests(table.io()))
            {
                if (manifests.add(manifest.path()))
                {
                    paths.add(manifest.path());
                    try
                    {
                        if (manifest.content() == ManifestContent.DATA)
                        {
                            try (CloseableIterable<String> files =
                                    ManifestFiles.readPaths(manifest,
                                            table.io(), table.specs()))
                            {
                                files.forEach(paths::add);
                            }
                        }
                        else
                        {
                            try (ManifestReader<DeleteFile> deletes =
                                    ManifestFiles.readDeleteManifest(manifest,
                                            table.io(), table.specs()))
                            {
                                deletes.forEach(delete ->
                                        paths.add(delete.location()));
                            }
                        }
                    }
                    catch (IOException failure)
                    {
                        throw new UncheckedIOException(failure);
                    }
                }
            }
        }
        return paths;
    }
}
