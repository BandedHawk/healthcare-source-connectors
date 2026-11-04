/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import com.sdh.healthcare.connector.landing.config.TableSettings;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.util.SnapshotUtil;

/**
 * An Iceberg table (design §5.3.18): each Parquet data file an append adds
 * is landed as one item, of kind {@code table-file}, its bytes as they
 * are. The splitter's {@code parquet} format splits it row by row.
 * <p>
 * The table is followed snapshot by snapshot, along the current
 * snapshot's ancestry, from the snapshot the intake has landed everything
 * up to ({@link TableProgress}):
 * <ul>
 * <li>an {@code append} adds its data files as items;</li>
 * <li>a {@code replace} (compaction) only rewrites rows already landed,
 * so it is passed over;</li>
 * <li>an {@code overwrite} or {@code delete} changes rows, which new items
 * can't express: {@code intake.table_rewrite} is reported, and nothing is
 * landed for it.</li>
 * </ul>
 * The mark moves past a snapshot once all its items are landed (seen in
 * {@link #afterLanding}), so a restart carries on from there; the ledger
 * keeps an item from being landed twice. On the first start with
 * {@code start: earliest}, every data file of the current snapshot is
 * landed first (a backfill); with {@code latest}, only later appends. A
 * table with no snapshot at the first start is followed from its first
 * append, whichever {@code start} is set.
 * <p>
 * An item's reference is the data file's location; its version is 0 and
 * its tag the table's UUID, since data files never change. Files that
 * aren't Parquet, or are encrypted, can't be split and are skipped, as are
 * a backfill's files that have row-level deletes:
 * {@code intake.table_file_skipped}. If the snapshot the intake had got to
 * has expired, so the history in between is lost,
 * {@code intake.table_history_lost} is reported and every snapshot still
 * there is followed; the same if the table was rolled back past it and
 * written to again. A rollback to a snapshot already landed lands
 * nothing. A data file deleted before it is landed is reported
 * as {@code intake.table_file_missing} and passed over.
 * <p>
 * The catalog is the table's own; it and the table are loaded on first
 * use, and again after a failure.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class IcebergTableSource implements RemoteSource
{

    /**
     * A file to land, and which step of the plan it belongs to.
     *
     * @param item the item
     * @param step its step's index in the plan
     */
    private record Planned(ListedItem item, int step)
    {
    }

    /** The intake, for events. */
    private final String intake;

    /** The table's settings. */
    private final TableSettings settings;

    /** Where progress is kept; set once storage is ready. */
    private TableProgress progress;

    /** The catalog; null until loaded. */
    private Catalog catalog;

    /** The table; null until loaded, and after a failure. */
    private Table table;

    /** Whether the last call worked. */
    private volatile boolean reachable = true;

    /** How far the intake has got; null until loaded. */
    private TableProgress.Mark mark;

    /**
     * The snapshots the plan goes through, oldest first; the mark moves to
     * each once its items are landed. For a backfill, the one snapshot.
     */
    private List<TableProgress.Mark> steps = List.of();

    /** The items not yet landed, by reference. */
    private final Map<String, Planned> outstanding = new LinkedHashMap<>();

    /** Data files found missing, by reference, not to be offered again. */
    private final Set<String> gone = new HashSet<>();

    /**
     * The problems reported, as event, snapshot and reason, so each is
     * reported once.
     */
    private final Set<String> reported = new HashSet<>();

    /** The files each snapshot added, read once, by snapshot ID. */
    private final Map<Long, List<DataFile>> added = new HashMap<>();

    /** The backfill's files, read once, for its snapshot. */
    private List<DataFile> backfillFiles;

    /** The snapshot {@link #backfillFiles} are for. */
    private long backfillOf;

    /**
     * Creates the source; nothing is loaded until it is first used.
     *
     * @param intake   the intake, for events
     * @param settings the source's settings, of type Iceberg
     */
    public IcebergTableSource(String intake, SourceSettings settings)
    {
        this.intake = intake;
        this.settings = settings.table();
    }

    /**
     * Gives the source where its progress is kept, once storage is ready.
     *
     * @param progress the store
     */
    public synchronized void attach(TableProgress progress)
    {
        this.progress = progress;
    }

    /**
     * Table data files.
     *
     * @return {@link ItemKind#TABLE_FILE}
     */
    @Override
    public ItemKind kind()
    {
        return ItemKind.TABLE_FILE;
    }

    @Override
    public synchronized List<ListedItem> list() throws IOException
    {
        Table loaded;
        try
        {
            loaded = table();
            loaded.refresh();
        }
        catch (RuntimeException failure)
        {
            throw failed(failure);
        }
        if (mark == null)
        {
            mark = progress.load().orElse(null);
        }
        Snapshot current = loaded.currentSnapshot();
        if (mark == null)
        {
            // An empty table has nothing to backfill: follow it from the
            // start, so its first append is landed.
            mark = current == null
                    ? new TableProgress.Mark(TableProgress.NONE, false)
                    : new TableProgress.Mark(current.snapshotId(),
                            settings.earliest());
            progress.save(mark);
        }
        if (current == null)
        {
            reachable = true;
            return List.of();
        }
        List<Planned> plan;
        try
        {
            plan = mark.backfill() ? backfill(loaded)
                    : follow(loaded, current);
        }
        catch (RuntimeException failure)
        {
            throw failed(failure);
        }
        outstanding.clear();
        for (Planned planned : plan)
        {
            if (!gone.contains(planned.item().ref()))
            {
                outstanding.put(planned.item().ref(), planned);
            }
        }
        advance();
        reachable = true;
        List<ListedItem> items = new ArrayList<>();
        for (Planned planned : outstanding.values())
        {
            items.add(planned.item());
        }
        return items;
    }

    /**
     * Plans a backfill: every data file of the mark's snapshot, or of the
     * current one if it has expired.
     *
     * @param loaded the table
     * @return the files, in order of their sequence numbers
     */
    private List<Planned> backfill(Table loaded)
    {
        Snapshot snapshot = loaded.snapshot(mark.snapshotId());
        if (snapshot == null)
        {
            report("intake.table_history_lost", mark.snapshotId(), null);
            snapshot = loaded.currentSnapshot();
            mark = new TableProgress.Mark(snapshot.snapshotId(), true);
            progress.save(mark);
        }
        if (backfillFiles == null || backfillOf != snapshot.snapshotId())
        {
            List<DataFile> files = new ArrayList<>();
            boolean deletes = false;
            try (CloseableIterable<FileScanTask> tasks = loaded.newScan()
                    .useSnapshot(snapshot.snapshotId()).planFiles())
            {
                for (FileScanTask task : tasks)
                {
                    if (task.deletes().isEmpty())
                    {
                        files.add(task.file());
                    }
                    else
                    {
                        deletes = true;
                    }
                }
            }
            catch (IOException failure)
            {
                throw new org.apache.iceberg.exceptions.RuntimeIOException(
                        failure);
            }
            if (deletes)
            {
                report("intake.table_file_skipped", snapshot.snapshotId(),
                        "DELETES");
            }
            files.sort(Comparator.comparing(
                    (DataFile file) -> file.fileSequenceNumber() == null ? 0L
                            : file.fileSequenceNumber())
                    .thenComparing(file -> file.location()));
            backfillFiles = files;
            backfillOf = snapshot.snapshotId();
        }
        steps = List.of(new TableProgress.Mark(snapshot.snapshotId(),
                false));
        return items(loaded, snapshot.snapshotId(), backfillFiles, 0);
    }

    /**
     * Plans the snapshots after the mark, along the current snapshot's
     * ancestry.
     *
     * @param loaded  the table
     * @param current its current snapshot
     * @return the files the appends among them added, oldest first
     */
    private List<Planned> follow(Table loaded, Snapshot current)
    {
        if (loaded.snapshot(mark.snapshotId()) != null
                && SnapshotUtil.isAncestorOf(loaded, mark.snapshotId(),
                        current.snapshotId()))
        {
            // Rolled back to a snapshot already landed: nothing new.
            steps = List.of();
            return List.of();
        }
        List<Snapshot> after = new ArrayList<>();
        Snapshot snapshot = current;
        boolean found = false;
        while (snapshot != null)
        {
            if (snapshot.snapshotId() == mark.snapshotId())
            {
                found = true;
                break;
            }
            after.add(snapshot);
            Long parent = snapshot.parentId();
            if (parent == null ? mark.snapshotId() == TableProgress.NONE
                    : parent == mark.snapshotId())
            {
                found = true;
                break;
            }
            snapshot = parent == null ? null : loaded.snapshot(parent);
        }
        if (!found)
        {
            report("intake.table_history_lost", mark.snapshotId(), null);
        }
        Collections.reverse(after);
        List<TableProgress.Mark> planSteps = new ArrayList<>();
        List<Planned> plan = new ArrayList<>();
        for (Snapshot step : after)
        {
            planSteps.add(new TableProgress.Mark(step.snapshotId(), false));
            String operation = step.operation();
            if (DataOperations.APPEND.equals(operation))
            {
                plan.addAll(items(loaded, step.snapshotId(),
                        added.computeIfAbsent(step.snapshotId(),
                                id -> addedFiles(loaded, step)),
                        planSteps.size() - 1));
            }
            else if (!DataOperations.REPLACE.equals(operation))
            {
                report("intake.table_rewrite", step.snapshotId(), operation);
            }
        }
        steps = planSteps;
        return plan;
    }

    /**
     * The data files a snapshot added.
     *
     * @param loaded   the table
     * @param snapshot the snapshot
     * @return the files, sorted by location
     */
    private static List<DataFile> addedFiles(Table loaded, Snapshot snapshot)
    {
        List<DataFile> files = new ArrayList<>();
        for (DataFile file : SnapshotChanges.builderFor(loaded)
                .snapshot(snapshot).build().addedDataFiles())
        {
            files.add(file.copyWithoutStats());
        }
        files.sort(Comparator.comparing(file -> file.location()));
        return files;
    }

    /**
     * Turns data files into items, leaving out those that can't be split.
     *
     * @param loaded     the table
     * @param snapshotId the snapshot they come from, for events
     * @param files      the files
     * @param step       their step in the plan
     * @return the items
     */
    private List<Planned> items(Table loaded, long snapshotId,
            List<DataFile> files, int step)
    {
        String uuid = loaded.uuid().toString();
        List<Planned> items = new ArrayList<>();
        for (DataFile file : files)
        {
            if (file.format() != FileFormat.PARQUET)
            {
                report("intake.table_file_skipped", snapshotId, "FORMAT");
            }
            else if (file.keyMetadata() != null)
            {
                report("intake.table_file_skipped", snapshotId, "ENCRYPTED");
            }
            else
            {
                items.add(new Planned(new ListedItem(file.location(),
                        file.fileSizeInBytes(), 0, uuid), step));
            }
        }
        return items;
    }

    /**
     * Moves the mark past every step whose items are all landed, and saves
     * it if it moved.
     */
    private void advance()
    {
        int firstOpen = steps.size();
        for (Planned planned : outstanding.values())
        {
            firstOpen = Math.min(firstOpen, planned.step());
        }
        if (firstOpen == 0)
        {
            return;
        }
        TableProgress.Mark next = steps.get(firstOpen - 1);
        if (!next.equals(mark))
        {
            mark = next;
            progress.save(mark);
            backfillFiles = null;
            Set<Long> ahead = new HashSet<>();
            for (TableProgress.Mark step : steps.subList(firstOpen,
                    steps.size()))
            {
                ahead.add(step.snapshotId());
            }
            added.keySet().retainAll(ahead);
        }
    }

    @Override
    public synchronized InputStream open(ListedItem item, long offset)
            throws IOException
    {
        SeekableInputStream stream;
        try
        {
            InputFile file = table().io().newInputFile(item.ref(),
                    item.size());
            if (!file.exists())
            {
                throw missing(item);
            }
            stream = file.newStream();
            stream.seek(offset);
        }
        catch (ItemLander.ItemChangedException missing)
        {
            throw missing;
        }
        catch (NotFoundException | FileNotFoundException
                | NoSuchFileException gone)
        {
            // Some file stores say a file is missing only by failing.
            throw missing(item);
        }
        catch (IOException | RuntimeException failure)
        {
            throw failed(failure);
        }
        return new Watched(stream);
    }

    /**
     * Passes over a data file that has gone, deleted after its snapshot
     * expired, so the intake isn't held up by it.
     *
     * @param item the file
     * @return the exception that says the item has changed
     */
    private ItemLander.ItemChangedException missing(ListedItem item)
    {
        gone.add(item.ref());
        outstanding.remove(item.ref());
        report("intake.table_file_missing",
                mark == null ? 0 : mark.snapshotId(), null);
        advance();
        return new ItemLander.ItemChangedException();
    }

    /**
     * Notes that an item is landed, and moves the mark on if its snapshot
     * is done.
     *
     * @param item the landed item
     */
    @Override
    public synchronized void afterLanding(ListedItem item)
    {
        if (outstanding.remove(item.ref()) != null)
        {
            advance();
        }
    }

    @Override
    public boolean reachable()
    {
        return reachable;
    }

    /** Nothing to check: a table intake never writes to the table. */
    @Override
    public void checkWritable()
    {
    }

    @Override
    public synchronized void close()
    {
        table = null;
        if (catalog instanceof AutoCloseable closeable)
        {
            try
            {
                closeable.close();
            }
            catch (Exception ignored)
            {
                // Nothing more to release.
            }
        }
        catalog = null;
    }

    /**
     * The table, loading the catalog and the table if they aren't.
     *
     * @return the table
     */
    private Table table()
    {
        if (table == null)
        {
            if (catalog == null)
            {
                catalog = CatalogUtil.buildIcebergCatalog(
                        settings.catalogName(), settings.catalog(), null);
            }
            table = catalog.loadTable(settings.identifier());
        }
        return table;
    }

    /**
     * Reports a problem with a snapshot, once.
     *
     * @param event      the event's name
     * @param snapshotId the snapshot
     * @param reason     the reason, if there is one
     */
    private void report(String event, long snapshotId, String reason)
    {
        if (!reported.add(event + "/" + snapshotId + "/" + reason))
        {
            return;
        }
        Fields fields = Fields.of(Attr.INTAKE, intake)
                .with(Attr.SNAPSHOT_ID, snapshotId);
        if (reason != null)
        {
            fields = fields.with(Attr.REASON, reason);
        }
        Telemetry.get().auditWarn(event, fields);
    }

    /**
     * Turns a failure into one that says why, with no path, and drops the
     * table so it is loaded again.
     *
     * @param failure what was thrown
     * @return the exception to throw
     */
    private IntakeUnavailableException failed(Exception failure)
    {
        reachable = false;
        table = null;
        return new IntakeUnavailableException(classify(failure), failure);
    }

    /**
     * Says why a call to the catalog or storage failed.
     *
     * @param failure what was thrown
     * @return the problem
     */
    static IntakeProblem classify(Exception failure)
    {
        return switch (failure)
        {
            case NotAuthorizedException refused -> IntakeProblem.AUTH;
            case ForbiddenException refused -> IntakeProblem.PERMISSION;
            case NoSuchTableException missing -> IntakeProblem.NOT_FOUND;
            case NoSuchNamespaceException missing -> IntakeProblem.NOT_FOUND;
            default -> IntakeProblem.NETWORK;
        };
    }

    /**
     * A data file's bytes; a failed read marks the source unreachable, so
     * the item resumes later.
     */
    private final class Watched extends FilterInputStream
    {

        /**
         * Wraps the file's stream.
         *
         * @param input the stream
         */
        Watched(InputStream input)
        {
            super(input);
        }

        @Override
        public int read() throws IOException
        {
            try
            {
                return super.read();
            }
            catch (IOException | RuntimeException failure)
            {
                throw broken(failure);
            }
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            try
            {
                return super.read(buffer, offset, length);
            }
            catch (IOException | RuntimeException failure)
            {
                throw broken(failure);
            }
        }

        /** Closes the stream; a failure to close is ignored. */
        @Override
        public void close()
        {
            try
            {
                super.close();
            }
            catch (IOException | RuntimeException failure)
            {
                // The read already failed, or there is nothing to keep.
            }
        }

        /**
         * Marks the source unreachable after a failed read.
         *
         * @param failure what the read threw
         * @return the exception to throw
         */
        private IOException broken(Exception failure)
        {
            reachable = false;
            return new IntakeUnavailableException(IntakeProblem.NETWORK,
                    failure);
        }
    }
}
