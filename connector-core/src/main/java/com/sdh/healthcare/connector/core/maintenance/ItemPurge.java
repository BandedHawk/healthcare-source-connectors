/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.maintenance;

import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.checkpoint.TableCommitter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Purges items from a table (CONTROL-API-DESIGN.md §11, P4): every row of
 * the items is left out of each data file that may hold one, the file
 * written again without them, in one Iceberg rewrite per run. Only an
 * operator's request, approved by a second person, asks for it.
 *
 * <p>It runs as a purge of table maintenance ({@link TableMaintenance}),
 * so the files it replaces are deleted from storage in the same run: the
 * run commits its marker and expires every snapshot before it. Each run
 * rewrites at most a budget of bytes per table, as the pod's writer is
 * held meanwhile; a table with more left is taken further at the next.
 *
 * <p>Files are found by their {@code item_id} bounds, which the product's
 * time-ordered item IDs keep narrow. It never logs a value or an object
 * key.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ItemPurge implements TableMaintenance.Purge
{

    /** The column naming a row's item, in every table that has items. */
    public static final String ITEM_ID = RecordsTable.ITEM_ID;

    /** The most bytes of data files a run rewrites in one table. */
    public static final long BUDGET = 4L << 30;

    /**
     * What a run did in one table.
     *
     * @param files the data files rewritten or removed
     * @param rows  each item's rows left out, by item
     * @param more  whether files were left for the next run
     */
    public record Done(long files, Map<UUID, Long> rows, boolean more)
    {

        /**
         * Copies the counts.
         *
         * @param files the files
         * @param rows  the rows by item
         * @param more  whether more is left
         */
        public Done
        {
            rows = Map.copyOf(rows);
        }
    }

    /** Commits the rewrites. */
    private final TableCommitter committer;

    /** The items purged. */
    private final Set<UUID> items;

    /** The most bytes rewritten in a table each run. */
    private final long budget;

    /** What each table's run did, by table name, in the order run. */
    private final Map<String, Done> done = new LinkedHashMap<>();

    /** The tables run over, in order. */
    private final List<Table> tables = new ArrayList<>();

    /**
     * Creates a run.
     *
     * @param committer commits the rewrites
     * @param items     the items to purge
     * @param budget    the most bytes of data files rewritten in a table
     * @throws IllegalArgumentException if there are no items, or the
     *                                  budget isn't positive
     */
    public ItemPurge(TableCommitter committer, Set<UUID> items, long budget)
    {
        if (items == null || items.isEmpty())
        {
            throw new IllegalArgumentException("A purge needs an item");
        }
        if (budget <= 0)
        {
            throw new IllegalArgumentException("The budget must be"
                    + " positive");
        }
        this.committer = Objects.requireNonNull(committer, "committer");
        this.items = Set.copyOf(items);
        this.budget = budget;
    }

    /**
     * Whether a table has items at all.
     *
     * @param table the table
     * @return true if it has the item column
     */
    public static boolean hasItems(Table table)
    {
        return table.schema().findField(ITEM_ID) != null;
    }

    /**
     * Rewrites the table's files that hold rows of the items, up to the
     * budget, in one commit.
     *
     * @return how many data files were rewritten or removed
     * @throws IllegalStateException if a file's rows don't add up, or a
     *                               file has delete files
     */
    @Override
    public int purge(Table table, Instant now)
    {
        tables.add(table);
        Map<UUID, Long> rows = new TreeMap<>();
        if (!hasItems(table) || table.currentSnapshot() == null)
        {
            done.put(table.name(), new Done(0, rows, false));
            return 0;
        }
        long start = table.currentSnapshot().snapshotId();
        List<DataFile> dropped = new ArrayList<>();
        List<DataFile> added = new ArrayList<>();
        long bytes = 0;
        boolean more = false;
        try
        {
            for (DataFile file : candidates(table, items))
            {
                if (bytes > 0 && bytes + file.fileSizeInBytes() > budget)
                {
                    more = true;
                    break;
                }
                Map<UUID, Long> left = new TreeMap<>();
                TableMaintenance.Copied copied = TableMaintenance.copyRows(
                        table, List.of(file), "purge", row ->
                        {
                            Object item = row.getField(ITEM_ID);
                            if (item instanceof UUID id && items.contains(id))
                            {
                                left.merge(id, 1L, Long::sum);
                                return null;
                            }
                            return row;
                        });
                long out = left.values().stream().mapToLong(Long::longValue)
                        .sum();
                if (copied.read() != file.recordCount()
                        || copied.kept() + out != copied.read())
                {
                    if (copied.file() != null)
                    {
                        added.add(copied.file());
                    }
                    throw new IllegalStateException("Purge row count"
                            + " mismatch");
                }
                if (out == 0)
                {
                    // The file's bounds allowed an item, but no row was
                    // one: nothing to change.
                    delete(table, copied.file());
                    continue;
                }
                if (copied.file() != null)
                {
                    added.add(copied.file());
                }
                dropped.add(file);
                left.forEach((id, count) -> rows.merge(id, count,
                        Long::sum));
                bytes += file.fileSizeInBytes();
            }
            if (!dropped.isEmpty())
            {
                committer.commit(table, transaction ->
                {
                    RewriteFiles rewrite = transaction.newRewrite()
                            .validateFromSnapshot(start);
                    dropped.forEach(rewrite::deleteFile);
                    added.forEach(rewrite::addFile);
                    rewrite.commit();
                });
            }
        }
        catch (RuntimeException failure)
        {
            added.forEach(file -> delete(table, file));
            throw failure;
        }
        done.put(table.name(), new Done(dropped.size(), rows, more));
        return dropped.size();
    }

    /**
     * What each table's run did, by table name, in the order run.
     *
     * @return it
     */
    public Map<String, Done> done()
    {
        return Map.copyOf(done);
    }

    /**
     * The tables run over, in the order run.
     *
     * @return them
     */
    public List<Table> tables()
    {
        return List.copyOf(tables);
    }

    /**
     * Whether the run changed nothing in any table and left nothing for
     * the next: the items may be gone, which {@link #left} confirms.
     *
     * @return true if it did
     */
    public boolean quiet()
    {
        return done.values().stream().allMatch(each -> each.files() == 0
                && !each.more());
    }

    /**
     * How many rows of the items a table still holds.
     *
     * @param table the table
     * @param items the items
     * @return rows by item; an item with none is left out
     */
    public static Map<UUID, Long> left(Table table, Set<UUID> items)
    {
        Map<UUID, Long> counts = new TreeMap<>();
        if (!hasItems(table) || table.currentSnapshot() == null
                || items.isEmpty())
        {
            return counts;
        }
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(matching(items)).select(ITEM_ID).build())
        {
            for (Record row : rows)
            {
                if (row.getField(ITEM_ID) instanceof UUID id
                        && items.contains(id))
                {
                    counts.merge(id, 1L, Long::sum);
                }
            }
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        return counts;
    }

    /**
     * The filter matching the items' rows.
     *
     * @param items the items
     * @return it
     */
    private static Expression matching(Set<UUID> items)
    {
        return Expressions.in(ITEM_ID, items.toArray());
    }

    /**
     * The data files that may hold rows of the items, by their column
     * bounds.
     *
     * @param table the table
     * @param items the items
     * @return them
     * @throws IllegalStateException if one has delete files
     */
    private static List<DataFile> candidates(Table table, Set<UUID> items)
    {
        List<DataFile> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan()
                .filter(matching(items)).planFiles())
        {
            for (FileScanTask task : tasks)
            {
                if (!task.deletes().isEmpty())
                {
                    throw new IllegalStateException("A purge can't rewrite"
                            + " a file with delete files");
                }
                files.add(task.file());
            }
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        return files;
    }

    /**
     * Deletes a file that was never committed. One left behind is removed
     * by orphan cleanup.
     *
     * @param table the table
     * @param file  the file; null for none
     */
    private static void delete(Table table, DataFile file)
    {
        if (file == null)
        {
            return;
        }
        try
        {
            table.io().deleteFile(file.location());
        }
        catch (RuntimeException ignored)
        {
            // Orphan cleanup removes it.
        }
    }
}
