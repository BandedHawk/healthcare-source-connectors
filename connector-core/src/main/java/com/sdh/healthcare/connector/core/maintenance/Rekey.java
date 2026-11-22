/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.maintenance;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.checkpoint.TableCommitter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Re-keys a table's stored data from one KEK to another
 * (CONTROL-API-DESIGN.md §11): for each data file that may hold a row
 * whose batch data key is wrapped with the old KEK, it unwraps that key,
 * wraps it with the new one, checks the new wrap unwraps to the same key,
 * and writes the file again, in one Iceberg rewrite per run. The chunks'
 * ciphertext doesn't change, only the wrapped keys and their KEK
 * references: the chunks' AAD binds their item, record and chunk, not the
 * KEK.
 *
 * <p>It runs as a purge of table maintenance ({@link TableMaintenance}),
 * so the files it replaces are deleted from storage in the same run: the
 * run commits its marker and expires every snapshot before it. Each run
 * rewrites at most a budget of bytes per table, as the pod's writer is
 * held meanwhile; a table with more left is taken further at the next.
 *
 * <p>It never logs a key, a value or an object key, and holds no
 * plaintext data key past the row it re-wraps.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Rekey implements TableMaintenance.Purge
{

    /**
     * The column naming a row's KEK: {@code kek_ref}, in every table with
     * wrapped keys.
     */
    public static final String KEK_REF = RecordsTable.KEK_REF;

    /** The column holding a row's wrapped data key. */
    public static final String BDK_WRAPPED = RecordsTable.BDK_WRAPPED;

    /** The most bytes of data files a run rewrites in one table. */
    public static final long BUDGET = 4L << 30;

    /**
     * What a run did in one table.
     *
     * @param files the data files rewritten
     * @param rows  the rows re-wrapped
     * @param more  whether files were left for the next run
     */
    public record Done(long files, long rows, boolean more)
    {
    }

    /** Commits the rewrites. */
    private final TableCommitter committer;

    /** Unwraps with the old KEK and wraps with the new. */
    private final KeyProvider keys;

    /** The old KEK. */
    private final String from;

    /** The new KEK. */
    private final String to;

    /** The most bytes rewritten in a table each run. */
    private final long budget;

    /**
     * Each old wrapped key re-wrapped this run, by its bytes: a batch's
     * rows share one data key, unwrapped and wrapped once. Only wrapped
     * keys are kept.
     */
    private final Map<ByteBuffer, WrappedKey> rewrapped = new HashMap<>();

    /** What each table's run did, by table name, in the order run. */
    private final Map<String, Done> done = new LinkedHashMap<>();

    /** The tables run over, in order. */
    private final List<Table> tables = new ArrayList<>();

    /**
     * Creates a run.
     *
     * @param committer commits the rewrites
     * @param keys      unwraps with the old KEK and wraps with the new
     * @param from      the old KEK
     * @param to        the new KEK
     * @param budget    the most bytes of data files rewritten in a table
     * @throws IllegalArgumentException if the KEKs are the same or blank,
     *                                  or the budget isn't positive
     */
    public Rekey(TableCommitter committer, KeyProvider keys, String from,
            String to, long budget)
    {
        if (from == null || from.isBlank() || to == null || to.isBlank()
                || from.equals(to))
        {
            throw new IllegalArgumentException("A re-key needs two"
                    + " different KEKs");
        }
        if (budget <= 0)
        {
            throw new IllegalArgumentException("The budget must be"
                    + " positive");
        }
        this.committer = Objects.requireNonNull(committer, "committer");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.from = from;
        this.to = to;
        this.budget = budget;
    }

    /**
     * Whether a table holds wrapped keys at all.
     *
     * @param table the table
     * @return true if it has both columns
     */
    public static boolean wraps(Table table)
    {
        return table.schema().findField(KEK_REF) != null
                && table.schema().findField(BDK_WRAPPED) != null;
    }

    /**
     * Rewrites the table's files that hold rows wrapped with the old KEK,
     * up to the budget, in one commit.
     *
     * @return how many data files were rewritten
     * @throws IllegalStateException if a file's rows don't all copy, or a
     *                               new wrap doesn't give the key back
     */
    @Override
    public int purge(Table table, Instant now)
    {
        tables.add(table);
        if (!wraps(table) || table.currentSnapshot() == null)
        {
            done.put(table.name(), new Done(0, 0, false));
            return 0;
        }
        long start = table.currentSnapshot().snapshotId();
        List<DataFile> dropped = new ArrayList<>();
        List<DataFile> added = new ArrayList<>();
        long rows = 0;
        long bytes = 0;
        boolean more = false;
        try
        {
            for (DataFile file : candidates(table))
            {
                if (bytes > 0 && bytes + file.fileSizeInBytes() > budget)
                {
                    more = true;
                    break;
                }
                long[] changed = new long[1];
                TableMaintenance.Copied copied = TableMaintenance.copyRows(
                        table, List.of(file), "rekey",
                        row -> rewrap(row, changed));
                if (copied.file() != null)
                {
                    added.add(copied.file());
                }
                if (copied.read() != file.recordCount()
                        || copied.kept() != copied.read())
                {
                    throw new IllegalStateException("Re-key row count"
                            + " mismatch");
                }
                if (changed[0] == 0)
                {
                    // The file's bounds allowed the old KEK, but no row had
                    // it: nothing to change.
                    added.remove(copied.file());
                    delete(table, copied.file());
                    continue;
                }
                dropped.add(file);
                rows += changed[0];
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
     * the next: the tables may be done, which {@link #left} confirms.
     *
     * @return true if it did
     */
    public boolean quiet()
    {
        return done.values().stream().allMatch(each -> each.files() == 0
                && !each.more());
    }

    /**
     * How many rows of a table are still wrapped with a KEK: read from the
     * {@code kek_ref} column alone of the files that may hold it.
     *
     * @param table the table
     * @param kek   the KEK
     * @return how many rows
     */
    public static long left(Table table, String kek)
    {
        if (!wraps(table) || table.currentSnapshot() == null)
        {
            return 0;
        }
        long count = 0;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(KEK_REF, kek)).select(KEK_REF)
                .build())
        {
            for (Record row : rows)
            {
                if (kek.equals(row.getField(KEK_REF)))
                {
                    count++;
                }
            }
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        return count;
    }

    /**
     * How many rows of a table each KEK wraps, read from the
     * {@code kek_ref} column alone.
     *
     * @param table the table
     * @return rows by KEK, in order; empty if it holds no wrapped keys
     */
    public static Map<String, Long> byKek(Table table)
    {
        Map<String, Long> counts = new TreeMap<>();
        if (!wraps(table) || table.currentSnapshot() == null)
        {
            return counts;
        }
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .select(KEK_REF).build())
        {
            for (Record row : rows)
            {
                Object kek = row.getField(KEK_REF);
                counts.merge(kek == null ? "" : kek.toString(), 1L,
                        Long::sum);
            }
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        return counts;
    }

    /**
     * The data files that may hold rows wrapped with the old KEK, by their
     * column bounds.
     *
     * @param table the table
     * @return them
     */
    private List<DataFile> candidates(Table table)
    {
        List<DataFile> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan()
                .filter(Expressions.equal(KEK_REF, from)).planFiles())
        {
            for (FileScanTask task : tasks)
            {
                if (!task.deletes().isEmpty())
                {
                    throw new IllegalStateException("A re-key can't rewrite"
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
     * A row with its data key wrapped with the new KEK, if the old one
     * wrapped it; otherwise the row as it is.
     *
     * @param row     the row
     * @param changed counts the rows changed
     * @return the row to write
     */
    private Record rewrap(Record row, long[] changed)
    {
        if (!from.equals(row.getField(KEK_REF)))
        {
            return row;
        }
        ByteBuffer stored = ((ByteBuffer) row.getField(BDK_WRAPPED))
                .duplicate();
        byte[] old = new byte[stored.remaining()];
        stored.get(old);
        WrappedKey fresh = rewrapped.computeIfAbsent(ByteBuffer.wrap(old),
                ignored -> wrapAgain(old));
        row.setField(KEK_REF, fresh.keyRef());
        row.setField(BDK_WRAPPED, ByteBuffer.wrap(fresh.wrapped()));
        changed[0]++;
        return row;
    }

    /**
     * Unwraps a data key with the old KEK and wraps it with the new,
     * checking the new wrap gives the same key back.
     *
     * @param old the key wrapped with the old KEK
     * @return the key wrapped with the new
     * @throws IllegalStateException if the new wrap doesn't round-trip
     */
    private WrappedKey wrapAgain(byte[] old)
    {
        byte[] plain = keys.unwrap(new WrappedKey(from, old));
        byte[] back = null;
        try
        {
            WrappedKey fresh = keys.wrap(to, plain);
            back = keys.unwrap(fresh);
            if (!MessageDigest.isEqual(plain, back))
            {
                throw new IllegalStateException("A re-wrapped data key did"
                        + " not round-trip");
            }
            return fresh;
        }
        finally
        {
            Arrays.fill(plain, (byte) 0);
            if (back != null)
            {
                Arrays.fill(back, (byte) 0);
            }
        }
    }

    /**
     * Deletes a file that was never committed. One left behind is removed
     * by orphan cleanup.
     *
     * @param table the table
     * @param file  the file
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
