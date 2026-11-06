/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.crypto.MacedJson;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * The items an intake gave up part way, whose committed chunks are kept
 * for a grace period and then removed (design §5.3.13). Decided 2026-11-04
 * with the user: an item given up is recorded in the same commit that
 * clears its checkpoint, so the record can't be lost; table maintenance
 * removes its rows once the grace period has passed, and takes it off the
 * record in the same commit.
 * <p>
 * The record is the {@value LandingTables#LANDED_ITEMS} table property
 * {@code sdh.given-up.<intake>}: the item IDs and when each was given up,
 * MAC'd like a checkpoint, so nobody without the ID key can add an item
 * to be removed. An item with a summary row was landed in full, and is
 * never removed: it is taken off the record and reported instead. It holds
 * no PHI.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class GivenUpItems implements TableMaintenance.Purge
{

    /** The start of the table property's name; the intake follows. */
    public static final String PROPERTY_PREFIX = "sdh.given-up.";

    /** The label of the record's MAC. */
    private static final String LABEL = "sdh-given-up";

    /**
     * One item given up.
     *
     * @param itemId    the item
     * @param givenUpAt when it was given up
     */
    public record Entry(UUID itemId, Instant givenUpAt)
    {
    }

    /**
     * An intake's record, as MAC'd: the intake is part of it, so one
     * intake's record can't be copied to another.
     *
     * @param intake  the intake
     * @param entries the items, oldest first
     */
    private record Stored(String intake, List<Entry> entries)
    {
    }

    /** Commits under the intake's lease. */
    private final FencedCommitter committer;

    /** Makes the MACs, with the ID key. */
    private final RecordIds ids;

    /** The intake. */
    private final String intake;

    /** How long a given-up item's chunks are kept. */
    private final Duration grace;

    /**
     * Creates the purge of an intake's given-up items.
     *
     * @param committer commits under the intake's lease
     * @param ids       makes the MACs, with the ID key
     * @param intake    the intake
     * @param grace     how long a given-up item's chunks are kept
     */
    public GivenUpItems(FencedCommitter committer, RecordIds ids,
            String intake, Duration grace)
    {
        this.committer = committer;
        this.ids = ids;
        this.intake = intake;
        this.grace = grace;
    }

    /**
     * The table property an intake's record is kept in.
     *
     * @param intake the intake
     * @return the property's name
     */
    public static String propertyKey(String intake)
    {
        return PROPERTY_PREFIX + intake;
    }

    /**
     * The items an intake has given up and not yet removed.
     *
     * @param table  the {@value LandingTables#LANDED_ITEMS} table, as
     *               last refreshed
     * @param ids    makes the MACs, with the ID key
     * @param intake the intake
     * @return the items, oldest first
     * @throws CheckpointTamperedException if the record's MAC doesn't
     *                                     verify, or it names another
     *                                     intake
     */
    public static List<Entry> entries(Table table, RecordIds ids,
            String intake)
    {
        return decode(table.properties().get(propertyKey(intake)), ids,
                intake);
    }

    /**
     * Adds an item to an intake's record, on a transaction that clears its
     * checkpoint.
     *
     * @param transaction the transaction, on the table
     * @param ids         makes the MACs, with the ID key
     * @param intake      the intake
     * @param entry       the item given up
     */
    static void record(Transaction transaction, RecordIds ids,
            String intake, Entry entry)
    {
        List<Entry> entries = new ArrayList<>(decode(transaction.table()
                .properties().get(propertyKey(intake)), ids, intake));
        entries.add(entry);
        transaction.updateProperties().set(propertyKey(intake),
                codec(ids).encode(new Stored(intake, entries))).commit();
    }

    /**
     * Removes the chunks of the items given up more than the grace period
     * ago, and takes them off the record, in one commit. Files holding
     * only their rows are dropped; files they share with other items, after
     * compaction, are rewritten without them. Each item removed is audited
     * as {@code item.purged}, by ID.
     *
     * @param table the {@value LandingTables#LANDED_ITEMS} table
     * @param now   the time of the run
     * @return how many data files were dropped or rewritten
     */
    @Override
    public int purge(Table table, Instant now)
    {
        table.refresh();
        List<Entry> entries = entries(table, ids, intake);
        Set<UUID> due = new HashSet<>();
        Set<UUID> refused = new HashSet<>();
        for (Entry entry : entries)
        {
            if (!entry.givenUpAt().plus(grace).isAfter(now))
            {
                (landed(table, entry.itemId()) ? refused : due)
                        .add(entry.itemId());
            }
        }
        if (due.isEmpty() && refused.isEmpty())
        {
            return 0;
        }
        long start = table.currentSnapshot().snapshotId();
        List<DataFile> dropped = new ArrayList<>();
        List<DataFile> added = new ArrayList<>();
        try
        {
            for (DataFile file : files(table, due))
            {
                TableMaintenance.Copied copied = TableMaintenance.copyRows(
                        table, List.of(file), row -> keep(row, due),
                        "purge");
                if (copied.read() != file.recordCount())
                {
                    throw new IllegalStateException(
                            "Purge row count mismatch");
                }
                dropped.add(file);
                if (copied.file() != null)
                {
                    added.add(copied.file());
                }
            }
            List<Entry> kept = new ArrayList<>();
            for (Entry entry : entries)
            {
                if (!due.contains(entry.itemId())
                        && !refused.contains(entry.itemId()))
                {
                    kept.add(entry);
                }
            }
            committer.commit(table, transaction ->
            {
                if (!dropped.isEmpty())
                {
                    OverwriteFiles overwrite = transaction.newOverwrite()
                            .validateFromSnapshot(start)
                            .conflictDetectionFilter(Expressions.in(
                                    LandingTables.ITEM_ID, due.toArray()))
                            .validateNoConflictingData()
                            .validateNoConflictingDeletes();
                    dropped.forEach(overwrite::deleteFile);
                    added.forEach(overwrite::addFile);
                    overwrite.commit();
                }
                if (kept.isEmpty())
                {
                    transaction.updateProperties()
                            .remove(propertyKey(intake)).commit();
                }
                else
                {
                    transaction.updateProperties().set(propertyKey(intake),
                            codec(ids).encode(new Stored(intake, kept)))
                            .commit();
                }
            });
        }
        catch (RuntimeException failure)
        {
            added.forEach(file -> table.io().deleteFile(file.location()));
            throw failure;
        }
        for (UUID itemId : due)
        {
            Telemetry.get().audit("item.purged", Fields.of(Attr.ITEM_ID,
                    itemId).with(Attr.INTAKE, intake));
        }
        for (UUID itemId : refused)
        {
            Telemetry.get().auditWarn("item.purge_refused",
                    Fields.of(Attr.ITEM_ID, itemId).with(Attr.INTAKE,
                            intake).with(Attr.REASON, "LANDED"));
        }
        return dropped.size();
    }

    /**
     * Whether a row is kept: any row of another item. A summary row of an
     * item due for removal means it was landed after all, and stops the
     * purge, so a landed item is never removed in part.
     *
     * @param row the row
     * @param due the items being removed
     * @return true to keep it
     * @throws IllegalStateException if it is a summary row of one of them
     */
    private static boolean keep(Record row, Set<UUID> due)
    {
        if (!due.contains(row.getField(LandingTables.ITEM_ID)))
        {
            return true;
        }
        Object chunk = row.getField(LandingTables.CHUNK_INDEX);
        if (chunk instanceof Integer index
                && index == LandingTables.SUMMARY_ROW)
        {
            throw new IllegalStateException("A given-up item has a summary");
        }
        return false;
    }

    /**
     * Whether an item was landed in full, and so must never be removed.
     *
     * @param table  the table
     * @param itemId the item
     * @return true if it has a summary row
     */
    private static boolean landed(Table table, UUID itemId)
    {
        try
        {
            return LandedItems.summary(table, itemId).isPresent();
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
    }

    /**
     * The data files that may hold rows of the items.
     *
     * @param table the table
     * @param items the items
     * @return the files
     * @throws IllegalStateException if one has delete files, which the
     *                               landing tables never have
     */
    private static List<DataFile> files(Table table, Set<UUID> items)
    {
        List<DataFile> files = new ArrayList<>();
        if (items.isEmpty())
        {
            return files;
        }
        try (CloseableIterable<FileScanTask> tasks = table.newScan()
                .filter(Expressions.in(LandingTables.ITEM_ID,
                        items.toArray()))
                .planFiles())
        {
            for (FileScanTask task : tasks)
            {
                if (!task.deletes().isEmpty())
                {
                    throw new IllegalStateException(
                            "A landing table has delete files");
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
     * Reads a record, checking its MAC and its intake.
     *
     * @param stored the property's value; null if there is none
     * @param ids    makes the MACs, with the ID key
     * @param intake the intake it must be for
     * @return the items; empty if there is no record
     * @throws CheckpointTamperedException if it doesn't verify
     */
    private static List<Entry> decode(String stored, RecordIds ids,
            String intake)
    {
        if (stored == null)
        {
            return List.of();
        }
        Stored record;
        try
        {
            record = codec(ids).decode(stored);
        }
        catch (MacedJson.Tampered tampered)
        {
            throw new CheckpointTamperedException();
        }
        if (!record.intake().equals(intake))
        {
            throw new CheckpointTamperedException();
        }
        return record.entries();
    }

    /**
     * Writes and reads records with their MAC.
     *
     * @param ids makes the MACs, with the ID key
     * @return the codec
     */
    private static MacedJson<Stored> codec(RecordIds ids)
    {
        return new MacedJson<>(ids, LABEL, GivenUpItems::canonical,
                GivenUpItems::parse);
    }

    /**
     * A record's fields: the intake, and the items as
     * {@code <itemId>@<millis>}, comma-separated.
     *
     * @param stored the record
     * @return its fields
     */
    private static Map<String, Object> canonical(Stored stored)
    {
        List<String> items = new ArrayList<>(stored.entries().size());
        for (Entry entry : stored.entries())
        {
            items.add(entry.itemId() + "@"
                    + entry.givenUpAt().toEpochMilli());
        }
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("intake", stored.intake());
        fields.put("items", String.join(",", items));
        return fields;
    }

    /**
     * Rebuilds a record from its parsed fields.
     *
     * @param fields the fields
     * @return the record
     * @throws IllegalArgumentException if a field is missing or malformed
     */
    private static Stored parse(Map<String, Object> fields)
    {
        Object intake = fields.get("intake");
        Object items = fields.get("items");
        if (!(intake instanceof String name)
                || !(items instanceof String list))
        {
            throw new IllegalArgumentException("Malformed record");
        }
        List<Entry> entries = new ArrayList<>();
        for (String item : list.split(","))
        {
            if (item.isEmpty())
            {
                continue;
            }
            int at = item.indexOf('@');
            entries.add(new Entry(UUID.fromString(item.substring(0, at)),
                    Instant.ofEpochMilli(Long.parseLong(
                            item.substring(at + 1)))));
        }
        return new Stored(name, List.copyOf(entries));
    }
}
