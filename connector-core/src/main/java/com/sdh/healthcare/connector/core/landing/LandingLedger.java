/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import static com.sdh.healthcare.connector.core.landing.LandingTables.*;

import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;

/**
 * The ledger table (design §5.3.2): what has been landed, so nothing is
 * landed twice, kept after retention has purged the items themselves. It
 * holds no PHI: reference hashes, sizes, versions, item IDs and digests.
 * <p>
 * An entry is written straight after its item's final commit. Iceberg has
 * no commit across tables, so a crash in between leaves the last landed item
 * without an entry; {@link #repair} adds it at startup. Because items are
 * landed one at a time, only the last one can be missing.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LandingLedger
{

    /** The ledger table. */
    private final Table table;

    /** Makes the fenced commits. */
    private final FencedCommitter committer;

    /** How commits are retried. */
    private final RetryPolicy retry;

    /**
     * One landed item, as the ledger records it.
     *
     * @param intake   the intake it came in through
     * @param source   where it came from; null if it had no source
     *                 reference
     * @param itemId   its ID
     * @param digest   its digest
     * @param landedAt when it was landed
     * @param size     its size in bytes; -1 for an entry written before
     *                 sizes were recorded
     * @param sha256   the plain SHA-256 of its bytes, in lower-case hex;
     *                 null if not known
     */
    public record Entry(String intake, SourceRef source, UUID itemId,
            String digest, Instant landedAt, long size, String sha256)
    {

        /**
         * The entry for a landed item.
         *
         * @param item the item
         * @return its entry
         */
        public static Entry of(LandedItem item)
        {
            return new Entry(item.intake(), item.source(), item.itemId(),
                    item.digest(), item.landedAt(), item.size(),
                    item.sha256());
        }
    }

    /**
     * Creates a ledger.
     *
     * @param table     the {@value LandingTables#LEDGER} table
     * @param committer makes fenced commits under the intake's lease; it
     *                  must have established the epoch on this table too
     * @param retry     how commits are retried
     */
    public LandingLedger(Table table, FencedCommitter committer,
            RetryPolicy retry)
    {
        this.table = table;
        this.committer = committer;
        this.retry = retry;
    }

    /**
     * Records landed items, in one commit.
     *
     * @param entries the items
     * @throws Exception if storage still fails after the retries
     */
    public void record(List<Entry> entries) throws Exception
    {
        if (entries.isEmpty())
        {
            return;
        }
        DataFile file = write(entries);
        long started = System.nanoTime();
        retry.call("ledger.commit", RetryPolicy.TRANSIENT_STORAGE,
                attempt ->
                {
                    if (attempt > 1 && contains(entries.getLast().itemId()))
                    {
                        return null;
                    }
                    return committer.commit(table, transaction -> transaction
                            .newFastAppend().appendFile(file).commit());
                });
        Telemetry.get().commitDuration(System.nanoTime() - started,
                table.name());
    }

    /**
     * How many entries an intake has, at most, from the table's metadata
     * alone: the rows of every data file that may hold some of its entries.
     *
     * @param intake the intake's name
     * @return the count; more than the entries if files are shared with
     *         another intake
     * @throws IOException if the table can't be read
     */
    public long count(String intake) throws IOException
    {
        table.refresh();
        long rows = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan()
                .filter(Expressions.equal(INTAKE, intake)).planFiles())
        {
            for (FileScanTask task : tasks)
            {
                rows += task.file().recordCount();
            }
        }
        return rows;
    }

    /**
     * Loads an intake's entries into memory, with no limit on the heap the
     * index may take.
     *
     * @param intake the intake's name
     * @return the index of everything the intake has landed
     * @throws IOException if the table can't be read
     */
    public LedgerIndex load(String intake) throws IOException
    {
        return load(intake, Long.MAX_VALUE);
    }

    /**
     * Loads an intake's entries into memory, in an index sized for them.
     *
     * @param intake the intake's name
     * @param budget the bytes of heap the index may take before it warns
     *               ({@link LedgerIndex})
     * @return the index of everything the intake has landed
     * @throws IOException if the table can't be read
     */
    public LedgerIndex load(String intake, long budget) throws IOException
    {
        LedgerIndex index = new LedgerIndex(count(intake), budget, intake);
        try (CloseableIterable<Record> scan = IcebergGenerics.read(table)
                .select(SOURCE_REF_HASH, SOURCE_SIZE, SOURCE_VERSION,
                        ITEM_DIGEST)
                .where(Expressions.equal(INTAKE, intake)).build())
        {
            for (Record row : scan)
            {
                String hash = (String) row.getField(SOURCE_REF_HASH);
                LedgerIndex.Key key = hash.isEmpty() ? null
                        : new SourceRef(hash, (Long) row.getField(SOURCE_SIZE),
                                (String) row.getField(SOURCE_VERSION)).key();
                index.add(key, (String) row.getField(ITEM_DIGEST));
            }
        }
        index.checkBudget();
        return index;
    }

    /**
     * Adds the entry of the last landed item if a crash kept it from being
     * written, so it is in the ledger before the intake looks for new
     * items.
     *
     * @param lander the intake's lander
     * @param index  the loaded index, which the entry is added to as well
     * @return true if an entry was missing and has been added
     * @throws Exception if storage fails
     */
    public boolean repair(ItemLander lander, LedgerIndex index)
            throws Exception
    {
        LandedItem last = lander.lastLanded().orElse(null);
        if (last == null || contains(last.itemId()))
        {
            return false;
        }
        record(List.of(Entry.of(last)));
        index.add(last.source() == null ? null : last.source().key(),
                last.digest());
        return true;
    }

    /**
     * Tells whether the ledger has an item's entry.
     *
     * @param itemId the item
     * @return true if it has
     * @throws IOException if the table can't be read
     */
    public boolean contains(UUID itemId) throws IOException
    {
        return !itemIds(Expressions.equal(ITEM_ID, itemId)).isEmpty();
    }

    /**
     * Tells whether an intake landed an item: so a producer can only link
     * a new item to one of its own (design §5.3.8).
     *
     * @param intake the intake's name
     * @param itemId the item
     * @return true if the intake's ledger has it
     * @throws IOException if the table can't be read
     */
    public boolean contains(String intake, UUID itemId) throws IOException
    {
        return !itemIds(Expressions.and(Expressions.equal(INTAKE, intake),
                Expressions.equal(ITEM_ID, itemId))).isEmpty();
    }

    /**
     * Finds the entry of an intake's item by its source reference: for an
     * HTTP push, the first item sent with an idempotency key, so the same
     * receipt can be given again.
     *
     * @param intake the intake's name
     * @param source the source reference
     * @return the earliest such entry; empty if there is none
     * @throws IOException if the table can't be read
     */
    public Optional<Entry> find(String intake, SourceRef source)
            throws IOException
    {
        table.refresh();
        Entry first = null;
        try (CloseableIterable<Record> scan = IcebergGenerics.read(table)
                .where(Expressions.and(Expressions.equal(INTAKE, intake),
                        Expressions.equal(SOURCE_REF_HASH, source.hash()),
                        Expressions.equal(SOURCE_SIZE, source.size()),
                        Expressions.equal(SOURCE_VERSION, source.version())))
                .build())
        {
            for (Record row : scan)
            {
                Long size = (Long) row.getField(ITEM_SIZE);
                Entry entry = new Entry(intake, source,
                        (UUID) row.getField(ITEM_ID),
                        (String) row.getField(ITEM_DIGEST),
                        ((OffsetDateTime) row.getField(LANDED_AT)).toInstant(),
                        size == null ? -1 : size,
                        (String) row.getField(ITEM_SHA256));
                if (first == null
                        || entry.landedAt().isBefore(first.landedAt()))
                {
                    first = entry;
                }
            }
        }
        return Optional.ofNullable(first);
    }

    /**
     * The items landed with a digest: the exact answer when
     * {@link LedgerIndex#mayHaveContent} says there may be some.
     *
     * @param digest the digest
     * @return the items' IDs, in no order; empty if there are none
     * @throws IOException if the table can't be read
     */
    public List<UUID> itemsWithDigest(String digest) throws IOException
    {
        return itemIds(Expressions.equal(ITEM_DIGEST, digest));
    }

    /**
     * The item a landed item is an exact copy of (STAGES.md §15.5a): the
     * earliest item landed before it with the same digest that is still in
     * the ledger, leaving out the item it replaces. Item IDs are UUIDv7, so
     * the earliest has the lowest ID. As it only looks at earlier items,
     * asking again, after a crash, gives the same answer while that item is
     * kept.
     *
     * @param itemId   the item
     * @param digest   its digest
     * @param replaces the item it replaces; null if none
     * @return the earlier item; empty if there is none
     * @throws IOException if the table can't be read
     */
    public Optional<UUID> earliestCopy(UUID itemId, String digest,
            UUID replaces) throws IOException
    {
        return earliest(itemsWithDigest(digest), itemId, replaces);
    }

    /**
     * The earliest of some items landed before an item, leaving out the
     * item it replaces.
     *
     * @param items    the items, in any order
     * @param itemId   the item
     * @param replaces the item it replaces; null if none
     * @return the earliest of them; empty if none is earlier
     */
    public static Optional<UUID> earliest(List<UUID> items, UUID itemId,
            UUID replaces)
    {
        UUID found = null;
        for (UUID other : items)
        {
            if (!other.equals(replaces) && before(other, itemId)
                    && (found == null || before(other, found)))
            {
                found = other;
            }
        }
        return Optional.ofNullable(found);
    }

    /**
     * Tells whether one UUIDv7 comes before another, comparing them as
     * unsigned numbers, as their time order needs.
     *
     * @param one   the one
     * @param other the other
     * @return true if {@code one} is lower
     */
    private static boolean before(UUID one, UUID other)
    {
        int high = Long.compareUnsigned(one.getMostSignificantBits(),
                other.getMostSignificantBits());
        return high != 0 ? high < 0
                : Long.compareUnsigned(one.getLeastSignificantBits(),
                        other.getLeastSignificantBits()) < 0;
    }

    /**
     * The IDs of the entries that match a filter.
     *
     * @param filter which entries
     * @return their item IDs
     * @throws IOException if the table can't be read
     */
    private List<UUID> itemIds(Expression filter) throws IOException
    {
        table.refresh();
        List<UUID> found = new ArrayList<>();
        try (CloseableIterable<Record> scan = IcebergGenerics.read(table)
                .select(ITEM_ID).where(filter).build())
        {
            scan.forEach(row -> found.add((UUID) row.getField(ITEM_ID)));
        }
        return found;
    }

    /**
     * Writes entries to a new data file of the ledger table.
     *
     * @param entries the entries
     * @return the data file, ready to commit
     */
    private DataFile write(List<Entry> entries)
    {
        String location = table.locationProvider().newDataLocation(
                FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));
        DataWriter<Record> writer;
        try
        {
            writer = Parquet.writeData(table.io().newOutputFile(location))
                    .schema(table.schema())
                    .withSpec(table.spec())
                    .withSortOrder(table.sortOrder())
                    .setAll(table.properties())
                    .setAll(IngestTables.WRITE_OVERRIDES)
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .build();
        }
        catch (IOException unwritable)
        {
            throw new UncheckedIOException(unwritable);
        }
        try (writer)
        {
            for (Entry entry : entries)
            {
                GenericRecord row = GenericRecord.create(table.schema());
                row.setField(INTAKE, entry.intake());
                SourceRef source = entry.source();
                row.setField(SOURCE_REF_HASH,
                        source == null ? "" : source.hash());
                row.setField(SOURCE_SIZE, source == null ? -1L : source.size());
                row.setField(SOURCE_VERSION,
                        source == null ? "" : source.version());
                row.setField(ITEM_ID, entry.itemId());
                row.setField(ITEM_DIGEST, entry.digest());
                row.setField(LANDED_AT, OffsetDateTime.ofInstant(
                        entry.landedAt(), ZoneOffset.UTC));
                row.setField(ITEM_SIZE, entry.size());
                row.setField(ITEM_SHA256, entry.sha256());
                writer.write(row);
            }
        }
        catch (IOException unwritable)
        {
            throw new UncheckedIOException(unwritable);
        }
        return writer.toDataFile();
    }
}
