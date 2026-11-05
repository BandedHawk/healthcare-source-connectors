/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.inbox;

import static com.sdh.healthcare.connector.core.inbox.InboxTables.INTAKE;
import static com.sdh.healthcare.connector.core.inbox.InboxTables.ITEM_DIGEST;
import static com.sdh.healthcare.connector.core.inbox.InboxTables.ITEM_ID;
import static com.sdh.healthcare.connector.core.inbox.InboxTables.ITEM_SIZE;
import static com.sdh.healthcare.connector.core.inbox.InboxTables.KIND;
import static com.sdh.healthcare.connector.core.inbox.InboxTables.LANDED_AT;
import static com.sdh.healthcare.connector.core.inbox.InboxTables.RECEIVED_AT;
import static com.sdh.healthcare.connector.core.inbox.InboxTables.SEQ;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.crypto.MacedJson;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.landing.PairSet;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;

/**
 * The splitter's durable list of landed items to process (design §5.3.6).
 * Each {@code item.landed} message is recorded here, in one commit, before
 * it is acknowledged, so the broker never has to hold a message while an
 * item is split, which can take hours. Items are then processed in the
 * order they were received.
 * <p>
 * A message received again (a broker redelivery, or the landing node
 * sending the last item's message again after a restart) is recognised by
 * its item ID and not added twice. The item IDs are kept in memory, 16
 * bytes each.
 * <p>
 * How far the splitter has got is the <em>position</em>: the sequence
 * number of the last item finished, kept with a MAC in the table property
 * {@code sdh.inbox.<connector>}. An item is finished once its checkpoint is
 * {@code COMPLETED}, {@code FAILED} or {@code ABANDONED}; the position is
 * moved after that, so a crash in between only means the finished item is
 * looked at again and passed over.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Inbox
{

    /** What the position property's name starts with. */
    public static final String PROPERTY_PREFIX = "sdh.inbox.";

    /** Labels the position's MAC. */
    private static final String LABEL = "inbox-position";

    /**
     * An item waiting to be processed.
     *
     * @param seq        its place in the inbox, from 1
     * @param itemId     its item ID, which becomes its file ID
     * @param intake     the intake it came in through
     * @param kind       what kind of item it is
     * @param size       its size, in bytes
     * @param digest     its digest
     * @param landedAt   when it was landed
     * @param receivedAt when its message was recorded here
     */
    public record Entry(long seq, UUID itemId, String intake, String kind,
            long size, String digest, Instant landedAt, Instant receivedAt)
    {
    }

    /**
     * The stored position.
     *
     * @param position the sequence number of the last item finished
     */
    private record Position(long position)
    {
    }

    /** The inbox table. */
    private final Table table;

    /** Makes the fenced commits. */
    private final FencedCommitter committer;

    /** How commits are retried. */
    private final RetryPolicy retry;

    /** Gives the times items are received. */
    private final Clock clock;

    /** Writes and reads the position with its MAC. */
    private final MacedJson<Position> codec;

    /** The table property the position is kept in. */
    private final String positionKey;

    /** The item IDs in the inbox. */
    private final PairSet itemIds = new PairSet(0);

    /** The highest sequence number in the inbox. */
    private long lastSeq;

    /** The sequence number of the last item finished. */
    private long position;

    /**
     * Creates the inbox; {@link #load} reads what is in it.
     *
     * @param table       the inbox table
     * @param committer   makes fenced commits under the lease
     * @param ids         MACs the position, with the ID key
     * @param connectorId the splitter's ID, which names the position
     * @param retry       how commits are retried
     * @param clock       gives the times items are received
     */
    public Inbox(Table table, FencedCommitter committer, SectionIds ids,
            String connectorId, RetryPolicy retry, Clock clock)
    {
        this.table = table;
        this.committer = committer;
        this.retry = retry;
        this.clock = clock;
        this.codec = new MacedJson<>(ids, LABEL, value ->
        {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("position", value.position());
            return fields;
        }, fields -> new Position(((Number) fields.get("position"))
                .longValue()));
        this.positionKey = PROPERTY_PREFIX + connectorId;
    }

    /**
     * Reads the item IDs, the last sequence number and the position.
     *
     * @throws CheckpointTamperedException if the position's MAC doesn't
     *                                     verify
     * @throws IOException                 if the table can't be read
     */
    public synchronized void load() throws IOException
    {
        table.refresh();
        try (CloseableIterable<Record> scan = IcebergGenerics.read(table)
                .select(SEQ, ITEM_ID).build())
        {
            for (Record row : scan)
            {
                UUID itemId = (UUID) row.getField(ITEM_ID);
                itemIds.add(itemId.getMostSignificantBits(),
                        itemId.getLeastSignificantBits());
                lastSeq = Math.max(lastSeq, (Long) row.getField(SEQ));
            }
        }
        String stored = table.properties().get(positionKey);
        if (stored != null)
        {
            try
            {
                position = codec.decode(stored).position();
            }
            catch (MacedJson.Tampered tampered)
            {
                throw new CheckpointTamperedException();
            }
        }
    }

    /**
     * Records an item's message, unless the item is already in the inbox.
     * The caller acknowledges the message once this returns.
     *
     * @param message the item's message
     * @return true if it was added; false if it was already there
     * @throws Exception if storage still fails after the retries
     */
    public synchronized boolean add(ConnectorMessage.ItemLanded message)
            throws Exception
    {
        UUID itemId = message.fileId();
        if (itemIds.contains(itemId.getMostSignificantBits(),
                itemId.getLeastSignificantBits()))
        {
            Telemetry.get().audit("inbox.repeat",
                    Fields.of(Attr.ITEM_ID, itemId));
            return false;
        }
        long seq = lastSeq + 1;
        Instant now = clock.instant();
        DataFile file = write(seq, message, now);
        retry.call("inbox.commit", RetryPolicy.TRANSIENT_STORAGE, attempt ->
        {
            if (attempt > 1 && containsRow(itemId))
            {
                return null;
            }
            return committer.commit(table, transaction -> transaction
                    .newFastAppend().appendFile(file).commit());
        });
        lastSeq = seq;
        itemIds.add(itemId.getMostSignificantBits(),
                itemId.getLeastSignificantBits());
        Telemetry.get().audit("inbox.added", Fields.of(Attr.ITEM_ID, itemId)
                .with(Attr.INTAKE, message.intake()).with(Attr.SEQ, seq));
        notifyAll();
        return true;
    }

    /**
     * Whether an item is in the inbox, whether or not it has been
     * processed.
     *
     * @param itemId the item
     * @return true if it is
     */
    public synchronized boolean contains(UUID itemId)
    {
        return itemIds.contains(itemId.getMostSignificantBits(),
                itemId.getLeastSignificantBits());
    }

    /**
     * The next item to process: the first after the position. Waits up to
     * the given time for one to arrive if there is none.
     *
     * @param wait how long to wait for an item
     * @return the item; empty if none arrived in time
     * @throws IOException          if the table can't be read
     * @throws InterruptedException if the wait is interrupted
     */
    public synchronized Optional<Entry> next(Duration wait)
            throws IOException, InterruptedException
    {
        if (lastSeq <= position && wait.isPositive())
        {
            wait(wait.toMillis());
        }
        if (lastSeq <= position)
        {
            return Optional.empty();
        }
        table.refresh();
        Entry first = null;
        try (CloseableIterable<Record> scan = IcebergGenerics.read(table)
                .where(Expressions.greaterThan(SEQ, position)).build())
        {
            for (Record row : scan)
            {
                long seq = (Long) row.getField(SEQ);
                if (first == null || seq < first.seq())
                {
                    first = entry(row);
                }
            }
        }
        return Optional.ofNullable(first);
    }

    /**
     * Moves the position past a finished item.
     *
     * @param seq the finished item's sequence number
     * @throws Exception if storage still fails after the retries
     */
    public synchronized void done(long seq) throws Exception
    {
        if (seq <= position)
        {
            return;
        }
        String encoded = codec.encode(new Position(seq));
        retry.call("inbox.position", RetryPolicy.TRANSIENT_STORAGE,
                attempt -> committer.commit(table, transaction -> transaction
                        .updateProperties().set(positionKey, encoded)
                        .commit()));
        position = seq;
    }

    /**
     * The sequence number of the last finished item.
     *
     * @return the position; 0 before the first item is finished
     */
    public synchronized long position()
    {
        return position;
    }

    /**
     * How many items are waiting, for the {@code sdh.inbox.backlog}
     * gauge.
     *
     * @return the number of items after the position
     */
    public synchronized long backlog()
    {
        return lastSeq - position;
    }

    /**
     * Tells whether the table has a row for an item, after a commit whose
     * outcome isn't known.
     *
     * @param itemId the item
     * @return true if it has
     * @throws IOException if the table can't be read
     */
    private boolean containsRow(UUID itemId) throws IOException
    {
        table.refresh();
        try (CloseableIterable<Record> scan = IcebergGenerics.read(table)
                .select(ITEM_ID).where(Expressions.equal(ITEM_ID, itemId))
                .build())
        {
            return scan.iterator().hasNext();
        }
    }

    /**
     * Turns a row into an entry.
     *
     * @param row the row
     * @return the entry
     */
    private static Entry entry(Record row)
    {
        return new Entry((Long) row.getField(SEQ),
                (UUID) row.getField(ITEM_ID),
                (String) row.getField(INTAKE), (String) row.getField(KIND),
                (Long) row.getField(ITEM_SIZE),
                (String) row.getField(ITEM_DIGEST),
                ((OffsetDateTime) row.getField(LANDED_AT)).toInstant(),
                ((OffsetDateTime) row.getField(RECEIVED_AT)).toInstant());
    }

    /**
     * Writes one item's row to a new data file.
     *
     * @param seq        its sequence number
     * @param message    its message
     * @param receivedAt when it was received
     * @return the data file, ready to commit
     */
    private DataFile write(long seq, ConnectorMessage.ItemLanded message,
            Instant receivedAt)
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
            GenericRecord row = GenericRecord.create(table.schema());
            row.setField(SEQ, seq);
            row.setField(ITEM_ID, message.fileId());
            row.setField(INTAKE, message.intake());
            row.setField(KIND, message.kind());
            row.setField(ITEM_SIZE, message.size());
            row.setField(ITEM_DIGEST, message.itemDigest());
            row.setField(LANDED_AT, OffsetDateTime.ofInstant(
                    message.landedAt(), ZoneOffset.UTC));
            row.setField(RECEIVED_AT, OffsetDateTime.ofInstant(receivedAt,
                    ZoneOffset.UTC));
            writer.write(row);
        }
        catch (IOException unwritable)
        {
            throw new UncheckedIOException(unwritable);
        }
        return writer.toDataFile();
    }
}
