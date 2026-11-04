/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import static com.sdh.healthcare.connector.core.landing.LandingTables.*;

import com.nimbusds.jose.util.JSONArrayUtils;
import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.Mac;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Reads landed items back from {@value LandingTables#LANDED_ITEMS}, checking
 * every chunk and the item's digest. A few chunks are read at a time, so
 * memory stays small for an item of any size.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LandedItems
{

    /** How many chunks are read from the table at a time. */
    private static final int WINDOW = 8;

    /** The longest original name that is decrypted: 64 KiB. */
    private static final int MAX_NAME_BYTES = 64 << 10;

    /** The most the broker IDs of a batch may decrypt to: 64 MiB. */
    private static final int MAX_REFS_BYTES = 64 << 20;

    /** How long the digest chain's links are: 32 bytes. */
    private static final int CHAIN_BYTES = 32;

    /** Not used: there are only static methods. */
    private LandedItems()
    {
    }

    /**
     * Writes a landed item to a stream. Every chunk is checked as it is
     * decrypted, and the digest is worked out again and must match the
     * stored one. On any mismatch the caller must discard what was written.
     *
     * @param table  the {@value LandingTables#LANDED_ITEMS} table
     * @param itemId the item
     * @param keys   unwraps the batch data keys
     * @param ids    makes the digest, with the ID key
     * @param output where the item's bytes go
     * @return the item's summary row
     * @throws IOException             if the item isn't fully landed, or
     *                                 the table or stream fails
     * @throws ChunkIntegrityException if a chunk is missing, duplicated,
     *                                 altered or out of place, or the
     *                                 digest doesn't match
     */
    public static Record read(Table table, UUID itemId, KeyProvider keys,
            SectionIds ids, OutputStream output) throws IOException
    {
        Record summary = summary(table, itemId).orElseThrow(
                () -> new IOException("Item isn't fully landed"));
        long size = (Long) summary.getField(ITEM_SIZE);
        int chunkCount = (Integer) summary.getField(CHUNK_COUNT);
        int chunkSize = (Integer) summary.getField(CHUNK_SIZE);
        int chunksPerBlock =
                (Integer) summary.getField(DIGEST_BLOCK_SIZE) / chunkSize;
        Map<UUID, byte[]> dataKeys = new HashMap<>();
        byte[] chain = new byte[CHAIN_BYTES];
        Mac link = null;
        for (int first = 0; first < chunkCount; first += WINDOW)
        {
            List<Record> rows = rows(table, Expressions.and(
                    Expressions.equal(ITEM_ID, itemId),
                    Expressions.greaterThanOrEqual(CHUNK_INDEX, first),
                    Expressions.lessThan(CHUNK_INDEX, first + WINDOW)));
            rows.sort(Comparator.comparingInt(
                    row -> (Integer) row.getField(CHUNK_INDEX)));
            if (rows.size() != Math.min(WINDOW, chunkCount - first))
            {
                throw new ChunkIntegrityException(
                        "Item chunks missing or duplicated");
            }
            for (int offset = 0; offset < rows.size(); offset++)
            {
                Record row = rows.get(offset);
                int chunkIndex = first + offset;
                if ((Integer) row.getField(CHUNK_INDEX) != chunkIndex)
                {
                    throw new ChunkIntegrityException(
                            "Item chunk out of place");
                }
                byte[] dataKey = dataKeys.computeIfAbsent(
                        (UUID) row.getField(BATCH_ID),
                        batch -> keys.unwrap(new WrappedKey(
                                (String) row.getField(KEK_REF),
                                bytes(row.getField(BDK_WRAPPED)))));
                int length = (int) Math.min(chunkSize,
                        Math.max(0, size - (long) chunkIndex * chunkSize));
                byte[] plaintext = SectionCrypto.decryptChunk(
                        SectionCrypto.sectionKey(dataKey, itemId, 0), itemId,
                        0, chunkIndex, 0, length,
                        bytes(row.getField(CIPHERTEXT)));
                if (chunkIndex % chunksPerBlock == 0)
                {
                    link = ids.newLabelledMac(ItemLander.DIGEST_LABEL);
                    link.update(chain);
                }
                link.update(plaintext);
                if (chunkIndex % chunksPerBlock == chunksPerBlock - 1
                        || chunkIndex == chunkCount - 1)
                {
                    chain = link.doFinal();
                }
                output.write(plaintext);
            }
        }
        if (!SectionIds.encode(chain).equals(summary.getField(ITEM_DIGEST)))
        {
            throw new ChunkIntegrityException("Item digest mismatch");
        }
        return summary;
    }

    /**
     * An item's summary row, present once it is fully landed.
     *
     * @param table  the {@value LandingTables#LANDED_ITEMS} table
     * @param itemId the item
     * @return the summary row; empty if the item isn't fully landed
     * @throws IOException if the table can't be read
     */
    public static Optional<Record> summary(Table table, UUID itemId)
            throws IOException
    {
        return rows(table, Expressions.and(
                Expressions.equal(ITEM_ID, itemId),
                Expressions.equal(CHUNK_INDEX, SUMMARY_ROW)))
                .stream().findFirst();
    }

    /**
     * Where retention must keep rows from so that no item is purged in part
     * (design §5.3.13). An item's chunks are committed in batches, each
     * stamped with its own {@value LandingTables#LANDED_AT}, so an item
     * landed across midnight has rows on both sides of a day boundary. The
     * cutoff moves back to the start of the earliest item that began before
     * it and finished at or after it, and of the item still being landed, if
     * there is one.
     *
     * @param table      the {@value LandingTables#LANDED_ITEMS} table
     * @param cutoff     the first instant retention would keep
     * @param checkpoint the intake's checkpoint, if it has one
     * @return the instant to keep rows from: {@code cutoff} or earlier
     * @throws IOException if the table can't be read
     */
    public static Instant keepFrom(Table table, Instant cutoff,
            Optional<LandingCheckpoint> checkpoint) throws IOException
    {
        Instant keep = cutoff;
        if (checkpoint.isPresent() && !checkpoint.get().done()
                && checkpoint.get().startedAt().isBefore(keep))
        {
            keep = checkpoint.get().startedAt();
        }
        long cutoffMicros = ChronoUnit.MICROS.between(Instant.EPOCH, cutoff);
        table.refresh();
        if (table.currentSnapshot() == null)
        {
            return keep;
        }
        try (CloseableIterable<Record> straddling = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(CHUNK_INDEX, SUMMARY_ROW),
                        Expressions.greaterThanOrEqual(LANDED_AT,
                                cutoffMicros),
                        Expressions.lessThan(STARTED_AT, cutoffMicros)))
                .select(STARTED_AT)
                .build())
        {
            for (Record row : straddling)
            {
                Object started = row.getField(STARTED_AT);
                if (started instanceof OffsetDateTime time
                        && time.toInstant().isBefore(keep))
                {
                    keep = time.toInstant();
                }
            }
        }
        return keep;
    }

    /**
     * The record index kept on a message batch's summary row.
     *
     * @param summary the item's summary row
     * @return the index; empty if the item isn't a message batch
     */
    @SuppressWarnings("unchecked")
    public static Optional<RecordIndex> records(Record summary)
    {
        Object offsets = summary.getField(RECORD_OFFSETS);
        if (offsets == null)
        {
            return Optional.empty();
        }
        return Optional.of(new RecordIndex((List<Long>) offsets,
                (List<String>) summary.getField(RECORD_KEYS)));
    }

    /**
     * The keys of the messages an intake landed from a time on, to
     * recognise any the broker delivers again (design §5.3.16). Only summary
     * rows are read, which hold no ciphertext, and only from the days at or
     * after {@code since}. (Iceberg can't project a list column alone.)
     *
     * @param table  the {@value LandingTables#LANDED_ITEMS} table
     * @param intake the intake
     * @param since  the earliest landing time to read
     * @return the keys, in no particular order
     * @throws IOException if the table can't be read
     */
    @SuppressWarnings("unchecked")
    public static List<String> recordKeys(Table table, String intake,
            Instant since) throws IOException
    {
        List<String> keys = new ArrayList<>();
        table.refresh();
        if (table.currentSnapshot() == null)
        {
            return keys;
        }
        long sinceMicros = ChronoUnit.MICROS.between(Instant.EPOCH, since);
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(CHUNK_INDEX, SUMMARY_ROW),
                        Expressions.equal(INTAKE, intake),
                        Expressions.greaterThanOrEqual(LANDED_AT,
                                sinceMicros),
                        Expressions.notNull(RECORD_KEYS)))
                .build())
        {
            for (Record row : rows)
            {
                for (String key : (List<String>) row.getField(RECORD_KEYS))
                {
                    if (key != null)
                    {
                        keys.add(key);
                    }
                }
            }
        }
        return keys;
    }

    /**
     * Decrypts the original name or path kept on a summary row. It is PHI:
     * never log it.
     *
     * @param summary the item's summary row
     * @param keys    unwraps the row's data key
     * @return the name; empty if none was recorded
     */
    public static Optional<String> originalName(Record summary,
            KeyProvider keys)
    {
        Object encrypted = summary.getField(ORIGINAL_NAME_ENC);
        if (encrypted == null)
        {
            return Optional.empty();
        }
        UUID itemId = (UUID) summary.getField(ITEM_ID);
        byte[] dataKey = keys.unwrap(new WrappedKey(
                (String) summary.getField(KEK_REF),
                bytes(summary.getField(BDK_WRAPPED))));
        return Optional.of(new String(SectionCrypto.decryptSmall(
                SectionCrypto.sectionKey(dataKey, itemId, -1), itemId, -1, 0,
                1, MAX_NAME_BYTES, bytes(encrypted)), StandardCharsets.UTF_8));
    }

    /**
     * Decrypts the broker IDs of a message batch's messages. They are the
     * producer's own references and may be PHI: never log them.
     *
     * @param summary the item's summary row
     * @param keys    unwraps the row's data key
     * @return each message's ID, in order, null for one without; empty if
     *         none were recorded
     * @throws ChunkIntegrityException if they don't decrypt or parse
     */
    public static Optional<List<String>> messageRefs(Record summary,
            KeyProvider keys)
    {
        Object encrypted = summary.getField(RECORD_REFS_ENC);
        if (encrypted == null)
        {
            return Optional.empty();
        }
        UUID itemId = (UUID) summary.getField(ITEM_ID);
        byte[] dataKey = keys.unwrap(new WrappedKey(
                (String) summary.getField(KEK_REF),
                bytes(summary.getField(BDK_WRAPPED))));
        String json = new String(SectionCrypto.decryptSmall(
                SectionCrypto.sectionKey(dataKey, itemId, RECORD_REFS_SEQ),
                itemId, RECORD_REFS_SEQ, 0, 1, MAX_REFS_BYTES,
                bytes(encrypted)), StandardCharsets.UTF_8);
        try
        {
            List<String> refs = new ArrayList<>();
            for (Object ref : JSONArrayUtils.parse(json))
            {
                refs.add(ref == null ? null : ref.toString());
            }
            return Optional.of(Collections.unmodifiableList(refs));
        }
        catch (ParseException malformed)
        {
            throw new ChunkIntegrityException("Malformed broker IDs");
        }
    }

    /**
     * The item a summary row says this one replaces.
     *
     * @param summary the item's summary row
     * @return the earlier item; empty if none
     */
    public static Optional<UUID> replaces(Record summary)
    {
        return Optional.ofNullable((UUID) summary.getField(REPLACES_ITEM_ID));
    }

    /**
     * Reads the rows that match a filter.
     *
     * @param table  the table
     * @param filter which rows
     * @return copies of the rows
     * @throws IOException if the table can't be read
     */
    static List<Record> rows(Table table, Expression filter)
            throws IOException
    {
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<Record> scan =
                IcebergGenerics.read(table).where(filter).build())
        {
            scan.forEach(row -> rows.add(row.copy()));
        }
        return rows;
    }

    /**
     * The bytes of a binary column's value.
     *
     * @param value the value, a buffer
     * @return a copy of its remaining bytes
     */
    static byte[] bytes(Object value)
    {
        ByteBuffer buffer = ((ByteBuffer) value).duplicate();
        byte[] copy = new byte[buffer.remaining()];
        buffer.get(copy);
        return copy;
    }
}
