/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import static com.sdh.healthcare.connector.core.landing.LandingTables.BATCH_ID;
import static com.sdh.healthcare.connector.core.landing.LandingTables.BDK_WRAPPED;
import static com.sdh.healthcare.connector.core.landing.LandingTables.CHUNK_COUNT;
import static com.sdh.healthcare.connector.core.landing.LandingTables.CHUNK_INDEX;
import static com.sdh.healthcare.connector.core.landing.LandingTables.CHUNK_SIZE;
import static com.sdh.healthcare.connector.core.landing.LandingTables.CIPHERTEXT;
import static com.sdh.healthcare.connector.core.landing.LandingTables.ITEM_DIGEST;
import static com.sdh.healthcare.connector.core.landing.LandingTables.ITEM_ID;
import static com.sdh.healthcare.connector.core.landing.LandingTables.ITEM_SIZE;
import static com.sdh.healthcare.connector.core.landing.LandingTables.KEK_REF;

import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;

/**
 * A landed item as random-access bytes, for the splitter (design §5.3.6).
 * Chunks are read from {@value LandingTables#LANDED_ITEMS} a few at a time
 * and kept decrypted for a while, so splitting, which mostly reads forward,
 * reads each chunk once. Every chunk is checked as it is decrypted: its
 * authentication tag binds it to the item and its place, so a chunk that is
 * missing, moved or altered is refused with
 * {@link ChunkIntegrityException}.
 * <p>
 * The digest isn't worked out again here, because reads needn't cover the
 * whole item in order; {@link LandedItems#read} does, for restores.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LandedItemSource implements SeekableByteSource
{

    /** How many chunks are read from the table at a time. */
    static final int WINDOW = 4;

    /** How many windows are kept decrypted. */
    static final int CACHED_WINDOWS = 2;

    /** The item isn't in landing, or isn't fully landed. */
    public static final class NotLandedException extends IOException
    {

        /** For serialization. */
        private static final long serialVersionUID = 1L;

        /** Creates the exception, with a fixed message without PHI. */
        public NotLandedException()
        {
            super("Item isn't fully landed");
        }
    }

    /** The {@value LandingTables#LANDED_ITEMS} table. */
    private final Table table;

    /** The item. */
    private final UUID itemId;

    /** Unwraps the batch data keys. */
    private final KeyProvider keys;

    /** The item's size, in bytes. */
    private final long size;

    /** How many chunks it has. */
    private final int chunkCount;

    /** The plaintext size of each chunk but the last. */
    private final int chunkSize;

    /** The item's digest, from its summary row. */
    private final String digest;

    /** Unwrapped data keys, by batch. */
    private final Map<UUID, byte[]> dataKeys = new HashMap<>();

    /** Decrypted windows, least recently used first. */
    private final Map<Integer, byte[][]> windows =
            new LinkedHashMap<>(4, 0.75f, true)
            {
                private static final long serialVersionUID = 1L;

                @Override
                protected boolean removeEldestEntry(
                        Map.Entry<Integer, byte[][]> eldest)
                {
                    return size() > CACHED_WINDOWS;
                }
            };

    /**
     * Holds an item's layout.
     *
     * @param table      the table
     * @param itemId     the item
     * @param keys       unwraps the data keys
     * @param summary    the item's summary row
     */
    private LandedItemSource(Table table, UUID itemId, KeyProvider keys,
            Record summary)
    {
        this.table = table;
        this.itemId = itemId;
        this.keys = keys;
        this.size = (Long) summary.getField(ITEM_SIZE);
        this.chunkCount = (Integer) summary.getField(CHUNK_COUNT);
        this.chunkSize = (Integer) summary.getField(CHUNK_SIZE);
        this.digest = (String) summary.getField(ITEM_DIGEST);
    }

    /**
     * Opens a fully landed item. The table is refreshed first, since the
     * landing node commits to it from another process.
     *
     * @param table  the {@value LandingTables#LANDED_ITEMS} table
     * @param itemId the item
     * @param keys   unwraps the batch data keys
     * @return the item's bytes
     * @throws NotLandedException if the item has no summary row
     * @throws IOException        if the table can't be read
     */
    public static LandedItemSource open(Table table, UUID itemId,
            KeyProvider keys) throws IOException
    {
        table.refresh();
        Record summary = LandedItems.summary(table, itemId)
                .orElseThrow(NotLandedException::new);
        return new LandedItemSource(table, itemId, keys, summary);
    }

    @Override
    public long size()
    {
        return size;
    }

    /**
     * The item's digest, as recorded when it was landed.
     *
     * @return the digest, in base32
     */
    public String digest()
    {
        return digest;
    }

    @Override
    public int read(long position, ByteBuffer destination) throws IOException
    {
        if (position >= size)
        {
            return -1;
        }
        if (!destination.hasRemaining())
        {
            return 0;
        }
        int index = (int) (position / chunkSize);
        byte[] chunk = chunk(index);
        int within = (int) (position - (long) index * chunkSize);
        int count = Math.min(destination.remaining(), chunk.length - within);
        destination.put(chunk, within, count);
        return count;
    }

    @Override
    public void close()
    {
        synchronized (this)
        {
            windows.clear();
            dataKeys.clear();
        }
    }

    /**
     * A chunk's plaintext, from the cache or the table.
     *
     * @param index the chunk's index
     * @return its bytes
     * @throws IOException if the table can't be read
     */
    private synchronized byte[] chunk(int index) throws IOException
    {
        int window = index / WINDOW;
        byte[][] chunks = windows.get(window);
        if (chunks == null)
        {
            chunks = load(window);
            windows.put(window, chunks);
        }
        return chunks[index - window * WINDOW];
    }

    /**
     * Reads and decrypts one window of chunks.
     *
     * @param window the window's number
     * @return its chunks' plaintexts, in order
     * @throws IOException if the table can't be read
     */
    private byte[][] load(int window) throws IOException
    {
        int first = window * WINDOW;
        int count = Math.min(WINDOW, chunkCount - first);
        List<Record> rows = LandedItems.rows(table, Expressions.and(
                Expressions.equal(ITEM_ID, itemId),
                Expressions.greaterThanOrEqual(CHUNK_INDEX, first),
                Expressions.lessThan(CHUNK_INDEX, first + count)));
        rows.sort(Comparator.comparingInt(
                row -> (Integer) row.getField(CHUNK_INDEX)));
        if (rows.size() != count)
        {
            throw new ChunkIntegrityException(
                    "Item chunks missing or duplicated");
        }
        byte[][] chunks = new byte[count][];
        long bytes = 0;
        for (int offset = 0; offset < count; offset++)
        {
            Record row = rows.get(offset);
            int chunkIndex = first + offset;
            if ((Integer) row.getField(CHUNK_INDEX) != chunkIndex)
            {
                throw new ChunkIntegrityException("Item chunk out of place");
            }
            byte[] dataKey = dataKeys.computeIfAbsent(
                    (UUID) row.getField(BATCH_ID),
                    batch -> keys.unwrap(new WrappedKey(
                            (String) row.getField(KEK_REF),
                            LandedItems.bytes(row.getField(BDK_WRAPPED)))));
            int length = (int) Math.min(chunkSize,
                    size - (long) chunkIndex * chunkSize);
            chunks[offset] = SectionCrypto.decryptChunk(
                    SectionCrypto.sectionKey(dataKey, itemId, 0), itemId, 0,
                    chunkIndex, 0, length,
                    LandedItems.bytes(row.getField(CIPHERTEXT)));
            bytes += length;
        }
        Telemetry.get().bytesRead(bytes, "landing");
        return chunks;
    }
}
