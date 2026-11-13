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
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.table.DataFileRows;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

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

    /** The columns a chunk is read with. */
    private static final List<String> CHUNK_COLUMNS = List.of(ITEM_ID,
            CHUNK_INDEX, CIPHERTEXT, BDK_WRAPPED, KEK_REF, BATCH_ID);

    /**
     * A landed item's summary row, and where its final data file is when
     * its {@code item.landed} said (PERFORMANCE.md §3.2).
     *
     * @param row        the summary row
     * @param file       the data file of the item's final commit; null if
     *                   not known, when its chunks are found by scan
     * @param fileSize   that file's size in bytes; -1 without a file
     * @param summaryRow the summary row's position in it; -1 without a file
     */
    public record Summary(Record row, String file, long fileSize,
            long summaryRow)
    {

        /**
         * The first chunk in the final data file: that file holds the
         * item's last chunks, in order, and then the summary row.
         *
         * @return the chunk's index; the chunk count if none is there or
         *         the file isn't known
         */
        int firstChunkInFile()
        {
            int chunkCount = (Integer) row.getField(CHUNK_COUNT);
            return file == null ? chunkCount
                    : (int) Math.max(0, chunkCount - summaryRow);
        }
    }

    /** The {@value LandingTables#LANDED_ITEMS} table. */
    private final Table table;

    /** The item's final data file; null if not known. */
    private final String file;

    /** That file's size in bytes. */
    private final long fileSize;

    /** The first of the item's chunks in that file. */
    private final int firstInFile;

    /** Whether the table has been refreshed for a scan. */
    private boolean refreshed;

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

    /** Where each message starts, for a message batch; or null. */
    private final MessageIndex messageIndex;

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
     * @param located    the item's summary row, and where its final data
     *                   file is
     * @param refreshed  whether the table was just refreshed
     */
    private LandedItemSource(Table table, UUID itemId, KeyProvider keys,
            Summary located, boolean refreshed)
    {
        Record summary = located.row();
        this.table = table;
        this.itemId = itemId;
        this.file = located.file();
        this.fileSize = located.fileSize();
        this.firstInFile = located.firstChunkInFile();
        this.refreshed = refreshed;
        this.keys = keys;
        this.size = (Long) summary.getField(ITEM_SIZE);
        this.chunkCount = (Integer) summary.getField(CHUNK_COUNT);
        this.chunkSize = (Integer) summary.getField(CHUNK_SIZE);
        this.digest = (String) summary.getField(ITEM_DIGEST);
        this.messageIndex = LandedItems.messageIndex(summary).orElse(null);
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
        return new LandedItemSource(table, itemId, keys,
                new Summary(summary, null, -1, -1), true);
    }

    /**
     * Opens a fully landed item whose summary row has been read: its chunks
     * in the final data file are read by position, any before it by scan.
     *
     * @param table   the {@value LandingTables#LANDED_ITEMS} table
     * @param itemId  the item
     * @param keys    unwraps the batch data keys
     * @param summary its summary row, from {@link #summary}
     * @return the item's bytes
     */
    public static LandedItemSource open(Table table, UUID itemId,
            KeyProvider keys, Summary summary)
    {
        return new LandedItemSource(table, itemId, keys, summary, false);
    }

    /**
     * Reads a landed item's summary row: by position, where its
     * {@code item.landed} says (PERFORMANCE.md §3.2), otherwise by scan of
     * the refreshed table.
     *
     * @param table   the {@value LandingTables#LANDED_ITEMS} table
     * @param itemId  the item
     * @param storage where its {@code item.landed} says it is; null if not
     *                known
     * @return the summary; empty if the item isn't fully landed
     * @throws IOException if the table can't be read
     */
    public static Optional<Summary> summary(Table table, UUID itemId,
            StorageRef storage) throws IOException
    {
        if (storage != null && storage.hasFile())
        {
            try
            {
                List<Record> found = DataFileRows.read(table.io(),
                        table.schema(), storage.file(), storage.fileSize(),
                        storage.firstRow(), 1, table.schema().columns()
                                .stream().map(Types.NestedField::name)
                                .toList());
                if (found.size() == 1
                        && itemId.equals(found.get(0).getField(ITEM_ID))
                        && (Integer) found.get(0).getField(CHUNK_INDEX)
                                == LandingTables.SUMMARY_ROW)
                {
                    return Optional.of(new Summary(found.get(0),
                            storage.file(), storage.fileSize(),
                            storage.firstRow()));
                }
            }
            catch (NotFoundException gone)
            {
                // Rewritten by maintenance: found by scan below.
            }
        }
        table.refresh();
        return LandedItems.summary(table, itemId)
                .map(row -> new Summary(row, null, -1, -1));
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

    /**
     * Where each message starts, if the item is a message batch.
     *
     * @return the message index; empty for other items
     */
    public Optional<MessageIndex> messageIndex()
    {
        return Optional.ofNullable(messageIndex);
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
     * Reads chunks by their position in the item's final data file, where
     * they are in it.
     *
     * @param first the first chunk's index
     * @param count how many chunks
     * @return their rows; null if they aren't all in that file, or it is
     *         gone, or the rows there are another item's: then they are
     *         found by scan
     * @throws IOException if the file can't be read
     */
    private List<Record> positioned(int first, int count) throws IOException
    {
        if (file == null || first < firstInFile)
        {
            return null;
        }
        try
        {
            List<Record> found = new ArrayList<>(DataFileRows.read(
                    table.io(), table.schema(), file, fileSize,
                    first - firstInFile, count, CHUNK_COLUMNS));
            for (Record row : found)
            {
                if (!itemId.equals(row.getField(ITEM_ID)))
                {
                    return null;
                }
            }
            return found.size() == count ? found : null;
        }
        catch (NotFoundException gone)
        {
            return null;
        }
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
        List<Record> rows = positioned(first, count);
        if (rows == null)
        {
            if (!refreshed)
            {
                // The landing node commits from another process.
                table.refresh();
                refreshed = true;
            }
            rows = LandedItems.rows(table, Expressions.and(
                    Expressions.equal(ITEM_ID, itemId),
                    Expressions.greaterThanOrEqual(CHUNK_INDEX, first),
                    Expressions.lessThan(CHUNK_INDEX, first + count)));
        }
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
            chunks[offset] = RecordCrypto.decryptChunk(
                    RecordCrypto.recordKey(dataKey, itemId, 0), itemId, 0,
                    chunkIndex, 0, length,
                    LandedItems.bytes(row.getField(CIPHERTEXT)));
            bytes += length;
        }
        Telemetry.get().bytesRead(bytes, "landing");
        return chunks;
    }
}
