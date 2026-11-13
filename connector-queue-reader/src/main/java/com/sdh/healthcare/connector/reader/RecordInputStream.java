/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.reader;

import static com.sdh.healthcare.connector.contract.table.RecordsTable.BATCH_ID;
import static com.sdh.healthcare.connector.contract.table.RecordsTable.BDK_WRAPPED;
import static com.sdh.healthcare.connector.contract.table.RecordsTable.CHUNK_COUNT;
import static com.sdh.healthcare.connector.contract.table.RecordsTable.CHUNK_INDEX;
import static com.sdh.healthcare.connector.contract.table.RecordsTable.CIPHERTEXT;
import static com.sdh.healthcare.connector.contract.table.RecordsTable.ITEM_ID;
import static com.sdh.healthcare.connector.contract.table.RecordsTable.INGESTED_AT;
import static com.sdh.healthcare.connector.contract.table.RecordsTable.KEK_REF;
import static com.sdh.healthcare.connector.contract.table.RecordsTable.PLAINTEXT_LENGTH;
import static com.sdh.healthcare.connector.contract.table.RecordsTable.RECORD_ID;
import static com.sdh.healthcare.connector.contract.table.RecordsTable.SEQ;

import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordLocation;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.table.DataFileRows;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Streams one record: reads its chunk rows, by position in its data file
 * or by scan, puts them in order and decrypts them one at a time.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class RecordInputStream extends InputStream
{

    /** The columns a record is read with. */
    private static final List<String> COLUMNS = List.of(RECORD_ID, ITEM_ID,
            SEQ, CHUNK_INDEX, CHUNK_COUNT, PLAINTEXT_LENGTH, CIPHERTEXT,
            BDK_WRAPPED, KEK_REF, BATCH_ID);

    /** What {@link #check} returns for a row of another copy. */
    private static final int OTHER_COPY = -1;

    /** Where the record is, as its message or row gives it. */
    private final RecordLocation location;

    /** The batch data keys unwrapped so far. */
    private final BatchKeyCache keys;

    /** The record's chunk rows, read by position or by scan. */
    private final CloseableIterable<Record> rows;

    /** Where the scan has got to. */
    private final Iterator<Record> rowIterator;

    /** Chunks the scan returned ahead of order, by index, still encrypted. */
    private final Map<Integer, byte[]> ahead = new HashMap<>();

    /** The record's key, derived from the first row's batch key. */
    private SecretKey recordKey;

    /** The batch the first row came from; every row must match it. */
    private UUID batchId;

    /** The index of the next chunk to decrypt. */
    private int nextIndex;

    /** How many plaintext bytes have been decrypted so far. */
    private long delivered;

    /** The decrypted chunk being read. */
    private byte[] current = new byte[0];

    /** The read position in {@link #current}. */
    private int position;

    /** Whether the scan has returned any row. */
    private boolean sawRows;

    /**
     * Starts reading a record's rows: by position in its data file when its
     * location says which (PERFORMANCE.md §3.2), otherwise by scan.
     *
     * @param table   the records table
     * @param location where the record is
     * @param keys    the batch data keys unwrapped so far
     */
    private RecordInputStream(Table table, RecordLocation location,
            BatchKeyCache keys)
    {
        this.location = location;
        this.keys = keys;
        List<Record> positioned = byPosition(table, location);
        this.rows = positioned != null
                ? CloseableIterable.withNoopClose(positioned)
                : scan(table, location);
        this.rowIterator = rows.iterator();
    }

    /**
     * Reads a record's rows by their position in its data file.
     *
     * @param table    the records table
     * @param location where the record is
     * @return its rows; null if the location gives no file, or the file is
     *         gone (compacted and expired, PERFORMANCE.md §3.3) or its rows
     *         there are another record's, when the record is found by scan
     * @throws UncheckedIOException if the file can't be read
     */
    private static List<Record> byPosition(Table table,
            RecordLocation location)
    {
        StorageRef storage = location.storage();
        if (!storage.hasFile())
        {
            return null;
        }
        try
        {
            List<Record> found = DataFileRows.read(table.io(),
                    table.schema(), storage.file(), storage.fileSize(),
                    storage.firstRow(), location.chunkCount(), COLUMNS);
            if (found.size() != location.chunkCount())
            {
                return null;
            }
            for (Record row : found)
            {
                if (!location.itemId().equals(row.getField(ITEM_ID))
                        || location.seq() != (Long) row.getField(SEQ))
                {
                    return null;
                }
            }
            return found;
        }
        catch (NotFoundException gone)
        {
            return null;
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
    }

    /**
     * Scans for a record's rows, pruned to its day.
     *
     * @param table    the records table
     * @param location where the record is
     * @return the scan
     */
    private static CloseableIterable<Record> scan(Table table,
            RecordLocation location)
    {
        long dayStart = TimeUnit.SECONDS.toMicros(location.storage()
                .ingestedDate().atStartOfDay(ZoneOffset.UTC).toEpochSecond());
        Expression filter = Expressions.and(
                Expressions.equal(ITEM_ID, location.itemId()),
                Expressions.equal(SEQ, location.seq()),
                Expressions.greaterThanOrEqual(INGESTED_AT, dayStart),
                Expressions.lessThan(INGESTED_AT,
                        dayStart + TimeUnit.DAYS.toMicros(1)));
        return IcebergGenerics.read(table)
                .where(filter)
                .select(COLUMNS)
                .build();
    }

    /**
     * Opens a record for reading.
     *
     * @param table   the records table
     * @param location where the record is
     * @param keys    the batch data keys unwrapped so far
     * @return the stream
     */
    static RecordInputStream open(Table table, RecordLocation location,
            BatchKeyCache keys)
    {
        return new RecordInputStream(table, location, keys);
    }

    /**
     * {@inheritDoc}
     *
     * @throws RecordNotFoundException if the record has no rows
     * @throws ChunkIntegrityException if a chunk is missing, duplicated, out
     *         of place, of the wrong length, or fails authentication
     */
    @Override
    public int read() throws IOException
    {
        if (!fill())
        {
            return -1;
        }
        return current[position++] & 0xff;
    }

    /**
     * {@inheritDoc}
     *
     * @throws RecordNotFoundException if the record has no rows
     * @throws ChunkIntegrityException if a chunk is missing, duplicated, out
     *         of place, of the wrong length, or fails authentication
     */
    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException
    {
        if (length == 0)
        {
            return 0;
        }
        if (!fill())
        {
            return -1;
        }
        int count = Math.min(length, current.length - position);
        System.arraycopy(current, position, buffer, offset, count);
        position += count;
        return count;
    }

    /**
     * Decrypts the next chunk once the current one is read, scanning ahead
     * for it if it hasn't been seen yet.
     *
     * @return false at the end of the record
     * @throws RecordNotFoundException if the record has no rows
     * @throws ChunkIntegrityException if a chunk is missing, duplicated, out
     *         of place, of the wrong length, or fails authentication
     */
    private boolean fill()
    {
        while (position >= current.length)
        {
            if (nextIndex >= location.chunkCount())
            {
                if (delivered != location.plaintextLength())
                {
                    throw new ChunkIntegrityException(
                            "Record length mismatch");
                }
                return false;
            }
            byte[] stored = ahead.remove(nextIndex);
            while (stored == null)
            {
                if (!rowIterator.hasNext())
                {
                    if (!sawRows)
                    {
                        throw new RecordNotFoundException();
                    }
                    throw new ChunkIntegrityException("Record incomplete");
                }
                Record row = rowIterator.next();
                sawRows = true;
                int index = check(row);
                if (index == OTHER_COPY)
                {
                    continue;
                }
                byte[] ciphertext =
                        bytes((ByteBuffer) row.getField(CIPHERTEXT));
                if (index == nextIndex)
                {
                    stored = ciphertext;
                }
                else if (index < nextIndex
                        || ahead.putIfAbsent(index, ciphertext) != null)
                {
                    throw new ChunkIntegrityException("Duplicate chunk");
                }
            }
            int expected = RecordCrypto.expectedChunkLength(
                    location.plaintextLength(), nextIndex);
            current = RecordCrypto.decryptChunk(recordKey, location.itemId(),
                    location.seq(), nextIndex, location.chunkCount(), expected,
                    stored);
            position = 0;
            delivered += current.length;
            nextIndex++;
        }
        return true;
    }

    /**
     * Checks a row against the record's location, and sets up the record
     * key on the first row. The record's chunks are read from the batch of
     * the first row: a row of the same record in another batch is a second
     * copy of it, appended by a stage worker that lost its claim (STAGES.md
     * §6), and is skipped. Every append holds whole records, so each copy is
     * complete, and the same record ID means the same bytes.
     *
     * @param row the row
     * @return the row's chunk index; {@link #OTHER_COPY} for a row of
     *         another copy
     * @throws ChunkIntegrityException if the row doesn't match the location,
     *         or its index is out of range
     */
    private int check(Record row)
    {
        if (!location.recordId().equals(row.getField(RECORD_ID))
                || location.chunkCount() != (Integer) row.getField(CHUNK_COUNT)
                || location.plaintextLength()
                        != (Long) row.getField(PLAINTEXT_LENGTH))
        {
            throw new ChunkIntegrityException(
                    "Row does not match the record's location");
        }
        UUID rowBatch = (UUID) row.getField(BATCH_ID);
        if (recordKey == null)
        {
            batchId = rowBatch;
            byte[] batchKey = keys.get(rowBatch,
                    new WrappedKey((String) row.getField(KEK_REF),
                            bytes((ByteBuffer) row.getField(BDK_WRAPPED))));
            try
            {
                recordKey = RecordCrypto.recordKey(batchKey,
                        location.itemId(), location.seq());
            }
            finally
            {
                Arrays.fill(batchKey, (byte) 0);
            }
        }
        else if (!batchId.equals(rowBatch))
        {
            return OTHER_COPY;
        }
        int index = (Integer) row.getField(CHUNK_INDEX);
        if (index < 0 || index >= location.chunkCount())
        {
            throw new ChunkIntegrityException("Chunk index out of range");
        }
        return index;
    }

    /**
     * Copies a buffer's remaining bytes, leaving the buffer untouched.
     *
     * @param buffer the buffer
     * @return the bytes
     */
    private static byte[] bytes(ByteBuffer buffer)
    {
        byte[] copy = new byte[buffer.remaining()];
        buffer.duplicate().get(copy);
        return copy;
    }

    /**
     * Wipes the current chunk, drops chunks held ahead, and closes the
     * scan.
     *
     * @throws IOException if the scan can't be closed
     */
    @Override
    public void close() throws IOException
    {
        Arrays.fill(current, (byte) 0);
        ahead.clear();
        rows.close();
    }
}
