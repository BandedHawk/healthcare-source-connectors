/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.reader;

import static com.sdh.healthcare.connector.contract.table.SectionsTable.BATCH_ID;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.BDK_WRAPPED;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.CHUNK_COUNT;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.CHUNK_INDEX;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.CIPHERTEXT;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.FILE_ID;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.INGESTED_AT;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.KEK_REF;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.PLAINTEXT_LENGTH;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.SECTION_ID;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.SEQ;

import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionInStorage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Streams one section: scans its chunk rows, puts them in order and
 * decrypts them one at a time.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class SectionInputStream extends InputStream
{

    /** The message announcing the section. */
    private final SectionInStorage message;

    /** The batch data keys unwrapped so far. */
    private final BatchKeyCache keys;

    /** The scan of the section's chunk rows. */
    private final CloseableIterable<Record> rows;

    /** Where the scan has got to. */
    private final Iterator<Record> rowIterator;

    /** Chunks the scan returned ahead of order, by index, still encrypted. */
    private final Map<Integer, byte[]> ahead = new HashMap<>();

    /** The section's key, derived from the first row's batch key. */
    private SecretKey sectionKey;

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
     * Starts the scan of a section's rows.
     *
     * @param table the sections table
     * @param message the message announcing the section
     * @param keys the batch data keys unwrapped so far
     */
    private SectionInputStream(Table table, SectionInStorage message,
            BatchKeyCache keys)
    {
        this.message = message;
        this.keys = keys;
        long dayStart = TimeUnit.SECONDS.toMicros(message.storage()
                .ingestedDate().atStartOfDay(ZoneOffset.UTC).toEpochSecond());
        Expression filter = Expressions.and(
                Expressions.equal(FILE_ID, message.fileId()),
                Expressions.equal(SEQ, message.seq()),
                Expressions.greaterThanOrEqual(INGESTED_AT, dayStart),
                Expressions.lessThan(INGESTED_AT,
                        dayStart + TimeUnit.DAYS.toMicros(1)));
        this.rows = IcebergGenerics.read(table)
                .where(filter)
                .select(SECTION_ID, CHUNK_INDEX, CHUNK_COUNT, PLAINTEXT_LENGTH,
                        CIPHERTEXT, BDK_WRAPPED, KEK_REF, BATCH_ID)
                .build();
        this.rowIterator = rows.iterator();
    }

    /**
     * Opens a section for reading.
     *
     * @param table the sections table
     * @param message the message announcing the section
     * @param keys the batch data keys unwrapped so far
     * @return the stream
     */
    static SectionInputStream open(Table table, SectionInStorage message,
            BatchKeyCache keys)
    {
        return new SectionInputStream(table, message, keys);
    }

    /**
     * {@inheritDoc}
     *
     * @throws SectionNotFoundException if the section has no rows
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
     * @throws SectionNotFoundException if the section has no rows
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
     * @return false at the end of the section
     * @throws SectionNotFoundException if the section has no rows
     * @throws ChunkIntegrityException if a chunk is missing, duplicated, out
     *         of place, of the wrong length, or fails authentication
     */
    private boolean fill()
    {
        while (position >= current.length)
        {
            if (nextIndex >= message.chunkCount())
            {
                if (delivered != message.plaintextLength())
                {
                    throw new ChunkIntegrityException(
                            "Section length mismatch");
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
                        throw new SectionNotFoundException();
                    }
                    throw new ChunkIntegrityException("Section incomplete");
                }
                Record row = rowIterator.next();
                sawRows = true;
                int index = check(row);
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
            int expected = SectionCrypto.expectedChunkLength(
                    message.plaintextLength(), nextIndex);
            current = SectionCrypto.decryptChunk(sectionKey, message.fileId(),
                    message.seq(), nextIndex, message.chunkCount(), expected,
                    stored);
            position = 0;
            delivered += current.length;
            nextIndex++;
        }
        return true;
    }

    /**
     * Checks a row against the message and sets up the section key on the
     * first row.
     *
     * @param row the row
     * @return the row's chunk index
     * @throws ChunkIntegrityException if the row doesn't match the message
     *         or the first row's batch, or its index is out of range
     */
    private int check(Record row)
    {
        if (!message.sectionId().equals(row.getField(SECTION_ID))
                || message.chunkCount() != (Integer) row.getField(CHUNK_COUNT)
                || message.plaintextLength()
                        != (Long) row.getField(PLAINTEXT_LENGTH))
        {
            throw new ChunkIntegrityException("Row does not match message");
        }
        UUID rowBatch = (UUID) row.getField(BATCH_ID);
        if (sectionKey == null)
        {
            batchId = rowBatch;
            byte[] batchKey = keys.get(rowBatch,
                    new WrappedKey((String) row.getField(KEK_REF),
                            bytes((ByteBuffer) row.getField(BDK_WRAPPED))));
            try
            {
                sectionKey = SectionCrypto.sectionKey(batchKey,
                        message.fileId(), message.seq());
            }
            finally
            {
                Arrays.fill(batchKey, (byte) 0);
            }
        }
        else if (!batchId.equals(rowBatch))
        {
            throw new ChunkIntegrityException(
                    "Chunks from different batches");
        }
        int index = (Integer) row.getField(CHUNK_INDEX);
        if (index < 0 || index >= message.chunkCount())
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
