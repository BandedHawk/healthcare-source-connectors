/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import com.sdh.healthcare.connector.core.convert.Representation;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import static com.sdh.healthcare.connector.contract.table.RecordsTable.*;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.position.RecordOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.SplitRecord;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;

/**
 * Writes one commit batch of records to a single Parquet data file (design
 * §7.2, §8.2).
 * <p>
 * There is one batch data key per batch, wrapped once. Every record is
 * chunked, compressed and encrypted with its own key, derived from it.
 * Records are streamed from the source; at most one chunk is held in memory
 * per record being processed.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RecordBatchWriter implements AutoCloseable
{

    /** Where the batch data keys come from. */
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * A record given as bytes in memory, for tests and spikes.
     *
     * @param itemId       the id of its file
     * @param seq          its number in the file, from 0
     * @param sourceOffset where it starts in the file, in bytes
     * @param format       the name of the file's format
     * @param plaintext    its bytes
     */
    public record RecordInput(UUID itemId, long seq, long sourceOffset,
            String format, byte[] plaintext)
    {
    }

    /**
     * A record as written to the batch: what its message and audit record
     * need.
     *
     * @param recordId        the record's id, a keyed hash of its stored
     *                        bytes
     * @param itemId          the id of its file
     * @param seq             its number in the file, from 0
     * @param sourceOffset    where it starts in the file, in bytes
     * @param sourceLength    how many bytes of the file it covers
     * @param format          the name of the file's format
     * @param representation  what the stored bytes are (design §6.4)
     * @param error           set when conversion failed and the record is
     *                        stored in its original form
     * @param errorPosition   where in the record conversion failed, as a
     *                        byte position; -1 without an error. Not stored:
     *                        for audit only.
     * @param plaintextLength the stored length before compression and
     *                        encryption
     * @param chunkCount      how many encrypted chunks it is stored in
     * @param origin          where the record's text came from in the file;
     *                        null if not worked out
     * @param errorAt         where in the file conversion failed, as a line
     *                        and column; null without an error, or when the
     *                        error has no place (a record too large to
     *                        convert, say)
     */
    public record WrittenRecord(String recordId, UUID itemId, long seq,
            long sourceOffset, long sourceLength, String format,
            Representation representation, ErrorCode error,
            long errorPosition, long plaintextLength, int chunkCount,
            RecordOrigin origin, SourcePosition errorAt)
    {
    }

    /** The {@code records} table the batch is written for. */
    private final Table table;

    /** Makes record ids, keyed hashes of the stored bytes. */
    private final RecordIds ids;

    /** The batch's id, also the data file's name. */
    private final UUID batchId = UUID.randomUUID();

    /** When the batch is written, in UTC: every row's ingestion time. */
    private final OffsetDateTime ingestedAt;

    /** The batch data key, wiped when the batch is finished or abandoned. */
    private final byte[] batchDataKey = new byte[32];

    /** The batch data key, wrapped by the key encryption key. */
    private final WrappedKey wrapped;

    /** The Parquet writer of the batch's data file. */
    private final DataWriter<Record> writer;

    /** Wraps rows so the partition can be worked out from them. */
    private final InternalRecordWrapper wrapper;

    /** The records written so far, in order. */
    private final List<WrittenRecord> written = new ArrayList<>();

    /** The total plaintext length of the records written so far. */
    private long plaintextBytes;

    /**
     * Starts a batch: makes and wraps its data key and opens its data file in
     * the partition of {@code ingestedAt}.
     *
     * @param table      the {@code records} table
     * @param keys       the key provider that wraps the data key
     * @param kekRef     which key encryption key wraps it
     * @param ids        makes record ids
     * @param ingestedAt when the batch is written
     * @throws UncheckedIOException if the data file can't be created
     */
    public RecordBatchWriter(Table table, KeyProvider keys, String kekRef,
            RecordIds ids, Instant ingestedAt)
    {
        this.table = table;
        this.ids = ids;
        this.ingestedAt = OffsetDateTime.ofInstant(ingestedAt, ZoneOffset.UTC);
        RANDOM.nextBytes(batchDataKey);
        this.wrapped = keys.wrap(kekRef, batchDataKey);
        this.wrapper = new InternalRecordWrapper(table.schema().asStruct());

        GenericRecord probe = GenericRecord.create(table.schema());
        probe.setField(INGESTED_AT, this.ingestedAt);
        PartitionKey partition =
                new PartitionKey(table.spec(), table.schema());
        partition.partition(wrapper.wrap(probe));

        String location = table.locationProvider().newDataLocation(
                table.spec(), partition,
                FileFormat.PARQUET.addExtension(batchId.toString()));
        try
        {
            this.writer = Parquet.writeData(table.io().newOutputFile(location))
                    .schema(table.schema())
                    .withSpec(table.spec())
                    .withPartition(partition)
                    .withSortOrder(table.sortOrder())
                    .setAll(table.properties())
                    .setAll(IngestTables.WRITE_OVERRIDES)
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .build();
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
    }

    /**
     * The day the batch is written, in UTC: its partition.
     *
     * @return the date
     */
    public LocalDate ingestedDate()
    {
        return ingestedAt.toLocalDate();
    }

    /**
     * Which key encryption key wrapped the batch data key.
     *
     * @return the key's reference
     */
    public String kekRef()
    {
        return wrapped.keyRef();
    }

    /**
     * Stores a record given as bytes in memory, as split. For tests and
     * spikes.
     *
     * @param input the record
     * @return the record as written
     * @throws UncheckedIOException if it can't be read
     */
    public WrittenRecord add(RecordInput input)
    {
        long length = input.plaintext().length;
        SplitRecord record = new SplitRecord(input.sourceOffset(), length,
                List.of(new SplitRecord.SourceRange(0, length)),
                ResumePoint.at(input.sourceOffset() + length));
        try
        {
            return add(input.itemId(), input.seq(), input.format(), record,
                    SeekableByteSource.of(input.plaintext()));
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
    }

    /**
     * A record hashed and encrypted, ready to be written as rows.
     *
     * @param record the record as it will be written
     * @param chunks its encrypted chunks, in order
     */
    public record EncryptedRecord(WrittenRecord record, List<byte[]> chunks)
    {
    }

    /**
     * Reads, hashes and encrypts a record of at most one chunk, as split.
     * It is thread-safe, so the pipeline runs it on a worker pool; the result
     * is then passed to {@link #write} on the writer thread.
     *
     * @param itemId the id of its file
     * @param seq    its number in the file, from 0
     * @param format the name of the file's format
     * @param record the record
     * @param source the file it is read from
     * @return the encrypted record
     * @throws IOException              if the source can't be read or ends
     *                                  inside the record
     * @throws IllegalArgumentException if the record spans more than one
     *                                  chunk
     */
    public EncryptedRecord encryptSmall(UUID itemId, long seq, String format,
            SplitRecord record, SeekableByteSource source) throws IOException
    {
        long length = record.plaintextLength();
        if (RecordCrypto.chunkCount(length) != 1)
        {
            throw new IllegalArgumentException(
                    "SplitRecord spans more than one chunk");
        }
        byte[] plaintext = record.readAll(source);
        if (plaintext.length != length)
        {
            throw new IOException("Source ended inside a record");
        }
        return encrypt(itemId, seq, format, record, plaintext,
                Representation.raw(format), null, -1, null);
    }

    /**
     * Hashes and encrypts record bytes already in memory (converted JSON,
     * or the original after a failed conversion) of at most one chunk. It is
     * thread-safe, and wipes {@code plaintext}.
     *
     * @param itemId         the id of its file
     * @param seq            its number in the file, from 0
     * @param format         the name of the file's format
     * @param lineage        the record as split: its place in the original
     *                       file and its origin
     * @param plaintext      the bytes to store
     * @param representation what they are
     * @param error          why conversion failed; null if it didn't
     * @param errorPosition  where in the record it failed, in bytes; -1
     *                       without an error
     * @param errorAt        where in the file it failed; null if it didn't,
     *                       or the error has no place
     * @return the encrypted record
     * @throws IllegalArgumentException if the bytes span more than one chunk
     */
    public EncryptedRecord encrypt(UUID itemId, long seq, String format,
            SplitRecord lineage, byte[] plaintext,
            Representation representation, ErrorCode error,
            long errorPosition, SourcePosition errorAt)
    {
        if (RecordCrypto.chunkCount(plaintext.length) != 1)
        {
            throw new IllegalArgumentException(
                    "SplitRecord spans more than one chunk");
        }
        try
        {
            String recordId =
                    RecordIds.encode(ids.newMac().doFinal(plaintext));
            SecretKey key =
                    RecordCrypto.recordKey(batchDataKey, itemId, seq);
            byte[] ciphertext = RecordCrypto.encryptChunk(key, itemId, seq, 0,
                    1, plaintext);
            return new EncryptedRecord(new WrittenRecord(recordId, itemId,
                    seq, lineage.sourceOffset(), lineage.sourceLength(),
                    format, representation, error, errorPosition,
                    plaintext.length, 1, lineage.origin(), errorAt),
                    List.of(ciphertext));
        }
        finally
        {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    /**
     * Stores record bytes held in memory, of any size. Writer thread only;
     * wipes {@code plaintext}.
     *
     * @param itemId         the id of its file
     * @param seq            its number in the file, from 0
     * @param format         the name of the file's format
     * @param lineage        the record as split: its place in the original
     *                       file and its origin
     * @param plaintext      the bytes to store
     * @param representation what they are
     * @param error          why conversion failed; null if it didn't
     * @param errorPosition  where in the record it failed, in bytes; -1
     *                       without an error
     * @param errorAt        where in the file it failed; null if it didn't,
     *                       or the error has no place
     * @return the record as written
     * @throws IOException if the bytes can't be read back while storing them
     */
    public WrittenRecord addBytes(UUID itemId, long seq, String format,
            SplitRecord lineage, byte[] plaintext,
            Representation representation, ErrorCode error,
            long errorPosition, SourcePosition errorAt)
            throws IOException
    {
        if (RecordCrypto.chunkCount(plaintext.length) == 1)
        {
            return write(encrypt(itemId, seq, format, lineage, plaintext,
                    representation, error, errorPosition, errorAt));
        }
        try
        {
            SplitRecord inMemory = new SplitRecord(lineage.sourceOffset(),
                    lineage.sourceLength(),
                    List.of(new SplitRecord.SourceRange(0, plaintext.length)),
                    lineage.resume(), lineage.origin());
            return add(itemId, seq, format, inMemory,
                    SeekableByteSource.of(plaintext), representation, error,
                    errorPosition, errorAt);
        }
        finally
        {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    /**
     * Writes an encrypted record's rows. Writer thread only; records must
     * arrive in sequence order.
     *
     * @param encrypted the record, from {@link #encrypt} or
     *                  {@link #encryptSmall}
     * @return the record as written
     */
    public WrittenRecord write(EncryptedRecord encrypted)
    {
        WrittenRecord record = encrypted.record();
        for (int chunkIndex = 0; chunkIndex < encrypted.chunks().size();
                chunkIndex++)
        {
            writeRow(record, chunkIndex, encrypted.chunks().get(chunkIndex));
        }
        written.add(record);
        plaintextBytes += record.plaintextLength();
        return record;
    }

    /**
     * Adds a record of any size, as split, streaming it from the source.
     * Writer thread only.
     * <p>
     * A record of several chunks is read twice: once to work out its id,
     * which every chunk's row carries, and once to encrypt and write it.
     * Memory therefore stays at one chunk however large the record is. The
     * second pass checks the hash again, so a source that changed in between
     * fails the batch.
     *
     * @param itemId the id of its file
     * @param seq    its number in the file, from 0
     * @param format the name of the file's format
     * @param record the record
     * @param source the file it is read from
     * @return the record as written
     * @throws IOException if the source can't be read, ends inside the
     *                     record or changes while it is stored
     */
    public WrittenRecord add(UUID itemId, long seq, String format,
            SplitRecord record, SeekableByteSource source) throws IOException
    {
        return add(itemId, seq, format, record, source,
                Representation.raw(format), null, -1, null);
    }

    /**
     * As {@link #add(UUID, long, String, SplitRecord, SeekableByteSource)},
     * stating what the bytes are.
     *
     * @param itemId         the id of its file
     * @param seq            its number in the file, from 0
     * @param format         the name of the file's format
     * @param record         the record
     * @param source         the bytes it is read from
     * @param representation what they are
     * @param error          why conversion failed; null if it didn't
     * @param errorPosition  where in the record it failed, in bytes; -1
     *                       without an error
     * @param errorAt        where in the file it failed; null if it didn't,
     *                       or the error has no place
     * @return the record as written
     * @throws IOException if the source can't be read, ends inside the
     *                     record or changes while it is stored
     */
    public WrittenRecord add(UUID itemId, long seq, String format,
            SplitRecord record, SeekableByteSource source,
            Representation representation, ErrorCode error,
            long errorPosition, SourcePosition errorAt) throws IOException
    {
        long length = record.plaintextLength();
        int chunkCount = RecordCrypto.chunkCount(length);
        if (chunkCount == 1)
        {
            byte[] plaintext = record.readAll(source);
            if (plaintext.length != length)
            {
                throw new IOException("Source ended inside a record");
            }
            return write(encrypt(itemId, seq, format, record, plaintext,
                    representation, error, errorPosition, errorAt));
        }
        byte[] buffer = new byte[RecordCrypto.CHUNK_SIZE];
        try
        {
            Mac first = ids.newMac();
            try (InputStream input = record.open(source))
            {
                for (int chunkIndex = 0; chunkIndex < chunkCount;
                        chunkIndex++)
                {
                    first.update(buffer, 0,
                            readChunk(input, buffer, length, chunkIndex));
                }
            }
            byte[] digest = first.doFinal();
            WrittenRecord stored = new WrittenRecord(
                    RecordIds.encode(digest), itemId, seq,
                    record.sourceOffset(), record.sourceLength(), format,
                    representation, error, errorPosition, length, chunkCount,
                    record.origin(), errorAt);
            SecretKey key =
                    RecordCrypto.recordKey(batchDataKey, itemId, seq);
            Mac second = ids.newMac();
            try (InputStream input = record.open(source))
            {
                for (int chunkIndex = 0; chunkIndex < chunkCount;
                        chunkIndex++)
                {
                    int chunkLength =
                            readChunk(input, buffer, length, chunkIndex);
                    second.update(buffer, 0, chunkLength);
                    writeRow(stored, chunkIndex, RecordCrypto.encryptChunk(
                            key, itemId, seq, chunkIndex, chunkCount,
                            Arrays.copyOf(buffer, chunkLength)));
                }
            }
            if (!MessageDigest.isEqual(digest, second.doFinal()))
            {
                throw new IOException(
                        "Source changed while a record was being stored");
            }
            written.add(stored);
            plaintextBytes += length;
            return stored;
        }
        finally
        {
            Arrays.fill(buffer, (byte) 0);
        }
    }

    /**
     * Reads one chunk of a record into the buffer.
     *
     * @param input      the record's plaintext
     * @param buffer     where the chunk goes, from its start
     * @param length     the record's whole length
     * @param chunkIndex which chunk this is, from 0
     * @return the chunk's length
     * @throws IOException if the record ends before the chunk does
     */
    private static int readChunk(InputStream input, byte[] buffer,
            long length, int chunkIndex) throws IOException
    {
        int expected = RecordCrypto.expectedChunkLength(length, chunkIndex);
        if (input.readNBytes(buffer, 0, expected) != expected)
        {
            throw new IOException("Source ended inside a record");
        }
        return expected;
    }

    /**
     * Writes one chunk of a record as a row.
     *
     * @param record     the record
     * @param chunkIndex which chunk this is, from 0
     * @param ciphertext the encrypted chunk
     */
    private void writeRow(WrittenRecord record, int chunkIndex,
            byte[] ciphertext)
    {
        GenericRecord row = GenericRecord.create(table.schema());
        row.setField(RECORD_ID, record.recordId());
        row.setField(ITEM_ID, record.itemId());
        row.setField(SEQ, record.seq());
        row.setField(CHUNK_INDEX, chunkIndex);
        row.setField(CHUNK_COUNT, record.chunkCount());
        row.setField(SOURCE_OFFSET, record.sourceOffset());
        row.setField(SOURCE_LENGTH, record.sourceLength());
        row.setField(FORMAT, record.format());
        row.setField(PLAINTEXT_LENGTH, record.plaintextLength());
        row.setField(CIPHERTEXT, ByteBuffer.wrap(ciphertext));
        row.setField(BDK_WRAPPED, ByteBuffer.wrap(wrapped.wrapped()));
        row.setField(KEK_REF, wrapped.keyRef());
        row.setField(BATCH_ID, batchId);
        row.setField(INGESTED_AT, ingestedAt);
        row.setField(CONTENT_TYPE, record.representation().contentType());
        row.setField(MAPPING, record.representation().mapping());
        row.setField(ERROR_CODE,
                record.error() == null ? null : record.error().name());
        if (record.origin() != null)
        {
            row.setField(RECORD_LINE, record.origin().inRecord().line());
            row.setField(RECORD_COLUMN,
                    record.origin().inRecord().column());
            row.setField(SOURCE_LINE, record.origin().inItem().line());
            row.setField(SOURCE_COLUMN, record.origin().inItem().column());
        }
        if (record.errorAt() != null)
        {
            row.setField(ERROR_LINE, record.errorAt().line());
            row.setField(ERROR_COLUMN, record.errorAt().column());
        }
        writer.write(row);
    }

    /**
     * The total plaintext length of the records written so far, for the
     * {@code commit.max-bytes} threshold.
     *
     * @return the length, in bytes
     */
    public long plaintextBytes()
    {
        return plaintextBytes;
    }

    /**
     * How many records have been written.
     *
     * @return the number of records
     */
    public int size()
    {
        return written.size();
    }

    /**
     * The records written so far, in order.
     *
     * @return a copy of the list
     */
    public List<WrittenRecord> written()
    {
        return List.copyOf(written);
    }

    /**
     * Closes the Parquet file, for appending in the batch commit, and wipes
     * the batch data key.
     *
     * @return the data file
     * @throws UncheckedIOException if the file can't be closed
     */
    public DataFile finish()
    {
        try
        {
            writer.close();
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
        finally
        {
            Arrays.fill(batchDataKey, (byte) 0);
        }
        return writer.toDataFile();
    }

    /**
     * Abandons the batch. Best effort, on the failure path only: it never
     * throws, so the error that stopped the batch is the one reported. After
     * a failed {@link #finish()} Parquet refuses a second close ("Cannot
     * return unfinished footer"), which used to replace the original error.
     */
    @Override
    public void close()
    {
        Arrays.fill(batchDataKey, (byte) 0);
        try
        {
            writer.close();
        }
        catch (IOException | RuntimeException ignored)
        {
            // An abandoned batch: orphan cleanup removes the file.
        }
    }
}
