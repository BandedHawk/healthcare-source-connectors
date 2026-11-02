/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import com.sdh.healthcare.connector.core.convert.Representation;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import static com.sdh.healthcare.connector.contract.table.SectionsTable.*;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.position.SectionOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.Section;
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
 * Writes one commit batch of sections to a single Parquet data file (design
 * §7.2, §8.2).
 * <p>
 * There is one batch data key per batch, wrapped once. Every section is
 * chunked, compressed and encrypted with its own key, derived from it.
 * Sections are streamed from the source; at most one chunk is held in memory
 * per section being processed.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SectionBatchWriter implements AutoCloseable
{

    /** Where the batch data keys come from. */
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * A section given as bytes in memory, for tests and spikes.
     *
     * @param fileId       the id of its file
     * @param seq          its number in the file, from 0
     * @param sourceOffset where it starts in the file, in bytes
     * @param format       the name of the file's format
     * @param plaintext    its bytes
     */
    public record SectionInput(UUID fileId, long seq, long sourceOffset,
            String format, byte[] plaintext)
    {
    }

    /**
     * A section as written to the batch: what its message and audit record
     * need.
     *
     * @param sectionId       the section's id, a keyed hash of its stored
     *                        bytes
     * @param fileId          the id of its file
     * @param seq             its number in the file, from 0
     * @param sourceOffset    where it starts in the file, in bytes
     * @param sourceLength    how many bytes of the file it covers
     * @param format          the name of the file's format
     * @param representation  what the stored bytes are (design §6.4)
     * @param error           set when conversion failed and the section is
     *                        stored in its original form
     * @param errorPosition   where in the section conversion failed, as a
     *                        byte position; -1 without an error. Not stored:
     *                        for audit only.
     * @param plaintextLength the stored length before compression and
     *                        encryption
     * @param chunkCount      how many encrypted chunks it is stored in
     * @param origin          where the section's text came from in the file;
     *                        null if not worked out
     * @param errorAt         where in the file conversion failed, as a line
     *                        and column; null without an error, or when the
     *                        error has no place (a section too large to
     *                        convert, say)
     */
    public record WrittenSection(String sectionId, UUID fileId, long seq,
            long sourceOffset, long sourceLength, String format,
            Representation representation, ErrorCode error,
            long errorPosition, long plaintextLength, int chunkCount,
            SectionOrigin origin, SourcePosition errorAt)
    {
    }

    /** The {@code sections} table the batch is written for. */
    private final Table table;

    /** Makes section ids, keyed hashes of the stored bytes. */
    private final SectionIds ids;

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

    /** The sections written so far, in order. */
    private final List<WrittenSection> written = new ArrayList<>();

    /** The total plaintext length of the sections written so far. */
    private long plaintextBytes;

    /**
     * Starts a batch: makes and wraps its data key and opens its data file in
     * the partition of {@code ingestedAt}.
     *
     * @param table      the {@code sections} table
     * @param keys       the key provider that wraps the data key
     * @param kekRef     which key encryption key wraps it
     * @param ids        makes section ids
     * @param ingestedAt when the batch is written
     * @throws UncheckedIOException if the data file can't be created
     */
    public SectionBatchWriter(Table table, KeyProvider keys, String kekRef,
            SectionIds ids, Instant ingestedAt)
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
     * Stores a section given as bytes in memory, as split. For tests and
     * spikes.
     *
     * @param input the section
     * @return the section as written
     * @throws UncheckedIOException if it can't be read
     */
    public WrittenSection add(SectionInput input)
    {
        long length = input.plaintext().length;
        Section section = new Section(input.sourceOffset(), length,
                List.of(new Section.SourceRange(0, length)),
                ResumePoint.at(input.sourceOffset() + length));
        try
        {
            return add(input.fileId(), input.seq(), input.format(), section,
                    SeekableByteSource.of(input.plaintext()));
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
    }

    /**
     * A section hashed and encrypted, ready to be written as rows.
     *
     * @param section the section as it will be written
     * @param chunks  its encrypted chunks, in order
     */
    public record EncryptedSection(WrittenSection section, List<byte[]> chunks)
    {
    }

    /**
     * Reads, hashes and encrypts a section of at most one chunk, as split.
     * It is thread-safe, so the pipeline runs it on a worker pool; the result
     * is then passed to {@link #write} on the writer thread.
     *
     * @param fileId  the id of its file
     * @param seq     its number in the file, from 0
     * @param format  the name of the file's format
     * @param section the section
     * @param source  the file it is read from
     * @return the encrypted section
     * @throws IOException              if the source can't be read or ends
     *                                  inside the section
     * @throws IllegalArgumentException if the section spans more than one
     *                                  chunk
     */
    public EncryptedSection encryptSmall(UUID fileId, long seq, String format,
            Section section, SeekableByteSource source) throws IOException
    {
        long length = section.plaintextLength();
        if (SectionCrypto.chunkCount(length) != 1)
        {
            throw new IllegalArgumentException(
                    "Section spans more than one chunk");
        }
        byte[] plaintext = section.readAll(source);
        if (plaintext.length != length)
        {
            throw new IOException("Source ended inside a section");
        }
        return encrypt(fileId, seq, format, section, plaintext,
                Representation.raw(format), null, -1, null);
    }

    /**
     * Hashes and encrypts section bytes already in memory (converted JSON,
     * or the original after a failed conversion) of at most one chunk. It is
     * thread-safe, and wipes {@code plaintext}.
     *
     * @param fileId         the id of its file
     * @param seq            its number in the file, from 0
     * @param format         the name of the file's format
     * @param lineage        the section as split: its place in the original
     *                       file and its origin
     * @param plaintext      the bytes to store
     * @param representation what they are
     * @param error          why conversion failed; null if it didn't
     * @param errorPosition  where in the section it failed, in bytes; -1
     *                       without an error
     * @param errorAt        where in the file it failed; null if it didn't,
     *                       or the error has no place
     * @return the encrypted section
     * @throws IllegalArgumentException if the bytes span more than one chunk
     */
    public EncryptedSection encrypt(UUID fileId, long seq, String format,
            Section lineage, byte[] plaintext, Representation representation,
            ErrorCode error, long errorPosition, SourcePosition errorAt)
    {
        if (SectionCrypto.chunkCount(plaintext.length) != 1)
        {
            throw new IllegalArgumentException(
                    "Section spans more than one chunk");
        }
        try
        {
            String sectionId =
                    SectionIds.encode(ids.newMac().doFinal(plaintext));
            SecretKey key =
                    SectionCrypto.sectionKey(batchDataKey, fileId, seq);
            byte[] ciphertext = SectionCrypto.encryptChunk(key, fileId, seq, 0,
                    1, plaintext);
            return new EncryptedSection(new WrittenSection(sectionId, fileId,
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
     * Stores section bytes held in memory, of any size. Writer thread only;
     * wipes {@code plaintext}.
     *
     * @param fileId         the id of its file
     * @param seq            its number in the file, from 0
     * @param format         the name of the file's format
     * @param lineage        the section as split: its place in the original
     *                       file and its origin
     * @param plaintext      the bytes to store
     * @param representation what they are
     * @param error          why conversion failed; null if it didn't
     * @param errorPosition  where in the section it failed, in bytes; -1
     *                       without an error
     * @param errorAt        where in the file it failed; null if it didn't,
     *                       or the error has no place
     * @return the section as written
     * @throws IOException if the bytes can't be read back while storing them
     */
    public WrittenSection addBytes(UUID fileId, long seq, String format,
            Section lineage, byte[] plaintext, Representation representation,
            ErrorCode error, long errorPosition, SourcePosition errorAt)
            throws IOException
    {
        if (SectionCrypto.chunkCount(plaintext.length) == 1)
        {
            return write(encrypt(fileId, seq, format, lineage, plaintext,
                    representation, error, errorPosition, errorAt));
        }
        try
        {
            Section inMemory = new Section(lineage.sourceOffset(),
                    lineage.sourceLength(),
                    List.of(new Section.SourceRange(0, plaintext.length)),
                    lineage.resume(), lineage.origin());
            return add(fileId, seq, format, inMemory,
                    SeekableByteSource.of(plaintext), representation, error,
                    errorPosition, errorAt);
        }
        finally
        {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    /**
     * Writes an encrypted section's rows. Writer thread only; sections must
     * arrive in sequence order.
     *
     * @param encrypted the section, from {@link #encrypt} or
     *                  {@link #encryptSmall}
     * @return the section as written
     */
    public WrittenSection write(EncryptedSection encrypted)
    {
        WrittenSection section = encrypted.section();
        for (int chunkIndex = 0; chunkIndex < encrypted.chunks().size();
                chunkIndex++)
        {
            writeRow(section, chunkIndex, encrypted.chunks().get(chunkIndex));
        }
        written.add(section);
        plaintextBytes += section.plaintextLength();
        return section;
    }

    /**
     * Adds a section of any size, as split, streaming it from the source.
     * Writer thread only.
     * <p>
     * A section of several chunks is read twice: once to work out its id,
     * which every chunk's row carries, and once to encrypt and write it.
     * Memory therefore stays at one chunk however large the section is. The
     * second pass checks the hash again, so a source that changed in between
     * fails the batch.
     *
     * @param fileId  the id of its file
     * @param seq     its number in the file, from 0
     * @param format  the name of the file's format
     * @param section the section
     * @param source  the file it is read from
     * @return the section as written
     * @throws IOException if the source can't be read, ends inside the
     *                     section or changes while it is stored
     */
    public WrittenSection add(UUID fileId, long seq, String format,
            Section section, SeekableByteSource source) throws IOException
    {
        return add(fileId, seq, format, section, source,
                Representation.raw(format), null, -1, null);
    }

    /**
     * As {@link #add(UUID, long, String, Section, SeekableByteSource)},
     * stating what the bytes are.
     *
     * @param fileId         the id of its file
     * @param seq            its number in the file, from 0
     * @param format         the name of the file's format
     * @param section        the section
     * @param source         the bytes it is read from
     * @param representation what they are
     * @param error          why conversion failed; null if it didn't
     * @param errorPosition  where in the section it failed, in bytes; -1
     *                       without an error
     * @param errorAt        where in the file it failed; null if it didn't,
     *                       or the error has no place
     * @return the section as written
     * @throws IOException if the source can't be read, ends inside the
     *                     section or changes while it is stored
     */
    public WrittenSection add(UUID fileId, long seq, String format,
            Section section, SeekableByteSource source,
            Representation representation, ErrorCode error,
            long errorPosition, SourcePosition errorAt) throws IOException
    {
        long length = section.plaintextLength();
        int chunkCount = SectionCrypto.chunkCount(length);
        if (chunkCount == 1)
        {
            byte[] plaintext = section.readAll(source);
            if (plaintext.length != length)
            {
                throw new IOException("Source ended inside a section");
            }
            return write(encrypt(fileId, seq, format, section, plaintext,
                    representation, error, errorPosition, errorAt));
        }
        byte[] buffer = new byte[SectionCrypto.CHUNK_SIZE];
        try
        {
            Mac first = ids.newMac();
            try (InputStream input = section.open(source))
            {
                for (int chunkIndex = 0; chunkIndex < chunkCount;
                        chunkIndex++)
                {
                    first.update(buffer, 0,
                            readChunk(input, buffer, length, chunkIndex));
                }
            }
            byte[] digest = first.doFinal();
            WrittenSection stored = new WrittenSection(
                    SectionIds.encode(digest), fileId, seq,
                    section.sourceOffset(), section.sourceLength(), format,
                    representation, error, errorPosition, length, chunkCount,
                    section.origin(), errorAt);
            SecretKey key =
                    SectionCrypto.sectionKey(batchDataKey, fileId, seq);
            Mac second = ids.newMac();
            try (InputStream input = section.open(source))
            {
                for (int chunkIndex = 0; chunkIndex < chunkCount;
                        chunkIndex++)
                {
                    int chunkLength =
                            readChunk(input, buffer, length, chunkIndex);
                    second.update(buffer, 0, chunkLength);
                    writeRow(stored, chunkIndex, SectionCrypto.encryptChunk(
                            key, fileId, seq, chunkIndex, chunkCount,
                            Arrays.copyOf(buffer, chunkLength)));
                }
            }
            if (!MessageDigest.isEqual(digest, second.doFinal()))
            {
                throw new IOException(
                        "Source changed while a section was being stored");
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
     * Reads one chunk of a section into the buffer.
     *
     * @param input      the section's plaintext
     * @param buffer     where the chunk goes, from its start
     * @param length     the section's whole length
     * @param chunkIndex which chunk this is, from 0
     * @return the chunk's length
     * @throws IOException if the section ends before the chunk does
     */
    private static int readChunk(InputStream input, byte[] buffer,
            long length, int chunkIndex) throws IOException
    {
        int expected = SectionCrypto.expectedChunkLength(length, chunkIndex);
        if (input.readNBytes(buffer, 0, expected) != expected)
        {
            throw new IOException("Source ended inside a section");
        }
        return expected;
    }

    /**
     * Writes one chunk of a section as a row.
     *
     * @param section    the section
     * @param chunkIndex which chunk this is, from 0
     * @param ciphertext the encrypted chunk
     */
    private void writeRow(WrittenSection section, int chunkIndex,
            byte[] ciphertext)
    {
        GenericRecord row = GenericRecord.create(table.schema());
        row.setField(SECTION_ID, section.sectionId());
        row.setField(FILE_ID, section.fileId());
        row.setField(SEQ, section.seq());
        row.setField(CHUNK_INDEX, chunkIndex);
        row.setField(CHUNK_COUNT, section.chunkCount());
        row.setField(SOURCE_OFFSET, section.sourceOffset());
        row.setField(SOURCE_LENGTH, section.sourceLength());
        row.setField(FORMAT, section.format());
        row.setField(PLAINTEXT_LENGTH, section.plaintextLength());
        row.setField(CIPHERTEXT, ByteBuffer.wrap(ciphertext));
        row.setField(BDK_WRAPPED, ByteBuffer.wrap(wrapped.wrapped()));
        row.setField(KEK_REF, wrapped.keyRef());
        row.setField(BATCH_ID, batchId);
        row.setField(INGESTED_AT, ingestedAt);
        row.setField(CONTENT_TYPE, section.representation().contentType());
        row.setField(MAPPING, section.representation().mapping());
        row.setField(ERROR_CODE,
                section.error() == null ? null : section.error().name());
        if (section.origin() != null)
        {
            row.setField(SECTION_LINE, section.origin().inSection().line());
            row.setField(SECTION_COLUMN,
                    section.origin().inSection().column());
            row.setField(SOURCE_LINE, section.origin().inFile().line());
            row.setField(SOURCE_COLUMN, section.origin().inFile().column());
        }
        if (section.errorAt() != null)
        {
            row.setField(ERROR_LINE, section.errorAt().line());
            row.setField(ERROR_COLUMN, section.errorAt().column());
        }
        writer.write(row);
    }

    /**
     * The total plaintext length of the sections written so far, for the
     * {@code commit.max-bytes} threshold.
     *
     * @return the length, in bytes
     */
    public long plaintextBytes()
    {
        return plaintextBytes;
    }

    /**
     * How many sections have been written.
     *
     * @return the number of sections
     */
    public int size()
    {
        return written.size();
    }

    /**
     * The sections written so far, in order.
     *
     * @return a copy of the list
     */
    public List<WrittenSection> written()
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
