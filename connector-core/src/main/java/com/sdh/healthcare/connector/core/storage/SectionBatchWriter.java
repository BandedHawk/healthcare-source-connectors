package com.sdh.healthcare.connector.core.storage;

import static com.sdh.healthcare.connector.contract.table.SectionsTable.*;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
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
 * Writes one commit batch of sections to a single Parquet data file (design §7.2, §8.2).
 *
 * <p>One batch data key per batch, wrapped once. Every section is chunked, compressed and encrypted
 * with its own derived key. Sections are streamed from the source; at most one chunk is held in memory
 * per section being processed.
 */
public final class SectionBatchWriter implements AutoCloseable {
    private static final SecureRandom RANDOM = new SecureRandom();

    public record SectionInput(UUID fileId, long seq, long sourceOffset, String format, byte[] plaintext) {}

    public record WrittenSection(String sectionId, UUID fileId, long seq, long sourceOffset, long sourceLength,
            String format, long plaintextLength, int chunkCount) {}

    private final Table table;
    private final SectionIds ids;
    private final UUID batchId = UUID.randomUUID();
    private final OffsetDateTime ingestedAt;
    private final byte[] bdk = new byte[32];
    private final WrappedKey wrapped;
    private final DataWriter<Record> writer;
    private final InternalRecordWrapper wrapper;
    private final List<WrittenSection> written = new ArrayList<>();
    private long plaintextBytes;

    public SectionBatchWriter(Table table, KeyProvider keys, String kekRef, SectionIds ids, Instant ingestedAt) {
        this.table = table;
        this.ids = ids;
        this.ingestedAt = OffsetDateTime.ofInstant(ingestedAt, ZoneOffset.UTC);
        RANDOM.nextBytes(bdk);
        this.wrapped = keys.wrap(kekRef, bdk);
        this.wrapper = new InternalRecordWrapper(table.schema().asStruct());

        GenericRecord probe = GenericRecord.create(table.schema());
        probe.setField(INGESTED_AT, this.ingestedAt);
        PartitionKey partition = new PartitionKey(table.spec(), table.schema());
        partition.partition(wrapper.wrap(probe));

        String location = table.locationProvider()
                .newDataLocation(table.spec(), partition, FileFormat.PARQUET.addExtension(batchId.toString()));
        try {
            this.writer = Parquet.writeData(table.io().newOutputFile(location))
                    .schema(table.schema())
                    .withSpec(table.spec())
                    .withPartition(partition)
                    .withSortOrder(table.sortOrder())
                    .setAll(table.properties())
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public LocalDate ingestedDate() {
        return ingestedAt.toLocalDate();
    }

    public String kekRef() {
        return wrapped.keyRef();
    }

    /** Spike/test convenience: a section given as bytes in memory. */
    public WrittenSection add(SectionInput in) {
        long len = in.plaintext().length;
        Section section = new Section(in.sourceOffset(), len, List.of(new Section.SourceRange(0, len)),
                ResumePoint.at(in.sourceOffset() + len));
        try {
            return add(in.fileId(), in.seq(), in.format(), section, SeekableByteSource.of(in.plaintext()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A section hashed and encrypted, ready to be written as rows. */
    public record EncryptedSection(WrittenSection section, List<byte[]> chunks) {}

    /**
     * Reads, hashes and encrypts a section of at most one chunk. Thread-safe, so the pipeline runs it on a
     * worker pool; the result is then passed to {@link #write} on the writer thread.
     */
    public EncryptedSection encryptSmall(UUID fileId, long seq, String format, Section section,
            SeekableByteSource src) throws IOException {
        long len = section.plaintextLength();
        if (SectionCrypto.chunkCount(len) != 1) {
            throw new IllegalArgumentException("Section spans more than one chunk");
        }
        byte[] plain = section.readAll(src);
        if (plain.length != len) {
            throw new IOException("Source ended inside a section");
        }
        try {
            String sectionId = SectionIds.encode(ids.newMac().doFinal(plain));
            SecretKey key = SectionCrypto.sectionKey(bdk, fileId, seq);
            byte[] ct = SectionCrypto.encryptChunk(key, fileId, seq, 0, 1, plain);
            return new EncryptedSection(new WrittenSection(sectionId, fileId, seq, section.sourceOffset(),
                    section.sourceLength(), format, len, 1), List.of(ct));
        } finally {
            Arrays.fill(plain, (byte) 0);
        }
    }

    /** Writes an encrypted section's rows. Writer thread only; sections must arrive in seq order. */
    public WrittenSection write(EncryptedSection e) {
        WrittenSection ws = e.section();
        for (int i = 0; i < e.chunks().size(); i++) {
            writeRow(ws, i, e.chunks().get(i));
        }
        written.add(ws);
        plaintextBytes += ws.plaintextLength();
        return ws;
    }

    /**
     * Adds a section of any size, streaming it from {@code src}. Writer thread only.
     *
     * <p>A multi-chunk section is read twice: once to compute the section ID, which every chunk row
     * carries, and once to encrypt and write. Memory therefore stays at one chunk however large the section
     * is. The second pass re-checks the hash, so a source that changed in between fails the batch.
     */
    public WrittenSection add(UUID fileId, long seq, String format, Section section, SeekableByteSource src)
            throws IOException {
        long len = section.plaintextLength();
        int chunkCount = SectionCrypto.chunkCount(len);
        if (chunkCount == 1) {
            return write(encryptSmall(fileId, seq, format, section, src));
        }
        byte[] buf = new byte[SectionCrypto.CHUNK_SIZE];
        try {
            Mac first = ids.newMac();
            try (InputStream in = section.open(src)) {
                for (int i = 0; i < chunkCount; i++) {
                    first.update(buf, 0, readChunk(in, buf, len, i));
                }
            }
            byte[] digest = first.doFinal();
            WrittenSection ws = new WrittenSection(SectionIds.encode(digest), fileId, seq, section.sourceOffset(),
                    section.sourceLength(), format, len, chunkCount);
            SecretKey key = SectionCrypto.sectionKey(bdk, fileId, seq);
            Mac second = ids.newMac();
            try (InputStream in = section.open(src)) {
                for (int i = 0; i < chunkCount; i++) {
                    int n = readChunk(in, buf, len, i);
                    second.update(buf, 0, n);
                    writeRow(ws, i, SectionCrypto.encryptChunk(key, fileId, seq, i, chunkCount,
                            Arrays.copyOf(buf, n)));
                }
            }
            if (!MessageDigest.isEqual(digest, second.doFinal())) {
                throw new IOException("Source changed while a section was being stored");
            }
            written.add(ws);
            plaintextBytes += len;
            return ws;
        } finally {
            Arrays.fill(buf, (byte) 0);
        }
    }

    private static int readChunk(InputStream in, byte[] buf, long len, int index) throws IOException {
        int expected = SectionCrypto.expectedChunkLength(len, index);
        if (in.readNBytes(buf, 0, expected) != expected) {
            throw new IOException("Source ended inside a section");
        }
        return expected;
    }

    private void writeRow(WrittenSection ws, int chunkIndex, byte[] ciphertext) {
        GenericRecord r = GenericRecord.create(table.schema());
        r.setField(SECTION_ID, ws.sectionId());
        r.setField(FILE_ID, ws.fileId());
        r.setField(SEQ, ws.seq());
        r.setField(CHUNK_INDEX, chunkIndex);
        r.setField(CHUNK_COUNT, ws.chunkCount());
        r.setField(SOURCE_OFFSET, ws.sourceOffset());
        r.setField(SOURCE_LENGTH, ws.sourceLength());
        r.setField(FORMAT, ws.format());
        r.setField(PLAINTEXT_LENGTH, ws.plaintextLength());
        r.setField(CIPHERTEXT, ByteBuffer.wrap(ciphertext));
        r.setField(BDK_WRAPPED, ByteBuffer.wrap(wrapped.wrapped()));
        r.setField(KEK_REF, wrapped.keyRef());
        r.setField(BATCH_ID, batchId);
        r.setField(INGESTED_AT, ingestedAt);
        writer.write(r);
    }

    /** Sum of plaintext lengths written so far; used for the {@code commit.max-bytes} threshold. */
    public long plaintextBytes() {
        return plaintextBytes;
    }

    public int size() {
        return written.size();
    }

    public List<WrittenSection> written() {
        return List.copyOf(written);
    }

    /** Closes the Parquet file and returns it for appending in the batch commit. */
    public DataFile finish() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            Arrays.fill(bdk, (byte) 0);
        }
        return writer.toDataFile();
    }

    @Override
    public void close() {
        Arrays.fill(bdk, (byte) 0);
        try {
            writer.close();
        } catch (IOException ignored) {
            // abandoned batch; orphan cleanup removes the file
        }
    }
}
