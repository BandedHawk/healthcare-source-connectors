package com.sdh.healthcare.connector.core.storage;

import static com.sdh.healthcare.connector.contract.table.SectionsTable.*;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
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
 * with its own derived key. Spike scope: section bytes are passed in memory. The pipeline version will
 * stream byte ranges from the reader.
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

    public WrittenSection add(SectionInput in) {
        byte[] plain = in.plaintext();
        int chunkCount = SectionCrypto.chunkCount(plain.length);
        Mac mac = ids.newMac();
        mac.update(plain);
        String sectionId = SectionIds.encode(mac.doFinal());
        SecretKey key = SectionCrypto.sectionKey(bdk, in.fileId(), in.seq());
        for (int i = 0; i < chunkCount; i++) {
            int from = i * SectionCrypto.CHUNK_SIZE;
            byte[] chunk = Arrays.copyOfRange(plain, from, from + SectionCrypto.expectedChunkLength(plain.length, i));
            GenericRecord r = GenericRecord.create(table.schema());
            r.setField(SECTION_ID, sectionId);
            r.setField(FILE_ID, in.fileId());
            r.setField(SEQ, in.seq());
            r.setField(CHUNK_INDEX, i);
            r.setField(CHUNK_COUNT, chunkCount);
            r.setField(SOURCE_OFFSET, in.sourceOffset());
            r.setField(SOURCE_LENGTH, (long) plain.length);
            r.setField(FORMAT, in.format());
            r.setField(PLAINTEXT_LENGTH, (long) plain.length);
            r.setField(CIPHERTEXT, ByteBuffer.wrap(
                    SectionCrypto.encryptChunk(key, in.fileId(), in.seq(), i, chunkCount, chunk)));
            r.setField(BDK_WRAPPED, ByteBuffer.wrap(wrapped.wrapped()));
            r.setField(KEK_REF, wrapped.keyRef());
            r.setField(BATCH_ID, batchId);
            r.setField(INGESTED_AT, ingestedAt);
            writer.write(r);
        }
        WrittenSection ws = new WrittenSection(sectionId, in.fileId(), in.seq(), in.sourceOffset(), plain.length,
                in.format(), plain.length, chunkCount);
        written.add(ws);
        return ws;
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
