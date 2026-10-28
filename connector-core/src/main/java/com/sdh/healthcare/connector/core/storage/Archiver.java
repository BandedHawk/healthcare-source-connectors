package com.sdh.healthcare.connector.core.storage;

import static com.sdh.healthcare.connector.core.storage.ArchiveTables.*;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.crypto.MacedJson;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Archives a whole original file into {@code source_files} or {@code failed_files} (design §4, §8.2, §11), resumably.
 *
 * <ul>
 *   <li>The file is stored in chunks (8 MiB) with the section envelope: one batch data key per commit, chunk key =
 *       HKDF(BDK, fileId, seq 0), AAD = fileId ‖ 0 ‖ chunkIndex ‖ chunkCount. The original name, if given, is
 *       encrypted the same way with seq −1.</li>
 *   <li>The file digest is a keyed hash chain over fixed blocks (64 MiB):
 *       {@code H_i = HMAC(idKey, "sdh-file-digest" ‖ 0 ‖ H_{i−1} ‖ block_i)}, {@code H_{−1}} = 32 zero bytes, and
 *       {@code fileDigest = base32(H_last)}. An empty file is one empty block. Unlike a single HMAC, the chain can
 *       be resumed from the last H.</li>
 *   <li>Progress is committed every {@code commitBytes} (whole blocks) with the archive checkpoint. The last commit
 *       adds the summary row and marks the checkpoint done.</li>
 * </ul>
 */
public final class Archiver {
    private static final Logger LOG = LoggerFactory.getLogger(Archiver.class);
    private static final SecureRandom RANDOM = new SecureRandom();
    public static final int DEFAULT_BLOCK = 64 << 20;

    /**
     * @param commitBytes commit after at least this many bytes (rounded up to whole blocks); default 512 MiB
     */
    public record Settings(String connectorId, String kekRef, long commitBytes, RetryPolicy retry, int chunkSize,
            int blockSize) {
        public Settings {
            if (chunkSize < 1 || blockSize < chunkSize || blockSize % chunkSize != 0 || commitBytes < 1) {
                throw new IllegalArgumentException("blockSize must be a positive multiple of chunkSize");
            }
        }

        public static Settings defaults(String connectorId, String kekRef) {
            return new Settings(connectorId, kekRef, 512L << 20, RetryPolicy.DEFAULT, SectionCrypto.CHUNK_SIZE,
                    DEFAULT_BLOCK);
        }

        public Settings withRetry(RetryPolicy r) {
            return new Settings(connectorId, kekRef, commitBytes, r, chunkSize, blockSize);
        }

        /** Smaller chunks and blocks, for tests. */
        public Settings withSizes(int chunk, int block, long commit) {
            return new Settings(connectorId, kekRef, commit, retry, chunk, block);
        }
    }

    public record Result(String digest, Instant finishedAt, long chunks) {}

    private final Table table;
    private final FencedCommitter committer;
    private final KeyProvider keys;
    private final SectionIds ids;
    private final Settings settings;
    private final Clock clock;
    private final MacedJson<ArchiveCheckpoint> codec;
    private final String checkpointKey;
    private LongConsumer afterCommit = b -> {};

    public Archiver(Table table, FencedCommitter committer, KeyProvider keys, SectionIds ids, Settings settings,
            Clock clock) {
        this.table = table;
        this.committer = committer;
        this.keys = keys;
        this.ids = ids;
        this.settings = settings;
        this.clock = clock;
        this.codec = ArchiveCheckpoint.codec(ids);
        this.checkpointKey = ArchiveCheckpoint.propertyKey(settings.connectorId());
    }

    /** Test hook: runs after each archive commit with the next block number. Throwing simulates a crash. */
    public void afterCommit(LongConsumer hook) {
        this.afterCommit = hook;
    }

    public Optional<ArchiveCheckpoint> checkpoint() {
        table.refresh();
        String v = table.properties().get(checkpointKey);
        if (v == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(codec.decode(v));
        } catch (MacedJson.Tampered e) {
            throw new CheckpointTamperedException();
        }
    }

    /** The finished archive of {@code fileId}, if any; needs no source. */
    public Optional<Result> finished(UUID fileId) {
        return checkpoint().filter(c -> c.fileId().equals(fileId) && c.done())
                .map(c -> new Result(c.digest(), c.finishedAt(), -1));
    }

    /**
     * Archives {@code src} as {@code fileId}, continuing from the stored checkpoint if it belongs to this file.
     *
     * @param summary fills the table-specific summary columns (completion or failure details); gets the finish time
     */
    public Result archive(UUID fileId, SeekableByteSource src, Optional<String> originalName,
            BiConsumer<GenericRecord, Instant> summary) throws Exception {
        Optional<ArchiveCheckpoint> stored = checkpoint().filter(c -> c.fileId().equals(fileId));
        if (stored.isPresent() && stored.get().done()) {
            return new Result(stored.get().digest(), stored.get().finishedAt(), -1);
        }
        long size = src.size();
        int chunkSize = settings.chunkSize();
        int perBlock = settings.blockSize() / chunkSize;
        long chunks = Math.max(1, (size + chunkSize - 1) / chunkSize);
        if (chunks > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("File has too many chunks");
        }
        long blocks = Math.max(1, (chunks + perBlock - 1) / perBlock);
        long block = stored.map(ArchiveCheckpoint::nextBlock).orElse(0L);
        byte[] chain = stored.map(c -> Base64.getDecoder().decode(c.chainHash())).orElse(new byte[32]);
        if (block > 0) {
            LOG.info("archive.resumed table={} fileId={} block={} of={}", table.name(), fileId, block, blocks);
        }

        BatchFile batch = null;
        try {
            for (; block < blocks; block++) {
                if (batch == null) {
                    batch = new BatchFile();
                }
                Mac mac = ids.newLabelledMac("sdh-file-digest");
                mac.update(chain);
                for (long c = block * perBlock; c < Math.min(chunks, (block + 1) * perBlock); c++) {
                    int len = (int) Math.min(chunkSize, Math.max(0, size - c * chunkSize));
                    byte[] plain = src.readFully(c * chunkSize, len);
                    mac.update(plain);
                    batch.chunk(fileId, (int) c, (int) chunks, size, plain);
                    Arrays.fill(plain, (byte) 0);
                }
                chain = mac.doFinal();
                boolean last = block == blocks - 1;
                if (last || batch.bytes >= settings.commitBytes()) {
                    Instant now = clock.instant();
                    String digest = last ? SectionIds.encode(chain) : null;
                    if (last) {
                        batch.summary(fileId, (int) chunks, size, originalName, digest, r -> summary.accept(r, now));
                    }
                    ArchiveCheckpoint next = new ArchiveCheckpoint(fileId, block + 1,
                            Base64.getEncoder().encodeToString(chain), last, digest, last ? now : null);
                    commit(batch.finish(), next);
                    batch = null;
                    afterCommit.accept(block + 1);
                    if (last) {
                        LOG.info("archive.done table={} fileId={} chunks={}", table.name(), fileId, chunks);
                        return new Result(digest, now, chunks);
                    }
                }
            }
            throw new IllegalStateException("unreachable");
        } finally {
            if (batch != null) {
                batch.abandon();
            }
        }
    }

    private void commit(DataFile file, ArchiveCheckpoint next) throws Exception {
        String encoded = codec.encode(next);
        settings.retry().call("archive.commit", RetryPolicy.TRANSIENT_STORAGE, attempt -> {
            if (attempt > 1) {
                Optional<ArchiveCheckpoint> cur = checkpoint();
                if (cur.isPresent() && cur.get().fileId().equals(next.fileId())
                        && cur.get().nextBlock() >= next.nextBlock()) {
                    return null;
                }
            }
            return committer.commit(table, tx -> {
                tx.newFastAppend().appendFile(file).commit();
                tx.updateProperties().set(checkpointKey, encoded).commit();
            });
        });
    }

    /** One Parquet data file for one commit, with its own batch data key. */
    private final class BatchFile {
        private final UUID batchId = UUID.randomUUID();
        private final byte[] bdk = new byte[32];
        private final WrappedKey wrapped;
        private final OffsetDateTime archivedAt = OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        private final DataWriter<Record> writer;
        private long bytes;

        BatchFile() {
            RANDOM.nextBytes(bdk);
            wrapped = keys.wrap(settings.kekRef(), bdk);
            GenericRecord probe = GenericRecord.create(table.schema());
            probe.setField(ARCHIVED_AT, archivedAt);
            PartitionKey partition = new PartitionKey(table.spec(), table.schema());
            partition.partition(new InternalRecordWrapper(table.schema().asStruct()).wrap(probe));
            String location = table.locationProvider().newDataLocation(table.spec(), partition,
                    FileFormat.PARQUET.addExtension(batchId.toString()));
            try {
                writer = Parquet.writeData(table.io().newOutputFile(location))
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

        private GenericRecord row(UUID fileId, int index, int count, long size, byte[] ciphertext) {
            GenericRecord r = GenericRecord.create(table.schema());
            r.setField(FILE_ID, fileId);
            r.setField(CHUNK_INDEX, index);
            r.setField(CHUNK_COUNT, count);
            r.setField(CHUNK_SIZE, settings.chunkSize());
            r.setField(FILE_SIZE, size);
            r.setField(CIPHERTEXT, ByteBuffer.wrap(ciphertext));
            r.setField(BDK_WRAPPED, ByteBuffer.wrap(wrapped.wrapped()));
            r.setField(KEK_REF, wrapped.keyRef());
            r.setField(BATCH_ID, batchId);
            r.setField(ARCHIVED_AT, archivedAt);
            return r;
        }

        void chunk(UUID fileId, int index, int count, long size, byte[] plain) {
            SecretKey key = SectionCrypto.sectionKey(bdk, fileId, 0);
            writer.write(row(fileId, index, count, size, SectionCrypto.encryptChunk(key, fileId, 0, index, count,
                    plain)));
            bytes += plain.length;
        }

        void summary(UUID fileId, int count, long size, Optional<String> name, String digest,
                java.util.function.Consumer<GenericRecord> fill) {
            GenericRecord r = row(fileId, SUMMARY_ROW, count, size, new byte[0]);
            name.ifPresent(n -> r.setField(ORIGINAL_NAME_ENC, ByteBuffer.wrap(SectionCrypto.encryptChunk(
                    SectionCrypto.sectionKey(bdk, fileId, -1), fileId, -1, 0, 1,
                    n.getBytes(StandardCharsets.UTF_8)))));
            r.setField(FILE_DIGEST, digest);
            r.setField(DIGEST_BLOCK_SIZE, settings.blockSize());
            fill.accept(r);
            writer.write(r);
        }

        DataFile finish() {
            try {
                writer.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                Arrays.fill(bdk, (byte) 0);
            }
            return writer.toDataFile();
        }

        void abandon() {
            Arrays.fill(bdk, (byte) 0);
            try {
                writer.close();
            } catch (IOException ignored) {
                // orphan file; removed by orphan cleanup
            }
        }
    }
}
