package com.sdh.healthcare.connector.core.storage;

import static com.sdh.healthcare.connector.core.storage.ArchiveTables.*;

import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
 * Restores an archived file from {@code source_files} or {@code failed_files} and checks its digest. Used by the
 * operator {@code restore} command (design §11) and tests. Reads a window of chunks at a time, so memory stays
 * small for any file size.
 */
public final class ArchiveReader {
    private static final int WINDOW = 8;

    public record Restored(UUID fileId, long fileSize, String digest, Optional<String> originalName, Record summary) {}

    private ArchiveReader() {}

    /**
     * Writes bytes {@code [fromOffset, fileSize)} of the archived file to {@code out}. When {@code fromOffset} is 0
     * the digest is recomputed and must match the stored one.
     */
    public static Restored restore(Table table, UUID fileId, KeyProvider keys, SectionIds ids, long fromOffset,
            OutputStream out) throws IOException {
        Record summary = rows(table, Expressions.and(Expressions.equal(FILE_ID, fileId),
                Expressions.equal(CHUNK_INDEX, SUMMARY_ROW))).stream().findFirst()
                .orElseThrow(() -> new IOException("No finished archive for this file"));
        long size = (Long) summary.getField(FILE_SIZE);
        int count = (Integer) summary.getField(CHUNK_COUNT);
        int chunkSize = (Integer) summary.getField(CHUNK_SIZE);
        int perBlock = (Integer) summary.getField(DIGEST_BLOCK_SIZE) / chunkSize;
        String stored = (String) summary.getField(FILE_DIGEST);

        byte[] chain = new byte[32];
        Mac mac = null;
        boolean verify = fromOffset == 0;
        Map<String, byte[]> bdks = new HashMap<>();
        for (int w = 0; w < count; w += WINDOW) {
            List<Record> rows = rows(table, Expressions.and(Expressions.equal(FILE_ID, fileId),
                    Expressions.greaterThanOrEqual(CHUNK_INDEX, w), Expressions.lessThan(CHUNK_INDEX, w + WINDOW)));
            rows.sort(Comparator.comparingInt(r -> (Integer) r.getField(CHUNK_INDEX)));
            int expectedRows = Math.min(WINDOW, count - w);
            if (rows.size() != expectedRows) {
                throw new ChunkIntegrityException("Archive chunks missing or duplicated");
            }
            for (int i = 0; i < rows.size(); i++) {
                Record r = rows.get(i);
                int index = w + i;
                if ((Integer) r.getField(CHUNK_INDEX) != index || (Integer) r.getField(CHUNK_COUNT) != count) {
                    throw new ChunkIntegrityException("Archive chunk out of place");
                }
                byte[] bdk = bdks.computeIfAbsent(r.getField(BATCH_ID).toString(), k -> keys.unwrap(
                        new WrappedKey((String) r.getField(KEK_REF), bytes(r.getField(BDK_WRAPPED)))));
                int len = (int) Math.min(chunkSize, Math.max(0, size - (long) index * chunkSize));
                byte[] plain = SectionCrypto.decryptChunk(SectionCrypto.sectionKey(bdk, fileId, 0), fileId, 0, index,
                        count, len, bytes(r.getField(CIPHERTEXT)));
                if (verify) {
                    if (index % perBlock == 0) {
                        mac = ids.newLabelledMac("sdh-file-digest");
                        mac.update(chain);
                    }
                    mac.update(plain);
                    if (index % perBlock == perBlock - 1 || index == count - 1) {
                        chain = mac.doFinal();
                    }
                }
                long start = (long) index * chunkSize;
                if (start + len > fromOffset) {
                    int skip = (int) Math.max(0, fromOffset - start);
                    out.write(plain, skip, len - skip);
                }
            }
        }
        if (verify && !SectionIds.encode(chain).equals(stored)) {
            throw new ChunkIntegrityException("Archive digest mismatch");
        }
        Optional<String> name = Optional.ofNullable(summary.getField(ORIGINAL_NAME_ENC)).map(enc -> {
            byte[] bdk = keys.unwrap(new WrappedKey((String) summary.getField(KEK_REF),
                    bytes(summary.getField(BDK_WRAPPED))));
            return new String(SectionCrypto.decryptSmall(SectionCrypto.sectionKey(bdk, fileId, -1), fileId, -1, 0, 1,
                    64 << 10, bytes(enc)), StandardCharsets.UTF_8);
        });
        return new Restored(fileId, size, stored, name, summary);
    }

    private static List<Record> rows(Table table, Expression filter) throws IOException {
        List<Record> out = new ArrayList<>();
        try (CloseableIterable<Record> it = IcebergGenerics.read(table).where(filter).build()) {
            it.forEach(r -> out.add(r.copy()));
        }
        return out;
    }

    static byte[] bytes(Object o) {
        ByteBuffer b = ((ByteBuffer) o).duplicate();
        byte[] a = new byte[b.remaining()];
        b.get(a);
        return a;
    }
}
