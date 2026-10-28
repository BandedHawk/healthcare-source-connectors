package com.sdh.healthcare.connector.reader;

import static com.sdh.healthcare.connector.contract.table.SectionsTable.*;

import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
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

/** Streams one section: scans its chunk rows, puts them in order and decrypts them one at a time. */
final class SectionInputStream extends InputStream {
    private final SectionStored msg;
    private final BatchKeyCache keys;
    private final CloseableIterable<Record> rows;
    private final Iterator<Record> it;
    private final Map<Integer, byte[]> ahead = new HashMap<>();

    private SecretKey sectionKey;
    private UUID batchId;
    private int nextIndex;
    private long delivered;
    private byte[] current = new byte[0];
    private int pos;
    private boolean sawRows;

    private SectionInputStream(Table table, SectionStored msg, BatchKeyCache keys) {
        this.msg = msg;
        this.keys = keys;
        long dayStart = TimeUnit.SECONDS.toMicros(
                msg.storage().ingestedDate().atStartOfDay(ZoneOffset.UTC).toEpochSecond());
        Expression filter = Expressions.and(
                Expressions.equal(FILE_ID, msg.fileId()),
                Expressions.equal(SEQ, msg.seq()),
                Expressions.greaterThanOrEqual(INGESTED_AT, dayStart),
                Expressions.lessThan(INGESTED_AT, dayStart + TimeUnit.DAYS.toMicros(1)));
        this.rows = IcebergGenerics.read(table)
                .where(filter)
                .select(SECTION_ID, CHUNK_INDEX, CHUNK_COUNT, PLAINTEXT_LENGTH, CIPHERTEXT, BDK_WRAPPED, KEK_REF,
                        BATCH_ID)
                .build();
        this.it = rows.iterator();
    }

    static SectionInputStream open(Table table, SectionStored msg, BatchKeyCache keys) {
        return new SectionInputStream(table, msg, keys);
    }

    @Override
    public int read() throws IOException {
        if (!fill()) {
            return -1;
        }
        return current[pos++] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        if (!fill()) {
            return -1;
        }
        int n = Math.min(len, current.length - pos);
        System.arraycopy(current, pos, b, off, n);
        pos += n;
        return n;
    }

    private boolean fill() {
        while (pos >= current.length) {
            if (nextIndex >= msg.chunkCount()) {
                if (delivered != msg.plaintextLength()) {
                    throw new ChunkIntegrityException("Section length mismatch");
                }
                return false;
            }
            byte[] stored = ahead.remove(nextIndex);
            while (stored == null) {
                if (!it.hasNext()) {
                    if (!sawRows) {
                        throw new SectionNotFoundException();
                    }
                    throw new ChunkIntegrityException("Section incomplete");
                }
                Record r = it.next();
                sawRows = true;
                int idx = check(r);
                byte[] ct = bytes((ByteBuffer) r.getField(CIPHERTEXT));
                if (idx == nextIndex) {
                    stored = ct;
                } else if (idx < nextIndex || ahead.putIfAbsent(idx, ct) != null) {
                    throw new ChunkIntegrityException("Duplicate chunk");
                }
            }
            int expected = SectionCrypto.expectedChunkLength(msg.plaintextLength(), nextIndex);
            current = SectionCrypto.decryptChunk(sectionKey, msg.fileId(), msg.seq(), nextIndex, msg.chunkCount(),
                    expected, stored);
            pos = 0;
            delivered += current.length;
            nextIndex++;
        }
        return true;
    }

    /** Checks a row against the message and sets up the section key on the first row. */
    private int check(Record r) {
        if (!msg.sectionId().equals(r.getField(SECTION_ID))
                || msg.chunkCount() != (Integer) r.getField(CHUNK_COUNT)
                || msg.plaintextLength() != (Long) r.getField(PLAINTEXT_LENGTH)) {
            throw new ChunkIntegrityException("Row does not match message");
        }
        UUID rowBatch = (UUID) r.getField(BATCH_ID);
        if (sectionKey == null) {
            batchId = rowBatch;
            byte[] bdk = keys.get(rowBatch, new WrappedKey((String) r.getField(KEK_REF),
                    bytes((ByteBuffer) r.getField(BDK_WRAPPED))));
            try {
                sectionKey = SectionCrypto.sectionKey(bdk, msg.fileId(), msg.seq());
            } finally {
                Arrays.fill(bdk, (byte) 0);
            }
        } else if (!batchId.equals(rowBatch)) {
            throw new ChunkIntegrityException("Chunks from different batches");
        }
        int idx = (Integer) r.getField(CHUNK_INDEX);
        if (idx < 0 || idx >= msg.chunkCount()) {
            throw new ChunkIntegrityException("Chunk index out of range");
        }
        return idx;
    }

    private static byte[] bytes(ByteBuffer b) {
        byte[] a = new byte[b.remaining()];
        b.duplicate().get(a);
        return a;
    }

    @Override
    public void close() throws IOException {
        Arrays.fill(current, (byte) 0);
        ahead.clear();
        rows.close();
    }
}
