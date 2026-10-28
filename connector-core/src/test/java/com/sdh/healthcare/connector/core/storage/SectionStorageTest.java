package com.sdh.healthcare.connector.core.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.table.SectionsTable;
import com.sdh.healthcare.connector.core.TestCatalogs;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;

/** Spike §16.1: Iceberg's Parquet writer and reader on Java 25 with the trimmed hadoop-common. */
class SectionStorageTest {

    @Test
    void writesEncryptedSectionsToParquetAndReadsThemBack() throws Exception {
        Table table = IngestTables.sections(TestCatalogs.jdbc().newClient(), "ns_" + UUID.randomUUID().toString().replace("-", ""));
        FencedCommitter committer = new FencedCommitter(new LeaseToken() {
            public long epoch() { return 1; }
            public void checkHeld() {}
        });
        committer.establish(table);

        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        AesKeyWrapProvider keys = new AesKeyWrapProvider(Map.of("kek-1", kg.generateKey()));
        SectionIds ids = new SectionIds(new byte[32]);
        UUID fileId = UUID.randomUUID();
        byte[] big = new byte[SectionCrypto.CHUNK_SIZE + 1000]; // two chunks
        new java.util.Random(1).nextBytes(big);
        byte[] small = "<ClinicalDocument xmlns=\"urn:hl7-org:v3\"/>".getBytes(StandardCharsets.UTF_8);

        DataFile df;
        try (SectionBatchWriter w = new SectionBatchWriter(table, keys, "kek-1", ids, Instant.now())) {
            w.add(new SectionBatchWriter.SectionInput(fileId, 0, 0, "ccda", small));
            w.add(new SectionBatchWriter.SectionInput(fileId, 1, small.length, "ccda", big));
            df = w.finish();
        }
        committer.commit(table, tx -> tx.newFastAppend().appendFile(df).commit());
        assertThat(df.recordCount()).isEqualTo(3);

        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(Expressions.equal(SectionsTable.FILE_ID, fileId),
                        Expressions.equal(SectionsTable.SEQ, 1L)))
                .build()) {
            ByteBuffer out = ByteBuffer.allocate(big.length);
            var sorted = new java.util.ArrayList<Record>();
            rows.forEach(r -> sorted.add(r.copy()));
            sorted.sort(java.util.Comparator.comparingInt(r -> (Integer) r.getField(SectionsTable.CHUNK_INDEX)));
            assertThat(sorted).hasSize(2);
            for (Record r : sorted) {
                ByteBuffer wrapped = (ByteBuffer) r.getField(SectionsTable.BDK_WRAPPED);
                byte[] bdk = keys.unwrap(new WrappedKey((String) r.getField(SectionsTable.KEK_REF), bytes(wrapped)));
                int idx = (Integer) r.getField(SectionsTable.CHUNK_INDEX);
                out.put(SectionCrypto.decryptChunk(SectionCrypto.sectionKey(bdk, fileId, 1), fileId, 1, idx, 2,
                        SectionCrypto.expectedChunkLength(big.length, idx),
                        bytes((ByteBuffer) r.getField(SectionsTable.CIPHERTEXT))));
            }
            assertThat(out.array()).isEqualTo(big);
        }
    }

    static byte[] bytes(ByteBuffer b) {
        byte[] a = new byte[b.remaining()];
        b.duplicate().get(a);
        return a;
    }
}
