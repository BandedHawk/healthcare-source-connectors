/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;

/**
 * Spike §16.1: Iceberg's Parquet writer and reader on Java 25 with the
 * trimmed hadoop-common.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SectionStorageTest
{

    /**
     * Sections are written encrypted, in chunks, and decrypt back.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void writesEncryptedSectionsToParquetAndReadsThemBack() throws Exception
    {
        Table table = IngestTables.sections(TestCatalogs.jdbc().newClient(),
                "ns_" + UUID.randomUUID().toString().replace("-", ""));
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return 1;
            }

            @Override
            public void checkHeld()
            {
            }
        });
        committer.establish(table);

        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        AesKeyWrapProvider keys = new AesKeyWrapProvider(
                Map.of("kek-1", generator.generateKey()));
        SectionIds ids = new SectionIds(new byte[32]);
        UUID fileId = UUID.randomUUID();
        byte[] big = new byte[SectionCrypto.CHUNK_SIZE + 1000]; // two chunks
        new Random(1).nextBytes(big);
        byte[] small = "<ClinicalDocument xmlns=\"urn:hl7-org:v3\"/>"
                .getBytes(StandardCharsets.UTF_8);

        DataFile dataFile;
        try (SectionBatchWriter writer = new SectionBatchWriter(table, keys,
                "kek-1", ids, Instant.now()))
        {
            writer.add(new SectionBatchWriter.SectionInput(fileId, 0, 0,
                    "ccda", small));
            writer.add(new SectionBatchWriter.SectionInput(fileId, 1,
                    small.length, "ccda", big));
            dataFile = writer.finish();
        }
        committer.commit(table, transaction -> transaction.newFastAppend()
                .appendFile(dataFile).commit());
        assertThat(dataFile.recordCount()).isEqualTo(3);

        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(SectionsTable.FILE_ID, fileId),
                        Expressions.equal(SectionsTable.SEQ, 1L)))
                .build())
        {
            ByteBuffer out = ByteBuffer.allocate(big.length);
            List<Record> sorted = new ArrayList<>();
            rows.forEach(row -> sorted.add(row.copy()));
            sorted.sort(Comparator.comparingInt(row ->
                    (Integer) row.getField(SectionsTable.CHUNK_INDEX)));
            assertThat(sorted).hasSize(2);
            for (Record row : sorted)
            {
                ByteBuffer wrapped =
                        (ByteBuffer) row.getField(SectionsTable.BDK_WRAPPED);
                byte[] bdk = keys.unwrap(new WrappedKey(
                        (String) row.getField(SectionsTable.KEK_REF),
                        bytes(wrapped)));
                int chunkIndex =
                        (Integer) row.getField(SectionsTable.CHUNK_INDEX);
                out.put(SectionCrypto.decryptChunk(
                        SectionCrypto.sectionKey(bdk, fileId, 1), fileId, 1,
                        chunkIndex, 2,
                        SectionCrypto.expectedChunkLength(big.length,
                                chunkIndex),
                        bytes((ByteBuffer) row.getField(
                                SectionsTable.CIPHERTEXT))));
            }
            assertThat(out.array()).isEqualTo(big);
        }
    }

    /**
     * A buffer's remaining bytes, without moving it.
     *
     * @param buffer the buffer
     * @return a copy of its bytes
     */
    static byte[] bytes(ByteBuffer buffer)
    {
        byte[] copy = new byte[buffer.remaining()];
        buffer.duplicate().get(copy);
        return copy;
    }
}
