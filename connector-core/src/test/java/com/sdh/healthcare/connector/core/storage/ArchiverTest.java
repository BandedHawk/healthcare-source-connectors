package com.sdh.healthcare.connector.core.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiverTest {
    static final int CHUNK = 1024;
    static final int BLOCK = 4 * CHUNK;

    Table table;
    AesKeyWrapProvider keys;
    SectionIds ids = new SectionIds(new byte[32]);
    long epoch;

    @BeforeEach
    void setUp() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "memory://wh-" + UUID.randomUUID()));
        table = IngestTables.sourceFiles(catalog, "sdh_ingest");
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        keys = new AesKeyWrapProvider(Map.of("source-kek/1", kg.generateKey()));
    }

    Archiver archiver() {
        long e = ++epoch;
        FencedCommitter c = new FencedCommitter(new LeaseToken() {
            public long epoch() { return e; }
            public void checkHeld() {}
        });
        c.establish(table);
        return new Archiver(table, c, keys, ids, Archiver.Settings.defaults("file-connector", "source-kek/1")
                .withSizes(CHUNK, BLOCK, BLOCK) // commit every block
                .withRetry(new RetryPolicy(2, Duration.ofMillis(1), Duration.ofMillis(1))), Clock.systemUTC());
    }

    static byte[] data(int n) {
        byte[] b = new byte[n];
        new Random(n).nextBytes(b);
        return b;
    }

    ArchiveReader.Restored restore(UUID id, ByteArrayOutputStream out, long from) throws Exception {
        return ArchiveReader.restore(table, id, keys, ids, from, out);
    }

    @Test
    void archivesAndRestoresWithDigestAndName() throws Exception {
        byte[] d = data(3 * BLOCK + 100); // 4 blocks, 13 chunks
        UUID id = UUID.randomUUID();
        Archiver.Result r = archiver().archive(id, SeekableByteSource.of(d), Optional.of("jones.x12"), (rec, at) -> {});
        assertThat(r.chunks()).isEqualTo(13);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArchiveReader.Restored restored = restore(id, out, 0);
        assertThat(out.toByteArray()).isEqualTo(d);
        assertThat(restored.digest()).isEqualTo(r.digest());
        assertThat(restored.originalName()).contains("jones.x12");

        ByteArrayOutputStream tail = new ByteArrayOutputStream();
        restore(id, tail, 5000);
        assertThat(tail.toByteArray()).isEqualTo(java.util.Arrays.copyOfRange(d, 5000, d.length));
        // Archiving again is a no-op.
        assertThat(archiver().archive(id, SeekableByteSource.of(d), Optional.empty(), (rec, at) -> {}).digest())
                .isEqualTo(r.digest());
    }

    @Test
    void resumesAfterACrashWithTheSameDigestAndNoDuplicateChunks() throws Exception {
        byte[] d = data(3 * BLOCK + 100);
        UUID reference = UUID.randomUUID();
        String expected = archiver().archive(reference, SeekableByteSource.of(d), Optional.empty(), (r, at) -> {})
                .digest();

        UUID id = UUID.randomUUID();
        Archiver first = archiver();
        first.afterCommit(next -> {
            if (next == 2) {
                throw new Crash();
            }
        });
        assertThatThrownBy(() -> first.archive(id, SeekableByteSource.of(d), Optional.empty(), (r, at) -> {}))
                .isInstanceOf(Crash.class);
        assertThat(first.checkpoint().orElseThrow().nextBlock()).isEqualTo(2);

        Archiver.Result r = archiver().archive(id, SeekableByteSource.of(d), Optional.empty(), (rec, at) -> {});
        assertThat(r.digest()).isEqualTo(expected); // digest depends on content only
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        restore(id, out, 0);
        assertThat(out.toByteArray()).isEqualTo(d);
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(ArchiveTables.FILE_ID, id)).build()) {
            assertThat(rows).hasSize(13 + 1);
        }
    }

    @Test
    void emptyFileAndDifferentContentDigests() throws Exception {
        UUID a = UUID.randomUUID();
        String empty = archiver().archive(a, SeekableByteSource.of(new byte[0]), Optional.empty(), (r, at) -> {})
                .digest();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        restore(a, out, 0);
        assertThat(out.size()).isZero();
        byte[] d = data(100);
        byte[] d2 = d.clone();
        d2[50] ^= 1;
        String one = archiver().archive(UUID.randomUUID(), SeekableByteSource.of(d), Optional.empty(), (r, at) -> {})
                .digest();
        String two = archiver().archive(UUID.randomUUID(), SeekableByteSource.of(d2), Optional.empty(), (r, at) -> {})
                .digest();
        assertThat(java.util.Set.of(empty, one, two)).hasSize(3);
    }

    static final class Crash extends Error {}
}
