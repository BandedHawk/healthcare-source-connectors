package com.sdh.healthcare.connector.core.pipeline;

import static com.sdh.healthcare.connector.contract.table.SectionsTable.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class IngestPipelineTest {
    static final String KEK = "sections-kek/1";
    static final int RECORDS = 250;
    static final int BATCH = 100;

    InMemoryCatalog catalog;
    TableIdentifier tableId = TableIdentifier.of("sdh_ingest", DEFAULT_NAME);
    AesKeyWrapProvider keys;
    KeyPair signer;
    KeyPair recipient;
    MessageSealer sealer;
    MessageOpener opener;
    long epochs;
    final List<IngestPipeline> open = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "memory://wh-" + UUID.randomUUID()));
        IngestTables.sections(catalog, "sdh_ingest");
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        keys = new AesKeyWrapProvider(Map.of(KEK, kg.generateKey()));
        signer = ec();
        recipient = ec();
        sealer = new MessageSealer(new ECDSASigner((ECPrivateKey) signer.getPrivate()), "connector",
                recipient.getPublic(), "downstream");
        opener = new MessageOpener(Map.of("downstream", recipient.getPrivate()),
                Map.of("connector", (ECPublicKey) signer.getPublic()));
    }

    @AfterEach
    void tearDown() {
        open.forEach(IngestPipeline::close);
    }

    PipelineSettings settings() {
        return PipelineSettings.defaults("file-connector", "csv", "prod", KEK)
                .withCommit(BATCH, 1L << 30, Duration.ofHours(1))
                .withWorkers(4)
                .withRetry(new RetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(5)));
    }

    /** A fresh "process": its own table handle, a higher lease epoch, and the ID key loaded from the table. */
    IngestPipeline process(PipelineSettings s, MessagePublisher publisher, PipelineHooks hooks) {
        long epoch = ++epochs;
        Table table = catalog.loadTable(tableId);
        FencedCommitter committer = new FencedCommitter(new LeaseToken() {
            public long epoch() { return epoch; }
            public void checkHeld() {}
        });
        committer.establish(table);
        byte[] idKey = IdKeys.loadOrCreate(table, committer, keys, KEK);
        IngestPipeline p = new IngestPipeline(s, table, committer, keys, new SectionIds(idKey),
                SplitterRegistry.load(), sealer, publisher, Clock.systemUTC());
        p.setHooks(hooks);
        open.add(p);
        return p;
    }

    static List<String> records() {
        List<String> r = new ArrayList<>();
        for (int i = 0; i < RECORDS; i++) {
            r.add(i + ",name-" + i + ",\"note " + i + "\nsecond line, with comma\"\n");
        }
        return r;
    }

    static SeekableByteSource csv(List<String> records) {
        return SeekableByteSource.of(String.join("", records).getBytes(UTF_8));
    }

    // ---- happy path ----

    @Test
    void splitsEncryptsCommitsAndPublishesInBatches() throws Exception {
        RecordingPublisher pub = new RecordingPublisher();
        IngestPipeline p = process(settings(), pub, PipelineHooks.NONE);
        IngestPipeline.SplitResult result = p.split(csv(records()));

        assertThat(result.sectionCount()).isEqualTo(RECORDS);
        Checkpoint cp = p.checkpoint().orElseThrow();
        assertThat(cp.state()).isEqualTo(Checkpoint.State.ARCHIVING);
        assertThat(cp.publishedSeq()).isEqualTo(RECORDS - 1);

        List<ConnectorMessage> msgs = decode(pub.sent);
        assertThat(msgs.getFirst()).isInstanceOf(FileStarted.class);
        List<SectionStored> sections = sections(msgs);
        assertThat(sections).extracting(SectionStored::seq)
                .containsExactlyElementsOf(java.util.stream.LongStream.range(0, RECORDS).boxed().toList());
        assertThat(sections).allSatisfy(s -> {
            assertThat(s.fileId()).isEqualTo(result.fileId());
            assertThat(s.storage().catalog()).isEqualTo("prod");
            assertThat(s.storage().namespace()).isEqualTo("sdh_ingest");
            assertThat(s.storage().table()).isEqualTo(DEFAULT_NAME);
        });
        // Three batches → three data snapshots, each referenced by its messages.
        assertThat(sections.stream().map(s -> s.storage().snapshotId()).distinct()).hasSize(3);

        Table table = catalog.loadTable(tableId);
        SectionIds ids = new SectionIds(IdKeys.loadOrCreate(table, null, keys, KEK));
        List<String> expected = records();
        for (SectionStored s : List.of(sections.get(0), sections.get(137), sections.get(249))) {
            byte[] plain = expected.get((int) s.seq()).getBytes(UTF_8);
            assertThat(s.sectionId()).isEqualTo(ids.of(plain));
            assertThat(decrypt(table, s.fileId(), s.seq())).isEqualTo(plain);
        }
        assertThat(dataRows(table, result.fileId())).hasSize(RECORDS);
    }

    @Test
    void multiChunkSectionIsStreamedInTwoPasses() throws Exception {
        String big = "1,\"" + "x".repeat(SectionCrypto.CHUNK_SIZE + 500) + "\"\n";
        String small = "2,y\n";
        RecordingPublisher pub = new RecordingPublisher();
        IngestPipeline p = process(settings(), pub, PipelineHooks.NONE);
        UUID fileId = p.split(csv(List.of(small, big, small))).fileId();

        List<SectionStored> s = sections(decode(pub.sent));
        assertThat(s).extracting(SectionStored::chunkCount).containsExactly(1, 2, 1);
        Table table = catalog.loadTable(tableId);
        assertThat(decrypt(table, fileId, 1)).isEqualTo(big.getBytes(UTF_8));
        assertThat(decrypt(table, fileId, 2)).isEqualTo(small.getBytes(UTF_8));
    }

    @Test
    void zeroIntervalCommitsEverySection() throws Exception {
        RecordingPublisher pub = new RecordingPublisher();
        IngestPipeline p = process(settings().withCommit(BATCH, 1L << 30, Duration.ZERO), pub, PipelineHooks.NONE);
        p.split(csv(records().subList(0, 5)));
        assertThat(sections(decode(pub.sent)).stream().map(s -> s.storage().snapshotId()).distinct()).hasSize(5);
    }

    @Test
    void emptyFileGoesStraightToArchiving() throws Exception {
        RecordingPublisher pub = new RecordingPublisher();
        IngestPipeline p = process(settings(), pub, PipelineHooks.NONE);
        IngestPipeline.SplitResult empty = p.split(SeekableByteSource.of(new byte[0]));
        assertThat(empty.sectionCount()).isZero();
        assertThat(empty.checkpoint().state()).isEqualTo(Checkpoint.State.ARCHIVING);
        assertThat(decode(pub.sent)).singleElement().isInstanceOf(FileStarted.class);
        // Splitting the same file again is a no-op: it is already waiting for archiving.
        assertThat(p.split(SeekableByteSource.of(new byte[0])).fileId()).isEqualTo(empty.fileId());
    }

    @Test
    void transientBrokerFailuresAreRetried() throws Exception {
        RecordingPublisher pub = new RecordingPublisher();
        pub.fail = n -> n % 3 == 1; // every third send fails once; its retry is a later call
        IngestPipeline p = process(settings(), pub, PipelineHooks.NONE);
        p.split(csv(records()));
        assertThat(sections(decode(pub.sent)).stream().map(SectionStored::seq).distinct()).hasSize(RECORDS);
    }

    // ---- crash and resume (design §15) ----

    enum Crash {
        AFTER_WRITE_FIRST_BATCH, AFTER_WRITE_SECOND_BATCH, AFTER_COMMIT_FIRST_BATCH, AFTER_COMMIT_SECOND_BATCH,
        BEFORE_END_OF_FILE, BROKER_DOWN_MID_PUBLISH
    }

    @ParameterizedTest
    @EnumSource(Crash.class)
    void resumesAfterACrashWithNoGapsAndAtMostOneBatchOfDuplicates(Crash crash) throws Exception {
        RecordingPublisher pub = new RecordingPublisher();
        PipelineHooks hooks = new PipelineHooks() {
            @Override
            public void batchWritten(long lastSeq) {
                if ((crash == Crash.AFTER_WRITE_FIRST_BATCH && lastSeq == BATCH - 1)
                        || (crash == Crash.AFTER_WRITE_SECOND_BATCH && lastSeq == 2 * BATCH - 1)) {
                    throw new SimulatedCrash();
                }
            }

            @Override
            public void batchCommitted(long lastSeq) {
                if ((crash == Crash.AFTER_COMMIT_FIRST_BATCH && lastSeq == BATCH - 1)
                        || (crash == Crash.AFTER_COMMIT_SECOND_BATCH && lastSeq == 2 * BATCH - 1)) {
                    throw new SimulatedCrash();
                }
            }

            @Override
            public void beforeEndOfFile() {
                if (crash == Crash.BEFORE_END_OF_FILE) {
                    throw new SimulatedCrash();
                }
            }
        };
        if (crash == Crash.BROKER_DOWN_MID_PUBLISH) {
            pub.fail = n -> n >= 150;
        }
        List<String> recs = records();
        IngestPipeline first = process(settings(), pub, hooks);
        assertThatThrownBy(() -> first.split(csv(recs)))
                .isInstanceOfAny(SimulatedCrash.class, PublishFailedException.class);
        first.close();
        UUID firstFileId = first.checkpoint().orElseThrow().fileId();
        assertThat(first.checkpoint().orElseThrow().state()).isEqualTo(Checkpoint.State.SPLITTING);

        pub.fail = n -> false;
        IngestPipeline second = process(settings(), pub, PipelineHooks.NONE);
        IngestPipeline.SplitResult result = second.split(csv(recs));
        assertThat(result.fileId()).isEqualTo(firstFileId);
        assertThat(result.sectionCount()).isEqualTo(RECORDS);

        List<SectionStored> sent = sections(decode(pub.sent));
        Map<Long, List<SectionStored>> bySeq = sent.stream().collect(Collectors.groupingBy(SectionStored::seq));
        assertThat(bySeq.keySet()).hasSize(RECORDS).allMatch(seq -> seq >= 0 && seq < RECORDS);
        bySeq.values().forEach(copies -> assertThat(copies.stream().map(SectionStored::sectionId).distinct())
                .hasSize(1));
        assertThat(sent.size() - RECORDS).as("duplicates").isLessThanOrEqualTo(BATCH);

        Table table = catalog.loadTable(tableId);
        List<Record> rows = dataRows(table, firstFileId);
        assertThat(rows).hasSize(RECORDS);
        assertThat(rows.stream().map(r -> r.getField(SEQ)).distinct()).hasSize(RECORDS);
        SectionIds ids = new SectionIds(IdKeys.loadOrCreate(table, null, keys, KEK));
        assertThat(rows).allSatisfy(r -> assertThat(r.getField(SECTION_ID))
                .isEqualTo(ids.of(recs.get(((Long) r.getField(SEQ)).intValue()).getBytes(UTF_8))));
    }

    // ---- guards ----

    @Test
    void anotherFileIsRefusedUntilTheInProgressOneIsAbandoned() throws Exception {
        RecordingPublisher pub = new RecordingPublisher();
        IngestPipeline first = process(settings(), pub, new PipelineHooks() {
            @Override
            public void batchCommitted(long lastSeq) {
                throw new SimulatedCrash();
            }
        });
        assertThatThrownBy(() -> first.split(csv(records()))).isInstanceOf(SimulatedCrash.class);

        IngestPipeline second = process(settings(), pub, PipelineHooks.NONE);
        SeekableByteSource other = csv(List.of("9,other\n"));
        assertThatThrownBy(() -> second.split(other)).isInstanceOf(IllegalStateException.class);
        second.abandonInProgress();
        assertThat(second.checkpoint().orElseThrow().state()).isEqualTo(Checkpoint.State.ABANDONED);
        assertThat(second.split(other).sectionCount()).isEqualTo(1);
        // A file still being archived is protected too.
        assertThatThrownBy(() -> second.split(csv(records()))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void staleInstanceIsFencedOutBeforeWritingAnything() throws Exception {
        RecordingPublisher pub = new RecordingPublisher();
        IngestPipeline stale = process(settings(), pub, PipelineHooks.NONE);
        process(settings(), pub, PipelineHooks.NONE); // successor establishes a higher epoch
        assertThatThrownBy(() -> stale.split(csv(records()))).isInstanceOf(LeaseLostException.class);
        assertThat(pub.sent).isEmpty();
    }

    @Test
    void tamperedCheckpointStopsTheService() throws Exception {
        RecordingPublisher pub = new RecordingPublisher();
        IngestPipeline p = process(settings(), pub, PipelineHooks.NONE);
        p.split(csv(records().subList(0, 3)));
        Table table = catalog.loadTable(tableId);
        String key = Checkpoint.propertyKey("file-connector");
        String forged = table.properties().get(key).replace("\"state\":\"ARCHIVING\"", "\"state\":\"SPLITTING\"");
        table.updateProperties().set(key, forged).commit();
        assertThatThrownBy(p::checkpoint).isInstanceOf(CheckpointTamperedException.class);
    }

    // ---- helpers ----

    static final class SimulatedCrash extends RuntimeException {}

    static final class RecordingPublisher implements MessagePublisher {
        final List<OutboundMessage> sent = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger calls = new AtomicInteger();
        volatile IntPredicate fail = n -> false;
        /** Calls whose send takes a while; a send interrupted midway never reaches the broker. */
        volatile IntPredicate slow = n -> false;

        @Override
        public CompletableFuture<Void> publish(OutboundMessage m) {
            int n = calls.getAndIncrement();
            if (fail.test(n)) {
                return CompletableFuture.failedFuture(new IOException("broker unavailable"));
            }
            if (slow.test(n)) {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return CompletableFuture.failedFuture(e);
                }
            }
            sent.add(m);
            return CompletableFuture.completedFuture(null);
        }
    }

    List<ConnectorMessage> decode(List<OutboundMessage> sent) throws Exception {
        List<ConnectorMessage> out = new ArrayList<>();
        synchronized (sent) {
            for (OutboundMessage m : sent) {
                ConnectorMessage cm = opener.open(m.body());
                assertThat(cm.messageId()).isEqualTo(m.messageId());
                out.add(cm);
            }
        }
        return out;
    }

    static List<SectionStored> sections(List<ConnectorMessage> msgs) {
        return msgs.stream().filter(SectionStored.class::isInstance).map(SectionStored.class::cast).toList();
    }

    static List<Record> dataRows(Table table, UUID fileId) throws IOException {
        List<Record> out = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(FILE_ID, fileId)).build()) {
            rows.forEach(r -> out.add(r.copy()));
        }
        return out;
    }

    byte[] decrypt(Table table, UUID fileId, long seq) throws IOException {
        Map<Integer, Record> chunks = new HashMap<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(Expressions.equal(FILE_ID, fileId), Expressions.equal(SEQ, seq))).build()) {
            rows.forEach(r -> chunks.put((Integer) r.getField(CHUNK_INDEX), r.copy()));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < chunks.size(); i++) {
            Record r = chunks.get(i);
            byte[] bdk = keys.unwrap(new WrappedKey((String) r.getField(KEK_REF), bytes(r.getField(BDK_WRAPPED))));
            int count = (Integer) r.getField(CHUNK_COUNT);
            long len = (Long) r.getField(PLAINTEXT_LENGTH);
            out.writeBytes(SectionCrypto.decryptChunk(SectionCrypto.sectionKey(bdk, fileId, seq), fileId, seq, i,
                    count, SectionCrypto.expectedChunkLength(len, i), bytes(r.getField(CIPHERTEXT))));
        }
        return out.toByteArray();
    }

    static byte[] bytes(Object o) {
        ByteBuffer b = ((ByteBuffer) o).duplicate();
        byte[] a = new byte[b.remaining()];
        b.get(a);
        return a;
    }

    static KeyPair ec() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp256r1"));
        return g.generateKeyPair();
    }
}
