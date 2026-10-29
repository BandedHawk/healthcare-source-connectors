package com.sdh.healthcare.connector.core.pipeline;

import static com.sdh.healthcare.connector.core.storage.ArchiveTables.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecoveryEnvelope;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.pipeline.FileProcessor.Outcome;
import com.sdh.healthcare.connector.core.pipeline.FileProcessor.Result;
import com.sdh.healthcare.connector.core.pipeline.IngestPipelineTest.RecordingPublisher;
import com.sdh.healthcare.connector.core.pipeline.OutcomeRecorder.ArchiveLocation;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitLimits;
import com.sdh.healthcare.connector.core.storage.ArchiveReader;
import com.sdh.healthcare.connector.core.storage.Archiver;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyPair;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FileProcessorTest {
    static final String SECTIONS_KEK = "sections-kek/1";
    static final String SOURCE_KEK = "source-kek/1";
    static final String FAILED_KEK = "failed-kek/1";

    InMemoryCatalog catalog;
    AesKeyWrapProvider keys;
    KeyPair signer;
    KeyPair recipient;
    KeyPair recovery;
    MessageOpener opener;
    RecordingPublisher pub = new RecordingPublisher();
    MemRecorder recorder = new MemRecorder();
    long epochs;
    final List<IngestPipeline> open = new ArrayList<>();

    // per-process knobs
    KeyProvider failedKeys;
    PipelineSettings settings;
    PipelineHooks hooks = PipelineHooks.NONE;
    java.util.function.LongConsumer sourceArchiveHook = b -> {};

    @BeforeEach
    void setUp() throws Exception {
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "memory://wh-" + UUID.randomUUID()));
        IngestTables.sections(catalog, "sdh_ingest");
        IngestTables.sourceFiles(catalog, "sdh_ingest");
        IngestTables.failedFiles(catalog, "sdh_ingest");
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        keys = new AesKeyWrapProvider(Map.of(SECTIONS_KEK, kg.generateKey(), SOURCE_KEK, kg.generateKey(),
                FAILED_KEK, kg.generateKey()));
        failedKeys = keys;
        signer = IngestPipelineTest.ec();
        recipient = IngestPipelineTest.ec();
        recovery = IngestPipelineTest.ec();
        opener = new MessageOpener(Map.of("downstream", recipient.getPrivate()),
                Map.of("connector", (ECPublicKey) signer.getPublic()));
        settings = PipelineSettings.defaults("file-connector", "csv", "prod", SECTIONS_KEK)
                .withCommit(3, 1L << 30, Duration.ofHours(1))
                .withWorkers(2)
                .withRetry(new RetryPolicy(2, Duration.ofMillis(1), Duration.ofMillis(2)));
    }

    @AfterEach
    void tearDown() {
        open.forEach(IngestPipeline::close);
        Telemetry.install(io.opentelemetry.api.OpenTelemetry.noop());
    }

    Table table(String name) {
        return catalog.loadTable(TableIdentifier.of("sdh_ingest", name));
    }

    /** A fresh process: new lease epoch on all three tables, pipeline, two archivers, processor. */
    FileProcessor process() throws Exception {
        long epoch = ++epochs;
        FencedCommitter committer = new FencedCommitter(new LeaseToken() {
            public long epoch() { return epoch; }
            public void checkHeld() {}
        });
        Table sections = table("sections");
        Table source = table(SOURCE_FILES);
        Table failed = table(FAILED_FILES);
        for (Table t : List.of(sections, source, failed)) {
            committer.establish(t);
        }
        SectionIds ids = new SectionIds(IdKeys.loadOrCreate(sections, committer, keys, SECTIONS_KEK));
        RetryPolicy fast = new RetryPolicy(2, Duration.ofMillis(1), Duration.ofMillis(2));
        Archiver sourceArchiver = new Archiver(source, committer, keys, ids,
                Archiver.Settings.defaults("file-connector", SOURCE_KEK).withSizes(64, 256, 256).withRetry(fast),
                Clock.systemUTC());
        sourceArchiver.afterCommit(sourceArchiveHook);
        Archiver failedArchiver = new Archiver(failed, committer, failedKeys, ids,
                Archiver.Settings.defaults("file-connector", FAILED_KEK).withSizes(64, 256, 256).withRetry(fast),
                Clock.systemUTC());
        IngestPipeline p = new IngestPipeline(settings, sections, committer, keys, ids, SplitterRegistry.load(),
                new MessageSealer(new ECDSASigner((ECPrivateKey) signer.getPrivate()), "connector",
                        recipient.getPublic(), "downstream"), pub, Clock.systemUTC());
        p.setHooks(hooks);
        open.add(p);
        return new FileProcessor(p, sourceArchiver, failedArchiver, recorder, (ECPublicKey) recovery.getPublic());
    }

    static String csv(int n) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < n; i++) {
            b.append(i).append(",patient-").append(i).append(",\"note\n").append(i).append("\"\n");
        }
        return b.toString();
    }

    List<ConnectorMessage> messages() throws Exception {
        List<ConnectorMessage> out = new ArrayList<>();
        synchronized (pub.sent) {
            for (OutboundMessage m : pub.sent) {
                out.add(opener.open(m.body()));
            }
        }
        return out;
    }

    byte[] restore(String table, UUID id) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Table t = table(table);
        ArchiveReader.restore(t, id, table.equals(SOURCE_FILES) ? keys : failedKeys,
                new SectionIds(IdKeys.loadOrCreate(table("sections"), null, keys, SECTIONS_KEK)), 0, out);
        return out.toByteArray();
    }

    // ---- completion ----

    @Test
    void completesAFileAndMovesOnToTheNext() throws Exception {
        MemSource file = new MemSource("smith.csv", csv(10));
        Outcome o = process().process(file);

        assertThat(o.result()).isEqualTo(Result.COMPLETED);
        assertThat(o.sectionCount()).isEqualTo(10);
        assertThat(file.deleted).isTrue();
        List<ConnectorMessage> msgs = messages();
        assertThat(msgs.getFirst()).isInstanceOf(FileStarted.class);
        FileCompleted fc = (FileCompleted) msgs.getLast();
        assertThat(fc.sectionCount()).isEqualTo(10);
        assertThat(fc.fileSize()).isEqualTo(file.data.length);
        assertThat(recorder.completed).singleElement().satisfies(r -> {
            assertThat(r.fileId()).isEqualTo(o.fileId());
            assertThat(r.fileDigest()).isEqualTo(fc.fileDigest());
        });
        assertThat(restore(SOURCE_FILES, o.fileId())).isEqualTo(file.data);

        FileProcessor p = process();
        assertThat(p.inProgress()).isEmpty();
        assertThat(p.process(new MemSource("next.csv", csv(2))).result()).isEqualTo(Result.COMPLETED);
    }

    @Test
    void crashMidArchiveResumesTheArchive() throws Exception {
        MemSource file = new MemSource("big.csv", csv(40)); // a few hundred bytes → several archive commits
        sourceArchiveHook = next -> {
            if (next == 1) {
                throw new Crash();
            }
        };
        FileProcessor first = process();
        assertThatThrownBy(() -> first.process(file)).isInstanceOf(Crash.class);
        assertThat(first.inProgress()).get().extracting(Checkpoint::state).isEqualTo(Checkpoint.State.ARCHIVING);
        assertThat(file.deleted).isFalse();

        sourceArchiveHook = b -> {};
        Outcome o = process().process(file);
        assertThat(o.result()).isEqualTo(Result.COMPLETED);
        assertThat(restore(SOURCE_FILES, o.fileId())).isEqualTo(file.data);
    }

    @Test
    void crashAfterTheOriginalIsDeletedIsFinishedWithoutTheSource() throws Exception {
        MemSource file = new MemSource("gone.csv", csv(5));
        file.crashAfterDelete = true;
        FileProcessor first = process();
        assertThatThrownBy(() -> first.process(file)).isInstanceOf(Crash.class);
        assertThat(file.deleted).isTrue();

        FileProcessor second = process();
        assertThat(second.inProgress()).isPresent();
        Outcome o = second.recoverWithoutSource().orElseThrow();
        assertThat(o.result()).isEqualTo(Result.COMPLETED);
        assertThat(second.inProgress()).isEmpty();
        assertThat(messages().stream().filter(FileCompleted.class::isInstance)).isNotEmpty();
    }

    @Test
    void sourceThatDisappearsMidSplitIsAbandoned() throws Exception {
        hooks = new PipelineHooks() {
            @Override
            public void batchCommitted(long lastSeq) {
                throw new Crash();
            }
        };
        FileProcessor first = process();
        assertThatThrownBy(() -> first.process(new MemSource("x.csv", csv(10)))).isInstanceOf(Crash.class);
        hooks = PipelineHooks.NONE;
        Outcome o = process().recoverWithoutSource().orElseThrow();
        assertThat(o.result()).isEqualTo(Result.ABANDONED);
        assertThat(process().inProgress()).isEmpty();
    }

    // ---- telemetry (design §12) ----

    @Test
    void emitsTracesAuditEventsAndMetricsWithoutPhi() throws Exception {
        InMemorySpanExporter spans = InMemorySpanExporter.create();
        InMemoryLogRecordExporter logs = InMemoryLogRecordExporter.create();
        InMemoryMetricReader metrics = InMemoryMetricReader.create();
        Telemetry.install(OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans))
                        .build())
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(SimpleLogRecordProcessor.create(logs)).build())
                .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metrics).build())
                .build());

        FileProcessor p = process();
        Outcome good = p.process(new MemSource("smith-john.csv", csv(10)));
        Outcome bad = p.process(new MemSource("jones-mary.csv", csv(7) + "8,\"never closed\n"));

        // Traces: one root per file, with the stages underneath.
        var all = spans.getFinishedSpanItems();
        var roots = all.stream().filter(sp -> sp.getName().equals("ingest.file")).toList();
        assertThat(roots).hasSize(2);
        assertThat(roots).allMatch(r -> !r.getParentSpanContext().isValid());
        assertThat(all).extracting(io.opentelemetry.sdk.trace.data.SpanData::getName)
                .contains("split.batch", "storage.commit", "publish.batch", "archive");
        var failedRoot = roots.stream().filter(r -> bad.fileId().toString()
                .equals(r.getAttributes().get(AttributeKey.stringKey("sdh.file_id")))).findFirst().orElseThrow();
        assertThat(failedRoot.getStatus().getStatusCode()).isEqualTo(io.opentelemetry.api.trace.StatusCode.ERROR);
        assertThat(failedRoot.getAttributes().get(AttributeKey.stringKey("sdh.error_code")))
                .isEqualTo("PARSE_UNTERMINATED_QUOTE");
        // publish.batch runs on another thread but stays inside its batch's trace.
        var batchIds = all.stream().filter(sp -> sp.getName().equals("split.batch"))
                .map(io.opentelemetry.sdk.trace.data.SpanData::getSpanId).toList();
        assertThat(all.stream().filter(sp -> sp.getName().equals("publish.batch"))
                .filter(sp -> batchIds.contains(sp.getParentSpanId()))).isNotEmpty();

        // Audit events.
        var events = logs.getFinishedLogRecordItems().stream().map(l -> l.getEventName()).toList();
        assertThat(events).contains("sdh.audit.file.discovered", "sdh.audit.file.started", "sdh.audit.batch.committed",
                "sdh.audit.file.archived", "sdh.audit.file.completed", "sdh.audit.file.failing",
                "sdh.audit.file.failed", "sdh.audit.lease.fenced_table");

        // Metrics.
        var data = metrics.collectAllMetrics();
        assertThat(sum(data, "sdh.sections.stored")).isEqualTo(10 + 6);
        assertThat(sum(data, "sdh.files.completed")).isEqualTo(1);
        assertThat(sum(data, "sdh.files.failed")).isEqualTo(1);
        assertThat(data).extracting(io.opentelemetry.sdk.metrics.data.MetricData::getName)
                .contains("sdh.bytes.read", "sdh.sections.published", "sdh.commit.duration", "sdh.publish.duration",
                        "sdh.files.in_progress", "sdh.file.progress_ratio");

        // No PHI anywhere: only allow-listed keys, and no filename or record content in any value or body.
        List<io.opentelemetry.api.common.Attributes> attrs = new ArrayList<>();
        all.forEach(sp -> attrs.add(sp.getAttributes()));
        logs.getFinishedLogRecordItems().forEach(l -> attrs.add(l.getAttributes()));
        data.forEach(m -> m.getData().getPoints().forEach(pt -> attrs.add(pt.getAttributes())));
        for (var a : attrs) {
            a.forEach((k, v) -> {
                assertThat(k.getKey()).isIn(com.sdh.healthcare.connector.core.telemetry.Attr.ALLOWED_KEYS);
                assertThat(String.valueOf(v)).doesNotContain("smith").doesNotContain("jones")
                        .doesNotContain("patient");
            });
        }
        assertThat(logs.getFinishedLogRecordItems()).allSatisfy(l -> assertThat(String.valueOf(l.getBodyValue()))
                .doesNotContain("smith").doesNotContain("patient"));
        assertThat(good.result()).isEqualTo(Result.COMPLETED);
    }

    static long sum(java.util.Collection<io.opentelemetry.sdk.metrics.data.MetricData> data, String name) {
        return data.stream().filter(m -> m.getName().equals(name))
                .flatMap(m -> m.getLongSumData().getPoints().stream())
                .mapToLong(io.opentelemetry.sdk.metrics.data.LongPointData::getValue).sum();
    }

    // ---- failure ----

    @Test
    void parseErrorArchivesToFailedFilesAndKeepsStoredSections() throws Exception {
        String content = csv(7) + "8,\"never closed\n9,x\n";
        MemSource file = new MemSource("bad.csv", content);
        Outcome o = process().process(file);

        assertThat(o.result()).isEqualTo(Result.FAILED);
        assertThat(o.failure().code()).isEqualTo(ErrorCode.PARSE_UNTERMINATED_QUOTE);
        assertThat(o.failure().stage()).isEqualTo("split");
        assertThat(o.failure().offset()).isEqualTo(csv(7).length());
        assertThat(o.archive()).isEqualTo(ArchiveLocation.FAILED_FILES);
        assertThat(file.deleted).isTrue();
        // Batches of 3: sections 0..5 were committed before the error, and every one is announced (the last batch
        // may be announced twice: once by its own publish, once on failure).
        assertThat(messages().stream().filter(SectionStored.class::isInstance)
                .map(m -> ((SectionStored) m).seq()).distinct().sorted().toList())
                .containsExactly(0L, 1L, 2L, 3L, 4L, 5L);
        assertThat(messages().stream().filter(FileCompleted.class::isInstance)).isEmpty();
        assertThat(recorder.failed).singleElement().satisfies(r -> {
            assertThat(r.lastCommittedSeq()).isEqualTo(5);
            assertThat(r.archivedTo()).isEqualTo(ArchiveLocation.FAILED_FILES);
            assertThat(r.fileDigest()).isNotNull();
        });
        assertThat(restore(FAILED_FILES, o.fileId())).isEqualTo(file.data);
        var summary = ArchiveReader.restore(table(FAILED_FILES), o.fileId(), keys,
                new SectionIds(IdKeys.loadOrCreate(table("sections"), null, keys, SECTIONS_KEK)), 0,
                OutputStream.nullOutputStream()).summary();
        assertThat(summary.getField(ERROR_CODE)).isEqualTo("PARSE_UNTERMINATED_QUOTE");
        assertThat(summary.getField(FAILURE_SEQ)).isEqualTo(5L);

        FileProcessor next = process();
        assertThat(next.inProgress()).isEmpty();
        assertThat(next.process(new MemSource("ok.csv", csv(1))).result()).isEqualTo(Result.COMPLETED);
    }

    @Test
    void sectionsCommittedJustBeforeAFailureAreStillAnnounced() throws Exception {
        // Sends for the second batch (calls 4..6, after file.started and batch 1) are slow, so the parse error is hit
        // while that batch is still publishing. Regression: the publish was cancelled mid-send, and the sections it
        // hadn't sent yet were stored but never announced.
        pub.slow = n -> n >= 4;
        Outcome o = process().process(new MemSource("bad.csv", csv(7) + "8,\"never closed\n"));
        assertThat(o.result()).isEqualTo(Result.FAILED);
        assertThat(messages().stream().filter(SectionStored.class::isInstance)
                .map(m -> ((SectionStored) m).seq()).distinct().sorted().toList())
                .containsExactly(0L, 1L, 2L, 3L, 4L, 5L);
    }

    @Test
    void tooLargeFileGoesStraightToTheLocalFallback() throws Exception {
        settings = settings.withSplit(settings.split().withLimits(new SplitLimits(100, 512 << 20, Long.MAX_VALUE)));
        MemSource file = new MemSource("huge.csv", csv(20));
        Outcome o = process().process(file);

        assertThat(o.result()).isEqualTo(Result.FAILED);
        assertThat(o.failure().code()).isEqualTo(ErrorCode.LIMIT_FILE_SIZE);
        assertThat(o.failure().stage()).isEqualTo("intake");
        assertThat(o.archive()).isEqualTo(ArchiveLocation.LOCAL_FALLBACK);
        assertThat(messages()).isEmpty(); // rejected before ingestion: no file.started
        assertThat(file.deleted).isTrue();
        ByteArrayOutputStream plain = new ByteArrayOutputStream();
        RecoveryEnvelope.Metadata meta = RecoveryEnvelope.decrypt(
                new ByteArrayInputStream(recorder.fallbacks.get(o.fileId())), recovery.getPrivate(), plain);
        assertThat(plain.toByteArray()).isEqualTo(file.data);
        assertThat(meta.originalName()).contains("huge.csv");
    }

    @Test
    void kmsDownForFailedFilesUsesTheLocalFallback() throws Exception {
        failedKeys = new KeyProvider() {
            public WrappedKey wrap(String keyRef, byte[] dataKey) {
                throw new IllegalArgumentException("KMS unreachable");
            }

            public byte[] unwrap(WrappedKey wrapped) {
                throw new IllegalArgumentException("KMS unreachable");
            }
        };
        MemSource file = new MemSource("bad.csv", "1,\"open\n");
        Outcome o = process().process(file);
        assertThat(o.archive()).isEqualTo(ArchiveLocation.LOCAL_FALLBACK);
        assertThat(recorder.fallbacks).containsKey(o.fileId());
        assertThat(process().inProgress()).isEmpty(); // FAILED was still recorded
    }

    @Test
    void fallbackFailureKeepsTheOriginal() throws Exception {
        settings = settings.withSplit(settings.split().withLimits(new SplitLimits(10, 512 << 20, Long.MAX_VALUE)));
        recorder.fallbackBroken = true;
        MemSource file = new MemSource("huge.csv", csv(5));
        assertThatThrownBy(() -> process().process(file)).isInstanceOf(IOException.class);
        assertThat(file.deleted).isFalse();
        assertThat(recorder.failed).isEmpty();
    }

    @Test
    void brokerDownFailsTheFileWithPublishFailed() throws Exception {
        pub.fail = n -> n > 0; // file.started gets through, nothing else
        Outcome o = process().process(new MemSource("p.csv", csv(4)));
        assertThat(o.result()).isEqualTo(Result.FAILED);
        assertThat(o.failure().code()).isEqualTo(ErrorCode.PUBLISH_FAILED);
        assertThat(o.failure().stage()).isEqualTo("publish");
        assertThat(o.archive()).isEqualTo(ArchiveLocation.FAILED_FILES);
    }

    @Test
    void crashWhileFailingFinishesTheFailureOnRestart() throws Exception {
        recorder.crashOnFailed = true;
        MemSource file = new MemSource("bad.csv", "1,\"open\n");
        FileProcessor first = process();
        assertThatThrownBy(() -> first.process(file)).isInstanceOf(Crash.class);
        assertThat(first.inProgress()).get().extracting(Checkpoint::state).isEqualTo(Checkpoint.State.FAILING);

        recorder.crashOnFailed = false;
        Outcome o = process().process(file);
        assertThat(o.result()).isEqualTo(Result.FAILED);
        assertThat(o.archive()).isEqualTo(ArchiveLocation.FAILED_FILES);
        assertThat(file.deleted).isTrue();
        assertThat(restore(FAILED_FILES, o.fileId())).isEqualTo(file.data);
    }

    // ---- doubles ----

    static final class Crash extends Error {}

    static final class MemSource implements SourceFile {
        final String name;
        final byte[] data;
        volatile boolean deleted;
        boolean crashAfterDelete;

        MemSource(String name, String content) {
            this.name = name;
            this.data = content.getBytes(UTF_8);
        }

        @Override
        public SeekableByteSource open() throws IOException {
            if (deleted) {
                throw new java.nio.file.NoSuchFileException("deleted");
            }
            return SeekableByteSource.of(data);
        }

        @Override
        public Optional<String> originalName() {
            return Optional.of(name);
        }

        @Override
        public void deleteOriginal() {
            deleted = true;
            if (crashAfterDelete) {
                throw new Crash();
            }
        }
    }

    static final class MemRecorder implements OutcomeRecorder {
        final List<CompletionRecord> completed = Collections.synchronizedList(new ArrayList<>());
        final List<FailureRecord> failed = Collections.synchronizedList(new ArrayList<>());
        final Map<UUID, byte[]> fallbacks = new HashMap<>();
        boolean fallbackBroken;
        boolean crashOnFailed;

        @Override
        public void completed(CompletionRecord r) {
            completed.removeIf(c -> c.fileId().equals(r.fileId()));
            completed.add(r);
        }

        @Override
        public void failed(FailureRecord r) {
            if (crashOnFailed) {
                throw new Crash();
            }
            failed.removeIf(c -> c.fileId().equals(r.fileId()));
            failed.add(r);
        }

        @Override
        public FallbackFile openFallback(UUID fileId) throws IOException {
            if (fallbackBroken) {
                throw new IOException("disk full");
            }
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            return new FallbackFile() {
                public OutputStream out() {
                    return buf;
                }

                public void commit() {
                    fallbacks.put(fileId, buf.toByteArray());
                }

                public void close() {}
            };
        }
    }
}
