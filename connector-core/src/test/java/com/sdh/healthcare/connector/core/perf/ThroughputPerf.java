package com.sdh.healthcare.connector.core.perf;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.pipeline.FileProcessor;
import com.sdh.healthcare.connector.core.pipeline.IngestPipeline;
import com.sdh.healthcare.connector.core.pipeline.OutcomeRecorder;
import com.sdh.healthcare.connector.core.pipeline.PipelineSettings;
import com.sdh.healthcare.connector.core.pipeline.SourceFile;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.storage.Archiver;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Throughput of the whole ingest path (design §13): split, hash, compress, encrypt, Parquet write, fenced commits,
 * message sealing, then the archive pass. Run with {@code -Pperf} (see {@code scripts/perf.sh}); never part of the
 * normal build.
 *
 * <p>The pipeline is assembled as the connector assembles it, with production defaults: commits every 10,000
 * sections or 512 MiB, one encrypt worker per core, 512 MiB archive commits. The broker is replaced by a counter
 * that acknowledges at once, so broker latency is excluded; every message is still sealed (ECDH-ES + ES256).
 *
 * <p>Settings (system properties): {@code perf.size} per file (default 1GiB), {@code perf.formats} (default
 * ccda,x12,csv), {@code perf.storage} {@code local} (default; Iceberg on local disk) or {@code s3} (SeaweedFS in
 * Docker), {@code perf.workers} (default: cores), {@code perf.recipient} {@code ec} (default) or {@code rsa}.
 * Results go to {@code target/perf/}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ThroughputPerf {
    static final String SECTIONS_KEK = "sections-kek/1";
    static final String SOURCE_KEK = "source-kek/1";
    static final String FAILED_KEK = "failed-kek/1";
    static final Path OUT = Path.of("target", "perf");

    final long size = parseSize(System.getProperty("perf.size", "1GiB"));
    final String storage = System.getProperty("perf.storage", "local");
    final int workers = Integer.getInteger("perf.workers", Runtime.getRuntime().availableProcessors());
    /** The message recipient's key type: {@code ec} (P-256, ECDH-ES+A256KW) or {@code rsa} (3072, RSA-OAEP-256). */
    final String recipientType = System.getProperty("perf.recipient", "ec");
    final List<String> rows = new ArrayList<>();

    PerfStorage backend;
    AesKeyWrapProvider keys;
    KeyPair signer;
    KeyPair recipient;
    KeyPair recovery;
    InMemorySpanExporter spans;

    static Stream<String> formats() {
        return Stream.of(System.getProperty("perf.formats", "ccda,x12,csv").split(",")).map(String::strip);
    }

    @BeforeAll
    void setUp() throws Exception {
        Files.createDirectories(OUT);
        backend = PerfStorage.create(storage);
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        keys = new AesKeyWrapProvider(Map.of(SECTIONS_KEK, kg.generateKey(), SOURCE_KEK, kg.generateKey(),
                FAILED_KEK, kg.generateKey()));
        signer = ec();
        recipient = switch (recipientType) {
            case "ec" -> ec();
            case "rsa" -> {
                KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
                g.initialize(3072);
                yield g.generateKeyPair();
            }
            default -> throw new IllegalArgumentException("perf.recipient must be ec or rsa");
        };
        recovery = ec();
        spans = InMemorySpanExporter.create();
        Telemetry.install(OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans))
                        .build())
                .build());
    }

    @AfterAll
    void report() throws IOException {
        Telemetry.install(io.opentelemetry.api.OpenTelemetry.noop());
        // Described before closing: the S3 backend's address is gone once its container stops.
        String described = backend == null ? storage : backend.describe();
        if (backend != null) {
            backend.close();
        }
        var os = ManagementFactory.getOperatingSystemMXBean();
        StringBuilder b = new StringBuilder();
        b.append("# Throughput, ").append(Instant.now()).append("\n\n");
        b.append("Storage: ").append(described).append(". CPUs: ")
                .append(os.getAvailableProcessors()).append(", encrypt workers: ").append(workers)
                .append(", message recipient key: ").append(recipientType.equals("rsa") ? "RSA-3072" : "P-256")
                .append(", max heap: ").append(Runtime.getRuntime().maxMemory() >> 20).append(" MiB, Java ")
                .append(System.getProperty("java.version")).append(", ").append(System.getProperty("os.arch"))
                .append(".\n\n");
        b.append("| Format | Size MiB | Sections | Total s | MiB/s | Sections/s | Split s (MiB/s) | Archive s (MiB/s)"
                + " | Avg cores | Busiest thread | Peak heap MiB | GC pause % | Full GCs | Stored ÷ source |\n");
        b.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|\n");
        rows.forEach(r -> b.append(r).append('\n'));
        String name = "throughput-" + storage + "-" + recipientType + ".md";
        Files.writeString(OUT.resolve(name), b.toString());
        System.out.println("\n" + b);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("formats")
    void throughput(String format) throws Exception {
        Path input = Files.createDirectories(OUT.resolve("input")).resolve("perf." + format);
        SyntheticFiles.Generated g = switch (format) {
            case "ccda" -> SyntheticFiles.ccda(input, size, 1);
            case "x12" -> SyntheticFiles.x12(input, size, 2);
            case "csv" -> SyntheticFiles.csv(input, size, 3);
            default -> throw new IllegalArgumentException("Unknown format " + format);
        };
        SplitConfig split = format.equals("csv") ? SplitConfig.defaults("csv").withOption("csv.header", "true")
                : SplitConfig.defaults(format);

        Catalog catalog = backend.catalog();
        String ns = "perf_" + format + "_" + UUID.randomUUID().toString().substring(0, 8);
        Table sections = IngestTables.sections(catalog, ns);
        Table source = IngestTables.sourceFiles(catalog, ns);
        Table failed = IngestTables.failedFiles(catalog, ns);
        FencedCommitter committer = new FencedCommitter(new LeaseToken() {
            public long epoch() {
                return 1;
            }

            public void checkHeld() {}
        });
        for (Table t : List.of(sections, source, failed)) {
            committer.establish(t);
        }
        SectionIds ids = new SectionIds(IdKeys.loadOrCreate(sections, committer, keys, SECTIONS_KEK));
        CountingPublisher pub = new CountingPublisher();
        PipelineSettings settings = PipelineSettings.defaults("perf", format, "perf", SECTIONS_KEK)
                .withSplit(split).withWorkers(workers);
        Clock clock = Clock.systemUTC();
        try (IngestPipeline pipeline = new IngestPipeline(settings, sections, committer, keys, ids,
                SplitterRegistry.load(), new MessageSealer(new ECDSASigner((ECPrivateKey) signer.getPrivate()),
                        "connector", recipient.getPublic(), "downstream"), pub, clock)) {
            FileProcessor processor = new FileProcessor(pipeline,
                    new Archiver(source, committer, keys, ids, Archiver.Settings.defaults("perf", SOURCE_KEK), clock),
                    new Archiver(failed, committer, keys, ids, Archiver.Settings.defaults("perf", FAILED_KEK), clock),
                    new NoRecords(), (java.security.interfaces.ECPublicKey) recovery.getPublic());

            System.gc();
            spans.reset();
            HeapSampler heap = HeapSampler.start();
            ThreadCpu threads = ThreadCpu.start();
            var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long cpu0 = os.getProcessCpuTime();
            long[] gc0 = gcTotals();
            long t0 = System.nanoTime();
            FileProcessor.Outcome o = processor.process(new PathSource(input));
            long wall = System.nanoTime() - t0;
            long cpu = os.getProcessCpuTime() - cpu0;
            long peakHeap = heap.stop();
            long[] gc1 = gcTotals();
            ThreadCpu.Busiest busiest = threads.busiest(wall);

            assertThat(o.result()).as("outcome").isEqualTo(FileProcessor.Result.COMPLETED);
            assertThat(o.sectionCount()).isEqualTo(g.sections());
            // Every generated section converts to JSON; a raw fallback would measure the wrong path.
            assertThat(pipeline.failedSectionCount(o.fileId())).as("sections stored raw").isZero();
            assertThat(pub.count.get()).as("file.started + sections + file.completed").isEqualTo(g.sections() + 2);

            long archiveNanos = spans.getFinishedSpanItems().stream().filter(s -> s.getName().equals("archive"))
                    .mapToLong(s -> s.getEndEpochNanos() - s.getStartEpochNanos()).sum();
            long splitNanos = wall - archiveNanos;
            sections.refresh();
            source.refresh();
            long stored = totalSize(sections) + totalSize(source);
            double mib = g.bytes() / 1048576.0;
            String row = String.format(Locale.ROOT,
                    "| %s | %.0f | %,d | %.1f | %.0f | %,.0f | %.1f (%.0f) | %.1f (%.0f) | %.1f | %s %.0f%% | %d | %.1f | %d"
                            + " | %.2f |",
                    format, mib, g.sections(), wall / 1e9, mib / (wall / 1e9), g.sections() / (wall / 1e9),
                    splitNanos / 1e9, mib / (splitNanos / 1e9), archiveNanos / 1e9, mib / (archiveNanos / 1e9),
                    (double) cpu / wall, busiest.name(), busiest.percent(), peakHeap >> 20,
                    100.0 * (gc1[0] - gc0[0]) * 1e6 / wall, gc1[1] - gc0[1], (double) stored / g.bytes());
            rows.add(row);
            System.out.println(row);
        } finally {
            Files.deleteIfExists(input);
            backend.dropNamespace(ns);
        }
    }

    static long totalSize(Table t) {
        return t.currentSnapshot() == null ? 0
                : Long.parseLong(t.currentSnapshot().summary().getOrDefault("total-files-size", "0"));
    }

    static long parseSize(String s) {
        String v = s.strip().toUpperCase(Locale.ROOT);
        long mult = 1;
        for (String[] u : new String[][] {{"GIB", "1073741824"}, {"GB", "1073741824"}, {"MIB", "1048576"},
            {"MB", "1048576"}}) {
            if (v.endsWith(u[0])) {
                mult = Long.parseLong(u[1]);
                v = v.substring(0, v.length() - u[0].length()).strip();
                break;
            }
        }
        return (long) (Double.parseDouble(v) * mult);
    }

    static KeyPair ec() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp256r1"));
        return g.generateKeyPair();
    }

    /**
     * CPU time per thread over a run. The busiest single thread's share of wall time shows a single-threaded
     * bottleneck (near 100%) that the average across cores hides.
     */
    static final class ThreadCpu {
        record Busiest(String name, double percent) {}

        private final java.lang.management.ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        private final Map<Long, Long> start = new java.util.HashMap<>();

        static ThreadCpu start() {
            ThreadCpu t = new ThreadCpu();
            for (long id : t.mx.getAllThreadIds()) {
                t.start.put(id, Math.max(0, t.mx.getThreadCpuTime(id)));
            }
            return t;
        }

        Busiest busiest(long wallNanos) {
            String name = "?";
            long most = 0;
            for (var info : mx.getThreadInfo(mx.getAllThreadIds())) {
                if (info == null) {
                    continue;
                }
                long used = mx.getThreadCpuTime(info.getThreadId()) - start.getOrDefault(info.getThreadId(), 0L);
                if (used > most) {
                    most = used;
                    name = info.getThreadName().replaceAll("-\\d+$", "");
                }
            }
            return new Busiest(name, 100.0 * most / wallNanos);
        }
    }

    /**
     * Stop-the-world GC time in ms (G1's young and mixed pauses, plus any full collection) and the number of full
     * collections. G1's concurrent cycle runs beside the application, so it isn't counted as pause time.
     */
    static long[] gcTotals() {
        long pauseMs = 0;
        long full = 0;
        for (var gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc.getName().contains("Concurrent")) {
                continue;
            }
            pauseMs += Math.max(0, gc.getCollectionTime());
            if (gc.getName().contains("Old")) {
                full += Math.max(0, gc.getCollectionCount());
            }
        }
        return new long[] {pauseMs, full};
    }

    /** Heap in use, sampled every 50 ms: the sum of per-pool peaks would overstate it, as pools peak at different times. */
    static final class HeapSampler {
        private final Thread thread;
        private volatile boolean running = true;
        private volatile long peak;

        private HeapSampler() {
            var memory = ManagementFactory.getMemoryMXBean();
            thread = Thread.ofPlatform().daemon().name("heap-sampler").start(() -> {
                while (running) {
                    peak = Math.max(peak, memory.getHeapMemoryUsage().getUsed());
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
        }

        static HeapSampler start() {
            return new HeapSampler();
        }

        long stop() throws InterruptedException {
            running = false;
            thread.join();
            return peak;
        }
    }

    /** Stands in for the broker: acknowledges at once and counts. */
    static final class CountingPublisher implements MessagePublisher {
        final AtomicLong count = new AtomicLong();

        @Override
        public CompletableFuture<Void> publish(OutboundMessage message) {
            count.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    /** A file on local disk, kept after processing so the harness controls clean-up. */
    record PathSource(Path path) implements SourceFile {
        @Override
        public SeekableByteSource open() throws IOException {
            return SeekableByteSource.open(path);
        }

        @Override
        public Optional<String> originalName() {
            return Optional.of(path.getFileName().toString());
        }

        @Override
        public void deleteOriginal() {}
    }

    /** Outcome records aren't part of what's measured. */
    static final class NoRecords implements OutcomeRecorder {
        @Override
        public void completed(CompletionRecord r) {}

        @Override
        public void failed(FailureRecord r) {}

        @Override
        public FallbackFile openFallback(UUID fileId) {
            throw new UnsupportedOperationException("no fallback in throughput runs");
        }
    }
}
