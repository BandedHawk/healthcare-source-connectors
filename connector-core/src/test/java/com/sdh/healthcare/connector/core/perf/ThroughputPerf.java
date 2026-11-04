/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.perf;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.pipeline.IngestPipeline;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.core.pipeline.PipelineSettings;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import com.sun.management.OperatingSystemMXBean;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.crypto.KeyGenerator;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Throughput and memory use of the whole ingest path (design §13): split,
 * hash, compress, encrypt, Parquet write, fenced commits and message
 * sealing, after the file is landed (the landing pass). Run with
 * {@code -Pperf} (see {@code scripts/perf.sh}, or the NetBeans actions in
 * {@code nbactions.xml}); never part of the normal build or the
 * integration tests, because the results depend on the machine.
 *
 * <p>The pipeline is assembled as the connector assembles it, with
 * production defaults: commits every 10,000 sections or 512 MiB, one
 * encrypt worker per core, 512 MiB landing commits. The broker is replaced
 * by a counter that acknowledges at once, so broker latency is excluded;
 * every message is still sealed (ECDH-ES + ES256).
 *
 * <p>Settings (system properties): {@code perf.size} per file (default
 * 1GiB), {@code perf.formats} (default ccda,x12; csv is also available),
 * {@code perf.storage} {@code local} (default; Iceberg on local disk) or
 * {@code s3} (SeaweedFS in Docker), {@code perf.workers} (default: cores),
 * {@code perf.recipient} {@code ec} (default) or {@code rsa}. The heap is
 * set by the profile ({@code perf.heap}, default 1024m, as in a 2 GiB
 * Kubernetes container).
 *
 * <p>Results are printed at the end and written to {@code target/perf/}:
 * {@code throughput-<storage>-<recipient>.md} for the latest run, and
 * {@code history.csv}, which gains one row per format per run until
 * {@code mvn clean}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ThroughputPerf
{

    /** The KEK id for section data keys. */
    static final String SECTIONS_KEK = "sections-kek/1";

    /** The KEK id for landed item data keys. */
    static final String LANDED_KEK = "landed-items-kek/1";

    /** Where inputs and results are written. */
    static final Path OUT = Path.of("target", "perf");

    /** The results history, one row per format per run. */
    static final Path HISTORY = OUT.resolve("history.csv");

    /** The size of each generated file. */
    final long size = parseSize(System.getProperty("perf.size", "1GiB"));

    /** {@code local} or {@code s3}. */
    final String storage = System.getProperty("perf.storage", "local");

    /** The number of encrypt workers. */
    final int workers = Integer.getInteger("perf.workers",
            Runtime.getRuntime().availableProcessors());

    /**
     * The message recipient's key type: {@code ec} (P-256, ECDH-ES+A256KW)
     * or {@code rsa} (3072, RSA-OAEP-256).
     */
    final String recipientType =
            System.getProperty("perf.recipient", "ec");

    /** The results so far, one per format. */
    final List<Result> results = new ArrayList<>();

    /** Where the tables are stored. */
    PerfStorage backend;

    /** The KEKs. */
    AesKeyWrapProvider keys;

    /** The connector's signing key. */
    KeyPair signer;

    /** The message recipient's key. */
    KeyPair recipient;

    /** Collects the pipeline's spans. */
    InMemorySpanExporter spans;

    /**
     * The formats to run.
     *
     * @return the format names from {@code perf.formats}
     */
    static Stream<String> formats()
    {
        return Stream.of(System.getProperty("perf.formats", "ccda,x12")
                .split(",")).map(String::strip);
    }

    /**
     * Starts the storage, makes the keys and installs telemetry.
     *
     * @throws Exception if a key or the storage can't be made
     */
    @BeforeAll
    void setUp() throws Exception
    {
        Files.createDirectories(OUT);
        backend = PerfStorage.create(storage);
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(SECTIONS_KEK,
                generator.generateKey(), LANDED_KEK,
                generator.generateKey()));
        signer = ec();
        recipient = switch (recipientType)
        {
            case "ec" -> ec();
            case "rsa" ->
            {
                KeyPairGenerator pairs = KeyPairGenerator.getInstance("RSA");
                pairs.initialize(3072);
                yield pairs.generateKeyPair();
            }
            default -> throw new IllegalArgumentException(
                    "perf.recipient must be ec or rsa");
        };
        spans = InMemorySpanExporter.create();
        Telemetry.install(OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(spans))
                        .build())
                .build());
    }

    /**
     * Stops the storage and writes the report and history.
     *
     * @throws IOException if the results can't be written
     */
    @AfterAll
    void report() throws IOException
    {
        Telemetry.install(OpenTelemetry.noop());
        // Described before closing: the S3 backend's address is gone once
        // its container stops.
        String described = backend == null ? storage : backend.describe();
        if (backend != null)
        {
            backend.close();
        }
        int processors =
                ManagementFactory.getOperatingSystemMXBean()
                        .getAvailableProcessors();
        StringBuilder text = new StringBuilder();
        text.append("# Throughput, ").append(Instant.now()).append("\n\n");
        text.append("Storage: ").append(described).append(". CPUs: ")
                .append(processors).append(", encrypt workers: ")
                .append(workers)
                .append(", message recipient key: ")
                .append(recipientType.equals("rsa") ? "RSA-3072" : "P-256")
                .append(", max heap: ")
                .append(Runtime.getRuntime().maxMemory() >> 20)
                .append(" MiB, Java ")
                .append(System.getProperty("java.version")).append(", ")
                .append(System.getProperty("os.arch"))
                .append(".\n\n");
        text.append("## Throughput\n\n");
        text.append("| Format | Size MiB | Sections | Total s | MiB/s"
                + " | Sections/s | Split s (MiB/s) | Land s (MiB/s)"
                + " | Avg cores | Busiest thread | Stored ÷ source |\n");
        text.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|\n");
        results.forEach(result -> text.append(result.throughputRow())
                .append('\n'));
        text.append("\n## Memory\n\n");
        text.append("| Format | Heap after GC, peak MiB"
                + " | Heap in use, peak MiB | Direct buffers, peak MiB"
                + " | Process RSS, peak MiB | Allocated GiB"
                + " | Allocation MiB/s | GC pause % | Full GCs |\n");
        text.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        results.forEach(result -> text.append(result.memoryRow())
                .append('\n'));
        text.append("""

                - **Heap after GC, peak:** the most heap still in use \
                right after a collection; the closest measure of what the \
                run needs. A young collection leaves old garbage in place, \
                so this is an upper bound.
                - **Heap in use, peak:** sampled every 50 ms. G1 grows \
                towards the maximum heap when it can, so this shows the \
                heap it was given more than the heap it needed.
                - **Direct buffers:** off-heap buffers (zstd, Parquet, \
                NIO), limited by -XX:MaxDirectMemorySize (default: the \
                maximum heap).
                - **Process RSS:** the whole JVM's resident memory, \
                sampled every 50 ms: what a container memory limit sees. \
                The JVM rarely hands memory back, so a later format can \
                show an earlier one's peak.
                """);
        String name = "throughput-" + storage + "-" + recipientType + ".md";
        Files.writeString(OUT.resolve(name), text.toString());
        appendHistory();
        System.out.println("\n" + text);
        System.out.println("Written to " + OUT.resolve(name).toAbsolutePath()
                + " and " + HISTORY.toAbsolutePath());
    }

    /**
     * Appends one row per format to the history, so runs can be compared
     * (a spreadsheet opens it directly).
     *
     * @throws IOException if the history can't be written
     */
    void appendHistory() throws IOException
    {
        StringBuilder csv = new StringBuilder();
        if (!Files.exists(HISTORY))
        {
            csv.append(Result.CSV_HEADER).append('\n');
        }
        String when = Instant.now().toString();
        long maxHeapMib = Runtime.getRuntime().maxMemory() >> 20;
        results.forEach(result -> csv.append(result.csvRow(when, storage,
                recipientType, maxHeapMib, workers)).append('\n'));
        Files.writeString(HISTORY, csv.toString(), StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    /**
     * One format's measurements.
     *
     * @param format the format name
     * @param mib the input size in MiB
     * @param sections the number of sections
     * @param wallNanos the whole run's wall time
     * @param splitNanos the split pass's wall time
     * @param archiveNanos the landing pass's wall time
     * @param cores the average cores in use
     * @param busiestThread the name of the busiest thread
     * @param busiestPercent the busiest thread's share of wall time
     * @param storedRatio stored bytes over source bytes
     * @param heapAfterGcPeak the most heap in use after a collection
     * @param heapInUsePeak the most heap in use when sampled
     * @param directPeak the most direct buffer memory in use
     * @param rssPeak the largest resident set, or -1 if unknown
     * @param allocated the bytes allocated
     * @param gcPauseMs the stop-the-world GC time
     * @param fullGcs the number of full collections
     */
    record Result(String format, double mib, long sections, long wallNanos,
            long splitNanos, long archiveNanos, double cores,
            String busiestThread, double busiestPercent, double storedRatio,
            long heapAfterGcPeak, long heapInUsePeak, long directPeak,
            long rssPeak, long allocated, long gcPauseMs, long fullGcs)
    {

        /** The history file's header row. */
        static final String CSV_HEADER = "timestamp,storage,recipient,"
                + "max_heap_mib,workers,format,size_mib,sections,"
                + "total_s,mib_per_s,sections_per_s,split_s,land_s,"
                + "avg_cores,busiest_thread,busiest_pct,"
                + "stored_ratio,heap_after_gc_peak_mib,heap_in_use_peak_mib,"
                + "direct_peak_mib,rss_peak_mib,"
                + "allocated_gib,allocation_mib_per_s,gc_pause_pct,full_gcs";

        /**
         * The whole run's wall time.
         *
         * @return seconds
         */
        double seconds()
        {
            return wallNanos / 1e9;
        }

        /**
         * GC pause time as a share of wall time.
         *
         * @return the percentage
         */
        double gcPercent()
        {
            return 100.0 * gcPauseMs * 1e6 / wallNanos;
        }

        /**
         * The throughput table row.
         *
         * @return the Markdown row
         */
        String throughputRow()
        {
            return String.format(Locale.ROOT,
                    "| %s | %.0f | %,d | %.1f | %.0f | %,.0f | %.1f (%.0f)"
                    + " | %.1f (%.0f) | %.1f | %s %.0f%% | %.2f |",
                    format, mib, sections, seconds(), mib / seconds(),
                    sections / seconds(), splitNanos / 1e9,
                    mib / (splitNanos / 1e9), archiveNanos / 1e9,
                    mib / (archiveNanos / 1e9), cores, busiestThread,
                    busiestPercent, storedRatio);
        }

        /**
         * The memory table row.
         *
         * @return the Markdown row
         */
        String memoryRow()
        {
            return String.format(Locale.ROOT,
                    "| %s | %d | %d | %d | %s | %.1f | %,.0f | %.1f | %d |",
                    format, heapAfterGcPeak >> 20, heapInUsePeak >> 20,
                    directPeak >> 20,
                    rssPeak < 0 ? "n/a" : Long.toString(rssPeak >> 20),
                    allocated / 1073741824.0, (allocated >> 20) / seconds(),
                    gcPercent(), fullGcs);
        }

        /**
         * The history row.
         *
         * @param when the run's time
         * @param storage the storage kind
         * @param recipient the recipient key type
         * @param maxHeapMib the maximum heap
         * @param workers the number of encrypt workers
         * @return the CSV row
         */
        String csvRow(String when, String storage, String recipient,
                long maxHeapMib, int workers)
        {
            return String.format(Locale.ROOT,
                    "%s,%s,%s,%d,%d,%s,%.0f,%d,%.2f,%.1f,%.0f,%.2f,%.2f,"
                    + "%.2f,%s,%.0f,%.3f,"
                    + "%d,%d,%d,%d,%.2f,%.0f,%.2f,%d", when, storage,
                    recipient, maxHeapMib, workers, format, mib,
                    sections, seconds(), mib / seconds(),
                    sections / seconds(), splitNanos / 1e9,
                    archiveNanos / 1e9, cores, busiestThread,
                    busiestPercent, storedRatio, heapAfterGcPeak >> 20,
                    heapInUsePeak >> 20, directPeak >> 20,
                    rssPeak < 0 ? -1 : rssPeak >> 20,
                    allocated / 1073741824.0, (allocated >> 20) / seconds(),
                    gcPercent(), fullGcs);
        }
    }

    /**
     * Lands and processes one generated file of a format, measuring time,
     * CPU and memory.
     *
     * @param format the format name
     * @throws Exception if the test fails to run
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("formats")
    void throughput(String format) throws Exception
    {
        Path input = Files.createDirectories(OUT.resolve("input"))
                .resolve("perf." + format);
        SyntheticFiles.Generated generated = switch (format)
        {
            case "ccda" -> SyntheticFiles.ccda(input, size, 1);
            case "x12" -> SyntheticFiles.x12(input, size, 2);
            case "csv" -> SyntheticFiles.csv(input, size, 3);
            default -> throw new IllegalArgumentException(
                    "Unknown format " + format);
        };
        SplitConfig split = format.equals("csv")
                ? SplitConfig.defaults("csv").withOption("csv.header", "true")
                : SplitConfig.defaults(format);

        Catalog catalog = backend.catalog();
        String namespace = "perf_" + format + "_"
                + UUID.randomUUID().toString().substring(0, 8);
        Table sections = IngestTables.sections(catalog, namespace);
        Table source = LandingTables.landedItems(catalog, namespace);
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
        for (Table table : List.of(sections, source))
        {
            committer.establish(table);
        }
        SectionIds ids = new SectionIds(
                IdKeys.loadOrCreate(sections, committer, keys, SECTIONS_KEK));
        CountingPublisher publisher = new CountingPublisher();
        PipelineSettings settings = PipelineSettings
                .defaults("perf", format, "perf", SECTIONS_KEK)
                .withSplit(split).withWorkers(workers);
        Clock clock = Clock.systemUTC();
        try (IngestPipeline pipeline = new IngestPipeline(settings, sections,
                committer, keys, ids, SplitterRegistry.load(),
                new MessageSealer(new ECDSASigner(
                        (ECPrivateKey) signer.getPrivate()), "connector",
                        recipient.getPublic(), "downstream"),
                publisher, clock))
        {
            ItemProcessor processor = new ItemProcessor(pipeline);
            ItemLander lander = new ItemLander(source, committer, keys, ids,
                    ItemLander.Settings.defaults("perf", LANDED_KEK), clock);

            System.gc();
            spans.reset();
            MemorySampler memory = MemorySampler.start();
            HeapAfterGc afterGc = HeapAfterGc.start();
            com.sun.management.ThreadMXBean threadBean =
                    (com.sun.management.ThreadMXBean)
                            ManagementFactory.getThreadMXBean();
            long allocatedBefore = threadBean.getTotalThreadAllocatedBytes();
            ThreadCpu threads = ThreadCpu.start();
            OperatingSystemMXBean system = (OperatingSystemMXBean)
                    ManagementFactory.getOperatingSystemMXBean();
            long cpuBefore = system.getProcessCpuTime();
            long[] gcBefore = gcTotals();
            long start = System.nanoTime();
            UUID itemId = UuidV7.next(clock.millis());
            LandedItem landed = lander.land(new NewItem(itemId, ItemKind.FILE,
                    null, null, null), offset ->
            {
                InputStream in = Files.newInputStream(input);
                in.skipNBytes(offset);
                return in;
            });
            long landNanos = System.nanoTime() - start;
            ItemProcessor.Outcome outcome = processor.process(itemId,
                    landed.digest(),
                    () -> LandedItemSource.open(source, itemId, keys));
            long wall = System.nanoTime() - start;
            long cpu = system.getProcessCpuTime() - cpuBefore;
            long allocated =
                    threadBean.getTotalThreadAllocatedBytes() - allocatedBefore;
            MemorySampler.Peaks peaks = memory.stop();
            long heapAfterGcPeak = afterGc.stop();
            long[] gcAfter = gcTotals();
            ThreadCpu.Busiest busiest = threads.busiest(wall);

            assertThat(outcome.result()).as("outcome")
                    .isEqualTo(ItemProcessor.Result.COMPLETED);
            assertThat(outcome.sectionCount())
                    .isEqualTo(generated.sections());
            // Every generated section converts to JSON; a raw fallback
            // would measure the wrong path.
            assertThat(pipeline.failedSectionCount(outcome.fileId()))
                    .as("sections stored raw").isZero();
            assertThat(publisher.count.get())
                    .as("file.started + sections + file.completed")
                    .isEqualTo(generated.sections() + 2);

            long archiveNanos = landNanos;
            long splitNanos = wall - landNanos;
            sections.refresh();
            source.refresh();
            long stored = totalSize(sections) + totalSize(source);
            Result result = new Result(format, generated.bytes() / 1048576.0,
                    generated.sections(), wall, splitNanos, archiveNanos,
                    (double) cpu / wall, busiest.name(), busiest.percent(),
                    (double) stored / generated.bytes(), heapAfterGcPeak,
                    peaks.heap(), peaks.direct(), peaks.rss(), allocated,
                    gcAfter[0] - gcBefore[0], gcAfter[1] - gcBefore[1]);
            results.add(result);
            System.out.println(result.throughputRow());
            System.out.println(result.memoryRow());
        }
        finally
        {
            Files.deleteIfExists(input);
            backend.dropNamespace(namespace);
        }
    }

    /**
     * The total size of a table's data files.
     *
     * @param table the table
     * @return bytes, or 0 for an empty table
     */
    static long totalSize(Table table)
    {
        return table.currentSnapshot() == null ? 0
                : Long.parseLong(table.currentSnapshot().summary()
                        .getOrDefault("total-files-size", "0"));
    }

    /**
     * Parses a size such as {@code 1GiB} or {@code 512MB}.
     *
     * @param text the size; binary units either way
     * @return bytes
     */
    static long parseSize(String text)
    {
        String value = text.strip().toUpperCase(Locale.ROOT);
        long multiplier = 1;
        for (String[] unit : new String[][] {{"GIB", "1073741824"},
            {"GB", "1073741824"}, {"MIB", "1048576"}, {"MB", "1048576"}})
        {
            if (value.endsWith(unit[0]))
            {
                multiplier = Long.parseLong(unit[1]);
                value = value.substring(0, value.length() - unit[0].length())
                        .strip();
                break;
            }
        }
        return (long) (Double.parseDouble(value) * multiplier);
    }

    /**
     * A new P-256 key pair.
     *
     * @return the key pair
     * @throws Exception if the curve isn't available
     */
    static KeyPair ec() throws Exception
    {
        KeyPairGenerator pairs = KeyPairGenerator.getInstance("EC");
        pairs.initialize(new ECGenParameterSpec("secp256r1"));
        return pairs.generateKeyPair();
    }

    /**
     * CPU time per thread over a run. The busiest single thread's share of
     * wall time shows a single-threaded bottleneck (near 100%) that the
     * average across cores hides.
     */
    static final class ThreadCpu
    {

        /**
         * The busiest thread.
         *
         * @param name its name, without a trailing number
         * @param percent its CPU time as a share of wall time
         */
        record Busiest(String name, double percent)
        {
        }

        /** The thread bean. */
        private final ThreadMXBean threadBean =
                ManagementFactory.getThreadMXBean();

        /** Each thread's CPU time at the start, by thread id. */
        private final Map<Long, Long> start = new HashMap<>();

        /**
         * Records every thread's CPU time now.
         *
         * @return the measurement
         */
        static ThreadCpu start()
        {
            ThreadCpu cpu = new ThreadCpu();
            for (long id : cpu.threadBean.getAllThreadIds())
            {
                cpu.start.put(id,
                        Math.max(0, cpu.threadBean.getThreadCpuTime(id)));
            }
            return cpu;
        }

        /**
         * The thread that used the most CPU since the start.
         *
         * @param wallNanos the wall time since the start
         * @return the busiest thread
         */
        Busiest busiest(long wallNanos)
        {
            String name = "?";
            long most = 0;
            for (ThreadInfo info :
                    threadBean.getThreadInfo(threadBean.getAllThreadIds()))
            {
                if (info == null)
                {
                    continue;
                }
                long used = threadBean.getThreadCpuTime(info.getThreadId())
                        - start.getOrDefault(info.getThreadId(), 0L);
                if (used > most)
                {
                    most = used;
                    name = info.getThreadName().replaceAll("-\\d+$", "");
                }
            }
            return new Busiest(name, 100.0 * most / wallNanos);
        }
    }

    /**
     * Stop-the-world GC time in ms (G1's young and mixed pauses, plus any
     * full collection) and the number of full collections. G1's concurrent
     * cycle runs beside the application, so it isn't counted as pause
     * time.
     *
     * @return the pause time and the full collections
     */
    static long[] gcTotals()
    {
        long pauseMs = 0;
        long full = 0;
        for (GarbageCollectorMXBean collector :
                ManagementFactory.getGarbageCollectorMXBeans())
        {
            if (collector.getName().contains("Concurrent"))
            {
                continue;
            }
            pauseMs += Math.max(0, collector.getCollectionTime());
            if (collector.getName().contains("Old"))
            {
                full += Math.max(0, collector.getCollectionCount());
            }
        }
        return new long[] {pauseMs, full};
    }

    /**
     * Heap in use, direct buffers and the process's resident memory,
     * sampled every 50 ms. Sampled as totals: the sum of per-pool peaks
     * would overstate the heap, as pools peak at different times.
     */
    static final class MemorySampler
    {

        /**
         * The peaks seen.
         *
         * @param heap heap in use
         * @param direct direct buffer memory
         * @param rss resident set size, or -1 if unknown
         */
        record Peaks(long heap, long direct, long rss)
        {
        }

        /** The process's memory statistics (Linux). */
        private static final Path STATM = Path.of("/proc/self/statm");

        /** The page size statm counts in. */
        private static final long PAGE = 4096;

        /** The sampling thread. */
        private final Thread thread;

        /** Cleared to stop sampling. */
        private volatile boolean running = true;

        /** The peak heap in use. */
        private volatile long heap;

        /** The peak direct buffer memory. */
        private volatile long direct;

        /** The peak resident set size, or -1 if unknown. */
        private volatile long rss = -1;

        /** Starts sampling. */
        private MemorySampler()
        {
            MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
            BufferPoolMXBean directPool = ManagementFactory
                    .getPlatformMXBeans(BufferPoolMXBean.class).stream()
                    .filter(pool -> pool.getName().equals("direct"))
                    .findFirst().orElse(null);
            thread = Thread.ofPlatform().daemon().name("memory-sampler")
                    .start(() ->
                    {
                        while (running)
                        {
                            heap = Math.max(heap,
                                    memory.getHeapMemoryUsage().getUsed());
                            if (directPool != null)
                            {
                                direct = Math.max(direct,
                                        directPool.getMemoryUsed());
                            }
                            rss = Math.max(rss, residentBytes());
                            try
                            {
                                Thread.sleep(50);
                            }
                            catch (InterruptedException interrupted)
                            {
                                return;
                            }
                        }
                    });
        }

        /**
         * Resident set size from /proc (Linux, 4 KiB pages).
         *
         * @return bytes, or -1 elsewhere
         */
        static long residentBytes()
        {
            try
            {
                return Long.parseLong(Files.readString(STATM).strip()
                        .split("\\s+")[1]) * PAGE;
            }
            catch (IOException | RuntimeException failure)
            {
                return -1;
            }
        }

        /**
         * Starts sampling.
         *
         * @return the sampler
         */
        static MemorySampler start()
        {
            return new MemorySampler();
        }

        /**
         * Stops sampling.
         *
         * @return the peaks seen
         * @throws InterruptedException if interrupted while waiting for the
         *         sampling thread
         */
        Peaks stop() throws InterruptedException
        {
            running = false;
            thread.join();
            return new Peaks(heap, direct, rss);
        }
    }

    /**
     * The most heap in use right after any collection during the run, from
     * the collectors' notifications.
     */
    static final class HeapAfterGc implements NotificationListener
    {

        /** The names of the heap's memory pools. */
        private final Set<String> heapPools = new HashSet<>();

        /** The peak heap in use after a collection. */
        private final AtomicLong peak = new AtomicLong();

        /**
         * Starts listening to every collector.
         *
         * @return the listener
         */
        static HeapAfterGc start()
        {
            HeapAfterGc listener = new HeapAfterGc();
            for (MemoryPoolMXBean pool :
                    ManagementFactory.getMemoryPoolMXBeans())
            {
                if (pool.getType() == MemoryType.HEAP)
                {
                    listener.heapPools.add(pool.getName());
                }
            }
            for (GarbageCollectorMXBean collector :
                    ManagementFactory.getGarbageCollectorMXBeans())
            {
                ((NotificationEmitter) collector)
                        .addNotificationListener(listener, null, null);
            }
            return listener;
        }

        /**
         * Records the heap in use after a collection.
         *
         * @param notification the collector's notification
         * @param handback unused
         */
        @Override
        public void handleNotification(Notification notification,
                Object handback)
        {
            if (!notification.getType().equals(GarbageCollectionNotificationInfo
                    .GARBAGE_COLLECTION_NOTIFICATION))
            {
                return;
            }
            GcInfo info = GarbageCollectionNotificationInfo.from(
                    (CompositeData) notification.getUserData()).getGcInfo();
            long used = info.getMemoryUsageAfterGc().entrySet().stream()
                    .filter(entry -> heapPools.contains(entry.getKey()))
                    .mapToLong(entry -> entry.getValue().getUsed()).sum();
            peak.accumulateAndGet(used, Math::max);
        }

        /**
         * Stops listening.
         *
         * @return the peak heap in use after a collection
         * @throws Exception if a listener can't be removed
         */
        long stop() throws Exception
        {
            for (GarbageCollectorMXBean collector :
                    ManagementFactory.getGarbageCollectorMXBeans())
            {
                ((NotificationEmitter) collector)
                        .removeNotificationListener(this);
            }
            return peak.get();
        }
    }

    /** Stands in for the broker: acknowledges at once and counts. */
    static final class CountingPublisher implements MessagePublisher
    {

        /** The messages published. */
        final AtomicLong count = new AtomicLong();

        /**
         * Counts the message and acknowledges it.
         *
         * @param message the message
         * @return a completed future
         */
        @Override
        public CompletableFuture<Void> publish(OutboundMessage message)
        {
            count.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }
}
