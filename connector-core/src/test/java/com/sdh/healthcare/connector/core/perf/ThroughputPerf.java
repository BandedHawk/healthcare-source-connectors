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
import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointCodec;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The core benchmark (design §13.1): throughput and memory use of the
 * split, encrypt and store path (split, hash, compress, encrypt, Parquet
 * write, fenced commits and message sealing) without the applications
 * around it. Each file is landed first, outside the measurement, then
 * split from landing. The landing node and the splitter application are
 * measured by {@code LandingPerf} and {@code SplitterPerf}; set beside
 * them, this shows what the applications add. Run with {@code -Pperf}
 * (see {@code scripts/perf.sh}, or the NetBeans actions in
 * {@code nbactions.xml}); never part of the normal build or the
 * integration tests, because the results depend on the machine.
 *
 * <p>The pipeline is assembled as the connector assembles it, with
 * production defaults: commits every 10,000 records or 512 MiB, one
 * encrypt worker per core. The broker is replaced by a counter that
 * acknowledges at once, so broker latency is excluded; every message is
 * still sealed (ECDH-ES + ES256).
 *
 * <p>Settings (system properties) besides those of {@link PerfSettings}:
 * {@code perf.formats} (default ccda,x12; csv is also available),
 * {@code perf.workers} (default: cores), {@code perf.recipient}
 * {@code ec} (default) or {@code rsa}. It runs when {@code perf.stages}
 * includes {@code core}. Results go to {@link PerfOutput} as stage
 * {@code core}, workload {@code bulk-<format>}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ThroughputPerf
{

    /** The KEK id for record data keys. */
    static final String RECORDS_KEK = "records-kek/1";

    /** The KEK id for landed item data keys. */
    static final String LANDED_KEK = "landed-items-kek/1";

    /** The seed of a CSV file. */
    static final long CSV_SEED = 3;

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
    final List<StageResult> results = new ArrayList<>();

    /** Where the tables are stored. */
    PerfStorage backend;

    /** The KEKs. */
    AesKeyWrapProvider keys;

    /** The connector's signing key. */
    KeyPair signer;

    /** The message recipient's key. */
    KeyPair recipient;

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
     * Starts the storage, makes the keys and installs telemetry, if the
     * core stage is selected.
     *
     * @throws Exception if a key or the storage can't be made
     */
    @BeforeAll
    void setUp() throws Exception
    {
        Assumptions.assumeTrue(PerfSettings.runs(PerfSettings.CORE),
                "perf.stages doesn't include core");
        backend = PerfStorage.create(PerfSettings.storage());
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(RECORDS_KEK,
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
        // Spans are recorded as in the connector, so their cost is counted.
        Telemetry.install(OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(
                                InMemorySpanExporter.create()))
                        .build())
                .build());
    }

    /**
     * Records the results and closes the storage.
     *
     * @throws Exception if the results can't be written
     */
    @AfterAll
    void report() throws Exception
    {
        if (backend == null)
        {
            return;
        }
        Telemetry.install(OpenTelemetry.noop());
        // Described before closing: the S3 backend's address is gone once
        // its container stops.
        String described = backend.describe();
        backend.close();
        PerfOutput.record(PerfSettings.CORE, results, List.of(
                "Storage: " + described + ". Encrypt workers: " + workers
                        + ", message recipient key: "
                        + (recipientType.equals("rsa") ? "RSA-3072"
                                : "P-256") + ".",
                "",
                "- **Wall time:** splitting one landed file, from opening"
                        + " it to its last commit; landing it is done"
                        + " before and not counted.",
                "- **Latency:** the same as the wall time for a single"
                        + " file, so it isn't given.",
                "- **Heap after GC, peak:** the most heap still in use"
                        + " right after a collection; the closest measure"
                        + " of what the run needs, and an upper bound.",
                "- **Heap in use, peak:** sampled every 50 ms; G1 grows"
                        + " towards the maximum heap when it can.",
                "- **Process RSS:** the whole JVM's resident memory,"
                        + " sampled: what a container limit sees."));
    }

    /**
     * Lands one generated file of a format, then splits it from landing,
     * measuring time, CPU and memory.
     *
     * @param format the format name
     * @throws Exception if the test fails to run
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("formats")
    void throughput(String format) throws Exception
    {
        Path input = Files.createDirectories(PerfSettings.out()
                .resolve("input")).resolve("core." + format);
        SyntheticFiles.Generated generated = switch (format)
        {
            case "ccda" -> Workload.bulkFile(PerfSettings.BULK_CCDA, input);
            case "x12" -> Workload.bulkFile(PerfSettings.BULK_X12, input);
            case "csv" -> SyntheticFiles.csv(input, PerfSettings.size(),
                    CSV_SEED);
            default -> throw new IllegalArgumentException(
                    "Unknown format " + format);
        };
        SplitConfig split = format.equals("csv")
                ? SplitConfig.defaults("csv").withOption("csv.header", "true")
                : SplitConfig.defaults(format);

        Catalog catalog = backend.catalog();
        String namespace = "perf_" + format + "_"
                + UUID.randomUUID().toString().substring(0, 8);
        Table records = IngestTables.splitRecords(catalog, namespace);
        Table source = LandingTables.landedItems(catalog, namespace);
        LeaseToken lease = new LeaseToken()
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
        };
        FencedCommitter committer = new FencedCommitter(lease);
        committer.establish(source);
        RecordIds ids = new RecordIds(
                IdKeys.loadOrCreateShared(records, keys, RECORDS_KEK));
        ItemCheckpoints checkpoints = new ItemCheckpoints(
                TestPostgres.get().getPostgresDatabase(), namespace,
                new CheckpointCodec(ids), Duration.ofSeconds(30));
        checkpoints.createTableIfMissing();
        CountingPublisher publisher = new CountingPublisher();
        PipelineSettings settings = PipelineSettings
                .defaults("perf", format, "perf", RECORDS_KEK)
                .withSplit(split).withWorkers(workers);
        Clock clock = Clock.systemUTC();
        try (IngestPipeline pipeline = new IngestPipeline(settings, records,
                checkpoints, keys, ids, SplitterRegistry.load(),
                new MessageSealer(new ECDSASigner(
                        (ECPrivateKey) signer.getPrivate()), "connector",
                        recipient.getPublic(), "downstream"),
                publisher, clock))
        {
            ItemProcessor processor = new ItemProcessor(pipeline, "perf");
            ItemLander lander = new ItemLander(source, committer, keys, ids,
                    ItemLander.Settings.defaults("perf", LANDED_KEK), clock);
            UUID itemId = UuidV7.next(clock.millis());
            LandedItem landed = lander.land(new NewItem(itemId, ItemKind.FILE,
                    null, null, null), offset ->
            {
                InputStream in = Files.newInputStream(input);
                in.skipNBytes(offset);
                return in;
            });
            long landedBytes = totalSize(source);

            ResourceMonitor monitor = ResourceMonitor.start();
            ItemProcessor.Outcome outcome = processor.process(itemId,
                    landed.digest(),
                    () -> LandedItemSource.open(source, itemId, keys),
                    lease);
            ResourceMonitor.Resources resources = monitor.stop();

            assertThat(outcome.result()).as("outcome")
                    .isEqualTo(ItemProcessor.Result.COMPLETED);
            assertThat(outcome.recordCount())
                    .isEqualTo(generated.records());
            assertThat(pipeline.failedRecordCount(outcome.itemId()))
                    .as("records the splitter couldn't parse").isZero();
            assertThat(publisher.count.get())
                    .as("records + item.split")
                    .isEqualTo(generated.records() + 1);

            records.refresh();
            StageResult result = new StageResult(PerfSettings.CORE,
                    "bulk-" + format, "-", 1, generated.bytes(),
                    generated.records(), resources.wallNanos(), null,
                    resources,
                    (double) (totalSize(records) + landedBytes)
                            / generated.bytes());
            results.add(result);
            System.out.println(result.throughputRow());
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
        table.refresh();
        return table.currentSnapshot() == null ? 0
                : Long.parseLong(table.currentSnapshot().summary()
                        .getOrDefault("total-files-size", "0"));
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

    /** Stands in for the broker: acknowledges at once and counts. */
    static final class CountingPublisher implements MessagePublisher
    {

        /** The messages published. */
        final AtomicLong count = new AtomicLong();

        /** Creates the counter. */
        CountingPublisher()
        {
        }

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
