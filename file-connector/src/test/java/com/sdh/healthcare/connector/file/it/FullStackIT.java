/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.INTAKE;
import static com.sdh.healthcare.connector.file.it.ItSupport.awaitSplit;
import static com.sdh.healthcare.connector.file.it.ItSupport.csvRowJson;
import static com.sdh.healthcare.connector.file.it.ItSupport.requireDocker;
import static com.sdh.healthcare.connector.file.it.ItSupport.splitterKeys;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordFailed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance.Report;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.TestLanding;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.ECPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.camel.CamelContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The deployed shape (design §8.1, §10.2): the Spring Boot app with
 * Postgres (lease + JDBC catalog), SeaweedFS through Iceberg's S3FileIO,
 * and Kafka. A downstream consumer then reads every record back with
 * connector-queue-reader, and no stored object may contain plaintext.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
class FullStackIT
{

    /** A marker in every record, which must never appear in storage. */
    static final String MARKER = "PHI-MARKER-7f3a";

    /** The bucket holding the warehouse. */
    static final String BUCKET = "sdh-warehouse";

    /** The S3 access key. */
    static final String ACCESS_KEY = "sdh-it-access";

    /** The S3 secret key. */
    static final String SECRET_KEY = "sdh-it-secret-key-0123456789";

    /**
     * SeaweedFS rejects signed requests unless S3 identities are
     * configured, as any real deployment would.
     */
    static final String S3_CONFIG = """
            {"identities":[{"name":"sdh","credentials":\
            [{"accessKey":"%s","secretKey":"%s"}],
              "actions":["Admin","Read","Write","List","Tagging"]}]}"""
            .formatted(ACCESS_KEY, SECRET_KEY);

    /** The key files. */
    static final TestKeys KEYS;

    /** The lease database and catalog. */
    static final PostgreSQLContainer POSTGRES;

    /** The object store. */
    static final GenericContainer<?> SEAWEED;

    /** The broker. */
    static final KafkaContainer KAFKA;

    /**
     * OpenTelemetry Collector: receives the app's OTLP export and writes it
     * to a file the test reads back.
     */
    static final GenericContainer<?> COLLECTOR;

    /** The collector's configuration: everything OTLP to one file. */
    static final String COLLECTOR_CONFIG = """
            receivers:
              otlp:
                protocols:
                  http:
                    endpoint: 0.0.0.0:4318
            exporters:
              file:
                path: /tmp/otel-out.json
            service:
              pipelines:
                traces: {receivers: [otlp], exporters: [file]}
                metrics: {receivers: [otlp], exporters: [file]}
                logs: {receivers: [otlp], exporters: [file]}
            """;

    /** The reasons the app asked to exit, if any. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /** The in-memory queue landed items are announced on. */
    static final String LANDED_QUEUE = "seda:sdh-fullstack-landed?size=1000";

    static
    {
        requireDocker();
        try
        {
            Path root = Files.createTempDirectory("sdh-fullstack");
            KEYS = new TestKeys(Files.createDirectories(root.resolve("keys")));
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
        POSTGRES = new PostgreSQLContainer("postgres:18.4");
        SEAWEED = new GenericContainer<>("chrislusf/seaweedfs:4.48")
                .withCopyToContainer(Transferable.of(S3_CONFIG),
                        "/etc/seaweedfs/s3.json")
                .withCommand("server", "-s3",
                        "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data",
                        "-ip.bind=0.0.0.0", "-volume.max=10")
                .withExposedPorts(8333)
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(Duration.ofMinutes(2)));
        KAFKA = new KafkaContainer("apache/kafka:4.2.2");
        COLLECTOR = new GenericContainer<>(
                "otel/opentelemetry-collector-contrib:0.162.0")
                .withCopyToContainer(Transferable.of(COLLECTOR_CONFIG),
                        "/etc/otelcol-contrib/config.yaml")
                // The image is distroless (no /tmp): provide the output
                // file, writable by the collector's user.
                .withCopyToContainer(Transferable.of("", 0666),
                        "/tmp/otel-out.json")
                .withExposedPorts(4318)
                .waitingFor(Wait.forLogMessage(".*Everything is ready.*", 1));
        COLLECTOR.start();
        POSTGRES.start();
        SEAWEED.start();
        KAFKA.start();
    }

    /** The running connector. */
    @Autowired
    ConnectorRuntime runtime;

    /** The lock the writer holds while it writes. */
    @Autowired
    ReentrantLock writerLock;

    /** The connector's settings. */
    @Autowired
    ConnectorProperties connectorProperties;

    /** The app's Camel context. */
    @Autowired
    CamelContext camel;

    /**
     * The object store's endpoint as this JVM reaches it.
     *
     * @return the URL
     */
    static String s3Endpoint()
    {
        return "http://" + SEAWEED.getHost() + ":"
                + SEAWEED.getMappedPort(8333);
    }

    /**
     * An S3 client for the object store.
     *
     * @return the client
     */
    static S3Client s3()
    {
        return S3Client.builder().endpointOverride(URI.create(s3Endpoint()))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .build();
    }

    /**
     * The JDBC catalog's properties.
     *
     * @return the properties
     */
    static Map<String, String> catalogProperties()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "jdbc");
        properties.put("uri", POSTGRES.getJdbcUrl());
        properties.put("jdbc.user", POSTGRES.getUsername());
        properties.put("jdbc.password", POSTGRES.getPassword());
        properties.put("jdbc.schema-version", "V1");
        properties.put("warehouse", "s3://" + BUCKET + "/wh");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.endpoint", s3Endpoint());
        properties.put("s3.path-style-access", "true");
        properties.put("s3.access-key-id", ACCESS_KEY);
        properties.put("s3.secret-access-key", SECRET_KEY);
        properties.put("client.region", "us-east-1");
        return properties;
    }

    /**
     * Beans replaced for the test.
     */
    @TestConfiguration
    static class Overrides
    {

        /**
         * Records fatal exits instead of exiting.
         *
         * @return the recording exit
         */
        @Bean
        @Primary
        FatalExit recordingFatalExit()
        {
            return FATAL::add;
        }
    }

    /**
     * Creates the bucket, and points the app at the containers.
     *
     * @param registry receives the properties
     * @throws Exception if the bucket can't be created or a key read
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception
    {
        try (S3Client s3 = s3())
        {
            s3.createBucket(request -> request.bucket(BUCKET));
        }
        registry.add("sdh.landing.intake", () -> INTAKE);
        registry.add("sdh.landing.queue-uri", () -> LANDED_QUEUE);
        registry.add("sdh.landing.idle-wait", () -> "200ms");
        registry.add("sdh.split.format", () -> "csv");
        registry.add("sdh.split.options.csv.header", () -> "true");
        registry.add("sdh.storage.catalog-name", () -> "sdh");
        catalogProperties().forEach((key, value) -> registry.add(
                "sdh.storage.catalog[" + key + "]", () -> value));
        registry.add("sdh.storage.commit.max-records", () -> "7");
        registry.add("sdh.lease.jdbc.url", POSTGRES::getJdbcUrl);
        registry.add("sdh.lease.jdbc.username", POSTGRES::getUsername);
        registry.add("sdh.lease.jdbc.password", POSTGRES::getPassword);
        registry.add("sdh.crypto.keystore.path", KEYS.keystore::toString);
        registry.add("sdh.crypto.keystore.password",
                () -> TestKeys.KS_PASSWORD);
        registry.add("sdh.crypto.keys.records-kek", () -> TestKeys.RECORDS);
        registry.add("sdh.crypto.keys.landed-items-kek",
                () -> TestKeys.LANDED);
        registry.add("sdh.crypto.keys.signing.kid", () -> "connector-2026");
        registry.add("sdh.crypto.keys.signing.pem", KEYS.signingPem::toString);
        registry.add("sdh.crypto.keys.signing.passphrase",
                () -> TestKeys.SIGNING_PASSPHRASE);
        registry.add("sdh.crypto.keys.recipient.kid", () -> "downstream-2026");
        registry.add("sdh.crypto.keys.recipient.pem",
                KEYS.recipientPem::toString);
        String pin = KeyMaterial.fingerprint(KEYS.recipient.getPublic());
        registry.add("sdh.crypto.keys.recipient.sha256", () -> pin);
        splitterKeys(KEYS).forEach((key, value) -> registry.add(key,
                () -> value));
        registry.add("sdh.telemetry.otlp.endpoint",
                () -> "http://" + COLLECTOR.getHost() + ":"
                        + COLLECTOR.getMappedPort(4318));
        registry.add("sdh.telemetry.metric-interval", () -> "1s");
        registry.add("sdh.queue.type", () -> "kafka");
        registry.add("sdh.queue.uri", () -> "kafka:sdh-records?brokers="
                + KAFKA.getBootstrapServers());
    }

    /**
     * An object's key from its location: "s3://bucket/a/b" → "a/b".
     *
     * @param location the object's location
     * @return its key
     */
    static String keyOf(String location)
    {
        String noScheme = location.substring(location.indexOf("://") + 3);
        return noScheme.substring(noScheme.indexOf('/') + 1);
    }

    /**
     * The keys of every object under a prefix.
     *
     * @param prefix the prefix
     * @return the keys
     */
    static List<String> keys(String prefix)
    {
        try (S3Client s3 = s3())
        {
            return s3.listObjectsV2Paginator(request -> request.bucket(BUCKET)
                            .prefix(prefix))
                    .contents().stream()
                    .map(S3Object::key).toList();
        }
    }

    /**
     * Two files are landed and split; downstream reads every record back,
     * maintenance compacts and purges on real S3, nothing stored is
     * readable, and telemetry reaches the collector without PHI.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void ingestsToS3AndDownstreamReadsEveryRecordBack() throws Exception
    {
        String header = "id,name,note\n";
        List<String> rows = new ArrayList<>();
        StringBuilder file = new StringBuilder(header);
        for (int index = 0; index < 30; index++)
        {
            String row = index + "," + MARKER + "-" + index
                    + ",\"multi\nline\"\n";
            rows.add(row);
            file.append(row);
        }
        // One record with a missing field: that record fails conversion and
        // is stored in its original form, while the file still completes
        // (design §6.4).
        String shortRow = "30," + MARKER + "-short\n";
        file.append(shortRow);
        TestLanding landing = new TestLanding(catalogProperties(),
                runtime.keys().wrap(), KEYS, INTAKE);
        LandedItem good = landing.land(file.toString(), "good.csv");
        LandedItem bad = landing.land(
                header + "1," + MARKER + ",\"never closed\n", "bad.csv");
        landing.announce(camel, LANDED_QUEUE, good);
        landing.announce(camel, LANDED_QUEUE, bad);
        awaitSplit(runtime, 2, Duration.ofMinutes(2));
        assertThat(FATAL).isEmpty();

        // Downstream: Kafka → connector-queue-reader → records from S3.
        List<ConnectorMessage> messages = new ArrayList<>();
        Catalog catalog = CatalogUtil.buildIcebergCatalog("sdh",
                catalogProperties(), null);
        try (QueueReader reader = QueueReader.builder()
                .recipientKey("downstream-2026", KEYS.recipient.getPrivate())
                .trustedSigner("connector-2026",
                        (ECPublicKey) KEYS.signing.getPublic())
                .catalog(catalog)
                .expectedCatalogName("sdh")
                .keyProvider(runtime.keys().wrap())
                .build();
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                        Map.of("bootstrap.servers",
                                KAFKA.getBootstrapServers(),
                                "group.id", "downstream",
                                "auto.offset.reset", "earliest"),
                        new StringDeserializer(), new StringDeserializer()))
        {
            consumer.subscribe(List.of("sdh-records"));
            // 2 × file.started, 30 × record.stored, 1 × record.failed,
            // 1 × file.completed (the bad file stores nothing).
            long until = System.currentTimeMillis() + 60_000;
            while (messages.size() < 34
                    && System.currentTimeMillis() < until)
            {
                for (ConsumerRecord<String, String> consumed :
                        consumer.poll(Duration.ofMillis(500)))
                {
                    messages.add(
                            reader.decode(consumed.value().getBytes(UTF_8)));
                }
            }
            List<RecordStored> records = messages.stream()
                    .filter(RecordStored.class::isInstance)
                    .map(RecordStored.class::cast).toList();
            assertThat(messages.stream().filter(FileStarted.class::isInstance))
                    .hasSize(2);
            assertThat(records).hasSize(30);
            for (RecordStored record : records)
            {
                assertThat(new String(reader.readRecord(record, 1 << 20),
                        UTF_8)).isEqualTo(csvRowJson(
                                rows.get((int) record.seq())));
            }
            assertThat(records).allSatisfy(record ->
                    assertThat(record.mapping()).isEqualTo("csv-row/1"));
            RecordFailed failed = messages.stream()
                    .filter(RecordFailed.class::isInstance)
                    .map(RecordFailed.class::cast).findFirst().orElseThrow();
            assertThat(failed.errorCode()).isEqualTo("CONVERT_CSV_FIELD_COUNT");
            assertThat(failed.seq()).isEqualTo(30);
            assertThat(failed.sourceLength()).isEqualTo(shortRow.length());
            assertThat(new String(reader.readRecord(failed, 1 << 20), UTF_8))
                    .isEqualTo(header + shortRow);
            FileCompleted done = messages.stream()
                    .filter(FileCompleted.class::isInstance)
                    .map(FileCompleted.class::cast).findFirst().orElseThrow();
            assertThat(done.recordCount()).isEqualTo(31);
            assertThat(done.failedRecordCount()).isEqualTo(1);
            assertThat(new String(landing.restore(done.itemId()), UTF_8))
                    .isEqualTo(file.toString());

            // Maintenance on real S3, between files (route idle, writer lock
            // held).
            writerLock.lock();
            try
            {
                String dataPrefix =
                        keyOf(runtime.tables().records().location())
                                + "/data/";
                String stray = dataPrefix + "stray-crashed-batch.parquet";
                try (S3Client s3 = s3())
                {
                    s3.putObject(request -> request.bucket(BUCKET).key(stray),
                            RequestBody.fromString("half-written"));
                }
                // Object timestamps come from the storage server's clock (the
                // minikube VM's), which can run a little ahead of this JVM's;
                // a minute's lead keeps the just-planted stray old enough to
                // count as an orphan.
                Report report = new TableMaintenance(runtime.committer(),
                        new MaintenanceSettings(Duration.ofDays(7), 10, true,
                                0.75, 2, 8L << 30, Duration.ZERO),
                        Clock.offset(Clock.systemUTC(), Duration.ofMinutes(1)))
                        .run(runtime.tables().records(),
                                RecordsTable.INGESTED_AT, Optional.empty(),
                                true);
                assertThat(report.compactedFiles())
                        .as("31 records in batches of 7").isEqualTo(5);
                assertThat(report.orphansDeleted()).isEqualTo(1);
                assertThat(keys(dataPrefix)).doesNotContain(stray);
                // Downstream still reads every record after compaction.
                for (RecordStored record : records)
                {
                    assertThat(new String(reader.readRecord(record,
                            1 << 20), UTF_8)).isEqualTo(csvRowJson(
                                    rows.get((int) record.seq())));
                }

                // Retention: three days on, a one-day policy drops today's
                // partition and purges its objects at once.
                Report purge = new TableMaintenance(runtime.committer(),
                        MaintenanceSettings.DEFAULT,
                        Clock.offset(Clock.systemUTC(), Duration.ofDays(3)))
                        .run(runtime.tables().records(),
                                RecordsTable.INGESTED_AT,
                                Optional.of(Duration.ofDays(1)), false);
                assertThat(purge.retentionDeletedFiles()).isEqualTo(1);
                assertThat(keys(dataPrefix))
                        .as("PHI data objects physically removed").isEmpty();
            }
            finally
            {
                writerLock.unlock();
            }
        }

        assertThat(FATAL).isEmpty();

        // Nothing in object storage is readable: data files, manifests and
        // metadata alike.
        try (S3Client s3 = s3())
        {
            List<S3Object> objects = s3.listObjectsV2Paginator(
                    request -> request.bucket(BUCKET)).contents().stream()
                    .toList();
            assertThat(objects).isNotEmpty();
            for (S3Object object : objects)
            {
                String content = new String(s3.getObjectAsBytes(request ->
                        request.bucket(BUCKET).key(object.key()))
                        .asByteArray(), ISO_8859_1);
                assertThat(content).as(object.key()).doesNotContain(MARKER)
                        .doesNotContain("good.csv")
                        .doesNotContain("bad.csv");
            }
        }

        // Telemetry reached the collector over OTLP: traces, metrics and
        // audit events, and none of it holds PHI.
        String exported = "";
        long until = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < until)
        {
            try
            {
                exported = COLLECTOR.copyFileFromContainer(
                        "/tmp/otel-out.json",
                        in -> new String(in.readAllBytes(), UTF_8));
            }
            catch (Exception notYet)
            {
                exported = "";
            }
            // Spans are exported in 5 s batches, logs every second, metrics
            // every second here: wait for the last of each.
            if (exported.contains("\"maintenance\"")
                    && exported.contains("sdh.audit.maintenance.retention")
                    && exported.contains("sdh.items.failed"))
            {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(exported).contains("\"ingest.item\"", "\"split.batch\"",
                "\"storage.commit\"", "\"publish.batch\"", "\"maintenance\"",
                "sdh.audit.inbox.added", "sdh.audit.file.started",
                "sdh.audit.batch.committed", "sdh.audit.file.completed",
                "sdh.audit.file.failed", "sdh.audit.lease.acquired",
                "sdh.audit.maintenance.retention",
                "sdh.records.stored", "sdh.items.completed",
                "sdh.items.failed", "sdh.commit.duration");
        assertThat(exported).as("no PHI or filenames in exported telemetry")
                .doesNotContain(MARKER).doesNotContain("good.csv")
                .doesNotContain("bad.csv");
    }
}
