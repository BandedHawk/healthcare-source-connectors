package com.sdh.healthcare.connector.file.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.table.SectionsTable;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.storage.ArchiveReader;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.config.KeyMaterial;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.file.runtime.FatalExit;
import com.sdh.healthcare.connector.reader.QueueReader;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.interfaces.ECPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
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
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The deployed shape (design §8.1, §10.2): the Spring Boot app with Postgres (lease + JDBC catalog), SeaweedFS
 * through Iceberg's S3FileIO, and Kafka. A downstream consumer then reads every section back with
 * connector-queue-reader, and no stored object may contain plaintext.
 */
@SpringBootTest(properties = "camel.springboot.main-run-controller=false")
class FullStackIT {
    static final String MARKER = "PHI-MARKER-7f3a";
    static final String BUCKET = "sdh-warehouse";
    static final String ACCESS_KEY = "sdh-it-access";
    static final String SECRET_KEY = "sdh-it-secret-key-0123456789";
    /** SeaweedFS rejects signed requests unless S3 identities are configured, as any real deployment would. */
    static final String S3_CONFIG = """
            {"identities":[{"name":"sdh","credentials":[{"accessKey":"%s","secretKey":"%s"}],
              "actions":["Admin","Read","Write","List","Tagging"]}]}""".formatted(ACCESS_KEY, SECRET_KEY);
    static final Path INBOUND;
    static final TestKeys KEYS;
    static final PostgreSQLContainer POSTGRES;
    static final GenericContainer<?> SEAWEED;
    static final KafkaContainer KAFKA;
    /** OpenTelemetry Collector: receives the app's OTLP export and writes it to a file the test reads back. */
    static final GenericContainer<?> COLLECTOR;
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
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    static {
        ItSupport.requireDocker();
        try {
            Path root = Files.createTempDirectory("sdh-fullstack");
            INBOUND = Files.createDirectories(root.resolve("inbound"));
            KEYS = new TestKeys(Files.createDirectories(root.resolve("keys")));
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
        POSTGRES = new PostgreSQLContainer("postgres:18.4");
        SEAWEED = new GenericContainer<>("chrislusf/seaweedfs:4.48")
                .withCopyToContainer(org.testcontainers.images.builder.Transferable.of(S3_CONFIG), "/etc/seaweedfs/s3.json")
                .withCommand("server", "-s3", "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data", "-ip.bind=0.0.0.0",
                        "-volume.max=10")
                .withExposedPorts(8333)
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
        KAFKA = new KafkaContainer("apache/kafka:4.2.2");
        COLLECTOR = new GenericContainer<>("otel/opentelemetry-collector-contrib:0.162.0")
                .withCopyToContainer(org.testcontainers.images.builder.Transferable.of(COLLECTOR_CONFIG),
                        "/etc/otelcol-contrib/config.yaml")
                // The image is distroless (no /tmp): provide the output file, writable by the collector's user.
                .withCopyToContainer(org.testcontainers.images.builder.Transferable.of("", 0666), "/tmp/otel-out.json")
                .withExposedPorts(4318)
                .waitingFor(Wait.forLogMessage(".*Everything is ready.*", 1));
        COLLECTOR.start();
        POSTGRES.start();
        SEAWEED.start();
        KAFKA.start();
    }

    static String s3Endpoint() {
        return "http://" + SEAWEED.getHost() + ":" + SEAWEED.getMappedPort(8333);
    }

    static S3Client s3() {
        return S3Client.builder().endpointOverride(URI.create(s3Endpoint())).region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .build();
    }

    static Map<String, String> catalogProps() {
        Map<String, String> m = new HashMap<>();
        m.put("type", "jdbc");
        m.put("uri", POSTGRES.getJdbcUrl());
        m.put("jdbc.user", POSTGRES.getUsername());
        m.put("jdbc.password", POSTGRES.getPassword());
        m.put("jdbc.schema-version", "V1");
        m.put("warehouse", "s3://" + BUCKET + "/wh");
        m.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        m.put("s3.endpoint", s3Endpoint());
        m.put("s3.path-style-access", "true");
        m.put("s3.access-key-id", ACCESS_KEY);
        m.put("s3.secret-access-key", SECRET_KEY);
        m.put("client.region", "us-east-1");
        return m;
    }

    @TestConfiguration
    static class Overrides {
        @Bean
        @Primary
        FatalExit recordingFatalExit() {
            return FATAL::add;
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) throws Exception {
        try (S3Client s3 = s3()) {
            s3.createBucket(b -> b.bucket(BUCKET));
        }
        r.add("sdh.file.inbound-dir", INBOUND::toString);
        r.add("sdh.file.min-age", () -> "500ms");
        r.add("sdh.file.poll-interval", () -> "200ms");
        r.add("sdh.split.format", () -> "csv");
        r.add("sdh.split.options.csv.header", () -> "true");
        r.add("sdh.storage.catalog-name", () -> "sdh");
        catalogProps().forEach((k, v) -> r.add("sdh.storage.catalog[" + k + "]", () -> v));
        r.add("sdh.storage.commit.max-sections", () -> "7");
        r.add("sdh.lease.jdbc.url", POSTGRES::getJdbcUrl);
        r.add("sdh.lease.jdbc.username", POSTGRES::getUsername);
        r.add("sdh.lease.jdbc.password", POSTGRES::getPassword);
        r.add("sdh.crypto.keystore.path", KEYS.keystore::toString);
        r.add("sdh.crypto.keystore.password", () -> TestKeys.KS_PASSWORD);
        r.add("sdh.crypto.keys.sections-kek", () -> TestKeys.SECTIONS);
        r.add("sdh.crypto.keys.source-files-kek", () -> TestKeys.SOURCE);
        r.add("sdh.crypto.keys.failed-files-kek", () -> TestKeys.FAILED);
        r.add("sdh.crypto.keys.signing.kid", () -> "connector-2026");
        r.add("sdh.crypto.keys.signing.pem", KEYS.signingPem::toString);
        r.add("sdh.crypto.keys.signing.passphrase", () -> TestKeys.SIGNING_PASSPHRASE);
        r.add("sdh.crypto.keys.recipient.kid", () -> "downstream-2026");
        r.add("sdh.crypto.keys.recipient.pem", KEYS.recipientPem::toString);
        String pin = KeyMaterial.fingerprint(KEYS.recipient.getPublic());
        r.add("sdh.crypto.keys.recipient.sha256", () -> pin);
        r.add("sdh.crypto.keys.recovery-public", KEYS.recoveryPem::toString);
        r.add("sdh.telemetry.otlp.endpoint",
                () -> "http://" + COLLECTOR.getHost() + ":" + COLLECTOR.getMappedPort(4318));
        r.add("sdh.telemetry.metric-interval", () -> "1s");
        r.add("sdh.queue.type", () -> "kafka");
        r.add("sdh.queue.uri", () -> "kafka:sdh-sections?brokers=" + KAFKA.getBootstrapServers());
    }

    @Autowired
    ConnectorRuntime runtime;

    @Autowired
    ReentrantLock writerLock;

    /** "s3://bucket/a/b" → "a/b". */
    static String keyOf(String location) {
        String noScheme = location.substring(location.indexOf("://") + 3);
        return noScheme.substring(noScheme.indexOf('/') + 1);
    }

    static List<String> keys(String prefix) {
        try (S3Client s3 = s3()) {
            return s3.listObjectsV2Paginator(b -> b.bucket(BUCKET).prefix(prefix)).contents().stream()
                    .map(S3Object::key).toList();
        }
    }

    @Test
    void ingestsToS3AndDownstreamReadsEverySectionBack() throws Exception {
        String header = "id,name,note\n";
        List<String> records = new ArrayList<>();
        StringBuilder file = new StringBuilder(header);
        for (int i = 0; i < 30; i++) {
            String rec = i + "," + MARKER + "-" + i + ",\"multi\nline\"\n";
            records.add(rec);
            file.append(rec);
        }
        Path good = INBOUND.resolve("good.csv");
        Files.writeString(good, file);
        Files.setLastModifiedTime(good, FileTime.from(Instant.now().minusSeconds(30)));
        Path bad = INBOUND.resolve("bad.csv");
        Files.writeString(bad, header + "1," + MARKER + ",\"never closed\n");

        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline && (Files.exists(good) || Files.exists(bad))) {
            Thread.sleep(250);
        }
        assertThat(FATAL).isEmpty();
        assertThat(good).doesNotExist();
        assertThat(bad).doesNotExist();

        // Downstream: Kafka → connector-queue-reader → sections from S3.
        List<ConnectorMessage> msgs = new ArrayList<>();
        Catalog catalog = CatalogUtil.buildIcebergCatalog("sdh", catalogProps(), null);
        try (QueueReader reader = QueueReader.builder()
                .recipientKey("downstream-2026", KEYS.recipient.getPrivate())
                .trustedSigner("connector-2026", (ECPublicKey) KEYS.signing.getPublic())
                .catalog(catalog)
                .expectedCatalogName("sdh")
                .keyProvider(runtime.keys().wrap())
                .build();
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                        "bootstrap.servers", KAFKA.getBootstrapServers(), "group.id", "downstream",
                        "auto.offset.reset", "earliest"), new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("sdh-sections"));
            // 2 × file.started, 30 × section.stored, 1 × file.completed (the bad file stores nothing).
            long until = System.currentTimeMillis() + 60_000;
            while (msgs.size() < 33 && System.currentTimeMillis() < until) {
                for (var rec : consumer.poll(Duration.ofMillis(500))) {
                    msgs.add(reader.decode(rec.value().getBytes(UTF_8)));
                }
            }
            List<SectionStored> sections = msgs.stream().filter(SectionStored.class::isInstance)
                    .map(SectionStored.class::cast).toList();
            assertThat(msgs.stream().filter(FileStarted.class::isInstance)).hasSize(2);
            assertThat(sections).hasSize(30);
            for (SectionStored s : sections) {
                assertThat(new String(reader.readSection(s, 1 << 20), UTF_8))
                        .isEqualTo(header + records.get((int) s.seq()));
            }
            FileCompleted done = msgs.stream().filter(FileCompleted.class::isInstance)
                    .map(FileCompleted.class::cast).findFirst().orElseThrow();
            ByteArrayOutputStream restored = new ByteArrayOutputStream();
            ArchiveReader.restore(runtime.sourceFiles(), done.fileId(), runtime.keys().wrap(), runtime.ids(), 0,
                    restored);
            assertThat(restored.toString(UTF_8)).isEqualTo(file.toString());

            // Maintenance on real S3, between files (route idle, writer lock held).
            writerLock.lock();
            try {
                String dataPrefix = keyOf(runtime.sections().location()) + "/data/";
                String stray = dataPrefix + "stray-crashed-batch.parquet";
                try (S3Client s3 = s3()) {
                    s3.putObject(b -> b.bucket(BUCKET).key(stray),
                            software.amazon.awssdk.core.sync.RequestBody.fromString("half-written"));
                }
                var report = new TableMaintenance(runtime.committer(), new MaintenanceSettings(Duration.ofDays(7), 10,
                        true, 0.75, 2, 8L << 30, Duration.ZERO), Clock.systemUTC())
                        .run(runtime.sections(), SectionsTable.INGESTED_AT, Optional.empty(), true);
                assertThat(report.compactedFiles()).as("30 sections in batches of 7").isEqualTo(5);
                assertThat(report.orphansDeleted()).isEqualTo(1);
                assertThat(keys(dataPrefix)).doesNotContain(stray);
                // Downstream still reads every section after compaction.
                for (SectionStored s : sections) {
                    assertThat(new String(reader.readSection(s, 1 << 20), UTF_8))
                            .isEqualTo(header + records.get((int) s.seq()));
                }

                // Retention: three days on, a one-day policy drops today's partition and purges its objects at once.
                var purge = new TableMaintenance(runtime.committer(), MaintenanceSettings.DEFAULT,
                        Clock.offset(Clock.systemUTC(), Duration.ofDays(3)))
                        .run(runtime.sections(), SectionsTable.INGESTED_AT, Optional.of(Duration.ofDays(1)), false);
                assertThat(purge.retentionDeletedFiles()).isEqualTo(1);
                assertThat(keys(dataPrefix)).as("PHI data objects physically removed").isEmpty();
            } finally {
                writerLock.unlock();
            }
        }

        // Nothing in object storage is readable: data files, manifests and metadata alike.
        try (S3Client s3 = s3()) {
            List<S3Object> objects = s3.listObjectsV2Paginator(b -> b.bucket(BUCKET)).contents().stream().toList();
            assertThat(objects).isNotEmpty();
            for (S3Object o : objects) {
                String content = new String(s3.getObjectAsBytes(b -> b.bucket(BUCKET).key(o.key())).asByteArray(),
                        java.nio.charset.StandardCharsets.ISO_8859_1);
                assertThat(content).as(o.key()).doesNotContain(MARKER).doesNotContain("good.csv")
                        .doesNotContain("bad.csv");
            }
        }

        // Telemetry reached the collector over OTLP: traces, metrics and audit events, and none of it holds PHI.
        String exported = "";
        long until = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < until) {
            try {
                exported = COLLECTOR.copyFileFromContainer("/tmp/otel-out.json",
                        in -> new String(in.readAllBytes(), UTF_8));
            } catch (Exception notYet) {
                exported = "";
            }
            // Spans are exported in 5 s batches, logs every second, metrics every second here: wait for the last of each.
            if (exported.contains("\"maintenance\"") && exported.contains("sdh.audit.maintenance.retention")
                    && exported.contains("sdh.files.failed")) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(exported).contains("\"ingest.file\"", "\"split.batch\"", "\"storage.commit\"",
                "\"publish.batch\"", "\"archive\"", "\"maintenance\"",
                "sdh.audit.file.started", "sdh.audit.batch.committed", "sdh.audit.file.completed",
                "sdh.audit.file.failed", "sdh.audit.lease.acquired", "sdh.audit.maintenance.retention",
                "sdh.sections.stored", "sdh.files.completed", "sdh.files.failed", "sdh.commit.duration");
        assertThat(exported).as("no PHI or filenames in exported telemetry")
                .doesNotContain(MARKER).doesNotContain("good.csv").doesNotContain("bad.csv");
    }
}
