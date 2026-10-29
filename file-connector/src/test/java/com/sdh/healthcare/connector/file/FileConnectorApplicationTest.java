package com.sdh.healthcare.connector.file;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.core.storage.ArchiveReader;
import com.sdh.healthcare.connector.file.config.KeyMaterial;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.file.runtime.FatalExit;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.interfaces.ECPublicKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The whole application on real components without Docker: embedded Postgres (lease + JDBC Iceberg catalog),
 * in-memory object storage, key files on disk, the Camel file route, and a SEDA queue standing in for the broker.
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
class FileConnectorApplicationTest {
    static final Path ROOT;
    static final Path INBOUND;
    static final TestKeys KEYS;
    static final EmbeddedPostgres PG;

    static {
        try {
            ROOT = Files.createTempDirectory("sdh-app");
            INBOUND = Files.createDirectories(ROOT.resolve("inbound"));
            KEYS = new TestKeys(Files.createDirectories(ROOT.resolve("keys")));
            PG = EmbeddedPostgres.start();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static final List<String> FATAL = new CopyOnWriteArrayList<>();

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
        String jdbc = PG.getJdbcUrl("postgres", "postgres");
        r.add("sdh.file.inbound-dir", INBOUND::toString);
        r.add("sdh.file.min-age", () -> "500ms");
        r.add("sdh.file.poll-interval", () -> "200ms");
        r.add("sdh.split.format", () -> "csv");
        r.add("sdh.storage.catalog-name", () -> "sdh");
        r.add("sdh.storage.catalog.type", () -> "jdbc");
        r.add("sdh.storage.catalog.uri", () -> jdbc);
        r.add("sdh.storage.catalog.warehouse", () -> "memory://wh-" + UUID.randomUUID());
        r.add("sdh.storage.catalog.io-impl", () -> "org.apache.iceberg.inmemory.InMemoryFileIO");
        r.add("sdh.storage.catalog[jdbc.user]", () -> "postgres");
        r.add("sdh.storage.catalog[jdbc.password]", () -> "postgres");
        r.add("sdh.storage.catalog[jdbc.schema-version]", () -> "V1");
        r.add("sdh.storage.commit.max-sections", () -> "5");
        r.add("sdh.maintenance.compaction.min-input-files", () -> "2");
        r.add("sdh.lease.jdbc.url", () -> jdbc);
        r.add("sdh.lease.jdbc.username", () -> "postgres");
        r.add("sdh.lease.jdbc.password", () -> "postgres");
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
        r.add("sdh.queue.type", () -> "camel");
        r.add("sdh.queue.uri", () -> "seda:sdh-out?size=100000");
        r.add("sdh.retry.max-attempts", () -> "2");
        r.add("sdh.retry.initial-backoff", () -> "10ms");
    }

    @Autowired
    CamelContext camel;

    @Autowired
    ConnectorRuntime runtime;

    @Autowired
    com.sdh.healthcare.connector.file.runtime.MaintenanceScheduler maintenance;

    @Autowired
    java.util.concurrent.locks.ReentrantLock writerLock;

    static String csv(int n, String tag) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < n; i++) {
            b.append(i).append(',').append(tag).append("-patient-").append(i).append(",\"note\nline\"\n");
        }
        return b.toString();
    }

    static Path write(String name, String content, int ageSeconds) throws Exception {
        Path p = INBOUND.resolve(name);
        Files.writeString(p, content);
        Files.setLastModifiedTime(p, FileTime.from(Instant.now().minusSeconds(ageSeconds)));
        return p;
    }

    static long count(Path dir) throws Exception {
        if (!Files.isDirectory(dir)) {
            return 0;
        }
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".json")).count();
        }
    }

    @Test
    void everyBrokerComponentResolvesAndJackson2IsPinned() {
        for (String c : new String[] {"kafka", "aws2-sqs", "google-pubsub", "azure-servicebus", "azure-eventhubs",
                "jms", "spring-rabbitmq", "file"}) {
            assertThat(camel.getComponent(c)).as(c).isNotNull();
        }
        assertThat(com.fasterxml.jackson.databind.cfg.PackageVersion.VERSION.toString()).startsWith("2.22");
    }

    @Test
    void ingestsFilesOldestFirstAndFailsABadOne() throws Exception {
        String older = csv(12, "b");
        String newer = csv(3, "a");
        Path b = write("b-smith.csv", older, 60);
        Path a = write("a-jones.csv", newer, 30);
        Path bad = write("c-bad.csv", "1,ok\n2,\"never closed\n", 10);

        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline
                && (Files.exists(a) || Files.exists(b) || Files.exists(bad)
                        || count(INBOUND.resolve("completed")) < 2 || count(INBOUND.resolve("failed")) < 1)) {
            Thread.sleep(200);
        }
        assertThat(FATAL).isEmpty();
        assertThat(a).doesNotExist();
        assertThat(b).doesNotExist();
        assertThat(bad).doesNotExist();
        assertThat(count(INBOUND.resolve("completed"))).isEqualTo(2);
        assertThat(count(INBOUND.resolve("failed"))).isEqualTo(1);

        MessageOpener opener = new MessageOpener(Map.of("downstream-2026", KEYS.recipient.getPrivate()),
                Map.of("connector-2026", (ECPublicKey) KEYS.signing.getPublic()));
        List<ConnectorMessage> msgs = new ArrayList<>();
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        String body;
        while ((body = consumer.receiveBody("seda:sdh-out", 500, String.class)) != null) {
            msgs.add(opener.open(body.getBytes(UTF_8)));
        }
        List<FileStarted> started = msgs.stream().filter(FileStarted.class::isInstance)
                .map(FileStarted.class::cast).toList();
        assertThat(started).extracting(FileStarted::fileSize)
                .containsExactly((long) older.length(), (long) newer.length(), (long) "1,ok\n2,\"never closed\n".length());
        assertThat(msgs.stream().filter(SectionStored.class::isInstance)).hasSize(15);
        List<FileCompleted> completed = msgs.stream().filter(FileCompleted.class::isInstance)
                .map(FileCompleted.class::cast).toList();
        assertThat(completed).extracting(FileCompleted::sectionCount).containsExactly(12L, 3L);

        UUID olderId = started.getFirst().fileId();
        ByteArrayOutputStream restored = new ByteArrayOutputStream();
        ArchiveReader.Restored r = ArchiveReader.restore(runtime.sourceFiles(), olderId, runtime.keys().wrap(),
                runtime.ids(), 0, restored);
        assertThat(restored.toString(UTF_8)).isEqualTo(older);
        assertThat(r.originalName()).contains("b-smith.csv");
        assertThat(r.digest()).isEqualTo(completed.getFirst().fileDigest());

        Path failedRecord = INBOUND.resolve("failed").resolve(started.getLast().fileId() + ".json");
        assertThat(Files.readString(failedRecord)).contains("PARSE_UNTERMINATED_QUOTE", "FAILED_FILES")
                .doesNotContain("bad.csv");
        try (Stream<Path> s = Files.list(INBOUND.resolve("completed"))) {
            assertThat(s.map(FileConnectorApplicationTest::read)).noneMatch(t -> t.contains("smith") || t.contains("jones"));
        }
        assertThat(runtime.processor().inProgress()).isEmpty();

        // Maintenance between files: compacts the small batch files, data still restores byte-for-byte.
        writerLock.lock();
        try {
            var reports = maintenance.runOnce();
            assertThat(reports).hasSize(3);
            assertThat(reports.getFirst().compactedFiles()).isGreaterThanOrEqualTo(2);
        } finally {
            writerLock.unlock();
        }
        ByteArrayOutputStream again = new ByteArrayOutputStream();
        ArchiveReader.restore(runtime.sourceFiles(), olderId, runtime.keys().wrap(), runtime.ids(), 0, again);
        assertThat(again.toString(UTF_8)).isEqualTo(older);
        assertThat(FATAL).isEmpty();
    }

    static String read(Path p) {
        try {
            return Files.readString(p);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
