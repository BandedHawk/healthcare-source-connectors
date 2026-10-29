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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.interfaces.ECPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The connector on an object store other than S3 (design §8.1), with a JDBC catalog and Postgres for the lease:
 * ingest, downstream read-back through the store's own FileIO, archive restore, maintenance, and no plaintext
 * anywhere in the bucket. Each subclass supplies the store (an emulator) and its Iceberg FileIO settings.
 */
abstract class ObjectStoreItBase {
    static final String MARKER = "PHI-MARKER-5c1d";
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    @TestConfiguration
    static class Overrides {
        @Bean
        @Primary
        FatalExit recordingFatalExit() {
            return FATAL::add;
        }
    }

    /** Everything one store's test needs outside Spring: inbound directory, keys, Postgres and the configuration. */
    record Fixture(Path inbound, TestKeys keys, PostgreSQLContainer postgres, Map<String, String> catalog,
            String queue) {

        /** {@code storeProps}: the FileIO settings and warehouse; the JDBC catalog settings are added here. */
        static Fixture create(String name, Map<String, String> storeProps) throws Exception {
            Path root = Files.createTempDirectory("sdh-" + name);
            PostgreSQLContainer pg = new PostgreSQLContainer("postgres:18.4");
            pg.start();
            Map<String, String> catalog = new LinkedHashMap<>();
            catalog.put("type", "jdbc");
            catalog.put("uri", pg.getJdbcUrl());
            catalog.put("jdbc.user", pg.getUsername());
            catalog.put("jdbc.password", pg.getPassword());
            catalog.put("jdbc.schema-version", "V1");
            catalog.putAll(storeProps);
            return new Fixture(Files.createDirectories(root.resolve("inbound")),
                    new TestKeys(Files.createDirectories(root.resolve("keys"))), pg, catalog, "sdh-" + name);
        }

        void register(DynamicPropertyRegistry r) {
            Map<String, String> c = new LinkedHashMap<>();
            c.put("sdh.file.inbound-dir", inbound.toString());
            c.put("sdh.file.min-age", "300ms");
            c.put("sdh.file.poll-interval", "200ms");
            c.put("sdh.split.format", "csv");
            c.put("sdh.split.options.csv.header", "true");
            c.put("sdh.storage.catalog-name", "sdh");
            catalog.forEach((k, v) -> c.put("sdh.storage.catalog[" + k + "]", v));
            c.put("sdh.storage.commit.max-sections", "7");
            c.put("sdh.lease.jdbc.url", postgres.getJdbcUrl());
            c.put("sdh.lease.jdbc.username", postgres.getUsername());
            c.put("sdh.lease.jdbc.password", postgres.getPassword());
            c.put("sdh.crypto.keystore.path", keys.keystore.toString());
            c.put("sdh.crypto.keystore.password", TestKeys.KS_PASSWORD);
            c.put("sdh.crypto.keys.sections-kek", TestKeys.SECTIONS);
            c.put("sdh.crypto.keys.source-files-kek", TestKeys.SOURCE);
            c.put("sdh.crypto.keys.failed-files-kek", TestKeys.FAILED);
            c.put("sdh.crypto.keys.signing.kid", "connector-2026");
            c.put("sdh.crypto.keys.signing.pem", keys.signingPem.toString());
            c.put("sdh.crypto.keys.signing.passphrase", TestKeys.SIGNING_PASSPHRASE);
            c.put("sdh.crypto.keys.recipient.kid", "downstream-2026");
            c.put("sdh.crypto.keys.recipient.pem", keys.recipientPem.toString());
            try {
                c.put("sdh.crypto.keys.recipient.sha256", KeyMaterial.fingerprint(keys.recipient.getPublic()));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            c.put("sdh.crypto.keys.recovery-public", keys.recoveryPem.toString());
            c.put("sdh.queue.type", "camel");
            c.put("sdh.queue.uri", "seda:" + queue + "?size=100000");
            c.forEach((k, v) -> r.add(k, () -> v));
        }

        /** A separate catalog client, as a downstream process would have. A JDBC catalog finds tables by name. */
        Catalog newClient() {
            return CatalogUtil.buildIcebergCatalog("sdh", catalog, null);
        }
    }

    @Autowired
    ConnectorRuntime runtime;

    @Autowired
    CamelContext camel;

    @Autowired
    ReentrantLock writerLock;

    abstract Fixture fixture();

    /** Full locations of every object under {@code location}. */
    abstract List<String> list(String location);

    /** Whether the emulator supports what orphan-file cleanup needs (listing through the FileIO). */
    abstract boolean orphanCleanupSupported();

    @Test
    void ingestsReadsBackAndMaintainsTables() throws Exception {
        Fixture f = fixture();
        String header = "id,name,note\n";
        List<String> records = new ArrayList<>();
        StringBuilder file = new StringBuilder(header);
        for (int i = 0; i < 30; i++) {
            String rec = i + "," + MARKER + "-" + i + ",\"multi\nline\"\n";
            records.add(rec);
            file.append(rec);
        }
        Path good = f.inbound().resolve("good.csv");
        Files.writeString(good, file);
        Files.setLastModifiedTime(good, FileTime.from(Instant.now().minusSeconds(30)));
        Path bad = f.inbound().resolve("bad.csv");
        Files.writeString(bad, header + "1," + MARKER + ",\"never closed\n");

        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline && (Files.exists(good) || Files.exists(bad))) {
            Thread.sleep(250);
        }
        assertThat(FATAL).isEmpty();
        assertThat(good).doesNotExist();
        assertThat(bad).doesNotExist();

        List<ConnectorMessage> msgs = new ArrayList<>();
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        String body;
        try (QueueReader reader = QueueReader.builder()
                .recipientKey("downstream-2026", f.keys().recipient.getPrivate())
                .trustedSigner("connector-2026", (ECPublicKey) f.keys().signing.getPublic())
                .catalog(f.newClient())
                .expectedCatalogName("sdh")
                .keyProvider(runtime.keys().wrap())
                .build()) {
            while ((body = consumer.receiveBody("seda:" + f.queue(), 1000, String.class)) != null) {
                msgs.add(reader.decode(body.getBytes(UTF_8)));
            }
            List<SectionStored> sections = msgs.stream().filter(SectionStored.class::isInstance)
                    .map(SectionStored.class::cast).toList();
            assertThat(msgs.stream().filter(FileStarted.class::isInstance)).hasSize(2);
            assertThat(sections).hasSize(30);
            for (SectionStored s : sections) {
                assertThat(new String(reader.readSection(s, 1 << 20), UTF_8))
                        .isEqualTo(ItSupport.csvRowJson(records.get((int) s.seq())));
            }
            FileCompleted done = msgs.stream().filter(FileCompleted.class::isInstance)
                    .map(FileCompleted.class::cast).findFirst().orElseThrow();
            ByteArrayOutputStream restored = new ByteArrayOutputStream();
            ArchiveReader.restore(runtime.sourceFiles(), done.fileId(), runtime.keys().wrap(), runtime.ids(), 0,
                    restored);
            assertThat(restored.toString(UTF_8)).isEqualTo(file.toString());

            // Maintenance, between files: compaction, orphan cleanup where the emulator can list, then retention.
            writerLock.lock();
            try {
                FileIO io = runtime.sections().io();
                String dataPrefix = runtime.sections().location() + "/data/";
                String stray = dataPrefix + "stray-crashed-batch.parquet";
                boolean orphans = orphanCleanupSupported();
                if (orphans) {
                    try (OutputStream out = io.newOutputFile(stray).create()) {
                        out.write("half-written".getBytes(UTF_8));
                    }
                }
                // Object timestamps come from the emulator's clock (the minikube VM's), which can run a little
                // ahead of this JVM's; a minute's lead keeps the just-planted stray old enough to count as an orphan.
                var report = new TableMaintenance(runtime.committer(), new MaintenanceSettings(Duration.ofDays(7), 10,
                        true, 0.75, 2, 8L << 30, Duration.ZERO), Clock.offset(Clock.systemUTC(), Duration.ofMinutes(1)))
                        .run(runtime.sections(), SectionsTable.INGESTED_AT, Optional.empty(), orphans);
                assertThat(report.compactedFiles()).as("30 sections in batches of 7").isEqualTo(5);
                if (orphans) {
                    assertThat(report.orphansDeleted()).isEqualTo(1);
                    assertThat(list(dataPrefix)).doesNotContain(stray);
                }
                for (SectionStored s : sections) {
                    assertThat(new String(reader.readSection(s, 1 << 20), UTF_8))
                            .isEqualTo(ItSupport.csvRowJson(records.get((int) s.seq())));
                }
                var purge = new TableMaintenance(runtime.committer(), MaintenanceSettings.DEFAULT,
                        Clock.offset(Clock.systemUTC(), Duration.ofDays(3)))
                        .run(runtime.sections(), SectionsTable.INGESTED_AT, Optional.of(Duration.ofDays(1)), false);
                assertThat(purge.retentionDeletedFiles()).isEqualTo(1);
                assertThat(list(dataPrefix)).as("PHI data objects physically removed").isEmpty();
            } finally {
                writerLock.unlock();
            }
        }

        // Nothing the connector wrote is readable: data, manifests and metadata alike.
        FileIO io = runtime.sections().io();
        String warehouse = f.catalog().get("warehouse");
        List<String> objects = list(warehouse);
        assertThat(objects).isNotEmpty();
        for (String o : objects) {
            String content;
            try (InputStream in = io.newInputFile(o).newStream()) {
                content = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.ISO_8859_1);
            } catch (IOException e) {
                throw new IOException("reading " + o, e);
            }
            assertThat(content).as(o).doesNotContain(MARKER).doesNotContain("good.csv").doesNotContain("bad.csv");
        }
    }
}
