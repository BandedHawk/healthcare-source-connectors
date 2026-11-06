/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.TestLanding;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.ECPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * The splitter on an object store other than S3 (design §8.1), with a
 * JDBC catalog and Postgres for the lease: items landed in the same store
 * are split, read back downstream through the store's own FileIO, restored
 * from landing, maintained, and no plaintext is anywhere in the bucket.
 * The test stands in for the landing node. Each subclass supplies the
 * store (an emulator) and its Iceberg FileIO settings.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
abstract class ObjectStoreItBase
{

    /** A marker in every record, which must never appear in clear. */
    static final String MARKER = "PHI-MARKER-5c1d";

    /** Fatal errors the connector reported; there should be none. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /** Records fatal errors instead of exiting the test JVM. */
    @TestConfiguration
    static class Overrides
    {

        /**
         * The recording exit hook.
         *
         * @return the hook
         */
        @Bean
        @Primary
        FatalExit recordingFatalExit()
        {
            return FATAL::add;
        }
    }

    /**
     * Everything one store's test needs outside Spring: keys, Postgres and
     * the configuration.
     *
     * @param keys     the key files
     * @param postgres the lease database and catalog
     * @param catalog  the Iceberg catalog's properties
     * @param queue    the base name of the test's queues
     */
    record Fixture(TestKeys keys, PostgreSQLContainer postgres,
            Map<String, String> catalog, String queue)
    {

        /**
         * Starts Postgres and writes the keys.
         *
         * @param name            the store's name, for directories and queues
         * @param storeProperties the FileIO settings and warehouse; the
         *        JDBC catalog settings are added here
         * @return the fixture
         * @throws Exception if Postgres can't start or the keys be written
         */
        static Fixture create(String name, Map<String, String> storeProperties)
                throws Exception
        {
            Path root = Files.createTempDirectory("sdh-" + name);
            PostgreSQLContainer postgres =
                    new PostgreSQLContainer("postgres:18.4");
            postgres.start();
            Map<String, String> catalog = new LinkedHashMap<>();
            catalog.put("type", "jdbc");
            catalog.put("uri", postgres.getJdbcUrl());
            catalog.put("jdbc.user", postgres.getUsername());
            catalog.put("jdbc.password", postgres.getPassword());
            catalog.put("jdbc.schema-version", "V1");
            catalog.putAll(storeProperties);
            return new Fixture(new TestKeys(
                    Files.createDirectories(root.resolve("keys"))), postgres,
                    catalog, "sdh-" + name);
        }

        /**
         * Gives the connector its settings.
         *
         * @param registry the Spring property registry
         */
        void register(DynamicPropertyRegistry registry)
        {
            Map<String, String> settings = new LinkedHashMap<>();
            settings.put("sdh.landing.intake", ItSupport.INTAKE);
            settings.put("sdh.landing.queue-uri", landedQueue());
            settings.put("sdh.landing.idle-wait", "200ms");
            settings.put("sdh.split.format", "csv");
            settings.put("sdh.split.options.csv.header", "true");
            settings.put("sdh.storage.catalog-name", "sdh");
            catalog.forEach((key, value) -> settings.put(
                    "sdh.storage.catalog[" + key + "]", value));
            settings.put("sdh.storage.commit.max-records", "7");
            settings.put("sdh.lease.jdbc.url", postgres.getJdbcUrl());
            settings.put("sdh.lease.jdbc.username", postgres.getUsername());
            settings.put("sdh.lease.jdbc.password", postgres.getPassword());
            settings.put("sdh.crypto.keystore.path", keys.keystore.toString());
            settings.put("sdh.crypto.keystore.password",
                    TestKeys.KS_PASSWORD);
            settings.put("sdh.crypto.keys.records-kek", TestKeys.RECORDS);
            settings.put("sdh.crypto.keys.landed-items-kek", TestKeys.LANDED);
            settings.put("sdh.crypto.keys.signing.kid", "connector-2026");
            settings.put("sdh.crypto.keys.signing.pem",
                    keys.signingPem.toString());
            settings.put("sdh.crypto.keys.signing.passphrase",
                    TestKeys.SIGNING_PASSPHRASE);
            settings.put("sdh.crypto.keys.recipient.kid", "downstream-2026");
            settings.put("sdh.crypto.keys.recipient.pem",
                    keys.recipientPem.toString());
            try
            {
                settings.put("sdh.crypto.keys.recipient.sha256",
                        KeyMaterial.fingerprint(keys.recipient.getPublic()));
            }
            catch (Exception failure)
            {
                throw new IllegalStateException(failure);
            }
            ItSupport.splitterKeys(keys).forEach(settings::put);
            settings.put("sdh.queue.type", "camel");
            settings.put("sdh.queue.uri", "seda:" + queue + "?size=100000");
            settings.forEach((key, value) -> registry.add(key, () -> value));
        }

        /**
         * Where the splitter takes {@code item.landed} messages from.
         *
         * @return the queue URI
         */
        String landedQueue()
        {
            return "seda:" + queue + "-landed?size=1000";
        }

        /**
         * A separate catalog client, as a downstream process would have. A
         * JDBC catalog finds tables by name.
         *
         * @return the catalog
         */
        Catalog newClient()
        {
            return CatalogUtil.buildIcebergCatalog("sdh", catalog, null);
        }
    }

    /** The running connector. */
    @Autowired
    ConnectorRuntime runtime;

    /** The connector's Camel context. */
    @Autowired
    CamelContext camel;


    /**
     * The store's fixture.
     *
     * @return the fixture
     */
    abstract Fixture fixture();

    /**
     * Full locations of every object under a location.
     *
     * @param location the location
     * @return the objects' locations
     */
    abstract List<String> list(String location);

    /**
     * Whether the emulator supports what orphan-file cleanup needs
     * (listing through the FileIO).
     *
     * @return true if it does
     */
    abstract boolean orphanCleanupSupported();

    /**
     * Items are split, read back downstream, restored and maintained, and
     * nothing in the bucket is readable.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void ingestsReadsBackAndMaintainsTables() throws Exception
    {
        Fixture fixture = fixture();
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
        TestLanding landing = new TestLanding(fixture.catalog(),
                runtime.keys().wrap(), fixture.keys(), ItSupport.INTAKE);
        LandedItem good = landing.land(file.toString(), "good.csv");
        LandedItem bad = landing.land(header + "1," + MARKER
                + ",\"never closed\n", "bad.csv");
        landing.announce(camel, fixture.landedQueue(), good);
        landing.announce(camel, fixture.landedQueue(), bad);
        ItSupport.awaitSplit(runtime, 2, Duration.ofMinutes(2));
        assertThat(FATAL).isEmpty();

        List<ConnectorMessage> messages = new ArrayList<>();
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        String body;
        try (QueueReader reader = QueueReader.builder()
                .recipientKey("downstream-2026",
                        fixture.keys().recipient.getPrivate())
                .trustedSigner("connector-2026",
                        (ECPublicKey) fixture.keys().signing.getPublic())
                .catalog(fixture.newClient())
                .expectedCatalogName("sdh")
                .keyProvider(runtime.keys().wrap())
                .build())
        {
            while ((body = consumer.receiveBody("seda:" + fixture.queue(),
                    1000, String.class)) != null)
            {
                messages.add(reader.decode(body.getBytes(UTF_8)));
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
                        UTF_8)).isEqualTo(ItSupport.csvRowJson(
                                rows.get((int) record.seq())));
            }
            FileCompleted done = messages.stream()
                    .filter(FileCompleted.class::isInstance)
                    .map(FileCompleted.class::cast).findFirst().orElseThrow();
            assertThat(new String(landing.restore(done.itemId()), UTF_8))
                    .isEqualTo(file.toString());

            // Maintenance, between files: compaction, orphan cleanup where
            // the emulator can list, then retention.
            {
                FileIO io = runtime.tables().splitRecords().io();
                String dataPrefix =
                        runtime.tables().splitRecords().location() + "/data/";
                String stray = dataPrefix + "stray-crashed-batch.parquet";
                boolean orphans = orphanCleanupSupported();
                if (orphans)
                {
                    try (OutputStream out = io.newOutputFile(stray).create())
                    {
                        out.write("half-written".getBytes(UTF_8));
                    }
                }
                // Object timestamps come from the emulator's clock (the
                // minikube VM's), which can run a little ahead of this
                // JVM's; a minute's lead keeps the just-planted stray old
                // enough to count as an orphan.
                TableMaintenance.Report report = new TableMaintenance(
                        ItSupport.committer(),
                        new MaintenanceSettings(Duration.ofDays(7), 10, true,
                                0.75, 2, 8L << 30, Duration.ZERO),
                        Clock.offset(Clock.systemUTC(), Duration.ofMinutes(1)))
                        .run(runtime.tables().splitRecords(),
                                RecordsTable.INGESTED_AT, Optional.empty(),
                                orphans);
                assertThat(report.compactedFiles())
                        .as("30 records in batches of 7").isEqualTo(5);
                if (orphans)
                {
                    assertThat(report.orphansDeleted()).isEqualTo(1);
                    assertThat(list(dataPrefix)).doesNotContain(stray);
                }
                for (RecordStored record : records)
                {
                    assertThat(new String(reader.readRecord(record,
                            1 << 20), UTF_8)).isEqualTo(ItSupport.csvRowJson(
                                    rows.get((int) record.seq())));
                }
                TableMaintenance.Report purge = new TableMaintenance(
                        ItSupport.committer(), MaintenanceSettings.DEFAULT,
                        Clock.offset(Clock.systemUTC(), Duration.ofDays(3)))
                        .run(runtime.tables().splitRecords(),
                                RecordsTable.INGESTED_AT,
                                Optional.of(Duration.ofDays(1)), false);
                assertThat(purge.retentionDeletedFiles()).isEqualTo(1);
                assertThat(list(dataPrefix))
                        .as("PHI data objects physically removed").isEmpty();
            }
        }

        // Nothing the connector wrote is readable: data, manifests and
        // metadata alike.
        FileIO io = runtime.tables().splitRecords().io();
        String warehouse = fixture.catalog().get("warehouse");
        List<String> objects = list(warehouse);
        assertThat(objects).isNotEmpty();
        for (String object : objects)
        {
            String content;
            try (InputStream in = io.newInputFile(object).newStream())
            {
                content = new String(in.readAllBytes(), ISO_8859_1);
            }
            catch (IOException failure)
            {
                throw new IOException("reading " + object, failure);
            }
            assertThat(content).as(object).doesNotContain(MARKER)
                    .doesNotContain("good.csv").doesNotContain("bad.csv");
        }
    }
}
