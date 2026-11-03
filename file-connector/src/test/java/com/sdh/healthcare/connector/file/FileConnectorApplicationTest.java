/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemLanded;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.file.inbox.InboxReceiver;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.file.runtime.MaintenanceScheduler;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
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
 * The whole splitter on real components without Docker: embedded Postgres
 * (lease and JDBC Iceberg catalog), in-memory object storage, key files on
 * disk, and SEDA queues standing in for the brokers. The test plays the
 * landing node with {@link TestLanding}, in the same catalog.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
class FileConnectorApplicationTest
{

    /** The intake the splitter serves. */
    static final String INTAKE = "claims";

    /** Where {@code item.landed} messages arrive. */
    static final String LANDED_QUEUE = "seda:landed?size=1000";

    /** Where the splitter's messages go. */
    static final String OUT_QUEUE = "seda:sdh-out?size=100000";

    /** The test's key files. */
    static final TestKeys KEYS;

    /** The lease database and catalog. */
    static final EmbeddedPostgres PG;

    /** The in-memory warehouse, shared with the test's landing catalog. */
    static final String WAREHOUSE = "memory://wh-" + UUID.randomUUID();

    static
    {
        try
        {
            Path root = Files.createTempDirectory("sdh-app");
            KEYS = new TestKeys(Files.createDirectories(root.resolve("keys")));
            PG = EmbeddedPostgres.start();
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
    }

    /** Fatal exits the splitter asked for. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /** Records fatal exits rather than ending the test JVM. */
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
     * The splitter's settings.
     *
     * @param registry where they go
     * @throws Exception if the recipient key can't be read
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception
    {
        catalogProperties().forEach((key, value) -> registry.add(
                "sdh.storage.catalog[" + key + "]", () -> value));
        String jdbc = PG.getJdbcUrl("postgres", "postgres");
        registry.add("sdh.landing.intake", () -> INTAKE);
        registry.add("sdh.landing.queue-uri", () -> LANDED_QUEUE);
        registry.add("sdh.landing.idle-wait", () -> "200ms");
        registry.add("sdh.split.format", () -> "csv");
        registry.add("sdh.storage.catalog-name", () -> "sdh");
        registry.add("sdh.storage.commit.max-sections", () -> "5");
        registry.add("sdh.maintenance.compaction.min-input-files", () -> "2");
        registry.add("sdh.lease.jdbc.url", () -> jdbc);
        registry.add("sdh.lease.jdbc.username", () -> "postgres");
        registry.add("sdh.lease.jdbc.password", () -> "postgres");
        registry.add("sdh.crypto.keystore.path", KEYS.keystore::toString);
        registry.add("sdh.crypto.keystore.password",
                () -> TestKeys.KS_PASSWORD);
        registry.add("sdh.crypto.keys.sections-kek", () -> TestKeys.SECTIONS);
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
        registry.add("sdh.crypto.keys.inbox.kid", () -> TestKeys.INBOX_KID);
        registry.add("sdh.crypto.keys.inbox.pem", KEYS.inboxPem::toString);
        registry.add("sdh.crypto.keys.inbox.passphrase",
                () -> TestKeys.INBOX_PASSPHRASE);
        registry.add("sdh.crypto.keys.landing-signer.kid",
                () -> TestKeys.LANDING_SIGNER_KID);
        registry.add("sdh.crypto.keys.landing-signer.pem",
                KEYS.landingSignerPem::toString);
        registry.add("sdh.queue.type", () -> "camel");
        registry.add("sdh.queue.uri", () -> OUT_QUEUE);
        registry.add("sdh.retry.max-attempts", () -> "2");
        registry.add("sdh.retry.initial-backoff", () -> "10ms");
    }

    /**
     * The JDBC catalog's properties, for the splitter and the test.
     *
     * @return the properties
     */
    static Map<String, String> catalogProperties()
    {
        Map<String, String> catalog = new HashMap<>();
        catalog.put("type", "jdbc");
        catalog.put("uri", PG.getJdbcUrl("postgres", "postgres"));
        catalog.put("warehouse", WAREHOUSE);
        catalog.put("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO");
        catalog.put("jdbc.user", "postgres");
        catalog.put("jdbc.password", "postgres");
        catalog.put("jdbc.schema-version", "V1");
        return catalog;
    }

    @Autowired
    CamelContext camel;

    @Autowired
    ConnectorRuntime runtime;

    @Autowired
    MaintenanceScheduler maintenance;

    @Autowired
    ReentrantLock writerLock;

    /**
     * Rows of CSV, each with a multi-line quoted field.
     *
     * @param count how many
     * @param tag   marks the rows
     * @return the CSV
     */
    static String csv(int count, String tag)
    {
        StringBuilder text = new StringBuilder();
        for (int row = 0; row < count; row++)
        {
            text.append(row).append(',').append(tag).append("-patient-")
                    .append(row).append(",\"note\nline\"\n");
        }
        return text.toString();
    }

    @Test
    void everyBrokerComponentResolvesAndJackson2IsPinned()
    {
        for (String component : new String[] {"kafka", "aws2-sqs",
                "google-pubsub", "azure-servicebus", "azure-eventhubs",
                "jms", "spring-rabbitmq"})
        {
            assertThat(camel.getComponent(component)).as(component)
                    .isNotNull();
        }
        assertThat(com.fasterxml.jackson.databind.cfg.PackageVersion.VERSION
                .toString()).startsWith("2.22");
    }

    @Test
    void splitsLandedItemsInOrderOnceEachAndFailsABadOne() throws Exception
    {
        String older = csv(12, "b");
        String newer = csv(3, "a");
        String bad = "1,ok\n2,\"never closed\n";
        TestLanding landing = new TestLanding(catalogProperties(),
                runtime.keys().wrap(), KEYS, INTAKE);
        LandedItem first = landing.land(older, "b-smith.csv");
        LandedItem second = landing.land(newer, "a-jones.csv");
        LandedItem third = landing.land(bad, null);
        for (LandedItem item : List.of(first, second, third))
        {
            landing.announce(camel, LANDED_QUEUE, item);
        }

        List<ConnectorMessage> messages = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline
                && (count(messages, FileStarted.class) < 3
                        || runtime.inbox().backlog() > 0
                        || runtime.processor().inProgress().isPresent()))
        {
            drain(messages);
        }
        assertThat(FATAL).isEmpty();
        assertThat(runtime.inbox().backlog()).isZero();
        assertThat(runtime.processor().inProgress()).isEmpty();

        // A repeat is recorded once; other messages are refused.
        InboxReceiver receiver = runtime.receiver();
        assertThat(receiver.receive(landing.announcement(first)))
                .isEqualTo(InboxReceiver.Result.REPEAT);
        assertThat(receiver.receive(landing.seal(otherIntake(first))))
                .isEqualTo(InboxReceiver.Result.REJECTED);
        assertThat(receiver.receive(landing.seal(
                first.message("other", LandingTables.DEFAULT_NAMESPACE))))
                .isEqualTo(InboxReceiver.Result.REJECTED);
        MessageSealer stranger = new MessageSealer(
                new ECDSASigner((ECPrivateKey) KEYS.signing.getPrivate()),
                TestKeys.LANDING_SIGNER_KID, KEYS.inbox.getPublic(),
                TestKeys.INBOX_KID);
        assertThat(receiver.receive(stranger.seal(
                second.message("sdh", LandingTables.DEFAULT_NAMESPACE))))
                .isEqualTo(InboxReceiver.Result.REJECTED);
        Thread.sleep(500);
        drain(messages);

        List<FileStarted> started = messages.stream()
                .filter(FileStarted.class::isInstance)
                .map(FileStarted.class::cast).toList();
        assertThat(started).extracting(FileStarted::fileId).containsExactly(
                first.itemId(), second.itemId(), third.itemId());
        assertThat(started).extracting(FileStarted::fileSize).containsExactly(
                (long) older.length(), (long) newer.length(),
                (long) bad.length());
        assertThat(messages.stream().filter(SectionStored.class::isInstance))
                .hasSize(15);
        List<FileCompleted> completed = messages.stream()
                .filter(FileCompleted.class::isInstance)
                .map(FileCompleted.class::cast).toList();
        assertThat(completed).extracting(FileCompleted::fileId)
                .containsExactly(first.itemId(), second.itemId());
        assertThat(completed).extracting(FileCompleted::sectionCount)
                .containsExactly(12L, 3L);
        assertThat(completed).extracting(FileCompleted::fileDigest)
                .containsExactly(first.digest(), second.digest());

        // Maintenance between items compacts the small batch files.
        writerLock.lock();
        try
        {
            var reports = maintenance.runOnce();
            assertThat(reports).hasSize(2);
            assertThat(reports.getFirst().compactedFiles())
                    .isGreaterThanOrEqualTo(2);
        }
        finally
        {
            writerLock.unlock();
        }
        assertThat(FATAL).isEmpty();
    }

    /**
     * An item's message, naming another intake.
     *
     * @param item the item
     * @return the message
     */
    static ItemLanded otherIntake(LandedItem item)
    {
        ItemLanded message =
                item.message("sdh", LandingTables.DEFAULT_NAMESPACE);
        return new ItemLanded(message.fileId(), "other", message.kind(),
                message.size(), message.itemDigest(), message.landedAt(),
                message.storage());
    }

    /**
     * Opens what the splitter has published so far.
     *
     * @param messages where they go
     * @throws Exception if one can't be opened
     */
    void drain(List<ConnectorMessage> messages) throws Exception
    {
        MessageOpener opener = new MessageOpener(
                Map.of("downstream-2026", KEYS.recipient.getPrivate()),
                Map.of("connector-2026",
                        (ECPublicKey) KEYS.signing.getPublic()));
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        String body;
        while ((body = consumer.receiveBody(OUT_QUEUE, 200,
                String.class)) != null)
        {
            messages.add(opener.open(body.getBytes(UTF_8)));
        }
    }

    /**
     * Counts messages of a type.
     *
     * @param messages the messages
     * @param type     the type
     * @return how many there are
     */
    static long count(List<ConnectorMessage> messages,
            Class<? extends ConnectorMessage> type)
    {
        return messages.stream().filter(type::isInstance).count();
    }
}
