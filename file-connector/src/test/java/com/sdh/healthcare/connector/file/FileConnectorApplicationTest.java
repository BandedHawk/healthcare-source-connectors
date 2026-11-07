/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.cfg.PackageVersion;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.ContractVersion;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.broker.receive.BatchHandler.Outcome;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemLanded;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.checkpoint.LeasedCommitter;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.pipeline.IngestPipeline;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.file.split.SplitHandler;
import com.sdh.healthcare.connector.file.split.SplitReconciler;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * The whole split stage on real components without Docker: embedded
 * Postgres (leases, checkpoints and the JDBC Iceberg catalog), in-memory
 * object storage, key files on disk, and a SEDA queue standing in for the
 * output broker. The test plays the landing node with {@link TestLanding},
 * in the same catalog, and hands its messages to the split handler as the
 * input route would; the route itself, on each broker, is tested in
 * connector-broker and the image tests.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = {"camel.main.run-controller=false",
        "sdh.split.autostart=false"})
class FileConnectorApplicationTest
{

    /** The intake the stage serves. */
    static final String INTAKE = "claims";

    /** Where the stage's messages go. */
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

    /** Fatal exits the stage asked for. */
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
     * The stage's settings. The input is never started: nothing listens
     * at its address.
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
        registry.add("sdh.stage.input.type", () -> "kafka");
        registry.add("sdh.stage.input.uri",
                () -> "kafka:sdh.landed?brokers=localhost:1&groupId=split");
        registry.add("sdh.stage.registry-schema", () -> "sdh_split");
        registry.add("sdh.split.format", () -> "csv");
        registry.add("sdh.storage.catalog-name", () -> "sdh");
        registry.add("sdh.storage.commit.max-records", () -> "5");
        registry.add("sdh.maintenance.compaction.min-input-files", () -> "2");
        registry.add("sdh.lease.jdbc.url", () -> jdbc);
        registry.add("sdh.lease.jdbc.username", () -> "postgres");
        registry.add("sdh.lease.jdbc.password", () -> "postgres");
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
        registry.add("sdh.crypto.keys.inbox.kid", () -> TestKeys.INBOX_KID);
        registry.add("sdh.crypto.keys.inbox.pem", KEYS.inboxPem::toString);
        registry.add("sdh.crypto.keys.inbox.passphrase",
                () -> TestKeys.INBOX_PASSPHRASE);
        registry.add("sdh.crypto.keys.landing-signer.kid",
                () -> TestKeys.LANDING_SIGNER_KID);
        registry.add("sdh.crypto.keys.landing-signer.pem",
                KEYS.landingSignerPem::toString);
        registry.add("sdh.stage.output.type", () -> "camel");
        registry.add("sdh.stage.output.uri", () -> OUT_QUEUE);
        registry.add("sdh.retry.max-attempts", () -> "2");
        registry.add("sdh.retry.initial-backoff", () -> "10ms");
    }

    /**
     * The JDBC catalog's properties, for the stage and the test.
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

    /** The app's Camel context. */
    @Autowired
    CamelContext camel;

    /** The stage's runtime. */
    @Autowired
    ConnectorRuntime runtime;

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

    /**
     * Every broker's Camel component resolves, and Jackson 2 is the pinned
     * version.
     */
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
        assertThat(PackageVersion.VERSION
                .toString()).startsWith("2.22");
    }

    /**
     * Landed items are split, each announced once it is; a bad one fails,
     * a repeat is only announced again, strangers are refused, and
     * maintenance compacts the shared table.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void splitsLandedItemsAndFailsABadOne() throws Exception
    {
        String older = csv(12, "b");
        String newer = csv(3, "a");
        String bad = "1,ok\n2,\"never closed\n";
        TestLanding landing = new TestLanding(catalogProperties(),
                runtime.keys().wrap(), KEYS, INTAKE);
        LandedItem first = landing.land(older, "b-smith.csv");
        LandedItem second = landing.land(newer, "a-jones.csv");
        LandedItem third = landing.land(bad, null);
        SplitHandler handler = handler();
        for (LandedItem item : List.of(first, second, third))
        {
            assertThat(handle(handler, landing.announcement(item)))
                    .isEqualTo(Outcome.DONE);
        }
        List<ConnectorMessage> messages = drain();
        assertThat(FATAL).isEmpty();

        List<ItemSplit> split = of(messages, ItemSplit.class);
        assertThat(split).extracting(ItemSplit::itemId).containsExactly(
                first.itemId(), second.itemId(), third.itemId());
        assertThat(split).extracting(ItemSplit::itemSize).containsExactly(
                (long) older.length(), (long) newer.length(),
                (long) bad.length());
        assertThat(split).extracting(ItemSplit::itemDigest).containsExactly(
                first.digest(), second.digest(), third.digest());
        assertThat(split.subList(0, 2)).extracting(ItemSplit::recordCount)
                .containsExactly(12L, 3L);
        assertThat(split.subList(0, 2)).extracting(ItemSplit::failure)
                .containsOnlyNulls();
        assertThat(split.get(2).failure().stage()).isEqualTo("split");
        assertThat(split.get(2).failure().code())
                .isEqualTo("PARSE_UNTERMINATED_QUOTE");
        assertThat(split).allSatisfy(item ->
                assertThat(item.intake()).isEqualTo(INTAKE));
        // The producer's reference, from landing, is passed on.
        assertThat(split.getFirst().producerRef()).isEqualTo("b-smith.csv");
        assertThat(of(messages, RecordSplit.class))
                .hasSizeGreaterThanOrEqualTo(15)
                .allSatisfy(record ->
                        assertThat(record.recordId()).isNotBlank());

        // A repeat is only announced again; other messages are refused.
        assertThat(handle(handler, landing.announcement(first)))
                .isEqualTo(Outcome.DONE);
        assertThat(of(drain(), ItemSplit.class)).singleElement()
                .isEqualTo(split.getFirst());
        assertThat(handle(handler, landing.seal(otherIntake(first))))
                .isEqualTo(Outcome.DONE);
        assertThat(handle(handler, landing.seal(
                first.message("other", LandingTables.DEFAULT_NAMESPACE))))
                .isEqualTo(Outcome.DONE);
        MessageSealer stranger = new MessageSealer(
                new ECDSASigner((ECPrivateKey) KEYS.signing.getPrivate()),
                TestKeys.LANDING_SIGNER_KID, KEYS.inbox.getPublic(),
                TestKeys.INBOX_KID);
        assertThat(handle(handler, stranger.seal(
                second.message("sdh", LandingTables.DEFAULT_NAMESPACE))))
                .isEqualTo(Outcome.DONE);
        assertThat(drain()).isEmpty();

        // Maintenance compacts the small batch files.
        TableMaintenance.Report report = new TableMaintenance(
                new LeasedCommitter(new Held()), MaintenanceSettings.DEFAULT,
                Clock.systemUTC()).run(runtime.tables().splitRecords(),
                        RecordsTable.INGESTED_AT, Optional.empty(), false);
        assertThat(report.compactedFiles()).isGreaterThanOrEqualTo(2);
        assertThat(FATAL).isEmpty();
    }

    /**
     * A landed item whose {@code item.landed} message the broker lost is
     * found and split all the same, once its grace has passed: not
     * before, not twice, and not an item of another intake. Its message,
     * should it turn up later, only announces it again.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void recoversLandedItemsWhoseMessagesWereLost() throws Exception
    {
        TestLanding landing = new TestLanding(catalogProperties(),
                runtime.keys().wrap(), KEYS, INTAKE);
        TestLanding elsewhere = new TestLanding(catalogProperties(),
                runtime.keys().wrap(), KEYS, "other-intake");
        LandedItem lostFirst = landing.land(csv(2, "lost1"), "lost-1.csv");
        LandedItem lostSecond = landing.land(csv(3, "lost2"), "lost-2.csv");
        LandedItem foreign = elsewhere.land(csv(1, "foreign"), null);

        // Within the grace, a message may still be on its way.
        Instant now = Instant.now();
        assertThat(reconciler(now, Duration.ofMinutes(10)).reconcile())
                .isZero();

        // Past it, both are split, in the order they were landed.
        SplitReconciler reconciler = reconciler(
                now.plus(Duration.ofMinutes(11)), Duration.ofMinutes(10));
        assertThat(reconciler.reconcile()).isEqualTo(2);
        assertThat(reconciler.reconcile()).as("never twice").isZero();
        List<ConnectorMessage> messages = drain();
        assertThat(of(messages, ItemSplit.class)).extracting(ItemSplit::itemId)
                .containsExactly(lostFirst.itemId(), lostSecond.itemId());
        assertThat(of(messages, RecordSplit.class)).hasSize(5);
        assertThat(runtime.tables().checkpoints().known(
                List.of(foreign.itemId()))).isEmpty();

        // The lost message turns up after all: announced again, no more.
        assertThat(handle(handler(), landing.announcement(lostFirst)))
                .isEqualTo(Outcome.DONE);
        assertThat(of(drain(), ItemSplit.class)).extracting(ItemSplit::itemId)
                .containsExactly(lostFirst.itemId());
        assertThat(FATAL).isEmpty();
    }

    /**
     * An item left part way, its pod stopped and its message lost, is taken
     * on again from its checkpoint once the grace has passed, although its
     * checkpoint says it was started; and only once.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void resumesAnItemLeftPartWayWhoseMessageWasLost() throws Exception
    {
        TestLanding landing = new TestLanding(catalogProperties(),
                runtime.keys().wrap(), KEYS, INTAKE);
        String content = csv(3, "partway");
        LandedItem item = landing.land(content, "partway.csv");
        // A pod started it and stopped before its first batch.
        runtime.tables().checkpoints().save(Checkpoint.start(item.itemId(),
                IngestPipeline.itemFingerprint(item.itemId()), "csv",
                content.getBytes(UTF_8).length, Instant.now()), new Held());

        Instant now = Instant.now();
        assertThat(reconciler(now, Duration.ofMinutes(10)).reconcile())
                .as("within the grace").isZero();
        SplitReconciler reconciler = reconciler(
                now.plus(Duration.ofMinutes(11)), Duration.ofMinutes(10));
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(reconciler.reconcile()).as("never twice").isZero();
        List<ConnectorMessage> messages = drain();
        assertThat(of(messages, ItemSplit.class)).extracting(ItemSplit::itemId)
                .containsExactly(item.itemId());
        assertThat(of(messages, RecordSplit.class)).hasSize(3);
        assertThat(FATAL).isEmpty();
    }

    /**
     * {@code item.landed} messages of other contract versions, as when the
     * landing node and the stage are upgraded at different times: a later
     * or earlier minor version is taken, and its item split; a later major
     * version is held, to be tried again, without stopping the stage; an
     * earlier major version, which no upgrade will read, is rejected so it
     * blocks nothing behind it; and neither item is stranded.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void takesOtherMinorsHoldsALaterMajorAndRejectsAnEarlier() throws Exception
    {
        TestLanding landing = new TestLanding(catalogProperties(),
                runtime.keys().wrap(), KEYS, INTAKE);
        LandedItem newer = landing.land(csv(2, "newer"), null);
        LandedItem older = landing.land(csv(1, "older"), null);
        LandedItem nextMajor = landing.land(csv(1, "major"), null);
        LandedItem lastMajor = landing.land(csv(1, "last"), null);
        SplitHandler handler = handler();

        Map<String, Object> later = payload(newer,
                ContractVersion.MAJOR + "." + (ContractVersion.MINOR + 1));
        later.put("priority", "high");
        assertThat(handle(handler, landing.seal(later)))
                .isEqualTo(Outcome.DONE);
        Map<String, Object> earlier =
                payload(older, ContractVersion.MAJOR + ".2");
        earlier.remove("replaces");
        assertThat(handle(handler, landing.seal(earlier)))
                .isEqualTo(Outcome.DONE);
        assertThat(of(drain(), ItemSplit.class)).extracting(ItemSplit::itemId)
                .containsExactly(newer.itemId(), older.itemId());

        byte[] major = landing.seal(payload(nextMajor,
                (ContractVersion.MAJOR + 1) + ".0"));
        assertThat(handle(handler, major)).isEqualTo(Outcome.RETRY);
        byte[] earlierMajor = landing.seal(payload(lastMajor,
                (ContractVersion.MAJOR - 1) + ".4"));
        assertThat(handle(handler, earlierMajor)).isEqualTo(Outcome.DONE);
        assertThat(runtime.tables().checkpoints().known(
                List.of(nextMajor.itemId(), lastMajor.itemId()))).isEmpty();
        assertThat(FATAL).isEmpty();

        // Neither item is stranded: reconciling splits them.
        assertThat(reconciler(Instant.now().plus(Duration.ofMinutes(11)),
                Duration.ofMinutes(10)).reconcile()).isPositive();
        assertThat(of(drain(), ItemSplit.class)).extracting(ItemSplit::itemId)
                .contains(nextMajor.itemId(), lastMajor.itemId());
    }

    /**
     * A lease that is always held, for maintenance the test runs itself.
     */
    static final class Held implements LeaseToken
    {

        /** Creates the lease. */
        Held()
        {
        }

        /**
         * The epoch.
         *
         * @return 1
         */
        @Override
        public long epoch()
        {
            return 1;
        }

        /** Never finds the lease lost. */
        @Override
        public void checkHeld()
        {
        }
    }

    /**
     * The split handler, as the input route would call it.
     *
     * @return the handler
     */
    SplitHandler handler()
    {
        return new SplitHandler(runtime.splitter(), runtime.keys().inbox(),
                INTAKE, "sdh", LandingTables.DEFAULT_NAMESPACE, FATAL::add);
    }

    /**
     * Hands one message to the handler, as a batch of one.
     *
     * @param handler the handler
     * @param body    the message
     * @return what became of it
     * @throws InterruptedException if interrupted
     */
    static Outcome handle(SplitHandler handler, byte[] body)
            throws InterruptedException
    {
        return handler.handle(List.of(new BrokerMessage(body,
                UUID.randomUUID().toString(), Map.of())));
    }

    /**
     * A reconciler for the stage, at a fixed time.
     *
     * @param now   the time it runs at
     * @param grace how long an item may wait for its message
     * @return the reconciler
     */
    SplitReconciler reconciler(Instant now, Duration grace)
    {
        return new SplitReconciler(runtime.tables().checkpoints(),
                runtime.splitter(), runtime.tables().landedItems(), INTAKE,
                grace, Clock.fixed(now, ZoneOffset.UTC));
    }

    /**
     * An item's {@code item.landed} payload, as this build writes it, but
     * marked with another contract version.
     *
     * @param item    the item
     * @param version the version
     * @return the payload, which the caller may change further
     */
    static Map<String, Object> payload(LandedItem item, String version)
    {
        Map<String, Object> payload = new LinkedHashMap<>(
                PayloadCodec.toJson(item.message("sdh",
                        LandingTables.DEFAULT_NAMESPACE)));
        payload.put("v", version);
        return payload;
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
        return new ItemLanded(message.itemId(), "other", message.kind(),
                message.size(), message.itemDigest(), message.landedAt(),
                message.storage());
    }

    /**
     * Opens what the stage has published since the last call.
     *
     * @return the messages, in the order sent
     * @throws Exception if one can't be opened
     */
    List<ConnectorMessage> drain() throws Exception
    {
        MessageOpener opener = new MessageOpener(
                Map.of("downstream-2026", KEYS.recipient.getPrivate()),
                Map.of("connector-2026",
                        (ECPublicKey) KEYS.signing.getPublic()));
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        List<ConnectorMessage> messages = new ArrayList<>();
        String body;
        while ((body = consumer.receiveBody(OUT_QUEUE, 200,
                String.class)) != null)
        {
            messages.add(opener.open(body.getBytes(UTF_8)));
        }
        return messages;
    }

    /**
     * The messages of a type.
     *
     * @param <T>      the type
     * @param messages the messages
     * @param type     the type
     * @return those of the type, in order
     */
    static <T extends ConnectorMessage> List<T> of(
            List<ConnectorMessage> messages, Class<T> type)
    {
        return messages.stream().filter(type::isInstance).map(type::cast)
                .toList();
    }
}
