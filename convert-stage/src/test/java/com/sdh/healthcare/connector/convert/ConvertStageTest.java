/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.broker.receive.BatchHandler;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.RecordInput;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.WrittenRecord;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.stage.StageRegistry;
import com.sdh.healthcare.connector.stage.StageWorker;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the convert stage as it runs: the shared worker with the
 * {@link ConvertProcessor}, its registry on a real Postgres, and the
 * {@link ConvertReconciler}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ConvertStageTest
{

    /** The namespace both tables are in. */
    static final String NS = "sdh_ingest";

    /** The KEK the tables' data keys are wrapped with. */
    static final String KEK = "records-kek/1";

    /** When the records were split. */
    static final Instant SPLIT_AT = Instant.parse("2026-11-06T10:00:00Z");

    /** Gives each test its own registry schema. */
    static final AtomicInteger SCHEMAS = new AtomicInteger();

    /** The registry's database. */
    static EmbeddedPostgres postgres;

    /** Signs what the split stage sends. */
    KeyPair splitSigner;

    /** The convert stage's own key, which its input is sealed to. */
    KeyPair recipient;

    /** Holds the KEK. */
    AesKeyWrapProvider kms;

    /** The catalog, named "prod". */
    InMemoryCatalog catalog;

    /** The split stage's table. */
    Table splitRecords;

    /** The shared records table. */
    Table records;

    /** Reads records. */
    QueueReader reader;

    /** The stage's registry. */
    StageRegistry registry;

    /** The stage's processor, as the worker runs it. */
    ConvertProcessor processor;

    /** The worker. */
    StageWorker<ConvertProcessor.Prepared> worker;

    /** What the stage sent. */
    final List<OutboundMessage> sent =
            Collections.synchronizedList(new ArrayList<>());

    /** The item. */
    final UUID item = UUID.randomUUID();

    /** Creates the tests; JUnit runs each test on a new instance. */
    ConvertStageTest()
    {
    }

    /**
     * Starts the database.
     *
     * @throws Exception if it can't start
     */
    @BeforeAll
    static void database() throws Exception
    {
        postgres = EmbeddedPostgres.start();
    }

    /**
     * Stops the database.
     *
     * @throws Exception if it can't stop
     */
    @AfterAll
    static void stopDatabase() throws Exception
    {
        postgres.close();
    }

    /**
     * Makes the keys, tables, registry and worker.
     *
     * @throws Exception if a key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        splitSigner = ec();
        recipient = ec();
        KeyPair signer = ec();
        KeyPair downstream = ec();
        KeyGenerator aes = KeyGenerator.getInstance("AES");
        aes.init(256);
        kms = new AesKeyWrapProvider(Map.of(KEK, aes.generateKey()));
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        splitRecords = IngestTables.splitRecords(catalog, NS);
        records = IngestTables.sharedRecords(catalog, NS);
        reader = QueueReader.builder().catalog(catalog)
                .expectedCatalogName("prod").keyProvider(kms).build();
        registry = new StageRegistry(postgres.getPostgresDatabase(),
                "convert_test_" + SCHEMAS.incrementAndGet(),
                ConvertProcessor.STAGE, "pod-a", Duration.ofSeconds(10));
        registry.createTablesIfMissing();
        Clock clock = Clock.fixed(SPLIT_AT.plusSeconds(3600), ZoneOffset.UTC);
        processor = new ConvertProcessor(reader,
                ConverterRegistry.load(),
                new ConvertProcessor.Settings(Map.of("csv.header", "true"),
                        1 << 20, 64L << 20, "prod", NS, "test-build"),
                new ConvertProcessor.Storage(records, kms, KEK,
                        new RecordIds(new byte[32])),
                null, clock);
        MessagePublisher publisher = message ->
        {
            sent.add(message);
            return CompletableFuture.completedFuture(null);
        };
        worker = new StageWorker<>(processor, registry,
                new MessageOpener(Map.of("convert", recipient.getPrivate()),
                        Map.of("split",
                                (ECPublicKey) splitSigner.getPublic())),
                new MessageSealer(
                        new ECDSASigner((ECPrivateKey) signer.getPrivate()),
                        "convert", downstream.getPublic(), "transform"),
                publisher, publisher,
                new StageWorker.Settings(2, Duration.ofMinutes(1),
                        Duration.ofSeconds(15), Duration.ofMillis(50),
                        Duration.ofSeconds(5)),
                clock);
    }

    /** Stops the worker and reader. */
    @AfterEach
    void tearDown()
    {
        worker.close();
        reader.close();
    }

    /**
     * Makes a key pair on P-256.
     *
     * @return the pair
     * @throws Exception if the JDK can't
     */
    static KeyPair ec() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    /**
     * A message as the split stage sends it, ready to receive.
     *
     * @param message the message
     * @return it sealed to the convert stage
     * @throws Exception if it can't be sealed
     */
    BrokerMessage received(ConnectorMessage message) throws Exception
    {
        MessageSealer sealer = new MessageSealer(
                new ECDSASigner((ECPrivateKey) splitSigner.getPrivate()),
                "split", recipient.getPublic(), "convert");
        return new BrokerMessage(sealer.seal(message), message.messageId(),
                Map.of());
    }

    /**
     * Writes CSV records to {@code split_records}, as the split stage does.
     *
     * @param rows the rows, each after the same header
     * @return a {@code record.split} for each
     */
    List<RecordSplit> split(String... rows)
    {
        RecordBatchWriter writer = new RecordBatchWriter(splitRecords, kms,
                KEK, new RecordIds(new byte[32]), SPLIT_AT);
        long offset = 0;
        for (int seq = 0; seq < rows.length; seq++)
        {
            byte[] text = ("a,b\n" + rows[seq] + "\n").getBytes(UTF_8);
            writer.add(new RecordInput(item, seq, offset, "csv", text));
            offset += text.length;
        }
        DataFile file = writer.finish();
        splitRecords.newAppend().appendFile(file).commit();
        StorageRef ref = new StorageRef("prod", NS, IngestTables.SPLIT_RECORDS,
                writer.ingestedDate(),
                splitRecords.currentSnapshot().snapshotId(), writer.kekRef());
        List<RecordSplit> messages = new ArrayList<>();
        for (WrittenRecord record : writer.written())
        {
            messages.add(new RecordSplit(record.recordId(), item,
                    record.seq(), record.sourceOffset(),
                    record.sourceLength(), "csv", "text/csv",
                    record.plaintextLength(), record.chunkCount(), ref, null,
                    null, null, -1, null, null));
        }
        return messages;
    }

    /**
     * The messages sent of a type.
     *
     * @param type the type's wire name
     * @return how many
     */
    long sentOf(String type)
    {
        return sent.stream()
                .filter(message -> message.type().wireName().equals(type))
                .count();
    }

    /**
     * Records are converted and announced; once the {@code item.split} has
     * come and every record is stored, {@code item.converted} is sent with
     * the item's intake. A message delivered again changes nothing.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void convertsRecordsAndCompletesTheItem() throws Exception
    {
        List<RecordSplit> messages = split("1,2", "3");
        ItemSplit total = new ItemSplit(item, "claims", 2, 0, 20, "digest",
                null, null, null, SPLIT_AT, SPLIT_AT);
        List<BrokerMessage> batch = List.of(received(messages.get(0)),
                received(total), received(messages.get(1)));
        assertThat(worker.handle(batch))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentOf("record.stored")).isEqualTo(1);
        assertThat(sentOf("record.failed")).isEqualTo(1);
        assertThat(sentOf("item.converted")).isEqualTo(1);
        assertThat(registry.completed(List.of(item))).containsExactly(item);

        assertThat(worker.handle(batch))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentOf("record.stored")).isEqualTo(1);
        assertThat(sentOf("item.converted")).isEqualTo(1);
    }

    /**
     * A record whose {@code record.split} never came is found by the
     * reconciler, rebuilt from its row and converted; an item whose
     * records are all here but whose {@code item.split} never came is
     * reported, and completed once it comes.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void reconcilesRecordsWhoseMessagesWereLost() throws Exception
    {
        List<RecordSplit> messages = split("1,2", "3,4");
        assertThat(worker.handleOpened(List.of(messages.get(0))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        ConvertReconciler reconciler = new ConvertReconciler(splitRecords,
                registry, worker, processor, "prod", NS, Duration.ofMinutes(10),
                Duration.ofDays(2),
                Clock.fixed(SPLIT_AT.plusSeconds(3600), ZoneOffset.UTC));

        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(registry.knownRecords(item)).containsExactlyInAnyOrder(
                messages.get(0).recordId(), messages.get(1).recordId());
        assertThat(sentOf("record.stored")).isEqualTo(2);
        InMemoryLogRecordExporter events = InMemoryLogRecordExporter.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build())
        {
            // The total is missing on each run, but audited once.
            Telemetry.install(sdk);
            assertThat(reconciler.reconcile()).isZero();
            assertThat(reconciler.reconcile()).isZero();
            assertThat(events.getFinishedLogRecordItems().stream()
                    .filter(event -> "sdh.audit.convert.item_total_missing"
                            .equals(event.getEventName())))
                    .singleElement().satisfies(event -> assertThat(
                            event.getAttributes().asMap()).containsEntry(
                                    AttributeKey.longKey("sdh.count"), 2L));
        }
        finally
        {
            Telemetry.install(OpenTelemetry.noop());
        }
        assertThat(sentOf("item.converted")).isZero();

        ItemSplit total = new ItemSplit(item, "claims", 2, 0, 20, "digest",
                null, null, null, SPLIT_AT, SPLIT_AT);
        assertThat(worker.handle(List.of(received(total))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentOf("item.converted")).isEqualTo(1);
        OutboundMessage stored = sent.stream()
                .filter(message -> message.type().wireName()
                        .equals("record.stored"))
                .findFirst().orElseThrow();
        assertThat(stored.itemId()).isEqualTo(item);
        // A record split more recently than the grace isn't looked at.
        ConvertReconciler early = new ConvertReconciler(splitRecords,
                registry, worker, processor, "prod", NS, Duration.ofMinutes(10),
                Duration.ofDays(2),
                Clock.fixed(SPLIT_AT.plusSeconds(60), ZoneOffset.UTC));
        assertThat(early.reconcile()).isZero();
    }

    /**
     * A record the reconciler rebuilt that then fails is audited, since
     * its queue message isn't known: the producer's result, if one is
     * sent, names no message for it. One that is converted isn't.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void auditsARebuiltRecordThatFailsWithoutItsMessage() throws Exception
    {
        List<RecordSplit> messages = split("1,2", "3", "5,6", "8");
        ConvertReconciler reconciler = new ConvertReconciler(splitRecords,
                registry, worker, processor, "prod", NS,
                Duration.ofMinutes(10), Duration.ofDays(2),
                Clock.fixed(SPLIT_AT.plusSeconds(3600), ZoneOffset.UTC));
        InMemoryLogRecordExporter events = InMemoryLogRecordExporter.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build())
        {
            Telemetry.install(sdk);
            // Received as the split stage sent it, a failing record knows
            // its message, and isn't audited.
            assertThat(worker.handleOpened(List.of(messages.get(3))))
                    .isEqualTo(BatchHandler.Outcome.DONE);
            assertThat(reconciler.reconcile()).isEqualTo(3);
            assertThat(events.getFinishedLogRecordItems().stream()
                    .filter(event -> "sdh.audit.convert.placement_unknown"
                            .equals(event.getEventName())))
                    .singleElement().satisfies(event -> assertThat(
                            event.getAttributes().asMap())
                            .containsEntry(AttributeKey.longKey("sdh.seq"),
                                    1L)
                            .containsEntry(
                                    AttributeKey.stringKey("sdh.item_id"),
                                    item.toString()));
        }
        finally
        {
            Telemetry.install(OpenTelemetry.noop());
        }
    }

    /**
     * After an outage longer than the lookback, records split before it
     * whose messages were lost are still found: the run starts at the
     * watermark, not the lookback, goes a day at a time, audits that it is
     * catching up, and leaves the watermark at the grace ago. The next run
     * is an ordinary one.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void catchesUpOnAnOutageLongerThanTheLookback() throws Exception
    {
        split("1,2", "3,4");
        // The last run before the outage, an hour before the records were
        // split: it notes how far it checked.
        Instant before = SPLIT_AT.minus(Duration.ofHours(1));
        assertThat(reconcilerAt(before).reconcile()).isZero();
        assertThat(registry.checkedTo())
                .hasValue(before.minus(Duration.ofMinutes(10)));

        // Five days later, three past the lookback, the messages are gone.
        Instant after = SPLIT_AT.plus(Duration.ofDays(5));
        InMemoryLogRecordExporter events = InMemoryLogRecordExporter.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build())
        {
            Telemetry.install(sdk);
            ConvertReconciler reconciler = reconcilerAt(after);
            assertThat(reconciler.reconcile()).isEqualTo(2);
            assertThat(registry.checkedTo())
                    .hasValue(after.minus(Duration.ofMinutes(10)));
            assertThat(reconciler.reconcile()).as("an ordinary run")
                    .isZero();
            assertThat(events.getFinishedLogRecordItems().stream()
                    .map(event -> event.getEventName())
                    .filter(name -> name.startsWith("sdh.audit.convert."
                            + "reconcile_")))
                    .containsExactly("sdh.audit.convert.reconcile_catching_up",
                            "sdh.audit.convert.reconcile_caught_up");
            assertThat(events.getFinishedLogRecordItems().stream()
                    .filter(event -> event.getEventName()
                            .endsWith("reconcile_catching_up")))
                    .singleElement().satisfies(event -> assertThat(
                            event.getAttributes().asMap()).containsEntry(
                                    AttributeKey.longKey("sdh.count"), 6L));
        }
        finally
        {
            Telemetry.install(OpenTelemetry.noop());
        }
        assertThat(sentOf("record.stored")).isEqualTo(2);
        assertThat(ConvertReconciler.days(after, after)).isZero();
        assertThat(ConvertReconciler.days(after,
                after.plus(Duration.ofHours(25)))).isEqualTo(2);
    }

    /**
     * The stage's reconciler at a time, with a 10-minute grace and a
     * 2-day lookback.
     *
     * @param now the time it runs at
     * @return the reconciler
     */
    ConvertReconciler reconcilerAt(Instant now)
    {
        return new ConvertReconciler(splitRecords, registry, worker,
                processor, "prod", NS, Duration.ofMinutes(10),
                Duration.ofDays(2), Clock.fixed(now, ZoneOffset.UTC));
    }
}
