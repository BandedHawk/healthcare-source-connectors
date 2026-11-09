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
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemConverted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.RecordInput;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.WrittenRecord;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.stage.RegistryReport;
import com.sdh.healthcare.connector.stage.StageRegistry;
import com.sdh.healthcare.connector.stage.StageWorker;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.nio.charset.StandardCharsets;
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

    /** Signs what the convert stage sends. */
    KeyPair signer;

    /** The next stage's key, which the convert stage's output is sealed to. */
    KeyPair downstream;

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

    /** The registry's schema for this test. */
    String schema;

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
        signer = ec();
        downstream = ec();
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
        schema = "convert_test_" + SCHEMAS.incrementAndGet();
        registry = new StageRegistry(postgres.getPostgresDatabase(), schema,
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
                publisher,
                new StageWorker.Settings(2, Duration.ofMinutes(1),
                        Duration.ofSeconds(15), Duration.ofMillis(50),
                        Duration.ofSeconds(5)));
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
        return splitFor(item, rows);
    }

    /**
     * Writes CSV records of an item to {@code split_records}, as the split
     * stage does.
     *
     * @param itemId the item
     * @param rows   the rows, each after the same header
     * @return a {@code record.split} for each
     */
    List<RecordSplit> splitFor(UUID itemId, String... rows)
    {
        RecordBatchWriter writer = new RecordBatchWriter(splitRecords, kms,
                KEK, new RecordIds(new byte[32]), SPLIT_AT);
        long offset = 0;
        for (int seq = 0; seq < rows.length; seq++)
        {
            byte[] text = ("a,b\n" + rows[seq] + "\n").getBytes(UTF_8);
            writer.add(new RecordInput(itemId, seq, offset, "csv", text));
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
            messages.add(new RecordSplit(record.recordId(), itemId,
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
        List<RecordSplit> messages = split("1,2", "3,4");
        ItemSplit total = new ItemSplit(item, "claims", 2, 0, 20, "digest",
                null, null, null, SPLIT_AT, SPLIT_AT);
        List<BrokerMessage> batch = List.of(received(messages.get(0)),
                received(total), received(messages.get(1)));
        assertThat(worker.handle(batch))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentOf("record.stored")).isEqualTo(2);
        assertThat(sentOf("record.failed")).isZero();
        assertThat(sentOf("item.converted")).isEqualTo(1);
        assertThat(registry.completed(List.of(item))).containsExactly(item);
        // Each record, its row and the item name the version that made
        // them (STAGES.md §14.3).
        String version = processor.version(messages.get(0));
        MessageOpener opener = new MessageOpener(
                Map.of("transform", downstream.getPrivate()),
                Map.of("convert", (ECPublicKey) signer.getPublic()));
        List<ConnectorMessage> opened = new ArrayList<>();
        for (OutboundMessage message : sent)
        {
            opened.add(opener.open(message.body()));
        }
        assertThat(opened).filteredOn(m -> m instanceof RecordInStorage)
                .hasSize(2).allSatisfy(m -> assertThat(
                        ((RecordInStorage) m).version()).isEqualTo(version));
        assertThat(opened).filteredOn(ItemConverted.class::isInstance)
                .singleElement().satisfies(m -> assertThat(
                        ((ItemConverted) m).versions())
                        .containsExactly(version));
        assertThat(CommittedRecords.all(records, item)).hasSize(2)
                .allSatisfy(row -> assertThat(row.stageVersion())
                        .isEqualTo(version));

        assertThat(worker.handle(batch))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentOf("record.stored")).isEqualTo(2);
        assertThat(sentOf("item.converted")).isEqualTo(1);
    }

    /**
     * A record that fails conversion is stored as split and kept in the
     * convert stage's fail store, not sent on; its item isn't complete
     * while the failure is open, and gets {@code item.progress} instead,
     * naming the intake (STAGES.md §15.6).
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void keepsAFailedRecordAndReportsTheItemInProgress() throws Exception
    {
        List<RecordSplit> messages = split("1,2", "3");
        ItemSplit total = new ItemSplit(item, "claims", 2, 0, 20, "digest",
                "claims.csv", null, null, SPLIT_AT, SPLIT_AT);
        assertThat(worker.handle(List.of(received(messages.get(0)),
                received(total), received(messages.get(1)))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentOf("record.stored")).isEqualTo(1);
        assertThat(sentOf("record.failed")).isZero();
        assertThat(sentOf("item.converted")).isZero();
        assertThat(sentOf("item.progress")).isEqualTo(1);
        assertThat(registry.completed(List.of(item))).isEmpty();
        MessageOpener opener = new MessageOpener(
                Map.of("transform", downstream.getPrivate()),
                Map.of("convert", (ECPublicKey) signer.getPublic()));
        ConnectorMessage.ItemProgressed progress = null;
        for (OutboundMessage message : sent)
        {
            if (opener.open(message.body())
                    instanceof ConnectorMessage.ItemProgressed opened)
            {
                progress = opened;
            }
        }
        assertThat(progress).isNotNull();
        assertThat(progress.stage()).isEqualTo("convert");
        assertThat(progress.intake()).isEqualTo("claims");
        assertThat(progress.producerRef()).isEqualTo("claims.csv");
        assertThat(progress.recordCount()).isEqualTo(2);
        assertThat(progress.passedCount()).isEqualTo(1);
        assertThat(progress.openCount()).isEqualTo(1);
        assertThat(registry.failureStore().failures(item)).singleElement()
                .satisfies(stored ->
                {
                    assertThat(stored.failure().seq()).isEqualTo(1);
                    assertThat(stored.failure().errorCode())
                            .isEqualTo("CONVERT_CSV_FIELD_COUNT");
                    assertThat(stored.state()).isEqualTo(
                            com.sdh.healthcare.connector.core.failure
                                    .FailureStore.State.FAILED);
                });

        // Delivered again, nothing new: no second item.progress.
        assertThat(worker.handle(List.of(received(messages.get(0)),
                received(total), received(messages.get(1)))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentOf("item.progress")).isEqualTo(1);
    }

    /**
     * An operator's correction of a record that failed conversion is
     * converted by the worker as the record was (STAGES.md §15.6c): stored
     * as a new row of records, marked correction_of the failed one, which
     * stays; sent on as record.stored; the failure CORRECTED; and the item
     * completed with both records passed. A replay sends the corrected
     * record too, and counts nothing failed.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void convertsAnOperatorsCorrection() throws Exception
    {
        List<RecordSplit> messages = split("1,2", "3");
        ItemSplit total = new ItemSplit(item, "claims", 2, 0, 20, "digest",
                "claims.csv", null, null, SPLIT_AT, SPLIT_AT);
        assertThat(worker.handle(List.of(received(messages.get(0)),
                received(total), received(messages.get(1)))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentOf("item.progress")).isEqualTo(1);
        CommittedRecords.Row failed = CommittedRecords.all(records, item)
                .stream().filter(row -> row.errorCode() != null)
                .findFirst().orElseThrow();

        // A header and the record, as the converter takes it: synthetic.
        registry.correctionStore().submit(item, 1,
                "a,b\n3,4\n".getBytes(StandardCharsets.UTF_8), kms, KEK,
                "alice");
        worker.applyCorrections();
        worker.revisit();

        records.refresh();
        assertThat(CommittedRecords.all(records, item)).hasSize(2)
                .anySatisfy(row -> assertThat(row.recordId())
                        .isEqualTo(failed.recordId()));
        assertThat(CommittedRecords.corrections(records, item))
                .singleElement().satisfies(row ->
                {
                    assertThat(row.seq()).isEqualTo(1);
                    assertThat(row.errorCode()).isNull();
                    assertThat(row.correctionOf())
                            .isEqualTo(failed.recordId());
                });
        assertThat(sentOf("record.stored")).isEqualTo(2);
        assertThat(sentOf("item.converted")).isEqualTo(1);
        assertThat(registry.failureStore().failure(item, 1)).get()
                .extracting(FailureStore.Stored::state)
                .isEqualTo(FailureStore.State.CORRECTED);
        MessageOpener opener = new MessageOpener(
                Map.of("transform", downstream.getPrivate()),
                Map.of("convert", (ECPublicKey) signer.getPublic()));
        for (OutboundMessage message : sent)
        {
            if (opener.open(message.body())
                    instanceof ItemConverted converted)
            {
                assertThat(converted.recordCount()).isEqualTo(2);
                assertThat(converted.failedRecordCount()).isZero();
            }
        }

        List<ConnectorMessage> again = ConvertProcessor.replayMessages(item,
                records, "prod", NS, "claims", 2, 1, SPLIT_AT);
        assertThat(again).hasSize(3);
        assertThat(((ItemConverted) again.get(2)).failedRecordCount())
                .isZero();
    }

    /**
     * A replay's messages are the ones the stage first sent, rebuilt from
     * {@code records} and the registry: each record's {@code record.stored}
     * or {@code record.failed}, the same but for the snapshot named, then
     * {@code item.converted} with the same counts and intake, at the time
     * the registry says the item completed (STAGES.md §13.2).
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aReplayRebuildsTheMessagesFirstSent() throws Exception
    {
        List<RecordSplit> messages = split("1,2", "3,4");
        ItemSplit total = new ItemSplit(item, "claims", 2, 0, 20, "digest",
                null, null, null, SPLIT_AT, SPLIT_AT);
        assertThat(worker.handle(List.of(received(messages.get(0)),
                received(total), received(messages.get(1)))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        MessageOpener opener = new MessageOpener(
                Map.of("transform", downstream.getPrivate()),
                Map.of("convert", (ECPublicKey) signer.getPublic()));
        List<ConnectorMessage> first = new ArrayList<>();
        for (OutboundMessage message : sent)
        {
            first.add(opener.open(message.body()));
        }

        RegistryReport.Item done = new RegistryReport(
                postgres.getPostgresDatabase(), schema, ConvertProcessor.STAGE)
                .item(item).orElseThrow();
        records.refresh();
        List<ConnectorMessage> again = ConvertProcessor.replayMessages(item,
                records, "prod", NS, total.intake(), done.done(),
                done.failed(), done.completedAt());

        assertThat(again).hasSize(first.size());
        assertThat(again.subList(0, again.size() - 1))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields(
                        "storage.snapshotId")
                .containsExactlyInAnyOrderElementsOf(
                        first.subList(0, first.size() - 1));
        assertThat(again.get(again.size() - 1)).usingRecursiveComparison()
                .ignoringFields("completedAt")
                .isEqualTo(first.get(first.size() - 1));
        assertThat(((ItemConverted) again.get(again.size() - 1))
                .completedAt()).isEqualTo(done.completedAt());
    }

    /**
     * A record an operator's correction made at the split stage, whose
     * {@code record.split} never came, is rebuilt by the reconciler as any
     * other (STAGES.md §15.6d).
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void reconcilesARecordCorrectedAtTheSplitStage() throws Exception
    {
        List<RecordSplit> messages = split("1,2");
        RecordBatchWriter writer = new RecordBatchWriter(splitRecords, kms,
                KEK, new RecordIds(new byte[32]), SPLIT_AT);
        writer.correction(item, 1, "the-failed-piece");
        writer.add(new RecordInput(item, 1, 8, "csv",
                "a,b\n3,4\n".getBytes(UTF_8)));
        splitRecords.newAppend().appendFile(writer.finish()).commit();
        String corrected = writer.written().get(0).recordId();
        assertThat(worker.handleOpened(List.of(messages.get(0))))
                .isEqualTo(BatchHandler.Outcome.DONE);

        assertThat(new ConvertReconciler(splitRecords, registry, worker,
                processor, "prod", NS, Duration.ofMinutes(10),
                Duration.ofDays(2),
                Clock.fixed(SPLIT_AT.plusSeconds(3600), ZoneOffset.UTC))
                .reconcile()).isEqualTo(1);
        assertThat(registry.knownRecords(item)).containsExactlyInAnyOrder(
                messages.get(0).recordId(), corrected);
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
     * A record another pod claimed and never stored, its message lost with
     * that pod's batch, is left while the claim lives, then rebuilt once it
     * has expired and converted as a second attempt; the item completes.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void reconcilesARecordLeftClaimed() throws Exception
    {
        List<RecordSplit> messages = split("1,2", "3,4");
        assertThat(worker.handleOpened(List.of(messages.get(0))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        ItemSplit total = new ItemSplit(item, "claims", 2, 0, 20, "digest",
                null, null, null, SPLIT_AT, SPLIT_AT);
        assertThat(worker.handle(List.of(received(total))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        StageRegistry stopped = new StageRegistry(
                postgres.getPostgresDatabase(), schema, ConvertProcessor.STAGE,
                "pod-z", Duration.ofSeconds(10));
        String held = messages.get(1).recordId();
        stopped.claim(held, item, Duration.ofMinutes(5));
        ConvertReconciler reconciler = reconcilerAt(SPLIT_AT.plusSeconds(3600));
        assertThat(reconciler.reconcile()).as("its claim lives").isZero();

        stopped.release();
        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(registry.item(item)).hasValueSatisfying(
                progress -> assertThat(progress.done()).isEqualTo(2));
        assertThat(sentOf("record.stored")).isEqualTo(2);
        assertThat(sentOf("item.converted")).isEqualTo(1);
        assertThat(registry.claim(held, item, Duration.ofMinutes(1)))
                .isInstanceOfSatisfying(StageRegistry.Stored.class,
                        stored -> assertThat(stored.attempts()).isEqualTo(2));
    }

    /**
     * An item whose records are all stored and whose total came, but which
     * the batch that would have completed it lost, is completed by the
     * reconciler, and audited; an item still short of its total isn't.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void completesAnItemWhoseCompletionWasLost() throws Exception
    {
        List<RecordSplit> messages = split("1,2", "3,4");
        assertThat(worker.handleOpened(List.of(messages.get(0))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        ItemSplit total = new ItemSplit(item, "claims", 2, 0, 20, "digest",
                null, null, null, SPLIT_AT, SPLIT_AT);
        // As a batch that took the total, then failed before completing.
        registry.expect(item, 2, received(total).body());
        stillShortOfItsTotal();
        assertThat(worker.handleOpened(List.of(messages.get(1))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentOf("item.converted")).as("its own batch completes it")
                .isEqualTo(1);

        InMemoryLogRecordExporter events = InMemoryLogRecordExporter.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build())
        {
            Telemetry.install(sdk);
            UUID lost = UUID.randomUUID();
            List<RecordSplit> lostRecords = splitFor(lost, "5,6");
            assertThat(worker.handleOpened(List.of(lostRecords.get(0))))
                    .isEqualTo(BatchHandler.Outcome.DONE);
            registry.expect(lost, 1, received(new ItemSplit(lost, "claims", 1,
                    0, 10, "digest", null, null, null, SPLIT_AT, SPLIT_AT))
                    .body());
            assertThat(reconcilerAt(SPLIT_AT.plusSeconds(3600)).reconcile())
                    .isZero();
            assertThat(registry.completed(List.of(lost))).containsExactly(lost);
            assertThat(events.getFinishedLogRecordItems().stream()
                    .map(event -> event.getEventName())
                    .filter(name -> name.contains("completion_recovered")))
                    .containsExactly(
                            "sdh.audit.convert.item_completion_recovered");
        }
        finally
        {
            Telemetry.install(OpenTelemetry.noop());
        }
        assertThat(sentOf("item.converted")).isEqualTo(2);
    }

    /**
     * Runs the reconciler on an item one record short of its total: it is
     * left, not completed.
     *
     * @throws Exception if the test fails to run
     */
    void stillShortOfItsTotal() throws Exception
    {
        // The second record's message is still to come, younger than the
        // grace: nothing to rebuild or complete yet.
        assertThat(reconcilerAt(SPLIT_AT.plusSeconds(60)).reconcile())
                .isZero();
        assertThat(sentOf("item.converted")).isZero();
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
     * A record off the queue is noted as backlog only if it was committed
     * more than the grace ago; one the reconciler rebuilt isn't; and a
     * snapshot no longer in the table was expired, so is old.
     */
    @Test
    void notesABacklogOnlyForOldRecordsOffTheQueue()
    {
        List<RecordSplit> messages = split("1,2", "3,4");
        // The snapshot is stamped with the real time, not SPLIT_AT.
        Instant now = Instant.now();
        processorAt(now).classify(messages.get(0));
        assertThat(registry.sinceBacklog()).as("committed just now")
                .isEmpty();

        ConvertProcessor later = processorAt(now.plus(Duration.ofHours(1)));
        later.rebuilding(List.of(messages.get(0)));
        later.classify(messages.get(0));
        assertThat(registry.sinceBacklog()).as("rebuilt").isEmpty();
        later.rebuilt(List.of(messages.get(0)));
        later.classify(messages.get(0));
        assertThat(registry.sinceBacklog()).as("an hour old").isPresent();

        BacklogWatch watch = new BacklogWatch(
                IngestTables.splitRecords(catalog, NS), registry,
                Duration.ofMinutes(10), Clock.systemUTC());
        assertThat(watch.committedAt(-1L)).hasValue(Instant.EPOCH);
        assertThat(watch.committedAt(messages.get(0).storage().snapshotId()))
                .hasValueSatisfying(at -> assertThat(at)
                        .isBetween(now.minusSeconds(60), now));
    }

    /**
     * While a backlog is coming off the queue, a run rebuilds nothing and
     * leaves the watermark; after waiting the most it may, it goes ahead,
     * then waits again.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void waitsForABacklogToDrainThenGoesAheadAnyway() throws Exception
    {
        split("1,2", "3,4");
        registry.noteBacklog();
        InMemoryLogRecordExporter events = InMemoryLogRecordExporter.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build())
        {
            Telemetry.install(sdk);
            ConvertReconciler reconciler = reconcilerAt(
                    SPLIT_AT.plus(Duration.ofHours(1)),
                    new ConvertReconciler.Deferral(Duration.ofMinutes(15),
                            Duration.ofMillis(300)));
            assertThat(reconciler.reconcile()).isZero();
            assertThat(registry.knownRecords(item)).isEmpty();
            assertThat(registry.checkedTo()).as("the watermark stays")
                    .isEmpty();

            Thread.sleep(400);
            assertThat(reconciler.reconcile()).as("waited the most")
                    .isEqualTo(2);
            assertThat(registry.checkedTo()).isPresent();
            assertThat(reconciler.reconcile()).as("waits again").isZero();
            assertThat(deferrals(events)).containsExactly("BACKLOG",
                    "capped", "BACKLOG");
        }
        finally
        {
            Telemetry.install(OpenTelemetry.noop());
        }
        assertThat(sentOf("record.stored")).isEqualTo(2);
    }

    /**
     * At startup, with no backlog seen yet, a run waits one quiet time for
     * the pods to start receiving, then goes ahead; a wait left by an
     * earlier duties holder doesn't count.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void waitsAtStartupForTheQueueToStartDelivering() throws Exception
    {
        split("1,2", "3,4");
        // An earlier holder's backlog, long quiet, and its wait, left.
        registry.noteBacklog();
        registry.deferring();
        Thread.sleep(300);
        InMemoryLogRecordExporter events = InMemoryLogRecordExporter.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build())
        {
            Telemetry.install(sdk);
            ConvertReconciler reconciler = reconcilerAt(
                    SPLIT_AT.plus(Duration.ofHours(1)),
                    new ConvertReconciler.Deferral(Duration.ofMillis(250),
                            Duration.ofHours(6)));
            assertThat(reconciler.reconcile()).isZero();
            assertThat(registry.knownRecords(item)).isEmpty();
            Thread.sleep(300);
            assertThat(reconciler.reconcile()).isEqualTo(2);
            assertThat(reconciler.reconcile()).as("no wait now").isZero();
            assertThat(deferrals(events)).containsExactly("STARTING");
        }
        finally
        {
            Telemetry.install(OpenTelemetry.noop());
        }
    }

    /**
     * At startup on a stage where no backlog has ever been seen, so the
     * registry has no row for it yet, the wait still ends after one quiet
     * time. The sandbox found it didn't: the wait was never recorded.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void endsTheWaitAtStartupWithNoBacklogEverSeen() throws Exception
    {
        split("1,2", "3,4");
        ConvertReconciler reconciler = reconcilerAt(
                SPLIT_AT.plus(Duration.ofHours(1)),
                new ConvertReconciler.Deferral(Duration.ofMillis(250),
                        Duration.ofHours(6)));
        assertThat(reconciler.reconcile()).isZero();
        Thread.sleep(300);
        assertThat(reconciler.reconcile()).isEqualTo(2);
    }

    /**
     * The reconciler's waits audited, by reason, and {@code capped} for
     * each time it went ahead after waiting the most.
     *
     * @param events the audit events
     * @return them in order
     */
    static List<String> deferrals(InMemoryLogRecordExporter events)
    {
        return events.getFinishedLogRecordItems().stream()
                .filter(event -> event.getEventName()
                        .startsWith("sdh.audit.convert.reconcile_defer"))
                .map(event -> event.getEventName().endsWith("capped")
                        ? "capped"
                        : event.getAttributes()
                                .get(AttributeKey.stringKey("sdh.reason")))
                .toList();
    }

    /**
     * A processor like the test's, with a backlog watch at a time.
     *
     * @param now the time the watch sees
     * @return the processor
     */
    ConvertProcessor processorAt(Instant now)
    {
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        return new ConvertProcessor(reader, ConverterRegistry.load(),
                new ConvertProcessor.Settings(Map.of("csv.header", "true"),
                        1 << 20, 64L << 20, "prod", NS, "test-build"),
                new ConvertProcessor.Storage(records, kms, KEK,
                        new RecordIds(new byte[32])),
                null, clock, new BacklogWatch(
                        IngestTables.splitRecords(catalog, NS), registry,
                        Duration.ofMinutes(10), clock));
    }

    /**
     * The stage's reconciler at a time, with a 10-minute grace, a 2-day
     * lookback and a deferral.
     *
     * @param now      the time it runs at
     * @param deferral when it waits for a backlog
     * @return the reconciler
     */
    ConvertReconciler reconcilerAt(Instant now,
            ConvertReconciler.Deferral deferral)
    {
        return new ConvertReconciler(splitRecords, registry, worker,
                processor, "prod", NS, Duration.ofMinutes(10),
                Duration.ofDays(2), Clock.fixed(now, ZoneOffset.UTC),
                deferral);
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
