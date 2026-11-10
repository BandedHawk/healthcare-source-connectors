/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record;

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
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemResult;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemTransformed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordTransformed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.core.convert.Representation;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.SplitRecord;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.WrittenRecord;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import com.sdh.healthcare.connector.stage.StageRegistry;
import com.sdh.healthcare.connector.stage.StageWorker;
import io.opentelemetry.api.OpenTelemetry;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the library as a stage runs it: the shared worker with the
 * {@link RecordStageProcessor} as the transform stage, from
 * {@code records} to a gold table, its registry on a real Postgres,
 * results on, and the {@link RecordReconciler}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RecordStageTest
{

    /** The namespace every table is in. */
    static final String NS = "sdh_ingest";

    /** The KEK the tables' data keys are wrapped with. */
    static final String KEK = "records-kek/1";

    /** When the records were stored by the convert stage. */
    static final Instant STORED_AT = Instant.parse("2026-11-10T10:00:00Z");

    /** The gold table's name. */
    static final String GOLD = "bundles";

    /** Sends at once, or not at all. */
    static final RetryPolicy RETRY =
            new RetryPolicy(1, Duration.ofMillis(1), Duration.ofMillis(1));

    /** Gives each test its own registry schema. */
    static final AtomicInteger SCHEMAS = new AtomicInteger();

    /** The registry's database. */
    static EmbeddedPostgres postgres;

    /** Signs what the convert stage sends. */
    KeyPair convertSigner;

    /** The transform stage's own key, which its input is sealed to. */
    KeyPair recipient;

    /** Signs what the transform stage sends. */
    KeyPair signer;

    /** The next stage's key, which the output is sealed to. */
    KeyPair downstream;

    /** The producer's key, which results are sealed to. */
    KeyPair producer;

    /** Holds the KEK. */
    AesKeyWrapProvider kms;

    /** The catalog, named "prod". */
    InMemoryCatalog catalog;

    /** The split stage's table, before the source. */
    Table splitRecords;

    /** The source: the convert stage's table. */
    Table records;

    /** The sink. */
    Table gold;

    /** Reads records. */
    QueueReader reader;

    /** The registry's schema for this test. */
    String schema;

    /** The stage's registry. */
    StageRegistry registry;

    /** The stage's processor, as the worker runs it. */
    RecordStageProcessor processor;

    /** The worker. */
    StageWorker<RecordStageProcessor.Prepared> worker;

    /** What the stage sent on. */
    final List<OutboundMessage> sent =
            Collections.synchronizedList(new ArrayList<>());

    /** The results the stage sent. */
    final List<OutboundMessage> results =
            Collections.synchronizedList(new ArrayList<>());

    /** The item. */
    final UUID item = UUID.randomUUID();

    /** Creates the tests; JUnit runs each test on a new instance. */
    RecordStageTest()
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
        convertSigner = ec();
        recipient = ec();
        signer = ec();
        downstream = ec();
        producer = ec();
        KeyGenerator aes = KeyGenerator.getInstance("AES");
        aes.init(256);
        kms = new AesKeyWrapProvider(Map.of(KEK, aes.generateKey()));
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        splitRecords = IngestTables.splitRecords(catalog, NS);
        records = IngestTables.sharedRecords(catalog, NS);
        gold = IngestTables.shared(catalog, NS, GOLD);
        reader = QueueReader.builder().catalog(catalog)
                .expectedCatalogName("prod").keyProvider(kms).build();
        schema = "record_test_" + SCHEMAS.incrementAndGet();
        registry = new StageRegistry(postgres.getPostgresDatabase(), schema,
                StageMessages.TRANSFORM.stage(), "pod-a",
                Duration.ofSeconds(10));
        registry.createTablesIfMissing();
        Clock clock = Clock.fixed(STORED_AT.plusSeconds(3600), ZoneOffset.UTC);
        ResultReporter reporter = new ResultReporter(new ResultsChannel(
                new MessageSealer(new ECDSASigner(
                        (ECPrivateKey) signer.getPrivate()), "transform",
                        producer.getPublic(), "producer"),
                message ->
                {
                    results.add(message);
                    return CompletableFuture.completedFuture(null);
                }, null, RETRY, Duration.ofSeconds(5)), clock);
        processor = new RecordStageProcessor(
                new UpperTransform(StageMessages.TRANSFORM), reader,
                new RecordStageProcessor.Tables(
                        new RecordTable("prod", NS, "records", records),
                        new RecordStageProcessor.Sink(
                                new RecordTable("prod", NS, GOLD, gold), kms,
                                KEK, new RecordIds(new byte[32])),
                        List.of(new RecordStageProcessor.Earlier("split",
                                splitRecords))),
                64L << 20, reporter, clock, null);
        MessagePublisher publisher = message ->
        {
            sent.add(message);
            return CompletableFuture.completedFuture(null);
        };
        worker = new StageWorker<>(processor, registry,
                new MessageOpener(Map.of("transform", recipient.getPrivate()),
                        Map.of("convert",
                                (ECPublicKey) convertSigner.getPublic())),
                new MessageSealer(
                        new ECDSASigner((ECPrivateKey) signer.getPrivate()),
                        "transform", downstream.getPublic(), "load"),
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
     * A message as the convert stage sends it, ready to receive.
     *
     * @param message the message
     * @return it sealed to the transform stage
     * @throws Exception if it can't be sealed
     */
    BrokerMessage received(ConnectorMessage message) throws Exception
    {
        MessageSealer sealer = new MessageSealer(
                new ECDSASigner((ECPrivateKey) convertSigner.getPrivate()),
                "convert", recipient.getPublic(), "transform");
        return new BrokerMessage(sealer.seal(message), message.messageId(),
                Map.of());
    }

    /**
     * Writes records to {@code records} as the convert stage does: those
     * given as null are stored failed, as the convert stage stores a
     * record it couldn't convert, and get no message.
     *
     * @param texts the records' JSON; null for a failed one
     * @return a {@code record.stored} for each stored, null for each failed
     * @throws Exception if one can't be written
     */
    List<RecordStored> converted(String... texts) throws Exception
    {
        RecordBatchWriter writer = new RecordBatchWriter(records, kms, KEK,
                new RecordIds(new byte[32]), STORED_AT);
        long offset = 0;
        for (int seq = 0; seq < texts.length; seq++)
        {
            String text = texts[seq] == null ? "a,b" : texts[seq];
            SplitRecord lineage = SplitRecord.ofRange(offset, text.length(),
                    ResumePoint.at(offset + text.length()));
            if (texts[seq] == null)
            {
                writer.addBytes(item, seq, "csv", lineage, text.getBytes(UTF_8),
                        new Representation("text/csv", null),
                        ErrorCode.CONVERT_CSV_FIELD_COUNT, 0, null);
            }
            else
            {
                writer.addBytes(item, seq, "csv", lineage, text.getBytes(UTF_8),
                        Representation.json("csv/1"), null, -1, null);
            }
            offset += text.length();
        }
        records.newAppend().appendFile(writer.finish()).commit();
        StorageRef ref = new StorageRef("prod", NS, "records",
                writer.ingestedDate(), records.currentSnapshot().snapshotId(),
                writer.kekRef());
        List<RecordStored> messages = new ArrayList<>();
        for (WrittenRecord record : writer.written())
        {
            int seq = (int) record.seq();
            messages.add(texts[seq] == null ? null : new RecordStored(
                    record.recordId(), item, seq, record.sourceOffset(),
                    record.sourceLength(), "csv", Representation.JSON,
                    "csv/1", record.plaintextLength(), record.chunkCount(),
                    ref, null, "convert-v", null, null, false, null, seq,
                    "m" + seq, null));
        }
        return messages;
    }

    /**
     * Writes a piece to {@code split_records} that the split stage
     * couldn't parse, stored failed and never sent on.
     *
     * @param seq its number in the item
     * @throws Exception if it can't be written
     */
    void splitFailed(long seq) throws Exception
    {
        RecordBatchWriter writer = new RecordBatchWriter(splitRecords, kms,
                KEK, new RecordIds(new byte[32]), STORED_AT);
        writer.addBytes(item, seq, "csv", SplitRecord.ofRange(100, 2,
                ResumePoint.at(102)), "\"x".getBytes(UTF_8),
                new Representation("text/csv", null),
                ErrorCode.PARSE_UNTERMINATED_QUOTE, 0, null);
        splitRecords.newAppend().appendFile(writer.finish()).commit();
    }

    /**
     * The convert stage's item end.
     *
     * @param records     the records it handled
     * @param failed      how many of them failed there
     * @param resultsSent whether it sent the producer's result
     * @return the message
     */
    ItemConverted total(long records, long failed, boolean resultsSent)
    {
        return new ItemConverted(item, "claims", records, failed, STORED_AT,
                List.of("convert-v"), 1, 1, null, "claims.csv", null, null,
                resultsSent);
    }

    /**
     * The messages sent on, opened.
     *
     * @return them, in order
     * @throws Exception if one can't be opened
     */
    List<ConnectorMessage> opened() throws Exception
    {
        MessageOpener opener = new MessageOpener(
                Map.of("load", downstream.getPrivate()),
                Map.of("transform", (ECPublicKey) signer.getPublic()));
        List<ConnectorMessage> opened = new ArrayList<>();
        for (OutboundMessage message : sent)
        {
            opened.add(opener.open(message.body()));
        }
        return opened;
    }

    /**
     * The results sent, opened.
     *
     * @return them, in order
     * @throws Exception if one can't be opened
     */
    List<ItemResult> resultsSent() throws Exception
    {
        MessageOpener opener = new MessageOpener(
                Map.of("producer", producer.getPrivate()),
                Map.of("transform", (ECPublicKey) signer.getPublic()));
        List<ItemResult> opened = new ArrayList<>();
        for (OutboundMessage message : results)
        {
            opened.add((ItemResult) opener.open(message.body()));
        }
        return opened;
    }

    /**
     * Records are mapped into the sink and announced as
     * {@code record.transformed}; the item ends once every record the
     * convert stage sent on is stored, one it failed not waited for, and
     * the producer's result puts each problem down to the stage whose
     * table it failed in, counting every record of the item.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void mapsTheRecordsSentOnAndReportsEveryStagesProblems() throws Exception
    {
        List<RecordStored> messages = converted("{\"a\":1}", "{\"b\":2}",
                null);
        splitFailed(3);
        assertThat(worker.handle(List.of(received(messages.get(0)),
                received(total(3, 1, false)), received(messages.get(1)))))
                .isEqualTo(BatchHandler.Outcome.DONE);

        List<ConnectorMessage> opened = opened();
        assertThat(opened).filteredOn(RecordTransformed.class::isInstance)
                .hasSize(2).allSatisfy(message -> assertThat(
                        ((RecordTransformed) message).storage().table())
                        .isEqualTo(GOLD));
        assertThat(opened).filteredOn(ItemTransformed.class::isInstance)
                .singleElement().satisfies(message ->
                {
                    ItemTransformed end = (ItemTransformed) message;
                    assertThat(end.recordCount()).isEqualTo(2);
                    assertThat(end.failedRecordCount()).isZero();
                    assertThat(end.versions())
                            .containsExactly(UpperTransform.VERSION);
                    assertThat(end.producerRef()).isEqualTo("claims.csv");
                    assertThat(end.resultsSent()).isTrue();
                });
        assertThat(registry.completed(List.of(item))).containsExactly(item);
        assertThat(CommittedRecords.all(gold, item)).hasSize(2);

        assertThat(resultsSent()).singleElement().satisfies(result ->
        {
            assertThat(result.status()).isEqualTo(ItemResult.COMPLETED);
            assertThat(result.recordCount()).isEqualTo(4);
            assertThat(result.failedRecordCount()).isEqualTo(2);
            assertThat(result.problems()).extracting(Problem::stage,
                    Problem::code, Problem::seq).containsExactly(
                            org.assertj.core.groups.Tuple.tuple("convert",
                                    "CONVERT_CSV_FIELD_COUNT", 2L),
                            org.assertj.core.groups.Tuple.tuple("split",
                                    "PARSE_UNTERMINATED_QUOTE", 3L));
        });
    }

    /**
     * A record that fails here is stored in the sink as it came and kept
     * in the stage's fail store; the item gets {@code item.progress} from
     * this stage, and the producer an {@code incomplete} result naming it.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void keepsAFailedRecordAndReportsTheItemIncomplete() throws Exception
    {
        List<RecordStored> messages = converted("{\"a\":1}", "{\"bad\":1}");
        assertThat(worker.handle(List.of(received(messages.get(0)),
                received(total(2, 0, false)), received(messages.get(1)))))
                .isEqualTo(BatchHandler.Outcome.DONE);

        assertThat(opened()).filteredOn(ItemProgressed.class::isInstance)
                .singleElement().satisfies(message ->
                {
                    ItemProgressed progress = (ItemProgressed) message;
                    assertThat(progress.stage()).isEqualTo("transform");
                    assertThat(progress.intake()).isEqualTo("claims");
                    assertThat(progress.openCount()).isEqualTo(1);
                });
        assertThat(registry.completed(List.of(item))).isEmpty();
        assertThat(registry.failureStore().failures(item)).singleElement()
                .satisfies(stored -> assertThat(stored.failure().errorCode())
                        .isEqualTo("CONVERT_CSV_MALFORMED"));
        assertThat(resultsSent()).singleElement().satisfies(result ->
        {
            assertThat(result.status()).isEqualTo(ItemResult.INCOMPLETE);
            assertThat(result.problems()).singleElement().satisfies(
                    problem ->
                    {
                        assertThat(problem.stage()).isEqualTo("transform");
                        assertThat(problem.messageRef()).isEqualTo("m1");
                    });
        });
    }

    /**
     * An item whose stage before sent the producer's result already gets
     * a second, audited: results are meant for one stage of a pipeline.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void auditsResultsSentTwice() throws Exception
    {
        List<RecordStored> messages = converted("{\"a\":1}");
        InMemoryLogRecordExporter events = InMemoryLogRecordExporter.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build())
        {
            Telemetry.install(sdk);
            assertThat(worker.handle(List.of(received(messages.get(0)),
                    received(total(1, 0, true)))))
                    .isEqualTo(BatchHandler.Outcome.DONE);
            assertThat(events.getFinishedLogRecordItems().stream()
                    .map(event -> event.getEventName())
                    .filter(name -> name.endsWith("results_twice")))
                    .containsExactly("sdh.audit.stage.results_twice");
        }
        finally
        {
            Telemetry.install(OpenTelemetry.noop());
        }
        assertThat(resultsSent()).hasSize(1);
    }

    /**
     * Records whose messages were lost are rebuilt from the source as
     * {@code record.stored}, named by the version that stored them, and
     * transformed; the item then ends once its total comes.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void reconcilesRecordsWhoseMessagesWereLost() throws Exception
    {
        List<RecordStored> messages = converted("{\"a\":1}", "{\"b\":2}");
        assertThat(worker.handleOpened(List.of(messages.get(0))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        RecordReconciler reconciler = new RecordReconciler(
                new RecordTable("prod", NS, "records", records),
                StageMessages.TRANSFORM, registry, worker, processor,
                Duration.ofMinutes(10), Duration.ofDays(2),
                Clock.fixed(STORED_AT.plusSeconds(3600), ZoneOffset.UTC));

        assertThat(reconciler.reconcile()).isEqualTo(1);
        assertThat(registry.knownRecords(item)).containsExactlyInAnyOrder(
                messages.get(0).recordId(), messages.get(1).recordId());
        assertThat(opened()).filteredOn(RecordTransformed.class::isInstance)
                .hasSize(2);
        assertThat(CommittedRecords.all(gold, item)).hasSize(2);

        assertThat(worker.handle(List.of(received(total(2, 0, false)))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(opened()).filteredOn(ItemTransformed.class::isInstance)
                .hasSize(1);
        assertThat(registry.completed(List.of(item))).containsExactly(item);
    }

    /**
     * A record off the queue is noted as backlog only if it was committed
     * more than the grace ago; one the reconciler rebuilt isn't; and a
     * snapshot no longer in the table was expired, so is old. Moved from
     * the convert stage's tests.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void notesABacklogOnlyForOldRecordsOffTheQueue() throws Exception
    {
        List<RecordStored> messages = converted("{\"a\":1}");
        // The snapshot is stamped with the real time, not STORED_AT.
        Instant now = Instant.now();
        processorAt(now).classify(messages.get(0));
        assertThat(registry.sinceBacklog()).as("committed just now")
                .isEmpty();

        RecordStageProcessor later =
                processorAt(now.plus(Duration.ofHours(1)));
        later.rebuilding(List.of(messages.get(0)));
        later.classify(messages.get(0));
        assertThat(registry.sinceBacklog()).as("rebuilt").isEmpty();
        later.rebuilt(List.of(messages.get(0)));
        later.classify(messages.get(0));
        assertThat(registry.sinceBacklog()).as("an hour old").isPresent();

        BacklogWatch watch = new BacklogWatch(records, registry,
                Duration.ofMinutes(10), Clock.systemUTC());
        assertThat(watch.committedAt(-1L)).hasValue(Instant.EPOCH);
        assertThat(watch.committedAt(messages.get(0).storage().snapshotId()))
                .hasValueSatisfying(at -> assertThat(at)
                        .isBetween(now.minusSeconds(60), now));
    }

    /**
     * A catching-up run goes a day at a time: the days it covers are
     * counted whole. Moved from the convert stage's tests.
     */
    @Test
    void countsTheDaysACatchUpCovers()
    {
        Instant after = STORED_AT.plus(Duration.ofDays(5));
        assertThat(RecordReconciler.days(after, after)).isZero();
        assertThat(RecordReconciler.days(after,
                after.plus(Duration.ofHours(25)))).isEqualTo(2);
    }

    /**
     * A processor like the test's, with no results and a backlog watch at
     * a time.
     *
     * @param now the time the watch sees
     * @return the processor
     */
    RecordStageProcessor processorAt(Instant now)
    {
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        return new RecordStageProcessor(
                new UpperTransform(StageMessages.TRANSFORM), reader,
                new RecordStageProcessor.Tables(
                        new RecordTable("prod", NS, "records", records),
                        new RecordStageProcessor.Sink(
                                new RecordTable("prod", NS, GOLD, gold), kms,
                                KEK, new RecordIds(new byte[32])),
                        List.of()),
                64L << 20, null, clock, new BacklogWatch(records, registry,
                        Duration.ofMinutes(10), clock));
    }
}
