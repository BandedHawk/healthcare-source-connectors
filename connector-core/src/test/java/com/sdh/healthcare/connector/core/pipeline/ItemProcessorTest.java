/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.landing.LandedRefs;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.landing.MessageIndex;
import com.sdh.healthcare.connector.core.pipeline.IngestPipelineTest.RecordingPublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import java.io.ByteArrayInputStream;
import java.security.KeyPair;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ItemProcessor}: items landed by a real
 * {@link ItemLander}, read back through {@link LandedItemSource}, split by
 * a real pipeline on an in-memory catalog.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ItemProcessorTest
{

    /** The records KEK. */
    static final String RECORDS_KEK = "records-kek/1";

    /** The landing KEK. */
    static final String LANDED_KEK = "landed-items-kek/1";

    /** The split records table. */
    Table records;

    /** The schema of this test's checkpoint table. */
    final String schema = SplitTestSupport.freshSchema();

    /** The landed items table. */
    Table landed;

    /** Wraps and unwraps data keys for both. */
    AesKeyWrapProvider keys;

    /** The signing key pair. */
    KeyPair signer;

    /** Opens what the pipeline publishes. */
    MessageOpener opener;

    /** The recipient's key pair. */
    KeyPair recipient;

    /** Records what the pipeline publishes. */
    final RecordingPublisher publisher = new RecordingPublisher();

    /** The last lease epoch handed out. */
    long epochs;

    /** Pipelines to close after each test. */
    final List<IngestPipeline> open = new ArrayList<>();

    /** Lands items for the tests. */
    ItemLander lander;

    /**
     * Creates the tables, keys and lander.
     *
     * @throws Exception if a key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        records = IngestTables.splitRecords(catalog, "sdh_split");
        landed = LandingTables.landedItems(catalog, "sdh_landing");
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(RECORDS_KEK,
                generator.generateKey(), LANDED_KEK, generator.generateKey()));
        signer = IngestPipelineTest.ec();
        recipient = IngestPipelineTest.ec();
        opener = new MessageOpener(Map.of("downstream", recipient.getPrivate()),
                Map.of("connector", (ECPublicKey) signer.getPublic()));
        FencedCommitter landing = committer();
        landing.establish(landed);
        lander = new ItemLander(landed, landing, keys,
                new RecordIds(new byte[32]),
                ItemLander.Settings.defaults("claims", LANDED_KEK)
                        .withSizes(64, 256, 256), Clock.systemUTC());
    }

    /** Closes the pipelines. */
    @AfterEach
    void tearDown()
    {
        open.forEach(IngestPipeline::close);
    }

    /**
     * A committer under a new lease epoch.
     *
     * @return the committer
     */
    FencedCommitter committer()
    {
        long epoch = ++epochs;
        return new FencedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return epoch;
            }

            @Override
            public void checkHeld()
            {
            }
        });
    }

    /**
     * A processor as a fresh start of a split worker would make it.
     *
     * @return the processor
     * @throws Exception if the signer can't be made
     */
    ItemProcessor processor() throws Exception
    {
        return processor(Interruptions.NONE);
    }

    /**
     * A processor that looks for newer generations as given.
     *
     * @param interruptions says when a newer generation is queued
     * @return the processor
     * @throws Exception if the signer can't be made
     */
    ItemProcessor processor(Interruptions interruptions) throws Exception
    {
        RecordIds ids = new RecordIds(IdKeys.loadOrCreateShared(records,
                keys, RECORDS_KEK));
        PipelineSettings settings = PipelineSettings.defaults("splitter",
                "csv", "prod", RECORDS_KEK)
                .withCommit(3, 1L << 30, Duration.ofHours(1)).withWorkers(2)
                .withRetry(new RetryPolicy(2, Duration.ofMillis(1),
                        Duration.ofMillis(2)));
        IngestPipeline pipeline = new IngestPipeline(settings, records,
                SplitTestSupport.checkpoints(schema, ids), keys, ids,
                SplitterRegistry.load(),
                new MessageSealer(new ECDSASigner((ECPrivateKey) signer
                        .getPrivate()), "connector", recipient.getPublic(),
                        "downstream"), publisher, Clock.systemUTC());
        open.add(pipeline);
        pipeline.setInterruptions(interruptions);
        return new ItemProcessor(pipeline, "claims", failures(),
                corrections());
    }

    /**
     * The split stage's correction store, in the test's schema.
     *
     * @return the store
     */
    CorrectionStore corrections()
    {
        CorrectionStore corrections = new CorrectionStore(
                SplitTestSupport.database(), schema, "split",
                Duration.ofSeconds(10));
        corrections.createTableIfMissing();
        return corrections;
    }

    /**
     * Submits a correction as an operator would, and takes it as a pod
     * would, pending.
     *
     * @param itemId  the item
     * @param seq     the failure's record number
     * @param content the corrected text
     * @return the pending correction
     */
    CorrectionStore.Pending submit(UUID itemId, long seq, String content)
    {
        return submit(itemId, seq, content, CorrectionStore.Kind.CORRECTION);
    }

    /**
     * Submits a correction or a retry, and returns it as pending.
     *
     * @param itemId  the failure's item
     * @param seq     its record number
     * @param content what is submitted
     * @param kind    a correction or a retry
     * @return the pending submission
     */
    CorrectionStore.Pending submit(UUID itemId, long seq, String content,
            CorrectionStore.Kind kind)
    {
        CorrectionStore corrections = corrections();
        corrections.submit(itemId, seq, content.getBytes(UTF_8), keys,
                RECORDS_KEK, "op-test", kind);
        return corrections.pending(10).stream()
                .filter(pending -> pending.itemId().equals(itemId))
                .reduce((first, second) -> second).orElseThrow();
    }

    /**
     * A batch with its middle message unparseable.
     *
     * @param offsets each message's offset, filled in
     * @return the landed item
     * @throws Exception if landing fails
     */
    LandedItem batchWithABadMessage(List<Long> offsets) throws Exception
    {
        String[] messages = {"1,ok\n", "2,\"never closed\n", "3,ok\n"};
        StringBuilder joined = new StringBuilder();
        for (String message : messages)
        {
            offsets.add((long) joined.length());
            joined.append(message);
        }
        byte[] bytes = joined.toString().getBytes(UTF_8);
        return lander.land(new NewItem(
                UuidV7.next(System.currentTimeMillis()),
                ItemKind.MESSAGE_BATCH, null, null, null,
                new MessageIndex(offsets, Arrays.asList(null, null, null))),
                offset -> new ByteArrayInputStream(bytes, (int) offset,
                        bytes.length));
    }

    /**
     * The split stage's fail store, in the test's schema.
     *
     * @return the store
     */
    FailureStore failures()
    {
        FailureStore failures = new FailureStore(SplitTestSupport.database(),
                schema, "split", Duration.ofSeconds(10));
        failures.createTableIfMissing();
        return failures;
    }

    /**
     * Lands some text.
     *
     * @param content the text
     * @return the landed item
     * @throws Exception if landing fails
     */
    LandedItem land(String content) throws Exception
    {
        byte[] bytes = content.getBytes(UTF_8);
        return lander.land(new NewItem(UuidV7.next(System.currentTimeMillis()),
                ItemKind.FILE, null, "drop/smith.csv", null),
                offset -> new ByteArrayInputStream(bytes, (int) offset,
                        bytes.length));
    }

    /**
     * Processes a landed item.
     *
     * @param processor the processor
     * @param item      the item
     * @return the outcome
     * @throws Exception if processing throws
     */
    ItemProcessor.Outcome process(ItemProcessor processor, LandedItem item)
            throws Exception
    {
        return processor.process(item.itemId(), item.digest(),
                () -> LandedItemSource.open(landed, item.itemId(), keys),
                lease());
    }

    /**
     * The item's lease, at a higher epoch than any before, as a worker
     * taking the item over would hold.
     *
     * @return the lease
     */
    SplitTestSupport.TestLease lease()
    {
        return new SplitTestSupport.TestLease(++epochs);
    }

    /**
     * CSV rows whose quoted notes span lines, so they are long enough to
     * cross chunks.
     *
     * @param rows how many rows
     * @return the text
     */
    static String csv(int rows)
    {
        StringBuilder text = new StringBuilder();
        for (int row = 0; row < rows; row++)
        {
            text.append(row).append(",patient-").append(row)
                    .append(",\"note\n").append(row).append("\"\n");
        }
        return text.toString();
    }

    /**
     * Opens what was published.
     *
     * @return the messages, in order
     * @throws Exception if one can't be opened
     */
    List<ConnectorMessage> messages() throws Exception
    {
        List<ConnectorMessage> messages = new ArrayList<>();
        synchronized (publisher.sent)
        {
            for (OutboundMessage message : publisher.sent)
            {
                messages.add(opener.open(message.body()));
            }
        }
        return messages;
    }

    /**
     * An item is split under its item ID, and {@code item.split} reports
     * its landed digest. Processing it again sends {@code item.split} again,
     * without reading the item.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void completesAnItemUnderItsId() throws Exception
    {
        LandedItem item = land(csv(10));
        ItemProcessor processor = processor();
        ItemProcessor.Outcome outcome = process(processor, item);
        assertThat(outcome.result()).isEqualTo(ItemProcessor.Result.COMPLETED);
        assertThat(outcome.itemId()).isEqualTo(item.itemId());
        assertThat(outcome.recordCount()).isEqualTo(10);

        List<ConnectorMessage> messages = messages();
        assertThat(messages.stream().filter(RecordSplit.class::isInstance))
                .hasSize(10).allSatisfy(record -> assertThat(record.itemId())
                        .isEqualTo(item.itemId()));
        assertThat(messages.getLast()).isInstanceOfSatisfying(
                ItemSplit.class, split ->
                {
                    assertThat(split.itemDigest()).isEqualTo(item.digest());
                    assertThat(split.itemSize()).isEqualTo(item.size());
                    assertThat(split.intake()).isEqualTo("claims");
                    assertThat(split.recordCount()).isEqualTo(10);
                    assertThat(split.failure()).isNull();
                });

        int sent = publisher.sent.size();
        assertThat(processor().process(item.itemId(), item.digest(), () ->
        {
            throw new AssertionError("a finished item isn't read");
        }, lease()).result()).isEqualTo(ItemProcessor.Result.COMPLETED);
        assertThat(publisher.sent).hasSize(sent + 1);
        assertThat(messages().getLast().messageId())
                .isEqualTo(messages.getLast().messageId());
    }

    /**
     * The end of an item is audited as at every stage, once: with the
     * split stage's name, how it ended and its counts, so one query finds
     * an item that ended here and not at the next stage. Announcing a
     * finished item again doesn't audit it again.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void auditsTheEndOfAnItemOnceAsEveryStageDoes() throws Exception
    {
        InMemoryLogRecordExporter events = InMemoryLogRecordExporter.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build())
        {
            Telemetry.install(sdk);
            LandedItem done = land(csv(10));
            process(processor(), done);
            process(processor(), done);
            LandedItem broken = land("1,ok\n2,\"never closed\n");
            process(processor(), broken);

            List<LogRecordData> ended = events.getFinishedLogRecordItems()
                    .stream().filter(event -> "sdh.audit.stage.item_completed"
                            .equals(event.getEventName()))
                    .toList();
            assertThat(ended).hasSize(2);
            assertThat(ended.get(0).getAttributes().asMap())
                    .containsEntry(AttributeKey.stringKey("sdh.stage"),
                            "split")
                    .containsEntry(AttributeKey.stringKey("sdh.item_id"),
                            done.itemId().toString())
                    .containsEntry(AttributeKey.stringKey("sdh.result"),
                            "COMPLETED")
                    .containsEntry(AttributeKey.longKey("sdh.count"), 10L)
                    .containsEntry(AttributeKey.longKey("sdh.failed_count"),
                            0L);
            assertThat(ended.get(1).getAttributes().asMap())
                    .containsEntry(AttributeKey.stringKey("sdh.item_id"),
                            broken.itemId().toString())
                    .containsEntry(AttributeKey.stringKey("sdh.result"),
                            "INCOMPLETE");
            // Recorded in the fail store once, though processed twice.
            assertThat(events.getFinishedLogRecordItems().stream()
                    .filter(event -> "sdh.audit.failure.failed"
                            .equals(event.getEventName())))
                    .hasSize(1);
        }
        finally
        {
            Telemetry.install(OpenTelemetry.noop());
        }
    }

    /**
     * A message batch is split message by message: a message that can't be
     * split is stored as it is and announced with the splitter's error, and
     * the item still completes, with the messages either side of it.
     *
     * @throws Exception if the test fails
     */
    @Test
    void failsOnlyTheMessageOfABatchThatCantBeSplit() throws Exception
    {
        String[] messages = {"1,ok\n", "2,\"never closed\n", "3,ok\n"};
        StringBuilder joined = new StringBuilder();
        List<Long> offsets = new ArrayList<>();
        for (String message : messages)
        {
            offsets.add((long) joined.length());
            joined.append(message);
        }
        byte[] bytes = joined.toString().getBytes(UTF_8);
        LandedItem item = lander.land(new NewItem(
                UuidV7.next(System.currentTimeMillis()),
                ItemKind.MESSAGE_BATCH, null, null, null,
                new MessageIndex(offsets, Arrays.asList(null, null, null))),
                offset -> new ByteArrayInputStream(bytes, (int) offset,
                        bytes.length));

        ItemProcessor.Outcome outcome = process(processor(), item);
        assertThat(outcome.result()).isEqualTo(ItemProcessor.Result.COMPLETED);
        assertThat(outcome.recordCount()).isEqualTo(3);
        // The piece that failed is kept, in the fail store, and not sent on
        // (STAGES.md §15.1); the item is incomplete while it is open.
        List<RecordSplit> split = messages().stream()
                .filter(RecordSplit.class::isInstance)
                .map(RecordSplit.class::cast).toList();
        assertThat(split).extracting(RecordSplit::sourceOffset)
                .containsExactly(offsets.get(0), offsets.get(2));
        assertThat(split).extracting(RecordSplit::errorCode)
                .containsOnlyNulls();
        assertThat(messages()).noneMatch(ItemSplit.class::isInstance);
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemProgressed.class, progress ->
                {
                    assertThat(progress.stage()).isEqualTo("split");
                    assertThat(progress.recordCount()).isEqualTo(3);
                    assertThat(progress.passedCount()).isEqualTo(2);
                    assertThat(progress.openCount()).isEqualTo(1);
                    assertThat(progress.declinedCount()).isZero();
                    assertThat(progress.failure()).isNull();
                });
        assertThat(failures().failures(item.itemId())).singleElement()
                .satisfies(stored ->
                {
                    assertThat(stored.state())
                            .isEqualTo(FailureStore.State.FAILED);
                    assertThat(stored.failure().seq()).isEqualTo(1);
                    assertThat(stored.failure().sourceOffset())
                            .isEqualTo(offsets.get(1));
                    assertThat(stored.failure().errorCode())
                            .isEqualTo("PARSE_UNTERMINATED_QUOTE");
                    assertThat(stored.failure().recordId()).isNotNull();
                });
        assertThat(failures().counts(item.itemId()))
                .isEqualTo(new FailureStore.Counts(1, 0, 0));
    }

    /**
     * An item that can't be split fails, and stays failed.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void failsAnItemThatCantBeSplit() throws Exception
    {
        LandedItem item = land("1,ok\n2,\"never closed\n");
        ItemProcessor.Outcome outcome = process(processor(), item);
        assertThat(outcome.result()).isEqualTo(ItemProcessor.Result.FAILED);
        assertThat(outcome.failure().code())
                .isEqualTo(ErrorCode.PARSE_UNTERMINATED_QUOTE);
        assertThat(outcome.failure().stage()).isEqualTo("split");
        // The item as a whole is the failure, and is open (§15.6).
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemProgressed.class, progress ->
                {
                    assertThat(progress.failure().code())
                            .isEqualTo("PARSE_UNTERMINATED_QUOTE");
                    assertThat(progress.failure().stage()).isEqualTo("split");
                    assertThat(progress.passedCount())
                            .isEqualTo(outcome.recordCount());
                    assertThat(progress.openCount()).isEqualTo(1);
                });
        assertThat(failures().failures(item.itemId())).singleElement()
                .satisfies(stored -> assertThat(stored.failure().seq())
                        .isEqualTo(FailureStore.WHOLE_ITEM));
        assertThat(processor().checkpoint(item.itemId())).hasValueSatisfying(
                checkpoint -> assertThat(checkpoint.state())
                        .isEqualTo(Checkpoint.State.FAILED));
    }

    /**
     * An empty or blank item fails as a whole (STAGES.md §16), for an
     * operator to decide; declined, it completes with no records. A
     * whole-item correction that is itself blank fails the same way.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void failsAnEmptyItemForAnOperatorToDecide() throws Exception
    {
        LandedItem item = land(" \r\n");
        ItemProcessor processor = processor();
        ItemProcessor.Outcome outcome = process(processor, item);
        assertThat(outcome.result()).isEqualTo(ItemProcessor.Result.FAILED);
        assertThat(outcome.failure().code())
                .isEqualTo(ErrorCode.SPLIT_EMPTY);
        assertThat(outcome.recordCount()).isZero();
        assertThat(failures().failures(item.itemId())).singleElement()
                .satisfies(stored -> assertThat(stored.failure().seq())
                        .isEqualTo(FailureStore.WHOLE_ITEM));

        publisher.sent.clear();
        CorrectionStore.Outcome corrected = processor.correct(
                submit(item.itemId(), FailureStore.WHOLE_ITEM, "\n\n"),
                item.digest(), LandedRefs.NONE, lease());
        assertThat(corrected.passed()).isFalse();
        assertThat(corrected.errorCode())
                .isEqualTo(ErrorCode.SPLIT_EMPTY.name());

        FailureStore failures = failures();
        failures.change(item.itemId(), FailureStore.WHOLE_ITEM,
                java.util.Set.of(FailureStore.State.FAILED),
                FailureStore.State.DECLINED, "empty, as the producer says",
                "alice", null);
        publisher.sent.clear();
        processor.reannounce(item.itemId(), item.digest(), LandedRefs.NONE);
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, split ->
                {
                    // Complete, naming the failure it was declined for.
                    assertThat(split.recordCount()).isZero();
                    assertThat(split.failure().code())
                            .isEqualTo("SPLIT_EMPTY");
                });
    }

    /**
     * An item that isn't in landing fails at the landing stage.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void failsAnItemThatIsntLanded() throws Exception
    {
        UUID missing = UUID.randomUUID();
        ItemProcessor.Outcome outcome = processor().process(missing, "x",
                () -> LandedItemSource.open(landed, missing, keys), lease());
        assertThat(outcome.result()).isEqualTo(ItemProcessor.Result.FAILED);
        assertThat(outcome.failure().stage()).isEqualTo("landing");
        assertThat(outcome.failure().code())
                .isEqualTo(ErrorCode.SOURCE_READ_FAILED);
    }

    /**
     * A broker outage doesn't fail the item: it is thrown, and the item
     * resumes from its checkpoint later.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void retriesABrokerOutageInsteadOfFailing() throws Exception
    {
        LandedItem item = land(csv(10));
        publisher.fail = call -> call >= 2;
        assertThatThrownBy(() -> process(processor(), item))
                .isInstanceOf(PublishFailedException.class);
        assertThat(processor().checkpoint(item.itemId())).hasValueSatisfying(
                checkpoint -> assertThat(checkpoint.state())
                        .isEqualTo(Checkpoint.State.SPLITTING));

        publisher.fail = call -> false;
        ItemProcessor.Outcome outcome = process(processor(), item);
        assertThat(outcome.result()).isEqualTo(ItemProcessor.Result.COMPLETED);
        assertThat(messages().stream().filter(RecordSplit.class::isInstance)
                .map(message -> ((RecordSplit) message).seq()).distinct())
                .hasSize(10);
    }

    /**
     * An item split before a crash is finished without reading it again.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void finishesASplitItemWithoutReadingIt() throws Exception
    {
        LandedItem item = land(csv(4));
        // The four records are sent; item.split is not.
        publisher.fail = call -> call >= 4;
        assertThatThrownBy(() -> process(processor(), item))
                .isInstanceOf(Exception.class);
        assertThat(processor().checkpoint(item.itemId())).hasValueSatisfying(
                checkpoint -> assertThat(checkpoint.state())
                        .isEqualTo(Checkpoint.State.ARCHIVING));

        publisher.fail = call -> false;
        assertThat(processor().process(item.itemId(), item.digest(), () ->
        {
            throw new AssertionError("a split item isn't read");
        }, lease()).result()).isEqualTo(ItemProcessor.Result.COMPLETED);
    }

    /**
     * An operator's correction of a piece the splitter couldn't parse is
     * split alone, stored at the piece's number and place, marked as
     * correcting it, and sent; the failure is {@code SUPERSEDED} and the
     * item ends in {@code item.split}, nothing failed (STAGES.md §15.6d).
     *
     * @throws Exception if the test fails
     */
    @Test
    void correctsAPieceTheSplitterCouldntParse() throws Exception
    {
        List<Long> offsets = new ArrayList<>();
        LandedItem item = batchWithABadMessage(offsets);
        ItemProcessor processor = processor();
        process(processor, item);
        String original = failures().failure(item.itemId(), 1).orElseThrow()
                .failure().recordId();
        publisher.sent.clear();

        CorrectionStore.Pending pending = submit(item.itemId(), 1,
                "2,\"now closed\"\n");
        CorrectionStore.Outcome outcome = processor.correct(pending,
                item.digest(), LandedRefs.NONE, lease());

        assertThat(outcome.passed()).isTrue();
        assertThat(messages()).hasSize(2);
        assertThat(messages().getFirst()).isInstanceOfSatisfying(
                RecordSplit.class, split ->
                {
                    assertThat(split.seq()).isEqualTo(1);
                    assertThat(split.sourceOffset()).isEqualTo(offsets.get(1));
                    assertThat(split.errorCode()).isNull();
                    assertThat(split.recordId()).isEqualTo(outcome.recordId())
                            .isNotEqualTo(original);
                    // What it resolves (STAGES.md §15.6e), as the piece's
                    // next revision at this stage (§15.8.5).
                    assertThat(split.resolves()).isEqualTo(original);
                    assertThat(split.lineage()).isEqualTo(Lineage.FIRST
                            .nextRevision(Lineage.SPLIT));
                });
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, split ->
                {
                    assertThat(split.recordCount()).isEqualTo(3);
                    assertThat(split.failedRecordCount()).isZero();
                    assertThat(split.failure()).isNull();
                });
        assertThat(failures().failure(item.itemId(), 1)).hasValueSatisfying(
                stored -> assertThat(stored.state())
                        .isEqualTo(FailureStore.State.SUPERSEDED));
        records.refresh();
        assertThat(CommittedRecords.corrections(records, item.itemId()))
                .singleElement().satisfies(row ->
                {
                    assertThat(row.seq()).isEqualTo(1);
                    assertThat(row.correctionOf()).isEqualTo(original);
                    assertThat(row.lineage().revision(Lineage.SPLIT))
                            .isEqualTo(2);
                });
        // The original stays, as it failed.
        assertThat(CommittedRecords.all(records, item.itemId()))
                .filteredOn(row -> row.seq() == 1).singleElement()
                .satisfies(row -> assertThat(row.recordId())
                        .isEqualTo(original));
        assertThat(corrections().unnotified(Duration.ZERO)).isEmpty();
    }

    /**
     * A correction of a piece that splits into more than one record fails,
     * stores nothing, and leaves the failure open.
     *
     * @throws Exception if the test fails
     */
    @Test
    void failsACorrectionOfAPieceThatIsntOneRecord() throws Exception
    {
        LandedItem item = batchWithABadMessage(new ArrayList<>());
        ItemProcessor processor = processor();
        process(processor, item);
        publisher.sent.clear();

        CorrectionStore.Outcome outcome = processor.correct(
                submit(item.itemId(), 1, "2,a\n4,b\n"), item.digest(),
                LandedRefs.NONE, lease());

        assertThat(outcome.passed()).isFalse();
        assertThat(outcome.errorCode())
                .isEqualTo(ErrorCode.CORRECTION_RECORD_COUNT.name());
        assertThat(messages()).singleElement()
                .isInstanceOf(ItemProgressed.class);
        assertThat(failures().failure(item.itemId(), 1)).hasValueSatisfying(
                stored ->
                {
                    // The revision's own failure (STAGES.md §15.8.5).
                    assertThat(stored.state())
                            .isEqualTo(FailureStore.State.FAILED);
                    assertThat(stored.failure().lineage().revision(
                            Lineage.SPLIT)).isEqualTo(2);
                });
        records.refresh();
        assertThat(CommittedRecords.corrections(records, item.itemId()))
                .isEmpty();
    }

    /**
     * A correction of an item that couldn't be split to its end is the
     * item's next generation (STAGES.md §15.8.5): cut from the start of the
     * correction, each record of the new generation, caused by the
     * correction, resolving the item. The item then ends in
     * {@code item.split} of that generation with no failure, and the
     * generation before's failure is superseded; its rows are kept as
     * they were.
     *
     * @throws Exception if the test fails
     */
    @Test
    void correctsAnItemThatCouldntBeSplitToItsEnd() throws Exception
    {
        // Enough before the bad row for a batch to be committed.
        LandedItem item = land("1,ok\n2,ok\n3,ok\n4,ok\n5,\"never closed\n");
        ItemProcessor processor = processor();
        long kept = process(processor, item).recordCount();
        assertThat(kept).isPositive();
        publisher.sent.clear();

        InMemoryLogRecordExporter events = InMemoryLogRecordExporter.create();
        CorrectionStore.Outcome outcome;
        List<LogRecordData> ended;
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build())
        {
            Telemetry.install(sdk);
            outcome = processor().correct(submit(item.itemId(),
                    FailureStore.WHOLE_ITEM,
                    "1,ok\n2,ok\n3,ok\n4,ok\n5,\"now closed\"\n"),
                    item.digest(), LandedRefs.NONE, lease());
            ended = events.getFinishedLogRecordItems().stream()
                    .filter(event -> "sdh.audit.stage.item_completed"
                            .equals(event.getEventName()))
                    .toList();
        }
        finally
        {
            Telemetry.install(OpenTelemetry.noop());
        }

        assertThat(outcome.passed()).isTrue();
        Lineage second = Lineage.of(2, List.of());
        List<RecordSplit> split = messages().stream()
                .filter(RecordSplit.class::isInstance)
                .map(RecordSplit.class::cast).toList();
        assertThat(split).extracting(RecordSplit::seq)
                .containsExactly(0L, 1L, 2L, 3L, 4L);
        assertThat(split).allSatisfy(record ->
        {
            // Each resolves the item's failure (STAGES.md §15.6e).
            assertThat(record.resolves())
                    .isEqualTo(item.itemId().toString());
            assertThat(record.lineage()).isEqualTo(second);
            assertThat(record.cause().kind())
                    .isEqualTo(ConnectorMessage.Cause.CORRECTION);
        });
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, end ->
                {
                    assertThat(end.recordCount()).isEqualTo(5);
                    assertThat(end.generation()).isEqualTo(2);
                    assertThat(end.failure()).isNull();
                });
        assertThat(ended).singleElement().satisfies(event -> assertThat(
                        event.getAttributes().asMap())
                        .containsEntry(AttributeKey.stringKey("sdh.result"),
                                "COMPLETED")
                        .containsEntry(AttributeKey.longKey("sdh.count"),
                                5L));
        assertThat(failures().failures(item.itemId()))
                .filteredOn(stored -> stored.failure().lineage()
                        .generation() == 1)
                .singleElement().satisfies(stored -> assertThat(
                        stored.state())
                        .isEqualTo(FailureStore.State.SUPERSEDED));
        assertThat(processor.checkpoint(item.itemId())).hasValueSatisfying(
                checkpoint -> assertThat(checkpoint.generation())
                        .isEqualTo(2));
        records.refresh();
        assertThat(CommittedRecords.all(records, item.itemId(), 1))
                .extracting(CommittedRecords.Row::lineage)
                .containsOnly(Lineage.FIRST);
        assertThat(CommittedRecords.all(records, item.itemId(), 2))
                .hasSize(5);
    }

    /**
     * A correction of the whole item may change what passed before: it is
     * a generation of its own, cut from the correction, so the records
     * before are superseded, not compared (STAGES.md §15.8.5).
     *
     * @throws Exception if the test fails
     */
    @Test
    void correctsAnItemChangingWhatPassed() throws Exception
    {
        LandedItem item = land("1,ok\n2,ok\n3,ok\n4,ok\n5,\"never closed\n");
        ItemProcessor processor = processor();
        assertThat(process(processor, item).recordCount()).isPositive();

        CorrectionStore.Outcome outcome = processor.correct(
                submit(item.itemId(), FailureStore.WHOLE_ITEM,
                        "1,changed\n2,ok\n3,ok\n4,ok\n5,\"now closed\"\n"),
                item.digest(), LandedRefs.NONE, lease());

        assertThat(outcome.passed()).isTrue();
        records.refresh();
        assertThat(CommittedRecords.all(records, item.itemId(), 2))
                .hasSize(5);
    }

    /**
     * A retry of an item that failed as a whole cuts it again, unchanged,
     * as its next generation: one that fails again supersedes the failure
     * it was made for, and its own failure, of the new generation, is the
     * one open (STAGES.md §15.8.5). What it cut before the bad row is sent
     * as that generation's.
     *
     * @throws Exception if the test fails
     */
    @Test
    void makesAFailedRetryAFailureOfItsOwn() throws Exception
    {
        String content = "1,ok\n2,ok\n3,ok\n4,ok\n5,\"never closed\n";
        LandedItem item = land(content);
        ItemProcessor processor = processor();
        assertThat(process(processor, item).recordCount()).isPositive();
        publisher.sent.clear();

        CorrectionStore.Outcome outcome = processor.correct(
                submit(item.itemId(), FailureStore.WHOLE_ITEM, content,
                        CorrectionStore.Kind.RETRY),
                item.digest(), LandedRefs.NONE, lease());

        assertThat(outcome.passed()).isFalse();
        assertThat(failures().failures(item.itemId()))
                .filteredOn(stored -> stored.failure().seq()
                        == FailureStore.WHOLE_ITEM)
                .extracting(stored -> stored.failure().lineage().generation(),
                        FailureStore.Stored::state)
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple(1,
                                FailureStore.State.SUPERSEDED),
                        org.assertj.core.groups.Tuple.tuple(2,
                                FailureStore.State.FAILED));
        assertThat(messages()).filteredOn(RecordSplit.class::isInstance)
                .extracting(message -> ((RecordSplit) message).lineage())
                .isNotEmpty().containsOnly(Lineage.of(2, List.of()));
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemProgressed.class, progressed -> assertThat(
                        progressed.generation()).isEqualTo(2));
        assertThat(corrections().list(item.itemId(), FailureStore.WHOLE_ITEM))
                .singleElement().satisfies(retry ->
                {
                    assertThat(retry.kind())
                            .isEqualTo(CorrectionStore.Kind.RETRY);
                    assertThat(retry.errorCode()).startsWith("PARSE_");
                });
    }

    /**
     * A reprocess of an item split to its end cuts it again from the start
     * as its next generation (STAGES.md §15.8.4): each record of the new
     * generation, caused by the run, and its end of that generation. The
     * same reprocess taken again is a repeat, and an older one, taken
     * after, is superseded.
     *
     * @throws Exception if the test fails
     */
    @Test
    void reprocessesAnItemAsItsNextGeneration() throws Exception
    {
        LandedItem item = land("1,a\n2,b\n3,c\n4,d\n");
        ItemProcessor processor = processor();
        assertThat(process(processor, item).result())
                .isEqualTo(ItemProcessor.Result.COMPLETED);
        publisher.sent.clear();
        UUID runId = UuidV7.next(System.currentTimeMillis());

        ItemProcessor.Outcome outcome = reprocess(processor, item, 2, runId);

        assertThat(outcome.result())
                .isEqualTo(ItemProcessor.Result.COMPLETED);
        assertThat(outcome.recordCount()).isEqualTo(4);
        assertThat(messages()).filteredOn(RecordSplit.class::isInstance)
                .hasSize(4).allSatisfy(message ->
                {
                    RecordSplit record = (RecordSplit) message;
                    assertThat(record.lineage())
                            .isEqualTo(Lineage.of(2, List.of()));
                    assertThat(record.cause().kind())
                            .isEqualTo(ConnectorMessage.Cause.REPROCESS);
                    assertThat(record.cause().runId()).isEqualTo(runId);
                });
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, end ->
                {
                    assertThat(end.generation()).isEqualTo(2);
                    assertThat(end.interrupted()).isFalse();
                });

        assertThat(reprocess(processor, item, 2, runId).result())
                .isEqualTo(ItemProcessor.Result.COMPLETED);
        assertThat(reprocess(processor, item, 3, runId).result())
                .isEqualTo(ItemProcessor.Result.COMPLETED);
        assertThat(reprocess(processor, item, 2, runId).result())
                .isEqualTo(ItemProcessor.Result.SUPERSEDED);
        assertThat(processor.checkpoint(item.itemId())).hasValueSatisfying(
                checkpoint -> assertThat(checkpoint.generation())
                        .isEqualTo(3));
    }

    /**
     * A newer generation queued while an item is split stops it after the
     * batch under way (STAGES.md §15.8.6): its checkpoint is
     * {@code INTERRUPTED}, and its end, {@code item.split}, says so, with
     * what it cut. The newer generation then cuts the item from the start.
     *
     * @throws Exception if the test fails
     */
    @Test
    void interruptsAGenerationForANewerOne() throws Exception
    {
        LandedItem item = land("1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n7,g\n");
        ItemProcessor processor = processor(
                (itemId, generation) -> generation == 1 ? 2 : 0);

        ItemProcessor.Outcome first = process(processor, item);

        assertThat(first.result())
                .isEqualTo(ItemProcessor.Result.INTERRUPTED);
        assertThat(processor.checkpoint(item.itemId())).hasValueSatisfying(
                checkpoint -> assertThat(checkpoint.state())
                        .isEqualTo(Checkpoint.State.INTERRUPTED));
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, end ->
                {
                    assertThat(end.interrupted()).isTrue();
                    assertThat(end.generation()).isEqualTo(1);
                    assertThat(end.recordCount()).isEqualTo(3);
                });

        ItemProcessor.Outcome second = reprocess(processor, item, 2,
                UuidV7.next(System.currentTimeMillis()));

        assertThat(second.result())
                .isEqualTo(ItemProcessor.Result.COMPLETED);
        assertThat(second.recordCount()).isEqualTo(7);
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, end ->
                {
                    assertThat(end.interrupted()).isFalse();
                    assertThat(end.generation()).isEqualTo(2);
                    assertThat(end.recordCount()).isEqualTo(7);
                });
    }

    /**
     * A correction of the whole item whose generation is older than the
     * item's by the time it is applied, a reprocess having come first,
     * isn't cut: it fails, {@code CORRECTION_SUPERSEDED}, and no failure is
     * added (STAGES.md §15.8.5).
     *
     * @throws Exception if the test fails
     */
    @Test
    void failsACorrectionAReprocessSuperseded() throws Exception
    {
        LandedItem item = land("1,ok\n2,ok\n3,ok\n4,ok\n5,\"never closed\n");
        ItemProcessor processor = processor();
        process(processor, item);
        CorrectionStore.Pending correction = submit(item.itemId(),
                FailureStore.WHOLE_ITEM,
                "1,ok\n2,ok\n3,ok\n4,ok\n5,\"now closed\"\n");
        reprocess(processor, item, 3, UuidV7.next(System.currentTimeMillis()));
        int failuresBefore = failures().failures(item.itemId()).size();

        CorrectionStore.Outcome outcome = processor.correct(correction,
                item.digest(), LandedRefs.NONE, lease());

        assertThat(outcome.passed()).isFalse();
        assertThat(outcome.errorCode())
                .isEqualTo(ErrorCode.CORRECTION_SUPERSEDED.name());
        assertThat(failures().failures(item.itemId()))
                .hasSize(failuresBefore);
        assertThat(processor.checkpoint(item.itemId())).hasValueSatisfying(
                checkpoint -> assertThat(checkpoint.generation())
                        .isEqualTo(3));
    }

    /**
     * Reprocesses a landed item as a generation.
     *
     * @param processor  the processor
     * @param item       the item
     * @param generation the generation
     * @param runId      the run
     * @return the outcome
     * @throws Exception if processing throws
     */
    ItemProcessor.Outcome reprocess(ItemProcessor processor, LandedItem item,
            int generation, UUID runId) throws Exception
    {
        return processor.reprocess(item.itemId(), item.digest(),
                () -> LandedItemSource.open(landed, item.itemId(), keys),
                lease(), LandedRefs.NONE, generation, runId);
    }

    /**
     * An item complete at the split stage whose failure an operator
     * reopens is announced in progress, reopening its completion
     * downstream, and once the failure is declined again completes a
     * second time under an ID of its own (STAGES.md §15.6e); the same end
     * announced again keeps its number.
     *
     * @throws Exception if the test fails
     */
    @Test
    void completesAgainAnItemWhoseFailureIsReopened() throws Exception
    {
        LandedItem item = land("1,ok\n2,\"never closed\n");
        ItemProcessor processor = processor();
        process(processor, item);
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemProgressed.class, progress ->
                        assertThat(progress.completion()).isZero());
        FailureStore failures = failures();
        long seq = failures.failures(item.itemId()).getFirst().failure()
                .seq();

        failures.change(item.itemId(), seq,
                java.util.Set.of(FailureStore.State.FAILED),
                FailureStore.State.DECLINED, "declined", "alice", null);
        publisher.sent.clear();
        processor.reannounce(item.itemId(), item.digest(), LandedRefs.NONE);
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, split ->
                {
                    assertThat(split.completion()).isEqualTo(1);
                    assertThat(split.messageId())
                            .isEqualTo("is-" + item.itemId());
                });
        // The same end again: the same number, a repeat downstream drops.
        publisher.sent.clear();
        processor.reannounce(item.itemId(), item.digest(), LandedRefs.NONE);
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, split ->
                        assertThat(split.completion()).isEqualTo(1));

        failures.change(item.itemId(), seq,
                java.util.Set.of(FailureStore.State.DECLINED),
                FailureStore.State.FAILED, "reopened", "alice", null);
        publisher.sent.clear();
        processor.reannounce(item.itemId(), item.digest(), LandedRefs.NONE);
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemProgressed.class, progress ->
                {
                    assertThat(progress.completion()).isEqualTo(1);
                    assertThat(progress.openCount()).isEqualTo(1);
                    assertThat(progress.messageId()).endsWith("~c1");
                });

        failures.change(item.itemId(), seq,
                java.util.Set.of(FailureStore.State.FAILED),
                FailureStore.State.DECLINED, "declined", "alice", null);
        publisher.sent.clear();
        processor.reannounce(item.itemId(), item.digest(), LandedRefs.NONE);
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, split ->
                {
                    assertThat(split.completion()).isEqualTo(2);
                    assertThat(split.messageId())
                            .isEqualTo("is-" + item.itemId() + "~c2");
                });
    }

    /**
     * Items whose ends were sent before ends were numbered (STAGES.md
     * §15.6f) are numbered 1 at the next start if none of their failures
     * is open, so reopening one withdraws completion 1; an item still open
     * isn't touched, and completes first under its usual ID.
     *
     * @throws Exception if the test fails
     */
    @Test
    void numbersTheCompletionsOfEarlierItems() throws Exception
    {
        ItemProcessor processor = processor();
        LandedItem clean = land("1,ok\n2,ok\n");
        LandedItem declined = land("1,ok\n2,\"never closed\n");
        LandedItem open = land("3,ok\n4,\"never closed\n");
        for (LandedItem item : List.of(clean, declined, open))
        {
            process(processor, item);
        }
        FailureStore failures = failures();
        long seq = failures.failures(declined.itemId()).getFirst().failure()
                .seq();
        failures.change(declined.itemId(), seq,
                java.util.Set.of(FailureStore.State.FAILED),
                FailureStore.State.DECLINED, "declined", "alice", null);
        processor.reannounce(declined.itemId(), declined.digest(),
                LandedRefs.NONE);
        // As an earlier build left them: no numbers.
        try (java.sql.Connection connection =
                SplitTestSupport.database().getConnection();
                java.sql.Statement statement = connection.createStatement())
        {
            statement.executeUpdate("UPDATE " + schema + ".split_item"
                    + " SET completion = 0, completed = false");
        }

        ItemCheckpoints checkpoints = SplitTestSupport.checkpoints(schema,
                new RecordIds(IdKeys.loadOrCreateShared(records, keys,
                        RECORDS_KEK)));
        assertThat(checkpoints.numberEarlierCompletions(schema, "split"))
                .isEqualTo(2);
        assertThat(checkpoints.numberEarlierCompletions(schema, "split"))
                .isZero();

        failures.change(declined.itemId(), seq,
                java.util.Set.of(FailureStore.State.DECLINED),
                FailureStore.State.FAILED, "reopened", "alice", null);
        publisher.sent.clear();
        processor.reannounce(declined.itemId(), declined.digest(),
                LandedRefs.NONE);
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemProgressed.class, progress ->
                {
                    assertThat(progress.completion()).isEqualTo(1);
                    assertThat(progress.messageId()).endsWith("~c1");
                });

        long openSeq = failures.failures(open.itemId()).getFirst()
                .failure().seq();
        failures.change(open.itemId(), openSeq,
                java.util.Set.of(FailureStore.State.FAILED),
                FailureStore.State.DECLINED, "declined", "alice", null);
        publisher.sent.clear();
        processor.reannounce(open.itemId(), open.digest(), LandedRefs.NONE);
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, split -> assertThat(split.messageId())
                        .isEqualTo("is-" + open.itemId()));
    }
}
