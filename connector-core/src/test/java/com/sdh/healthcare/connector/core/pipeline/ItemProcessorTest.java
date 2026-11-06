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
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.landing.MessageIndex;
import com.sdh.healthcare.connector.core.pipeline.IngestPipelineTest.RecordingPublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
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
        return new ItemProcessor(pipeline, "claims");
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
                            "FAILED");
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
        List<RecordSplit> split = messages().stream()
                .filter(RecordSplit.class::isInstance)
                .map(RecordSplit.class::cast).toList();
        assertThat(split).extracting(RecordSplit::sourceOffset)
                .containsExactlyElementsOf(offsets);
        assertThat(split).extracting(RecordSplit::errorCode)
                .containsExactly(null, "PARSE_UNTERMINATED_QUOTE", null);
        assertThat(split.get(1).sourceLength())
                .isEqualTo(messages[1].length());
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, done -> assertThat(
                        done.failedRecordCount()).isEqualTo(1));
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
        assertThat(messages().getLast()).isInstanceOfSatisfying(
                ItemSplit.class, done ->
                {
                    assertThat(done.failure().code())
                            .isEqualTo("PARSE_UNTERMINATED_QUOTE");
                    assertThat(done.failure().stage()).isEqualTo("split");
                });
        assertThat(processor().checkpoint(item.itemId())).hasValueSatisfying(
                checkpoint -> assertThat(checkpoint.state())
                        .isEqualTo(Checkpoint.State.FAILED));
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
}
