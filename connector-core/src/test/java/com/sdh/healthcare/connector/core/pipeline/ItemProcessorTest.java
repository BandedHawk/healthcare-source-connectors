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
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionFailed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.landing.RecordIndex;
import com.sdh.healthcare.connector.core.pipeline.IngestPipelineTest.RecordingPublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
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

    /** The sections KEK. */
    static final String SECTIONS_KEK = "sections-kek/1";

    /** The landing KEK. */
    static final String LANDED_KEK = "landed-items-kek/1";

    /** The sections table. */
    Table sections;

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
        sections = IngestTables.sections(catalog, "sdh_ingest");
        landed = LandingTables.landedItems(catalog, "sdh_landing");
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(SECTIONS_KEK,
                generator.generateKey(), LANDED_KEK, generator.generateKey()));
        signer = IngestPipelineTest.ec();
        recipient = IngestPipelineTest.ec();
        opener = new MessageOpener(Map.of("downstream", recipient.getPrivate()),
                Map.of("connector", (ECPublicKey) signer.getPublic()));
        FencedCommitter landing = committer();
        landing.establish(landed);
        lander = new ItemLander(landed, landing, keys,
                new SectionIds(new byte[32]),
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
     * A processor as a fresh start of the splitter would make it.
     *
     * @return the processor
     * @throws Exception if the signer can't be made
     */
    ItemProcessor processor() throws Exception
    {
        FencedCommitter committer = committer();
        committer.establish(sections);
        SectionIds ids = new SectionIds(IdKeys.loadOrCreate(sections,
                committer, keys, SECTIONS_KEK));
        PipelineSettings settings = PipelineSettings.defaults("splitter",
                "csv", "prod", SECTIONS_KEK)
                .withCommit(3, 1L << 30, Duration.ofHours(1)).withWorkers(2)
                .withRetry(new RetryPolicy(2, Duration.ofMillis(1),
                        Duration.ofMillis(2)));
        IngestPipeline pipeline = new IngestPipeline(settings, sections,
                committer, keys, ids, SplitterRegistry.load(),
                new MessageSealer(new ECDSASigner((ECPrivateKey) signer
                        .getPrivate()), "connector", recipient.getPublic(),
                        "downstream"), publisher, Clock.systemUTC());
        open.add(pipeline);
        return new ItemProcessor(pipeline);
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
                () -> LandedItemSource.open(landed, item.itemId(), keys));
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
     * An item is split under its item ID, and file.completed reports its
     * landed digest. Processing it again changes nothing.
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
        assertThat(outcome.fileId()).isEqualTo(item.itemId());
        assertThat(outcome.sectionCount()).isEqualTo(10);

        List<ConnectorMessage> messages = messages();
        assertThat(messages.getFirst()).isInstanceOfSatisfying(
                FileStarted.class, started -> assertThat(started.fileId())
                        .isEqualTo(item.itemId()));
        assertThat(messages.stream().filter(SectionStored.class::isInstance))
                .hasSize(10);
        assertThat(messages.getLast()).isInstanceOfSatisfying(
                FileCompleted.class, completed ->
                {
                    assertThat(completed.fileDigest())
                            .isEqualTo(item.digest());
                    assertThat(completed.fileSize()).isEqualTo(item.size());
                });

        int sent = publisher.sent.size();
        assertThat(processor().process(item.itemId(), item.digest(), () ->
        {
            throw new AssertionError("a finished item isn't read");
        }).result()).isEqualTo(ItemProcessor.Result.COMPLETED);
        assertThat(publisher.sent).hasSize(sent);
    }

    /**
     * A message batch is split message by message: a message that can't be
     * split is announced as a failed section with the splitter's error, and
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
                new RecordIndex(offsets, Arrays.asList(null, null, null))),
                offset -> new ByteArrayInputStream(bytes, (int) offset,
                        bytes.length));

        ItemProcessor.Outcome outcome = process(processor(), item);
        assertThat(outcome.result()).isEqualTo(ItemProcessor.Result.COMPLETED);
        assertThat(outcome.sectionCount()).isEqualTo(3);
        List<ConnectorMessage> published = messages();
        assertThat(published.stream().filter(SectionStored.class::isInstance)
                .map(m -> ((SectionStored) m).sourceOffset()))
                .containsExactly(offsets.get(0), offsets.get(2));
        assertThat(published.stream().filter(SectionFailed.class::isInstance))
                .singleElement().isInstanceOfSatisfying(SectionFailed.class,
                        failed ->
                        {
                            assertThat(failed.errorCode()).isEqualTo(
                                    "PARSE_UNTERMINATED_QUOTE");
                            assertThat(failed.sourceOffset())
                                    .isEqualTo(offsets.get(1));
                            assertThat(failed.sourceLength()).isEqualTo(
                                    messages[1].length());
                        });
        assertThat(published.getLast()).isInstanceOfSatisfying(
                FileCompleted.class, completed -> assertThat(
                        completed.failedSectionCount()).isEqualTo(1));
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
        assertThat(messages()).noneMatch(FileCompleted.class::isInstance);
        assertThat(processor().inProgress()).isEmpty();
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
                () -> LandedItemSource.open(landed, missing, keys));
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
        assertThat(processor().inProgress()).hasValueSatisfying(
                checkpoint -> assertThat(checkpoint.state())
                        .isEqualTo(Checkpoint.State.SPLITTING));

        publisher.fail = call -> false;
        ItemProcessor.Outcome outcome = process(processor(), item);
        assertThat(outcome.result()).isEqualTo(ItemProcessor.Result.COMPLETED);
        assertThat(messages().stream().filter(SectionStored.class::isInstance)
                .map(message -> ((SectionStored) message).seq()).distinct())
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
        publisher.fail = call -> call >= 5;
        assertThatThrownBy(() -> process(processor(), item))
                .isInstanceOf(Exception.class);
        assertThat(processor().inProgress()).hasValueSatisfying(
                checkpoint -> assertThat(checkpoint.state())
                        .isEqualTo(Checkpoint.State.ARCHIVING));

        publisher.fail = call -> false;
        assertThat(processor().process(item.itemId(), item.digest(), () ->
        {
            throw new AssertionError("a split item isn't read");
        }).result()).isEqualTo(ItemProcessor.Result.COMPLETED);
    }

    /**
     * An item in progress can be given up, so another can start.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void abandonsAnItemInProgress() throws Exception
    {
        LandedItem first = land(csv(10));
        publisher.fail = call -> call >= 2;
        assertThatThrownBy(() -> process(processor(), first))
                .isInstanceOf(PublishFailedException.class);
        publisher.fail = call -> false;

        ItemProcessor processor = processor();
        Checkpoint stuck = processor.inProgress().orElseThrow();
        assertThat(processor.abandon(stuck).result())
                .isEqualTo(ItemProcessor.Result.ABANDONED);
        assertThat(process(processor(), land(csv(2))).result())
                .isEqualTo(ItemProcessor.Result.COMPLETED);
    }
}
