/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandingCheckpoint;
import com.sdh.healthcare.connector.core.landing.LandingLedger;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.landing.RecordIndex;
import com.sdh.healthcare.connector.core.landing.SourceRef;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.landing.TelemetryCapture;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for {@link BatchService} on a real lander and ledger, an in-memory
 * catalog, and a publisher that keeps what it is sent.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class BatchServiceTest
{

    /** Captures the metrics recorded. */
    @RegisterExtension
    final TelemetryCapture telemetry = new TelemetryCapture();

    /** The intake under test. */
    static final String INTAKE = "queue";

    /** The KEK that wraps the data keys. */
    static final String KEK = "landed-items-kek/1";

    /** A quick retry policy with one attempt. */
    static final RetryPolicy RETRY =
            new RetryPolicy(1, Duration.ofMillis(1), Duration.ofMillis(1));

    /** Thrown from the commit hook to stand in for a crash. */
    static final class Crash extends Error
    {

        /** For serialization. */
        private static final long serialVersionUID = 1L;
    }

    /** The landed items table. */
    Table items;

    /** The ledger table. */
    Table ledgerTable;

    /** Wraps the data keys. */
    AesKeyWrapProvider keys;

    /** Makes the hashes and MACs. */
    final SectionIds ids = new SectionIds(new byte[32]);

    /** The last lease epoch handed out. */
    long epoch;

    /** The signing key pair. */
    KeyPair signing;

    /** The recipient's key pair. */
    KeyPair recipient;

    /** What the publisher was sent. */
    final List<OutboundMessage> sent = new CopyOnWriteArrayList<>();

    /** How many sends should fail before they start working. */
    final AtomicInteger failSends = new AtomicInteger();

    /** The lander of the last service made by {@link #service()}. */
    ItemLander lander;

    /** The reasons given to the exit hook. */
    final List<String> fatal = new CopyOnWriteArrayList<>();

    /**
     * Creates the catalog, tables and keys.
     *
     * @throws Exception if a key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        items = LandingTables.landedItems(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        ledgerTable = LandingTables.ledger(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(KEK, generator.generateKey()));
        KeyPairGenerator ec = KeyPairGenerator.getInstance("EC");
        ec.initialize(new ECGenParameterSpec("secp256r1"));
        signing = ec.generateKeyPair();
        recipient = ec.generateKeyPair();
    }

    /**
     * A committer under a new lease epoch, as after a restart.
     *
     * @return the committer
     */
    FencedCommitter committer()
    {
        long ours = ++epoch;
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return ours;
            }

            @Override
            public void checkHeld()
            {
            }
        });
        committer.establish(items);
        committer.establish(ledgerTable);
        return committer;
    }

    /**
     * A lander with small chunks, so a few bytes take several commits.
     *
     * @param committer makes the commits
     * @return the lander
     */
    ItemLander lander(FencedCommitter committer)
    {
        return new ItemLander(items, committer, keys, ids,
                ItemLander.Settings.defaults(INTAKE, KEK).withRetry(RETRY)
                        .withSizes(4, 8, 8), Clock.systemUTC());
    }

    /**
     * A service as a fresh start would make it.
     *
     * @param lander    the lander, under its committer
     * @param committer the lander's committer
     * @param window    the dedup window
     * @return the service
     * @throws Exception if the ledger can't be loaded
     */
    BatchService service(ItemLander lander, FencedCommitter committer,
            Duration window) throws Exception
    {
        LandingLedger ledger = new LandingLedger(ledgerTable, committer,
                RETRY);
        Announcer announcer = new Announcer(new MessageSealer(
                new ECDSASigner((ECPrivateKey) signing.getPrivate()),
                "connector", recipient.getPublic(), "downstream"),
                message ->
                {
                    if (failSends.getAndDecrement() > 0)
                    {
                        return CompletableFuture.failedFuture(
                                new IOException("broker down"));
                    }
                    sent.add(message);
                    return CompletableFuture.completedFuture(null);
                }, "sdh", LandingTables.DEFAULT_NAMESPACE, RETRY,
                Duration.ofSeconds(5));
        return new BatchService(INTAKE, new IntakeService.Storage(lander,
                ledger, ledger.load(INTAKE), ids), items, announcer,
                fatal::add, Clock.systemUTC(), window);
    }

    /**
     * A service on a fresh committer and lander, with an hour's window.
     *
     * @return the service
     * @throws Exception if the ledger can't be loaded
     */
    BatchService service() throws Exception
    {
        FencedCommitter committer = committer();
        lander = lander(committer);
        return service(lander, committer, Duration.ofHours(1));
    }

    /**
     * Messages whose bodies are their IDs, or none for a null.
     *
     * @param ids the IDs, or bodies of messages without one, as
     *            {@code "-body"}
     * @return the messages
     */
    static List<BatchService.Message> messages(String... ids)
    {
        List<BatchService.Message> messages = new ArrayList<>();
        for (String id : ids)
        {
            boolean none = id.startsWith("-");
            messages.add(new BatchService.Message(
                    id.getBytes(StandardCharsets.UTF_8),
                    none ? null : id));
        }
        return messages;
    }

    /**
     * The last landed item.
     *
     * @return the item
     * @throws IOException if the table can't be read
     */
    LandedItem last() throws IOException
    {
        return lander.lastLanded().orElseThrow();
    }

    /**
     * A batch becomes one item: the bodies one after another, with a record
     * index giving where each starts and its keyed ID; a message without
     * an ID has a null key. The IDs themselves are kept encrypted, for the
     * producer's receipt.
     *
     * @throws Exception if the test fails
     */
    @Test
    void landsABatchAsOneItemWithItsRecordIndex() throws Exception
    {
        BatchService service = service();
        BatchService.Result result =
                service.land(messages("one", "-two", "three"));
        assertThat(result).isEqualTo(new BatchService.Result(
                BatchService.Status.LANDED, 3, 0));
        assertThat(sent).hasSize(1);

        LandedItem item = last();
        assertThat(item.kind()).isEqualTo(ItemKind.MESSAGE_BATCH);
        ByteArrayOutputStream content = new ByteArrayOutputStream();
        LandedItems.read(items, item.itemId(), keys, ids, content);
        assertThat(content.toString(StandardCharsets.UTF_8))
                .isEqualTo("one-twothree");
        RecordIndex records = LandedItems.records(LandedItems.summary(items,
                item.itemId()).orElseThrow()).orElseThrow();
        assertThat(records.offsets()).containsExactly(0L, 3L, 7L);
        assertThat(records.keys().get(0)).isEqualTo(SourceRef.of(ids, INTAKE,
                "message\u0000one", -1, "").hash());
        assertThat(records.keys().get(1)).isNull();
        assertThat(records.keys().get(2)).isNotNull()
                .doesNotContain("three");
        assertThat(service.recentKeys()).isEqualTo(2);
        assertThat(LandedItems.messageRefs(LandedItems.summary(items,
                item.itemId()).orElseThrow(), keys))
                .contains(Arrays.asList("one", null, "three"));
        assertThat(fatal).isEmpty();
    }

    /**
     * A message delivered again is skipped, within a batch or across them;
     * a batch of nothing but those lands nothing; messages without an ID
     * are always landed.
     *
     * @throws Exception if the test fails
     */
    @Test
    void skipsMessagesDeliveredAgain() throws Exception
    {
        BatchService service = service();
        assertThat(service.land(messages("a", "b", "a")))
                .isEqualTo(new BatchService.Result(
                        BatchService.Status.LANDED, 2, 1));
        assertThat(service.land(messages("b", "c")))
                .isEqualTo(new BatchService.Result(
                        BatchService.Status.LANDED, 1, 1));
        UUID before = last().itemId();
        assertThat(service.land(messages("a", "c")))
                .isEqualTo(new BatchService.Result(
                        BatchService.Status.SKIPPED, 0, 2));
        assertThat(last().itemId()).isEqualTo(before);
        assertThat(service.land(messages("-x", "-x")).landed())
                .isEqualTo(2);
        assertThat(sent).hasSize(3);
    }

    /**
     * After a restart, the keys landed within the window before the last
     * batch are loaded, so a message delivered again is still skipped.
     *
     * @throws Exception if the test fails
     */
    @Test
    void recognisesMessagesDeliveredAgainAfterARestart() throws Exception
    {
        BatchService service = service();
        service.land(messages("a", "b"));
        service.land(messages("c", "-d"));

        BatchService restarted = service();
        restarted.recoverAtStartup();
        assertThat(restarted.recentKeys()).isEqualTo(3);
        assertThat(restarted.land(messages("b", "c")).status())
                .isEqualTo(BatchService.Status.SKIPPED);
        assertThat(restarted.land(messages("a", "e")))
                .isEqualTo(new BatchService.Result(
                        BatchService.Status.LANDED, 1, 1));
        assertThat(fatal).isEmpty();
    }

    /**
     * Keys are forgotten once they fall outside the window before the
     * newest batch, not by the clock.
     */
    @Test
    void forgetsKeysOutsideTheWindow()
    {
        BatchService.RecentKeys recent =
                new BatchService.RecentKeys(Duration.ofMinutes(10));
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        String a = SourceRef.of(ids, INTAKE, "message\u0000a", -1, "").hash();
        String b = SourceRef.of(ids, INTAKE, "message\u0000b", -1, "").hash();
        recent.add(start, Arrays.asList(a, null));
        recent.add(start.plus(Duration.ofMinutes(10)), List.of(b));
        assertThat(recent.size()).isEqualTo(2);
        recent.add(start.plus(Duration.ofMinutes(11)), List.of());
        assertThat(recent.size()).isEqualTo(1);
        assertThat(recent.contains(new SourceRef(b, -1, "").key())).isTrue();
        assertThat(recent.contains(new SourceRef(a, -1, "").key())).isFalse();
    }

    /**
     * While the broker is down, a landed batch is still landed, but the
     * next batch fails until the last one's message is sent; then both
     * are.
     *
     * @throws Exception if the test fails
     */
    @Test
    void landsNothingMoreUntilTheLastBatchIsAnnounced() throws Exception
    {
        BatchService service = service();
        failSends.set(2);
        assertThat(service.land(messages("a")).status())
                .isEqualTo(BatchService.Status.LANDED);
        assertThat(service.land(messages("b")).status())
                .isEqualTo(BatchService.Status.FAILED);
        assertThat(sent).isEmpty();
        assertThat(service.land(messages("b")).status())
                .isEqualTo(BatchService.Status.LANDED);
        assertThat(sent).hasSize(2);
        assertThat(fatal).isEmpty();
    }

    /**
     * After a crash part-way through a batch, startup clears its
     * checkpoint, since its messages come again, and batches land again.
     *
     * @throws Exception if the test fails
     */
    @Test
    void clearsAnUnfinishedBatchAtStartup() throws Exception
    {
        ItemLander crashing = lander(committer());
        crashing.afterCommit(nextBlock ->
        {
            throw new Crash();
        });
        UUID itemId = UUID.randomUUID();
        assertThatThrownBy(() -> crashing.land(new NewItem(itemId,
                ItemKind.MESSAGE_BATCH, SourceRef.of(ids, INTAKE, "batch",
                        -1, ""), null, null, new RecordIndex(List.of(0L),
                                Arrays.asList((String) null))),
                offset -> new ByteArrayInputStream(new byte[40])))
                .isInstanceOf(Crash.class);

        BatchService service = service();
        service.recoverAtStartup();
        assertThat(lander.checkpoint().filter(c -> !c.done())
                .map(LandingCheckpoint::itemId)).isEqualTo(Optional.empty());
        assertThat(service.land(messages("a")).status())
                .isEqualTo(BatchService.Status.LANDED);
        assertThat(fatal).isEmpty();
    }

    /**
     * An error that means the node can't go on stops it, and the batch is
     * reported fatal, so its messages aren't acknowledged.
     *
     * @throws Exception if the test fails
     */
    @Test
    void stopsTheNodeOnAFatalError() throws Exception
    {
        FencedCommitter committer = committer();
        ItemLander crashing = lander(committer);
        BatchService service =
                service(crashing, committer, Duration.ofHours(1));
        crashing.afterCommit(nextBlock ->
        {
            throw new Crash();
        });
        assertThat(service.land(messages("0123456789abcdef")).status())
                .isEqualTo(BatchService.Status.FATAL);
        assertThat(fatal).singleElement().isEqualTo("Crash");
    }

    /**
     * Each landed batch records its size, every message delivered again
     * counts as a duplicate, and the landed item counts with its bytes.
     *
     * @throws Exception if the test fails
     */
    @Test
    void recordsBatchSizesAndDuplicates() throws Exception
    {
        BatchService service = service();
        service.land(messages("a", "bb", "a"));
        service.land(messages("a", "bb"));
        Attributes intake = Attributes.of(
                AttributeKey.stringKey("sdh.intake"), INTAKE);
        assertThat(telemetry.count("sdh.landing.batch_size", intake))
                .isEqualTo(1);
        assertThat(telemetry.sum("sdh.landing.batch_size", intake))
                .isEqualTo(2);
        assertThat(telemetry.value("sdh.landing.duplicates", intake))
                .isEqualTo(3);
        Attributes batches = intake.toBuilder()
                .put("sdh.kind", "message-batch").build();
        assertThat(telemetry.value("sdh.landing.items", batches))
                .isEqualTo(1);
        assertThat(telemetry.value("sdh.landing.bytes", batches))
                .isEqualTo(3);
        assertThat(service.ledgerItems()).isEqualTo(1);
    }
}
