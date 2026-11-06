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
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandingCheckpoint;
import com.sdh.healthcare.connector.core.landing.LandingLedger;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.landing.SourceRef;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import com.sdh.healthcare.connector.landing.TelemetryCapture;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
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
 * Tests for {@link PushService} on a real lander and ledger, an in-memory
 * catalog, and a publisher that keeps what it is sent.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class PushServiceTest
{

    /** Captures the metrics recorded. */
    @RegisterExtension
    final TelemetryCapture telemetry = new TelemetryCapture();

    /** The intake under test. */
    static final String INTAKE = "push";

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
    final RecordIds ids = new RecordIds(new byte[32]);

    /** The last lease epoch handed out. */
    long epoch;

    /** The signing key pair. */
    KeyPair signing;

    /** The recipient's key pair. */
    KeyPair recipient;

    /** What the publisher was sent. */
    final List<OutboundMessage> sent = new CopyOnWriteArrayList<>();

    /** What the producer's results queue was sent. */
    final List<OutboundMessage> results = new CopyOnWriteArrayList<>();

    /** The producer's key pair. */
    KeyPair producer;

    /** How many sends should fail before they start working. */
    final AtomicInteger failSends = new AtomicInteger();

    /** The lander of the last service made by {@link #service(long)}. */
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
        producer = ec.generateKeyPair();
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
     * @param lander       the lander, under its committer
     * @param committer    the lander's committer
     * @param maxItemBytes the largest body accepted; 0 for no limit
     * @return the service
     * @throws Exception if the ledger can't be loaded
     */
    PushService service(ItemLander lander, FencedCommitter committer,
            long maxItemBytes) throws Exception
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
                Duration.ofSeconds(5), new Announcer.Receipts(
                        new ResultsChannel(new MessageSealer(
                                new ECDSASigner((ECPrivateKey)
                                        signing.getPrivate()),
                                "connector", producer.getPublic(),
                                "producer"),
                                message ->
                                {
                                    results.add(message);
                                    return CompletableFuture
                                            .completedFuture(null);
                                }, null, RETRY, Duration.ofSeconds(5)),
                        items, keys));
        return new PushService(INTAKE, new IntakeService.Storage(lander,
                ledger, ledger.load(INTAKE), ids), announcer, fatal::add,
                Clock.systemUTC(), maxItemBytes, Duration.ofMillis(100));
    }

    /**
     * A service on a fresh committer and lander.
     *
     * @param maxItemBytes the largest body accepted; 0 for no limit
     * @return the service
     * @throws Exception if the ledger can't be loaded
     */
    PushService service(long maxItemBytes) throws Exception
    {
        FencedCommitter committer = committer();
        lander = lander(committer);
        return service(lander, committer, maxItemBytes);
    }

    /**
     * A push with a body.
     *
     * @param service the service
     * @param sender  who sends it
     * @param key     its idempotency key; null for none
     * @param body    its body
     * @return what happened
     */
    static PushService.Result push(PushService service, String sender,
            String key, String body)
    {
        return service.receive(new PushService.Push(sender, key,
                body.length()), new ByteArrayInputStream(body.getBytes()));
    }

    /**
     * The checkpoint's unfinished item, if there is one. It is read through
     * the service's own lander: a new committer would fence it.
     *
     * @param lander the lander
     * @return the item; empty if none is unfinished
     */
    static Optional<UUID> unfinished(ItemLander lander)
    {
        return lander.checkpoint()
                .filter(checkpoint -> !checkpoint.done())
                .map(LandingCheckpoint::itemId);
    }

    /**
     * A push is landed and announced; the same key from the same sender
     * gets the same receipt and lands nothing, even after a restart; from
     * another sender it is a new push.
     *
     * @throws Exception if the test fails
     */
    @Test
    void landsAPushAndRepeatsItsReceiptForTheSameKey() throws Exception
    {
        PushService service = service(0);
        PushService.Result first = push(service, "a", "k-1", "0123456789ab");
        assertThat(first.status()).isEqualTo(PushService.Status.LANDED);
        assertThat(first.receipt().size()).isEqualTo(12);
        assertThat(first.receipt().sha256()).hasSize(64);
        assertThat(sent).hasSize(1);

        PushService.Result again = push(service, "a", "k-1", "other");
        assertThat(again.status()).isEqualTo(PushService.Status.REPLAYED);
        assertThat(again.receipt()).isEqualTo(first.receipt());
        assertThat(sent).hasSize(1);

        // A restart fences the old service, and announces the last item
        // again, since its message may not have been acknowledged.
        PushService restarted = service(0);
        PushService.Result after = push(restarted, "a", "k-1", "other");
        assertThat(after.status()).isEqualTo(PushService.Status.REPLAYED);
        assertThat(after.receipt()).isEqualTo(first.receipt());
        assertThat(sent).hasSize(2);

        PushService.Result other =
                push(restarted, "b", "k-1", "0123456789ab");
        assertThat(other.status()).isEqualTo(PushService.Status.LANDED);
        assertThat(other.receipt().itemId())
                .isNotEqualTo(first.receipt().itemId());
        assertThat(sent).hasSize(3);
        assertThat(fatal).isEmpty();
    }

    /**
     * A body that breaks off part-way, after chunks were committed, is
     * refused and its checkpoint cleared, so the next push lands.
     *
     * @throws Exception if the test fails
     */
    @Test
    void clearsAPushThatBrokeOff() throws Exception
    {
        PushService service = service(0);
        InputStream breaks = new InputStream()
        {
            int given;

            @Override
            public int read()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public int read(byte[] buffer, int offset, int length)
                    throws IOException
            {
                if (given >= 24 && length > 0)
                {
                    throw new IOException("connection reset");
                }
                int count = Math.min(length, 4);
                given += count;
                return count;
            }
        };
        PushService.Result broken = service.receive(
                new PushService.Push("a", null, -1), breaks);
        assertThat(broken.status()).isEqualTo(PushService.Status.BROKEN);
        assertThat(unfinished(lander)).isEmpty();
        assertThat(push(service, "a", null, "after").status())
                .isEqualTo(PushService.Status.LANDED);
        assertThat(sent).hasSize(1);
        assertThat(fatal).isEmpty();
    }

    /**
     * Bodies over the limit, or shorter or longer than their stated
     * length, are refused and leave nothing unfinished.
     *
     * @throws Exception if the test fails
     */
    @Test
    void refusesBodiesOfTheWrongSize() throws Exception
    {
        PushService service = service(10);
        assertThat(push(service, "a", null, "01234567890").status())
                .isEqualTo(PushService.Status.TOO_LARGE);
        assertThat(service.receive(new PushService.Push("a", null, 9),
                new ByteArrayInputStream("0123".getBytes())).status())
                .isEqualTo(PushService.Status.BROKEN);
        assertThat(service.receive(new PushService.Push("a", null, 2),
                new ByteArrayInputStream("0123".getBytes())).status())
                .isEqualTo(PushService.Status.BROKEN);
        assertThat(unfinished(lander)).isEmpty();
        assertThat(push(service, "a", null, "0123456789").status())
                .isEqualTo(PushService.Status.LANDED);
        assertThat(sent).hasSize(1);
    }

    /**
     * While the broker is down, a landed push keeps its receipt, but no
     * further push lands until its message is sent; then both are.
     *
     * @throws Exception if the test fails
     */
    @Test
    void landsNothingMoreUntilTheLastPushIsAnnounced() throws Exception
    {
        PushService service = service(0);
        failSends.set(2);
        PushService.Result first = push(service, "a", null, "one");
        assertThat(first.status()).isEqualTo(PushService.Status.LANDED);
        assertThat(push(service, "a", null, "two").status())
                .isEqualTo(PushService.Status.UNAVAILABLE);
        assertThat(sent).isEmpty();
        assertThat(push(service, "a", null, "three").status())
                .isEqualTo(PushService.Status.LANDED);
        assertThat(sent).hasSize(2);
        assertThat(fatal).isEmpty();
    }

    /**
     * After a crash part-way through a push, startup clears its
     * checkpoint, since a push can't be resumed, and pushes land again.
     *
     * @throws Exception if the test fails
     */
    @Test
    void clearsAnUnfinishedPushAtStartup() throws Exception
    {
        ItemLander crashing = lander(committer());
        crashing.afterCommit(nextBlock ->
        {
            throw new Crash();
        });
        UUID itemId = UUID.randomUUID();
        assertThatThrownBy(() -> crashing.land(new NewItem(itemId,
                ItemKind.PUSH, SourceRef.of(ids, INTAKE, "push", -1, ""),
                null, null), offset -> new ByteArrayInputStream(
                        new byte[40]))).isInstanceOf(Crash.class);
        assertThat(unfinished(crashing)).contains(itemId);

        PushService service = service(0);
        service.recoverAtStartup();
        assertThat(unfinished(lander)).isEmpty();
        assertThat(push(service, "a", null, "fresh").status())
                .isEqualTo(PushService.Status.LANDED);
        assertThat(fatal).isEmpty();
    }

    /**
     * A push's receipt goes to its producer, sealed to the producer's key,
     * with its correlation ID and the item it replaces; a push naming an
     * item this intake didn't land is refused, and nothing is landed.
     *
     * @throws Exception if the test fails
     */
    @Test
    void sendsAReceiptWithTheProducersReferences() throws Exception
    {
        PushService service = service(0);
        PushService.Result first = push(service, "sender", null, "one");
        UUID earlier = first.receipt().itemId();
        PushService.Result second = service.receive(new PushService.Push(
                "sender", null, 3, "corr-42", earlier),
                new ByteArrayInputStream("two".getBytes()));
        assertThat(second.status()).isEqualTo(PushService.Status.LANDED);

        MessageOpener opener = new MessageOpener(
                Map.of("producer", producer.getPrivate()),
                Map.of("connector", (ECPublicKey) signing.getPublic()));
        assertThat(results).hasSize(2);
        ConnectorMessage.ItemReceived receipt = (ConnectorMessage.ItemReceived)
                opener.open(results.get(1).body());
        assertThat(receipt.itemId()).isEqualTo(second.receipt().itemId());
        assertThat(receipt.producerRef()).isEqualTo("corr-42");
        assertThat(receipt.replaces()).isEqualTo(earlier);
        assertThat(receipt.sha256()).isEqualTo(second.receipt().sha256());
        assertThat(receipt.kind()).isEqualTo("push");
        assertThat(results.get(1).messageId())
                .isEqualTo("rc-" + receipt.itemId());
        assertThat(((ConnectorMessage.ItemLanded) new MessageOpener(
                Map.of("downstream", recipient.getPrivate()),
                Map.of("connector", (ECPublicKey) signing.getPublic()))
                .open(sent.get(1).body())).replaces()).isEqualTo(earlier);

        int landed = sent.size();
        PushService.Result unknown = service.receive(new PushService.Push(
                "sender", null, 5, null, UUID.randomUUID()),
                new ByteArrayInputStream("three".getBytes()));
        assertThat(unknown.status())
                .isEqualTo(PushService.Status.UNKNOWN_REPLACES);
        assertThat(sent).hasSize(landed);
        assertThat(results).hasSize(2);
    }

    /**
     * An error that means the node can't go on stops it, and the push is
     * told to try later.
     *
     * @throws Exception if the test fails
     */
    @Test
    void stopsTheNodeOnAFatalError() throws Exception
    {
        FencedCommitter committer = committer();
        ItemLander lander = lander(committer);
        PushService service = service(lander, committer, 0);
        lander.afterCommit(nextBlock ->
        {
            throw new Crash();
        });
        assertThat(push(service, "a", null, "0123456789abcdef").status())
                .isEqualTo(PushService.Status.UNAVAILABLE);
        assertThat(fatal).singleElement().isEqualTo("Crash");
    }

    /**
     * Every push counts by its result, with how long it waited; a replay is
     * a duplicate; a landed push counts with its bytes.
     *
     * @throws Exception if the test fails
     */
    @Test
    void countsPushesByResult() throws Exception
    {
        PushService service = service(0);
        push(service, "a", "k-1", "0123456789ab");
        push(service, "a", "k-1", "other");
        Attributes intake = Attributes.of(
                AttributeKey.stringKey("sdh.intake"), INTAKE);
        AttributeKey<String> result = AttributeKey.stringKey("sdh.result");
        assertThat(telemetry.value("sdh.push.requests",
                intake.toBuilder().put(result, "landed").build()))
                .isEqualTo(1);
        assertThat(telemetry.value("sdh.push.requests",
                intake.toBuilder().put(result, "replayed").build()))
                .isEqualTo(1);
        assertThat(telemetry.count("sdh.push.wait_duration", intake))
                .isEqualTo(2);
        assertThat(telemetry.value("sdh.landing.duplicates", intake))
                .isEqualTo(1);
        Attributes pushes = intake.toBuilder().put("sdh.kind", "push")
                .build();
        assertThat(telemetry.value("sdh.landing.items", pushes)).isEqualTo(1);
        assertThat(telemetry.value("sdh.landing.bytes", pushes))
                .isEqualTo(12);
        assertThat(service.waiting()).isZero();
    }
}
