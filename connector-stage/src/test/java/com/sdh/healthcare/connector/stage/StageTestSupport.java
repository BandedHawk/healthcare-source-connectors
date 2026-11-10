/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemConverted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

/**
 * What the stage tests share: a Postgres, the keys messages are sealed
 * with on the way in and out, a processor that stores record ids, and a
 * publisher that keeps what it is sent.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class StageTestSupport
{

    /** One Postgres for every test; each test uses its own schema. */
    static final EmbeddedPostgres POSTGRES;

    /** Signs what the stage receives, as the stage before would. */
    static final KeyPair UPSTREAM_SIGNER;

    /** The stage's own key, that what it receives is sealed to. */
    static final KeyPair STAGE_RECIPIENT;

    /** Signs what the stage sends. */
    static final KeyPair STAGE_SIGNER;

    /** The key of the stage after, that what it sends is sealed to. */
    static final KeyPair DOWNSTREAM_RECIPIENT;

    /** Where a test's records are said to be stored. */
    static final StorageRef REF = new StorageRef("cat", "sdh_split",
            "split_records", LocalDate.of(2026, 11, 5), 7L, "kek-1");

    /** The KEK corrections are wrapped with in the tests. */
    static final String CORRECTION_KEK = "kek-test";

    /** Wraps the corrections' keys. */
    static final com.sdh.healthcare.connector.contract.crypto.KeyProvider
            CORRECTION_KEYS = new com.sdh.healthcare.connector.contract
                    .crypto.AesKeyWrapProvider(Map.of(CORRECTION_KEK,
                            new javax.crypto.spec.SecretKeySpec(new byte[32],
                                    "AES")));

    /** How many schemas have been handed out. */
    private static final AtomicInteger SCHEMAS = new AtomicInteger();

    static
    {
        try
        {
            POSTGRES = EmbeddedPostgres.start();
            UPSTREAM_SIGNER = ec();
            STAGE_RECIPIENT = ec();
            STAGE_SIGNER = ec();
            DOWNSTREAM_RECIPIENT = ec();
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        catch (GeneralSecurityException failure)
        {
            throw new IllegalStateException(failure);
        }
    }

    /** Not instantiated: this class only has static members. */
    private StageTestSupport()
    {
    }

    /**
     * Makes a key pair on P-256.
     *
     * @return the pair
     * @throws GeneralSecurityException if the JDK can't
     */
    static KeyPair ec() throws GeneralSecurityException
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    /**
     * The database.
     *
     * @return its data source
     */
    static DataSource database()
    {
        return POSTGRES.getPostgresDatabase();
    }

    /**
     * A schema no other test uses.
     *
     * @return its name
     */
    static String freshSchema()
    {
        return "stage_test_" + SCHEMAS.incrementAndGet();
    }

    /**
     * A registry for a worker, with its tables made.
     *
     * @param schema the schema
     * @param owner  the worker's name
     * @return the registry
     */
    static StageRegistry registry(String schema, String owner)
    {
        StageRegistry registry = new StageRegistry(database(), schema,
                "convert", owner, java.time.Duration.ofSeconds(10));
        registry.createTablesIfMissing();
        return registry;
    }

    /**
     * Seals messages as the stage before sends them.
     *
     * @return the sealer
     */
    static MessageSealer upstreamSealer()
    {
        return sealer(UPSTREAM_SIGNER, STAGE_RECIPIENT);
    }

    /**
     * Seals the stage's messages.
     *
     * @return the sealer
     */
    static MessageSealer stageSealer()
    {
        return sealer(STAGE_SIGNER, DOWNSTREAM_RECIPIENT);
    }

    /**
     * A sealer.
     *
     * @param signer    signs
     * @param recipient whose key it seals to
     * @return the sealer
     */
    private static MessageSealer sealer(KeyPair signer, KeyPair recipient)
    {
        try
        {
            return new MessageSealer(
                    new ECDSASigner((ECPrivateKey) signer.getPrivate()),
                    "sig", recipient.getPublic(), "rcp");
        }
        catch (com.nimbusds.jose.JOSEException failure)
        {
            throw new IllegalStateException(failure);
        }
    }

    /**
     * Opens what the stage receives.
     *
     * @return the opener
     */
    static MessageOpener stageOpener()
    {
        return new MessageOpener(Map.of("rcp", STAGE_RECIPIENT.getPrivate()),
                Map.of("sig", (ECPublicKey) UPSTREAM_SIGNER.getPublic()));
    }

    /**
     * Opens what the stage sends, as the stage after would.
     *
     * @param body the sealed message
     * @return the message
     */
    static ConnectorMessage openDownstream(byte[] body)
    {
        try
        {
            return new MessageOpener(
                    Map.of("rcp", DOWNSTREAM_RECIPIENT.getPrivate()),
                    Map.of("sig", (ECPublicKey) STAGE_SIGNER.getPublic()))
                    .open(body);
        }
        catch (MessageRejectedException failure)
        {
            throw new IllegalStateException(failure);
        }
    }

    /**
     * A record's message, as the stage before sends it, ready to receive.
     *
     * @param itemId its item
     * @param seq    its number in the item
     * @return the message, sealed
     */
    static BrokerMessage record(UUID itemId, int seq)
    {
        return received(new RecordStored("rec-" + itemId + "-" + seq, itemId,
                seq, seq * 10L, 10, "csv", "application/json", "csv-row/1",
                10, 1, REF));
    }

    /**
     * An item's total, as the stage before sends it.
     *
     * @param itemId  the item
     * @param records how many records it has
     * @return the message, sealed
     */
    static BrokerMessage total(UUID itemId, long records)
    {
        return received(new ItemConverted(itemId, "convert", records, 0,
                Instant.EPOCH));
    }

    /**
     * A message as the stage receives it.
     *
     * @param message the message
     * @return it sealed, with its id
     */
    static BrokerMessage received(ConnectorMessage message)
    {
        return new BrokerMessage(upstreamSealer().seal(message),
                message.messageId(), Map.of());
    }

    /**
     * A processor that stores each record's id, with an output digest of
     * its version and the id.
     */
    static class TestProcessor implements StageProcessor<String>
    {

        /** The version, which decides the output. */
        private final int version;

        /** Every record committed, in order, over all workers. */
        final List<String> committed;

        /** The attempt each record was processed at, last first. */
        final Map<String, Integer> attempts = new ConcurrentHashMap<>();

        /** How many more calls to process are to fail. */
        final AtomicInteger failures = new AtomicInteger();

        /** How many times a record was processed, over all calls. */
        final AtomicInteger processed = new AtomicInteger();

        /** Whether every commit fails, as for a worker about to stop. */
        final java.util.concurrent.atomic.AtomicBoolean commitFails =
                new java.util.concurrent.atomic.AtomicBoolean();

        /** How many commits were tried. */
        final AtomicInteger commits = new AtomicInteger();

        /** Each item-completed step run, in order. */
        final List<ItemProgress> completed = new CopyOnWriteArrayList<>();

        /** Each item-progressed step run, in order. */
        final List<ItemProgress> progressed = new CopyOnWriteArrayList<>();

        /** How many more item-completed steps are to fail. */
        final AtomicInteger completedFailures = new AtomicInteger();

        /**
         * Creates a processor.
         *
         * @param version   the version, which decides the output
         * @param committed where commits go, shared between workers
         */
        TestProcessor(int version, List<String> committed)
        {
            this.version = version;
            this.committed = committed;
        }

        @Override
        public String stage()
        {
            return "convert";
        }

        @Override
        public StageInput classify(ConnectorMessage message)
        {
            return switch (message)
            {
                case RecordStored stored -> new StageInput.RecordInput(
                        stored.recordId(), stored.itemId(), stored.seq(),
                        stored.lineage());
                case ItemConverted completed -> new StageInput.ItemTotal(
                        completed.itemId(), completed.recordCount(), false,
                        completed.completion());
                case ConnectorMessage.ItemProgressed progressed ->
                        new StageInput.ItemTotal(progressed.itemId(),
                                progressed.passedCount(), true,
                                progressed.completion());
                default -> new StageInput.Unwanted("wrong_type");
            };
        }

        @Override
        public String version(ConnectorMessage message)
        {
            return "v" + version;
        }

        @Override
        public StageOutput<String> process(ConnectorMessage message,
                int attempt)
        {
            processed.incrementAndGet();
            if (failures.getAndUpdate(left -> Math.max(0, left - 1)) > 0)
            {
                throw new IllegalStateException("A dependency is down");
            }
            RecordStored stored = (RecordStored) message;
            attempts.put(stored.recordId(), attempt);
            boolean failed = stored.seq() % 5 == 4;
            return new StageOutput<>("v" + version + ":" + stored.recordId(),
                    failed, stored.recordId(), failed ? "TEST_FAILED" : null);
        }

        @Override
        public List<ConnectorMessage> commit(
                List<StageOutput<String>> outputs)
        {
            commits.incrementAndGet();
            if (commitFails.get())
            {
                throw new IllegalStateException("Storage is down");
            }
            List<ConnectorMessage> messages = new ArrayList<>();
            for (StageOutput<String> output : outputs)
            {
                committed.add(output.value());
                if (output.failed())
                {
                    // Not sent on (STAGES.md §15.1).
                    messages.add(null);
                    continue;
                }
                messages.add(new RecordStored(output.value(),
                        UUID.fromString(output.value().substring(4, 40)), 0,
                        0, 10, "csv", "application/json", "test/1", 10, 1,
                        REF));
            }
            return messages;
        }

        @Override
        public boolean corrects()
        {
            return true;
        }

        /**
         * A correction passes unless its content says {@code bad}.
         */
        @Override
        public Corrected<String> correct(
                com.sdh.healthcare.connector.core.failure.CorrectionStore
                        .Pending correction)
        {
            String content = new String(correction.open(CORRECTION_KEYS),
                    java.nio.charset.StandardCharsets.UTF_8);
            boolean failed = content.contains("bad");
            String value = "rec-" + correction.itemId() + "-"
                    + correction.seq() + "-c" + correction.correction();
            return new Corrected<>(new StageOutput<>("v" + version + ":"
                    + value, failed, value, failed ? "TEST_FAILED" : null),
                    "v" + version);
        }

        @Override
        public ConnectorMessage itemComplete(ItemProgress progress)
        {
            return new ItemConverted(progress.itemId(), "convert",
                    progress.done(), progress.unresolved(), Instant.EPOCH,
                    List.of(), progress.nextCompletion());
        }

        @Override
        public ConnectorMessage itemProgress(ItemProgress progress)
        {
            return new ConnectorMessage.ItemProgressed(progress.itemId(),
                    "convert", "convert", progress.done(), progress.passed(),
                    progress.open(), progress.declined(), null, null, null,
                    Instant.EPOCH, progress.versions(),
                    progress.completion());
        }

        @Override
        public void itemProgressed(ItemProgress progress)
        {
            progressed.add(progress);
        }

        @Override
        public void itemCompleted(ItemProgress progress)
        {
            if (completedFailures.getAndUpdate(left -> Math.max(0, left - 1))
                    > 0)
            {
                throw new IllegalStateException("Results are down");
            }
            completed.add(progress);
        }
    }

    /**
     * A publisher that keeps what it is sent, and can be made to fail.
     */
    static final class RecordingPublisher implements MessagePublisher
    {

        /** What was sent, in order. */
        final List<OutboundMessage> sent =
                Collections.synchronizedList(new ArrayList<>());

        /** How many more sends are to fail. */
        final AtomicInteger failures = new AtomicInteger();

        /** Creates a publisher that takes everything. */
        RecordingPublisher()
        {
        }

        @Override
        public CompletableFuture<Void> publish(OutboundMessage message)
        {
            if (failures.getAndUpdate(left -> Math.max(0, left - 1)) > 0)
            {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("The broker is down"));
            }
            sent.add(message);
            return CompletableFuture.completedFuture(null);
        }

        /**
         * What was sent, opened.
         *
         * @return the messages, in order
         */
        List<ConnectorMessage> opened()
        {
            List<ConnectorMessage> messages = new ArrayList<>();
            synchronized (sent)
            {
                for (OutboundMessage message : sent)
                {
                    messages.add(openDownstream(message.body()));
                }
            }
            return messages;
        }
    }
}
