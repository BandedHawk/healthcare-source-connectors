/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.broker.receive.BatchHandler.Outcome;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.checkpoint.LeasedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.TableCommitter;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.TestLanding;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.file.split.SplitHandler;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.interfaces.ECPublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.DockerClientFactory;

/**
 * Shared checks for the broker and storage integration tests.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ItSupport
{

    /** The intake the splitter serves in the integration tests. */
    static final String INTAKE = "claims";

    /** Not instantiable. */
    private ItSupport()
    {
    }

    /**
     * The splitter's own inbox key and the landing node's signing key, as
     * {@code sdh.crypto.keys.*} settings.
     *
     * @param keys the key files
     * @return the settings
     */
    static Map<String, String> splitterKeys(TestKeys keys)
    {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("sdh.crypto.keys.inbox.kid", TestKeys.INBOX_KID);
        settings.put("sdh.crypto.keys.inbox.pem", keys.inboxPem.toString());
        settings.put("sdh.crypto.keys.inbox.passphrase",
                TestKeys.INBOX_PASSPHRASE);
        settings.put("sdh.crypto.keys.landing-signer.kid",
                TestKeys.LANDING_SIGNER_KID);
        settings.put("sdh.crypto.keys.landing-signer.pem",
                keys.landingSignerPem.toString());
        return settings;
    }

    /**
     * The split stage's settings, for a test that hands it its
     * {@code item.landed} messages itself: the input route is never
     * started ({@code sdh.split.autostart=false}), and nothing listens at
     * its address. Items are CSV with a header, committed in batches of
     * 7 records, and the stage runs no maintenance of its own.
     *
     * @param keys     the key files
     * @param catalog  the Iceberg catalog's properties
     * @param jdbcUrl  the lease database
     * @param user     its user
     * @param password its password
     * @param output   the Camel endpoint the stage's messages go to
     * @return the settings, by Spring property name
     */
    static Map<String, String> splitStage(TestKeys keys,
            Map<String, String> catalog, String jdbcUrl, String user,
            String password, String output)
    {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("sdh.landing.intake", INTAKE);
        settings.put("sdh.stage.input.type", "kafka");
        settings.put("sdh.stage.input.uri",
                "kafka:sdh.landed?brokers=localhost:1&groupId=split");
        settings.put("sdh.stage.registry-schema", "sdh_split");
        settings.put("sdh.stage.ready-port", "0");
        settings.put("sdh.stage.output.type", "camel");
        settings.put("sdh.stage.output.uri", output);
        settings.put("sdh.split.format", "csv");
        settings.put("sdh.split.options.csv.header", "true");
        settings.put("sdh.storage.catalog-name", TestLanding.CATALOG);
        catalog.forEach((key, value) -> settings.put(
                "sdh.storage.catalog[" + key + "]", value));
        settings.put("sdh.storage.commit.max-records", "7");
        // The tests run maintenance themselves, between items.
        settings.put("sdh.maintenance.enabled", "false");
        settings.put("sdh.lease.jdbc.url", jdbcUrl);
        settings.put("sdh.lease.jdbc.username", user);
        settings.put("sdh.lease.jdbc.password", password);
        settings.put("sdh.crypto.keystore.path", keys.keystore.toString());
        settings.put("sdh.crypto.keystore.password", TestKeys.KS_PASSWORD);
        settings.put("sdh.crypto.keys.records-kek", TestKeys.RECORDS);
        settings.put("sdh.crypto.keys.landed-items-kek", TestKeys.LANDED);
        settings.put("sdh.crypto.keys.signing.kid", "connector-2026");
        settings.put("sdh.crypto.keys.signing.pem",
                keys.signingPem.toString());
        settings.put("sdh.crypto.keys.signing.passphrase",
                TestKeys.SIGNING_PASSPHRASE);
        settings.put("sdh.crypto.keys.recipient.kid", "downstream-2026");
        settings.put("sdh.crypto.keys.recipient.pem",
                keys.recipientPem.toString());
        try
        {
            settings.put("sdh.crypto.keys.recipient.sha256",
                    KeyMaterial.fingerprint(keys.recipient.getPublic()));
        }
        catch (Exception failure)
        {
            throw new IllegalStateException(failure);
        }
        settings.putAll(splitterKeys(keys));
        return settings;
    }

    /**
     * The convert stage's settings, as its {@code application.yml} would
     * hold them, flat: it reads the split stage's messages from Kafka and
     * stores and announces each record, with keys that follow on from the
     * split stage's (the split stage's recipient is its inbox). Secrets are
     * left out, for the environment, unless asked for.
     *
     * @param keys     the key files
     * @param keyPath  where the stage finds a key file, such as a path in
     *        a mounted secret
     * @param catalog  the Iceberg catalog's properties, as the stage
     *        reaches the catalog
     * @param lease    the lease database's URL, user and password
     * @param brokers  the Kafka bootstrap servers, as the stage reaches them
     * @param input    the topic the split stage sends to
     * @param output   the topic for downstream readers
     * @param secrets  whether to add the keystore password and key
     *        passphrases
     * @return the settings, by Spring property name
     * @throws Exception if the readers' key's fingerprint can't be made
     */
    static Map<String, String> convertStage(TestKeys keys,
            Function<Path, String> keyPath, Map<String, String> catalog,
            List<String> lease, String brokers, String input, String output,
            boolean secrets) throws Exception
    {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("sdh.stage.input.type", "kafka");
        settings.put("sdh.stage.input.uri", "kafka:" + input + "?brokers="
                + brokers + "&groupId=convert-stage");
        settings.put("sdh.stage.output.type", "kafka");
        settings.put("sdh.stage.output.uri",
                "kafka:" + output + "?brokers=" + brokers);
        settings.put("sdh.stage.dlq.type", "kafka");
        settings.put("sdh.stage.dlq.uri",
                "kafka:sdh-convert-dlq?brokers=" + brokers);
        settings.put("sdh.stage.registry-schema", "sdh_convert");
        settings.put("sdh.stage.ready-port", "0");
        settings.put("sdh.convert.options[csv.header]", "true");
        settings.put("sdh.storage.catalog-name", TestLanding.CATALOG);
        catalog.forEach((key, value) -> settings.put(
                "sdh.storage.catalog[" + key + "]", value));
        settings.put("sdh.lease.jdbc.url", lease.get(0));
        settings.put("sdh.lease.jdbc.username", lease.get(1));
        settings.put("sdh.lease.jdbc.password", lease.get(2));
        settings.put("sdh.crypto.keystore.path", keyPath.apply(keys.keystore));
        settings.put("sdh.crypto.keys.records-kek", TestKeys.RECORDS);
        settings.put("sdh.crypto.keys.signing.kid",
                TestKeys.CONVERT_SIGNER_KID);
        settings.put("sdh.crypto.keys.signing.pem",
                keyPath.apply(keys.convertSigningPem));
        settings.put("sdh.crypto.keys.recipient.kid", TestKeys.READER_KID);
        settings.put("sdh.crypto.keys.recipient.pem",
                keyPath.apply(keys.readerPem));
        settings.put("sdh.crypto.keys.recipient.sha256",
                KeyMaterial.fingerprint(keys.reader.getPublic()));
        settings.put("sdh.crypto.keys.inbox.kid", TestKeys.CONVERT_INBOX_KID);
        settings.put("sdh.crypto.keys.inbox.pem",
                keyPath.apply(keys.convertInboxPem));
        settings.put("sdh.crypto.keys.landing-signer.kid",
                TestKeys.SPLIT_SIGNER_KID);
        settings.put("sdh.crypto.keys.landing-signer.pem",
                keyPath.apply(keys.splitSignerPem));
        if (secrets)
        {
            settings.put("sdh.crypto.keystore.password", TestKeys.KS_PASSWORD);
            settings.put("sdh.crypto.keys.signing.passphrase",
                    TestKeys.SIGNING_PASSPHRASE);
            settings.put("sdh.crypto.keys.inbox.passphrase",
                    TestKeys.INBOX_PASSPHRASE);
        }
        return settings;
    }

    /**
     * A reader of the convert stage's messages, as downstream has one.
     *
     * @param keys    the key files
     * @param catalog the catalog the records are in
     * @param wrap    unwraps the records' keys
     * @return the reader
     */
    static QueueReader convertReader(TestKeys keys, Catalog catalog,
            KeyProvider wrap)
    {
        return QueueReader.builder()
                .recipientKey(TestKeys.READER_KID, keys.reader.getPrivate())
                .trustedSigner(TestKeys.CONVERT_SIGNER_KID,
                        (ECPublicKey) keys.convertSigning.getPublic())
                .catalog(catalog)
                .expectedCatalogName(TestLanding.CATALOG)
                .keyProvider(wrap)
                .build();
    }

    /**
     * Hands items' {@code item.landed} messages to the split stage, one
     * batch each, as its input route would; each is split, or fails, by
     * the time this returns.
     *
     * @param runtime the running stage
     * @param landing the landing node that landed them
     * @param fatal   collects fatal exits
     * @param items   the items
     * @throws InterruptedException if interrupted
     */
    static void split(ConnectorRuntime runtime, TestLanding landing,
            List<String> fatal, LandedItem... items)
            throws InterruptedException
    {
        SplitHandler handler = new SplitHandler(runtime.splitter(),
                runtime.keys().inbox(), INTAKE, TestLanding.CATALOG,
                LandingTables.DEFAULT_NAMESPACE, fatal::add);
        for (LandedItem item : items)
        {
            assertThat(handler.handle(List.of(new BrokerMessage(
                    landing.announcement(item), UUID.randomUUID().toString(),
                    Map.of())))).as("item " + item.itemId())
                    .isEqualTo(Outcome.DONE);
        }
    }

    /**
     * Decodes what a stage has sent to an in-memory queue so far.
     *
     * @param camel  the Camel context the queue is in
     * @param queue  the queue's endpoint
     * @param reader decodes the messages
     * @return the messages, in the order sent
     * @throws Exception if one can't be opened
     */
    static List<ConnectorMessage> drain(CamelContext camel, String queue,
            QueueReader reader) throws Exception
    {
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        List<ConnectorMessage> messages = new ArrayList<>();
        String body;
        while ((body = consumer.receiveBody(queue, 1000, String.class))
                != null)
        {
            messages.add(reader.decode(
                    body.getBytes(StandardCharsets.UTF_8)));
        }
        return messages;
    }

    /**
     * Each split record reads back downstream as it was cut: the CSV
     * header, then its row.
     *
     * @param reader  reads the records
     * @param records the records' messages
     * @param header  the header row, with its line end
     * @param rows    the rows, by sequence number
     * @throws Exception if a record can't be read
     */
    static void assertSplitRecords(QueueReader reader,
            List<RecordSplit> records, String header, List<String> rows)
            throws Exception
    {
        for (RecordSplit record : records)
        {
            assertThat(new String(reader.readRecord(record, 1 << 20),
                    StandardCharsets.UTF_8)).as("record " + record.seq())
                    .isEqualTo(header + rows.get((int) record.seq()));
        }
    }

    /**
     * The split stage's ends for a good item and one it couldn't parse:
     * the good one is split; the bad one fails as a whole and stays open
     * for an operator, so it is reported in progress, never split
     * (STAGES.md §15.6).
     *
     * @param messages the split stage's output
     * @param good     the good item
     * @param bad      the item with an unterminated quote
     */
    static void assertGoodSplitBadOpen(List<ConnectorMessage> messages,
            UUID good, UUID bad)
    {
        assertThat(of(messages, ItemSplit.class))
                .extracting(ItemSplit::itemId).containsExactly(good);
        assertThat(of(messages, ItemProgressed.class)).singleElement()
                .satisfies(progress ->
                {
                    assertThat(progress.itemId()).isEqualTo(bad);
                    assertThat(progress.stage()).isEqualTo("split");
                    assertThat(progress.failure().code())
                            .isEqualTo("PARSE_UNTERMINATED_QUOTE");
                });
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

    /**
     * Commits as maintenance would under the duties lease, with a lease
     * that is always held: for tests that run maintenance themselves.
     *
     * @return the committer
     */
    static TableCommitter committer()
    {
        return new LeasedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return 1;
            }

            @Override
            public void checkHeld()
            {
            }
        });
    }

    /**
     * Waits until the split stage has finished a number of items, completed
     * or failed.
     *
     * @param runtime the running connector
     * @param items   how many items
     * @param timeout how long to wait at most
     * @throws InterruptedException if interrupted while waiting
     */
    static void awaitSplit(ConnectorRuntime runtime, long items,
            Duration timeout) throws InterruptedException
    {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline
                && runtime.tables().checkpoints().countFinished() < items)
        {
            Thread.sleep(200);
        }
        assertThat(runtime.tables().checkpoints().countFinished())
                .as("items split").isGreaterThanOrEqualTo(items);
    }

    /**
     * Fails (not skips) with a pointer to the runner script when no Docker
     * API is reachable.
     */
    static void requireDocker()
    {
        if (!DockerClientFactory.instance().isDockerAvailable())
        {
            Assertions.fail("No Docker API reachable. With minikube, run"
                    + " scripts/it-minikube.sh, or set DOCKER_HOST, "
                    + "DOCKER_TLS_VERIFY and DOCKER_CERT_PATH from"
                    + " `minikube -p sdh-it docker-env`.");
        }
    }

    /**
     * The stored form of a test CSV row {@code id,name,"multi\nline"}
     * (no other quoting): a JSON array of its fields by position, the
     * header dropped (design §6.4, csv-row/1).
     *
     * @param row the row, with or without its line end
     * @return the JSON
     */
    static String csvRowJson(String row)
    {
        String line = row.endsWith("\n")
                ? row.substring(0, row.length() - 1) : row;
        int firstComma = line.indexOf(',');
        int secondComma = line.indexOf(',', firstComma + 1);
        String note = line.substring(secondComma + 1);
        if (note.startsWith("\"") && note.endsWith("\""))
        {
            note = note.substring(1, note.length() - 1);
        }
        return "[\"" + line.substring(0, firstComma) + "\",\""
                + line.substring(firstComma + 1, secondComma) + "\",\""
                + note.replace("\n", "\\n") + "\"]";
    }

    /**
     * Two files' messages interleaved, as two files would never be, to
     * make per-file ordering visible.
     *
     * @param first   the first file's ID
     * @param second  the second file's ID
     * @param perFile how many messages each
     * @return the messages
     */
    static List<OutboundMessage> interleaved(UUID first, UUID second,
            int perFile)
    {
        List<OutboundMessage> out = new ArrayList<>();
        for (int seq = 0; seq < perFile; seq++)
        {
            out.add(message(first, seq));
            out.add(message(second, seq));
        }
        return out;
    }

    /**
     * A test message whose ID and body name its file and sequence number.
     * Its key is the file's ID, so the adapters' order by key is tested
     * with a file's messages sharing one.
     *
     * @param itemId the file's ID
     * @param seq    the sequence number
     * @return the message
     */
    static OutboundMessage message(UUID itemId, int seq)
    {
        String id = itemId + "-" + seq;
        return new OutboundMessage(id, MessageType.RECORD_STORED, itemId,
                itemId.toString(),
                ("jwe." + id).getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Publishes every message and waits up to 2 minutes for all the
     * acknowledgements.
     *
     * @param publisher the publisher
     * @param messages  the messages
     * @throws Exception if a publish fails or times out
     */
    static void publishAll(MessagePublisher publisher,
            List<OutboundMessage> messages) throws Exception
    {
        List<CompletableFuture<Void>> acks = new ArrayList<>();
        for (OutboundMessage message : messages)
        {
            acks.add(publisher.publish(message));
        }
        CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new))
                .get(2, TimeUnit.MINUTES);
    }

    /**
     * One message as a consumer saw it; null where the broker doesn't
     * carry the value.
     *
     * @param messageId   the message ID
     * @param type        the message type
     * @param itemId      the file ID
     * @param contentType the content type
     * @param orderingKey the broker's ordering key
     * @param body        the body
     */
    record Received(String messageId, String type, String itemId,
            String contentType, String orderingKey, String body)
    {
    }

    /**
     * Every message arrived exactly once, each file's in order, with the
     * metadata and body intact.
     *
     * @param sent     the messages sent
     * @param got      the messages received
     * @param metadata whether the broker carries type, file ID and content
     *        type
     */
    static void assertDelivered(List<OutboundMessage> sent,
            List<Received> got, boolean metadata)
    {
        assertThat(got).extracting(Received::messageId)
                .containsExactlyInAnyOrderElementsOf(sent.stream()
                        .map(OutboundMessage::messageId).toList());
        Map<String, List<Integer>> seqByFile = new LinkedHashMap<>();
        for (Received received : got)
        {
            String itemId = received.messageId().substring(0, 36);
            assertThat(received.orderingKey()).as("ordering key")
                    .isEqualTo(itemId);
            assertThat(received.body())
                    .isEqualTo("jwe." + received.messageId());
            if (metadata)
            {
                assertThat(received.itemId()).isEqualTo(itemId);
                assertThat(received.type()).isEqualTo("record.stored");
                assertThat(received.contentType())
                        .isEqualTo(OutboundMessage.CONTENT_TYPE);
            }
            seqByFile.computeIfAbsent(itemId, key -> new ArrayList<>())
                    .add(Integer.parseInt(received.messageId().substring(37)));
        }
        seqByFile.values().forEach(seqs ->
                assertThat(seqs).as("per-file order").isSorted());
    }
}
