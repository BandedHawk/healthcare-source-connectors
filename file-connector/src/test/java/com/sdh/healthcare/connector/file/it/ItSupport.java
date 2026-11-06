/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
     * Waits until the splitter has finished a number of items from its
     * inbox, and nothing is in progress.
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
                && (runtime.inbox().position() < items
                        || runtime.processor().inProgress().isPresent()))
        {
            Thread.sleep(200);
        }
        assertThat(runtime.inbox().position()).as("items split")
                .isGreaterThanOrEqualTo(items);
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
