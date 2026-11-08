/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemConverted;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.io.IOException;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Replayer}: every message is sent and acknowledged, in
 * order, across windows, sealed again under its own message ID with a
 * broker ID of its own; a send the broker doesn't acknowledge is tried
 * again, and one that never is fails the replay.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ReplayerTest
{

    /** The time broker IDs are made from. */
    static final Instant NOW = Instant.parse("2026-11-07T06:00:00Z");

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
     * More messages than a window are all acknowledged, each under its
     * broker ID, and open to what was given; the one the broker first
     * refuses is sent again. The second window goes in order, after the
     * first.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void sendsEveryMessageUnderItsOwnBrokerId() throws Exception
    {
        KeyPair signer = ec();
        KeyPair next = ec();
        MessageSealer sealer = new MessageSealer(
                new ECDSASigner((ECPrivateKey) signer.getPrivate()), "stage",
                next.getPublic(), "next");
        List<ConnectorMessage> messages = new ArrayList<>();
        for (int i = 0; i < Replayer.WINDOW + 10; i++)
        {
            messages.add(new ItemConverted(UUID.randomUUID(), "claims", i, 0,
                    NOW));
        }
        List<OutboundMessage> acknowledged = new CopyOnWriteArrayList<>();
        AtomicInteger sends = new AtomicInteger();
        Replayer replayer = new Replayer(sealer, message ->
        {
            if (sends.incrementAndGet() == 3)
            {
                return CompletableFuture.failedFuture(
                        new IOException("broker busy"));
            }
            acknowledged.add(message);
            return CompletableFuture.completedFuture(null);
        }, new RetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(1)),
                Clock.fixed(NOW, ZoneOffset.UTC));

        String suffix = replayer.send(messages);
        assertThat(suffix).isEqualTo("~replay-" + NOW.toEpochMilli());
        assertThat(acknowledged).hasSize(messages.size());
        MessageOpener opener = new MessageOpener(
                Map.of("next", next.getPrivate()),
                Map.of("stage", (ECPublicKey) signer.getPublic()));
        List<String> ids = new ArrayList<>();
        for (OutboundMessage message : acknowledged)
        {
            ConnectorMessage inside = opener.open(message.body());
            assertThat(message.messageId())
                    .isEqualTo(inside.messageId() + suffix);
            ids.add(inside.messageId());
        }
        assertThat(ids).containsExactlyInAnyOrderElementsOf(messages.stream()
                .map(ConnectorMessage::messageId).toList());
        assertThat(ids.subList(Replayer.WINDOW, ids.size()))
                .containsExactlyElementsOf(messages.subList(Replayer.WINDOW,
                        messages.size()).stream()
                        .map(ConnectorMessage::messageId).toList());
    }

    /**
     * A message the broker never acknowledges fails the replay.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void failsWhenTheBrokerNeverAcknowledges() throws Exception
    {
        KeyPair signer = ec();
        MessageSealer sealer = new MessageSealer(
                new ECDSASigner((ECPrivateKey) signer.getPrivate()), "stage",
                ec().getPublic(), "next");
        Replayer replayer = new Replayer(sealer,
                message -> CompletableFuture.failedFuture(
                        new IOException("broker down")),
                new RetryPolicy(2, Duration.ofMillis(1), Duration.ofMillis(1)),
                Clock.fixed(NOW, ZoneOffset.UTC));
        assertThatThrownBy(() -> replayer.send(List.of(new ItemConverted(
                UUID.randomUUID(), "claims", 1, 0, NOW))))
                .isInstanceOf(Exception.class);
    }
}
