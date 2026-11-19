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
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.SourceRef;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Announcer}'s group sends: a group's messages are sealed
 * several at once and still sent in the order given (docs/PERFORMANCE.md
 * §3.4).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class AnnouncerTest
{

    /** One try, at once. */
    static final RetryPolicy RETRY =
            new RetryPolicy(1, Duration.ofMillis(1), Duration.ofMillis(1));

    /** Creates the tests; JUnit runs each test on a new instance. */
    AnnouncerTest()
    {
    }

    /**
     * A landed item, as a group's would be.
     *
     * @param index its place in the group
     * @return the item
     */
    static LandedItem item(int index)
    {
        return new LandedItem(UUID.randomUUID(), "intake-a", ItemKind.OBJECT,
                new SourceRef("ref-" + index, index, "v1"), index,
                "digest-" + index, Instant.EPOCH, 1, "kek", "sha-" + index,
                null);
    }

    /**
     * A sealer for a fresh key pair each.
     *
     * @return the sealer
     * @throws Exception if the keys can't be made
     */
    static MessageSealer sealer() throws Exception
    {
        KeyPairGenerator ec = KeyPairGenerator.getInstance("EC");
        ec.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair signing = ec.generateKeyPair();
        KeyPair recipient = ec.generateKeyPair();
        return new MessageSealer(
                new ECDSASigner((ECPrivateKey) signing.getPrivate()),
                "connector", recipient.getPublic(), "downstream");
    }

    /**
     * Sealed four at once, a group's messages are sent in its order, each
     * once; one the broker refused is sent again on its own, after the
     * rest.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void sealsSeveralAtOnceAndSendsInOrder() throws Exception
    {
        List<Announcer.Announcement> group = new ArrayList<>();
        for (int index = 0; index < 23; index++)
        {
            group.add(new Announcer.Announcement(item(index), null));
        }
        List<OutboundMessage> sent =
                Collections.synchronizedList(new ArrayList<>());
        AtomicInteger sends = new AtomicInteger();
        Announcer announcer = new Announcer(sealer(), message ->
        {
            // The sixth send is refused once.
            if (sends.incrementAndGet() == 6)
            {
                return CompletableFuture.failedFuture(
                        new IOException("broker down"));
            }
            sent.add(message);
            return CompletableFuture.completedFuture(null);
        }, "sdh", LandingTables.DEFAULT_NAMESPACE, RETRY,
                Duration.ofSeconds(5), null, 4);

        announcer.announceAll(group);

        List<UUID> expected = new ArrayList<>();
        for (Announcer.Announcement announcement : group)
        {
            expected.add(announcement.item().itemId());
        }
        UUID refused = expected.remove(5);
        expected.add(refused);
        assertThat(sent).extracting(OutboundMessage::itemId)
                .containsExactlyElementsOf(expected);
        assertThat(sent).allSatisfy(message ->
                assertThat(message.body()).isNotEmpty());
    }

    /**
     * At least one message is sealed at a time.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void needsAtLeastOneSealer() throws Exception
    {
        MessageSealer sealer = sealer();
        assertThatThrownBy(() -> new Announcer(sealer,
                message -> CompletableFuture.completedFuture(null), "sdh",
                LandingTables.DEFAULT_NAMESPACE, RETRY,
                Duration.ofSeconds(5), null, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
