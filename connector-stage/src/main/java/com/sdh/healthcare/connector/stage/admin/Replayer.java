/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Sends a stage's messages for an item again, for a replay (STAGES.md
 * §13.2): each is the message first sent, sealed again, under its own
 * {@code messageId}, but the broker is given an ID of its own,
 * {@code <messageId>~replay-<millis>}, since Artemis, Service Bus and SQS
 * FIFO drop a message whose ID they have seen without saying so. The next
 * stage recognises records by item and record ID, so nothing downstream
 * changes. Messages are sent in order, a window at a time, and each must be
 * acknowledged by the broker; one that isn't is tried again with the
 * stage's retry settings.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Replayer
{

    /** What a replay adds to the message ID, for the broker's ID. */
    public static final String REPLAY_SUFFIX = "~replay-";

    /** How long to wait for the broker's acknowledgement of each send. */
    static final Duration ACK_TIMEOUT = Duration.ofSeconds(30);

    /** How many messages are sent before their acknowledgements are awaited. */
    static final int WINDOW = 256;

    /** Seals each message to the next stage. */
    private final MessageSealer sealer;

    /** Sends to the stage's output. */
    private final MessagePublisher publisher;

    /** How a send that isn't acknowledged is tried again. */
    private final RetryPolicy retry;

    /** Gives the time that makes each replay's broker IDs new. */
    private final Clock clock;

    /**
     * Creates the sender.
     *
     * @param sealer    seals each message to the next stage
     * @param publisher sends to the stage's output
     * @param retry     how a send is tried again
     * @param clock     gives the time
     */
    public Replayer(MessageSealer sealer, MessagePublisher publisher,
            RetryPolicy retry, Clock clock)
    {
        this.sealer = sealer;
        this.publisher = publisher;
        this.retry = retry;
        this.clock = clock;
    }

    /**
     * Seals and sends messages again, in order, and waits for the broker.
     *
     * @param messages the messages, as first sent
     * @return the suffix their broker IDs were given
     * @throws Exception if one can't be sealed, or isn't acknowledged
     *                   after the retries
     */
    public String send(List<? extends ConnectorMessage> messages)
            throws Exception
    {
        String suffix = REPLAY_SUFFIX + clock.millis();
        for (int from = 0; from < messages.size(); from += WINDOW)
        {
            List<OutboundMessage> window = new ArrayList<>();
            List<CompletableFuture<Void>> sends = new ArrayList<>();
            for (ConnectorMessage message : messages.subList(from,
                    Math.min(messages.size(), from + WINDOW)))
            {
                OutboundMessage first =
                        OutboundMessage.of(message, sealer.seal(message));
                OutboundMessage replay = new OutboundMessage(
                        first.messageId() + suffix, first.type(),
                        first.itemId(), first.key(), first.body());
                window.add(replay);
                sends.add(publisher.publish(replay));
            }
            for (int i = 0; i < window.size(); i++)
            {
                OutboundMessage replay = window.get(i);
                try
                {
                    sends.get(i).get(ACK_TIMEOUT.toMillis(),
                            TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException interrupted)
                {
                    Thread.currentThread().interrupt();
                    throw interrupted;
                }
                catch (Exception unacknowledged)
                {
                    retry.call("stage.replay", failure -> true, attempt ->
                            publisher.publish(replay).get(
                                    ACK_TIMEOUT.toMillis(),
                                    TimeUnit.MILLISECONDS));
                }
            }
        }
        return suffix;
    }
}
