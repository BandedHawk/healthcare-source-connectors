/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.send;

import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.util.concurrent.CompletableFuture;

/**
 * Sends one message at a time: each send starts once the previous one is
 * acknowledged (or failed; the pipeline retries failures). For brokers whose
 * client reorders sends that are in flight together
 * ({@link QueueType#keepsOrderWhenPipelined()}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class SequentialPublisher implements ClosablePublisher
{

    /** Does the sending. */
    private final ClosablePublisher inner;

    /** The last send started; the next one waits for it. */
    private CompletableFuture<Void> tail =
            CompletableFuture.completedFuture(null);

    /**
     * Wraps a publisher.
     *
     * @param inner does the sending
     */
    SequentialPublisher(ClosablePublisher inner)
    {
        this.inner = inner;
    }

    /**
     * Sends a message once the one before it has been acknowledged or has
     * failed.
     *
     * @param message the message
     * @return completes when the broker acknowledges it
     */
    @Override
    public synchronized CompletableFuture<Void> publish(
            OutboundMessage message)
    {
        CompletableFuture<Void> next = tail
                .handle((ignored, failure) -> null)
                .thenCompose(ignored -> inner.publish(message));
        tail = next;
        return next;
    }

    /** Closes the wrapped publisher. */
    @Override
    public void close()
    {
        inner.close();
    }
}
