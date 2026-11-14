/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import java.util.List;

/**
 * Handles a batch of received messages: lands them, for the landing node,
 * or does a stage's work on them. Called from one thread at a time,
 * unless the handler says it may be called from several (on Kafka, one
 * per consumer: {@code consumersCount}), and never with an empty batch.
 * The batch's messages are acknowledged only after it returns
 * {@link Outcome#DONE}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@FunctionalInterface
public interface BatchHandler
{

    /** What became of a batch. */
    enum Outcome
    {

        /** It was handled, or needs nothing more: acknowledge it. */
        DONE,

        /**
         * It couldn't be handled now, but may be later: keep it, and try
         * it again in place after a pause. Handing it back would raise its
         * messages' delivery counts, and a broker with a delivery limit
         * would dead-letter them during a long outage.
         */
        RETRY,

        /**
         * Nothing more can be handled: take no more messages, and leave
         * this batch unacknowledged until the node stops.
         */
        FATAL
    }

    /**
     * Handles a batch.
     *
     * @param messages the messages, in the order the broker gave them
     * @return what became of them
     * @throws InterruptedException if interrupted while waiting
     */
    Outcome handle(List<BrokerMessage> messages) throws InterruptedException;
}
