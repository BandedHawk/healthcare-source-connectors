/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;


/**
 * Asks a broker, through its own client, whether the consumer can still
 * consume from it, for a broker whose Camel consumer doesn't say when it
 * loses or regains its broker (design §5.3.16). A check never receives,
 * locks or settles a message.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
interface BrokerCheck extends AutoCloseable
{

    /**
     * Asks the broker.
     *
     * @return null if it answered and the queue, topic or subscription is
     *         there; otherwise why not
     * @throws InterruptedException if interrupted while waiting
     */
    BrokerProblem check() throws InterruptedException;

    /** Closes the client. */
    @Override
    void close();
}
