/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

/**
 * Told whether the broker is answering ({@link BrokerWatch}, the SQS poll
 * strategy, the Artemis route). The user decides what an outage means: the
 * landing node reports it as its intake being unreachable. Called from the
 * consumer's and the watch's threads.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface BrokerHealth
{

    /** The broker answered: a poll, receive or check worked. */
    void answered();

    /**
     * The broker couldn't be reached. Called again for every failure while
     * the outage lasts.
     *
     * @param reason why
     */
    void unreachable(BrokerProblem reason);
}
