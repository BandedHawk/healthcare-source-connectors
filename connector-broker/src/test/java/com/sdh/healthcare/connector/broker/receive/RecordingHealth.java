/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

/**
 * A broker's health as the tests see it: whether it answered last, why not,
 * and how often it answered.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class RecordingHealth implements BrokerHealth
{

    /** Why the broker can't be reached; null while it answers. */
    private BrokerProblem problem;

    /** How many times the broker answered. */
    private int answers;

    @Override
    public synchronized void answered()
    {
        problem = null;
        answers++;
    }

    @Override
    public synchronized void unreachable(BrokerProblem reason)
    {
        problem = reason;
    }

    /**
     * Whether the broker answered last, or hasn't been tried.
     *
     * @return true unless an outage is in progress
     */
    synchronized boolean reachable()
    {
        return problem == null;
    }

    /**
     * Why the broker can't be reached.
     *
     * @return the reason; null while it answers
     */
    synchronized BrokerProblem problem()
    {
        return problem;
    }

    /**
     * How many times the broker answered.
     *
     * @return the count
     */
    synchronized int answers()
    {
        return answers;
    }
}
