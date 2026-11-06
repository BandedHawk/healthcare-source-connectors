/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import java.util.concurrent.Semaphore;

/**
 * Heap set aside for converting records, shared by the threads that convert
 * (design §6.4). Before a record is read and converted, its worker takes
 * what the conversion will need at its peak, and gives it back once the
 * record is ready to write; a conversion that doesn't fit waits for
 * others to finish. Small records need next to nothing, so only large
 * ones ever wait, and several large records can't run the connector out
 * of memory together.
 * <p>
 * Counted in KiB. Waiting is first come, first served, so a large record
 * isn't starved by a stream of small ones. A record needing more than the
 * whole budget takes all of it and runs alone.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ConversionBudget
{

    /** The KiB free, handed out in arrival order. */
    private final Semaphore free;

    /** The whole budget, in KiB. */
    private final int total;

    /**
     * Creates a budget.
     *
     * @param bytes the budget; at least 1 KiB is used
     */
    public ConversionBudget(long bytes)
    {
        total = (int) Math.min(Integer.MAX_VALUE, Math.max(1, bytes >> 10));
        free = new Semaphore(total, true);
    }

    /**
     * Waits until {@code bytes} are free and takes them.
     *
     * @param bytes what the conversion needs at its peak
     * @return what was taken, to hand back to {@link #release}
     * @throws InterruptedException if the wait is interrupted (the batch is
     *                              being abandoned)
     */
    public int acquire(long bytes) throws InterruptedException
    {
        int kib = (int) Math.min(total, Math.max(1, (bytes + 1023) >> 10));
        free.acquire(kib);
        return kib;
    }

    /**
     * Gives back what {@link #acquire} took.
     *
     * @param taken what it returned
     */
    public void release(int taken)
    {
        free.release(taken);
    }

    /**
     * KiB free now; for tests.
     *
     * @return the KiB free
     */
    int available()
    {
        return free.availablePermits();
    }
}
