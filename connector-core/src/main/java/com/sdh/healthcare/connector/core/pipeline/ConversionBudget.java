/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.pipeline;

import java.util.concurrent.Semaphore;

/**
 * Heap set aside for converting sections, shared by the pipeline's workers (design §6.4). Before a section is read
 * and converted, its worker takes what the conversion will need at its peak, and gives it back once the section is
 * ready to write; a conversion that doesn't fit waits for others to finish. Small sections need next to nothing, so
 * only large ones ever wait, and several large sections can't run the connector out of memory together.
 *
 * <p>Counted in KiB. Waiting is first come, first served, so a large section isn't starved by a stream of small
 * ones. A section needing more than the whole budget takes all of it and runs alone.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ConversionBudget {
    private final Semaphore free;
    private final int total;

    /** @param bytes the budget; at least 1 KiB is used */
    ConversionBudget(long bytes) {
        total = (int) Math.min(Integer.MAX_VALUE, Math.max(1, bytes >> 10));
        free = new Semaphore(total, true);
    }

    /**
     * Waits until {@code bytes} are free and takes them.
     *
     * @return what was taken, to hand back to {@link #release}
     * @throws InterruptedException if the wait is interrupted (the batch is being abandoned)
     */
    int acquire(long bytes) throws InterruptedException {
        int kib = (int) Math.min(total, Math.max(1, (bytes + 1023) >> 10));
        free.acquire(kib);
        return kib;
    }

    void release(int taken) {
        free.release(taken);
    }

    /** KiB free now; for tests. */
    int available() {
        return free.availablePermits();
    }
}
