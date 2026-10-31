/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Tests for {@link ConversionBudget}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ConversionBudgetTest {

    /** Room is taken in KiB, rounded up, and a request larger than the budget takes all of it. */
    @Test
    void takesWhatIsAskedUpToTheWholeBudget() throws Exception {
        ConversionBudget budget = new ConversionBudget(10 << 10);
        int small = budget.acquire(1);
        assertThat(small).isEqualTo(1);
        budget.release(small);
        int huge = budget.acquire(1L << 40);
        assertThat(huge).isEqualTo(10);
        assertThat(budget.available()).isZero();
        budget.release(huge);
        assertThat(budget.available()).isEqualTo(10);
    }

    /** A conversion that doesn't fit waits until another gives its room back. */
    @Test
    void waitsForRoom() throws Exception {
        ConversionBudget budget = new ConversionBudget(10 << 10);
        int first = budget.acquire(8 << 10);
        CompletableFuture<Integer> second = CompletableFuture.supplyAsync(() -> {
            try {
                return budget.acquire(4 << 10);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        assertTimesOut(() -> second.get(200, TimeUnit.MILLISECONDS));
        budget.release(first);
        assertThat(second.get(5, TimeUnit.SECONDS)).isEqualTo(4);
    }

    private static void assertTimesOut(Executable e) {
        try {
            e.execute();
            throw new AssertionError("expected to wait");
        } catch (TimeoutException expected) {
            // still waiting, as it should be
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
