/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

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
class ConversionBudgetTest
{

    /**
     * Room is taken in KiB, rounded up, and a request larger than the
     * budget takes all of it.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void takesWhatIsAskedUpToTheWholeBudget() throws Exception
    {
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

    /**
     * A conversion that doesn't fit waits until another gives its room
     * back.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void waitsForRoom() throws Exception
    {
        ConversionBudget budget = new ConversionBudget(10 << 10);
        int first = budget.acquire(8 << 10);
        CompletableFuture<Integer> second = CompletableFuture.supplyAsync(() ->
        {
            try
            {
                return budget.acquire(4 << 10);
            }
            catch (InterruptedException interrupted)
            {
                throw new IllegalStateException(interrupted);
            }
        });
        assertTimesOut(() -> second.get(200, TimeUnit.MILLISECONDS));
        budget.release(first);
        assertThat(second.get(5, TimeUnit.SECONDS)).isEqualTo(4);
    }

    /**
     * Checks that a call times out.
     *
     * @param call the call
     * @throws AssertionError if it returns or fails otherwise
     */
    private static void assertTimesOut(Executable call)
    {
        try
        {
            call.execute();
            throw new AssertionError("expected to wait");
        }
        catch (TimeoutException expected)
        {
            // still waiting, as it should be
        }
        catch (Throwable other)
        {
            throw new AssertionError(other);
        }
    }
}
