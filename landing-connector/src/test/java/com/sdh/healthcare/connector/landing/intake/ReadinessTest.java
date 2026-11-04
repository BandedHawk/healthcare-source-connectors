/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Readiness}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ReadinessTest
{

    /** The fake clock, in nanoseconds. */
    final AtomicLong now = new AtomicLong();

    /** A tracker with a one-minute settle time. */
    final Readiness readiness =
            new Readiness(Duration.ofMinutes(1), now::get);

    /**
     * Moves the clock on.
     *
     * @param seconds how far
     */
    void advance(int seconds)
    {
        now.addAndGet(Duration.ofSeconds(seconds).toNanos());
    }

    /** An item is ready once it has looked the same for the settle time. */
    @Test
    void readyOnceUnchangedForTheSettleTime()
    {
        ListedItem item = new ListedItem("a.csv", 10, 1000);
        assertThat(readiness.ready(item)).isFalse();
        advance(59);
        assertThat(readiness.ready(item)).isFalse();
        advance(1);
        assertThat(readiness.ready(item)).isTrue();
    }

    /** A change in size or version starts the wait again. */
    @Test
    void aChangeStartsTheWaitAgain()
    {
        assertThat(readiness.ready(new ListedItem("a.csv", 10, 1000)))
                .isFalse();
        advance(50);
        assertThat(readiness.ready(new ListedItem("a.csv", 20, 1000)))
                .isFalse();
        advance(50);
        assertThat(readiness.ready(new ListedItem("a.csv", 20, 2000)))
                .isFalse();
        advance(60);
        assertThat(readiness.ready(new ListedItem("a.csv", 20, 2000)))
                .isTrue();
    }

    /**
     * An object written again with the same size and time, but a new tag,
     * starts the wait again.
     */
    @Test
    void aNewTagStartsTheWaitAgain()
    {
        assertThat(readiness.ready(new ListedItem("a.csv", 10, 1000, "e1")))
                .isFalse();
        advance(50);
        assertThat(readiness.ready(new ListedItem("a.csv", 10, 1000, "e2")))
                .isFalse();
        advance(59);
        assertThat(readiness.ready(new ListedItem("a.csv", 10, 1000, "e2")))
                .isFalse();
        advance(1);
        assertThat(readiness.ready(new ListedItem("a.csv", 10, 1000, "e2")))
                .isTrue();
    }

    /** Items missing from a listing are forgotten when it ends. */
    @Test
    void forgetsItemsThatLeaveTheListing()
    {
        readiness.ready(new ListedItem("a.csv", 1, 1));
        readiness.ready(new ListedItem("b.csv", 1, 1));
        readiness.listingDone();
        assertThat(readiness.waiting()).isEqualTo(2);

        advance(60);
        assertThat(readiness.ready(new ListedItem("a.csv", 1, 1))).isTrue();
        readiness.listingDone();
        assertThat(readiness.waiting()).isEqualTo(1);

        advance(60);
        assertThat(readiness.ready(new ListedItem("b.csv", 1, 1)))
                .as("b.csv was gone for a listing, so starts again")
                .isFalse();
    }

    /** A zero settle time makes every item ready at once. */
    @Test
    void zeroSettleIsReadyAtOnce()
    {
        Readiness immediate = new Readiness(Duration.ZERO, now::get);
        assertThat(immediate.ready(new ListedItem("a.csv", 1, 1))).isTrue();
    }
}
