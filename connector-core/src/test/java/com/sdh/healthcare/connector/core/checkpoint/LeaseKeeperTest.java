/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LeaseKeeper}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class LeaseKeeperTest
{

    /** A 60s TTL, renewed every 15s, with a 10s margin. */
    static final LeaseKeeper.Settings SETTINGS = new LeaseKeeper.Settings(
            Duration.ofSeconds(60), Duration.ofSeconds(15),
            Duration.ofSeconds(10));

    /** The test's monotonic clock, in nanoseconds. */
    final AtomicLong now = new AtomicLong(1_000_000_000L);

    /** The losses reported, in order. */
    final List<LeaseLostException> lost = new ArrayList<>();

    /** The sleeps asked for, in order. */
    final List<Duration> sleeps = new ArrayList<>();

    /** A store whose renew behaviour the test controls. */
    static class FakeStore implements LeaseStore
    {

        /** What renew does: {@code ok}, {@code fail} or {@code gone}. */
        volatile String renewMode = "ok";

        /** Whether the lease was released. */
        volatile boolean released;

        /**
         * {@inheritDoc}
         *
         * @return a grant at epoch 7
         */
        @Override
        public LeaseGrant acquire(String leaseName, String owner,
                Duration ttl) throws LeaseHeldException
        {
            return new LeaseGrant(leaseName, owner, 7,
                    Instant.now().plus(ttl));
        }

        /**
         * {@inheritDoc}
         *
         * @throws IllegalStateException in {@code fail} mode
         */
        @Override
        public boolean renew(LeaseGrant grant, Duration ttl)
        {
            return switch (renewMode)
            {
                case "fail" ->
                    throw new IllegalStateException("store down");
                case "gone" ->
                    false;
                default ->
                    true;
            };
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void release(LeaseGrant grant)
        {
            released = true;
        }
    }

    /**
     * A store held by another instance until the test's clock reaches
     * {@code freeAt}.
     */
    final class HeldStore extends FakeStore
    {

        /** When the lease becomes free, in the test's nanoseconds. */
        volatile long freeAt;

        /** How many times acquire was called. */
        int attempts;

        /**
         * {@inheritDoc}
         *
         * @throws LeaseHeldException until {@code freeAt}
         */
        @Override
        public LeaseGrant acquire(String leaseName, String owner,
                Duration ttl) throws LeaseHeldException
        {
            attempts++;
            if (now.get() < freeAt)
            {
                throw new LeaseHeldException("other/1/x", Instant.now());
            }
            return super.acquire(leaseName, owner, ttl);
        }
    }

    /**
     * Takes the lease without background renewal.
     *
     * @param store the store
     * @return the keeper
     * @throws Exception if it can't be taken
     */
    LeaseKeeper keeper(FakeStore store) throws Exception
    {
        return LeaseKeeper.acquire(store, "l", "me", SETTINGS, now::get,
                this::sleep, lost::add, false);
    }

    /**
     * Moves the test's clock on.
     *
     * @param duration how far
     */
    void advance(Duration duration)
    {
        now.addAndGet(duration.toNanos());
    }

    /**
     * Records a sleep and moves the clock on by it.
     *
     * @param duration how long
     */
    void sleep(Duration duration)
    {
        sleeps.add(duration);
        advance(duration);
    }

    /**
     * A crashed holder's lease is waited for and taken when it lapses.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void waitsForALeaseThatLapses() throws Exception
    {
        // A crashed holder: its lease lapses 40 s from now, well within the
        // 70 s startup wait (ttl + margin).
        HeldStore store = new HeldStore();
        store.freeAt = now.get() + Duration.ofSeconds(40).toNanos();
        long start = now.get();
        LeaseKeeper keeper = LeaseKeeper.acquire(store, "l", "me", SETTINGS,
                now::get, this::sleep, lost::add, false);
        assertThat(keeper.epoch()).isEqualTo(7);
        assertThat(Duration.ofNanos(now.get() - start))
                .isEqualTo(Duration.ofSeconds(40));
        assertThat(sleeps).allSatisfy(slept ->
                assertThat(slept).isEqualTo(LeaseKeeper.STARTUP_POLL));
        assertThat(store.attempts).isEqualTo(21);
        // Validity counts from the attempt that succeeded, not from the
        // start of the wait.
        advance(Duration.ofSeconds(49));
        keeper.checkHeld();
    }

    /** A live holder's lease is refused after ttl + margin. */
    @Test
    void refusesWhenTheHolderIsAliveAfterTheWait()
    {
        // A live holder keeps renewing, so the lease never lapses: refused
        // after ttl + margin.
        HeldStore store = new HeldStore();
        store.freeAt = Long.MAX_VALUE;
        long start = now.get();
        assertThatThrownBy(() -> LeaseKeeper.acquire(store, "l", "me",
                SETTINGS, now::get, this::sleep, lost::add, false))
                .isInstanceOf(LeaseHeldException.class);
        assertThat(Duration.ofNanos(now.get() - start))
                .isEqualTo(SETTINGS.startupWait())
                .isEqualTo(Duration.ofSeconds(70));
        assertThat(sleeps.getLast())
                .isLessThanOrEqualTo(LeaseKeeper.STARTUP_POLL);
    }

    /**
     * A standby waits for as long as the active node runs.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aStandbyWaitsAsLongAsItTakes() throws Exception
    {
        // The active node runs for an hour; the standby keeps waiting,
        // then takes over once it lapses.
        HeldStore store = new HeldStore();
        store.freeAt = now.get() + Duration.ofHours(1).toNanos();
        long start = now.get();
        LeaseKeeper keeper = LeaseKeeper.acquire(store, "l", "me", SETTINGS,
                null, now::get, this::sleep, lost::add, false);
        assertThat(keeper.epoch()).isEqualTo(7);
        assertThat(Duration.ofNanos(now.get() - start))
                .isEqualTo(Duration.ofHours(1));
        assertThat(sleeps).allSatisfy(slept ->
                assertThat(slept).isEqualTo(LeaseKeeper.STARTUP_POLL));
    }

    /** An interrupted standby gives up and keeps the interrupt. */
    @Test
    void anInterruptedStandbyGivesUp()
    {
        HeldStore store = new HeldStore();
        store.freeAt = Long.MAX_VALUE;
        assertThatThrownBy(() -> LeaseKeeper.acquire(store, "l", "me",
                SETTINGS, null, now::get, duration ->
        {
            throw new InterruptedException();
        }, lost::add, false)).isInstanceOf(LeaseHeldException.class);
        assertThat(Thread.interrupted()).as("interrupt flag restored")
                .isTrue();
    }

    /** An interrupted startup wait gives up and keeps the interrupt. */
    @Test
    void anInterruptedWaitGivesUp()
    {
        HeldStore store = new HeldStore();
        store.freeAt = Long.MAX_VALUE;
        assertThatThrownBy(() -> LeaseKeeper.acquire(store, "l", "me",
                SETTINGS, now::get, duration ->
        {
            throw new InterruptedException();
        }, lost::add, false)).isInstanceOf(LeaseHeldException.class);
        assertThat(Thread.interrupted()).as("interrupt flag restored")
                .isTrue();
        assertThat(store.attempts).isEqualTo(1);
    }

    /**
     * Renewals keep the lease, and closing releases it.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void successfulRenewalsKeepTheLease() throws Exception
    {
        FakeStore store = new FakeStore();
        LeaseKeeper keeper = keeper(store);
        for (int renewal = 0; renewal < 20; renewal++)
        {
            advance(Duration.ofSeconds(15));
            keeper.renewOnce();
        }
        keeper.checkHeld();
        assertThat(keeper.epoch()).isEqualTo(7);
        keeper.close();
        assertThat(store.released).isTrue();
    }

    /**
     * With the store unreachable, the lease counts as lost once ttl less
     * the margin has passed, and isn't released.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void selfFencesWhenStoreIsUnreachableForTtlMinusMargin()
            throws Exception
    {
        FakeStore store = new FakeStore();
        LeaseKeeper keeper = keeper(store);
        store.renewMode = "fail";
        advance(Duration.ofSeconds(15));
        keeper.renewOnce();
        advance(Duration.ofSeconds(15));
        keeper.renewOnce();
        advance(Duration.ofSeconds(15));
        keeper.renewOnce();
        // 45 s since the last good renewal (acquire): still inside 60 - 10.
        keeper.checkHeld();
        advance(Duration.ofSeconds(5));
        assertThatThrownBy(keeper::checkHeld)
                .isInstanceOf(LeaseLostException.class)
                .extracting(error -> ((LeaseLostException) error).reason())
                .isEqualTo(LeaseLostException.Reason.EXPIRED);
        assertThat(lost).hasSize(1);
        keeper.close();
        assertThat(store.released).as("a lost lease is not released")
                .isFalse();
    }

    /**
     * A refused renewal means the lease was taken over.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void takeOverIsDetectedOnNextRenewal() throws Exception
    {
        FakeStore store = new FakeStore();
        LeaseKeeper keeper = keeper(store);
        store.renewMode = "gone";
        advance(Duration.ofSeconds(15));
        keeper.renewOnce();
        assertThat(keeper.isHeld()).isFalse();
        assertThat(lost).singleElement()
                .extracting(LeaseLostException::reason)
                .isEqualTo(LeaseLostException.Reason.TAKEN_OVER);
    }

    /** Settings must allow two renewals to fail in a row. */
    @Test
    void settingsMustAllowTwoMissedRenewals()
    {
        assertThatThrownBy(() -> new LeaseKeeper.Settings(
                Duration.ofSeconds(30), Duration.ofSeconds(10),
                Duration.ofSeconds(10)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
