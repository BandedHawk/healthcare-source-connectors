/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the single-instance lease: acquires it, renews it in the
 * background and fences itself.
 * <p>
 * Validity is tracked with the local monotonic clock. The holder treats
 * the lease as valid only until {@code (time the last successful renewal
 * was sent) + ttl - safetyMargin}. Measuring from when the request was
 * sent, not when it returned, means a slow reply can only shorten that
 * window. The margin covers clock-rate drift between this host and the
 * store. If that point passes without a successful renewal, the lease
 * counts as lost, even if the store would still say it is ours.
 * <p>
 * Self-fencing alone is not enough, because a process can pause (GC, VM
 * suspend) between checking and committing. That is why every table commit
 * also checks the epoch ({@link FencedCommitter}).
 * <p>
 * At startup a held lease is waited for, up to
 * {@link Settings#startupWait()}. A holder that crashed stops renewing, so
 * its lease lapses within one TTL and this instance takes over without
 * exiting. Under an orchestrator that restarts failed processes with
 * growing delays (Kubernetes), exiting at once would add those delays to
 * the takeover. A lease still held after the wait has a live holder:
 * {@link LeaseHeldException}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LeaseKeeper implements LeaseToken, AutoCloseable
{

    /** Logs waits and renewal failures, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(LeaseKeeper.class);

    /**
     * How long to sleep between attempts while waiting for a held lease at
     * startup.
     */
    static final Duration STARTUP_POLL = Duration.ofSeconds(2);

    /**
     * The lease timings.
     *
     * @param ttl           how long the store keeps the lease without a
     *                      renewal; also the longest a successor waits
     *                      after a crash
     * @param renewInterval time between renewals
     * @param safetyMargin  subtracted from ttl locally to allow for clock
     *                      drift
     */
    public record Settings(Duration ttl, Duration renewInterval,
            Duration safetyMargin)
    {

        /** A 60s TTL, renewed every 15s, with a 10s margin. */
        public static final Settings DEFAULT = new Settings(
                Duration.ofSeconds(60), Duration.ofSeconds(15),
                Duration.ofSeconds(10));

        /**
         * Checks that the timings fit together.
         *
         * @param ttl           how long the store keeps the lease
         * @param renewInterval time between renewals
         * @param safetyMargin  allowance for clock drift
         * @throws IllegalArgumentException unless
         *                                  {@code 2 * renewInterval
         *                                  + safetyMargin < ttl}
         */
        public Settings
        {
            // At least two renewals can fail in a row before the instance
            // fences itself.
            if (renewInterval.multipliedBy(2).plus(safetyMargin)
                    .compareTo(ttl) >= 0)
            {
                throw new IllegalArgumentException(
                        "Require 2 * renewInterval + safetyMargin < ttl");
            }
        }

        /**
         * How long a renewal keeps the lease valid locally.
         *
         * @return the ttl less the margin
         */
        Duration localValidity()
        {
            return ttl.minus(safetyMargin);
        }

        /**
         * How long startup waits for a held lease: a crashed holder's lease
         * lapses within the TTL, and the margin allows for clock-rate drift
         * between the holder and the store.
         *
         * @return the ttl plus the margin
         */
        public Duration startupWait()
        {
            return ttl.plus(safetyMargin);
        }
    }

    /** Sleeps between startup attempts; replaced in tests. */
    interface Sleeper
    {

        /**
         * Sleeps.
         *
         * @param duration how long
         * @throws InterruptedException if interrupted
         */
        void sleep(Duration duration) throws InterruptedException;
    }

    /** Where the lease is kept. */
    private final LeaseStore store;

    /** The lease this instance holds. */
    private final LeaseGrant grant;

    /** The lease timings. */
    private final Settings settings;

    /** The monotonic clock, in nanoseconds. */
    private final LongSupplier nanoClock;

    /** Told once, when the lease is lost. */
    private final Consumer<LeaseLostException> onLost;

    /** Runs renewals; null when they aren't scheduled. */
    private final ScheduledExecutorService scheduler;

    /** The scheduled renewals; null when they aren't scheduled. */
    private final ScheduledFuture<?> renewal;

    /** The monotonic time at which the lease stops being valid locally. */
    private volatile long validUntilNanos;

    /** Why the lease was lost; null while it is held. */
    private volatile LeaseLostException lost;

    /**
     * Creates a keeper for a lease just taken.
     *
     * @param store            where the lease is kept
     * @param grant            the lease
     * @param validUntilNanos  when it stops being valid locally
     * @param settings         the lease timings
     * @param nanoClock        the monotonic clock
     * @param onLost           told when the lease is lost
     * @param scheduleRenewals whether to renew in the background
     */
    private LeaseKeeper(LeaseStore store, LeaseGrant grant,
            long validUntilNanos, Settings settings, LongSupplier nanoClock,
            Consumer<LeaseLostException> onLost, boolean scheduleRenewals)
    {
        this.store = store;
        this.grant = grant;
        this.validUntilNanos = validUntilNanos;
        this.settings = settings;
        this.nanoClock = nanoClock;
        this.onLost = onLost;
        if (scheduleRenewals)
        {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().name("sdh-lease-renewal")
                            .daemon(true).factory());
            long every = settings.renewInterval().toMillis();
            this.renewal = scheduler.scheduleWithFixedDelay(this::renewOnce,
                    every, every, TimeUnit.MILLISECONDS);
        }
        else
        {
            this.scheduler = null;
            this.renewal = null;
        }
    }

    /**
     * Acquires the lease, waiting up to {@link Settings#startupWait()} if
     * another instance holds it, and starts background renewal.
     *
     * @param store     where the lease is kept
     * @param leaseName the lease's name
     * @param owner     who is taking it
     * @param settings  the lease timings
     * @param onLost    told when the lease is lost
     * @return the keeper
     * @throws LeaseHeldException if the lease is still held after the wait:
     *                            another instance is alive
     */
    public static LeaseKeeper acquire(LeaseStore store, String leaseName,
            String owner, Settings settings,
            Consumer<LeaseLostException> onLost) throws LeaseHeldException
    {
        return acquire(store, leaseName, owner, settings, System::nanoTime,
                duration -> Thread.sleep(duration), onLost, true);
    }

    /**
     * Acquires the lease as a standby (design §5.3.5): waits for it for as
     * long as it takes, then starts background renewal. A standby pod for
     * a landing intake calls this, so it takes over within one TTL of the
     * active node stopping, without exiting and restarting in between.
     *
     * @param store     where the lease is kept
     * @param leaseName the lease's name
     * @param owner     who is taking it
     * @param settings  the lease timings
     * @param onLost    told when the lease is lost
     * @return the keeper
     * @throws LeaseHeldException if the wait is interrupted
     */
    public static LeaseKeeper acquireAsStandby(LeaseStore store,
            String leaseName, String owner, Settings settings,
            Consumer<LeaseLostException> onLost) throws LeaseHeldException
    {
        return acquire(store, leaseName, owner, settings, null,
                System::nanoTime, duration -> Thread.sleep(duration), onLost,
                true);
    }

    /**
     * Acquires the lease, waiting up to {@link Settings#startupWait()}.
     *
     * @param store            where the lease is kept
     * @param leaseName        the lease's name
     * @param owner            who is taking it
     * @param settings         the lease timings
     * @param nanoClock        the monotonic clock
     * @param sleeper          sleeps between attempts
     * @param onLost           told when the lease is lost
     * @param scheduleRenewals whether to renew in the background
     * @return the keeper
     * @throws LeaseHeldException if the lease is still held after the wait
     */
    static LeaseKeeper acquire(LeaseStore store, String leaseName,
            String owner, Settings settings, LongSupplier nanoClock,
            Sleeper sleeper, Consumer<LeaseLostException> onLost,
            boolean scheduleRenewals) throws LeaseHeldException
    {
        return acquire(store, leaseName, owner, settings,
                settings.startupWait(), nanoClock, sleeper, onLost,
                scheduleRenewals);
    }

    /**
     * Acquires the lease, waiting for it if it is held.
     *
     * @param store            where the lease is kept
     * @param leaseName        the lease's name
     * @param owner            who is taking it
     * @param settings         the lease timings
     * @param maxWait          how long to wait for a held lease; null waits
     *                         for as long as it takes (standby)
     * @param nanoClock        the monotonic clock
     * @param sleeper          sleeps between attempts
     * @param onLost           told when the lease is lost
     * @param scheduleRenewals whether to renew in the background
     * @return the keeper
     * @throws LeaseHeldException if the lease is still held after the wait,
     *                            or the wait is interrupted
     */
    static LeaseKeeper acquire(LeaseStore store, String leaseName,
            String owner, Settings settings, Duration maxWait,
            LongSupplier nanoClock, Sleeper sleeper,
            Consumer<LeaseLostException> onLost, boolean scheduleRenewals)
            throws LeaseHeldException
    {
        long waitUntil = maxWait == null
                ? 0 : nanoClock.getAsLong() + maxWait.toNanos();
        boolean waiting = false;
        while (true)
        {
            long sentAt = nanoClock.getAsLong();
            try
            {
                LeaseGrant grant = store.acquire(leaseName, owner,
                        settings.ttl());
                Telemetry.get().audit("lease.acquired",
                        Fields.of(Attr.EPOCH, grant.epoch()));
                return new LeaseKeeper(store, grant,
                        sentAt + settings.localValidity().toNanos(),
                        settings, nanoClock, onLost, scheduleRenewals);
            }
            catch (LeaseHeldException held)
            {
                long left = maxWait == null
                        ? Long.MAX_VALUE : waitUntil - nanoClock.getAsLong();
                if (left <= 0)
                {
                    throw held;
                }
                if (!waiting)
                {
                    // The holder ID is host, PID and a random suffix: no
                    // PHI.
                    if (maxWait == null)
                    {
                        LOG.info("lease.standby holder={}", held.holder());
                    }
                    else
                    {
                        LOG.info("lease.waiting holder={} up_to_s={}",
                                held.holder(), maxWait.toSeconds());
                    }
                    waiting = true;
                }
                try
                {
                    sleeper.sleep(Duration.ofNanos(
                            Math.min(left, STARTUP_POLL.toNanos())));
                }
                catch (InterruptedException interrupted)
                {
                    Thread.currentThread().interrupt();
                    throw held;
                }
            }
        }
    }

    /**
     * A unique, PHI-free owner ID for this process: host, PID and a random
     * suffix.
     *
     * @return the owner ID
     */
    public static String defaultOwner()
    {
        String host;
        try
        {
            host = InetAddress.getLocalHost().getHostName();
        }
        catch (Exception unknown)
        {
            host = "unknown-host";
        }
        return host + "/" + ManagementFactory.getRuntimeMXBean().getPid()
                + "/" + UUID.randomUUID();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long epoch()
    {
        return grant.epoch();
    }

    /**
     * The lease this instance holds.
     *
     * @return the grant
     */
    public LeaseGrant grant()
    {
        return grant;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkHeld()
    {
        LeaseLostException reason = lost;
        if (reason != null)
        {
            throw reason;
        }
        if (nanoClock.getAsLong() - validUntilNanos >= 0)
        {
            throw markLost(LeaseLostException.Reason.EXPIRED);
        }
    }

    /**
     * Tells whether the lease is still held.
     *
     * @return false once it is lost or has run out locally
     */
    public boolean isHeld()
    {
        try
        {
            checkHeld();
            return true;
        }
        catch (LeaseLostException notHeld)
        {
            return false;
        }
    }

    /**
     * One renewal attempt. Called by the scheduler; package-private for
     * tests.
     */
    void renewOnce()
    {
        if (lost != null)
        {
            return;
        }
        long sentAt = nanoClock.getAsLong();
        try
        {
            if (store.renew(grant, settings.ttl()))
            {
                validUntilNanos = sentAt + settings.localValidity().toNanos();
            }
            else
            {
                markLost(LeaseLostException.Reason.TAKEN_OVER);
            }
        }
        catch (RuntimeException failure)
        {
            // Store unreachable: keep trying on the next tick until the
            // local window runs out.
            LOG.warn("lease.renew_failed epoch={} error={}", grant.epoch(),
                    failure.getClass().getSimpleName());
            if (nanoClock.getAsLong() - validUntilNanos >= 0)
            {
                markLost(LeaseLostException.Reason.EXPIRED);
            }
        }
    }

    /**
     * Records the lease as lost, the first time only: stops renewals and
     * tells the listener.
     *
     * @param reason why it was lost
     * @return the exception that records the loss; the first one, if it
     *         was already lost
     */
    private synchronized LeaseLostException markLost(
            LeaseLostException.Reason reason)
    {
        if (lost == null)
        {
            lost = new LeaseLostException(reason);
            Telemetry.get().auditError("lease.lost",
                    Fields.of(Attr.EPOCH, grant.epoch())
                            .with(Attr.REASON, reason));
            if (renewal != null)
            {
                renewal.cancel(false);
            }
            onLost.accept(lost);
        }
        return lost;
    }

    /**
     * Stops renewing and releases the lease so a successor need not wait
     * for expiry.
     */
    @Override
    public void close()
    {
        if (renewal != null)
        {
            renewal.cancel(false);
            scheduler.shutdownNow();
        }
        if (lost == null)
        {
            try
            {
                store.release(grant);
                Telemetry.get().audit("lease.released",
                        Fields.of(Attr.EPOCH, grant.epoch()));
            }
            catch (RuntimeException failure)
            {
                LOG.warn("lease.release_failed epoch={} error={}",
                        grant.epoch(), failure.getClass().getSimpleName());
            }
        }
    }
}
