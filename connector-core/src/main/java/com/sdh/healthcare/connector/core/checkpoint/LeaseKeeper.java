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
 * Holds the single-instance lease: acquires it, renews it in the background and fences itself.
 *
 * <p>Validity is tracked with the local monotonic clock. The holder treats the lease as valid only
 * until {@code (time the last successful renewal was sent) + ttl - safetyMargin}. Measuring from when the
 * request was sent, not when it returned, means a slow reply can only shorten that window. The margin
 * covers clock-rate drift between this host and the store. If that point passes without a successful
 * renewal, the lease counts as lost, even if the store would still say it is ours.
 *
 * <p>Self-fencing alone is not enough, because a process can pause (GC, VM suspend) between checking
 * and committing. That is why every table commit also checks the epoch ({@link FencedCommitter}).
 */
public final class LeaseKeeper implements LeaseToken, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LeaseKeeper.class);

    /**
     * @param ttl how long the store keeps the lease without a renewal; also the longest a successor waits
     *     after a crash
     * @param renewInterval time between renewals
     * @param safetyMargin subtracted from ttl locally to allow for clock drift
     */
    public record Settings(Duration ttl, Duration renewInterval, Duration safetyMargin) {
        public static final Settings DEFAULT = new Settings(Duration.ofSeconds(60), Duration.ofSeconds(15),
                Duration.ofSeconds(10));

        public Settings {
            // At least two renewals can fail in a row before the instance fences itself.
            if (renewInterval.multipliedBy(2).plus(safetyMargin).compareTo(ttl) >= 0) {
                throw new IllegalArgumentException("Require 2 * renewInterval + safetyMargin < ttl");
            }
        }

        Duration localValidity() {
            return ttl.minus(safetyMargin);
        }
    }

    private final LeaseStore store;
    private final LeaseGrant grant;
    private final Settings settings;
    private final LongSupplier nanoClock;
    private final Consumer<LeaseLostException> onLost;
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> renewal;

    private volatile long validUntilNanos;
    private volatile LeaseLostException lost;

    private LeaseKeeper(LeaseStore store, LeaseGrant grant, long validUntilNanos, Settings settings,
            LongSupplier nanoClock, Consumer<LeaseLostException> onLost, boolean scheduleRenewals) {
        this.store = store;
        this.grant = grant;
        this.validUntilNanos = validUntilNanos;
        this.settings = settings;
        this.nanoClock = nanoClock;
        this.onLost = onLost;
        if (scheduleRenewals) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                    .name("sdh-lease-renewal").daemon(true).factory());
            long every = settings.renewInterval().toMillis();
            this.renewal = scheduler.scheduleWithFixedDelay(this::renewOnce, every, every, TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.renewal = null;
        }
    }

    /** Acquires the lease and starts background renewal. */
    public static LeaseKeeper acquire(LeaseStore store, String leaseName, String owner, Settings settings,
            Consumer<LeaseLostException> onLost) throws LeaseHeldException {
        return acquire(store, leaseName, owner, settings, System::nanoTime, onLost, true);
    }

    static LeaseKeeper acquire(LeaseStore store, String leaseName, String owner, Settings settings,
            LongSupplier nanoClock, Consumer<LeaseLostException> onLost, boolean scheduleRenewals)
            throws LeaseHeldException {
        long sentAt = nanoClock.getAsLong();
        LeaseGrant grant = store.acquire(leaseName, owner, settings.ttl());
        Telemetry.get().audit("lease.acquired", Fields.of(Attr.EPOCH, grant.epoch()));
        return new LeaseKeeper(store, grant, sentAt + settings.localValidity().toNanos(), settings, nanoClock,
                onLost, scheduleRenewals);
    }

    /** A unique, PHI-free owner ID for this process: host, PID and a random suffix. */
    public static String defaultOwner() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown-host";
        }
        return host + "/" + ManagementFactory.getRuntimeMXBean().getPid() + "/" + UUID.randomUUID();
    }

    @Override
    public long epoch() {
        return grant.epoch();
    }

    public LeaseGrant grant() {
        return grant;
    }

    @Override
    public void checkHeld() {
        LeaseLostException l = lost;
        if (l != null) {
            throw l;
        }
        if (nanoClock.getAsLong() - validUntilNanos >= 0) {
            throw markLost(LeaseLostException.Reason.EXPIRED);
        }
    }

    public boolean isHeld() {
        try {
            checkHeld();
            return true;
        } catch (LeaseLostException e) {
            return false;
        }
    }

    /** One renewal attempt. Called by the scheduler; package-private for tests. */
    void renewOnce() {
        if (lost != null) {
            return;
        }
        long sentAt = nanoClock.getAsLong();
        try {
            if (store.renew(grant, settings.ttl())) {
                validUntilNanos = sentAt + settings.localValidity().toNanos();
            } else {
                markLost(LeaseLostException.Reason.TAKEN_OVER);
            }
        } catch (RuntimeException e) {
            // Store unreachable: keep trying on the next tick until the local window runs out.
            LOG.warn("lease.renew_failed epoch={} error={}", grant.epoch(), e.getClass().getSimpleName());
            if (nanoClock.getAsLong() - validUntilNanos >= 0) {
                markLost(LeaseLostException.Reason.EXPIRED);
            }
        }
    }

    private synchronized LeaseLostException markLost(LeaseLostException.Reason reason) {
        if (lost == null) {
            lost = new LeaseLostException(reason);
            Telemetry.get().auditError("lease.lost", Fields.of(Attr.EPOCH, grant.epoch()).with(Attr.REASON, reason));
            if (renewal != null) {
                renewal.cancel(false);
            }
            onLost.accept(lost);
        }
        return lost;
    }

    /** Stops renewing and releases the lease so a successor need not wait for expiry. */
    @Override
    public void close() {
        if (renewal != null) {
            renewal.cancel(false);
            scheduler.shutdownNow();
        }
        if (lost == null) {
            try {
                store.release(grant);
                Telemetry.get().audit("lease.released", Fields.of(Attr.EPOCH, grant.epoch()));
            } catch (RuntimeException e) {
                LOG.warn("lease.release_failed epoch={} error={}", grant.epoch(), e.getClass().getSimpleName());
            }
        }
    }
}
