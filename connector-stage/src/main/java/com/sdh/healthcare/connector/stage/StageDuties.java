/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.core.checkpoint.LeaseHeldException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseKeeper;
import com.sdh.healthcare.connector.core.checkpoint.LeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.LeasedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.TableCommitter;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stage's work that one pod does for all (STAGES.md §8, §9): reconciling,
 * that is finding work whose messages were lost, and table maintenance.
 * Every pod waits for the duties lease, {@code <stage>/<connector>/duties},
 * as a standby; the one that holds it does the duties, and the next takes
 * over within one TTL if it stops. Losing the lease stops the duties on
 * that pod, which goes back to waiting; the stage's own work carries on.
 * <p>
 * Maintenance commits through a {@link LeasedCommitter} on the duties
 * lease. It runs while other pods append to the stage's shared table:
 * Iceberg checks each append and rewrite against the other, and orphan
 * cleanup leaves files younger than its minimum age, which covers any
 * append in flight.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StageDuties implements AutoCloseable
{

    /** How long to wait before taking the lease again after an error. */
    static final Duration ERROR_PAUSE = Duration.ofSeconds(30);

    /** How long closing waits for the duties to stop. */
    static final Duration STOP_WAIT = Duration.ofSeconds(60);

    /** Logs taking and losing the duties, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(StageDuties.class);

    /** Finds work whose messages were lost, on the holder of the lease. */
    @FunctionalInterface
    public interface Reconciler
    {

        /**
         * Looks once.
         *
         * @throws InterruptedException if interrupted: the duties are
         *                              closing
         * @throws Exception            if it fails; it runs again at the
         *                              next interval
         */
        void reconcile() throws Exception;
    }

    /** Makes the maintenance scheduler for a holder of the lease. */
    @FunctionalInterface
    public interface Maintenance
    {

        /**
         * Makes the scheduler, not yet started.
         *
         * @param committer commits under the duties lease
         * @param fatal     told of a fatal error; a lost lease ends the
         *                  duties instead of the process
         * @return the scheduler
         */
        MaintenanceScheduler create(TableCommitter committer, FatalExit fatal);
    }

    /** Where the lease is kept. */
    private final LeaseStore leases;

    /** The stage's name, which starts the lease's and the log events'. */
    private final String stage;

    /** The lease's name. */
    private final String leaseName;

    /** The lease timings. */
    private final LeaseKeeper.Settings leaseSettings;

    /** Finds lost work; null if that is off. */
    private final Reconciler reconciler;

    /** How often the reconciler runs. */
    private final Duration reconcileInterval;

    /** Makes the maintenance scheduler. */
    private final Maintenance maintenance;

    /** Stops the process on a fatal error. */
    private final FatalExit fatal;

    /** False once closing has begun. */
    private volatile boolean running = true;

    /** The duties' thread, once started. */
    private Thread thread;

    /**
     * Creates the duties; {@link #start} starts them.
     *
     * @param leases            where the lease is kept
     * @param stage             the stage's name, such as {@code split}
     * @param connectorId       the stage's ID; with the name, it names the
     *                          lease
     * @param leaseSettings     the lease timings
     * @param reconciler        finds lost work; null if that is off
     * @param reconcileInterval how often the reconciler runs
     * @param maintenance       makes the maintenance scheduler
     * @param fatal             stops the process on a fatal error
     */
    public StageDuties(LeaseStore leases, String stage, String connectorId,
            LeaseKeeper.Settings leaseSettings, Reconciler reconciler,
            Duration reconcileInterval, Maintenance maintenance,
            FatalExit fatal)
    {
        this.leases = leases;
        this.stage = stage;
        this.leaseName = stage + "/" + connectorId + "/duties";
        this.leaseSettings = leaseSettings;
        this.reconciler = reconciler;
        this.reconcileInterval = reconcileInterval;
        this.maintenance = maintenance;
        this.fatal = fatal;
    }

    /** Starts waiting for the lease, on a thread of its own. */
    public synchronized void start()
    {
        thread = Thread.ofPlatform().name("sdh-" + stage + "-duties")
                .start(this::run);
    }

    /** The thread's loop: take the lease, do the duties, until closed. */
    private void run()
    {
        while (running)
        {
            try
            {
                holdAndRun();
            }
            catch (InterruptedException interrupted)
            {
                return;
            }
            catch (RuntimeException failure)
            {
                LOG.warn("stage.duties_failed stage={} error={} retry_in_s={}",
                        stage, failure.getClass().getSimpleName(),
                        ERROR_PAUSE.toSeconds());
                if (!pause())
                {
                    return;
                }
            }
        }
    }

    /**
     * Waits for the lease, then does the duties until it is lost or the
     * duties close.
     *
     * @throws InterruptedException if the duties are closing
     */
    private void holdAndRun() throws InterruptedException
    {
        CountDownLatch lost = new CountDownLatch(1);
        LeaseKeeper lease;
        try
        {
            lease = LeaseKeeper.acquireAsStandby(leases, leaseName,
                    LeaseKeeper.defaultOwner(), leaseSettings, failure ->
                    {
                        LOG.warn("stage.duties_lost stage={} reason={}",
                                stage, failure.reason());
                        lost.countDown();
                    });
        }
        catch (LeaseHeldException held)
        {
            // Only an interrupted wait gives up.
            throw new InterruptedException();
        }
        try (lease)
        {
            LOG.info("stage.duties_taken stage={}", stage);
            MaintenanceScheduler scheduler = maintenance.create(
                    new LeasedCommitter(lease), reason ->
                    {
                        if (reason.startsWith("LEASE_LOST"))
                        {
                            lost.countDown();
                        }
                        else
                        {
                            fatal.exit(reason);
                        }
                    });
            scheduler.start();
            try (scheduler)
            {
                while (running && lease.isHeld())
                {
                    reconcile();
                    if (lost.await(reconcileInterval.toMillis(),
                            TimeUnit.MILLISECONDS))
                    {
                        return;
                    }
                }
            }
        }
    }

    /**
     * Runs the reconciler, if it is on. If it fails, it runs again at the
     * next interval; only a fatal error stops the process.
     *
     * @throws InterruptedException if interrupted
     */
    private void reconcile() throws InterruptedException
    {
        if (reconciler == null)
        {
            return;
        }
        try
        {
            reconciler.reconcile();
        }
        catch (InterruptedException interrupted)
        {
            throw interrupted;
        }
        catch (Exception | Error failure)
        {
            Optional<String> reason = StageFailures.fatalReason(failure);
            if (reason.isPresent())
            {
                LOG.error("stage.fatal stage={} reason={}", stage,
                        reason.get());
                fatal.exit(reason.get());
                throw new InterruptedException();
            }
            LOG.warn("stage.reconcile_failed stage={} error={} next_in_s={}",
                    stage, failure.getClass().getSimpleName(),
                    reconcileInterval.toSeconds());
        }
    }

    /**
     * Pauses after an error.
     *
     * @return false if interrupted: the duties are closing
     */
    private boolean pause()
    {
        try
        {
            Thread.sleep(ERROR_PAUSE.toMillis());
            return true;
        }
        catch (InterruptedException interrupted)
        {
            return false;
        }
    }

    /**
     * Stops the duties and gives up the lease. A reconciler run is
     * interrupted; what it was doing is taken up again later.
     */
    @Override
    public void close()
    {
        running = false;
        Thread duties;
        synchronized (this)
        {
            duties = thread;
        }
        if (duties == null)
        {
            return;
        }
        duties.interrupt();
        try
        {
            duties.join(STOP_WAIT.toMillis());
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
        LOG.info("stage.duties_stopped stage={}", stage);
    }
}
