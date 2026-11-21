/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the stage's approved operations in its control pods
 * (CONTROL-API-DESIGN.md §6, §8.1): those a second person approved, which
 * the stage's workers don't carry out. A pod takes an operation's lease,
 * renews it while the operation runs, and finishes the record; if the pod
 * stops, the lease runs out and another pod runs the operation again,
 * which each such operation is made to bear. One runs at a time per pod.
 *
 * <p>It looks for work every {@link #EVERY}, and at once when an
 * operation is approved. It also deletes the break-glass results fetched
 * or past their time.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ControlRunner implements AutoCloseable
{

    /** How often it looks for work. */
    static final Duration EVERY = Duration.ofSeconds(10);

    /** How long a lease lasts unless renewed. */
    static final Duration LEASE = Duration.ofMinutes(2);

    /** How often a lease is renewed while its operation runs. */
    static final Duration RENEW = Duration.ofSeconds(30);

    /** The most operations looked at in one look. */
    private static final int BATCH = 10;

    /** Logs a look that failed, by its class alone. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ControlRunner.class);

    /** Carries out an operation, given its lease. */
    @FunctionalInterface
    interface Work
    {

        /**
         * Carries it out and finishes its record.
         *
         * @param operation its record
         * @param holder    the lease's holder, this pod
         */
        void run(ControlStore.Operation operation, String holder);
    }

    /** The operation records. */
    private final ControlStore store;

    /** The break-glass results. */
    private final ControlResults results;

    /** The stage's scope. */
    private final String scope;

    /** Carries out an operation. */
    private final Work work;

    /** This pod, as a lease's holder: unique to this process. */
    private final String holder = "control-" + UUID.randomUUID();

    /** Looks for work and renews leases. */
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(runnable ->
            {
                Thread thread = new Thread(runnable, "sdh-control-runner");
                thread.setDaemon(true);
                return thread;
            });

    /** Runs one operation at a time. */
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(runnable ->
            {
                Thread thread = new Thread(runnable, "sdh-control-work");
                thread.setDaemon(true);
                return thread;
            });

    /** Whether an operation is running here. */
    private final AtomicBoolean busy = new AtomicBoolean();

    /**
     * Creates the runner; nothing runs until it is started.
     *
     * @param store   the operation records
     * @param results the break-glass results
     * @param scope   the stage's scope
     * @param work    carries out an operation
     */
    ControlRunner(ControlStore store, ControlResults results, String scope,
            Work work)
    {
        this.store = store;
        this.results = results;
        this.scope = scope;
        this.work = work;
    }

    /** Starts looking for work. */
    void start()
    {
        scheduler.scheduleWithFixedDelay(this::look, 0, EVERY.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    /** Looks for work now, as an operation was approved. */
    void nudge()
    {
        if (!scheduler.isShutdown())
        {
            scheduler.execute(this::look);
        }
    }

    /**
     * Looks for an operation to run, and runs the first whose lease it
     * takes.
     */
    private void look()
    {
        try
        {
            results.sweep();
            if (busy.get())
            {
                return;
            }
            for (ControlStore.Operation each : store.runnable(scope, BATCH))
            {
                if (store.take(each.id(), holder, LEASE))
                {
                    busy.set(true);
                    store.progress(each.id(), ControlStore.State.RUNNING,
                            each.progress());
                    ScheduledFuture<?> renewing =
                            scheduler.scheduleWithFixedDelay(() -> renew(
                                    each.id()), RENEW.toMillis(),
                                    RENEW.toMillis(), TimeUnit.MILLISECONDS);
                    worker.execute(() -> run(each, renewing));
                    return;
                }
            }
        }
        catch (RuntimeException failure)
        {
            // The database may be down; the next look tries again. The
            // class alone is logged: a message could quote a value.
            LOG.warn("control.runner_look_failed {}",
                    failure.getClass().getSimpleName());
        }
    }

    /**
     * Runs an operation it holds, then stops renewing its lease.
     *
     * @param operation the operation
     * @param renewing  the lease's renewal
     */
    private void run(ControlStore.Operation operation,
            ScheduledFuture<?> renewing)
    {
        try
        {
            work.run(operation, holder);
        }
        finally
        {
            renewing.cancel(false);
            try
            {
                store.release(operation.id(), holder);
            }
            catch (RuntimeException unreleased)
            {
                // The lease runs out on its own.
            }
            busy.set(false);
            nudge();
        }
    }

    /**
     * Renews a lease while its operation runs.
     *
     * @param id the operation
     */
    private void renew(UUID id)
    {
        try
        {
            store.renew(id, holder, LEASE);
        }
        catch (RuntimeException failure)
        {
            LOG.warn("control.runner_renew_failed {}",
                    failure.getClass().getSimpleName());
        }
    }

    /** Stops looking, and stops the running operation's thread. */
    @Override
    public void close()
    {
        scheduler.shutdownNow();
        worker.shutdownNow();
    }
}
