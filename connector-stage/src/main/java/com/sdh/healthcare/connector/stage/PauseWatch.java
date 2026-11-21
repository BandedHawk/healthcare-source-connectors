/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches a pod's {@link ControlState}, and tells what takes work whenever
 * it is paused or resumed (CONTROL-API-DESIGN.md §9). It reads the state
 * once when started, before the pod takes any work, and every
 * {@link #EVERY} after; a read that fails keeps the state last read.
 *
 * <p>Pausing stops what takes new work: the input route, a reprocess
 * run's queueing, corrections, a landing node's listing or pushes. Work
 * already taken finishes and commits, and leases are still renewed, so
 * nothing is taken over and nothing is dropped.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PauseWatch implements AutoCloseable
{

    /** How often the state is read. */
    public static final Duration EVERY = Duration.ofSeconds(5);

    /** Logs a read that failed, by its class alone. */
    private static final Logger LOG = LoggerFactory.getLogger(
            PauseWatch.class);

    /** Reads whether the pod's scope is paused. */
    @FunctionalInterface
    public interface Reader
    {

        /**
         * Reads it.
         *
         * @return the pause; empty if not paused
         * @throws RuntimeException if it can't be read
         */
        Optional<ControlState.Paused> paused();
    }

    /** Takes work, and stops taking it while paused. */
    @FunctionalInterface
    public interface Pausable
    {

        /**
         * Stops or starts taking new work. Told the same again after a
         * failure, so it must bear that.
         *
         * @param paused true to stop, false to start again
         * @throws Exception if it can't
         */
        void paused(boolean paused) throws Exception;
    }

    /** Reads the state. */
    private final Reader reader;

    /** What the scope is, for the log. */
    private final String scope;

    /** How often it reads. */
    private final Duration every;

    /** Told of each change, in order. */
    private final List<Pausable> told;

    /** Reads the state every interval; null until started. */
    private ScheduledExecutorService scheduler;

    /** Whether the scope was paused when last read. */
    private volatile boolean paused;

    /**
     * Creates a watch, not started.
     *
     * @param reader reads the state
     * @param scope  what the scope is, for the log
     * @param every  how often it reads
     * @param told   told of each change, in order
     */
    public PauseWatch(Reader reader, String scope, Duration every,
            List<Pausable> told)
    {
        this.reader = reader;
        this.scope = scope;
        this.every = every;
        this.told = List.copyOf(told);
    }

    /**
     * Watches a scope's state every {@link #EVERY}, creating its table if
     * it isn't there.
     *
     * @param state the scope's state
     * @param told  told of each change, in order
     * @return the watch, not started
     * @throws IllegalStateException if the table can't be created
     */
    public static PauseWatch of(ControlState state, List<Pausable> told)
    {
        state.createTableIfMissing();
        return new PauseWatch(state::paused, state.scope(), EVERY, told);
    }

    /**
     * Reads the state, tells each of a pause found, then reads it again
     * every interval. A first read that fails is taken as not paused, as
     * the pod could take no work then anyway.
     */
    public synchronized void start()
    {
        if (scheduler != null)
        {
            return;
        }
        look();
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable ->
        {
            Thread thread = new Thread(runnable, "sdh-pause-watch");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleWithFixedDelay(this::look, every.toMillis(),
                every.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Whether the scope was paused when last read.
     *
     * @return true if it was
     */
    public boolean paused()
    {
        return paused;
    }

    /** Reads the state, and tells each of a change. */
    private void look()
    {
        boolean now;
        try
        {
            now = reader.paused().isPresent();
        }
        catch (RuntimeException failure)
        {
            LOG.warn("stage.pause_check_failed error={}",
                    failure.getClass().getSimpleName());
            return;
        }
        if (now == paused)
        {
            return;
        }
        for (Pausable each : told)
        {
            try
            {
                each.paused(now);
            }
            catch (Exception failure)
            {
                // Each is told again at the next look, which each bears;
                // the log says it, by the failure's class alone.
                LOG.warn("stage.pause_apply_failed error={}",
                        failure.getClass().getSimpleName());
                return;
            }
        }
        LOG.info("stage.{} scope={}", now ? "paused" : "resumed", scope);
        Telemetry.get().audit(now ? "stage.paused" : "stage.resumed",
                Fields.of(Attr.CONTROL_SCOPE, scope));
        paused = now;
    }

    /** Stops reading. */
    @Override
    public synchronized void close()
    {
        if (scheduler != null)
        {
            scheduler.shutdownNow();
        }
    }
}
