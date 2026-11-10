/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.split;

import com.sdh.healthcare.connector.core.checkpoint.HoldReleases;
import com.sdh.healthcare.connector.core.checkpoint.LeaseHeldException;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies operators' corrections of the split stage's failures (STAGES.md
 * §15.6d), on every pod, each interval: each pending correction under its
 * item's lease, then sends again the end of any item whose correction was
 * applied by a pod that stopped before sending it. It also applies
 * operators' requests to end the hold on an item held across versions
 * (§15.6e), which are applied as corrections are, by a pod under the
 * item's lease. Where the stage reprocesses, it queues and sends the
 * objects of operators' reprocess runs too (§15.8.4).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SplitCorrections implements AutoCloseable
{

    /** The corrections' log. */
    private static final Logger LOG =
            LoggerFactory.getLogger(SplitCorrections.class);

    /** The most corrections taken in one interval. */
    static final int LIMIT = 10;

    /** The pending corrections. */
    private final CorrectionStore store;

    /** Operators' requests to release held items; null for none. */
    private final HoldReleases releases;

    /** Applies them under the items' leases. */
    private final ItemSplitter splitter;

    /** The stage's reprocess; null if it doesn't reprocess. */
    private final SplitReprocess reprocess;

    /** How often to look. */
    private final Duration interval;

    /** How long a correction's item end may go unsent before it is sent. */
    private final Duration unsentAfter;

    /** Runs the loop; null until started. */
    private ScheduledExecutorService executor;

    /**
     * Creates the loop, not yet started.
     *
     * @param store       the correction store
     * @param splitter    applies corrections under the items' leases
     * @param interval    how often to look
     * @param unsentAfter how long an item's end may go unsent after its
     *                    correction, before it is sent again
     */
    public SplitCorrections(CorrectionStore store, ItemSplitter splitter,
            Duration interval, Duration unsentAfter)
    {
        this(store, null, splitter, interval, unsentAfter);
    }

    /**
     * Creates the loop, applying requests to release held items too, not
     * yet started.
     *
     * @param store       the correction store
     * @param releases    the requests to release held items
     * @param splitter    applies them under the items' leases
     * @param interval    how often to look
     * @param unsentAfter how long an item's end may go unsent after its
     *                    correction, before it is sent again
     */
    public SplitCorrections(CorrectionStore store, HoldReleases releases,
            ItemSplitter splitter, Duration interval, Duration unsentAfter)
    {
        this(store, releases, splitter, null, interval, unsentAfter);
    }

    /**
     * Creates the loop, applying requests to release held items and
     * queueing reprocess runs too, not yet started.
     *
     * @param store       the correction store
     * @param releases    the requests to release held items
     * @param splitter    applies them under the items' leases
     * @param reprocess   the stage's reprocess; null if it doesn't
     *                    reprocess
     * @param interval    how often to look
     * @param unsentAfter how long an item's end may go unsent after its
     *                    correction, before it is sent again
     */
    public SplitCorrections(CorrectionStore store, HoldReleases releases,
            ItemSplitter splitter, SplitReprocess reprocess,
            Duration interval, Duration unsentAfter)
    {
        this.reprocess = reprocess;
        this.store = store;
        this.releases = releases;
        this.splitter = splitter;
        this.interval = interval;
        this.unsentAfter = unsentAfter;
    }

    /** Starts looking, each interval, on a thread of its own. */
    public synchronized void start()
    {
        executor = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("sdh-split-corrections").daemon()
                        .factory());
        executor.scheduleWithFixedDelay(this::runOnce, interval.toMillis(),
                interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** One interval's work; a failure waits for the next. */
    private void runOnce()
    {
        try
        {
            applyPending();
            sweep();
            applyReleases();
            queueReprocess();
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
        catch (Exception | Error failure)
        {
            LOG.warn("split.corrections_failed error={}",
                    failure.getClass().getSimpleName());
        }
    }

    /**
     * Applies the pending corrections, oldest first. One whose item's lease
     * another pod holds waits for the next interval.
     *
     * @return how many were applied here
     * @throws Exception if storage or the broker fails
     */
    public int applyPending() throws Exception
    {
        int applied = 0;
        for (CorrectionStore.Pending pending : store.pending(LIMIT))
        {
            try
            {
                if (splitter.correct(pending, store).isPresent())
                {
                    applied++;
                }
            }
            catch (LeaseHeldException held)
            {
                // Another pod has the item; it or a later interval will.
            }
        }
        return applied;
    }

    /**
     * Applies the pending requests to release held items, oldest first.
     * One whose item's lease another pod holds waits for the next
     * interval.
     *
     * @return how many were applied or refused here
     * @throws Exception if storage or the broker fails
     */
    public int applyReleases() throws Exception
    {
        if (releases == null)
        {
            return 0;
        }
        int applied = 0;
        for (HoldReleases.Request request : releases.pending(LIMIT))
        {
            try
            {
                if (splitter.release(request, releases).isPresent())
                {
                    applied++;
                }
            }
            catch (LeaseHeldException held)
            {
                // Another pod has the item; it or a later interval will.
            }
        }
        return applied;
    }

    /**
     * Queues and sends a batch of reprocess runs' objects, if the stage
     * reprocesses.
     *
     * @return how many were sent
     * @throws Exception if storage or the broker fails
     */
    public int queueReprocess() throws Exception
    {
        return reprocess == null ? 0 : reprocess.queueAndSend();
    }

    /**
     * Sends again the end of each item whose correction was applied but
     * whose end wasn't sent since.
     *
     * @return how many were sent
     * @throws Exception if storage or the broker fails
     */
    public int sweep() throws Exception
    {
        int sent = 0;
        for (UUID itemId : store.unnotified(unsentAfter))
        {
            try
            {
                splitter.reannounce(itemId);
                sent++;
            }
            catch (LeaseHeldException held)
            {
                // Another pod has the item.
            }
        }
        return sent;
    }

    /** Stops looking, waiting for an interval under way. */
    @Override
    public synchronized void close()
    {
        if (executor == null)
        {
            return;
        }
        executor.shutdownNow();
        try
        {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
    }
}
