/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.inbox;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.inbox.Inbox;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.runtime.FatalExit;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits the inbox's items one at a time, in order (design §5.3.6). Each
 * item is read from landing, split, stored and published; its result is
 * sent to the producer, if results are on (design §5.3.8); then the
 * inbox's position moves past it. A checkpoint left in progress by an item that
 * isn't next in the inbox, such as a file from before landing, is
 * abandoned first.
 * <p>
 * An error the item can't be blamed for (storage or the broker down) is
 * retried, with a growing pause, from the item's checkpoint. Lease loss, a
 * commit whose outcome isn't known, or a tampered checkpoint stop the
 * process. The writer lock is held while an item is processed, so table
 * maintenance never runs at the same time.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class InboxWorker implements AutoCloseable
{

    /** The longest pause between attempts after errors. */
    static final Duration MAX_BACKOFF = Duration.ofMinutes(5);

    /** How long closing waits for an item to finish. */
    static final Duration STOP_WAIT = Duration.ofSeconds(60);

    /** Logs progress, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(InboxWorker.class);

    /** Splits, stores and publishes items. */
    private final ItemProcessor processor;

    /** Where items wait. */
    private final Inbox inbox;

    /** The {@code landed_items} table. */
    private final LandedItemsTable landedItems;

    /** Unwraps the landed items' data keys. */
    private final KeyProvider keys;

    /** How long to wait for an item before looking again. */
    private final Duration idleWait;

    /** The first pause after an error. */
    private final Duration initialBackoff;

    /** Stops the process on a fatal error. */
    private final FatalExit fatal;

    /** Held while an item is processed. */
    private final ReentrantLock writer;

    /** Sends results to the producer; null if they are off. */
    private final ResultReporter results;

    /** Adds items whose messages were lost; null if off. */
    private InboxReconciler reconciler;

    /** How often the reconciler runs. */
    private Duration reconcileInterval;

    /** When the reconciler is next due, in {@link System#nanoTime}. */
    private long reconcileDue;

    /** False once closing has begun. */
    private volatile boolean running = true;

    /** Errors in a row, for the pause. */
    private int consecutiveErrors;

    /** The worker's thread, once started. */
    private Thread thread;

    /**
     * Creates a worker.
     *
     * @param processor      splits, stores and publishes items
     * @param inbox          where items wait
     * @param landedItems    the {@code landed_items} table
     * @param keys           unwraps the landed items' data keys
     * @param idleWait       how long to wait for an item before looking
     *                       again
     * @param initialBackoff the first pause after an error
     * @param fatal          stops the process on a fatal error
     * @param writer         held while an item is processed
     */
    public InboxWorker(ItemProcessor processor, Inbox inbox,
            LandedItemsTable landedItems, KeyProvider keys, Duration idleWait,
            Duration initialBackoff, FatalExit fatal, ReentrantLock writer)
    {
        this(processor, inbox, landedItems, keys, idleWait, initialBackoff,
                fatal, writer, null);
    }

    /**
     * Creates a worker that sends each item's result.
     *
     * @param processor      splits, stores and publishes items
     * @param inbox          where items wait
     * @param landedItems    the {@code landed_items} table
     * @param keys           unwraps the landed items' data keys
     * @param idleWait       how long to wait for an item before looking
     *                       again
     * @param initialBackoff the first pause after an error
     * @param fatal          stops the process on a fatal error
     * @param writer         held while an item is processed
     * @param results        sends results to the producer; null if they
     *                       are off
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public InboxWorker(ItemProcessor processor, Inbox inbox,
            LandedItemsTable landedItems, KeyProvider keys, Duration idleWait,
            Duration initialBackoff, FatalExit fatal, ReentrantLock writer,
            ResultReporter results)
    {
        this.processor = processor;
        this.inbox = inbox;
        this.landedItems = landedItems;
        this.keys = keys;
        this.idleWait = idleWait;
        this.initialBackoff = initialBackoff;
        this.fatal = fatal;
        this.writer = writer;
        this.results = results;
    }

    /**
     * Has the worker look for landed items whose messages were lost, as
     * soon as it starts and then at each interval, between items. Called
     * before {@link #start}.
     *
     * @param reconciler finds and adds the items
     * @param interval   how often to look
     * @return this worker
     */
    public InboxWorker reconcileWith(InboxReconciler reconciler,
            Duration interval)
    {
        this.reconciler = reconciler;
        this.reconcileInterval = interval;
        this.reconcileDue = System.nanoTime();
        return this;
    }

    /** Starts the worker's thread. */
    public synchronized void start()
    {
        thread = Thread.ofPlatform().name("sdh-splitter").start(this::run);
        LOG.info("inbox.worker_started");
    }

    /**
     * Processes the next item, if one arrives within the idle wait.
     *
     * @return true if an item was processed
     * @throws Exception if it couldn't be; it is tried again next time
     */
    public boolean processNext() throws Exception
    {
        reconcileIfDue();
        Optional<Inbox.Entry> next = inbox.next(idleWait);
        if (next.isEmpty())
        {
            return false;
        }
        writer.lockInterruptibly();
        try
        {
            if (!running)
            {
                return false;
            }
            process(next.get());
            return true;
        }
        finally
        {
            writer.unlock();
        }
    }

    /**
     * Takes an item through the splitter, sends its result, then moves the
     * position past it.
     *
     * @param entry the item
     * @throws Exception if storage or the broker fails
     */
    private void process(Inbox.Entry entry) throws Exception
    {
        Optional<Checkpoint> inProgress = processor.inProgress();
        if (inProgress.isPresent()
                && !inProgress.get().fileId().equals(entry.itemId()))
        {
            processor.abandon(inProgress.get());
        }
        ItemProcessor.Outcome outcome = processor.process(entry.itemId(),
                entry.digest(), () -> LandedItemSource.open(
                        landedItems.get(), entry.itemId(), keys));
        if (results != null)
        {
            results.report(outcome);
        }
        inbox.done(entry.seq());
        LOG.info("inbox.done itemId={} seq={} result={} sections={}",
                entry.itemId(), entry.seq(), outcome.result(),
                outcome.sectionCount());
    }

    /**
     * Runs the reconciler if it is due. If it fails, splitting carries on
     * and it runs again at the next interval; only a fatal error, such as
     * a lost lease, is passed on, to stop the process.
     *
     * @throws Exception if interrupted, or the error is fatal
     */
    void reconcileIfDue() throws Exception
    {
        if (reconciler == null || System.nanoTime() - reconcileDue < 0)
        {
            return;
        }
        reconcileDue = System.nanoTime() + reconcileInterval.toNanos();
        try
        {
            reconciler.reconcile();
        }
        catch (InterruptedException interrupted)
        {
            throw interrupted;
        }
        catch (Exception failure)
        {
            if (fatalReason(failure).isPresent())
            {
                throw failure;
            }
            LOG.warn("inbox.reconcile_failed error={} next_in_s={}",
                    failure.getClass().getSimpleName(),
                    reconcileInterval.toSeconds());
        }
    }

    /** The thread's loop: process items until closed. */
    private void run()
    {
        while (running)
        {
            try
            {
                processNext();
                consecutiveErrors = 0;
            }
            catch (InterruptedException interrupted)
            {
                return;
            }
            catch (Exception | Error failure)
            {
                if (!onError(failure))
                {
                    return;
                }
            }
        }
    }

    /**
     * Stops the process on a fatal error; otherwise pauses before the next
     * attempt.
     *
     * @param failure the error
     * @return false if the loop must end
     */
    private boolean onError(Throwable failure)
    {
        Optional<String> reason = fatalReason(failure);
        if (reason.isPresent())
        {
            LOG.error("inbox.fatal reason={}", reason.get());
            fatal.exit(reason.get());
            return false;
        }
        consecutiveErrors++;
        long pause = Math.min(MAX_BACKOFF.toMillis(),
                initialBackoff.toMillis()
                        << Math.min(consecutiveErrors - 1, 20));
        LOG.warn("inbox.retry error={} attempt={} backoff_ms={}",
                failure.getClass().getSimpleName(), consecutiveErrors, pause);
        try
        {
            Thread.sleep(pause);
            return true;
        }
        catch (InterruptedException interrupted)
        {
            return false;
        }
    }

    /**
     * Tells whether an error must stop the process, and why.
     *
     * @param failure the error
     * @return the reason, for the exit; empty if the error can be retried
     */
    public static Optional<String> fatalReason(Throwable failure)
    {
        if (failure instanceof LeaseLostException lost)
        {
            return Optional.of("LEASE_LOST:" + lost.reason());
        }
        if (failure instanceof CheckpointTamperedException)
        {
            return Optional.of("CHECKPOINT_TAMPERED");
        }
        if (failure instanceof CommitStateUnknownException)
        {
            return Optional.of("COMMIT_STATE_UNKNOWN");
        }
        if (failure instanceof Error
                || failure instanceof IllegalStateException)
        {
            return Optional.of(failure.getClass().getSimpleName());
        }
        return Optional.empty();
    }

    /**
     * Stops the worker. An item being processed is given a while to
     * finish; if it doesn't, it is interrupted, and resumes from its
     * checkpoint next time.
     */
    @Override
    public void close()
    {
        running = false;
        Thread worker;
        synchronized (this)
        {
            worker = thread;
        }
        if (worker == null)
        {
            return;
        }
        if (writer.tryLock())
        {
            try
            {
                worker.interrupt();
            }
            finally
            {
                writer.unlock();
            }
        }
        try
        {
            worker.join(STOP_WAIT.toMillis());
            if (worker.isAlive())
            {
                LOG.warn("inbox.worker_interrupted");
                worker.interrupt();
                worker.join(STOP_WAIT.toMillis());
            }
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
        LOG.info("inbox.worker_stopped");
    }
}
