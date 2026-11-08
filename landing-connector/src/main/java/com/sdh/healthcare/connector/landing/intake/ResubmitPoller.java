/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.FatalExit;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Carries out operators' requests to land an item again (STAGES.md §13.5):
 * every {@link #INTERVAL}, the intake's open requests, oldest first, each
 * under the intake's writer lock ({@link LandingIntake#resubmit}). Only the
 * node holding the intake's lease runs it, so a standby does nothing.
 * <p>
 * A request landed is audited {@code item.resubmitted} (the new item,
 * naming the one it replaces, and the operator); one that can never be
 * carried out, {@code resubmit.refused}. One that fails because storage,
 * the key service or the broker is down stays open, logged
 * {@code resubmit.failed}, and is tried again at the next run, as is one
 * waiting for an unfinished item of the intake's own. A fatal error stops
 * the node, as it does for the intake.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ResubmitPoller implements AutoCloseable
{

    /** How often open requests are looked for. */
    public static final Duration INTERVAL = Duration.ofSeconds(10);

    /** The most requests read at each run. */
    static final int BATCH = 10;

    private static final Logger LOG =
            LoggerFactory.getLogger(ResubmitPoller.class);

    /** The intake's name. */
    private final String intake;

    /** The intake, which lands each request's item. */
    private final LandingIntake service;

    /** Lands the items, and keeps the requests. */
    private final Resubmitter resubmitter;

    /** Stops the process for a fatal error. */
    private final FatalExit fatal;

    /** How often to look. */
    private final Duration interval;

    /** Runs the polls, once started. */
    private ScheduledExecutorService executor;

    /**
     * Creates the poller; nothing runs until {@link #start}.
     *
     * @param intake      the intake's name
     * @param service     the intake
     * @param resubmitter lands the items
     * @param fatal       stops the process
     * @param interval    how often to look
     */
    public ResubmitPoller(String intake, LandingIntake service,
            Resubmitter resubmitter, FatalExit fatal, Duration interval)
    {
        this.intake = intake;
        this.service = service;
        this.resubmitter = resubmitter;
        this.fatal = fatal;
        this.interval = interval;
    }

    /** Starts looking for requests. */
    public synchronized void start()
    {
        if (executor != null)
        {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(runnable ->
        {
            Thread thread = new Thread(runnable, "landing-resubmit");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::poll, interval.toMillis(),
                interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Carries out the open requests, oldest first, until one has to wait.
     *
     * @return how many were finished, landed or refused
     */
    public int poll()
    {
        ResubmitRequests requests = resubmitter.requests();
        List<ResubmitRequests.Request> open;
        try
        {
            open = requests.open(intake, BATCH);
        }
        catch (RuntimeException failure)
        {
            LOG.warn("resubmit.failed error={}",
                    failure.getClass().getSimpleName());
            return 0;
        }
        int finished = 0;
        for (ResubmitRequests.Request request : open)
        {
            try
            {
                Resubmitter.Landed landed =
                        service.resubmit(resubmitter, request);
                requests.landed(request);
                finished++;
                Telemetry.get().audit("item.resubmitted",
                        Fields.of(Attr.ITEM_ID, landed.item().itemId())
                                .with(Attr.INTAKE, intake)
                                .with(Attr.OPERATOR, request.operator())
                                .with(Attr.REASON, "replaces item "
                                        + request.itemId()));
            }
            catch (Resubmitter.RefusedException refused)
            {
                Telemetry.get().auditWarn("resubmit.refused",
                        Fields.of(Attr.ITEM_ID, request.itemId())
                                .with(Attr.INTAKE, intake)
                                .with(Attr.OPERATOR, request.operator())
                                .with(Attr.REASON, refused.reason()));
                try
                {
                    requests.refused(request, refused.reason());
                    finished++;
                }
                catch (RuntimeException unrecorded)
                {
                    // Refused again at the next run.
                    LOG.warn("resubmit.failed error={}",
                            unrecorded.getClass().getSimpleName());
                    break;
                }
            }
            catch (Resubmitter.BusyException busy)
            {
                LOG.info("resubmit.waiting reason=unfinished_item");
                break;
            }
            catch (InterruptedException interrupted)
            {
                Thread.currentThread().interrupt();
                break;
            }
            catch (Exception | Error failure)
            {
                // A request store that can't be reached isn't fatal: what
                // was landed is found again at the next run.
                if (!(failure instanceof ResubmitRequests.UnavailableException)
                        && IntakeService.isFatal(failure))
                {
                    String reason = IntakeService.reason(failure);
                    LOG.error("intake.fatal reason={}", reason);
                    fatal.exit(reason);
                    break;
                }
                LOG.warn("resubmit.failed error={}",
                        failure.getClass().getSimpleName());
                break;
            }
        }
        return finished;
    }

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
        executor = null;
    }
}
