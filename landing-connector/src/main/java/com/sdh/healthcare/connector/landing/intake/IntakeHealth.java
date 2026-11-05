/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Whether an intake's source is answering, for the outage events and the
 * intake health gauges (design §5.3.4). Every intake that polls a source
 * reports through one of these: a listing that worked, or a poll of a
 * broker, is {@link #answered()}; one that couldn't reach the source is
 * {@link #unreachable}.
 * <p>
 * {@code intake.unreachable} is reported once per reason, until the
 * source answers again or the reason changes; {@code intake.reachable}
 * when it answers again. Neither carries a name or path. Thread-safe.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class IntakeHealth
{

    /** The intake's name. */
    private final String intake;

    /** The time, in nanoseconds. */
    private final LongSupplier nanoTime;

    /** Whether the source is unreachable now. */
    private boolean unreachable;

    /** Why, while it is unreachable; null for a directory. */
    private IntakeProblem problem;

    /** When the source last answered; when this was made, until then. */
    private long answeredAt;

    /** When the current outage began; meaningless while reachable. */
    private long outageFrom;

    /**
     * Creates the health of an intake whose source hasn't been tried yet.
     *
     * @param intake   the intake's name
     * @param nanoTime the time, in nanoseconds
     */
    public IntakeHealth(String intake, LongSupplier nanoTime)
    {
        this.intake = intake;
        this.nanoTime = nanoTime;
        this.answeredAt = nanoTime.getAsLong();
    }

    /**
     * Notes that the source answered: a listing or poll worked. Ends an
     * outage, reporting {@code intake.reachable}.
     */
    public void answered()
    {
        boolean wasUnreachable;
        synchronized (this)
        {
            answeredAt = nanoTime.getAsLong();
            wasUnreachable = unreachable;
            unreachable = false;
            problem = null;
        }
        if (wasUnreachable)
        {
            Telemetry.get().audit("intake.reachable",
                    Fields.of(Attr.INTAKE, intake));
        }
    }

    /**
     * Notes that the source couldn't be reached, reporting
     * {@code intake.unreachable} unless it already was for the same reason.
     *
     * @param reason why, for a remote source or a broker; null for a
     *               directory
     */
    public void unreachable(IntakeProblem reason)
    {
        synchronized (this)
        {
            if (unreachable && reason == problem)
            {
                return;
            }
            if (!unreachable)
            {
                outageFrom = nanoTime.getAsLong();
            }
            unreachable = true;
            problem = reason;
        }
        Fields fields = Fields.of(Attr.INTAKE, intake)
                .with(Attr.ERROR_CODE, ErrorCode.INTAKE_UNREACHABLE);
        if (reason != null)
        {
            fields = fields.with(Attr.REASON, reason.name());
        }
        Telemetry.get().auditError("intake.unreachable", fields);
    }

    /**
     * Why the source can't be reached now.
     *
     * @return the reason; null while it answers, or for a directory
     */
    public synchronized IntakeProblem problem()
    {
        return unreachable ? problem : null;
    }

    /**
     * Whether the source is reachable, as far as is known: until it is
     * first tried, it is taken to be.
     *
     * @return true unless an outage is in progress
     */
    public synchronized boolean reachable()
    {
        return !unreachable;
    }

    /**
     * For the {@code sdh.intake.last_poll_age} gauge.
     *
     * @return whole seconds since the source last answered
     */
    public synchronized long secondsSinceAnswered()
    {
        return TimeUnit.NANOSECONDS.toSeconds(
                nanoTime.getAsLong() - answeredAt);
    }

    /**
     * For the {@code sdh.intake.outage_seconds} gauge.
     *
     * @return whole seconds since the current outage began; 0 while the
     *         source is reachable
     */
    public synchronized long outageSeconds()
    {
        return unreachable ? TimeUnit.NANOSECONDS.toSeconds(
                nanoTime.getAsLong() - outageFrom) : 0;
    }
}
