/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.LeaseStatus;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Optional;

/**
 * Shows a status in parts, each read on its own, so one that can't be read
 * (an unreachable KMS, say) still leaves the others visible.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StatusParts
{

    /** The width of a part's label, so the values line up. */
    static final int LABEL_WIDTH = 21;

    /** Where the parts are shown. */
    private final PrintStream out;

    /** Whether every part so far was read. */
    private boolean complete = true;

    /**
     * Starts a status.
     *
     * @param out where the parts are shown
     */
    public StatusParts(PrintStream out)
    {
        this.out = out;
    }

    /** Reads one part of the status. */
    @FunctionalInterface
    public interface Part
    {

        /**
         * Reads the part.
         *
         * @return the text to show; further lines are indented with
         *         {@link #more}
         * @throws Exception if the part can't be read
         */
        String read() throws Exception;
    }

    /**
     * Shows one part, or that it is unavailable, and why: the exception's
     * class and message, which here name a setting or a table, never
     * data.
     *
     * @param label the part's label
     * @param part  reads the part
     * @return this status
     */
    public StatusParts show(String label, Part part)
    {
        try
        {
            out.printf("%-" + LABEL_WIDTH + "s %s%n", label + ":", part.read());
        }
        catch (Exception failure)
        {
            complete = false;
            out.printf("%-" + LABEL_WIDTH + "s unavailable (%s: %s)%n",
                    label + ":", failure.getClass().getSimpleName(),
                    failure.getMessage());
        }
        return this;
    }

    /**
     * Ends the status.
     *
     * @throws Unavailable if a part couldn't be read
     */
    public void end() throws Unavailable
    {
        if (!complete)
        {
            throw new Unavailable();
        }
    }

    /**
     * A further line of a part, indented under its first.
     *
     * @param text the line
     * @return the line, with the line break before it
     */
    public static String more(String text)
    {
        return String.format("%n%-" + LABEL_WIDTH + "s %s", "", text);
    }

    /**
     * A lease: who holds it and until when, or when it expired. Inspecting
     * it never takes it.
     *
     * @param leases where the leases are kept
     * @param name   the lease's name
     * @return the text to show
     */
    public static String lease(JdbcLeaseStore leases, String name)
    {
        Optional<LeaseStatus> inspected = leases.inspect(name);
        if (inspected.isEmpty())
        {
            return name + " never held";
        }
        LeaseStatus lease = inspected.get();
        if (lease.live())
        {
            return "%s held by %s, epoch %d, renewed %s, expires in %s"
                    .formatted(name, lease.owner(), lease.epoch(),
                            lease.renewedAt(), human(lease.remaining()));
        }
        return "%s free (expired %s ago; last holder %s, epoch %d)"
                .formatted(name, human(lease.remaining().negated()),
                        lease.owner(), lease.epoch());
    }

    /**
     * A duration in its largest sensible unit: seconds up to 2 minutes,
     * minutes up to 2 hours, hours up to 2 days, then days.
     *
     * @param duration the duration; negative counts as zero
     * @return the text, such as {@code 90s} or {@code 3h}
     */
    public static String human(Duration duration)
    {
        long seconds = Math.max(0, duration.toSeconds());
        if (seconds < 120)
        {
            return seconds + "s";
        }
        if (seconds < 7200)
        {
            return seconds / 60 + "m";
        }
        if (seconds < 172_800)
        {
            return seconds / 3600 + "h";
        }
        return seconds / 86_400 + "d";
    }

    /**
     * Some parts of a status couldn't be read; they are shown as
     * unavailable, so nothing more is printed for it.
     */
    public static final class Unavailable extends Exception
    {

        /** Creates the exception. */
        public Unavailable()
        {
            super("Some status parts are unavailable");
        }
    }
}
