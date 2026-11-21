/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.stage.RegistryReport;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Set;

/**
 * {@code stuck} for a stage on the shared worker (STAGES.md §13.6): the
 * items not complete that nothing is working on, each with why it waits
 * and the {@code reprocess} line that runs it again, and the records left
 * claimed, by item and pod, with their attempts and how long their claims
 * have been expired. For recovery by hand when the automation hasn't
 * recovered an item: it reads item IDs, counts and times, never a record's
 * ID or content. Reads only the lease database: no keys, no tables, no
 * queue.
 *
 * <p>Its logic is in {@link RegistryReport#stuck} and
 * {@link RegistryReport#leftClaims}, which return rows, and this class
 * only prints them, so a console can show the same rows (STAGES.md
 * §13.6).
 *
 * @param <C> what it reads from
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StuckCommand<C extends StageAdmin>
        implements AdminCommand<C>
{

    /** The most items, and item and pod pairs, listed. */
    static final int LISTED = 50;

    /**
     * How long since an item's last record was stored before it counts
     * as stuck, by default: the reconciler's grace, which it waits before
     * rebuilding what is missing.
     */
    static final Duration IDLE = Duration.ofMinutes(10);

    /** Creates the command. */
    public StuckCommand()
    {
    }

    @Override
    public String name()
    {
        return "stuck";
    }

    @Override
    public String usage()
    {
        return """
                stuck [--idle <minutes>]
                    Lists the items not complete that nothing is working
                    on: none of their records claimed, and none stored for
                    --idle minutes (10), each with why it waits and the
                    reprocess line that runs it again; then the records
                    left claimed, by item and pod, with their attempts and
                    how long their claims have been expired. Names no
                    record. Reads only: takes no lease and changes
                    nothing.""";
    }

    @Override
    public Set<String> options()
    {
        return Set.of("idle");
    }

    @Override
    public Set<String> flags()
    {
        return Set.of();
    }

    @Override
    public boolean readsOnly()
    {
        return true;
    }

    @Override
    public void run(AdminArgs arguments, C context, AuditRecord audit,
            PrintStream out) throws Exception
    {
        audit.add(Attr.STAGE, context.stage());
        Duration idle = arguments.option("idle").map(StuckCommand::minutes)
                .orElse(IDLE);
        StatusParts status = new StatusParts(out);
        status.show("stuck items",
                () -> items(context.registry(), idle, audit));
        status.show("claims", () -> claims(context.registry()));
        status.end();
    }

    /**
     * The items stuck, each with why and its {@code reprocess} line.
     *
     * @param registry the stage's registry
     * @param idle     how long since a record was stored, at least
     * @param audit    the audit event, given the count
     * @return the text to show
     */
    private static String items(RegistryReport registry, Duration idle,
            AuditRecord audit)
    {
        RegistryReport.Listed<RegistryReport.Stuck> stuck =
                registry.stuck(idle, LISTED);
        audit.add(Attr.COUNT, stuck.total());
        StringBuilder text = new StringBuilder();
        text.append(stuck.total()).append(" (none claimed, none stored for ")
                .append(StatusParts.human(idle)).append(')');
        for (RegistryReport.Stuck item : stuck.rows())
        {
            text.append(StatusParts.more(item.itemId() + " " + progress(item)
                    + "; " + why(item)));
            text.append(StatusParts.more("  sdh-admin reprocess --item-id "
                    + item.itemId()));
        }
        if (stuck.total() > stuck.rows().size())
        {
            text.append(StatusParts.more("and "
                    + (stuck.total() - stuck.rows().size()) + " more"));
        }
        return text.toString();
    }

    /**
     * How far a stuck item got.
     *
     * @param item the item
     * @return the text, such as {@code 40 of 50 stored (2 failed)}
     */
    private static String progress(RegistryReport.Stuck item)
    {
        String of = item.expected() < 0 ? " stored, of a total not received"
                : " of " + item.expected() + (item.upstreamOpen()
                        ? " so far" : "") + " stored";
        String failed = item.failed() > 0 ? " (" + item.failed() + " failed)"
                : "";
        String since = item.idle() == null ? ", none ever"
                : ", the last " + StatusParts.human(item.idle()) + " ago";
        return item.done() + of + failed + since;
    }

    /**
     * Why a stuck item waits, and what recovers it without an operator.
     *
     * @param item the item
     * @return the text
     */
    static String why(RegistryReport.Stuck item)
    {
        if (item.expired() > 0)
        {
            return item.expired() + " left claimed past their claim: the"
                    + " sweep hands them in again (§10)";
        }
        if (item.expected() < 0)
        {
            return "its end from the stage before hasn't come"
                    + (item.totalWanted() ? ": asked for again (§13.2)"
                            : "");
        }
        if (item.done() >= item.expected() && !item.upstreamOpen())
        {
            return "all stored, not completed: the reconciler completes it"
                    + " (§10)";
        }
        if (item.upstreamOpen())
        {
            return "still open at the stage before";
        }
        return (item.expected() - item.done()) + " not received: the"
                + " reconciler rebuilds them from the stage before (§9)";
    }

    /**
     * The records left claimed, by item and pod.
     *
     * @param registry the stage's registry
     * @return the text to show
     */
    private static String claims(RegistryReport registry)
    {
        RegistryReport.Listed<RegistryReport.LeftClaim> claims =
                registry.leftClaims(LISTED);
        if (claims.total() == 0)
        {
            return "none";
        }
        StringBuilder text = new StringBuilder();
        text.append(claims.total()).append(claims.total() == 1
                ? " item and pod" : " items and pods")
                .append(", the longest expired first");
        for (RegistryReport.LeftClaim claim : claims.rows())
        {
            String expiry = claim.expiry().isNegative()
                    ? "held, expires in "
                            + StatusParts.human(claim.expiry().negated())
                    : "expired " + StatusParts.human(claim.expiry())
                            + " ago";
            text.append(StatusParts.more(claim.itemId() + " " + claim.pod()
                    + ": " + claim.records()
                    + (claim.records() == 1 ? " record" : " records")
                    + ", at most " + claim.attempts()
                    + (claim.attempts() == 1 ? " attempt, " : " attempts, ")
                    + expiry));
        }
        if (claims.total() > claims.rows().size())
        {
            text.append(StatusParts.more("and "
                    + (claims.total() - claims.rows().size()) + " more"));
        }
        return text.toString();
    }

    /**
     * Reads {@code --idle}.
     *
     * @param text the argument
     * @return the duration
     * @throws AdminArgs.UsageException if it isn't a whole number of
     *                                  minutes from 0
     */
    private static Duration minutes(String text)
    {
        try
        {
            long minutes = Long.parseLong(text);
            if (minutes >= 0)
            {
                return Duration.ofMinutes(minutes);
            }
        }
        catch (NumberFormatException notNumber)
        {
            // Refused below.
        }
        throw new AdminArgs.UsageException(
                "--idle must be a whole number of minutes");
    }
}
