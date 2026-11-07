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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * {@code status} for a stage on the shared worker (STAGES.md §7): its
 * duties lease, its items not yet complete, the records claimed now and
 * how far its reconciler has checked; or, with {@code --item}, one item.
 * Reads only the lease database: no keys, no tables, no queue.
 *
 * @param <C> what it reads from
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StageStatusCommand<C extends StageAdmin>
        implements AdminCommand<C>
{

    /** The most items listed. */
    static final int LISTED = 20;

    /**
     * How far behind the reconciler's watermark may be before the status
     * warns: ordinarily it is about 25 minutes, the grace and one interval.
     */
    static final Duration STALE = Duration.ofHours(1);

    /** Creates the command. */
    public StageStatusCommand()
    {
    }

    @Override
    public String name()
    {
        return "status";
    }

    @Override
    public String usage()
    {
        return """
                status [--item <id>]
                    Shows the duties lease, the items not yet complete
                    (records stored, of how many) and the records claimed
                    now, and how far the reconciler has checked; with
                    --item, one item and its records by state. Reads only:
                    takes no lease and changes nothing.""";
    }

    @Override
    public Set<String> options()
    {
        return Set.of("item");
    }

    @Override
    public Set<String> flags()
    {
        return Set.of();
    }

    @Override
    public void run(AdminArgs arguments, C context, AuditRecord audit,
            PrintStream out) throws Exception
    {
        audit.add(Attr.STAGE, context.stage());
        StatusParts status = new StatusParts(out);
        status.show("duties", () -> StatusParts.lease(context.leases(),
                context.stage() + "/" + context.connectorId() + "/duties"));
        if (arguments.option("item").isPresent())
        {
            UUID itemId = itemId(arguments.required("item"));
            audit.add(Attr.ITEM_ID, itemId);
            status.show("item", () -> item(context.registry(), itemId));
        }
        else
        {
            status.show("items", () -> items(context.registry(), audit));
            status.show("records", () -> claims(context.registry()));
            status.show("reconciler",
                    () -> reconciled(context.registry()));
        }
        status.end();
    }

    /**
     * The items not yet complete.
     *
     * @param registry the stage's registry
     * @param audit    the audit event, given the count
     * @return the text to show
     */
    private static String items(RegistryReport registry, AuditRecord audit)
    {
        Map<String, Long> counts = registry.itemCounts();
        long incomplete = counts.get("incomplete");
        audit.add(Attr.COUNT, incomplete);
        StringBuilder text = new StringBuilder();
        text.append(incomplete).append(" in progress, ")
                .append(counts.get("complete")).append(" complete");
        List<RegistryReport.Item> listed = registry.incomplete(LISTED);
        for (RegistryReport.Item item : listed)
        {
            text.append(StatusParts.more(item.itemId() + " "
                    + progress(item)));
        }
        if (incomplete > listed.size())
        {
            text.append(StatusParts.more("and " + (incomplete - listed.size())
                    + " more; --item <id> shows one"));
        }
        return text.toString();
    }

    /**
     * The records claimed now.
     *
     * @param registry the stage's registry
     * @return the text to show
     */
    private static String claims(RegistryReport registry)
    {
        RegistryReport.Claims claims = registry.claims();
        String text = claims.claimed() + " claimed";
        if (claims.expired() > 0)
        {
            text += StatusParts.more(claims.expired() + " of them past their"
                    + " claim: their worker stopped, and the next delivery"
                    + " takes them over");
        }
        return text;
    }

    /**
     * How far the reconciler has checked, and a warning if that is long
     * ago: it runs every 15 minutes by default, after a 10-minute grace.
     *
     * @param registry the stage's registry
     * @return the text to show
     */
    private static String reconciled(RegistryReport registry)
    {
        Optional<RegistryReport.Reconciled> reconciled =
                registry.reconciled();
        if (reconciled.isEmpty())
        {
            return "hasn't checked yet: it starts at the lookback";
        }
        Duration behind = reconciled.get().behind();
        String text = "checked up to " + reconciled.get().checkedTo() + " ("
                + behind.toMinutes() + " min ago)";
        if (behind.compareTo(STALE) > 0)
        {
            text += StatusParts.more("more than " + STALE.toHours()
                    + " h behind: the duties pod is down, or catching up on"
                    + " an outage (convert.reconcile_catching_up;"
                    + " STAGES.md §9)");
        }
        return text;
    }

    /**
     * One item, and its records by state.
     *
     * @param registry the stage's registry
     * @param itemId   the item
     * @return the text to show
     */
    private static String item(RegistryReport registry, UUID itemId)
    {
        RegistryReport.Item item = registry.item(itemId).orElse(null);
        if (item == null)
        {
            return itemId + " none of its records received yet";
        }
        StringBuilder text = new StringBuilder();
        text.append(itemId).append(' ').append(progress(item));
        text.append(item.completedAt() == null ? ", in progress"
                : ", complete " + item.completedAt());
        Map<String, Long> states = registry.recordStates(itemId);
        long attempts = states.remove("attempts");
        text.append(StatusParts.more("records " + (states.isEmpty() ? "none"
                : states.entrySet().stream()
                        .map(state -> state.getKey() + " " + state.getValue())
                        .collect(Collectors.joining(", ")))
                + (attempts > 1 ? "; at most " + attempts + " attempts"
                        : "")));
        if (item.expected() < 0)
        {
            text.append(StatusParts.more("its item message hasn't been"
                    + " received: it can't complete until it is"
                    + " (STAGES.md §13.2)"));
        }
        return text.toString();
    }

    /**
     * How far an item has got.
     *
     * @param item the item
     * @return the text, such as {@code 40 of 50 stored (2 failed)}
     */
    private static String progress(RegistryReport.Item item)
    {
        String of = item.expected() < 0 ? " stored, total not yet received"
                : " of " + item.expected() + " stored";
        return item.done() + of + (item.failed() > 0
                ? " (" + item.failed() + " failed)" : "");
    }

    /**
     * Reads an item ID.
     *
     * @param text the argument
     * @return the ID
     * @throws AdminArgs.UsageException if it isn't a UUID
     */
    public static UUID itemId(String text)
    {
        try
        {
            return UUID.fromString(text);
        }
        catch (IllegalArgumentException notUuid)
        {
            throw new AdminArgs.UsageException("--item must be a UUID");
        }
    }
}
