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
import java.util.LinkedHashMap;
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
        implements CliOperation<C, StageStatusCommand.Request,
                StageStatusCommand.Result>,
        Answering<StageStatusCommand.Result>
{

    /** How many incomplete items are listed. */
    static final int LISTED = 20;

    /** How far behind the reconciler may be before it is flagged. */
    static final Duration STALE = Duration.ofHours(1);

    /**
     * What is asked.
     *
     * @param itemId one item to show; null for the stage as a whole
     */
    public record Request(UUID itemId)
    {
    }

    /**
     * What was found, each part read on its own.
     *
     * @param duties     the duties lease
     * @param item       the item asked for; null for the stage
     * @param items      the items not complete; null for one item
     * @param claims     the records claimed now; null for one item
     * @param reconciled how far the reconciler has checked; null for one
     *                   item
     */
    public record Result(Partial<StatusParts.Lease> duties,
            Partial<ItemDetail> item, Partial<Items> items,
            Partial<RegistryReport.Claims> claims,
            Partial<Optional<RegistryReport.Reconciled>> reconciled)
    {
    }

    /**
     * The stage's items.
     *
     * @param incomplete how many are in progress
     * @param complete   how many are complete
     * @param listed     the first of those in progress
     */
    public record Items(long incomplete, Long complete,
            List<RegistryReport.Item> listed)
    {
    }

    /**
     * One item.
     *
     * @param itemId   the item
     * @param item     its progress; null if none of its records came yet
     * @param states   its records counted by state
     * @param attempts the most attempts any of its records has had
     */
    public record ItemDetail(UUID itemId, RegistryReport.Item item,
            Map<String, Long> states, long attempts)
    {
    }

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
    public Permission permission()
    {
        return Permission.PIPELINE_VIEW;
    }

    @Override
    public Kind kind()
    {
        return Kind.QUERY;
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
    public Request bind(AdminArgs arguments, C context)
    {
        return new Request(arguments.option("item")
                .map(StageStatusCommand::itemId).orElse(null));
    }

    @Override
    public Result run(Request request, C context, Invocation invocation)
    {
        AuditRecord audit = invocation.audit();
        audit.add(Attr.STAGE, context.stage());
        Partial<StatusParts.Lease> duties = Partial.of(() ->
                StatusParts.Lease.read(context.leases(), context.stage()
                        + "/" + context.connectorId() + "/duties"));
        if (request.itemId() != null)
        {
            audit.add(Attr.ITEM_ID, request.itemId());
            return new Result(duties, Partial.of(() -> item(
                    context.registry(), request.itemId())), null, null,
                    null);
        }
        return new Result(duties, null,
                Partial.of(() -> items(context.registry(), audit)),
                Partial.of(() -> context.registry().claims()),
                Partial.of(() -> context.registry().reconciled()));
    }

    @Override
    public void print(Result result, PrintStream out) throws Exception
    {
        StatusParts status = new StatusParts(out);
        status.show("duties", () -> result.duties().get().text());
        if (result.item() != null)
        {
            status.show("item", () -> item(result.item().get()));
        }
        else
        {
            status.show("items", () -> items(result.items().get()));
            status.show("records", () -> claims(result.claims().get()));
            status.show("reconciler",
                    () -> reconciled(result.reconciled().get()));
        }
        status.end();
    }

    @Override
    public Map<String, Object> answer(Result result)
    {
        return Answers.object(
                "duties", Answers.part(result.duties(), Answers::lease),
                "item", Answers.part(result.item(),
                        StageStatusCommand::answer),
                "items", Answers.part(result.items(), items ->
                        Answers.object("incomplete", items.incomplete(),
                                "complete", items.complete(),
                                "listed", Answers.list(items.listed(),
                                        StageStatusCommand::answer))),
                "claims", Answers.part(result.claims(), claims ->
                        Answers.object("claimed", claims.claimed(),
                                "expired", claims.expired())),
                "reconciler", Answers.part(result.reconciled(),
                        reconciled -> reconciled.map(checked ->
                                (Object) Answers.object("checkedTo",
                                        checked.checkedTo(), "behind",
                                        checked.behind())).orElse(null)));
    }

    /**
     * An item's progress, as the API answers it.
     *
     * @param item the item
     * @return its counts
     */
    private static Object answer(RegistryReport.Item item)
    {
        return Answers.object("itemId", item.itemId(), "expected",
                item.expected(), "done", item.done(), "failed",
                item.failed(), "completedAt", item.completedAt());
    }

    /**
     * One item, as the API answers it.
     *
     * @param detail the item
     * @return its progress and its records by state
     */
    private static Object answer(ItemDetail detail)
    {
        return Answers.object("itemId", detail.itemId(), "progress",
                detail.item() == null ? null : answer(detail.item()),
                "records", detail.states(), "attempts", detail.attempts());
    }

    /**
     * Reads the items not complete, with the first of them.
     *
     * @param registry the stage's registry
     * @param audit    the audit event, given the count in progress
     * @return the items
     */
    private static Items items(RegistryReport registry, AuditRecord audit)
    {
        Map<String, Long> counts = registry.itemCounts();
        long incomplete = counts.get("incomplete");
        audit.add(Attr.COUNT, incomplete);
        return new Items(incomplete, counts.get("complete"),
                registry.incomplete(LISTED));
    }

    /**
     * The items not complete, with the first of them.
     *
     * @param items the items
     * @return the text to show
     */
    private static String items(Items items)
    {
        StringBuilder text = new StringBuilder();
        text.append(items.incomplete()).append(" in progress, ")
                .append(items.complete()).append(" complete");
        for (RegistryReport.Item item : items.listed())
        {
            text.append(StatusParts.more(item.itemId() + " "
                    + progress(item)));
        }
        if (items.incomplete() > items.listed().size())
        {
            text.append(StatusParts.more("and " + (items.incomplete()
                    - items.listed().size())
                    + " more; --item <id> shows one"));
        }
        return text.toString();
    }

    /**
     * The records claimed now, and how many of those claims expired.
     *
     * @param claims the claims
     * @return the text to show
     */
    private static String claims(RegistryReport.Claims claims)
    {
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
     * How far the reconciler has checked, flagged when it is far behind.
     *
     * @param reconciled where it has checked to; empty if it hasn't yet
     * @return the text to show
     */
    private static String reconciled(
            Optional<RegistryReport.Reconciled> reconciled)
    {
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
                    + " an outage (stage.reconcile_catching_up;"
                    + " STAGES.md §9)");
        }
        return text;
    }

    /**
     * Reads one item and its records by state.
     *
     * @param registry the stage's registry
     * @param itemId   the item
     * @return the item
     */
    private static ItemDetail item(RegistryReport registry, UUID itemId)
    {
        RegistryReport.Item item = registry.item(itemId).orElse(null);
        if (item == null)
        {
            return new ItemDetail(itemId, null, Map.of(), 0);
        }
        Map<String, Long> states = new LinkedHashMap<>(
                registry.recordStates(itemId));
        long attempts = states.remove("attempts");
        return new ItemDetail(itemId, item, states, attempts);
    }

    /**
     * One item and its records by state.
     *
     * @param detail the item
     * @return the text to show
     */
    private static String item(ItemDetail detail)
    {
        UUID itemId = detail.itemId();
        RegistryReport.Item item = detail.item();
        if (item == null)
        {
            return itemId + " none of its records received yet";
        }
        StringBuilder text = new StringBuilder();
        text.append(itemId).append(' ').append(progress(item));
        text.append(item.completedAt() == null ? ", in progress"
                : ", complete " + item.completedAt());
        Map<String, Long> states = detail.states();
        text.append(StatusParts.more("records " + (states.isEmpty() ? "none"
                : states.entrySet().stream()
                        .map(state -> state.getKey() + " " + state.getValue())
                        .collect(Collectors.joining(", ")))
                + (detail.attempts() > 1 ? "; at most " + detail.attempts()
                        + " attempts" : "")));
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
