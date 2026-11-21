/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.admin;

import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.landing.intake.ResubmitRequests;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.CliOperation;
import com.sdh.healthcare.connector.stage.admin.Invocation;
import com.sdh.healthcare.connector.stage.admin.Permission;
import java.io.PrintStream;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

/**
 * Asks the running landing node to land a landed item again, as a new item
 * that replaces it (STAGES.md §13.5): for an item the pipeline failed for a
 * reason that wasn't the item's, such as an outage before the split stage
 * recognised one. A reprocess at the split stage splits the same item
 * again; this lands the producer's data as a new item, linked to the one
 * it replaces (STAGES.md §15.8.9).
 * <p>
 * The command takes no lease and writes no landing storage: it leaves a
 * request in the lease database, which the node holding the intake's lease
 * carries out within seconds ({@code ResubmitPoller}), and waits for it.
 * Before asking, it checks that the item is fully landed by this intake.
 * An item with a request still open isn't asked for twice: the command
 * waits for that one. An item already landed again is refused unless
 * {@code --again} is given, so running the command twice doesn't land two
 * copies.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ResubmitCommand implements CliOperation<LandingAdminContext,
        ResubmitCommand.Request, ResubmitCommand.Result>
{

    /** The command's help. */
    static final String USAGE = """
            resubmit --item-id <id> [--again]
                Asks the running landing node to land a fully landed item
                again, from what landing kept, as a new item that replaces
                it (STAGES.md §13.5), and waits for it. The producer gets a
                receipt and a result for the new item, naming the one it
                replaces. Refused if the item was landed again before,
                unless --again.""";

    /** How long to wait for the node. */
    static final Duration WAIT = Duration.ofMinutes(2);

    /** How often to look while waiting. */
    static final Duration LOOK_EVERY = Duration.ofSeconds(2);

    /** Pauses between looks. */
    interface Pause
    {
        /** Sleeps. */
        Pause SLEEP = Thread::sleep;

        /**
         * Pauses.
         *
         * @param millis how long
         * @throws InterruptedException if interrupted
         */
        void pause(long millis) throws InterruptedException;
    }

    /** Gives the time, for the request's ID and the wait. */
    private final Clock clock;

    /** Pauses between looks. */
    private final Pause pause;

    /** Creates the command. */
    ResubmitCommand()
    {
        this(Clock.systemUTC(), Pause.SLEEP);
    }

    /**
     * Creates the command with a clock and pauses of the caller's, for
     * tests.
     *
     * @param clock gives the time
     * @param pause pauses between looks
     */
    ResubmitCommand(Clock clock, Pause pause)
    {
        this.clock = clock;
        this.pause = pause;
    }

    @Override
    public String name()
    {
        return "resubmit";
    }

    @Override
    public String usage()
    {
        return USAGE;
    }

    @Override
    public Set<String> flags()
    {
        return Set.of("again");
    }

    @Override
    public Set<String> options()
    {
        return Set.of("item-id");
    }

    /**
     * What is asked.
     *
     * @param itemId the item to land again
     * @param again  whether to land another copy of one landed again
     *               before
     */
    record Request(UUID itemId, boolean again)
    {
    }

    /**
     * The request left for the landing node.
     *
     * @param itemId  the item
     * @param request the request
     * @param earlier whether it was left before, and is waited for again
     */
    record Asked(UUID itemId, ResubmitRequests.Request request,
            boolean earlier)
    {
    }

    /**
     * What came of it.
     *
     * @param asked the request left
     * @param done  the request as last read: finished, or still waiting
     *              after {@link #WAIT}
     */
    record Result(Asked asked, ResubmitRequests.Request done)
    {
    }

    @Override
    public Permission permission()
    {
        return Permission.PIPELINE_OPERATE;
    }

    @Override
    public Kind kind()
    {
        return Kind.LONG;
    }

    @Override
    public Request bind(AdminArgs arguments, LandingAdminContext context)
    {
        return new Request(parseId(arguments.required("item-id")),
                arguments.flag("again"));
    }

    @Override
    public Result run(Request request, LandingAdminContext context,
            Invocation invocation) throws Exception
    {
        Asked asked = ask(request, context, invocation.audit());
        return finish(asked, context, invocation.audit());
    }

    /**
     * Leaves the request, says so, and only then waits for it, so an
     * operator sees it was asked for while the node lands it.
     */
    @Override
    public void run(AdminArgs arguments, LandingAdminContext context,
            AuditRecord audit, PrintStream out) throws Exception
    {
        Asked asked = ask(bind(arguments, context), context, audit);
        print(asked, out);
        printOutcome(finish(asked, context, audit), out);
    }

    @Override
    public void print(Result result, PrintStream out) throws Exception
    {
        print(result.asked(), out);
        printOutcome(result, out);
    }

    /**
     * Leaves a request for the landing node, unless one is open already.
     *
     * @param request what is asked
     * @param context the landing node's admin context
     * @param audit   the audit event
     * @return the request left, or found open
     * @throws Exception if the item can't be resubmitted, or the tables
     *                   can't be read
     */
    private Asked ask(Request request, LandingAdminContext context,
            AuditRecord audit) throws Exception
    {
        UUID itemId = request.itemId();
        audit.add(Attr.ITEM_ID, itemId);
        String intake = context.properties().landing().intake();
        audit.add(Attr.INTAKE, intake);

        Table landedItems = context.landedItems();
        landedItems.refresh();
        audit.add(Attr.TABLE, LandingTables.LANDED_ITEMS);
        Record summary = LandedItems.summary(landedItems, itemId)
                .orElseThrow(() -> new IllegalStateException(
                        "No fully landed item with this id: one still"
                                + " being landed can't be resubmitted"));
        if (!intake.equals(summary.getField(LandingTables.INTAKE)))
        {
            throw new IllegalStateException(
                    "The item was landed by another intake");
        }

        ResubmitRequests requests = context.resubmitRequests();
        List<ResubmitRequests.Request> earlier =
                requests.forItem(intake, itemId);
        Optional<ResubmitRequests.Request> open = earlier.stream()
                .filter(each -> !each.finished()).findFirst();
        if (open.isPresent())
        {
            return new Asked(itemId, open.get(), true);
        }
        Optional<ResubmitRequests.Request> landed = earlier.stream()
                .filter(r -> r.outcome() == ResubmitRequests.Outcome.LANDED)
                .reduce((first, second) -> second);
        if (landed.isPresent() && !request.again())
        {
            throw new IllegalStateException("The item was landed again"
                    + " before, as " + landed.get().newItemId()
                    + "; give --again to land another copy");
        }
        ResubmitRequests.Request left;
        try
        {
            left = requests.add(intake, itemId, audit.operator(),
                    UuidV7.next(clock.millis()));
        }
        catch (ResubmitRequests.AlreadyOpenException raced)
        {
            left = requests.forItem(intake, itemId).stream()
                    .filter(r -> !r.finished()).findFirst()
                    .orElseThrow(() -> raced);
        }
        return new Asked(itemId, left, false);
    }

    /**
     * Waits for the node to finish the request, up to {@link #WAIT}.
     *
     * @param asked   the request left
     * @param context the landing node's admin context
     * @param audit   the audit event, given the outcome
     * @return what came of it
     * @throws Exception if the node refused it, or it can't be read
     */
    private Result finish(Asked asked, LandingAdminContext context,
            AuditRecord audit) throws Exception
    {
        ResubmitRequests.Request done = waitFor(context.resubmitRequests(),
                asked.request());
        if (!done.finished())
        {
            audit.add(Attr.STATE, "WAITING");
            return new Result(asked, done);
        }
        if (done.outcome() == ResubmitRequests.Outcome.REFUSED)
        {
            audit.add(Attr.STATE, "REFUSED");
            throw new IllegalStateException(
                    "The landing node refused it: " + done.reason());
        }
        audit.add(Attr.STATE, "LANDED");
        audit.add(Attr.REASON, "new item " + done.newItemId());
        return new Result(asked, done);
    }

    /**
     * Says the request was left, or found open.
     *
     * @param asked the request
     * @param out   where to
     */
    private static void print(Asked asked, PrintStream out)
    {
        ResubmitRequests.Request request = asked.request();
        if (asked.earlier())
        {
            out.printf("the item already has a request, %s, from %s: waiting"
                    + " for it%n", request.requestId(), request.operator());
            return;
        }
        out.printf("asked the landing node to land %s again (request"
                + " %s)%n", asked.itemId(), request.requestId());
    }

    /**
     * Says what came of the request.
     *
     * @param result what came of it
     * @param out    where to
     */
    private static void printOutcome(Result result, PrintStream out)
    {
        ResubmitRequests.Request done = result.done();
        if (!done.finished())
        {
            out.printf("not landed yet after %d s: the node lands it once"
                    + " it holds the intake's lease and has no unfinished"
                    + " item of its own. Run this again to wait for it; it"
                    + " won't ask twice.%n", WAIT.toSeconds());
            return;
        }
        out.printf("landed again as %s, which replaces %s; the split stage"
                + " splits it as any new item%n", done.newItemId(),
                result.asked().itemId());
    }

    /**
     * Waits for the node to finish a request, up to {@link #WAIT}.
     *
     * @param requests the request store
     * @param request  the request
     * @return the request as last read
     * @throws InterruptedException if interrupted
     */
    private ResubmitRequests.Request waitFor(ResubmitRequests requests,
            ResubmitRequests.Request request) throws InterruptedException
    {
        long until = clock.millis() + WAIT.toMillis();
        ResubmitRequests.Request now = request;
        while (true)
        {
            now = requests.find(request.requestId()).orElse(now);
            if (now.finished() || clock.millis() >= until)
            {
                return now;
            }
            pause.pause(LOOK_EVERY.toMillis());
        }
    }

    /**
     * Reads an item ID.
     *
     * @param text the ID as typed
     * @return the ID
     * @throws AdminArgs.UsageException if it isn't a UUID
     */
    private static UUID parseId(String text)
    {
        try
        {
            return UUID.fromString(text);
        }
        catch (IllegalArgumentException notAnId)
        {
            throw new AdminArgs.UsageException("--item-id must be a UUID");
        }
    }
}
