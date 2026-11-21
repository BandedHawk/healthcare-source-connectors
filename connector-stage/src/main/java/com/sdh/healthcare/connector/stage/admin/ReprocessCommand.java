/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.failure.FailureReason;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.stage.ReprocessInputs;
import com.sdh.healthcare.connector.stage.ReprocessStore;
import java.io.PrintStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * {@code reprocess} (STAGES.md §15.8.4): has the stage process again,
 * under the version running now, the objects of its input an operator
 * chooses, each as the next revision of its place. The command records a
 * run and the objects it chose, a fixed list; the stage's pods queue them,
 * a batch each renewal interval, and each is stored through the currency
 * rule (§15.8.3) and sent on as any record, saying it was reprocessed.
 * With {@code --status} it lists a run, object by object; with
 * {@code --cancel} it cancels what of a run isn't yet queued.
 *
 * @param <C> the stage's context
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ReprocessCommand<C extends ReprocessAdmin>
        implements CliOperation<C, ReprocessCommand.Request,
                ReprocessCommand.Result>
{

    /** The most objects one run chooses. */
    static final int MAX_OBJECTS = 100_000;

    /** The most objects a dry run prints, one per line. */
    static final int MAX_PRINTED = 50;

    /** The event number a run's reason is sealed under. */
    private static final int REASON_EVENT = 1;

    /** The record number a run's reason is sealed under: none. */
    private static final long REASON_SEQ = -2;

    /** A version as {@code --made-by} takes it: no content, short. */
    private static final Pattern VERSION =
            Pattern.compile("[A-Za-z0-9._@:/+-]{1,200}");

    /**
     * What is asked: a run's status, its cancelling, or a new run.
     *
     * @param status     the status of a run
     * @param cancel     cancelling what of a run isn't yet queued
     * @param runId      the run, for its status or cancelling
     * @param itemIds    the items chosen; empty for any
     * @param seq        one record number of one item; null for all
     * @param madeBy     the version whose output is chosen; null for any
     * @param landedFrom the start of the landing times chosen; null for
     *                   any
     * @param landedTo   their end, excluded; null for any
     * @param intake     the intake the items were landed by; null for any
     * @param dryRun     whether to show what would be chosen, recording
     *                   nothing
     * @param reason     why, for a run; read only once it is to be
     *                   recorded
     */
    public record Request(boolean status, boolean cancel, UUID runId,
            List<UUID> itemIds, Long seq, String madeBy, Instant landedFrom,
            Instant landedTo, String intake, boolean dryRun,
            Given<String> reason)
    {
    }

    /** What was found or done. */
    public sealed interface Result permits Status, Cancelled, Chosen
    {
    }

    /**
     * A run, object by object.
     *
     * @param run     the run
     * @param objects its objects
     * @param counts  its objects counted by outcome, or else state
     */
    public record Status(ReprocessStore.Run run,
            List<ReprocessStore.Listed> objects, Map<String, Integer> counts)
            implements Result
    {
    }

    /**
     * A run's objects cancelled.
     *
     * @param runId     the run
     * @param cancelled how many were cancelled
     */
    public record Cancelled(UUID runId, int cancelled) implements Result
    {
    }

    /**
     * The objects chosen, and the run recorded for them.
     *
     * @param stage    the stage
     * @param selector what they were chosen by
     * @param chosen   the objects
     * @param pending  how many operators' corrections are pending at their
     *                 places
     * @param runId    the run recorded; null for a dry run or for nothing
     *                 chosen
     */
    public record Chosen(String stage, String selector,
            List<ReprocessStore.Chosen> chosen, int pending, UUID runId)
            implements Result
    {
    }

    /** Creates the command. */
    public ReprocessCommand()
    {
    }

    @Override
    public String name()
    {
        return "reprocess";
    }

    @Override
    public Permission permission()
    {
        return Permission.PIPELINE_OPERATE;
    }

    /**
     * A run's status needs only a viewer's permission; anything else, an
     * operator's.
     *
     * @param request the request
     * @return the permission
     */
    @Override
    public Set<Permission> permissions(Request request)
    {
        return EnumSet.of(request.status() ? Permission.PIPELINE_VIEW
                : Permission.PIPELINE_OPERATE);
    }

    @Override
    public Kind kind()
    {
        return Kind.LONG;
    }

    @Override
    public String usage()
    {
        return """
                reprocess (--item-id <id>[,<id>...] [--seq <n>]
                           | --made-by <version> [--item-id <id>[,...]]
                           | --landed-from <time> --landed-to <time>
                             [--intake <name>] [--item-id <id>[,...]])
                          [--dry-run]
                reprocess --status --run <id>
                reprocess --cancel --run <id>
                    Processes again, under the version running now, the
                    objects of the stage's input chosen: the current piece
                    at each place of the items given (--seq for one), or
                    those whose current output here was made by a version
                    (--made-by). At the split stage each is a whole item,
                    its next generation cut from the item as landed, and
                    --seq is refused; there items landed in a range of
                    time (ISO-8601, the end excluded) may be chosen too.
                    Elsewhere each is the next revision of its place,
                    stored and sent on as any record, saying it was
                    reprocessed; a place that holds something newer by the
                    time it is queued is refused. The reason is read from
                    standard input and kept encrypted; the run's ID is
                    printed. --dry-run records nothing and needs no reason.
                    --status lists a run, object by object; --cancel
                    cancels what of it isn't yet queued.""";
    }

    @Override
    public Set<String> options()
    {
        return Set.of("item-id", "seq", "made-by", "run", "landed-from",
                "landed-to", "intake");
    }

    @Override
    public Set<String> flags()
    {
        return Set.of("dry-run", "status", "cancel");
    }

    @Override
    public Request bind(AdminArgs arguments, C context)
    {
        return new Request(arguments.flag("status"),
                arguments.flag("cancel"),
                arguments.option("run").map(ReprocessCommand::runId)
                        .orElse(null),
                itemIds(arguments.option("item-id").orElse(null)),
                arguments.option("seq").map(FailureCommands::seq)
                        .orElse(null),
                arguments.option("made-by").orElse(null),
                arguments.option("landed-from")
                        .map(text -> instant(text, "--landed-from"))
                        .orElse(null),
                arguments.option("landed-to")
                        .map(text -> instant(text, "--landed-to"))
                        .orElse(null),
                arguments.option("intake").orElse(null),
                arguments.flag("dry-run"),
                () -> FailureCommands.reason(context.reasonInput()));
    }

    @Override
    public Result run(Request request, C context, Invocation invocation)
            throws Exception
    {
        AuditRecord audit = invocation.audit();
        audit.add(Attr.STAGE, context.stage());
        if (request.status() || request.cancel())
        {
            if (request.status() && request.cancel() || request.dryRun()
                    || !request.itemIds().isEmpty() || request.seq() != null
                    || request.madeBy() != null || landed(request))
            {
                throw new AdminArgs.UsageException("--status and --cancel"
                        + " take --run alone");
            }
            if (request.runId() == null)
            {
                throw new AdminArgs.UsageException("--run is required");
            }
            audit.add(Attr.RUN_ID, request.runId());
            if (request.status())
            {
                return status(context.reprocessRuns(), request.runId());
            }
            return cancel(context.reprocessRunsToChange(), request.runId(),
                    audit);
        }
        if (request.runId() != null)
        {
            throw new AdminArgs.UsageException("--run is for --status and"
                    + " --cancel");
        }
        return choose(request, context, audit);
    }

    @Override
    public void print(Result result, PrintStream out)
    {
        switch (result)
        {
            case Status status -> print(status, out);
            case Cancelled cancelled -> out.println("Run "
                    + cancelled.runId() + ": " + cancelled.cancelled()
                    + " object(s) not yet queued cancelled; any queued are"
                    + " processed");
            case Chosen chosen -> print(chosen, out);
        }
    }

    /**
     * Chooses the objects, and records a run for them unless it is a dry
     * run.
     *
     * @param request the request
     * @param context the stage
     * @param audit   the audit record
     * @return what was chosen, and the run
     * @throws Exception if the input, the registry or the reason can't be
     *                   read, or the run can't be recorded
     */
    private Chosen choose(Request request, C context, AuditRecord audit)
            throws Exception
    {
        List<UUID> itemIds = request.itemIds();
        String madeBy = request.madeBy();
        Long seq = request.seq();
        Landed landed = landedRange(request);
        if (context.wholeItems() && seq != null)
        {
            throw new AdminArgs.UsageException("--seq is refused at the "
                    + context.stage() + " stage: a reprocess there is of"
                    + " the whole item, its next generation");
        }
        if (landed != null && madeBy != null)
        {
            throw new AdminArgs.UsageException("Choose by --made-by or by"
                    + " landing time, not both");
        }
        if (itemIds.isEmpty() && madeBy == null && landed == null)
        {
            throw new AdminArgs.UsageException("Choose by --item-id,"
                    + " --made-by or --landed-from and --landed-to");
        }
        if (seq != null && (itemIds.size() != 1 || madeBy != null
                || landed != null))
        {
            throw new AdminArgs.UsageException("--seq takes one --item-id"
                    + " and no --made-by");
        }
        if (seq != null && seq < 0)
        {
            throw new AdminArgs.UsageException("--seq must be a record"
                    + " number: a whole item is chosen by --item-id alone");
        }
        if (madeBy != null && !VERSION.matcher(madeBy).matches())
        {
            throw new AdminArgs.UsageException("--made-by must be a version"
                    + " as the stage stores it");
        }
        String selector = selector(itemIds, seq, madeBy, landed);
        audit.add(Attr.SELECTOR, selector);
        List<ReprocessStore.Chosen> chosen = chosen(context, itemIds, seq,
                madeBy, landed);
        audit.add(Attr.COUNT, chosen.size());
        ReprocessStore runs = request.dryRun() ? context.reprocessRuns()
                : context.reprocessRunsToChange();
        int pending = runs.pendingCorrections(chosen);
        if (chosen.isEmpty())
        {
            return new Chosen(context.stage(), selector, chosen, pending,
                    null);
        }
        if (request.dryRun())
        {
            audit.add(Attr.STATE, "dry_run");
            return new Chosen(context.stage(), selector, chosen, pending,
                    null);
        }
        String text = request.reason().get();
        KeyProvider keys = context.reasonKeys();
        String kek = context.reasonKek();
        UUID runId = runs.record(selector, audit.operator(),
                context.version(), id -> FailureReason.seal(keys, kek, id,
                        REASON_SEQ, REASON_EVENT, text), chosen);
        audit.add(Attr.RUN_ID, runId);
        audit.add(Attr.STATE, ReprocessStore.RunState.PENDING.name());
        return new Chosen(context.stage(), selector, chosen, pending, runId);
    }

    /**
     * Prints what was chosen: nothing, what a dry run would reprocess, or
     * the run recorded.
     *
     * @param chosen what was chosen
     * @param out    where to
     */
    private static void print(Chosen chosen, PrintStream out)
    {
        List<ReprocessStore.Chosen> objects = chosen.chosen();
        if (objects.isEmpty())
        {
            out.println("Nothing chosen: no current piece of the input at"
                    + " the places given");
            return;
        }
        if (chosen.runId() == null)
        {
            out.println("Would reprocess " + objects.size() + " object(s) at"
                    + " the " + chosen.stage() + " stage ("
                    + chosen.selector() + "):");
            for (int index = 0; index < objects.size()
                    && index < MAX_PRINTED; index++)
            {
                out.println("  " + line(objects.get(index)));
            }
            if (objects.size() > MAX_PRINTED)
            {
                out.println("  ... and " + (objects.size() - MAX_PRINTED)
                        + " more");
            }
            warn(chosen.pending(), out);
            return;
        }
        out.println("Run " + chosen.runId() + ": " + objects.size()
                + " object(s) to reprocess at the " + chosen.stage()
                + " stage, queued by its pods within a renewal interval; the"
                + " reason kept encrypted. See reprocess --status --run "
                + chosen.runId());
        warn(chosen.pending(), out);
    }

    /**
     * The objects chosen: the current piece at each place of the items
     * given, or at each place whose current output was made by the
     * version.
     *
     * @param context the stage
     * @param itemIds the items; empty for any, with a version
     * @param seq     one record number; null for all
     * @param madeBy  the version; null for any
     * @param landed  the landing times; null for any
     * @return the objects, by item then record number
     * @throws Exception if the input or the registry can't be read
     */
    private List<ReprocessStore.Chosen> chosen(C context, List<UUID> itemIds,
            Long seq, String madeBy, Landed landed) throws Exception
    {
        ReprocessInputs inputs = context.reprocessInputs();
        Map<UUID, Set<Long>> places = new LinkedHashMap<>();
        if (madeBy != null)
        {
            places.putAll(context.madeBy(madeBy, itemIds, MAX_OBJECTS + 1));
        }
        else if (landed != null)
        {
            Set<UUID> narrowed = new LinkedHashSet<>(itemIds);
            for (UUID itemId : context.landedBetween(landed.intake(),
                    landed.from(), landed.to(), MAX_OBJECTS + 1))
            {
                if (narrowed.isEmpty() || narrowed.contains(itemId))
                {
                    places.put(itemId, null);
                }
            }
        }
        else
        {
            for (UUID itemId : itemIds)
            {
                places.put(itemId, null);
            }
        }
        List<ReprocessStore.Chosen> chosen = new ArrayList<>();
        for (Map.Entry<UUID, Set<Long>> item : places.entrySet())
        {
            for (ReprocessInputs.Input input
                    : inputs.current(item.getKey()))
            {
                long at = input.chosen().seq();
                if (input.failed() || seq != null && at != seq
                        || item.getValue() != null
                                && !item.getValue().contains(at))
                {
                    continue;
                }
                chosen.add(input.chosen());
            }
            if (chosen.size() > MAX_OBJECTS)
            {
                throw new AdminArgs.UsageException("More than " + MAX_OBJECTS
                        + " objects chosen: narrow it with --item-id");
            }
        }
        return chosen;
    }

    /**
     * Reads a run, object by object.
     *
     * @param runs  the stage's runs
     * @param runId the run
     * @return the run
     */
    private static Status status(ReprocessStore runs, UUID runId)
    {
        ReprocessStore.Run run = runs.run(runId).orElseThrow(
                () -> new IllegalStateException("No run " + runId
                        + " at this stage"));
        List<ReprocessStore.Listed> objects = runs.objects(runId);
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (ReprocessStore.Listed each : objects)
        {
            counts.merge(each.outcome() != null ? each.outcome()
                    : each.state().name(), 1, Integer::sum);
        }
        return new Status(run, objects, counts);
    }

    /**
     * Prints a run, object by object.
     *
     * @param status the run
     * @param out    where to
     */
    private static void print(Status status, PrintStream out)
    {
        ReprocessStore.Run run = status.run();
        out.println("Run " + run.runId() + " " + run.state() + ": "
                + run.selector() + ", by " + run.operator() + " at "
                + run.askedAt() + " under " + run.version()
                + (run.doneAt() == null ? "" : ", ended " + run.doneAt()));
        out.println("  " + status.objects().size() + " object(s): "
                + status.counts());
        for (ReprocessStore.Listed each : status.objects())
        {
            StringBuilder line = new StringBuilder("  ")
                    .append(line(each.chosen())).append(' ')
                    .append(each.state());
            if (each.line() != null)
            {
                line.append(", as ").append(each.line());
            }
            if (each.reason() != null)
            {
                line.append(": ").append(each.reason());
            }
            if (each.outcome() != null)
            {
                line.append(": ").append(each.outcome());
            }
            if (each.sameOutput() != null)
            {
                line.append(each.sameOutput() ? ", the same output"
                        : ", a different output");
            }
            out.println(line);
        }
    }

    /**
     * Cancels what of a run isn't yet queued.
     *
     * @param runs  the stage's runs
     * @param runId the run
     * @param audit the audit record
     * @return what was cancelled
     */
    private static Cancelled cancel(ReprocessStore runs, UUID runId,
            AuditRecord audit)
    {
        if (runs.run(runId).isEmpty())
        {
            throw new IllegalStateException("No run " + runId
                    + " at this stage");
        }
        int cancelled = runs.cancel(runId);
        audit.add(Attr.COUNT, cancelled);
        return new Cancelled(runId, cancelled);
    }

    /**
     * Warns of operators' corrections pending at the places chosen.
     *
     * @param pending how many
     * @param out     where to
     */
    private static void warn(int pending, PrintStream out)
    {
        if (pending > 0)
        {
            out.println("warning: " + pending + " correction(s) pending at"
                    + " these places: of a correction and a reprocess, the"
                    + " one processed after supersedes the other");
        }
    }

    /**
     * An object, as printed: its place, lineage and no content.
     *
     * @param chosen the object
     * @return the text
     */
    private static String line(ReprocessStore.Chosen chosen)
    {
        return chosen.itemId() + " seq " + chosen.seq() + " "
                + chosen.input();
    }

    /**
     * A range of landing times chosen.
     *
     * @param intake the intake given; null for the stage's own
     * @param from   the first instant, included
     * @param to     the last instant, excluded
     */
    private record Landed(String intake, Instant from, Instant to)
    {
    }

    /**
     * Whether a request chooses by landing time at all.
     *
     * @param request the request
     * @return true if it gives any of the landing options
     */
    private static boolean landed(Request request)
    {
        return request.landedFrom() != null || request.landedTo() != null
                || request.intake() != null;
    }

    /**
     * The landing times chosen by, checked.
     *
     * @param request the request
     * @return the times; null if none are given
     * @throws AdminArgs.UsageException if they don't make a range, or the
     *                                  intake isn't a name
     */
    private static Landed landedRange(Request request)
    {
        if (!landed(request))
        {
            return null;
        }
        Instant start = request.landedFrom();
        Instant end = request.landedTo();
        if (start == null || end == null)
        {
            throw new AdminArgs.UsageException("--landed-from and"
                    + " --landed-to go together, and --intake with them");
        }
        if (!start.isBefore(end))
        {
            throw new AdminArgs.UsageException("--landed-from must be before"
                    + " --landed-to");
        }
        String intake = request.intake();
        if (intake != null && !VERSION.matcher(intake).matches())
        {
            throw new AdminArgs.UsageException("--intake must be an"
                    + " intake's name");
        }
        return new Landed(intake, start, end);
    }

    /**
     * Reads an instant.
     *
     * @param text   the instant as typed, ISO-8601 with its offset
     * @param option its option, for the message
     * @return the instant
     * @throws AdminArgs.UsageException if it isn't one
     */
    private static Instant instant(String text, String option)
    {
        try
        {
            return OffsetDateTime.parse(text).toInstant();
        }
        catch (DateTimeParseException notTime)
        {
            throw new AdminArgs.UsageException(option + " must be an"
                    + " ISO-8601 time with its offset, such as"
                    + " 2026-11-09T00:00:00Z");
        }
    }

    /**
     * What a run chose by, as kept with it: item IDs, a number, a version,
     * an intake and times only.
     *
     * @param itemIds the items
     * @param seq     the record number, or null
     * @param madeBy  the version, or null
     * @param landed  the landing times, or null
     * @return the text
     */
    private static String selector(List<UUID> itemIds, Long seq,
            String madeBy, Landed landed)
    {
        StringBuilder text = new StringBuilder();
        if (madeBy != null)
        {
            text.append("--made-by ").append(madeBy);
        }
        if (landed != null)
        {
            text.append("--landed-from ").append(landed.from())
                    .append(" --landed-to ").append(landed.to());
            if (landed.intake() != null)
            {
                text.append(" --intake ").append(landed.intake());
            }
        }
        if (!itemIds.isEmpty())
        {
            if (!text.isEmpty())
            {
                text.append(' ');
            }
            text.append("--item-id ");
            for (int index = 0; index < itemIds.size(); index++)
            {
                text.append(index == 0 ? "" : ",").append(itemIds.get(index));
            }
        }
        if (seq != null)
        {
            text.append(" --seq ").append(seq);
        }
        return text.toString();
    }

    /**
     * Reads item IDs, separated by commas.
     *
     * @param text the IDs as typed; null for none
     * @return the IDs, each once, in order
     * @throws AdminArgs.UsageException if one isn't a UUID
     */
    private static List<UUID> itemIds(String text)
    {
        Set<UUID> ids = new LinkedHashSet<>();
        if (text != null)
        {
            for (String id : text.split(",", -1))
            {
                ids.add(FailureCommands.itemId(id.strip()));
            }
        }
        return List.copyOf(ids);
    }

    /**
     * Reads a run's ID.
     *
     * @param text the ID as typed
     * @return the ID
     * @throws AdminArgs.UsageException if it isn't a UUID
     */
    private static UUID runId(String text)
    {
        try
        {
            return UUID.fromString(text);
        }
        catch (IllegalArgumentException notUuid)
        {
            throw new AdminArgs.UsageException("--run must be a UUID");
        }
    }
}

