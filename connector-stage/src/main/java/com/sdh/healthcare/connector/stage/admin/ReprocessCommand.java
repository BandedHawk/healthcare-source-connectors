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
import java.util.ArrayList;
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
        implements AdminCommand<C>
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

    @Override
    public String name()
    {
        return "reprocess";
    }

    @Override
    public String usage()
    {
        return """
                reprocess (--item-id <id>[,<id>...] [--seq <n>]
                           | --made-by <version> [--item-id <id>[,...]])
                          [--dry-run]
                reprocess --status --run <id>
                reprocess --cancel --run <id>
                    Processes again, under the version running now, the
                    objects of the stage's input chosen: the current piece
                    at each place of the items given (--seq for one), or
                    those whose current output here was made by a version
                    (--made-by). Each is the next revision of its place,
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
        return Set.of("item-id", "seq", "made-by", "run");
    }

    @Override
    public Set<String> flags()
    {
        return Set.of("dry-run", "status", "cancel");
    }

    @Override
    public void run(AdminArgs arguments, C context, AuditRecord audit,
            PrintStream out) throws Exception
    {
        audit.add(Attr.STAGE, context.stage());
        boolean status = arguments.flag("status");
        boolean cancel = arguments.flag("cancel");
        if (status || cancel)
        {
            if (status && cancel || arguments.flag("dry-run")
                    || arguments.option("item-id").isPresent()
                    || arguments.option("seq").isPresent()
                    || arguments.option("made-by").isPresent())
            {
                throw new AdminArgs.UsageException("--status and --cancel"
                        + " take --run alone");
            }
            UUID runId = runId(arguments.required("run"));
            audit.add(Attr.RUN_ID, runId);
            if (status)
            {
                status(context.reprocessRuns(), runId, out);
            }
            else
            {
                cancel(context.reprocessRunsToChange(), runId, audit, out);
            }
            return;
        }
        if (arguments.option("run").isPresent())
        {
            throw new AdminArgs.UsageException("--run is for --status and"
                    + " --cancel");
        }
        choose(arguments, context, audit, out);
    }

    /**
     * Chooses objects, and records a run of them unless it is a dry run.
     *
     * @param arguments the options
     * @param context   the stage
     * @param audit     the audit record
     * @param out       where results go
     * @throws Exception if the stage's input or runs can't be reached
     */
    private void choose(AdminArgs arguments, C context, AuditRecord audit,
            PrintStream out) throws Exception
    {
        List<UUID> itemIds = itemIds(arguments.option("item-id")
                .orElse(null));
        String madeBy = arguments.option("made-by").orElse(null);
        Long seq = arguments.option("seq").map(FailureCommands::seq)
                .orElse(null);
        if (itemIds.isEmpty() && madeBy == null)
        {
            throw new AdminArgs.UsageException("Choose by --item-id or"
                    + " --made-by");
        }
        if (seq != null && (itemIds.size() != 1 || madeBy != null))
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
        String selector = selector(itemIds, seq, madeBy);
        audit.add(Attr.SELECTOR, selector);
        List<ReprocessStore.Chosen> chosen = chosen(context, itemIds, seq,
                madeBy);
        audit.add(Attr.COUNT, chosen.size());
        ReprocessStore runs = arguments.flag("dry-run")
                ? context.reprocessRuns() : context.reprocessRunsToChange();
        int pending = runs.pendingCorrections(chosen);
        if (chosen.isEmpty())
        {
            out.println("Nothing chosen: no current piece of the input at"
                    + " the places given");
            return;
        }
        if (arguments.flag("dry-run"))
        {
            audit.add(Attr.STATE, "dry_run");
            out.println("Would reprocess " + chosen.size() + " object(s) at"
                    + " the " + context.stage() + " stage (" + selector
                    + "):");
            for (int index = 0; index < chosen.size()
                    && index < MAX_PRINTED; index++)
            {
                out.println("  " + line(chosen.get(index)));
            }
            if (chosen.size() > MAX_PRINTED)
            {
                out.println("  ... and " + (chosen.size() - MAX_PRINTED)
                        + " more");
            }
            warn(pending, out);
            return;
        }
        String text = FailureCommands.reason(context.reasonInput());
        KeyProvider keys = context.reasonKeys();
        String kek = context.reasonKek();
        UUID runId = runs.record(selector, audit.operator(),
                context.version(), id -> FailureReason.seal(keys, kek, id,
                        REASON_SEQ, REASON_EVENT, text), chosen);
        audit.add(Attr.RUN_ID, runId);
        audit.add(Attr.STATE, ReprocessStore.RunState.PENDING.name());
        out.println("Run " + runId + ": " + chosen.size() + " object(s) to"
                + " reprocess at the " + context.stage() + " stage, queued"
                + " by its pods within a renewal interval; the reason kept"
                + " encrypted. See reprocess --status --run " + runId);
        warn(pending, out);
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
     * @return the objects, by item then record number
     * @throws Exception if the input or the registry can't be read
     */
    private List<ReprocessStore.Chosen> chosen(C context, List<UUID> itemIds,
            Long seq, String madeBy) throws Exception
    {
        ReprocessInputs inputs = context.reprocessInputs();
        Map<UUID, Set<Long>> places = new LinkedHashMap<>();
        if (madeBy == null)
        {
            for (UUID itemId : itemIds)
            {
                places.put(itemId, null);
            }
        }
        else
        {
            List<Object[]> made = context.reprocessRuns().madeBy(madeBy,
                    itemIds, MAX_OBJECTS + 1);
            for (Object[] place : made)
            {
                places.computeIfAbsent((UUID) place[0],
                        item -> new LinkedHashSet<>()).add((Long) place[1]);
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
     * Prints a run, object by object.
     *
     * @param runs  the stage's runs
     * @param runId the run
     * @param out   where to
     */
    private static void status(ReprocessStore runs, UUID runId,
            PrintStream out)
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
        out.println("Run " + runId + " " + run.state() + ": "
                + run.selector() + ", by " + run.operator() + " at "
                + run.askedAt() + " under " + run.version()
                + (run.doneAt() == null ? "" : ", ended " + run.doneAt()));
        out.println("  " + objects.size() + " object(s): " + counts);
        for (ReprocessStore.Listed each : objects)
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
     * @param out   where to say what was done
     */
    private static void cancel(ReprocessStore runs, UUID runId,
            AuditRecord audit, PrintStream out)
    {
        if (runs.run(runId).isEmpty())
        {
            throw new IllegalStateException("No run " + runId
                    + " at this stage");
        }
        int cancelled = runs.cancel(runId);
        audit.add(Attr.COUNT, cancelled);
        out.println("Run " + runId + ": " + cancelled + " object(s) not yet"
                + " queued cancelled; any queued are processed");
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
                    + " these places: each, processed after, supersedes"
                    + " what the reprocess makes");
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
     * What a run chose by, as kept with it: item IDs, a number and a
     * version only.
     *
     * @param itemIds the items
     * @param seq     the record number, or null
     * @param madeBy  the version, or null
     * @return the text
     */
    private static String selector(List<UUID> itemIds, Long seq,
            String madeBy)
    {
        StringBuilder text = new StringBuilder();
        if (madeBy != null)
        {
            text.append("--made-by ").append(madeBy);
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
