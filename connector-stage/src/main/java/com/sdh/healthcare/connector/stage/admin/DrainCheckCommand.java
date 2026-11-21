/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.stage.DrainGate;
import com.sdh.healthcare.connector.stage.PartWay;
import com.sdh.healthcare.connector.stage.StageReleases;
import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * {@code drain-check}: whether a pod with these settings would be let in
 * now, and why (STAGES.md §14.4a), changing nothing. It prints this
 * release, the current and previous ones, the items part way by version,
 * and the work waiting on an operator, which never holds a release back.
 *
 * @param <C> the stage's context
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class DrainCheckCommand<C extends DrainAdmin>
        implements CliOperation<C, DrainCheckCommand.Request,
                DrainCheckCommand.Result>,
        Answering<DrainCheckCommand.Result>
{

    /** What is asked: nothing but the check. */
    public record Request()
    {
    }

    /**
     * What was found.
     *
     * @param stage    the stage
     * @param mine     this release
     * @param decision whether a pod of it would be let in now
     * @param current  the stage's current release; null if none
     * @param previous its previous release; null if none
     * @param partWay  the items part way, by version
     * @param open     the open failures, by version and state
     * @param held     the items held, by version
     * @param waiting  how many reprocess objects wait to be queued
     */
    public record Result(String stage, StageReleases.Release mine,
            DrainGate.Decision decision, StageReleases.LetIn current,
            StageReleases.LetIn previous, List<PartWay.Items> partWay,
            List<FailureStore.OpenByVersion> open, List<PartWay.Items> held,
            long waiting)
    {
    }

    /** Creates the command. */
    public DrainCheckCommand()
    {
    }

    @Override
    public String name()
    {
        return "drain-check";
    }

    @Override
    public String usage()
    {
        return """
                drain-check
                    Whether a pod of this release would be let in now: as
                    the stage's current or previous release, with each
                    version it adds one some item is part way under, or
                    with no item part way under a version it wouldn't
                    stamp. Lists the items part way, by version, and what
                    waits on an operator (open failures, held items,
                    queued reprocessing), which never holds a release
                    back. Changes nothing.""";
    }

    @Override
    public Set<String> options()
    {
        return Set.of();
    }

    @Override
    public Set<String> flags()
    {
        return Set.of();
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
    public Request bind(AdminArgs arguments, C context)
    {
        return new Request();
    }

    @Override
    public Result run(Request request, C context, Invocation invocation)
            throws Exception
    {
        AuditRecord audit = invocation.audit();
        audit.add(Attr.STAGE, context.stage());
        StageReleases releases = context.stageReleases();
        StageReleases.Release mine = StageReleases.Release.of(
                context.releaseVersions());
        audit.add(Attr.RELEASE, mine.digest());
        DrainGate.Decision decision;
        try (DrainGate gate = new DrainGate(context.stage(), mine, releases,
                context.partWay(), Duration.ofMinutes(1), -1))
        {
            decision = gate.decide();
        }
        audit.add(Attr.COUNT, decision.blockingCount());
        StageReleases.LetIn[] both = releases.currentAndPrevious();
        return new Result(context.stage(), mine, decision, both[0], both[1],
                context.partWay().partWay(DrainGate.FIRST_ITEMS),
                context.failures().openByVersion(),
                context.held(DrainGate.FIRST_ITEMS),
                context.reprocessRuns().waiting());
    }

    @Override
    public void print(Result result, PrintStream out)
    {
        StageReleases.Release mine = result.mine();
        DrainGate.Decision decision = result.decision();
        out.println("Stage " + result.stage() + ", this release "
                + mine.digest() + ":");
        for (String version : mine.versions())
        {
            out.println("  " + version);
        }
        out.println("Current: " + letIn(result.current()));
        out.println("Previous: " + letIn(result.previous()));
        if (decision.letIn())
        {
            out.println("Would be let in: " + decision.how().wireName());
        }
        else
        {
            out.println("Held back: " + decision.blockingCount()
                    + " item(s) part way under "
                    + decision.blocking().size() + " version(s) it wouldn't"
                    + " stamp; see version --accept to let it in anyway");
        }
        out.println("Part way:");
        print(result.partWay(), out);
        out.println("Waiting on an operator (never holds a release back):");
        if (result.open().isEmpty())
        {
            out.println("  no open failures");
        }
        for (FailureStore.OpenByVersion each : result.open())
        {
            out.println("  " + each.count() + " " + each.state().name()
                    .toLowerCase(Locale.ROOT) + " failure(s) under "
                    + each.version());
        }
        if (!result.held().isEmpty())
        {
            out.println("  held items:");
            print(result.held(), out);
        }
        out.println("  " + result.waiting() + " reprocess object(s) waiting");
    }

    @Override
    public Map<String, Object> answer(Result result)
    {
        DrainGate.Decision decision = result.decision();
        return Answers.object("stage", result.stage(), "release",
                Answers.release(result.mine()), "letIn", decision.letIn(),
                "how", decision.letIn() ? decision.how().wireName() : null,
                "blockingCount", decision.blockingCount(), "blocking",
                Answers.list(decision.blocking(), DrainCheckCommand::answer),
                "current", Answers.letIn(result.current()), "previous",
                Answers.letIn(result.previous()), "partWay",
                Answers.list(result.partWay(), DrainCheckCommand::answer),
                "openFailures", Answers.list(result.open(), each ->
                        Answers.object("state", each.state(), "version",
                                each.version(), "count", each.count())),
                "held", Answers.list(result.held(),
                        DrainCheckCommand::answer),
                "reprocessWaiting", result.waiting());
    }

    /**
     * Items by version, as the API answers them.
     *
     * @param items the items under one version
     * @return how many, and the first
     */
    private static Object answer(PartWay.Items items)
    {
        return Answers.object("version", items.version(), "count",
                items.count(), "first", items.first());
    }

    /**
     * Prints items by version.
     *
     * @param items the items
     * @param out   where to
     */
    private static void print(List<PartWay.Items> items, PrintStream out)
    {
        if (items.isEmpty())
        {
            out.println("  none");
        }
        for (PartWay.Items each : items)
        {
            String first = each.first().stream().map(UUID::toString)
                    .collect(Collectors.joining(", "));
            out.println("  " + each.count() + " under " + each.version()
                    + (each.count() > each.first().size()
                            ? ", first " : ": ") + first);
        }
    }

    /**
     * Describes a release let in.
     *
     * @param letIn the release; null if none
     * @return the description
     */
    static String letIn(StageReleases.LetIn letIn)
    {
        if (letIn == null)
        {
            return "none";
        }
        return letIn.release().digest() + ", let in " + letIn.how() + " at "
                + letIn.at() + " (" + letIn.release().versions().size()
                + " version(s))";
    }
}
