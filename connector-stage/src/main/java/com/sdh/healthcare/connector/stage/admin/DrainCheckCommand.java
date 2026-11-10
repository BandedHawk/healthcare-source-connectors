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
        implements AdminCommand<C>
{

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
    public void run(AdminArgs arguments, C context, AuditRecord audit,
            PrintStream out) throws Exception
    {
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
        out.println("Stage " + context.stage() + ", this release "
                + mine.digest() + ":");
        for (String version : mine.versions())
        {
            out.println("  " + version);
        }
        StageReleases.LetIn[] both = releases.currentAndPrevious();
        out.println("Current: " + letIn(both[0]));
        out.println("Previous: " + letIn(both[1]));
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
        print(context.partWay().partWay(DrainGate.FIRST_ITEMS), out);
        out.println("Waiting on an operator (never holds a release back):");
        List<FailureStore.OpenByVersion> open =
                context.failures().openByVersion();
        if (open.isEmpty())
        {
            out.println("  no open failures");
        }
        for (FailureStore.OpenByVersion each : open)
        {
            out.println("  " + each.count() + " " + each.state().name()
                    .toLowerCase(Locale.ROOT) + " failure(s) under "
                    + each.version());
        }
        List<PartWay.Items> held = context.held(DrainGate.FIRST_ITEMS);
        if (!held.isEmpty())
        {
            out.println("  held items:");
            print(held, out);
        }
        out.println("  " + context.reprocessRuns().waiting()
                + " reprocess object(s) waiting");
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
