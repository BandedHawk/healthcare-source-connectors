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
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.stage.StageReleases;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;

/**
 * {@code version}: the releases let in at the stage, newest first; with
 * {@code --accept}, lets this release in although items are part way under
 * versions it wouldn't stamp (STAGES.md §14.4a, rule 5). The reason is
 * read from standard input and kept encrypted; the acceptance is audited
 * {@code stage.version_forced}.
 *
 * @param <C> the stage's context
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class VersionCommand<C extends DrainAdmin>
        implements AdminCommand<C>
{

    /** How many releases are listed. */
    static final int LISTED = 20;

    /** The record number an acceptance's reason is sealed with. */
    private static final long REASON_SEQ = 0;

    /** The event number an acceptance's reason is sealed with. */
    private static final int REASON_EVENT = 1;

    @Override
    public String name()
    {
        return "version";
    }

    @Override
    public String usage()
    {
        return """
                version [--accept < reason.txt]
                    Lists the releases let in at the stage, newest first,
                    and this one. --accept lets this release in although
                    items are part way under versions it wouldn't stamp:
                    a held pod of it starts within its check interval. The
                    reason is read from standard input and kept encrypted.
                    Run it where the held pod's settings are, as in the
                    held pod itself.""";
    }

    @Override
    public Set<String> options()
    {
        return Set.of();
    }

    @Override
    public Set<String> flags()
    {
        return Set.of("accept");
    }

    @Override
    public void run(AdminArgs arguments, C context, AuditRecord audit,
            PrintStream out) throws Exception
    {
        audit.add(Attr.STAGE, context.stage());
        StageReleases.Release mine = StageReleases.Release.of(
                context.releaseVersions());
        audit.add(Attr.RELEASE, mine.digest());
        if (arguments.flag("accept"))
        {
            accept(context, mine, audit, out);
            return;
        }
        out.println("This release: " + mine.digest() + " ("
                + mine.versions().size() + " version(s))");
        for (String version : mine.versions())
        {
            out.println("  " + version);
        }
        List<StageReleases.LetIn> history = context.stageReleases()
                .history(LISTED);
        out.println("Let in, newest first:");
        if (history.isEmpty())
        {
            out.println("  none");
        }
        for (StageReleases.LetIn letIn : history)
        {
            out.println("  " + DrainCheckCommand.letIn(letIn));
        }
        context.stageReleases().accepted(mine).ifPresent(accepted ->
                out.println("This release was accepted by "
                        + accepted.operator() + " at " + accepted.at()));
    }

    /**
     * Accepts this release, unless it is the current one.
     *
     * @param context the stage
     * @param mine    this release
     * @param audit   the command's audit event
     * @param out     where to report
     * @throws Exception if the reason can't be read or sealed, or the
     *                   database can't be reached
     */
    private void accept(C context, StageReleases.Release mine,
            AuditRecord audit, PrintStream out) throws Exception
    {
        StageReleases.LetIn current = context.stageReleases()
                .currentAndPrevious()[0];
        if (current != null
                && current.release().digest().equals(mine.digest()))
        {
            throw new AdminArgs.UsageException("This release, "
                    + mine.digest() + ", is the stage's current one already");
        }
        String text = FailureCommands.reason(context.reasonInput());
        KeyProvider keys = context.reasonKeys();
        String kek = context.reasonKek();
        StageReleases.Accepted accepted = context.stageReleasesToChange()
                .accept(mine, audit.operator(), id -> FailureReason.seal(
                        keys, kek, id, REASON_SEQ, REASON_EVENT, text));
        Telemetry.get().auditWarn("stage.version_forced", audit.fields()
                .with(Attr.VERSION_COUNT, mine.versions().size()));
        out.println("Release " + mine.digest() + " accepted ("
                + accepted.acceptId() + "), the reason kept encrypted: a"
                + " held pod of it is let in within its check interval");
    }
}
