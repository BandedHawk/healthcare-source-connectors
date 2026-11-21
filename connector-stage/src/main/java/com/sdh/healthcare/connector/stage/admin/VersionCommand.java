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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        implements CliOperation<C, VersionCommand.Request,
                VersionCommand.Result>, Answering<VersionCommand.Result>
{

    /** How many releases are listed. */
    static final int LISTED = 20;

    /** The record number an acceptance's reason is sealed with. */
    private static final long REASON_SEQ = 0;

    /** The event number an acceptance's reason is sealed with. */
    private static final int REASON_EVENT = 1;

    /**
     * What is asked.
     *
     * @param accept whether to let this release in
     * @param reason why, for an acceptance; read only then
     */
    public record Request(boolean accept, Given<String> reason)
    {
    }

    /**
     * What was found, or done.
     *
     * @param mine          this release
     * @param history       the releases let in, newest first; empty for
     *                      an acceptance
     * @param acceptedEarly when this release was accepted before, if it
     *                      was; empty for an acceptance
     * @param accepted      the acceptance made; null for a listing
     */
    public record Result(StageReleases.Release mine,
            List<StageReleases.LetIn> history,
            Optional<StageReleases.Accepted> acceptedEarly,
            StageReleases.Accepted accepted)
    {
    }

    /** Creates the command. */
    public VersionCommand()
    {
    }

    @Override
    public String name()
    {
        return "version";
    }

    @Override
    public Permission permission()
    {
        return Permission.PIPELINE_OPERATE;
    }

    /**
     * Listing needs only a viewer's permission; accepting, an operator's.
     *
     * @param request the request
     * @return the permission
     */
    @Override
    public Set<Permission> permissions(Request request)
    {
        return EnumSet.of(request.accept() ? Permission.PIPELINE_OPERATE
                : Permission.PIPELINE_VIEW);
    }

    @Override
    public Kind kind()
    {
        return Kind.ACTION;
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
    public Request bind(AdminArgs arguments, C context)
    {
        return new Request(arguments.flag("accept"),
                () -> FailureCommands.reason(context.reasonInput()));
    }

    @Override
    public Result run(Request request, C context, Invocation invocation)
            throws Exception
    {
        AuditRecord audit = invocation.audit();
        audit.add(Attr.STAGE, context.stage());
        StageReleases.Release mine = StageReleases.Release.of(
                context.releaseVersions());
        audit.add(Attr.RELEASE, mine.digest());
        if (request.accept())
        {
            return new Result(mine, List.of(), Optional.empty(),
                    accept(context, mine, request.reason(), audit));
        }
        List<StageReleases.LetIn> history = context.stageReleases()
                .history(LISTED);
        return new Result(mine, history,
                context.stageReleases().accepted(mine), null);
    }

    @Override
    public void print(Result result, PrintStream out)
    {
        StageReleases.Release mine = result.mine();
        if (result.accepted() != null)
        {
            out.println("Release " + mine.digest() + " accepted ("
                    + result.accepted().acceptId() + "), the reason kept"
                    + " encrypted: a held pod of it is let in within its"
                    + " check interval");
            return;
        }
        out.println("This release: " + mine.digest() + " ("
                + mine.versions().size() + " version(s))");
        for (String version : mine.versions())
        {
            out.println("  " + version);
        }
        out.println("Let in, newest first:");
        if (result.history().isEmpty())
        {
            out.println("  none");
        }
        for (StageReleases.LetIn letIn : result.history())
        {
            out.println("  " + DrainCheckCommand.letIn(letIn));
        }
        result.acceptedEarly().ifPresent(accepted ->
                out.println("This release was accepted by "
                        + accepted.operator() + " at " + accepted.at()));
    }

    @Override
    public Map<String, Object> answer(Result result)
    {
        return Answers.object("release", Answers.release(result.mine()),
                "history", Answers.list(result.history(), Answers::letIn),
                "accepted", result.acceptedEarly().map(accepted ->
                        (Object) Answers.object("acceptId",
                                accepted.acceptId(), "operator",
                                accepted.operator(), "at", accepted.at()))
                        .orElse(null),
                "acceptedNow", result.accepted() == null ? null
                        : Answers.object("acceptId",
                                result.accepted().acceptId()));
    }

    /**
     * Accepts this release, unless it is the current one.
     *
     * @param context the stage
     * @param mine    this release
     * @param reason  why
     * @param audit   the command's audit event
     * @return the acceptance
     * @throws Exception if the reason can't be read or sealed, or the
     *                   database can't be reached
     */
    private StageReleases.Accepted accept(C context,
            StageReleases.Release mine, Given<String> reason,
            AuditRecord audit) throws Exception
    {
        StageReleases.LetIn current = context.stageReleases()
                .currentAndPrevious()[0];
        if (current != null
                && current.release().digest().equals(mine.digest()))
        {
            throw new AdminArgs.UsageException("This release, "
                    + mine.digest() + ", is the stage's current one already");
        }
        String text = reason.get();
        KeyProvider keys = context.reasonKeys();
        String kek = context.reasonKek();
        StageReleases.Accepted accepted = context.stageReleasesToChange()
                .accept(mine, audit.operator(), id -> FailureReason.seal(
                        keys, kek, id, REASON_SEQ, REASON_EVENT, text));
        Telemetry.get().auditWarn("stage.version_forced", audit.fields()
                .with(Attr.VERSION_COUNT, mine.versions().size()));
        return accepted;
    }
}
