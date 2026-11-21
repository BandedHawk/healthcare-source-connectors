/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.HoldReleases;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.CliOperation;
import com.sdh.healthcare.connector.stage.admin.FailureCommands;
import com.sdh.healthcare.connector.stage.admin.Given;
import com.sdh.healthcare.connector.stage.admin.Invocation;
import com.sdh.healthcare.connector.stage.admin.Permission;
import java.io.PrintStream;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 * Ends the hold on an item held across versions of the split stage
 * (STAGES.md §14.3, §15.6e): records an operator's request, which the
 * stage's pods apply under the item's lease within a renewal interval.
 * {@code --resume} has the item split on from its checkpoint under the
 * version running now; {@code --close} ends it, {@code CLOSED}, with the
 * reason read from standard input, for an item landed again with landing's
 * {@code resubmit}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ReleaseCommand implements CliOperation<SplitAdminContext,
        ReleaseCommand.Request, ReleaseCommand.Result>
{

    /**
     * What is asked.
     *
     * @param itemId the item held
     * @param resume split it on from its checkpoint
     * @param close  end it, CLOSED
     * @param reason why, for closing; read only then
     */
    record Request(UUID itemId, boolean resume, boolean close,
            Given<String> reason)
    {
    }

    /**
     * What was requested.
     *
     * @param itemId   the item
     * @param action   resume or close
     * @param version  the version it is held under
     * @param number   the request's number
     * @param reasoned whether a reason was kept
     */
    record Result(UUID itemId, HoldReleases.Action action, String version,
            int number, boolean reasoned)
    {
    }

    /** Creates the command. */
    ReleaseCommand()
    {
    }

    @Override
    public String name()
    {
        return "release";
    }

    @Override
    public String usage()
    {
        return """
                release --item-id <id> --resume | --close < reason.txt
                    Ends the hold on an item HELD part way, begun under
                    another version of the split stage. The stage's pods
                    apply it within a renewal interval, under the item's
                    lease.
                    --resume  split it on from its checkpoint under the
                              version running now; its records keep the
                              version that made each
                    --close   end it, CLOSED, with the reason read from
                              standard input (kubectl exec -i), kept
                              encrypted: for an item landed again with
                              landing's resubmit. Nothing more is sent
                              for it; what was sent stays.""";
    }

    @Override
    public Set<String> options()
    {
        return Set.of("item-id");
    }

    @Override
    public Set<String> flags()
    {
        return Set.of("resume", "close");
    }

    @Override
    public Permission permission()
    {
        return Permission.PIPELINE_OPERATE;
    }

    @Override
    public Kind kind()
    {
        return Kind.ACTION;
    }

    @Override
    public Request bind(AdminArgs arguments, SplitAdminContext context)
    {
        return new Request(FailureCommands.itemId(
                arguments.required("item-id")), arguments.flag("resume"),
                arguments.flag("close"),
                () -> FailureCommands.reason(context.reasonInput()));
    }

    @Override
    public Result run(Request request, SplitAdminContext context,
            Invocation invocation) throws Exception
    {
        AuditRecord audit = invocation.audit();
        UUID itemId = request.itemId();
        boolean resume = request.resume();
        boolean close = request.close();
        if (resume == close)
        {
            throw new AdminArgs.UsageException(
                    "Give one of --resume and --close");
        }
        HoldReleases.Action action = resume ? HoldReleases.Action.RESUME
                : HoldReleases.Action.CLOSE;
        audit.add(Attr.ITEM_ID, itemId);
        audit.add(Attr.ACTION, action.name());
        String reason = close ? request.reason().get() : null;
        Checkpoint checkpoint = context.checkpoints().load(itemId)
                .orElseThrow(() -> new IllegalStateException(
                        "The item was never split"))
                .checkpoint();
        if (checkpoint.state() != Checkpoint.State.HELD)
        {
            throw new IllegalStateException("The item is "
                    + checkpoint.state() + ", not HELD");
        }
        int number;
        try
        {
            number = context.releasesToChange().request(itemId, action,
                    audit.operator(), reason, context.reasonKeys(),
                    context.reasonKek());
        }
        catch (HoldReleases.RefusedException refused)
        {
            throw new IllegalStateException(refused.getMessage());
        }
        audit.add(Attr.CORRECTION, number);
        return new Result(itemId, action, checkpoint.version(), number,
                reason != null);
    }

    @Override
    public void print(Result result, PrintStream out)
    {
        out.println("requested " + result.action().name().toLowerCase(
                Locale.ROOT) + " of " + result.itemId() + ", held under "
                + result.version() + " (request " + result.number()
                + (result.reasoned() ? ", the reason kept encrypted" : "")
                + "); the stage's pods apply it within a renewal interval:"
                + " see status --item " + result.itemId());
    }
}
