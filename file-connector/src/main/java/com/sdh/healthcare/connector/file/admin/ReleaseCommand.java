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
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.FailureCommands;
import java.io.PrintStream;
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
final class ReleaseCommand implements AdminCommand<SplitAdminContext>
{

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
    public void run(AdminArgs arguments, SplitAdminContext context,
            AuditRecord audit, PrintStream out) throws Exception
    {
        UUID itemId = FailureCommands.itemId(arguments.required("item-id"));
        boolean resume = arguments.flag("resume");
        boolean close = arguments.flag("close");
        if (resume == close)
        {
            throw new AdminArgs.UsageException(
                    "Give one of --resume and --close");
        }
        HoldReleases.Action action = resume ? HoldReleases.Action.RESUME
                : HoldReleases.Action.CLOSE;
        audit.add(Attr.ITEM_ID, itemId);
        audit.add(Attr.ACTION, action.name());
        String reason = close ? FailureCommands.reason(context.reasonInput())
                : null;
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
        out.println("requested " + action.name().toLowerCase(
                java.util.Locale.ROOT) + " of " + itemId + ", held under "
                + checkpoint.version() + " (request " + number
                + (reason == null ? "" : ", the reason kept encrypted")
                + "); the stage's pods apply it within a renewal interval:"
                + " see status --item " + itemId);
    }
}
