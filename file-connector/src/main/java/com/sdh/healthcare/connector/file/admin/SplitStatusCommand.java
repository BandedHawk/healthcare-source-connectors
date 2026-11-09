/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.HoldReleases;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.SplitItemReport;
import com.sdh.healthcare.connector.core.pipeline.IngestPipeline;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.StageStatusCommand;
import com.sdh.healthcare.connector.stage.admin.StatusParts;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * {@code status} for the split stage (STAGES.md §8): its duties lease, its
 * items by state, and those not yet finished with who holds each one's
 * lease; or, with {@code --item}, one item's checkpoint. The overview reads
 * only the lease database; an item's checkpoint also needs the ID key of
 * {@code split_records}, so the keys and the catalog.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class SplitStatusCommand implements AdminCommand<SplitAdminContext>
{

    /** The most unfinished items listed. */
    static final int LISTED = 20;

    /** Creates the command. */
    SplitStatusCommand()
    {
    }

    @Override
    public String name()
    {
        return "status";
    }

    @Override
    public String usage()
    {
        return """
                status [--item <id>]
                    Shows the duties lease, the items by state, and those
                    not yet finished with who holds each one's lease; with
                    --item, one item's checkpoint: how far it got, and why
                    it failed. Reads only: takes no lease and changes
                    nothing.""";
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
    public void run(AdminArgs arguments, SplitAdminContext context,
            AuditRecord audit, PrintStream out) throws Exception
    {
        audit.add(Attr.STAGE, ItemProcessor.STAGE);
        StatusParts status = new StatusParts(out);
        status.show("duties", () -> StatusParts.lease(context.leases(),
                ItemProcessor.STAGE + "/" + context.properties().connectorId()
                        + "/duties"));
        if (arguments.option("item").isPresent())
        {
            UUID itemId = StageStatusCommand.itemId(
                    arguments.required("item"));
            audit.add(Attr.ITEM_ID, itemId);
            status.show("lease", () -> StatusParts.lease(context.leases(),
                    itemLease(context, itemId)));
            status.show("checkpoint", () -> checkpoint(
                    context.checkpoints(), itemId, audit));
            status.show("releases", () -> releases(context.releases(),
                    itemId));
        }
        else
        {
            status.show("items", () -> items(context, audit));
        }
        status.end();
    }

    /**
     * An item's lease name.
     *
     * @param context the admin context
     * @param itemId  the item
     * @return the name
     */
    private static String itemLease(SplitAdminContext context, UUID itemId)
    {
        return ItemProcessor.STAGE + "/" + context.properties().connectorId()
                + "/" + itemId;
    }

    /**
     * The items by state, and those not yet finished.
     *
     * @param context the admin context
     * @param audit   the audit event, given how many aren't finished
     * @return the text to show
     */
    private static String items(SplitAdminContext context, AuditRecord audit)
    {
        SplitItemReport report = context.items();
        Map<String, Long> counts = report.countsByState();
        if (counts.isEmpty())
        {
            audit.add(Attr.COUNT, 0);
            return "none (no item split yet)";
        }
        long unfinished = counts.entrySet().stream()
                .filter(count -> !count.getKey().equals("COMPLETED")
                        && !count.getKey().equals("FAILED"))
                .mapToLong(Map.Entry::getValue).sum();
        audit.add(Attr.COUNT, unfinished);
        StringBuilder text = new StringBuilder(counts.entrySet().stream()
                .map(count -> count.getValue() + " " + count.getKey())
                .collect(Collectors.joining(", ")));
        List<SplitItemReport.Unfinished> listed = report.unfinished(LISTED);
        for (SplitItemReport.Unfinished item : listed)
        {
            text.append(StatusParts.more(item.itemId() + " " + item.state()
                    + ", epoch " + item.epoch() + ", last written "
                    + item.updatedAt()));
            text.append(StatusParts.more("  lease " + StatusParts.lease(
                    context.leases(), itemLease(context, item.itemId()))));
        }
        if (unfinished > listed.size())
        {
            text.append(StatusParts.more("and " + (unfinished - listed.size())
                    + " more; --item <id> shows one"));
        }
        return text.toString();
    }

    /**
     * An item's checkpoint: how far it got, and how it ended.
     *
     * @param checkpoints the stage's checkpoints
     * @param itemId      the item
     * @param audit       the audit event, given the item's state
     * @return the text to show
     */
    private static String checkpoint(ItemCheckpoints checkpoints, UUID itemId,
            AuditRecord audit)
    {
        ItemCheckpoints.Stored stored = checkpoints.load(itemId).orElse(null);
        if (stored == null)
        {
            return "none: the stage hasn't started this item";
        }
        Checkpoint checkpoint = stored.checkpoint();
        audit.add(Attr.STATE, checkpoint.state());
        StringBuilder text = new StringBuilder();
        text.append(checkpoint.state()).append(" (")
                .append(checkpoint.format()).append(")");
        if (checkpoint.state() == Checkpoint.State.SPLITTING)
        {
            long at = checkpoint.resumeAt().offset();
            text.append(String.format(", %.1f%% split (offset %d of %d bytes)",
                    checkpoint.itemSize() == 0 ? 100.0
                            : 100.0 * at / checkpoint.itemSize(),
                    at, checkpoint.itemSize()));
        }
        else
        {
            text.append(", ").append(checkpoint.itemSize()).append(" bytes");
        }
        text.append(", records committed ")
                .append(checkpoint.recordCount()).append(", sent ")
                .append(checkpoint.publishedSeq() + 1).append(", started ")
                .append(checkpoint.startedAt());
        if (stored.endedAt() != null)
        {
            text.append(", ended ").append(stored.endedAt());
        }
        if (checkpoint.failure() != null)
        {
            text.append(StatusParts.more("stage "
                    + checkpoint.failure().stage() + ", "
                    + checkpoint.failure().code() + " at offset "
                    + checkpoint.failure().offset() + ", at "
                    + checkpoint.failure().failedAt()));
        }
        if (stored.pending() != null)
        {
            text.append(StatusParts.more("a batch was being committed: the"
                    + " next worker to take the item resolves it"));
        }
        if (checkpoint.state().inProgress())
        {
            text.append(StatusParts.more("in progress: a worker resumes it"
                    + " when its item.landed is delivered again"));
        }
        if (checkpoint.state() == Checkpoint.State.SPLITTING
                && checkpoint.restarts() > 0)
        {
            text.append(StatusParts.more("restarted "
                    + checkpoint.restarts() + " time(s) in a row without"
                    + " progress; at " + IngestPipeline.FAIL_AFTER_RESTARTS
                    + " the item fails"));
        }
        return text.toString();
    }

    /**
     * An item's requests to release it from a hold (STAGES.md §15.6e),
     * without their reasons.
     *
     * @param releases the release store
     * @param itemId   the item
     * @return one line per request, or none
     */
    private static String releases(HoldReleases releases, UUID itemId)
    {
        List<HoldReleases.Listed> listed = releases.list(itemId);
        if (listed.isEmpty())
        {
            return "none";
        }
        StringBuilder text = new StringBuilder(listed.size() + " request(s)");
        for (HoldReleases.Listed each : listed)
        {
            HoldReleases.Request request = each.request();
            text.append(StatusParts.more(request.request() + " "
                    + request.action() + " by " + request.requestedBy()
                    + " at " + request.requestedAt() + ": " + each.state()
                    + (each.doneAt() == null ? "" : " at " + each.doneAt())
                    + (each.fromVersion() == null ? ""
                            : ", held under " + each.fromVersion())
                    + (each.toVersion() == null ? ""
                            : ", resumed under " + each.toVersion())
                    + (each.refusal() == null ? "" : ", " + each.refusal())
                    + (each.reasoned() ? " (reason kept, encrypted)" : "")));
        }
        return text.toString();
    }
}
