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
import com.sdh.healthcare.connector.stage.admin.Answering;
import com.sdh.healthcare.connector.stage.admin.Answers;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.CliOperation;
import com.sdh.healthcare.connector.stage.admin.Invocation;
import com.sdh.healthcare.connector.stage.admin.Partial;
import com.sdh.healthcare.connector.stage.admin.Permission;
import com.sdh.healthcare.connector.stage.admin.StageStatusCommand;
import com.sdh.healthcare.connector.stage.admin.StatusParts;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
final class SplitStatusCommand implements CliOperation<SplitAdminContext,
        StageStatusCommand.Request, SplitStatusCommand.Result>,
        Answering<SplitStatusCommand.Result>
{

    /** The most unfinished items listed. */
    static final int LISTED = 20;

    /** Creates the command. */
    SplitStatusCommand()
    {
    }

    /**
     * What was found, each part read on its own.
     *
     * @param duties     the duties lease
     * @param itemId     the item asked for; null for the stage
     * @param lease      the item's lease; null for the stage
     * @param checkpoint the item's checkpoint, if it has one; null for the
     *                   stage
     * @param releases   requests to release the item's hold; null for the
     *                   stage
     * @param items      the stage's items; null for one item
     */
    record Result(Partial<StatusParts.Lease> duties, UUID itemId,
            Partial<StatusParts.Lease> lease,
            Partial<Optional<ItemCheckpoints.Stored>> checkpoint,
            Partial<List<HoldReleases.Listed>> releases,
            Partial<Items> items)
    {
    }

    /**
     * The stage's items.
     *
     * @param counts the items by state
     * @param listed the first of those not finished, each with its lease
     */
    record Items(Map<String, Long> counts, List<Unfinished> listed)
    {
    }

    /**
     * An item not finished, with its lease.
     *
     * @param item  the item
     * @param lease its lease
     */
    record Unfinished(SplitItemReport.Unfinished item,
            StatusParts.Lease lease)
    {
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
    public StageStatusCommand.Request bind(AdminArgs arguments,
            SplitAdminContext context)
    {
        return new StageStatusCommand.Request(arguments.option("item")
                .map(StageStatusCommand::itemId).orElse(null));
    }

    @Override
    public Result run(StageStatusCommand.Request request,
            SplitAdminContext context, Invocation invocation)
    {
        AuditRecord audit = invocation.audit();
        audit.add(Attr.STAGE, ItemProcessor.STAGE);
        Partial<StatusParts.Lease> duties = Partial.of(() ->
                StatusParts.Lease.read(context.leases(), ItemProcessor.STAGE
                        + "/" + context.properties().connectorId()
                        + "/duties"));
        UUID itemId = request.itemId();
        if (itemId == null)
        {
            return new Result(duties, null, null, null, null,
                    Partial.of(() -> items(context, audit)));
        }
        audit.add(Attr.ITEM_ID, itemId);
        Partial<StatusParts.Lease> lease = Partial.of(() ->
                StatusParts.Lease.read(context.leases(),
                        itemLease(context, itemId)));
        Partial<Optional<ItemCheckpoints.Stored>> checkpoint =
                Partial.of(() -> context.checkpoints().load(itemId));
        if (checkpoint.value() != null)
        {
            checkpoint.value().ifPresent(stored ->
                    audit.add(Attr.STATE, stored.checkpoint().state()));
        }
        return new Result(duties, itemId, lease, checkpoint,
                Partial.of(() -> context.releases().list(itemId)), null);
    }

    @Override
    public void print(Result result, PrintStream out) throws Exception
    {
        StatusParts status = new StatusParts(out);
        status.show("duties", () -> result.duties().get().text());
        if (result.itemId() != null)
        {
            status.show("lease", () -> result.lease().get().text());
            status.show("checkpoint", () -> checkpoint(
                    result.checkpoint().get().orElse(null)));
            status.show("releases",
                    () -> releases(result.releases().get()));
        }
        else
        {
            status.show("items", () -> items(result.items().get()));
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

    @Override
    public Map<String, Object> answer(Result result)
    {
        return Answers.object(
                "duties", Answers.part(result.duties(), Answers::lease),
                "itemId", result.itemId(),
                "lease", Answers.part(result.lease(), Answers::lease),
                "checkpoint", Answers.part(result.checkpoint(), stored ->
                        stored.map(SplitStatusCommand::answer).orElse(null)),
                "releases", Answers.part(result.releases(), listed ->
                        Answers.list(listed, each -> Answers.object(
                                "request", each.request().request(),
                                "action", each.request().action(),
                                "by", each.request().requestedBy(),
                                "at", each.request().requestedAt(),
                                "state", each.state(), "doneAt",
                                each.doneAt(), "heldUnder",
                                each.fromVersion(), "resumedUnder",
                                each.toVersion(), "refusal",
                                each.refusal(), "reasonKept",
                                each.reasoned()))),
                "items", Answers.part(result.items(), items ->
                        Answers.object("counts", items.counts(),
                                "unfinished", Answers.list(items.listed(),
                                        each -> Answers.object("itemId",
                                                each.item().itemId(),
                                                "state",
                                                each.item().state(),
                                                "epoch",
                                                each.item().epoch(),
                                                "updatedAt",
                                                each.item().updatedAt(),
                                                "lease", Answers.lease(
                                                        each.lease()))))));
    }

    /**
     * An item's checkpoint, as the API answers it: how far it got, and
     * how it ended; never what the item holds.
     *
     * @param stored the checkpoint
     * @return its fields
     */
    private static Object answer(ItemCheckpoints.Stored stored)
    {
        Checkpoint checkpoint = stored.checkpoint();
        Checkpoint.Failure failure = checkpoint.failure();
        return Answers.object("state", checkpoint.state(), "format",
                checkpoint.format(), "bytes", checkpoint.itemSize(),
                "resumeAt", checkpoint.resumeAt() == null ? null
                        : checkpoint.resumeAt().offset(),
                "records", checkpoint.recordCount(), "sent",
                checkpoint.publishedSeq() + 1, "startedAt",
                checkpoint.startedAt(), "endedAt", stored.endedAt(),
                "restarts", checkpoint.restarts(), "batchPending",
                stored.pending() != null, "failure", failure == null ? null
                        : Answers.object("stage", failure.stage(), "code",
                                failure.code(), "offset", failure.offset(),
                                "at", failure.failedAt()));
    }

    /**
     * Reads the items by state, and those not yet finished, each with its
     * lease.
     *
     * @param context the admin context
     * @param audit   the audit event, given how many aren't finished
     * @return the items
     */
    private static Items items(SplitAdminContext context, AuditRecord audit)
    {
        SplitItemReport report = context.items();
        Map<String, Long> counts = report.countsByState();
        if (counts.isEmpty())
        {
            audit.add(Attr.COUNT, 0);
            return new Items(counts, List.of());
        }
        audit.add(Attr.COUNT, unfinished(counts));
        List<Unfinished> listed = new ArrayList<>();
        for (SplitItemReport.Unfinished item : report.unfinished(LISTED))
        {
            listed.add(new Unfinished(item, StatusParts.Lease.read(
                    context.leases(), itemLease(context, item.itemId()))));
        }
        return new Items(counts, listed);
    }

    /**
     * How many items aren't finished.
     *
     * @param counts the items by state
     * @return how many are neither completed nor failed
     */
    private static long unfinished(Map<String, Long> counts)
    {
        return counts.entrySet().stream()
                .filter(count -> !count.getKey().equals("COMPLETED")
                        && !count.getKey().equals("FAILED"))
                .mapToLong(Map.Entry::getValue).sum();
    }

    /**
     * The items by state, and those not yet finished.
     *
     * @param items the items
     * @return the text to show
     */
    private static String items(Items items)
    {
        Map<String, Long> counts = items.counts();
        if (counts.isEmpty())
        {
            return "none (no item split yet)";
        }
        long unfinished = unfinished(counts);
        StringBuilder text = new StringBuilder(counts.entrySet().stream()
                .map(count -> count.getValue() + " " + count.getKey())
                .collect(Collectors.joining(", ")));
        for (Unfinished each : items.listed())
        {
            SplitItemReport.Unfinished item = each.item();
            text.append(StatusParts.more(item.itemId() + " " + item.state()
                    + ", epoch " + item.epoch() + ", last written "
                    + item.updatedAt()));
            text.append(StatusParts.more("  lease " + each.lease().text()));
        }
        if (unfinished > items.listed().size())
        {
            text.append(StatusParts.more("and "
                    + (unfinished - items.listed().size())
                    + " more; --item <id> shows one"));
        }
        return text.toString();
    }

    /**
     * An item's checkpoint: how far it got, and how it ended.
     *
     * @param stored the checkpoint; null if the stage hasn't started the
     *               item
     * @return the text to show
     */
    private static String checkpoint(ItemCheckpoints.Stored stored)
    {
        if (stored == null)
        {
            return "none: the stage hasn't started this item";
        }
        Checkpoint checkpoint = stored.checkpoint();
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
     * @param listed the requests
     * @return one line per request, or none
     */
    private static String releases(List<HoldReleases.Listed> listed)
    {
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
