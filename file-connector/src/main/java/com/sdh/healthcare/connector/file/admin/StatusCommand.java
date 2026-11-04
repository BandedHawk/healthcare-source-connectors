/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointCodec;
import com.sdh.healthcare.connector.core.checkpoint.LeaseStatus;
import com.sdh.healthcare.connector.core.inbox.Inbox;
import com.sdh.healthcare.connector.core.pipeline.IngestPipeline;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import java.io.PrintStream;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.Table;

/**
 * {@code status}: the lease, the checkpoint (with the item in progress)
 * and the inbox backlog (design §11.4). Reads only. Each part is reported
 * on its own, so an unreachable KMS still leaves the lease visible; the
 * exit status is 1 if any part couldn't be read.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class StatusCommand implements AdminCommand
{

    /** The command's help. */
    static final String USAGE = """
            status
                Shows the lease, the checkpoint and item in progress, and
                the inbox backlog. Reads only: takes no lease and changes
                nothing.""";

    /** Creates the command. */
    StatusCommand()
    {
    }

    @Override
    public String name()
    {
        return "status";
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
    public void run(AdminArgs arguments, AdminContext context,
            AuditRecord audit, PrintStream out) throws Exception
    {
        boolean ok = part(out, "lease", () -> lease(context));
        ok &= part(out, "checkpoint", () -> checkpoint(context, audit));
        ok &= part(out, "inbox", () -> inbox(context));
        if (!ok)
        {
            throw new PartialStatusException();
        }
    }

    /** Reads one part of the status. */
    @FunctionalInterface
    private interface Part
    {

        /**
         * Reads the part.
         *
         * @return the text to show
         * @throws Exception if the part can't be read
         */
        String read() throws Exception;
    }

    /**
     * Prints one part, or that it is unavailable.
     *
     * @param out where to print
     * @param label the part's label
     * @param part reads the part
     * @return true if the part was read
     */
    private static boolean part(PrintStream out, String label, Part part)
    {
        try
        {
            out.printf("%-21s %s%n", label + ":", part.read());
            return true;
        }
        catch (Exception failure)
        {
            out.printf("%-21s unavailable (%s: %s)%n", label + ":",
                    failure.getClass().getSimpleName(), failure.getMessage());
            return false;
        }
    }

    /**
     * The lease: who holds it and until when, or when it expired.
     *
     * @param context the admin context
     * @return the text to show
     */
    private static String lease(AdminContext context)
    {
        String name = context.properties().lease().name();
        Optional<LeaseStatus> inspected = context.leaseStore().inspect(name);
        if (inspected.isEmpty())
        {
            return name + " never held";
        }
        LeaseStatus lease = inspected.get();
        if (lease.live())
        {
            return "%s held by %s, epoch %d, renewed %s, expires in %s"
                    .formatted(name, lease.owner(), lease.epoch(),
                            lease.renewedAt(), human(lease.remaining()));
        }
        return "%s free (expired %s ago; last holder %s, epoch %d)"
                .formatted(name, human(lease.remaining().negated()),
                        lease.owner(), lease.epoch());
    }

    /**
     * The checkpoint: the item in progress or last processed, and how far
     * it got.
     *
     * @param context the admin context
     * @param audit the audit event, given the item's ID and state
     * @return the text to show
     * @throws Exception if the table or ID key can't be read
     */
    private static String checkpoint(AdminContext context, AuditRecord audit)
            throws Exception
    {
        Table sections = context.sections();
        sections.refresh();
        String raw = sections.properties().get(Checkpoint.propertyKey(
                context.properties().connectorId()));
        if (raw == null)
        {
            return "none (no item processed yet)";
        }
        Checkpoint checkpoint = new CheckpointCodec(context.ids()).decode(raw);
        audit.add(Attr.FILE_ID, checkpoint.fileId());
        audit.add(Attr.STATE, checkpoint.state());
        StringBuilder text = new StringBuilder();
        text.append("item ").append(checkpoint.fileId()).append(' ')
                .append(checkpoint.state()).append(" (")
                .append(checkpoint.format()).append(")");
        if (checkpoint.state() == Checkpoint.State.SPLITTING)
        {
            long at = checkpoint.resumeAt().offset();
            text.append(String.format(", %.1f%% split (offset %d of %d bytes)",
                    checkpoint.fileSize() == 0 ? 100.0
                            : 100.0 * at / checkpoint.fileSize(),
                    at, checkpoint.fileSize()));
        }
        else
        {
            text.append(", ").append(checkpoint.fileSize()).append(" bytes");
        }
        text.append(", sections committed ")
                .append(checkpoint.sectionCount()).append(", published ")
                .append(checkpoint.publishedSeq() + 1).append(", started ")
                .append(checkpoint.startedAt());
        if (checkpoint.failure() != null)
        {
            text.append(String.format("%n%-21s stage %s, %s at offset %d,"
                    + " at %s", "", checkpoint.failure().stage(),
                    checkpoint.failure().code(),
                    checkpoint.failure().offset(),
                    checkpoint.failure().failedAt()));
        }
        if (checkpoint.state().inProgress())
        {
            text.append(String.format(
                    "%n%-21s in progress: the connector resumes it on start",
                    ""));
        }
        if (checkpoint.state() == Checkpoint.State.SPLITTING
                && checkpoint.restarts() > 0)
        {
            text.append(String.format("%n%-21s restarted %d time(s) in a row"
                    + " without progress; at %d the batch is"
                    + " stored unconverted, at %d the item fails", "",
                    checkpoint.restarts(),
                    IngestPipeline.SKIP_CONVERSION_AFTER_RESTARTS,
                    IngestPipeline.FAIL_AFTER_RESTARTS));
        }
        return text.toString();
    }

    /**
     * The inbox backlog.
     *
     * @param context the admin context
     * @return the text to show
     * @throws Exception if the inbox or ID key can't be read
     */
    private static String inbox(AdminContext context) throws Exception
    {
        // Read only: load() and backlog() never commit, so no committer is
        // needed.
        Inbox inbox = new Inbox(context.inbox(), null, context.ids(),
                context.properties().connectorId(), RetryPolicy.DEFAULT,
                Clock.systemUTC());
        inbox.load();
        long backlog = inbox.backlog();
        return backlog == 0 ? "no items waiting"
                : backlog + (backlog == 1 ? " item" : " items") + " waiting";
    }

    /**
     * A duration in its largest sensible unit: seconds up to 2 minutes,
     * minutes up to 2 hours, hours up to 2 days, then days.
     *
     * @param duration the duration; negative counts as zero
     * @return the text, such as {@code 90s} or {@code 3h}
     */
    static String human(Duration duration)
    {
        long seconds = Math.max(0, duration.toSeconds());
        if (seconds < 120)
        {
            return seconds + "s";
        }
        if (seconds < 7200)
        {
            return seconds / 60 + "m";
        }
        if (seconds < 172_800)
        {
            return seconds / 3600 + "h";
        }
        return seconds / 86_400 + "d";
    }

    /**
     * Some parts of the status couldn't be read; they are shown as
     * unavailable.
     */
    static final class PartialStatusException extends Exception
    {

        /** Creates the exception. */
        PartialStatusException()
        {
            super("Some status parts are unavailable");
        }
    }
}
