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
 * {@code status}: the lease, the checkpoint (with the item in progress) and the inbox backlog (design §11.4). Reads
 * only. Each part is reported on its own, so an unreachable KMS still leaves the lease visible; the exit status is 1
 * if any part couldn't be read.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class StatusCommand implements AdminCommand {
    static final String USAGE = """
            status
                Shows the lease, the checkpoint and item in progress, and the inbox backlog.
                Reads only: takes no lease and changes nothing.""";

    @Override
    public String name() {
        return "status";
    }

    @Override
    public Set<String> options() {
        return Set.of();
    }

    @Override
    public Set<String> flags() {
        return Set.of();
    }

    @Override
    public void run(AdminArgs a, AdminContext ctx, AuditRecord audit, PrintStream out) throws Exception {
        boolean ok = part(out, "lease", () -> lease(ctx));
        ok &= part(out, "checkpoint", () -> checkpoint(ctx, audit));
        ok &= part(out, "inbox", () -> inbox(ctx));
        if (!ok) {
            throw new PartialStatusException();
        }
    }

    @FunctionalInterface
    private interface Part {
        String read() throws Exception;
    }

    private static boolean part(PrintStream out, String label, Part p) {
        try {
            out.printf("%-21s %s%n", label + ":", p.read());
            return true;
        } catch (Exception e) {
            out.printf("%-21s unavailable (%s: %s)%n", label + ":", e.getClass().getSimpleName(), e.getMessage());
            return false;
        }
    }

    private static String lease(AdminContext ctx) {
        String name = ctx.props().lease().name();
        Optional<LeaseStatus> s = ctx.leaseStore().inspect(name);
        if (s.isEmpty()) {
            return name + " never held";
        }
        LeaseStatus l = s.get();
        if (l.live()) {
            return "%s held by %s, epoch %d, renewed %s, expires in %s".formatted(name, l.owner(), l.epoch(),
                    l.renewedAt(), human(l.remaining()));
        }
        return "%s free (expired %s ago; last holder %s, epoch %d)".formatted(name, human(l.remaining().negated()),
                l.owner(), l.epoch());
    }

    private static String checkpoint(AdminContext ctx, AuditRecord audit) throws Exception {
        Table sections = ctx.sections();
        sections.refresh();
        String raw = sections.properties().get(Checkpoint.propertyKey(ctx.props().connectorId()));
        if (raw == null) {
            return "none (no item processed yet)";
        }
        Checkpoint c = new CheckpointCodec(ctx.ids()).decode(raw);
        audit.add(Attr.FILE_ID, c.fileId());
        audit.add(Attr.STATE, c.state());
        StringBuilder b = new StringBuilder();
        b.append("item ").append(c.fileId()).append(' ').append(c.state()).append(" (").append(c.format())
                .append(")");
        if (c.state() == Checkpoint.State.SPLITTING) {
            long at = c.resumeAt().offset();
            b.append(String.format(", %.1f%% split (offset %d of %d bytes)",
                    c.fileSize() == 0 ? 100.0 : 100.0 * at / c.fileSize(), at, c.fileSize()));
        } else {
            b.append(", ").append(c.fileSize()).append(" bytes");
        }
        b.append(", sections committed ").append(c.sectionCount()).append(", published ")
                .append(c.publishedSeq() + 1).append(", started ").append(c.startedAt());
        if (c.failure() != null) {
            b.append(String.format("%n%-21s stage %s, %s at offset %d, at %s", "", c.failure().stage(),
                    c.failure().code(), c.failure().offset(), c.failure().failedAt()));
        }
        if (c.state().inProgress()) {
            b.append(String.format("%n%-21s in progress: the connector resumes it on start", ""));
        }
        if (c.state() == Checkpoint.State.SPLITTING && c.restarts() > 0) {
            b.append(String.format("%n%-21s restarted %d time(s) in a row without progress; at %d the batch is"
                    + " stored unconverted, at %d the item fails", "", c.restarts(),
                    IngestPipeline.SKIP_CONVERSION_AFTER_RESTARTS, IngestPipeline.FAIL_AFTER_RESTARTS));
        }
        return b.toString();
    }

    private static String inbox(AdminContext ctx) throws Exception {
        // Read only: load() and backlog() never commit, so no committer is needed.
        Inbox inbox = new Inbox(ctx.inbox(), null, ctx.ids(), ctx.props().connectorId(), RetryPolicy.DEFAULT,
                Clock.systemUTC());
        inbox.load();
        long backlog = inbox.backlog();
        return backlog == 0 ? "no items waiting" : backlog + (backlog == 1 ? " item" : " items") + " waiting";
    }

    static String human(Duration d) {
        long s = Math.max(0, d.toSeconds());
        if (s < 120) {
            return s + "s";
        }
        if (s < 7200) {
            return s / 60 + "m";
        }
        if (s < 172_800) {
            return s / 3600 + "h";
        }
        return s / 86_400 + "d";
    }

    /** Some parts of the status couldn't be read; they are shown as unavailable. */
    static final class PartialStatusException extends Exception {
        PartialStatusException() {
            super("Some status parts are unavailable");
        }
    }
}
