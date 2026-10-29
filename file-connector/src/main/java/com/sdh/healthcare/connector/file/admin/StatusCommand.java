package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointCodec;
import com.sdh.healthcare.connector.core.checkpoint.LeaseStatus;
import com.sdh.healthcare.connector.core.storage.ArchiveCheckpoint;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.Table;

/**
 * {@code status}: the lease, the checkpoint (with the file in progress), archive progress and the inbound backlog
 * (design §11.4). Reads only. Each part is reported on its own, so an unreachable KMS still leaves the lease and
 * backlog visible; the exit status is 1 if any part couldn't be read.
 */
final class StatusCommand implements AdminCommand {
    static final String USAGE = """
            status
                Shows the lease, the checkpoint and file in progress, archive progress and the inbound backlog.
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
        ok &= part(out, "archive source_files", () -> archive(ctx, ctx.sourceFiles()));
        ok &= part(out, "archive failed_files", () -> archive(ctx, ctx.failedFiles()));
        ok &= part(out, "inbound", () -> inbound(ctx.props().file().inboundDir()));
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
            return "none (no file processed yet)";
        }
        Checkpoint c = new CheckpointCodec(ctx.ids()).decode(raw);
        audit.add(Attr.FILE_ID, c.fileId());
        audit.add(Attr.STATE, c.state());
        StringBuilder b = new StringBuilder();
        b.append("file ").append(c.fileId()).append(' ').append(c.state()).append(" (").append(c.format())
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
        return b.toString();
    }

    private static String archive(AdminContext ctx, Table table) throws Exception {
        table.refresh();
        String raw = table.properties().get(ArchiveCheckpoint.propertyKey(ctx.props().connectorId()));
        if (raw == null) {
            return "nothing archived yet";
        }
        ArchiveCheckpoint c = ArchiveCheckpoint.codec(ctx.ids()).decode(raw);
        if (c.done()) {
            return "last file " + c.fileId() + " finished " + c.finishedAt();
        }
        return "file " + c.fileId() + " in progress, " + c.nextBlock() + " blocks archived";
    }

    private static String inbound(Path dir) throws IOException {
        if (dir == null) {
            return "sdh.file.inbound-dir not set";
        }
        if (!Files.isDirectory(dir)) {
            return "directory not found here";
        }
        long count = 0;
        long oldest = Long.MAX_VALUE;
        try (Stream<Path> s = Files.list(dir)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                String n = p.getFileName().toString();
                if (Files.isRegularFile(p) && !n.startsWith(".") && !n.endsWith(".done")) {
                    count++;
                    oldest = Math.min(oldest, Files.getLastModifiedTime(p).toMillis());
                }
            }
        }
        if (count == 0) {
            return "no files waiting";
        }
        return count + (count == 1 ? " file" : " files") + " waiting, oldest modified "
                + human(Duration.ofMillis(Instant.now().toEpochMilli() - oldest)) + " ago";
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
