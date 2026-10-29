package com.sdh.healthcare.connector.file.admin;

import static com.sdh.healthcare.connector.core.storage.ArchiveTables.*;

import com.sdh.healthcare.connector.contract.table.SectionsTable;
import com.sdh.healthcare.connector.core.storage.ArchiveReader;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * {@code restore}: decrypts an archived file, whole or from an offset, into a directory (design §11.4). Looks in
 * {@code failed_files} first, then {@code source_files}. The digest is checked on every restore; on a mismatch
 * nothing is left behind.
 */
final class RestoreCommand implements AdminCommand {
    static final String USAGE = """
            restore --file-id <id> --to <dir> [--from-offset <n> | --unprocessed] [--table failed|source] [--original-name]
                Decrypts an archived file into <dir>, checking its digest.
                --from-offset   start at this byte offset instead of 0
                --unprocessed   start after the last committed section of a failed file: only its unprocessed
                                tail (headers and envelopes before that point are not repeated)
                --table         where to look (default: failed_files, then source_files)
                --original-name name the output after the original file instead of <id>""";

    @Override
    public String name() {
        return "restore";
    }

    @Override
    public Set<String> flags() {
        return Set.of("unprocessed", "original-name");
    }

    @Override
    public Set<String> options() {
        return Set.of("file-id", "to", "from-offset", "table", "unprocessed", "original-name");
    }

    @Override
    public void run(AdminArgs a, AdminContext ctx, AuditRecord audit, PrintStream out) throws Exception {
        UUID fileId = parseId(a.required("file-id"));
        audit.add(Attr.FILE_ID, fileId);
        Path dir = OutputFiles.directory(a.required("to"));
        if (a.option("from-offset").isPresent() && a.flag("unprocessed")) {
            throw new AdminArgs.UsageException("Use --from-offset or --unprocessed, not both");
        }

        Table table = null;
        Record summary = null;
        for (String name : candidates(a.option("table"))) {
            Table t = ctx.table(name);
            Optional<Record> s = ArchiveReader.summary(t, fileId);
            if (s.isPresent()) {
                table = t;
                summary = s.get();
                break;
            }
        }
        if (table == null) {
            throw new IllegalStateException("No finished archive for this file id");
        }
        String tableName = table.name().substring(table.name().lastIndexOf('.') + 1);
        audit.add(Attr.TABLE, tableName);
        long size = (Long) summary.getField(FILE_SIZE);
        boolean failed = FAILED_FILES.equals(tableName);
        if (failed) {
            out.printf("failed file: stage %s, error %s at offset %d, last committed seq %d, failed at %s%n",
                    summary.getField(FAILURE_STAGE), summary.getField(ERROR_CODE), summary.getField(FAILURE_OFFSET),
                    summary.getField(FAILURE_SEQ), summary.getField(FAILED_AT));
        }

        long from;
        if (a.flag("unprocessed")) {
            if (!failed) {
                throw new AdminArgs.UsageException("--unprocessed applies to failed files only");
            }
            from = unprocessedFrom(ctx.sections(), fileId, (Long) summary.getField(FAILURE_SEQ));
        } else {
            from = a.option("from-offset").map(RestoreCommand::parseOffset).orElse(0L);
        }
        if (from > size) {
            throw new AdminArgs.UsageException("--from-offset is beyond the end of the file (" + size + " bytes)");
        }
        audit.add(Attr.OFFSET, from);

        String suffix = from > 0 ? ".from-" + from : "";
        String outName = fileId + suffix;
        Optional<String> original = Optional.empty();
        if (a.flag("original-name")) {
            original = ArchiveReader.originalName(summary, fileId, ctx.keys().wrap());
            if (original.isEmpty()) {
                throw new IllegalStateException("The archive holds no original name for this file");
            }
            outName = OutputFiles.safeName(original.get()) + suffix;
        }

        Table source = table;
        long written = OutputFiles.writeNew(dir, outName, o -> ArchiveReader.restore(source, fileId,
                ctx.keys().wrap(), ctx.ids(), from, o));
        audit.add(Attr.BYTES, written);
        // The original name is PHI-adjacent and stays out of output that may end up in logs.
        out.printf("restored %d bytes (offset %d of %d) from %s, digest verified, to %s%n", written, from, size,
                tableName, original.isPresent() ? dir + " under its original name" : dir.resolve(outName));
    }

    private static String[] candidates(Optional<String> table) {
        return table.map(t -> switch (t) {
            case "failed" -> new String[] {FAILED_FILES};
            case "source" -> new String[] {SOURCE_FILES};
            default -> throw new AdminArgs.UsageException("--table must be failed or source");
        }).orElse(new String[] {FAILED_FILES, SOURCE_FILES});
    }

    /** The end of the last committed section; 0 if none was committed. */
    static long unprocessedFrom(Table sections, UUID fileId, long lastSeq) throws IOException {
        if (lastSeq < 0) {
            return 0;
        }
        try (CloseableIterable<Record> rows = IcebergGenerics.read(sections)
                .where(Expressions.and(Expressions.equal(SectionsTable.FILE_ID, fileId),
                        Expressions.equal(SectionsTable.SEQ, lastSeq),
                        Expressions.equal(SectionsTable.CHUNK_INDEX, 0)))
                .select(SectionsTable.SOURCE_OFFSET, SectionsTable.SOURCE_LENGTH).build()) {
            for (Record r : rows) {
                return (Long) r.getField(SectionsTable.SOURCE_OFFSET) + (Long) r.getField(SectionsTable.SOURCE_LENGTH);
            }
        }
        throw new IllegalStateException("The file's last committed section is no longer stored (retention?); "
                + "use --from-offset");
    }

    static UUID parseId(String s) {
        try {
            return UUID.fromString(s);
        } catch (IllegalArgumentException e) {
            throw new AdminArgs.UsageException("--file-id must be a UUID");
        }
    }

    static long parseOffset(String s) {
        try {
            long v = Long.parseLong(s);
            if (v < 0) {
                throw new NumberFormatException();
            }
            return v;
        } catch (NumberFormatException e) {
            throw new AdminArgs.UsageException("--from-offset must be a non-negative number");
        }
    }
}
