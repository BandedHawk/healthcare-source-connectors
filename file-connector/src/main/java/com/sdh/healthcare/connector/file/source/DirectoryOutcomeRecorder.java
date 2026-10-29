package com.sdh.healthcare.connector.file.source;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.pipeline.OutcomeRecorder;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Writes finished-file records into {@code completed/} and {@code failed/} inside the inbound directory, plus the
 * local encrypted fallback {@code failed/<fileId>.enc} (design §5, §11).
 *
 * <p>Records hold no PHI: IDs, timestamps, counts, the keyed digest, error code and stage. The original filename
 * never appears. Every file is written to a hidden temp file, synced, then atomically renamed into place, with
 * owner-only permissions.
 */
public final class DirectoryOutcomeRecorder implements OutcomeRecorder {
    public static final String COMPLETED_DIR = "completed";
    public static final String FAILED_DIR = "failed";

    private static final boolean POSIX = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    private final Path completed;
    private final Path failed;

    public DirectoryOutcomeRecorder(Path inboundDir) throws IOException {
        this.completed = ensureDir(inboundDir.resolve(COMPLETED_DIR));
        this.failed = ensureDir(inboundDir.resolve(FAILED_DIR));
    }

    @Override
    public void completed(CompletionRecord r) throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("fileId", r.fileId().toString());
        m.put("status", "COMPLETED");
        m.put("startedAt", r.startedAt().toString());
        m.put("completedAt", r.completedAt().toString());
        m.put("sectionCount", r.sectionCount());
        m.put("failedSectionCount", r.failedSectionCount());
        m.put("byteCount", r.fileSize());
        m.put("fileDigest", r.fileDigest());
        writeAtomically(completed, r.fileId() + ".json", JSONObjectUtils.toJSONString(m).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void failed(FailureRecord r) throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("fileId", r.fileId().toString());
        m.put("status", "FAILED");
        m.put("startedAt", r.startedAt().toString());
        m.put("failedAt", r.failedAt().toString());
        m.put("stage", r.stage());
        m.put("errorCode", r.errorCode().name());
        m.put("offset", r.offset());
        m.put("lastCommittedSeq", r.lastCommittedSeq());
        m.put("byteCount", r.fileSize());
        m.put("archivedTo", r.archivedTo().name());
        if (r.fileDigest() != null) {
            m.put("fileDigest", r.fileDigest());
        }
        writeAtomically(failed, r.fileId() + ".json", JSONObjectUtils.toJSONString(m).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public FallbackFile openFallback(UUID fileId) throws IOException {
        Path target = failed.resolve(fileId + ".enc");
        Path tmp = failed.resolve("." + fileId + ".enc.tmp");
        FileChannel ch = open(tmp);
        OutputStream out = new BufferedOutputStream(Channels.newOutputStream(ch), 1 << 20);
        return new FallbackFile() {
            private boolean committed;

            @Override
            public OutputStream out() {
                return out;
            }

            @Override
            public void commit() throws IOException {
                out.flush();
                ch.force(true);
                out.close();
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                syncDir(failed);
                committed = true;
            }

            @Override
            public void close() throws IOException {
                if (!committed) {
                    try {
                        out.close();
                    } finally {
                        Files.deleteIfExists(tmp);
                    }
                }
            }
        };
    }

    private static void writeAtomically(Path dir, String name, byte[] content) throws IOException {
        Path tmp = dir.resolve("." + name + ".tmp");
        try (FileChannel ch = open(tmp)) {
            ch.write(java.nio.ByteBuffer.wrap(content));
            ch.force(true);
        }
        Files.move(tmp, dir.resolve(name), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        syncDir(dir);
    }

    private static FileChannel open(Path p) throws IOException {
        Files.deleteIfExists(p);
        if (POSIX) {
            return FileChannel.open(p, java.util.Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        }
        return FileChannel.open(p, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    private static Path ensureDir(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            FileAttribute<?>[] attrs = POSIX
                    ? new FileAttribute<?>[] {PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))}
                    : new FileAttribute<?>[0];
            Files.createDirectories(dir, attrs);
        }
        return dir;
    }

    /** Makes the rename durable. Best effort: not every platform can open a directory for sync. */
    private static void syncDir(Path dir) {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // not supported here
        }
    }
}
