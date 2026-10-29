package com.sdh.healthcare.connector.core.pipeline;

import static com.sdh.healthcare.connector.core.storage.ArchiveTables.*;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint.Failure;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.crypto.RecoveryEnvelope;
import com.sdh.healthcare.connector.core.pipeline.OutcomeRecorder.ArchiveLocation;
import com.sdh.healthcare.connector.core.pipeline.OutcomeRecorder.CompletionRecord;
import com.sdh.healthcare.connector.core.pipeline.OutcomeRecorder.FailureRecord;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.storage.Archiver;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.security.interfaces.ECPublicKey;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import com.sdh.healthcare.connector.core.storage.ArchiveTables;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes one source file through its whole lifecycle (design §4.1, §11):
 * SPLITTING → ARCHIVING → COMPLETED, or → FAILING → FAILED.
 *
 * <p><b>Completing:</b> archive the original to {@code source_files}, publish {@code file.completed}, write the
 * {@code completed/} record, delete the original, then commit {@code COMPLETED}. The original is deleted only after
 * its archive is committed, and every step can be repeated, so a restart at any point finishes the job; once the
 * archive is done, no source is needed ({@link #recoverWithoutSource()}).
 *
 * <p><b>Failing:</b> a parse or limit error, or a transient error that outlasted its retries, fails the file.
 * The decision is committed first ({@code FAILING}, with stage, code and offset), so a restart finishes failing
 * rather than retrying. Sections already stored stay. The original is archived to {@code failed_files}; if that
 * isn't possible (storage or KMS down, or the file is over {@code max-file-bytes}) it is encrypted locally to the
 * recovery key instead. Then the {@code failed/} record is written, the original deleted, and {@code FAILED}
 * committed.
 *
 * <p>Lease loss, unknown commit outcomes, a tampered checkpoint and invariant violations are never treated as file
 * failures: they propagate so the process exits and restarts from the checkpoint.
 */
public final class FileProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FileProcessor.class);

    public enum Result { COMPLETED, FAILED, ABANDONED }

    public record Outcome(UUID fileId, Result result, long sectionCount, Failure failure, ArchiveLocation archive) {}

    private final IngestPipeline pipeline;
    private final Archiver sourceArchiver;
    private final Archiver failedArchiver;
    private final OutcomeRecorder recorder;
    private final ECPublicKey recoveryKey;

    public FileProcessor(IngestPipeline pipeline, Archiver sourceArchiver, Archiver failedArchiver,
            OutcomeRecorder recorder, ECPublicKey recoveryKey) {
        this.pipeline = pipeline;
        this.sourceArchiver = sourceArchiver;
        this.failedArchiver = failedArchiver;
        this.recorder = recorder;
        this.recoveryKey = recoveryKey;
    }

    /** The file that must be finished before any other, if there is one. */
    public Optional<Checkpoint> inProgress() {
        return pipeline.checkpoint().filter(c -> c.state().inProgress());
    }

    /** For matching inbound files against {@link #inProgress()}. */
    public String fingerprint(SeekableByteSource src) throws IOException {
        return pipeline.fingerprint(src);
    }

    /** Processes one file: a new one, or the in-progress one (matched by fingerprint). Traced as {@code ingest.file}. */
    public Outcome process(SourceFile file) throws Exception {
        return traced("ingest.file", () -> {
            try (SeekableByteSource src = file.open()) {
                Telemetry.get().audit("file.discovered", Fields.of(Attr.BYTES, src.size()));
                return processOpen(file, src);
            }
        });
    }

    @FunctionalInterface
    private interface Work {
        Outcome run() throws Exception;
    }

    /** Root span for one file's work; records the outcome, and the in-progress gauge. */
    private Outcome traced(String name, Work work) throws Exception {
        Telemetry t = Telemetry.get();
        Span span = t.span(name, Fields.of(Attr.FORMAT, pipeline.settings().format()));
        t.fileStartedProcessing();
        try (Scope ignored = span.makeCurrent()) {
            Outcome o = work.run();
            if (o != null) {
                span.setAttribute(Attr.FILE_ID.key(), o.fileId().toString());
                span.setAttribute(Attr.RESULT.key(), o.result().name());
                if (o.failure() != null) {
                    span.setAttribute(Attr.STAGE.key(), o.failure().stage());
                    Telemetry.fail(span, o.failure().code());
                }
            }
            return o;
        } catch (Exception | Error e) {
            Telemetry.fail(span, e);
            throw e;
        } finally {
            span.end();
            t.fileFinishedProcessing();
        }
    }

    private Outcome processOpen(SourceFile file, SeekableByteSource src) throws Exception {
        String fingerprint = pipeline.fingerprint(src);
        Optional<Checkpoint> cp = pipeline.checkpoint().filter(c -> c.fingerprint().equals(fingerprint));
        if (cp.isPresent() && cp.get().state() == Checkpoint.State.FAILING) {
            return fail(cp.get(), file, src);
        }
        if (cp.isPresent() && cp.get().state() == Checkpoint.State.ARCHIVING) {
            return completeOrFail(cp.get(), file, src, fingerprint);
        }
        IngestPipeline.SplitResult split;
        try {
            split = pipeline.split(src);
        } catch (Exception e) {
            rethrowIfFatal(e);
            return fail(failingCheckpoint(e, "split", src, fingerprint), file, src);
        }
        return completeOrFail(split.checkpoint(), file, src, fingerprint);
    }

    /**
     * After a restart when the in-progress file is no longer in the inbound directory. Finishes it if its archive is
     * complete, otherwise records it as abandoned. Returns empty if nothing was in progress.
     */
    public Optional<Outcome> recoverWithoutSource() throws Exception {
        Optional<Checkpoint> found = inProgress();
        if (found.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(traced("ingest.recover", () -> recover(found.get())));
    }

    private Outcome recover(Checkpoint cp) throws Exception {
        switch (cp.state()) {
            case ARCHIVING -> {
                Optional<Archiver.Result> done = sourceArchiver.finished(cp.fileId());
                if (done.isPresent()) {
                    return finishCompletion(cp, done.get(), null);
                }
            }
            case FAILING -> {
                announceCommitted(cp, true);
                Optional<Archiver.Result> done = failedArchiver.finished(cp.fileId());
                if (done.isEmpty()) {
                    // Check failed/ for a local fallback.
                    Telemetry.get().auditWarn("file.failed_unarchived", Fields.of(Attr.FILE_ID, cp.fileId()));
                }
                return finishFailure(cp, true, done.map(Archiver.Result::digest).orElse(null),
                        done.isPresent() ? ArchiveLocation.FAILED_FILES : ArchiveLocation.UNKNOWN, null);
            }
            default -> { }
        }
        pipeline.commitCheckpoint(cp.withState(Checkpoint.State.ABANDONED));
        Telemetry.get().auditWarn("file.abandoned", Fields.of(Attr.FILE_ID, cp.fileId()).with(Attr.STATE, cp.state())
                .with(Attr.SEQ, cp.committedSeq()));
        return new Outcome(cp.fileId(), Result.ABANDONED, cp.sectionCount(), null, null);
    }

    // ---- completion ----

    private Outcome completeOrFail(Checkpoint cp, SourceFile file, SeekableByteSource src, String fingerprint)
            throws Exception {
        Archiver.Result archived;
        try {
            archived = sourceArchiver.archive(cp.fileId(), src, file.originalName(), (r, at) -> {
                r.setField(SECTION_COUNT, cp.sectionCount());
                r.setField(STARTED_AT, utc(cp.startedAt()));
                r.setField(COMPLETED_AT, utc(at));
            });
        } catch (Exception e) {
            rethrowIfFatal(e);
            return fail(failingCheckpoint(e, "archive", src, fingerprint), file, src);
        }
        Telemetry.get().audit("file.archived", Fields.of(Attr.FILE_ID, cp.fileId())
                .with(Attr.TABLE, ArchiveTables.SOURCE_FILES).with(Attr.BYTES, cp.fileSize()));
        return finishCompletion(cp, archived, file);
    }

    private Outcome finishCompletion(Checkpoint cp, Archiver.Result archived, SourceFile file) throws Exception {
        long failedSections = pipeline.failedSectionCount(cp.fileId());
        pipeline.publishNow(List.of(new FileCompleted(cp.fileId(), cp.sectionCount(), failedSections, cp.fileSize(),
                archived.digest(), cp.startedAt(), archived.finishedAt())));
        recorder.completed(new CompletionRecord(cp.fileId(), cp.startedAt(), archived.finishedAt(), cp.sectionCount(),
                failedSections, cp.fileSize(), archived.digest()));
        if (file != null) {
            file.deleteOriginal();
        }
        pipeline.commitCheckpoint(cp.withState(Checkpoint.State.COMPLETED));
        Telemetry.get().audit("file.completed", Fields.of(Attr.FILE_ID, cp.fileId())
                .with(Attr.COUNT, cp.sectionCount()).with(Attr.BYTES, cp.fileSize()));
        Telemetry.get().fileCompleted();
        return new Outcome(cp.fileId(), Result.COMPLETED, cp.sectionCount(), null, null);
    }

    // ---- failure ----

    /** Builds the FAILING checkpoint for an error: on the file's own checkpoint if it has one, else a new one. */
    private Checkpoint failingCheckpoint(Exception e, String stage, SeekableByteSource src, String fingerprint)
            throws IOException {
        Checkpoint base = null;
        try {
            base = pipeline.checkpoint()
                    .filter(c -> c.fingerprint().equals(fingerprint) && c.state().inProgress())
                    .orElse(null);
        } catch (RuntimeException unreadable) {
            if (unreadable instanceof CheckpointTamperedException) {
                throw unreadable;
            }
            // storage down: carry on with a fresh checkpoint; it may not be committable either
        }
        Instant now = pipeline.clock().instant();
        if (base == null) {
            base = Checkpoint.start(UuidV7.next(now.toEpochMilli()), fingerprint, pipeline.settings().format(),
                    src.size(), now);
        }
        ErrorCode code;
        long offset = base.resumeAt().offset();
        switch (e) {
            case SplitException se -> {
                code = se.code();
                offset = se.offset();
                if (code == ErrorCode.LIMIT_FILE_SIZE) {
                    stage = "intake";
                }
            }
            case PublishFailedException pf -> {
                code = ErrorCode.PUBLISH_FAILED;
                stage = "publish";
            }
            case IOException io -> code = ErrorCode.SOURCE_READ_FAILED;
            default -> code = ErrorCode.STORAGE_FAILED;
        }
        Telemetry.get().auditWarn("file.failing", Fields.of(Attr.FILE_ID, base.fileId()).with(Attr.STAGE, stage)
                .with(Attr.ERROR_CODE, code).with(Attr.OFFSET, offset).with(Attr.REASON, e.getClass().getSimpleName()));
        return base.failing(new Failure(stage, code, offset, now));
    }

    private Outcome fail(Checkpoint failing, SourceFile file, SeekableByteSource src) throws Exception {
        boolean recorded = tryCommit(failing);
        UUID fileId = failing.fileId();
        Failure f = failing.failure();
        announceCommitted(failing, recorded);
        String digest = null;
        ArchiveLocation location = ArchiveLocation.LOCAL_FALLBACK;
        if (recorded && f.code() != ErrorCode.LIMIT_FILE_SIZE) {
            try {
                digest = failedArchiver.archive(fileId, src, file.originalName(), (r, at) -> {
                    r.setField(SECTION_COUNT, failing.sectionCount());
                    r.setField(STARTED_AT, utc(failing.startedAt()));
                    r.setField(FAILURE_STAGE, f.stage());
                    r.setField(FAILURE_OFFSET, f.offset());
                    r.setField(FAILURE_SEQ, failing.committedSeq());
                    r.setField(ERROR_CODE, f.code().name());
                    r.setField(FAILED_AT, utc(f.failedAt()));
                }).digest();
                location = ArchiveLocation.FAILED_FILES;
            } catch (Exception e) {
                rethrowIfFatal(e);
                // Using the local fallback.
                Telemetry.get().auditWarn("archive.unavailable", Fields.of(Attr.FILE_ID, fileId)
                        .with(Attr.REASON, e.getClass().getSimpleName()));
            }
        }
        if (location == ArchiveLocation.LOCAL_FALLBACK) {
            // If this throws, the original is kept and the failure is retried on the next attempt.
            try (OutcomeRecorder.FallbackFile out = recorder.openFallback(fileId)) {
                RecoveryEnvelope.encrypt(src, fileId, file.originalName(), recoveryKey, out.out());
                out.commit();
            }
        }
        return finishFailure(failing, recorded, digest, location, file);
    }

    private Outcome finishFailure(Checkpoint failing, boolean recorded, String digest, ArchiveLocation location,
            SourceFile file) throws Exception {
        Failure f = failing.failure();
        recorder.failed(new FailureRecord(failing.fileId(), failing.startedAt(), f.failedAt(), f.stage(), f.code(),
                f.offset(), failing.committedSeq(), failing.fileSize(), location, digest));
        if (file != null) {
            file.deleteOriginal();
        }
        Telemetry.get().auditError("file.failed", Fields.of(Attr.FILE_ID, failing.fileId()).with(Attr.STAGE, f.stage())
                .with(Attr.ERROR_CODE, f.code()).with(Attr.OFFSET, f.offset()).with(Attr.SEQ, failing.committedSeq())
                .with(Attr.ARCHIVE, location));
        Telemetry.get().fileFailed(f.code(), f.stage());
        if (recorded) {
            tryCommit(failing.failed());
        }
        return new Outcome(failing.fileId(), Result.FAILED, failing.sectionCount(), f, location);
    }

    /**
     * Sections committed before a failure stay stored (design §11), so downstream must hear about every one of them.
     * Publishing of the last batch may not have been acknowledged yet, and no restart will republish it once the file
     * is FAILED, so it is done here. Skipped when the broker is the failure; the range is logged instead.
     */
    private void announceCommitted(Checkpoint failing, boolean storageAvailable) throws Exception {
        if (failing.committedSeq() <= failing.publishedSeq()) {
            return;
        }
        if (storageAvailable && failing.failure().code() != ErrorCode.PUBLISH_FAILED) {
            try {
                pipeline.announceCommitted(failing);
                return;
            } catch (Exception e) {
                rethrowIfFatal(e);
            }
        }
        Telemetry.get().auditWarn("publish.unannounced", Fields.of(Attr.FILE_ID, failing.fileId())
                .with(Attr.SEQ, failing.publishedSeq() + 1)
                .with(Attr.COUNT, failing.committedSeq() - failing.publishedSeq()));
    }

    /** Commits a checkpoint, reporting rather than throwing if storage is unavailable. */
    private boolean tryCommit(Checkpoint cp) throws Exception {
        try {
            pipeline.commitCheckpoint(cp);
            return true;
        } catch (Exception e) {
            rethrowIfFatal(e);
            LOG.warn("checkpoint.unavailable fileId={} state={} error={}", cp.fileId(), cp.state(),
                    e.getClass().getSimpleName());
            return false;
        }
    }

    static void rethrowIfFatal(Exception e) throws Exception {
        if (e instanceof LeaseLostException || e instanceof CommitStateUnknownException
                || e instanceof CheckpointTamperedException || e instanceof InterruptedException
                || e instanceof IllegalStateException) {
            throw e;
        }
    }

    private static OffsetDateTime utc(Instant i) {
        return OffsetDateTime.ofInstant(i, ZoneOffset.UTC);
    }
}
