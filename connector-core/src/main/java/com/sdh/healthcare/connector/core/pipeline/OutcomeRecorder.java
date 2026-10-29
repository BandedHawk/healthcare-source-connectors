package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.UUID;

/**
 * Where a connector keeps PHI-free records of finished files and the local encrypted fallback (design §5, §11).
 * Writes must be atomic and idempotent: they may be repeated after a restart.
 */
public interface OutcomeRecorder {

    void completed(CompletionRecord r) throws IOException;

    void failed(FailureRecord r) throws IOException;

    /** Opens the fallback for {@code fileId}. Nothing is kept unless {@link FallbackFile#commit()} is called. */
    FallbackFile openFallback(UUID fileId) throws IOException;

    interface FallbackFile extends AutoCloseable {
        OutputStream out();

        /** Makes the written file durable and visible. */
        void commit() throws IOException;

        /** Discards the file if it wasn't committed. */
        @Override
        void close() throws IOException;
    }

    /** @param failedSectionCount sections stored in their original form after failing conversion (design §6.4) */
    record CompletionRecord(UUID fileId, Instant startedAt, Instant completedAt, long sectionCount,
            long failedSectionCount, long fileSize, String fileDigest) {}

    enum ArchiveLocation { FAILED_FILES, LOCAL_FALLBACK, UNKNOWN }

    /**
     * @param offset           source byte offset of the failure (or where splitting had reached)
     * @param lastCommittedSeq last section stored before the failure; -1 if none
     * @param fileDigest       set when archived to {@code failed_files}
     */
    record FailureRecord(UUID fileId, Instant startedAt, Instant failedAt, String stage, ErrorCode errorCode,
            long offset, long lastCommittedSeq, long fileSize, ArchiveLocation archivedTo, String fileDigest) {}
}
