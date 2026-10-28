package com.sdh.healthcare.connector.core.checkpoint;

import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Progress of the file a connector is working on (design §4.1, §9.1). Stored, MAC'd, in a table property of the
 * {@code sections} table and written in the same atomic commit as the data it describes.
 *
 * <p>Contains no PHI: IDs, a keyed fingerprint, offsets, counts and error codes only.
 *
 * @param resumeAt     where splitting continues after a restart
 * @param nextSeq      seq of the next section to be produced
 * @param committedSeq last seq whose data is committed; -1 if none
 * @param publishedSeq last seq whose message the broker acknowledged, as known at this commit; -1 if none
 * @param failure      why the file is being failed; set in {@code FAILING} and {@code FAILED}, else null
 */
public record Checkpoint(
        UUID fileId,
        String fingerprint,
        String format,
        long fileSize,
        State state,
        ResumePoint resumeAt,
        long nextSeq,
        long committedSeq,
        long publishedSeq,
        Instant startedAt,
        Failure failure) {

    public enum State {
        SPLITTING, ARCHIVING, COMPLETED, FAILING, FAILED, ABANDONED;

        /** States that must be finished (or abandoned) before another file can start. */
        public boolean inProgress() {
            return this == SPLITTING || this == ARCHIVING || this == FAILING;
        }
    }

    /**
     * @param stage    where it failed: {@code intake}, {@code split}, {@code publish} or {@code archive}
     * @param offset   source byte offset of the failure, or where splitting had reached
     */
    public record Failure(String stage, ErrorCode code, long offset, Instant failedAt) {
        public Failure {
            Objects.requireNonNull(stage);
            Objects.requireNonNull(code);
            Objects.requireNonNull(failedAt);
        }
    }

    public static final String PROPERTY_PREFIX = "sdh.checkpoint.";

    public Checkpoint {
        Objects.requireNonNull(fileId);
        Objects.requireNonNull(fingerprint);
        Objects.requireNonNull(format);
        Objects.requireNonNull(state);
        Objects.requireNonNull(resumeAt);
        Objects.requireNonNull(startedAt);
        if (committedSeq != nextSeq - 1 || publishedSeq > committedSeq || publishedSeq < -1) {
            throw new IllegalArgumentException("Inconsistent checkpoint sequence numbers");
        }
        if ((failure != null) != (state == State.FAILING || state == State.FAILED)) {
            throw new IllegalArgumentException("Failure details belong to FAILING and FAILED only");
        }
    }

    /** A new file, nothing split yet. */
    public static Checkpoint start(UUID fileId, String fingerprint, String format, long fileSize, Instant startedAt) {
        return new Checkpoint(fileId, fingerprint, format, fileSize, State.SPLITTING, ResumePoint.START, 0, -1, -1,
                startedAt, null);
    }

    public static String propertyKey(String connectorId) {
        return PROPERTY_PREFIX + connectorId;
    }

    /** After committing a batch whose last section is {@code lastSeq}. */
    public Checkpoint committed(ResumePoint resume, long lastSeq, long published) {
        return new Checkpoint(fileId, fingerprint, format, fileSize, state, resume, lastSeq + 1, lastSeq,
                Math.max(publishedSeq, published), startedAt, failure);
    }

    public Checkpoint published(long published) {
        return new Checkpoint(fileId, fingerprint, format, fileSize, state, resumeAt, nextSeq, committedSeq,
                Math.max(publishedSeq, published), startedAt, failure);
    }

    /** Moves to a state without failure details (SPLITTING, ARCHIVING, COMPLETED, ABANDONED). */
    public Checkpoint withState(State s) {
        return new Checkpoint(fileId, fingerprint, format, fileSize, s, resumeAt, nextSeq, committedSeq,
                publishedSeq, startedAt, null);
    }

    public Checkpoint failing(Failure f) {
        return new Checkpoint(fileId, fingerprint, format, fileSize, State.FAILING, resumeAt, nextSeq, committedSeq,
                publishedSeq, startedAt, f);
    }

    public Checkpoint failed() {
        if (failure == null) {
            throw new IllegalStateException("Not failing");
        }
        return new Checkpoint(fileId, fingerprint, format, fileSize, State.FAILED, resumeAt, nextSeq, committedSeq,
                publishedSeq, startedAt, failure);
    }

    public long sectionCount() {
        return committedSeq + 1;
    }
}
