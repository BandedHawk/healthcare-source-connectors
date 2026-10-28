package com.sdh.healthcare.connector.core.checkpoint;

import com.sdh.healthcare.connector.core.split.ResumePoint;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Progress of the file a connector is working on (design §9.1). Stored, MAC'd, in a table property of the
 * {@code sections} table and written in the same atomic commit as the data it describes.
 *
 * <p>Contains no PHI: IDs, a keyed fingerprint, offsets and counts only.
 *
 * @param resumeAt     where splitting continues after a restart
 * @param nextSeq      seq of the next section to be produced
 * @param committedSeq last seq whose data is committed; -1 if none
 * @param publishedSeq last seq whose message the broker acknowledged, as known at this commit; -1 if none
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
        Instant startedAt) {

    public enum State { SPLITTING, ARCHIVING, COMPLETED, FAILED, ABANDONED }

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
    }

    /** A new file, nothing split yet. */
    public static Checkpoint start(UUID fileId, String fingerprint, String format, long fileSize, Instant startedAt) {
        return new Checkpoint(fileId, fingerprint, format, fileSize, State.SPLITTING, ResumePoint.START, 0, -1, -1,
                startedAt);
    }

    public static String propertyKey(String connectorId) {
        return PROPERTY_PREFIX + connectorId;
    }

    /** After committing a batch whose last section is {@code lastSeq}. */
    public Checkpoint committed(ResumePoint resume, long lastSeq, long published) {
        return new Checkpoint(fileId, fingerprint, format, fileSize, state, resume, lastSeq + 1, lastSeq,
                Math.max(publishedSeq, published), startedAt);
    }

    public Checkpoint published(long published) {
        return new Checkpoint(fileId, fingerprint, format, fileSize, state, resumeAt, nextSeq, committedSeq,
                Math.max(publishedSeq, published), startedAt);
    }

    public Checkpoint withState(State s) {
        return new Checkpoint(fileId, fingerprint, format, fileSize, s, resumeAt, nextSeq, committedSeq,
                publishedSeq, startedAt);
    }

    public long sectionCount() {
        return committedSeq + 1;
    }
}
