/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * The progress of the file a connector is working on (design §4.1, §9.1).
 * It is stored, MAC'd, in a table property of the {@code records} table,
 * and written in the same atomic commit as the data it describes.
 * <p>
 * It holds no PHI: IDs, a keyed fingerprint, offsets, counts and error
 * codes only.
 *
 * @param itemId       the file's ID
 * @param fingerprint  the file's keyed fingerprint
 * @param format       the name of the file's format
 * @param itemSize     the file's size, in bytes
 * @param state        where the file is in its life
 * @param resumeAt     where splitting continues after a restart
 * @param nextSeq      the seq of the next record to be produced
 * @param committedSeq the last seq whose data is committed; -1 if none
 * @param publishedSeq the last seq whose message the broker acknowledged,
 *                     as known at this commit; -1 if none
 * @param startedAt    when the file was started
 * @param failure      why the file is being failed; set in {@code FAILING}
 *                     and {@code FAILED}, otherwise null
 * @param restarts     how many times splitting has been resumed from this
 *                     checkpoint without committing a batch since (design
 *                     §9.2); 0 for a new file and after every batch commit.
 *                     It rises only when the connector stops part-way
 *                     through a batch, so a record that stops it every
 *                     time (by running it out of memory, say) is noticed
 *                     instead of crashing the connector forever. An
 *                     attempt that ends because a service was down is
 *                     taken back ({@link #notCounted})
 * @param version      the split stage's version the file was begun under
 *                     (STAGES.md §14): only that version resumes it; null
 *                     for one begun before versions were kept
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record Checkpoint(
        UUID itemId,
        String fingerprint,
        String format,
        long itemSize,
        State state,
        ResumePoint resumeAt,
        long nextSeq,
        long committedSeq,
        long publishedSeq,
        Instant startedAt,
        Failure failure,
        int restarts,
        String version)
{

    /** The prefix of the table property that holds a checkpoint. */
    public static final String PROPERTY_PREFIX = "sdh.checkpoint.";

    /** Where a file is in its life. */
    public enum State
    {
        /** Records are being split, stored and published. */
        SPLITTING,

        /** Every record is stored; the completion is being written. */
        ARCHIVING,

        /** Finished: every record stored and published. */
        COMPLETED,

        /** The decision to fail the file is durable; finishing that. */
        FAILING,

        /** Finished: the file couldn't be split. */
        FAILED,

        /** Given up without finishing. */
        ABANDONED,

        /**
         * Part way, begun under another version of the split stage; waits
         * for an operator (STAGES.md §14.3). Not resumed, not reconciled.
         */
        HELD,

        /**
         * Held, then ended by an operator ({@code release --close},
         * STAGES.md §15.6e), to be landed again: not resumed, reconciled or
         * announced. Its records already stored stay.
         */
        CLOSED;

        /**
         * Tells whether this state must be finished (or abandoned) before
         * another file can start.
         *
         * @return true for {@code SPLITTING}, {@code ARCHIVING} and
         *         {@code FAILING}
         */
        public boolean inProgress()
        {
            return this == SPLITTING || this == ARCHIVING || this == FAILING;
        }
    }

    /**
     * Why a file failed.
     *
     * @param stage    where it failed: {@code intake}, {@code split},
     *                 {@code publish} or {@code archive}
     * @param code     the error code
     * @param offset   the source byte offset of the failure, or where
     *                 splitting had reached
     * @param failedAt when it failed
     */
    public record Failure(String stage, ErrorCode code, long offset,
            Instant failedAt)
    {

        /**
         * Checks the fields.
         *
         * @param stage    where it failed
         * @param code     the error code
         * @param offset   where in the file
         * @param failedAt when
         * @throws NullPointerException if the stage, code or time is null
         */
        public Failure
        {
            Objects.requireNonNull(stage);
            Objects.requireNonNull(code);
            Objects.requireNonNull(failedAt);
        }
    }

    /**
     * Checks that the fields fit together.
     *
     * @param itemId       the file's ID
     * @param fingerprint  the file's keyed fingerprint
     * @param format       the file's format
     * @param itemSize     the file's size
     * @param state        where the file is in its life
     * @param resumeAt     where splitting continues
     * @param nextSeq      the next record's seq
     * @param committedSeq the last committed seq
     * @param publishedSeq the last acknowledged seq
     * @param startedAt    when the file was started
     * @param failure      why it is failing; null otherwise
     * @param restarts     restarts without progress
     * @throws NullPointerException     if a required field is null
     * @throws IllegalArgumentException if the sequence numbers don't fit,
     *                                  failure details are set in the wrong
     *                                  state, or restarts is negative
     */
    public Checkpoint
    {
        Objects.requireNonNull(itemId);
        Objects.requireNonNull(fingerprint);
        Objects.requireNonNull(format);
        Objects.requireNonNull(state);
        Objects.requireNonNull(resumeAt);
        Objects.requireNonNull(startedAt);
        if (committedSeq != nextSeq - 1 || publishedSeq > committedSeq
                || publishedSeq < -1)
        {
            throw new IllegalArgumentException(
                    "Inconsistent checkpoint sequence numbers");
        }
        if ((failure != null) != (state == State.FAILING
                || state == State.FAILED))
        {
            throw new IllegalArgumentException(
                    "Failure details belong to FAILING and FAILED only");
        }
        if (restarts < 0)
        {
            throw new IllegalArgumentException("Negative restart count");
        }
    }

    /**
     * A new file, with nothing split yet.
     *
     * @param itemId      the file's ID
     * @param fingerprint its keyed fingerprint
     * @param format      its format
     * @param itemSize    its size
     * @param startedAt   when it was started
     * @return the checkpoint, {@code SPLITTING} from the start
     */
    public static Checkpoint start(UUID itemId, String fingerprint,
            String format, long itemSize, Instant startedAt)
    {
        return start(itemId, fingerprint, format, itemSize, startedAt, null);
    }

    /**
     * A new file, with nothing split yet, begun under a version of the split
     * stage (STAGES.md §14): only that version resumes it.
     *
     * @param itemId      the file's ID
     * @param fingerprint its keyed fingerprint
     * @param format      its format
     * @param itemSize    its size
     * @param startedAt   when it was started
     * @param version     the split stage's version; null if not known
     * @return the checkpoint, {@code SPLITTING} from the start
     */
    public static Checkpoint start(UUID itemId, String fingerprint,
            String format, long itemSize, Instant startedAt, String version)
    {
        return new Checkpoint(itemId, fingerprint, format, itemSize,
                State.SPLITTING, ResumePoint.START, 0, -1, -1, startedAt,
                null, 0, version);
    }

    /**
     * The table property a connector's checkpoint is kept in.
     *
     * @param connectorId the connector's ID
     * @return the property's name
     */
    public static String propertyKey(String connectorId)
    {
        return PROPERTY_PREFIX + connectorId;
    }

    /**
     * After committing a batch. That is progress, so the restart count is
     * reset.
     *
     * @param resume    where splitting continues
     * @param lastSeq   the batch's last record
     * @param published the last seq acknowledged so far
     * @return the new checkpoint
     */
    public Checkpoint committed(ResumePoint resume, long lastSeq,
            long published)
    {
        return new Checkpoint(itemId, fingerprint, format, itemSize, state,
                resume, lastSeq + 1, lastSeq,
                Math.max(publishedSeq, published), startedAt, failure, 0,
                version);
    }

    /**
     * Splitting is resuming from this checkpoint: one more restart without
     * progress.
     *
     * @return the new checkpoint
     */
    public Checkpoint resumed()
    {
        return new Checkpoint(itemId, fingerprint, format, itemSize, state,
                resumeAt, nextSeq, committedSeq, publishedSeq, startedAt,
                failure, restarts + 1, version);
    }

    /**
     * Takes back the restart {@link #resumed} counted, for an attempt that
     * ended because a service it needs was down (STAGES.md §12): that says
     * nothing about the item.
     *
     * @return the new checkpoint; this one if no restart is counted
     */
    public Checkpoint notCounted()
    {
        if (restarts == 0)
        {
            return this;
        }
        return new Checkpoint(itemId, fingerprint, format, itemSize, state,
                resumeAt, nextSeq, committedSeq, publishedSeq, startedAt,
                failure, restarts - 1, version);
    }

    /**
     * Records acknowledgements. The published seq never goes back.
     *
     * @param published the last seq acknowledged
     * @return the new checkpoint
     */
    public Checkpoint published(long published)
    {
        return new Checkpoint(itemId, fingerprint, format, itemSize, state,
                resumeAt, nextSeq, committedSeq,
                Math.max(publishedSeq, published), startedAt, failure,
                restarts, version);
    }

    /**
     * Moves to a state without failure details.
     *
     * @param next {@code SPLITTING}, {@code ARCHIVING}, {@code COMPLETED}
     *             or {@code ABANDONED}
     * @return the new checkpoint
     */
    public Checkpoint withState(State next)
    {
        return new Checkpoint(itemId, fingerprint, format, itemSize, next,
                resumeAt, nextSeq, committedSeq, publishedSeq, startedAt,
                null, restarts, version);
    }

    /**
     * Resumes a held item under another version of the split stage, by an
     * operator's choice (STAGES.md §15.6e): {@code SPLITTING} again, under
     * that version from here on.
     *
     * @param next the version it resumes under
     * @return the new checkpoint
     * @throws IllegalStateException if it isn't {@code HELD}
     */
    public Checkpoint released(String next)
    {
        if (state != State.HELD)
        {
            throw new IllegalStateException("Not held");
        }
        return new Checkpoint(itemId, fingerprint, format, itemSize,
                State.SPLITTING, resumeAt, nextSeq, committedSeq,
                publishedSeq, startedAt, null, restarts, next);
    }

    /**
     * Starts failing the file.
     *
     * @param why why it fails
     * @return the new checkpoint, {@code FAILING}
     */
    public Checkpoint failing(Failure why)
    {
        return new Checkpoint(itemId, fingerprint, format, itemSize,
                State.FAILING, resumeAt, nextSeq, committedSeq, publishedSeq,
                startedAt, why, restarts, version);
    }

    /**
     * Finishes failing the file.
     *
     * @return the new checkpoint, {@code FAILED}
     * @throws IllegalStateException if it isn't failing
     */
    public Checkpoint failed()
    {
        if (failure == null)
        {
            throw new IllegalStateException("Not failing");
        }
        return new Checkpoint(itemId, fingerprint, format, itemSize,
                State.FAILED, resumeAt, nextSeq, committedSeq, publishedSeq,
                startedAt, failure, restarts, version);
    }

    /**
     * How many records are committed.
     *
     * @return the count
     */
    public long recordCount()
    {
        return committedSeq + 1;
    }
}
