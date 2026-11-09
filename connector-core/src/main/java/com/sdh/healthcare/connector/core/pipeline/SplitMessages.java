/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint.Failure;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.landing.LandedRefs;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The split stage's messages, as rebuilt from what it stored: a committed
 * record's {@code record.split} from its row of {@code split_records}, and
 * an item's {@code item.split} from its checkpoint. The stage sends a
 * record again with these after a crash, and announces a finished item
 * again with them; its replay (STAGES.md §13.2) sends them all again. Each
 * message is the one first sent, but for the snapshot it names.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SplitMessages
{

    /** The split stage's name, as {@code item.progress} gives it. */
    static final String STAGE = "split";

    /** Not instantiated. */
    private SplitMessages()
    {
    }

    /**
     * A committed record's {@code record.split}, placed in its landed
     * message for a batch.
     *
     * @param itemId its item
     * @param row    its row of {@code split_records}
     * @param ref    where it is stored: the row's day and KEK, and a
     *               snapshot that holds it
     * @param refs   what landing kept about its item
     * @return the message
     */
    public static RecordSplit recordSplit(UUID itemId,
            CommittedRecords.Row row, StorageRef ref, LandedRefs refs)
    {
        int message = refs.message(row.sourceOffset());
        return new RecordSplit(row.recordId(), itemId, row.seq(),
                row.sourceOffset(), row.sourceLength(), row.format(),
                row.contentType(), row.plaintextLength(), row.chunkCount(),
                ref, row.origin(), row.errorCode(), row.errorAt(), message,
                refs.messageRef(message), refs.messageReplaced(message),
                row.stageVersion(), row.correctionOf());
    }

    /**
     * Whether a committed record is announced downstream: only one that
     * passed. A piece the splitter couldn't parse is kept in
     * {@code split_records} with its error, and is in the fail store, not
     * sent on (STAGES.md §15.1, rule 2).
     *
     * @param row its row of {@code split_records}
     * @return true if it passed
     */
    public static boolean announced(CommittedRecords.Row row)
    {
        return row.errorCode() == null;
    }

    /**
     * An item's failures, as its fail store records them: each piece the
     * splitter couldn't parse, and the item as a whole if it couldn't be
     * split to the end.
     *
     * @param checkpoint its checkpoint
     * @param failed     its rows of {@code split_records} with an error
     * @return the failures, in sequence order, the item's own first
     */
    public static List<FailureStore.Failure> failures(Checkpoint checkpoint,
            List<CommittedRecords.Row> failed)
    {
        List<FailureStore.Failure> failures = new ArrayList<>();
        Failure failure = checkpoint.failure();
        if (failure != null)
        {
            failures.add(new FailureStore.Failure(checkpoint.itemId(),
                    FailureStore.WHOLE_ITEM, null, failure.code().name(),
                    failure.offset(), checkpoint.version()));
        }
        for (CommittedRecords.Row row : failed)
        {
            failures.add(new FailureStore.Failure(checkpoint.itemId(),
                    row.seq(), row.recordId(), row.errorCode(),
                    row.sourceOffset(), row.stageVersion()));
        }
        return failures;
    }

    /**
     * The message that ends an item at the split stage (STAGES.md §15.6):
     * {@code item.split} when none of its failures is open, counting the
     * records sent on; otherwise {@code item.progress}, sent again whenever
     * its counts change.
     *
     * @param checkpoint  its checkpoint
     * @param intake      the intake it came in through
     * @param failedCount its pieces stored in their original form
     * @param counts      its failures in the fail store, by state
     * @param digest      its digest, from landing
     * @param refs        what landing kept about it
     * @param endedAt     when its splitting ended, as stored; null if not
     *                    yet noted
     * @param now         the time, if neither that nor its failure gives
     *                    one
     * @return the message
     */
    public static ConnectorMessage itemEnd(Checkpoint checkpoint,
            String intake, long failedCount, FailureStore.Counts counts,
            String digest, LandedRefs refs, Instant endedAt, Instant now)
    {
        return itemEnd(checkpoint, intake, failedCount, counts, digest, refs,
                endedAt, now, Corrected.NONE);
    }

    /**
     * What operators' corrections that passed add to an item at the split
     * stage (STAGES.md §15.6d).
     *
     * @param pieces    its failed pieces a correction replaced: each
     *                  counted passed, not failed
     * @param added     the records a correction of the item as a whole
     *                  added after those already stored
     * @param wholeItem whether its failure as a whole is resolved, so its
     *                  end names no failure
     */
    public record Corrected(long pieces, long added, boolean wholeItem)
    {

        /** No corrections. */
        public static final Corrected NONE = new Corrected(0, 0, false);

        /**
         * Counts an item's corrections from its rows.
         *
         * @param checkpoint  its checkpoint
         * @param corrections its rows of {@code split_records} made from
         *                    corrections
         * @param wholeItem   whether its failure as a whole is resolved
         * @return the counts
         */
        public static Corrected of(Checkpoint checkpoint,
                List<CommittedRecords.Row> corrections, boolean wholeItem)
        {
            java.util.Set<Long> pieces = new java.util.TreeSet<>();
            java.util.Set<Long> added = new java.util.TreeSet<>();
            for (CommittedRecords.Row row : corrections)
            {
                if (row.errorCode() != null)
                {
                    continue;
                }
                (row.seq() < checkpoint.recordCount() ? pieces : added)
                        .add(row.seq());
            }
            return new Corrected(pieces.size(), added.size(), wholeItem);
        }
    }

    /**
     * The message that ends an item at the split stage, as {@link
     * #itemEnd(Checkpoint, String, long, FailureStore.Counts, String,
     * LandedRefs, Instant, Instant)} makes it, with what operators'
     * corrections added: a corrected piece counts as passed, the records a
     * correction of the whole item added count among its records, and once
     * that correction passed the item's end names no failure.
     *
     * @param checkpoint  its checkpoint
     * @param intake      the intake it came in through
     * @param failedCount its pieces stored in their original form
     * @param counts      its failures in the fail store, by state
     * @param digest      its digest, from landing
     * @param refs        what landing kept about it
     * @param endedAt     when its splitting ended, as stored; null if not
     *                    yet noted
     * @param now         the time, if neither that nor its failure gives
     *                    one
     * @param corrected   what its corrections added
     * @return the message
     */
    public static ConnectorMessage itemEnd(Checkpoint checkpoint,
            String intake, long failedCount, FailureStore.Counts counts,
            String digest, LandedRefs refs, Instant endedAt, Instant now,
            Corrected corrected)
    {
        return itemEnd(checkpoint, intake, failedCount, counts, digest, refs,
                endedAt, now, corrected, counts.open() == 0 ? 1 : 0);
    }

    /**
     * The message that ends an item at the split stage, as {@link
     * #itemEnd(Checkpoint, String, long, FailureStore.Counts, String,
     * LandedRefs, Instant, Instant, Corrected)} makes it, numbered by the
     * item's completions (STAGES.md §15.6e).
     *
     * @param checkpoint  its checkpoint
     * @param intake      the intake it came in through
     * @param failedCount its pieces stored in their original form
     * @param counts      its failures in the fail store, by state
     * @param digest      its digest, from landing
     * @param refs        what landing kept about it
     * @param endedAt     when its splitting ended, as stored; null if not
     *                    yet noted
     * @param now         the time, if neither that nor its failure gives
     *                    one
     * @param corrected   what its corrections added
     * @param completion  for {@code item.split}, the completion's number;
     *                    for {@code item.progress}, the completions before
     *                    it, as {@link
     *                    com.sdh.healthcare.connector.core.checkpoint
     *                    .ItemCheckpoints#announced} gives
     * @return the message
     */
    public static ConnectorMessage itemEnd(Checkpoint checkpoint,
            String intake, long failedCount, FailureStore.Counts counts,
            String digest, LandedRefs refs, Instant endedAt, Instant now,
            Corrected corrected, int completion)
    {
        ItemSplit split = itemSplit(checkpoint, intake, failedCount, digest,
                refs, endedAt, now);
        long records = checkpoint.recordCount() + corrected.added();
        long failed = failedCount - corrected.pieces();
        long passed = records - failed;
        Problem problem = corrected.wholeItem() ? null : split.failure();
        if (counts.open() == 0)
        {
            return new ItemSplit(split.itemId(), intake, passed, failed,
                    split.itemSize(), digest, split.producerRef(),
                    split.replaces(), problem, split.startedAt(),
                    split.completedAt(), split.versions(), completion);
        }
        return new ItemProgressed(split.itemId(), intake, STAGE, records,
                passed, counts.open(), counts.declined(), problem,
                split.producerRef(), split.replaces(), split.completedAt(),
                split.versions(), completion);
    }

    /**
     * Counts an item's failures when there is no fail store (tests, and a
     * stage run without a database): each is open.
     *
     * @param checkpoint  its checkpoint
     * @param failedCount its pieces stored in their original form
     * @return the counts
     */
    public static FailureStore.Counts unstored(Checkpoint checkpoint,
            long failedCount)
    {
        return new FailureStore.Counts(failedCount
                + (checkpoint.failure() == null ? 0 : 1), 0, 0);
    }

    /**
     * A finished item's {@code item.split}.
     *
     * @param checkpoint  its checkpoint
     * @param intake      the intake it came in through
     * @param failedCount its records stored in their original form
     * @param digest      its digest, from landing
     * @param refs        what landing kept about it
     * @param endedAt     when its splitting ended, as stored; null if not
     *                    yet noted
     * @param now         the time, if neither that nor its failure gives
     *                    one
     * @return the message
     */
    public static ItemSplit itemSplit(Checkpoint checkpoint, String intake,
            long failedCount, String digest, LandedRefs refs,
            Instant endedAt, Instant now)
    {
        Failure failure = checkpoint.failure();
        Problem problem = failure == null ? null
                : refs.place(new Problem(failure.code().name(),
                        failure.stage(), -1, failure.offset(), null, -1,
                        null, null));
        Instant finishedAt = endedAt != null ? endedAt
                : failure != null ? failure.failedAt()
                : now;
        return new ItemSplit(checkpoint.itemId(), intake,
                checkpoint.recordCount(), failedCount,
                checkpoint.itemSize(), digest, refs.producerRef(),
                refs.replaces(), problem, checkpoint.startedAt(), finishedAt,
                checkpoint.version() == null ? List.of()
                        : List.of(checkpoint.version()));
    }
}
