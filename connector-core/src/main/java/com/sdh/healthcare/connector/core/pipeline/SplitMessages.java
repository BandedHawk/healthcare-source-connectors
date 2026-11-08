/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint.Failure;
import com.sdh.healthcare.connector.core.landing.LandedRefs;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import java.time.Instant;
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
                refs.messageRef(message), refs.messageReplaced(message));
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
                refs.replaces(), problem, checkpoint.startedAt(), finishedAt);
    }
}
