/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemEnd;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemResult;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Sends an item's result to its producer (design §5.3.8, STAGES.md §17.5):
 * whether it completed or failed, and where each problem is, by stage,
 * record, byte offset, line and column, and for a queue batch the message,
 * its broker ID and the earlier message it replaces. Long lists of
 * problems go in parts of at most {@value #PART_SIZE} problems and
 * {@value #PART_BYTES} bytes of them. No content is sent.
 * <p>
 * It is built from the item's end, or the stage before's
 * {@code item.progress} while it has failures open, which carry the
 * producer's reference, the item it replaces and any failure of the item
 * as a whole, and from the item's failed records in the stage's sink and
 * the tables before it, whose rows keep their message. Failures aren't
 * sent between the stages (§15.1), so the stage reads each table. It is
 * sent after the stage's item end and before it marks the item complete,
 * so after a crash it is sent again, with the same message IDs; and while
 * a failure is open, after each {@code item.progress}, as
 * {@code incomplete} (§15.6).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ResultReporter
{

    /** The most problems in one part. */
    public static final int PART_SIZE = 1_000;

    /**
     * The most bytes of problems in one part. Sealing (JWS in JWE, both
     * base64url) makes a payload about 1.8 times larger, so a full part
     * stays under the smallest broker limit, 256 KB for SQS and Service
     * Bus, even when broker IDs are long.
     */
    public static final int PART_BYTES = 96 << 10;

    /** Sends the parts. */
    private final ResultsChannel channel;

    /** Stamps when the item finished. */
    private final Clock clock;

    /**
     * Creates a reporter.
     *
     * @param channel sends the parts
     * @param clock   stamps when an item finished
     */
    public ResultReporter(ResultsChannel channel, Clock clock)
    {
        this.channel = channel;
        this.clock = clock;
    }

    /**
     * What a result says of an item, beyond its problems: from the item's
     * end, or an {@code item.progress}, that reached the stage.
     *
     * @param itemId      the item's id
     * @param intake      the intake it came in through
     * @param failure     why it couldn't be split further; null if none
     * @param producerRef the producer's reference; null if none
     * @param replaces    the item it replaces; null if none
     * @param sameAs      the earlier item it is an exact copy of (STAGES.md
     *                    §15.5a); null if none
     */
    public record Facts(UUID itemId, String intake, Problem failure,
            String producerRef, UUID replaces, UUID sameAs)
    {

        /**
         * The facts in the message that gave an item's total.
         *
         * @param total the message, opened; null if none
         * @return the facts; null if it isn't one that has them
         */
        public static Facts of(ConnectorMessage total)
        {
            return switch (total)
            {
                case ItemEnd end -> new Facts(end.itemId(), end.intake(),
                        end.failure(), end.producerRef(), end.replaces(),
                        end.sameAs());
                case ItemProgressed progressed -> new Facts(
                        progressed.itemId(), progressed.intake(),
                        progressed.failure(), progressed.producerRef(),
                        progressed.replaces(), progressed.sameAs());
                case null, default -> null;
            };
        }
    }

    /**
     * A failed record, put down to the stage whose table it is in.
     *
     * @param stage  the stage, such as {@code convert}
     * @param failed the record
     */
    public record Failure(String stage, CommittedRecords.Failed failed)
    {
    }

    /**
     * Sends an item's result, numbered by the item's completions at the
     * stage (STAGES.md §15.6e).
     *
     * @param facts       what is known of the item
     * @param status      {@code completed} or {@code incomplete}
     * @param recordCount the records it was cut into, failed ones included
     * @param failed      its failed records, at any stage, in order
     * @param completion  for {@code completed}, the completion's number;
     *                    otherwise the completions before it
     * @throws Exception if a part couldn't be sent
     */
    public void report(Facts facts, String status, long recordCount,
            List<Failure> failed, int completion) throws Exception
    {
        for (ItemResult part : parts(facts, status, recordCount, failed,
                clock.instant(), PART_SIZE, PART_BYTES, completion))
        {
            channel.send(part);
        }
    }

    /**
     * Builds an item's result, in parts, numbered by its completions.
     *
     * @param facts       what is known of the item
     * @param asked       {@code completed} or {@code incomplete}; an item
     *                    that couldn't be split further is {@code failed}
     * @param recordCount the records it was cut into, failed ones included
     * @param failed      its failed records, in order
     * @param finishedAt  when it finished, or was reported
     * @param partSize    the most problems in a part
     * @param partBytes   the most bytes of problems in a part; a single
     *                    larger problem still gets a part of its own
     * @param completion  for {@code completed}, the completion's number;
     *                    otherwise the completions before it
     * @return the parts, at least one
     */
    public static List<ItemResult> parts(Facts facts, String asked,
            long recordCount, List<Failure> failed, Instant finishedAt,
            int partSize, int partBytes, int completion)
    {
        String status = facts.failure() == null ? asked : ItemResult.FAILED;
        List<Problem> problems = new ArrayList<>(failed.size());
        for (Failure failure : failed)
        {
            CommittedRecords.Failed record = failure.failed();
            problems.add(new Problem(record.errorCode(), failure.stage(),
                    record.seq(), record.sourceOffset(), record.errorAt(),
                    record.message(), record.messageRef(),
                    record.messageReplaces()));
        }
        List<Integer> starts = new ArrayList<>();
        starts.add(0);
        long bytes = 0;
        for (int index = 0; index < problems.size(); index++)
        {
            int size = PayloadCodec.encodedSize(problems.get(index)) + 1;
            int inPart = index - starts.getLast();
            if (inPart > 0 && (inPart == partSize || bytes + size > partBytes))
            {
                starts.add(index);
                bytes = 0;
            }
            bytes += size;
        }
        starts.add(problems.size());
        int partCount = starts.size() - 1;
        List<ItemResult> parts = new ArrayList<>(partCount);
        for (int part = 0; part < partCount; part++)
        {
            parts.add(new ItemResult(facts.itemId(), facts.intake(), status,
                    recordCount, failed.size(), facts.failure(),
                    problems.size(), part + 1, partCount,
                    problems.subList(starts.get(part), starts.get(part + 1)),
                    facts.producerRef(), facts.replaces(), finishedAt,
                    completion, facts.sameAs()));
        }
        return parts;
    }
}
