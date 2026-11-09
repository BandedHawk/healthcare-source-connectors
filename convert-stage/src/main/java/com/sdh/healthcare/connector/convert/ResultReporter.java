/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemResult;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import com.sdh.healthcare.connector.core.storage.CommittedRecords.Failed;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Sends an item's final result to its producer (design §5.3.8, STAGES.md
 * §9): whether it completed or failed, and where each problem is, by
 * record, byte offset, line and column, and for a queue batch the message,
 * its broker ID and the earlier message it replaces. Long lists of
 * problems go in parts of at most {@value #PART_SIZE} problems and
 * {@value #PART_BYTES} bytes of them. No content is sent.
 * <p>
 * It is built from the item's {@code item.split}, or the split stage's
 * {@code item.progress} while it has failures open, which carries the
 * producer's reference, the item it replaces and any failure of the item
 * as a whole, and from the item's failed records: those the splitter
 * couldn't parse, in {@code split_records}, and those that failed
 * conversion, in {@code records}, whose rows keep their message. Failures
 * aren't sent between the stages (STAGES.md §15.1), so the convert stage
 * reads both. It sends the result after {@code item.converted} and before
 * it marks the item complete, so after a crash it is sent again, with the
 * same message IDs; and while a failure is open, after each
 * {@code item.progress}, as {@code incomplete} (§15.6).
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

    /** The stage of a piece the splitter couldn't parse. */
    static final String SPLIT = "split";

    /** The stage of a record that failed conversion. */
    static final String CONVERT = "convert";

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
     * What a result says of an item, beyond its problems: from the split
     * stage's {@code item.split} or {@code item.progress}.
     *
     * @param itemId      the item's id
     * @param intake      the intake it came in through
     * @param failure     why it couldn't be split further; null if none
     * @param producerRef the producer's reference; null if none
     * @param replaces    the item it replaces; null if none
     */
    public record Facts(UUID itemId, String intake, Problem failure,
            String producerRef, UUID replaces)
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
                case ItemSplit split -> new Facts(split.itemId(),
                        split.intake(), split.failure(), split.producerRef(),
                        split.replaces());
                case ItemProgressed progressed -> new Facts(
                        progressed.itemId(), progressed.intake(),
                        progressed.failure(), progressed.producerRef(),
                        progressed.replaces());
                case null, default -> null;
            };
        }
    }

    /**
     * Sends an item's result.
     *
     * @param total  the item's {@code item.split}
     * @param failed its failed records, in order
     * @throws Exception if a part couldn't be sent
     */
    public void report(ItemSplit total, List<Failed> failed) throws Exception
    {
        for (ItemResult part : parts(total, failed, clock.instant(),
                PART_SIZE, PART_BYTES))
        {
            channel.send(part);
        }
    }

    /**
     * Sends an item's result: {@code failed} if it couldn't be split
     * further, otherwise the status given.
     *
     * @param facts       what is known of the item
     * @param status      {@code completed} or {@code incomplete}
     * @param recordCount the records it was cut into, failed ones included
     * @param failed      its failed records, at either stage, in order
     * @throws Exception if a part couldn't be sent
     */
    public void report(Facts facts, String status, long recordCount,
            List<Failed> failed) throws Exception
    {
        report(facts, status, recordCount, failed, 0);
    }

    /**
     * Sends an item's result, numbered by the item's completions at the
     * stage (STAGES.md §15.6e).
     *
     * @param facts       what is known of the item
     * @param status      {@code completed} or {@code incomplete}
     * @param recordCount the records it was cut into, failed ones included
     * @param failed      its failed records, at either stage, in order
     * @param completion  for {@code completed}, the completion's number;
     *                    otherwise the completions before it
     * @throws Exception if a part couldn't be sent
     */
    public void report(Facts facts, String status, long recordCount,
            List<Failed> failed, int completion) throws Exception
    {
        for (ItemResult part : parts(facts, status, recordCount, failed,
                clock.instant(), PART_SIZE, PART_BYTES, completion))
        {
            channel.send(part);
        }
    }

    /**
     * Builds an item's result, in parts.
     *
     * @param total      the item's {@code item.split}
     * @param failed     its failed records, in order
     * @param finishedAt when it finished
     * @param partSize   the most problems in a part
     * @param partBytes  the most bytes of problems in a part; a single
     *                   larger problem still gets a part of its own
     * @return the parts, at least one
     */
    public static List<ItemResult> parts(ItemSplit total,
            List<Failed> failed,
            Instant finishedAt, int partSize, int partBytes)
    {
        return parts(Facts.of(total), ItemResult.COMPLETED,
                total.recordCount(), failed, finishedAt, partSize,
                partBytes);
    }

    /**
     * Builds an item's result, in parts.
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
     * @return the parts, at least one
     */
    public static List<ItemResult> parts(Facts facts, String asked,
            long recordCount, List<Failed> failed, Instant finishedAt,
            int partSize, int partBytes)
    {
        return parts(facts, asked, recordCount, failed, finishedAt, partSize,
                partBytes, 0);
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
     * @param partBytes   the most bytes of problems in a part
     * @param completion  for {@code completed}, the completion's number;
     *                    otherwise the completions before it
     * @return the parts, at least one
     */
    public static List<ItemResult> parts(Facts facts, String asked,
            long recordCount, List<Failed> failed, Instant finishedAt,
            int partSize, int partBytes, int completion)
    {
        String status = facts.failure() == null ? asked : ItemResult.FAILED;
        List<Problem> problems = new ArrayList<>(failed.size());
        for (Failed record : failed)
        {
            problems.add(new Problem(record.errorCode(),
                    stageOf(record.errorCode()), record.seq(),
                    record.sourceOffset(), record.errorAt(), record.message(),
                    record.messageRef(), record.messageReplaces()));
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
                    completion));
        }
        return parts;
    }

    /**
     * The stage a failed record's problem is put down to: the convert
     * stage for a {@code CONVERT_*} code, the split stage for any other
     * (a piece it couldn't parse).
     *
     * @param code the record's error code
     * @return {@code convert} or {@code split}
     */
    static String stageOf(String code)
    {
        return code.startsWith("CONVERT_") ? CONVERT : SPLIT;
    }
}
