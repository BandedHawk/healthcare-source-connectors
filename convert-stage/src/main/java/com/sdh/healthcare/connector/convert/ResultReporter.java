/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

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

/**
 * Sends an item's final result to its producer (design §5.3.8, STAGES.md
 * §9): whether it completed or failed, and where each problem is, by
 * record, byte offset, line and column, and for a queue batch the message,
 * its broker ID and the earlier message it replaces. Long lists of
 * problems go in parts of at most {@value #PART_SIZE} problems and
 * {@value #PART_BYTES} bytes of them. No content is sent.
 * <p>
 * It is built from the item's {@code item.split}, which carries the
 * producer's reference, the item it replaces and any failure of the item
 * as a whole, and from the item's failed records in {@code records}, whose
 * rows keep their message. The convert stage sends it after
 * {@code item.converted} and before it marks the item complete, so after a
 * crash it is sent again, with the same message IDs.
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
        String status = total.failure() == null ? ItemResult.COMPLETED
                : ItemResult.FAILED;
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
            parts.add(new ItemResult(total.itemId(), total.intake(), status,
                    total.recordCount(), failed.size(), total.failure(),
                    problems.size(), part + 1, partCount,
                    problems.subList(starts.get(part), starts.get(part + 1)),
                    total.producerRef(), total.replaces(), finishedAt));
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
