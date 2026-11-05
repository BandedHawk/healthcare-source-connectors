/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.inbox;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemResult;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint.Failure;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.RecordIndex;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor.Outcome;
import com.sdh.healthcare.connector.core.pipeline.PublishFailedException;
import com.sdh.healthcare.connector.core.storage.CommittedSections;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.data.Record;

/**
 * Sends an item's final result to its producer (design §5.3.8): whether
 * it completed or failed, and where each problem is, by section, byte
 * offset, line and column, and for a message batch the message and its
 * broker ID. Long lists of problems go in parts of at most
 * {@value #PART_SIZE} problems and {@value #PART_BYTES} bytes of them. No
 * content is sent.
 * <p>
 * The worker sends the result before it moves the inbox past the item, so
 * a crash means it is sent again with the same message IDs.
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

    /** The stage of a section that failed conversion. */
    static final String CONVERT = "convert";

    /** Sends the parts. */
    private final ResultsChannel channel;

    /** Reads the item's failed sections. */
    private final ItemProcessor processor;

    /** The {@code landed_items} table. */
    private final LandedItemsTable landedItems;

    /** Unwraps the landed items' data keys. */
    private final KeyProvider keys;

    /** The intake the splitter serves. */
    private final String intake;

    /** Stamps when a completed item finished. */
    private final Clock clock;

    /**
     * Creates a reporter.
     *
     * @param channel     sends the parts
     * @param processor   reads the item's failed sections
     * @param landedItems the {@code landed_items} table
     * @param keys        unwraps the landed items' data keys
     * @param intake      the intake the splitter serves
     * @param clock       stamps when a completed item finished
     */
    public ResultReporter(ResultsChannel channel, ItemProcessor processor,
            LandedItemsTable landedItems, KeyProvider keys, String intake,
            Clock clock)
    {
        this.channel = channel;
        this.processor = processor;
        this.landedItems = landedItems;
        this.keys = keys;
        this.intake = intake;
        this.clock = clock;
    }

    /**
     * Sends an item's result. An abandoned item has none: it wasn't
     * finished, and is taken up again when it next arrives.
     *
     * @param outcome how the item ended
     * @throws PublishFailedException if a part couldn't be sent
     * @throws InterruptedException   if the wait is interrupted
     * @throws Exception              if the tables can't be read
     */
    public void report(Outcome outcome) throws Exception
    {
        if (outcome.result() == ItemProcessor.Result.ABANDONED)
        {
            return;
        }
        Optional<Record> summary = LandedItems.summary(landedItems.get(),
                outcome.fileId());
        Landed landed = summary.isEmpty() ? Landed.NONE
                : new Landed(
                        LandedItems.originalName(summary.get(), keys)
                                .orElse(null),
                        LandedItems.replaces(summary.get()).orElse(null),
                        LandedItems.records(summary.get()).orElse(null),
                        LandedItems.messageRefs(summary.get(), keys)
                                .orElse(null),
                        LandedItems.messageReplaces(summary.get(), keys)
                                .orElse(null));
        Instant finishedAt = outcome.failure() != null
                ? outcome.failure().failedAt() : clock.instant();
        for (ItemResult part : parts(outcome, intake,
                processor.failedSections(outcome.fileId()), landed,
                finishedAt, PART_SIZE, PART_BYTES))
        {
            channel.send(part);
        }
    }

    /**
     * What the landing node kept about an item that goes in its result.
     *
     * @param producerRef the producer's own reference; null if none
     * @param replaces    the item it replaces; null if none
     * @param records     where each message of a batch starts; null if
     *                    the item isn't a batch
     * @param messageRefs each message's broker ID, or null; null if not
     *                    kept
     * @param messageReplaces the earlier message each message replaces, or
     *                    null; null if none does
     */
    record Landed(String producerRef, UUID replaces, RecordIndex records,
            List<String> messageRefs, List<String> messageReplaces)
    {

        /** An item whose summary is gone. */
        static final Landed NONE = new Landed(null, null, null, null, null);
    }

    /**
     * Builds an item's result, in parts.
     *
     * @param outcome    how the item ended, completed or failed
     * @param intake     the intake
     * @param failed     its sections that failed conversion, in order
     * @param landed     what landing kept about it
     * @param finishedAt when it finished
     * @param partSize   the most problems in a part
     * @param partBytes  the most bytes of problems in a part; a single
     *                   larger problem still gets a part of its own
     * @return the parts, at least one
     */
    static List<ItemResult> parts(Outcome outcome, String intake,
            List<CommittedSections.Failed> failed, Landed landed,
            Instant finishedAt, int partSize, int partBytes)
    {
        String status = outcome.result() == ItemProcessor.Result.COMPLETED
                ? ItemResult.COMPLETED : ItemResult.FAILED;
        Failure failure = outcome.failure();
        Problem whole = failure == null ? null
                : problem(failure.code().name(), failure.stage(), -1,
                        failure.offset(), null, landed);
        List<Problem> problems = new ArrayList<>(failed.size());
        for (CommittedSections.Failed section : failed)
        {
            problems.add(problem(section.errorCode(), CONVERT, section.seq(),
                    section.sourceOffset(), section.errorAt(), landed));
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
            parts.add(new ItemResult(outcome.fileId(), intake, status,
                    outcome.sectionCount(), failed.size(), whole,
                    problems.size(), part + 1, partCount,
                    problems.subList(starts.get(part), starts.get(part + 1)),
                    landed.producerRef(), landed.replaces(), finishedAt));
        }
        return parts;
    }

    /**
     * A problem, placed in its message when the item is a batch.
     *
     * @param code   the error code
     * @param stage  where it was found
     * @param seq    the section, or -1 for the whole item
     * @param offset its byte offset in the item
     * @param at     its line and column; null if unknown
     * @param landed what landing kept about the item
     * @return the problem
     */
    private static Problem problem(String code, String stage, long seq,
            long offset, SourcePosition at, Landed landed)
    {
        int message = message(landed.records(), offset);
        return new Problem(code, stage, seq, offset, at, message,
                at(landed.messageRefs(), message),
                at(landed.messageReplaces(), message));
    }

    /**
     * One message's entry in a per-message list.
     *
     * @param list    the list, one entry per message; null if not kept
     * @param message the message's index; -1 if not a batch
     * @return the entry; null if there is none
     */
    private static String at(List<String> list, int message)
    {
        return message < 0 || list == null || message >= list.size()
                ? null : list.get(message);
    }

    /**
     * Finds the message of a batch that holds a byte offset: the last one
     * starting at or before it.
     *
     * @param records where each message starts; null if not a batch
     * @param offset  the byte offset in the item
     * @return the message's index; -1 if not a batch
     */
    static int message(RecordIndex records, long offset)
    {
        if (records == null)
        {
            return -1;
        }
        int found = Collections.binarySearch(records.offsets(), offset);
        if (found >= 0)
        {
            // Empty messages share an offset with the next: take the last.
            while (found + 1 < records.offsets().size()
                    && records.offsets().get(found + 1) == offset)
            {
                found++;
            }
            return found;
        }
        return Math.max(0, -found - 2);
    }
}
