/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.message;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemConverted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemEnd;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemTransformed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordTransformed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.position.RecordOrigin;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * The messages of each record stage (STAGES.md §17.4): a stage that reads
 * each record from its source table, makes one output, and stores it in
 * its sink. It says which of the stage before's messages it takes, and
 * makes the stage's own record message and item end, so the shared record
 * stage needs no stage's types of its own. The split stage is not one: it
 * cuts an item into many records.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum StageMessages
{

    /** The convert stage: split records in, converted records out. */
    CONVERT("convert", "split", MessageType.RECORD_SPLIT,
            MessageType.ITEM_SPLIT, MessageType.RECORD_STORED,
            MessageType.ITEM_CONVERTED),

    /** The transform stage: converted records in, mapped records out. */
    TRANSFORM("transform", "convert", MessageType.RECORD_STORED,
            MessageType.ITEM_CONVERTED, MessageType.RECORD_TRANSFORMED,
            MessageType.ITEM_TRANSFORMED);

    /** The stage's name, its place in a line ({@link Lineage}). */
    private final String stage;

    /** The name of the stage before it, whose output it takes. */
    private final String upstream;

    /** The record message it takes. */
    private final MessageType inputRecord;

    /** The item end it takes. */
    private final MessageType inputEnd;

    /** The record message it sends. */
    private final MessageType record;

    /** The item end it sends. */
    private final MessageType end;

    /**
     * Describes a stage's messages.
     *
     * @param stage       its name
     * @param upstream    the name of the stage before it
     * @param inputRecord the record message it takes
     * @param inputEnd    the item end it takes
     * @param record      the record message it sends
     * @param end         the item end it sends
     */
    StageMessages(String stage, String upstream, MessageType inputRecord,
            MessageType inputEnd, MessageType record, MessageType end)
    {
        this.stage = stage;
        this.upstream = upstream;
        this.inputRecord = inputRecord;
        this.inputEnd = inputEnd;
        this.record = record;
        this.end = end;
    }

    /**
     * The messages of a stage, by its name.
     *
     * @param stage {@code convert} or {@code transform}
     * @return its messages
     * @throws IllegalArgumentException for a stage that isn't a record
     *                                  stage
     */
    public static StageMessages of(String stage)
    {
        for (StageMessages messages : values())
        {
            if (messages.stage.equals(stage))
            {
                return messages;
            }
        }
        throw new IllegalArgumentException(
                "Not a record stage: " + stage);
    }

    /**
     * The stage's name.
     *
     * @return such as {@code convert}
     */
    public String stage()
    {
        return stage;
    }

    /**
     * The name of the stage before it.
     *
     * @return such as {@code split}
     */
    public String upstream()
    {
        return upstream;
    }

    /**
     * The record message the stage takes.
     *
     * @return its type
     */
    public MessageType inputRecord()
    {
        return inputRecord;
    }

    /**
     * The item end the stage takes.
     *
     * @return its type
     */
    public MessageType inputEnd()
    {
        return inputEnd;
    }

    /**
     * The record message the stage sends.
     *
     * @return its type
     */
    public MessageType record()
    {
        return record;
    }

    /**
     * The item end the stage sends.
     *
     * @return its type
     */
    public MessageType end()
    {
        return end;
    }

    /**
     * Whether a message is one the stage takes: the stage before's record
     * message or item end, or its {@code item.progress} (STAGES.md §15.6).
     *
     * @param message the message
     * @return true if the stage takes it
     */
    public boolean takes(ConnectorMessage message)
    {
        return message.type() == inputRecord || message.type() == inputEnd
                || message instanceof ItemProgressed progressed
                        && upstream.equals(progressed.stage());
    }

    /**
     * The stage's message for a record it stored.
     *
     * @param recordId        the output's id
     * @param itemId          the id of its item
     * @param seq             its number in the item, from 0
     * @param sourceOffset    where its source starts in the item, in bytes
     * @param sourceLength    how many bytes of the item it covers
     * @param format          the name of the item's format
     * @param contentType     what the stored bytes are
     * @param mapping         what made the output, such as a converter's
     *                        mapping or a WSTL set and its digest; null for
     *                        a record the convert stage stored as split
     * @param plaintextLength the stored length before compression and
     *                        encryption
     * @param chunkCount      how many encrypted chunks it is stored in
     * @param storage         where it is stored
     * @param origin          where its text came from in the item; null if
     *                        the splitter gave none
     * @param version         the stage's version that made it
     * @param resolves        the failed record a correction or retry
     *                        resolves; null otherwise
     * @param lineage         where it stands in its line; null for the
     *                        first of everything
     * @param message         the queue message the record starts in, from
     *                        0, carried on from its input; -1 if none
     * @param messageRef      that message's broker ID; null if none
     * @param messageReplaces the earlier message it replaces; null if none
     * @return {@code record.stored} or {@code record.transformed}, not
     *         superseded and with no cause
     */
    public RecordInStorage record(String recordId, UUID itemId, long seq,
            long sourceOffset, long sourceLength, String format,
            String contentType, String mapping, long plaintextLength,
            int chunkCount, StorageRef storage, RecordOrigin origin,
            String version, String resolves, Lineage lineage, int message,
            String messageRef, String messageReplaces)
    {
        return switch (this)
        {
            case CONVERT -> new RecordStored(recordId, itemId, seq,
                    sourceOffset, sourceLength, format, contentType, mapping,
                    plaintextLength, chunkCount, storage, origin, version,
                    resolves, lineage, false, null, message, messageRef,
                    messageReplaces);
            case TRANSFORM -> new RecordTransformed(recordId, itemId, seq,
                    sourceOffset, sourceLength, format, contentType, mapping,
                    plaintextLength, chunkCount, storage, origin, version,
                    resolves, lineage, false, null, message, messageRef,
                    messageReplaces);
        };
    }

    /**
     * The stage's end for an item it finished.
     *
     * @param itemId            the item's id
     * @param intake            the intake it came in through
     * @param recordCount       its records, failed ones included
     * @param failedRecordCount those that failed and were declined
     * @param completedAt       when the last was stored
     * @param versions          the stage's versions its records were made
     *                          under, in the order first used
     * @param completion        which completion this is, from 1
     * @param generation        the item's generation, from 1
     * @param sameAs            the earlier item with the same content; null
     *                          if none
     * @param producerRef       the producer's reference, carried on; null if
     *                          none
     * @param replaces          the item it replaces; null if none
     * @param failure           why the item as a whole couldn't be split
     *                          further, carried on; null if none
     * @param resultsSent       whether this stage, or one before it, sent
     *                          the producer its result
     * @return {@code item.converted} or {@code item.transformed}
     */
    public ItemEnd itemEnd(UUID itemId, String intake, long recordCount,
            long failedRecordCount, Instant completedAt,
            List<String> versions, int completion, int generation,
            UUID sameAs, String producerRef, UUID replaces, Problem failure,
            boolean resultsSent)
    {
        return switch (this)
        {
            case CONVERT -> new ItemConverted(itemId, intake, recordCount,
                    failedRecordCount, completedAt, versions, completion,
                    generation, sameAs, producerRef, replaces, failure,
                    resultsSent);
            case TRANSFORM -> new ItemTransformed(itemId, intake,
                    recordCount, failedRecordCount, completedAt, versions,
                    completion, generation, sameAs, producerRef, replaces,
                    failure, resultsSent);
        };
    }
}
