/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.stage.ReprocessInputs;
import com.sdh.healthcare.connector.stage.ReprocessStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.iceberg.Snapshot;

/**
 * A record stage's input for a reprocess (STAGES.md §15.8.4): the records
 * in its source table, with those the stage before made from operators'
 * corrections. At each place of an item's newest generation the newest
 * line is current, and is sent as its input message would have been, as
 * the reconciler rebuilds it (§9). A record the stage before failed is
 * current too, so a place it last failed isn't reprocessed from an older
 * record.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SourceInputs implements ReprocessInputs
{

    /** The source table. */
    private final RecordTable source;

    /** The stage's messages, for the type of its input. */
    private final StageMessages messages;

    /**
     * A stage's input.
     *
     * @param source   the source table
     * @param messages the stage's messages
     */
    public SourceInputs(RecordTable source, StageMessages messages)
    {
        this.source = source;
        this.messages = messages;
    }

    /**
     * {@inheritDoc}
     *
     * Read from the table's newest snapshot.
     *
     * @throws java.io.UncheckedIOException if the table can't be read
     */
    @Override
    public List<Input> current(UUID itemId)
    {
        source.table().refresh();
        Snapshot snapshot = source.table().currentSnapshot();
        if (snapshot == null)
        {
            return List.of();
        }
        List<CommittedRecords.Row> rows = new ArrayList<>(
                CommittedRecords.all(source.table(), itemId));
        rows.addAll(CommittedRecords.corrections(source.table(), itemId));
        int newest = 0;
        for (CommittedRecords.Row row : rows)
        {
            newest = Math.max(newest, row.lineage().generation());
        }
        Map<Long, CommittedRecords.Row> current = new TreeMap<>();
        for (CommittedRecords.Row row : rows)
        {
            if (row.lineage().generation() != newest)
            {
                continue;
            }
            CommittedRecords.Row held = current.get(row.seq());
            if (held == null || row.lineage().compareTo(held.lineage()) > 0)
            {
                current.put(row.seq(), row);
            }
        }
        List<Input> found = new ArrayList<>();
        for (CommittedRecords.Row row : current.values())
        {
            ReprocessStore.Chosen chosen = new ReprocessStore.Chosen(itemId,
                    row.seq(), row.lineage(), row.recordId());
            boolean failed = row.errorCode() != null;
            found.add(new Input(chosen, failed, failed ? null
                    : rebuild(messages, itemId, row, source.ref(
                            row.ingestedDate(), snapshot.snapshotId(),
                            row.kekRef()))));
        }
        return found;
    }

    /**
     * A record's input message, rebuilt from its row of the source table,
     * as the stage before sent it: for a reconciler that finds records
     * whose messages were lost (STAGES.md §9), and for a reprocess. It
     * knows no queue message its record is in, and no cause.
     *
     * @param messages the stage's messages, for the type of its input
     * @param itemId   the record's item
     * @param row      its row
     * @param ref      where it is: the row's day and KEK, and a snapshot
     *                 that holds it
     * @return the message, at the row's own lineage
     */
    public static RecordInStorage rebuild(StageMessages messages,
            UUID itemId, CommittedRecords.Row row, StorageRef ref)
    {
        return message(messages, itemId, row, ref, row.plaintextLength(),
                row.chunkCount(), row.errorCode(), row.errorAt(),
                row.stageVersion(), row.lineage(), -1, null, null);
    }

    /**
     * A stage's input message for a record, from a row that names its
     * place in the item, its format and its origin.
     *
     * @param messages        the stage's messages, for the type of its
     *                        input
     * @param itemId          the record's item
     * @param row             the row
     * @param ref             where the record is stored; null if it isn't,
     *                        as for an operator's correction
     * @param plaintextLength its stored length
     * @param chunkCount      how many chunks it is stored in
     * @param errorCode       for the convert stage's input, the splitter's
     *                        error; null if none
     * @param errorAt         where that was; null if unknown or none
     * @param version         the version of the stage before that made it;
     *                        null if not known
     * @param lineage         where it stands in its line; null for the
     *                        first of everything
     * @param message         the queue message it starts in; -1 if none
     * @param messageRef      that message's broker ID; null if none
     * @param messageReplaces the earlier message it replaces; null if none
     * @return the message
     */
    static RecordInStorage message(StageMessages messages, UUID itemId,
            CommittedRecords.Row row, StorageRef ref, long plaintextLength,
            int chunkCount, String errorCode, SourcePosition errorAt,
            String version, Lineage lineage, int message, String messageRef,
            String messageReplaces)
    {
        return switch (messages)
        {
            case CONVERT -> new RecordSplit(row.recordId(), itemId,
                    row.seq(), row.sourceOffset(), row.sourceLength(),
                    row.format(), row.contentType(), plaintextLength,
                    chunkCount, ref, row.origin(), errorCode, errorAt,
                    message, messageRef, messageReplaces, version, null,
                    lineage, false, null);
            case TRANSFORM -> new RecordStored(row.recordId(), itemId,
                    row.seq(), row.sourceOffset(), row.sourceLength(),
                    row.format(), row.contentType(), row.mapping(),
                    plaintextLength, chunkCount, ref, row.origin(), version,
                    null, lineage, false, null, message, messageRef,
                    messageReplaces);
        };
    }
}
