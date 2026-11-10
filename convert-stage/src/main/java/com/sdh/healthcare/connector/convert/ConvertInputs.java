/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.stage.ReprocessInputs;
import com.sdh.healthcare.connector.stage.ReprocessStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * The convert stage's input for a reprocess (STAGES.md §15.8.4): the
 * pieces in {@code split_records}, with those the split stage made from
 * operators' corrections. At each place of an item's newest generation the
 * newest line is current, and is sent as its {@code record.split} would
 * have been, as the reconciler rebuilds it (§9). A piece the split stage
 * failed is current too, so a place it last failed isn't reprocessed from
 * an older piece.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ConvertInputs implements ReprocessInputs
{

    /** {@code split_records}. */
    private final Table splitRecords;

    /** The catalog's name, put in messages. */
    private final String catalogName;

    /** The namespace of {@code split_records}. */
    private final String namespace;

    /**
     * The convert stage's input.
     *
     * @param splitRecords {@code split_records}
     * @param catalogName  the catalog's name, put in messages
     * @param namespace    the namespace of {@code split_records}
     */
    public ConvertInputs(Table splitRecords, String catalogName,
            String namespace)
    {
        this.splitRecords = splitRecords;
        this.catalogName = catalogName;
        this.namespace = namespace;
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
        splitRecords.refresh();
        Snapshot snapshot = splitRecords.currentSnapshot();
        if (snapshot == null)
        {
            return List.of();
        }
        List<CommittedRecords.Row> rows = new ArrayList<>(
                CommittedRecords.all(splitRecords, itemId));
        rows.addAll(CommittedRecords.corrections(splitRecords, itemId));
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
            found.add(new Input(chosen, failed,
                    failed ? null : split(itemId, row,
                            snapshot.snapshotId())));
        }
        return found;
    }

    /**
     * A piece's {@code record.split}, as the reconciler rebuilds it: it
     * knows no queue message its record is in.
     *
     * @param itemId     the item
     * @param row        the piece
     * @param snapshotId the snapshot it was read from
     * @return the message, at the piece's own lineage
     */
    private RecordSplit split(UUID itemId, CommittedRecords.Row row,
            long snapshotId)
    {
        StorageRef ref = new StorageRef(catalogName, namespace,
                IngestTables.SPLIT_RECORDS, row.ingestedDate(), snapshotId,
                row.kekRef());
        Lineage lineage = row.lineage();
        return new RecordSplit(row.recordId(), itemId, row.seq(),
                row.sourceOffset(), row.sourceLength(), row.format(),
                row.contentType(), row.plaintextLength(), row.chunkCount(),
                ref, row.origin(), row.errorCode(), row.errorAt(), -1, null,
                null, null, null, lineage, false);
    }
}
