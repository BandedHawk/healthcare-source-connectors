/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordLocation;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import java.time.LocalDate;
import java.util.UUID;
import org.apache.iceberg.Table;

/**
 * A table of records a record stage reads from (its source) or stores in
 * (its sink), with the columns of {@code RecordsTable.SCHEMA} (STAGES.md
 * §17.7), and the names messages give it.
 *
 * @param catalogName the catalog's name, as messages give it
 * @param namespace   the table's namespace
 * @param name        the table's name
 * @param table       the table, loaded
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record RecordTable(String catalogName, String namespace, String name,
        Table table)
{

    /**
     * Whether a message's storage reference names this table: a record
     * stage reads only where it was told to, whatever a message says.
     *
     * @param ref the reference
     * @return true if its catalog, namespace and table are this table's
     */
    public boolean names(StorageRef ref)
    {
        return ref != null && catalogName.equals(ref.catalog())
                && namespace.equals(ref.namespace())
                && name.equals(ref.table());
    }

    /**
     * A storage reference to rows of this table.
     *
     * @param ingestedDate the day the rows were written, in UTC
     * @param snapshotId   a snapshot that holds them
     * @param kekRef       the KEK their data keys are wrapped with
     * @return the reference
     */
    public StorageRef ref(LocalDate ingestedDate, long snapshotId,
            String kekRef)
    {
        return new StorageRef(catalogName, namespace, name, ingestedDate,
                snapshotId, kekRef);
    }

    /**
     * Where a record in this table is, from its row, for reading it with
     * the reader's own code: such as a failed record, which no message
     * announced.
     *
     * @param itemId     its item
     * @param row        its row
     * @param snapshotId a snapshot that holds it
     * @return its location
     */
    public RecordLocation location(UUID itemId, CommittedRecords.Row row,
            long snapshotId)
    {
        return new RecordLocation(row.recordId(), itemId, row.seq(),
                row.plaintextLength(), row.chunkCount(),
                ref(row.ingestedDate(), snapshotId, row.kekRef()));
    }
}
