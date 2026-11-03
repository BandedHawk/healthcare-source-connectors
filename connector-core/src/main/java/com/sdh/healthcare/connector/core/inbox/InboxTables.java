/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.inbox;

import static org.apache.iceberg.types.Types.NestedField.required;

import com.sdh.healthcare.connector.core.storage.IngestTables;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.types.Types;

/**
 * The splitter's {@value #INBOX} table (design §5.3.6): one row for each
 * {@code item.landed} message received, with no PHI.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class InboxTables
{

    /** The table's name. */
    public static final String INBOX = "inbox";

    /** The item's place in the inbox, from 1. */
    public static final String SEQ = "seq";

    /** The item's ID. */
    public static final String ITEM_ID = "item_id";

    /** The intake it came in through. */
    public static final String INTAKE = "intake";

    /** What kind of item it is. */
    public static final String KIND = "kind";

    /** Its size, in bytes. */
    public static final String ITEM_SIZE = "item_size";

    /** Its digest. */
    public static final String ITEM_DIGEST = "item_digest";

    /** When it was landed. */
    public static final String LANDED_AT = "landed_at";

    /** When its message was recorded. */
    public static final String RECEIVED_AT = "received_at";

    /** The table's columns. */
    public static final Schema SCHEMA = new Schema(
            required(1, SEQ, Types.LongType.get()),
            required(2, ITEM_ID, Types.UUIDType.get()),
            required(3, INTAKE, Types.StringType.get()),
            required(4, KIND, Types.StringType.get()),
            required(5, ITEM_SIZE, Types.LongType.get()),
            required(6, ITEM_DIGEST, Types.StringType.get()),
            required(7, LANDED_AT, Types.TimestampType.withZone()),
            required(8, RECEIVED_AT, Types.TimestampType.withZone()));

    /** Not used: there are only static methods. */
    private InboxTables()
    {
    }

    /**
     * Creates or loads the {@value #INBOX} table. It isn't partitioned: it
     * is small, and read whole at startup.
     *
     * @param catalog   the splitter's catalog
     * @param namespace the splitter's namespace
     * @return the table
     */
    public static Table inbox(Catalog catalog, String namespace)
    {
        return IngestTables.create(catalog, namespace, INBOX, SCHEMA,
                PartitionSpec.unpartitioned(),
                SortOrder.builderFor(SCHEMA).asc(SEQ).build());
    }
}
