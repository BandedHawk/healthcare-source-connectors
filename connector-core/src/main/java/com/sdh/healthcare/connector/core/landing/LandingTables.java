/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.sdh.healthcare.connector.core.storage.IngestTables;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.types.Types;

/**
 * The layout of the landing tables (design §5.3.2), which hold items exactly
 * as they were received, in a namespace of their own.
 * <p>
 * {@value #LANDED_ITEMS} holds each item encrypted in chunks, one row per
 * chunk, with the same envelope as sections. The values known only once the
 * whole item is in (its size, chunk count and digest, and where it came
 * from) are on one summary row, {@code chunk_index = -1}, written in the
 * item's final commit. A summary row means the item is fully landed.
 * <p>
 * {@value #LEDGER} records what has been landed, with no PHI: hashes, sizes
 * and versions only. It lets a read-only source be polled again and again
 * without landing the same file twice, and it is kept after retention has
 * purged the items themselves.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LandingTables
{

    /** The namespace the landing tables are in, unless configured. */
    public static final String DEFAULT_NAMESPACE = "sdh_landing";

    /** The table of landed items. */
    public static final String LANDED_ITEMS = "landed_items";

    /** The table of what has been landed. */
    public static final String LEDGER = "landing_ledger";

    /** The {@code chunk_index} of an item's summary row. */
    public static final int SUMMARY_ROW = -1;

    /** The item's ID, a UUIDv7. */
    public static final String ITEM_ID = "item_id";

    /** The chunk's position in the item, from 0; -1 on the summary row. */
    public static final String CHUNK_INDEX = "chunk_index";

    /** The encrypted chunk; empty on the summary row. */
    public static final String CIPHERTEXT = "ciphertext";

    /** The batch data key, wrapped by the KEK. */
    public static final String BDK_WRAPPED = "bdk_wrapped";

    /** Which KEK wrapped the batch data key. */
    public static final String KEK_REF = "kek_ref";

    /** The commit batch the row was written in. */
    public static final String BATCH_ID = "batch_id";

    /** When the row's batch was written; the table's partition. */
    public static final String LANDED_AT = "landed_at";

    /** The intake the item came in through. */
    public static final String INTAKE = "intake";

    /** What kind of item it is ({@link ItemKind}). */
    public static final String KIND = "kind";

    /** The item's size, in bytes. */
    public static final String ITEM_SIZE = "item_size";

    /** How many chunks the item is stored in. */
    public static final String CHUNK_COUNT = "chunk_count";

    /** The plaintext size of every chunk but the last. */
    public static final String CHUNK_SIZE = "chunk_size";

    /** The item's keyed, chained digest. */
    public static final String ITEM_DIGEST = "item_digest";

    /** How many bytes each link of the digest chain covers. */
    public static final String DIGEST_BLOCK_SIZE = "digest_block_size";

    /** When landing the item started. */
    public static final String STARTED_AT = "started_at";

    /** The keyed hash of where the item came from ({@link SourceRef}). */
    public static final String SOURCE_REF_HASH = "source_ref_hash";

    /** The source's size when the item was landed; -1 if not known. */
    public static final String SOURCE_SIZE = "source_size";

    /**
     * The source's version when the item was landed, such as a
     * modification time, ETag or version ID; empty if it has none.
     */
    public static final String SOURCE_VERSION = "source_version";

    /** The item's original name or path, encrypted. */
    public static final String ORIGINAL_NAME_ENC = "original_name_enc";

    /** The item this one replaces, when its producer says so. */
    public static final String REPLACES_ITEM_ID = "replaces_item_id";

    /** The columns of {@value #LANDED_ITEMS}. */
    public static final Schema LANDED_ITEMS_SCHEMA = new Schema(
            required(1, ITEM_ID, Types.UUIDType.get()),
            required(2, CHUNK_INDEX, Types.IntegerType.get(),
                    "-1 = summary row"),
            required(3, CIPHERTEXT, Types.BinaryType.get(),
                    "nonce ‖ AES-GCM(zstd(chunk)) ‖ tag"),
            required(4, BDK_WRAPPED, Types.BinaryType.get()),
            required(5, KEK_REF, Types.StringType.get()),
            required(6, BATCH_ID, Types.UUIDType.get()),
            required(7, LANDED_AT, Types.TimestampType.withZone()),
            optional(10, INTAKE, Types.StringType.get()),
            optional(11, KIND, Types.StringType.get()),
            optional(12, ITEM_SIZE, Types.LongType.get()),
            optional(13, CHUNK_COUNT, Types.IntegerType.get()),
            optional(14, CHUNK_SIZE, Types.IntegerType.get()),
            optional(15, ITEM_DIGEST, Types.StringType.get()),
            optional(16, DIGEST_BLOCK_SIZE, Types.IntegerType.get()),
            optional(17, STARTED_AT, Types.TimestampType.withZone()),
            optional(18, SOURCE_REF_HASH, Types.StringType.get()),
            optional(19, SOURCE_SIZE, Types.LongType.get()),
            optional(20, SOURCE_VERSION, Types.StringType.get()),
            optional(21, ORIGINAL_NAME_ENC, Types.BinaryType.get(),
                    "encrypted original name or path"),
            optional(22, REPLACES_ITEM_ID, Types.UUIDType.get()));

    /** The columns of {@value #LEDGER}. */
    public static final Schema LEDGER_SCHEMA = new Schema(
            required(1, INTAKE, Types.StringType.get()),
            required(2, SOURCE_REF_HASH, Types.StringType.get(),
                    "empty when the item has no source reference"),
            required(3, SOURCE_SIZE, Types.LongType.get()),
            required(4, SOURCE_VERSION, Types.StringType.get()),
            required(5, ITEM_ID, Types.UUIDType.get()),
            required(6, ITEM_DIGEST, Types.StringType.get()),
            required(7, LANDED_AT, Types.TimestampType.withZone()));

    /** Not used: there are only static methods. */
    private LandingTables()
    {
    }

    /**
     * Creates or loads the {@value #LANDED_ITEMS} table, partitioned by the
     * day rows were written and sorted by item and chunk, so a reader can
     * prune to an item's files.
     *
     * @param catalog   the Iceberg catalog
     * @param namespace the landing namespace, created if missing
     * @return the table
     */
    public static Table landedItems(Catalog catalog, String namespace)
    {
        Schema schema = LANDED_ITEMS_SCHEMA;
        return IngestTables.create(catalog, namespace, LANDED_ITEMS, schema,
                PartitionSpec.builderFor(schema).day(LANDED_AT).build(),
                SortOrder.builderFor(schema).asc(ITEM_ID).asc(CHUNK_INDEX)
                        .build());
    }

    /**
     * Creates or loads the {@value #LEDGER} table. It isn't partitioned:
     * retention never purges it, and it is read whole at startup.
     *
     * @param catalog   the Iceberg catalog
     * @param namespace the landing namespace, created if missing
     * @return the table
     */
    public static Table ledger(Catalog catalog, String namespace)
    {
        Schema schema = LEDGER_SCHEMA;
        return IngestTables.create(catalog, namespace, LEDGER, schema,
                PartitionSpec.unpartitioned(),
                SortOrder.builderFor(schema).asc(SOURCE_REF_HASH).build());
    }
}
