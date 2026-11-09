/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.table;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;

/**
 * The layout of the {@code records} table (design §8.2): one row per
 * encrypted chunk of a record. The constants are the table's default name,
 * its column names, its schema, its partitioning and its sort order.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RecordsTable
{

    /** The table's name unless the configuration gives another. */
    public static final String DEFAULT_NAME = "records";

    /** The record's id, the same for every chunk of it. */
    public static final String RECORD_ID = "record_id";

    /** The id of the file the record came from. */
    public static final String ITEM_ID = "item_id";

    /** The record's number in its file, from 0. */
    public static final String SEQ = "seq";

    /** The chunk's number in its record, from 0. */
    public static final String CHUNK_INDEX = "chunk_index";

    /** How many chunks the record has. */
    public static final String CHUNK_COUNT = "chunk_count";

    /** Where the record starts in the file, in bytes. */
    public static final String SOURCE_OFFSET = "source_offset";

    /** How many bytes of the file the record covers. */
    public static final String SOURCE_LENGTH = "source_length";

    /** The name of the file's format, such as {@code csv} or {@code ccda}. */
    public static final String FORMAT = "format";

    /** The record's length before compression and encryption. */
    public static final String PLAINTEXT_LENGTH = "plaintext_length";

    /** The chunk, encrypted: nonce, AES-GCM ciphertext and tag. */
    public static final String CIPHERTEXT = "ciphertext";

    /** The batch's data key, wrapped by the key encryption key. */
    public static final String BDK_WRAPPED = "bdk_wrapped";

    /** Which key encryption key wrapped the data key. */
    public static final String KEK_REF = "kek_ref";

    /** The id of the batch the row was committed in. */
    public static final String BATCH_ID = "batch_id";

    /** When the row was written. */
    public static final String INGESTED_AT = "ingested_at";

    /**
     * What the stored bytes are: {@code application/json} once converted,
     * else the original media type.
     */
    public static final String CONTENT_TYPE = "content_type";

    /**
     * The JSON mapping and its version, such as {@code csv-row/1}; null when
     * the record is stored as split.
     */
    public static final String MAPPING = "mapping";

    /**
     * Set when conversion failed and the record is stored in its original
     * form (design §6.4); null otherwise.
     */
    public static final String ERROR_CODE = "error_code";

    /**
     * The line of the record's origin (design §8.2, {@code RecordOrigin}):
     * where the text copied from the file starts in the record. Usually 1;
     * 2 after an XML declaration the splitter added. Null if the splitter gave
     * none.
     */
    public static final String RECORD_LINE = "record_line";

    /** The column that goes with {@link #RECORD_LINE}. */
    public static final String RECORD_COLUMN = "record_column";

    /**
     * The line in the file where the same text starts. Null if the splitter
     * gave none.
     */
    public static final String SOURCE_LINE = "source_line";

    /** The column that goes with {@link #SOURCE_LINE}. */
    public static final String SOURCE_COLUMN = "source_column";

    /**
     * The line in the file where conversion failed; null when it didn't
     * fail, or the place isn't known.
     */
    public static final String ERROR_LINE = "error_line";

    /** The column that goes with {@link #ERROR_LINE}. */
    public static final String ERROR_COLUMN = "error_column";

    /**
     * For a failed record of an item that is a batch of queue messages, the
     * number of the message it starts in, from 0 (STAGES.md §9); null
     * otherwise. A position, not PHI.
     */
    public static final String MESSAGE_INDEX = "message_index";

    /**
     * With {@link #MESSAGE_INDEX}, that message's broker ID and the earlier
     * message it replaces, as a JSON array of two (either may be null),
     * encrypted with the record's own key like a chunk at index
     * {@value #MESSAGE_REFS_CHUNK} of one; on the record's first chunk row
     * only.
     */
    public static final String MESSAGE_REFS_ENC = "message_refs_enc";

    /**
     * The chunk index {@link #MESSAGE_REFS_ENC} is encrypted at: never a
     * chunk's, so it can't be read as one.
     */
    public static final int MESSAGE_REFS_CHUNK = -1;

    /**
     * The version of the stage that wrote the row (STAGES.md §14): its build,
     * the implementation that made the record and a digest of the settings
     * that change it. Null on a row written before versions were kept.
     */
    public static final String STAGE_VERSION = "stage_version";

    /**
     * The version of the stage before that made the record this one was
     * made from: the split stage's, on a row of {@code records}. Null on
     * {@code split_records}, whose input is landed as received.
     */
    public static final String INPUT_VERSION = "input_version";

    /**
     * For a record made from an operator's correction (STAGES.md §15.6c):
     * the ID of the record that failed, which it corrects; null otherwise.
     */
    public static final String CORRECTION_OF = "correction_of";

    /**
     * The generation of the record's item (STAGES.md §15.8.2), from 1; null
     * on a row written before generations were kept, which is the first.
     */
    public static final String GENERATION = "generation";

    /**
     * The record's revision per stage, joined with dots without trailing
     * 1s, such as {@code 1.3} (STAGES.md §15.8.2); null on a row of the
     * first revision everywhere, or written before lineage was kept.
     */
    public static final String LINE = "line";

    /** The table's columns, with their Iceberg field ids. */
    public static final Schema SCHEMA = new Schema(
            required(1, RECORD_ID, Types.StringType.get()),
            required(2, ITEM_ID, Types.UUIDType.get()),
            required(3, SEQ, Types.LongType.get()),
            required(4, CHUNK_INDEX, Types.IntegerType.get()),
            required(5, CHUNK_COUNT, Types.IntegerType.get()),
            required(6, SOURCE_OFFSET, Types.LongType.get()),
            required(7, SOURCE_LENGTH, Types.LongType.get()),
            required(8, FORMAT, Types.StringType.get()),
            required(9, PLAINTEXT_LENGTH, Types.LongType.get(),
                    "record plaintext length before compression"),
            required(10, CIPHERTEXT, Types.BinaryType.get(),
                    "nonce ‖ AES-GCM ciphertext ‖ tag"),
            required(11, BDK_WRAPPED, Types.BinaryType.get()),
            required(12, KEK_REF, Types.StringType.get()),
            required(13, BATCH_ID, Types.UUIDType.get()),
            required(14, INGESTED_AT, Types.TimestampType.withZone()),
            required(15, CONTENT_TYPE, Types.StringType.get()),
            optional(16, MAPPING, Types.StringType.get()),
            optional(17, ERROR_CODE, Types.StringType.get(),
                    "conversion failed; stored in its original form"),
            optional(18, RECORD_LINE, Types.LongType.get(),
                    "origin: where the copied text starts in the record"),
            optional(19, RECORD_COLUMN, Types.IntegerType.get()),
            optional(20, SOURCE_LINE, Types.LongType.get(),
                    "origin: where the same text starts in the file"),
            optional(21, SOURCE_COLUMN, Types.IntegerType.get()),
            optional(22, ERROR_LINE, Types.LongType.get(),
                    "where in the file conversion failed"),
            optional(23, ERROR_COLUMN, Types.IntegerType.get()),
            optional(24, MESSAGE_INDEX, Types.IntegerType.get(),
                    "the queue message a failed record starts in"),
            optional(25, MESSAGE_REFS_ENC, Types.BinaryType.get(),
                    "that message's broker IDs, encrypted"),
            optional(26, STAGE_VERSION, Types.StringType.get(),
                    "the version of the stage that wrote the row"),
            optional(27, INPUT_VERSION, Types.StringType.get(),
                    "the version of the stage before that made its input"),
            optional(28, CORRECTION_OF, Types.StringType.get(),
                    "the failed record an operator's correction made this"
                            + " one from"),
            optional(29, GENERATION, Types.IntegerType.get(),
                    "the generation of the record's item"),
            optional(30, LINE, Types.StringType.get(),
                    "the record's revision per stage"));

    /** The partitioning: by the day each row was written. */
    public static final PartitionSpec SPEC =
            PartitionSpec.builderFor(SCHEMA).day(INGESTED_AT).build();

    /** The sort order: by file, then record, then chunk. */
    public static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA)
            .asc(ITEM_ID)
            .asc(SEQ)
            .asc(CHUNK_INDEX)
            .build();

    /** Not instantiated: this class only holds constants. */
    private RecordsTable()
    {
    }
}
