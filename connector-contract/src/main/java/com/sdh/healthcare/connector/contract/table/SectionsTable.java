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
 * The layout of the {@code sections} table (design §8.2): one row per
 * encrypted chunk of a section. The constants are the table's default name,
 * its column names, its schema, its partitioning and its sort order.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SectionsTable
{

    /** The table's name unless the configuration gives another. */
    public static final String DEFAULT_NAME = "sections";

    /** The section's id, the same for every chunk of it. */
    public static final String SECTION_ID = "section_id";

    /** The id of the file the section came from. */
    public static final String FILE_ID = "file_id";

    /** The section's number in its file, from 0. */
    public static final String SEQ = "seq";

    /** The chunk's number in its section, from 0. */
    public static final String CHUNK_INDEX = "chunk_index";

    /** How many chunks the section has. */
    public static final String CHUNK_COUNT = "chunk_count";

    /** Where the section starts in the file, in bytes. */
    public static final String SOURCE_OFFSET = "source_offset";

    /** How many bytes of the file the section covers. */
    public static final String SOURCE_LENGTH = "source_length";

    /** The name of the file's format, such as {@code csv} or {@code ccda}. */
    public static final String FORMAT = "format";

    /** The section's length before compression and encryption. */
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
     * the section is stored as split.
     */
    public static final String MAPPING = "mapping";

    /**
     * Set when conversion failed and the section is stored in its original
     * form (design §6.4); null otherwise.
     */
    public static final String ERROR_CODE = "error_code";

    /**
     * The line of the section's origin (design §8.2, {@code SectionOrigin}):
     * where the text copied from the file starts in the section. Usually 1;
     * 2 after an XML declaration the splitter added. Null in rows written
     * before contract 2.1.
     */
    public static final String SECTION_LINE = "section_line";

    /** The column that goes with {@link #SECTION_LINE}. */
    public static final String SECTION_COLUMN = "section_column";

    /**
     * The line in the file where the same text starts. Null in rows written
     * before contract 2.1.
     */
    public static final String SOURCE_LINE = "source_line";

    /** The column that goes with {@link #SOURCE_LINE}. */
    public static final String SOURCE_COLUMN = "source_column";

    /**
     * The line in the file where conversion failed; null when it didn't
     * fail, and in rows written before contract 2.1.
     */
    public static final String ERROR_LINE = "error_line";

    /** The column that goes with {@link #ERROR_LINE}. */
    public static final String ERROR_COLUMN = "error_column";

    /** The table's columns, with their Iceberg field ids. */
    public static final Schema SCHEMA = new Schema(
            required(1, SECTION_ID, Types.StringType.get()),
            required(2, FILE_ID, Types.UUIDType.get()),
            required(3, SEQ, Types.LongType.get()),
            required(4, CHUNK_INDEX, Types.IntegerType.get()),
            required(5, CHUNK_COUNT, Types.IntegerType.get()),
            required(6, SOURCE_OFFSET, Types.LongType.get()),
            required(7, SOURCE_LENGTH, Types.LongType.get()),
            required(8, FORMAT, Types.StringType.get()),
            required(9, PLAINTEXT_LENGTH, Types.LongType.get(),
                    "section plaintext length before compression"),
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
            optional(18, SECTION_LINE, Types.LongType.get(),
                    "origin: where the copied text starts in the section"),
            optional(19, SECTION_COLUMN, Types.IntegerType.get()),
            optional(20, SOURCE_LINE, Types.LongType.get(),
                    "origin: where the same text starts in the file"),
            optional(21, SOURCE_COLUMN, Types.IntegerType.get()),
            optional(22, ERROR_LINE, Types.LongType.get(),
                    "where in the file conversion failed"),
            optional(23, ERROR_COLUMN, Types.IntegerType.get()));

    /** The partitioning: by the day each row was written. */
    public static final PartitionSpec SPEC =
            PartitionSpec.builderFor(SCHEMA).day(INGESTED_AT).build();

    /** The sort order: by file, then section, then chunk. */
    public static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA)
            .asc(FILE_ID)
            .asc(SEQ)
            .asc(CHUNK_INDEX)
            .build();

    /** Not instantiated: this class only holds constants. */
    private SectionsTable()
    {
    }
}
