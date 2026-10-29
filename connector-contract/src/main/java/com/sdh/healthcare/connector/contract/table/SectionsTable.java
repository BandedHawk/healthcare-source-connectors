package com.sdh.healthcare.connector.contract.table;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;

/** Layout of the {@code sections} table (design §8.2). One row per encrypted chunk. */
public final class SectionsTable {
    public static final String DEFAULT_NAME = "sections";

    public static final String SECTION_ID = "section_id";
    public static final String FILE_ID = "file_id";
    public static final String SEQ = "seq";
    public static final String CHUNK_INDEX = "chunk_index";
    public static final String CHUNK_COUNT = "chunk_count";
    public static final String SOURCE_OFFSET = "source_offset";
    public static final String SOURCE_LENGTH = "source_length";
    public static final String FORMAT = "format";
    public static final String PLAINTEXT_LENGTH = "plaintext_length";
    public static final String CIPHERTEXT = "ciphertext";
    public static final String BDK_WRAPPED = "bdk_wrapped";
    public static final String KEK_REF = "kek_ref";
    public static final String BATCH_ID = "batch_id";
    public static final String INGESTED_AT = "ingested_at";
    /** What the stored bytes are: {@code application/json} once converted, else the original media type. */
    public static final String CONTENT_TYPE = "content_type";
    /** The JSON mapping and version, e.g. {@code csv-row/1}; null when stored as split. */
    public static final String MAPPING = "mapping";
    /** Set when conversion failed and the section is stored in its original form (design §6.4); null otherwise. */
    public static final String ERROR_CODE = "error_code";

    public static final Schema SCHEMA = new Schema(
            required(1, SECTION_ID, Types.StringType.get()),
            required(2, FILE_ID, Types.UUIDType.get()),
            required(3, SEQ, Types.LongType.get()),
            required(4, CHUNK_INDEX, Types.IntegerType.get()),
            required(5, CHUNK_COUNT, Types.IntegerType.get()),
            required(6, SOURCE_OFFSET, Types.LongType.get()),
            required(7, SOURCE_LENGTH, Types.LongType.get()),
            required(8, FORMAT, Types.StringType.get()),
            required(9, PLAINTEXT_LENGTH, Types.LongType.get(), "section plaintext length before compression"),
            required(10, CIPHERTEXT, Types.BinaryType.get(), "nonce ‖ AES-GCM ciphertext ‖ tag"),
            required(11, BDK_WRAPPED, Types.BinaryType.get()),
            required(12, KEK_REF, Types.StringType.get()),
            required(13, BATCH_ID, Types.UUIDType.get()),
            required(14, INGESTED_AT, Types.TimestampType.withZone()),
            required(15, CONTENT_TYPE, Types.StringType.get()),
            optional(16, MAPPING, Types.StringType.get()),
            optional(17, ERROR_CODE, Types.StringType.get(), "conversion failed; stored in its original form"));

    public static final PartitionSpec SPEC =
            PartitionSpec.builderFor(SCHEMA).day(INGESTED_AT).build();

    public static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA)
            .asc(FILE_ID)
            .asc(SEQ)
            .asc(CHUNK_INDEX)
            .build();

    private SectionsTable() {}
}
