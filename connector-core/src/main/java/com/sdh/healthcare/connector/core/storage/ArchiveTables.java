package com.sdh.healthcare.connector.core.storage;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;

/**
 * Layout of {@code source_files} and {@code failed_files} (design §8.2): the raw original file, encrypted in
 * chunks with the same envelope as sections.
 *
 * <p>Chunk rows ({@code chunk_index >= 0}) carry the ciphertext. Values known only once the whole file is
 * archived (digest, encrypted original name, completion or failure details) are on one summary row with
 * {@code chunk_index = -1}, written in the final archive commit. The summary row's chunk columns hold the
 * file's chunk count and size; its {@code ciphertext} is empty.
 */
public final class ArchiveTables {
    public static final String SOURCE_FILES = "source_files";
    public static final String FAILED_FILES = "failed_files";
    public static final int SUMMARY_ROW = -1;

    public static final String FILE_ID = "file_id";
    public static final String CHUNK_INDEX = "chunk_index";
    public static final String CHUNK_COUNT = "chunk_count";
    public static final String CHUNK_SIZE = "chunk_size";
    public static final String FILE_SIZE = "file_size";
    public static final String CIPHERTEXT = "ciphertext";
    public static final String BDK_WRAPPED = "bdk_wrapped";
    public static final String KEK_REF = "kek_ref";
    public static final String BATCH_ID = "batch_id";
    public static final String ARCHIVED_AT = "archived_at";
    public static final String ORIGINAL_NAME_ENC = "original_name_enc";
    public static final String FILE_DIGEST = "file_digest";
    public static final String SECTION_COUNT = "section_count";
    public static final String STARTED_AT = "started_at";
    public static final String DIGEST_BLOCK_SIZE = "digest_block_size";
    public static final String COMPLETED_AT = "completed_at";
    public static final String FAILURE_STAGE = "failure_stage";
    public static final String FAILURE_OFFSET = "failure_offset";
    public static final String FAILURE_SEQ = "failure_seq";
    public static final String ERROR_CODE = "error_code";
    public static final String FAILED_AT = "failed_at";
    public static final String REPLACES_FILE_ID = "replaces_file_id";

    private static List<Types.NestedField> base() {
        List<Types.NestedField> f = new ArrayList<>();
        f.add(required(1, FILE_ID, Types.UUIDType.get()));
        f.add(required(2, CHUNK_INDEX, Types.IntegerType.get(), "-1 = summary row"));
        f.add(required(3, CHUNK_COUNT, Types.IntegerType.get()));
        f.add(required(4, CHUNK_SIZE, Types.IntegerType.get(), "plaintext bytes per full chunk"));
        f.add(required(5, FILE_SIZE, Types.LongType.get()));
        f.add(required(6, CIPHERTEXT, Types.BinaryType.get(), "nonce ‖ AES-GCM(zstd(chunk)) ‖ tag"));
        f.add(required(7, BDK_WRAPPED, Types.BinaryType.get()));
        f.add(required(8, KEK_REF, Types.StringType.get()));
        f.add(required(9, BATCH_ID, Types.UUIDType.get()));
        f.add(required(10, ARCHIVED_AT, Types.TimestampType.withZone()));
        f.add(optional(11, ORIGINAL_NAME_ENC, Types.BinaryType.get(), "summary row: encrypted original filename"));
        f.add(optional(12, FILE_DIGEST, Types.StringType.get(), "summary row: chained keyed digest"));
        f.add(optional(13, SECTION_COUNT, Types.LongType.get()));
        f.add(optional(14, STARTED_AT, Types.TimestampType.withZone()));
        f.add(optional(16, DIGEST_BLOCK_SIZE, Types.IntegerType.get(), "summary row: block size of the digest chain"));
        return f;
    }

    public static final Schema SOURCE_FILES_SCHEMA;
    public static final Schema FAILED_FILES_SCHEMA;

    static {
        List<Types.NestedField> s = base();
        s.add(optional(15, COMPLETED_AT, Types.TimestampType.withZone()));
        SOURCE_FILES_SCHEMA = new Schema(s);

        List<Types.NestedField> f = base();
        f.add(optional(20, FAILURE_STAGE, Types.StringType.get()));
        f.add(optional(21, FAILURE_OFFSET, Types.LongType.get()));
        f.add(optional(22, FAILURE_SEQ, Types.LongType.get(), "last committed seq at failure; -1 if none"));
        f.add(optional(23, ERROR_CODE, Types.StringType.get()));
        f.add(optional(24, FAILED_AT, Types.TimestampType.withZone()));
        f.add(optional(25, REPLACES_FILE_ID, Types.UUIDType.get(), "set when an operator resubmits a repaired file"));
        FAILED_FILES_SCHEMA = new Schema(f);
    }

    public static PartitionSpec spec(Schema schema) {
        return PartitionSpec.builderFor(schema).day(ARCHIVED_AT).build();
    }

    public static SortOrder sortOrder(Schema schema) {
        return SortOrder.builderFor(schema).asc(FILE_ID).asc(CHUNK_INDEX).build();
    }

    private ArchiveTables() {}
}
