package com.sdh.healthcare.connector.core.storage;

import static com.sdh.healthcare.connector.contract.table.SectionsTable.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Reads back what is needed to rebuild {@code section.stored} messages from committed rows, without
 * touching ciphertext or keys (design §9.2, restart step 2).
 */
public final class CommittedSections {

    /** @param mapping null when stored as split; @param errorCode set when conversion failed (design §6.4) */
    public record Row(String sectionId, long seq, long sourceOffset, long sourceLength, String format,
            String contentType, String mapping, String errorCode, long plaintextLength, int chunkCount,
            LocalDate ingestedDate, String kekRef) {}

    private CommittedSections() {}

    /** Sections of {@code fileId} with {@code afterSeq < seq <= uptoSeq}, in seq order. */
    public static List<Row> read(Table table, UUID fileId, long afterSeq, long uptoSeq) {
        List<Row> out = new ArrayList<>();
        if (uptoSeq <= afterSeq) {
            return out;
        }
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(FILE_ID, fileId),
                        Expressions.greaterThan(SEQ, afterSeq),
                        Expressions.lessThanOrEqual(SEQ, uptoSeq),
                        Expressions.equal(CHUNK_INDEX, 0)))
                .select(SECTION_ID, SEQ, SOURCE_OFFSET, SOURCE_LENGTH, FORMAT, CONTENT_TYPE, MAPPING, ERROR_CODE,
                        PLAINTEXT_LENGTH, CHUNK_COUNT, INGESTED_AT, KEK_REF)
                .build()) {
            for (Record r : rows) {
                out.add(new Row(
                        (String) r.getField(SECTION_ID),
                        (Long) r.getField(SEQ),
                        (Long) r.getField(SOURCE_OFFSET),
                        (Long) r.getField(SOURCE_LENGTH),
                        (String) r.getField(FORMAT),
                        (String) r.getField(CONTENT_TYPE),
                        (String) r.getField(MAPPING),
                        (String) r.getField(ERROR_CODE),
                        (Long) r.getField(PLAINTEXT_LENGTH),
                        (Integer) r.getField(CHUNK_COUNT),
                        ((OffsetDateTime) r.getField(INGESTED_AT)).withOffsetSameInstant(ZoneOffset.UTC).toLocalDate(),
                        (String) r.getField(KEK_REF)));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        out.sort(Comparator.comparingLong(Row::seq));
        if (out.size() != uptoSeq - afterSeq) {
            throw new IllegalStateException("Committed sections missing for seq range (" + afterSeq + ", " + uptoSeq
                    + "]: found " + out.size());
        }
        return out;
    }

    /** How many of a file's committed sections failed conversion and are stored in their original form. */
    public static long countFailed(Table table, UUID fileId) {
        long n = 0;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(FILE_ID, fileId),
                        Expressions.equal(CHUNK_INDEX, 0),
                        Expressions.notNull(ERROR_CODE)))
                .select(SEQ)
                .build()) {
            for (Record ignored : rows) {
                n++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return n;
    }
}
