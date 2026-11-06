/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import static com.sdh.healthcare.connector.contract.table.RecordsTable.*;

import com.sdh.healthcare.connector.contract.position.RecordOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
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
 * Reads back, from committed rows of the {@code records} table, what is
 * needed to rebuild the messages announcing them, without touching
 * ciphertext or keys (design §9.2, restart step 2).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class CommittedRecords
{

    /**
     * What a committed record's message needs, from its first chunk's row.
     *
     * @param recordId        the record's id
     * @param seq             its number in the file, from 0
     * @param sourceOffset    where it starts in the file, in bytes
     * @param sourceLength    how many bytes of the file it covers
     * @param format          the name of the file's format
     * @param contentType     what the stored bytes are
     * @param mapping         the JSON mapping; null when stored as split
     * @param errorCode       set when conversion failed (design §6.4)
     * @param plaintextLength the stored length before compression and
     *                        encryption
     * @param chunkCount      how many encrypted chunks it is stored in
     * @param ingestedDate    the day it was written, in UTC
     * @param kekRef          which key encryption key wrapped its data key
     * @param origin          where the record's text came from in the file;
     *                        null for rows written before contract 2.1
     * @param errorAt         where in the file conversion failed; null
     *                        without an error, or before contract 2.1
     */
    public record Row(String recordId, long seq, long sourceOffset,
            long sourceLength, String format, String contentType,
            String mapping, String errorCode, long plaintextLength,
            int chunkCount, LocalDate ingestedDate, String kekRef,
            RecordOrigin origin, SourcePosition errorAt)
    {
    }

    /**
     * A committed record that failed conversion, with where it failed.
     *
     * @param seq          its number in the file, from 0
     * @param sourceOffset where it starts in the file, in bytes
     * @param errorCode    why conversion failed (design §6.4)
     * @param errorAt      where in the file conversion failed; null if
     *                     unknown
     */
    public record Failed(long seq, long sourceOffset, String errorCode,
            SourcePosition errorAt)
    {
    }

    /** Not instantiated: this class only has static methods. */
    private CommittedRecords()
    {
    }

    /**
     * Reads the committed records of a file in a range of sequence numbers.
     *
     * @param table    the {@code records} table
     * @param itemId   the file's id
     * @param afterSeq the range's start, not included
     * @param uptoSeq  the range's end, included
     * @return the records with {@code afterSeq < seq <= uptoSeq}, in
     *         sequence order
     * @throws UncheckedIOException  if the table can't be read
     * @throws IllegalStateException if a record in the range isn't there
     */
    public static List<Row> read(Table table, UUID itemId, long afterSeq,
            long uptoSeq)
    {
        List<Row> found = new ArrayList<>();
        if (uptoSeq <= afterSeq)
        {
            return found;
        }
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(ITEM_ID, itemId),
                        Expressions.greaterThan(SEQ, afterSeq),
                        Expressions.lessThanOrEqual(SEQ, uptoSeq),
                        Expressions.equal(CHUNK_INDEX, 0)))
                .select(RECORD_ID, SEQ, SOURCE_OFFSET, SOURCE_LENGTH, FORMAT,
                        CONTENT_TYPE, MAPPING, ERROR_CODE, PLAINTEXT_LENGTH,
                        CHUNK_COUNT, INGESTED_AT, KEK_REF, RECORD_LINE,
                        RECORD_COLUMN, SOURCE_LINE, SOURCE_COLUMN, ERROR_LINE,
                        ERROR_COLUMN)
                .build())
        {
            for (Record row : rows)
            {
                found.add(new Row(
                        (String) row.getField(RECORD_ID),
                        (Long) row.getField(SEQ),
                        (Long) row.getField(SOURCE_OFFSET),
                        (Long) row.getField(SOURCE_LENGTH),
                        (String) row.getField(FORMAT),
                        (String) row.getField(CONTENT_TYPE),
                        (String) row.getField(MAPPING),
                        (String) row.getField(ERROR_CODE),
                        (Long) row.getField(PLAINTEXT_LENGTH),
                        (Integer) row.getField(CHUNK_COUNT),
                        ((OffsetDateTime) row.getField(INGESTED_AT))
                                .withOffsetSameInstant(ZoneOffset.UTC)
                                .toLocalDate(),
                        (String) row.getField(KEK_REF),
                        origin(row),
                        position(row, ERROR_LINE, ERROR_COLUMN)));
            }
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
        found.sort(Comparator.comparingLong(Row::seq));
        if (found.size() != uptoSeq - afterSeq)
        {
            throw new IllegalStateException(
                    "Committed records missing for seq range (" + afterSeq
                    + ", " + uptoSeq + "]: found " + found.size());
        }
        return found;
    }

    /**
     * Tells whether a record of an item is committed: after an ambiguous
     * commit, or at resume, whether a batch ending with it landed.
     *
     * @param table  the table
     * @param itemId the item's id
     * @param seq    the record's sequence number
     * @return true if its first chunk is in the table
     * @throws UncheckedIOException if the table can't be read
     */
    public static boolean has(Table table, UUID itemId, long seq)
    {
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(ITEM_ID, itemId),
                        Expressions.equal(SEQ, seq),
                        Expressions.equal(CHUNK_INDEX, 0)))
                .select(SEQ)
                .build())
        {
            return rows.iterator().hasNext();
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
    }

    /**
     * Reads a row's origin.
     *
     * @param row a row of the table
     * @return the origin; null if it has none (written before contract 2.1)
     */
    private static RecordOrigin origin(Record row)
    {
        SourcePosition inRecord =
                position(row, RECORD_LINE, RECORD_COLUMN);
        SourcePosition inItem = position(row, SOURCE_LINE, SOURCE_COLUMN);
        return inRecord == null || inItem == null
                ? null : new RecordOrigin(inRecord, inItem);
    }

    /**
     * Reads a line and column held in two columns of a row.
     *
     * @param row          a row of the table
     * @param lineColumn   the name of the line's column
     * @param columnColumn the name of the column's column
     * @return the position; null if either is empty
     */
    private static SourcePosition position(Record row, String lineColumn,
            String columnColumn)
    {
        Long line = (Long) row.getField(lineColumn);
        Integer column = (Integer) row.getField(columnColumn);
        return line == null || column == null
                ? null : new SourcePosition(line, column);
    }

    /**
     * Counts a file's committed records that failed conversion and are
     * stored in their original form.
     *
     * @param table  the {@code records} table
     * @param itemId the file's id
     * @return how many there are
     * @throws UncheckedIOException if the table can't be read
     */
    public static long countFailed(Table table, UUID itemId)
    {
        long count = 0;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(ITEM_ID, itemId),
                        Expressions.equal(CHUNK_INDEX, 0),
                        Expressions.notNull(ERROR_CODE)))
                .select(SEQ)
                .build())
        {
            for (Record ignored : rows)
            {
                count++;
            }
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
        return count;
    }

    /**
     * Reads a file's committed records that failed conversion.
     *
     * @param table  the {@code records} table
     * @param itemId the file's id
     * @return them, in sequence order
     * @throws UncheckedIOException if the table can't be read
     */
    public static List<Failed> failed(Table table, UUID itemId)
    {
        List<Failed> found = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(ITEM_ID, itemId),
                        Expressions.equal(CHUNK_INDEX, 0),
                        Expressions.notNull(ERROR_CODE)))
                .select(SEQ, SOURCE_OFFSET, ERROR_CODE, ERROR_LINE,
                        ERROR_COLUMN)
                .build())
        {
            for (Record row : rows)
            {
                found.add(new Failed((Long) row.getField(SEQ),
                        (Long) row.getField(SOURCE_OFFSET),
                        (String) row.getField(ERROR_CODE),
                        position(row, ERROR_LINE, ERROR_COLUMN)));
            }
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
        found.sort(Comparator.comparingLong(Failed::seq));
        return found;
    }
}
