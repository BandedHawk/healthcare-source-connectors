/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import static com.sdh.healthcare.connector.contract.table.RecordsTable.*;

import com.nimbusds.jose.util.JSONArrayUtils;
import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.position.RecordOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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

    /** The most bytes a failed record's broker IDs may take, decrypted. */
    private static final int MAX_REFS_BYTES = 64 << 10;

    /** The columns a {@link Row} is read from. */
    private static final String[] ROW_COLUMNS = {RECORD_ID, SEQ,
        SOURCE_OFFSET, SOURCE_LENGTH, FORMAT, CONTENT_TYPE, MAPPING,
        ERROR_CODE, PLAINTEXT_LENGTH, CHUNK_COUNT, INGESTED_AT, KEK_REF,
        RECORD_LINE, RECORD_COLUMN, SOURCE_LINE, SOURCE_COLUMN, ERROR_LINE,
        ERROR_COLUMN, STAGE_VERSION};

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
     * @param stageVersion    the version of the stage that wrote it
     *                        (STAGES.md §14); null for a row from before
     * @param correctionOf    for a record made from an operator's
     *                        correction, the failed record it corrects
     *                        (STAGES.md §15.6c); null otherwise
     */
    public record Row(String recordId, long seq, long sourceOffset,
            long sourceLength, String format, String contentType,
            String mapping, String errorCode, long plaintextLength,
            int chunkCount, LocalDate ingestedDate, String kekRef,
            RecordOrigin origin, SourcePosition errorAt, String stageVersion,
            String correctionOf)
    {
    }

    /**
     * The columns a {@link Failed} is read from in a table: with
     * {@code correction_of} if the table has it yet.
     *
     * @param table the table
     * @return the columns
     */
    private static String[] failedColumns(Table table)
    {
        List<String> columns = new ArrayList<>(List.of(SEQ, SOURCE_OFFSET,
                ERROR_CODE, ERROR_LINE, ERROR_COLUMN, MESSAGE_INDEX,
                MESSAGE_REFS_ENC, BDK_WRAPPED, KEK_REF));
        if (table.schema().findField(CORRECTION_OF) != null)
        {
            columns.add(CORRECTION_OF);
        }
        return columns.toArray(String[]::new);
    }

    /**
     * The columns a {@link Row} is read from in a table: with
     * {@code correction_of} if the table has it yet.
     *
     * @param table the table
     * @return the columns
     */
    private static String[] rowColumns(Table table)
    {
        if (table.schema().findField(CORRECTION_OF) == null)
        {
            return ROW_COLUMNS;
        }
        String[] columns = Arrays.copyOf(ROW_COLUMNS, ROW_COLUMNS.length + 1);
        columns[ROW_COLUMNS.length] = CORRECTION_OF;
        return columns;
    }

    /**
     * A committed record that failed to split or convert, with where it
     * failed.
     *
     * @param seq             its number in the file, from 0
     * @param sourceOffset    where it starts in the file, in bytes
     * @param errorCode       why it failed (design §6.4)
     * @param errorAt         where in the file it failed; null if unknown
     * @param message         the queue message of a batch it starts in;
     *                        -1 if not noted
     * @param messageRef      that message's broker ID; null if none, or
     *                        not read
     * @param messageReplaces the earlier message it replaces; null if
     *                        none, or not read
     */
    public record Failed(long seq, long sourceOffset, String errorCode,
            SourcePosition errorAt, int message, String messageRef,
            String messageReplaces)
    {

        /**
         * A failed record with no message noted.
         *
         * @param seq          its number in the file, from 0
         * @param sourceOffset where it starts in the file, in bytes
         * @param errorCode    why it failed
         * @param errorAt      where in the file it failed; null if unknown
         */
        public Failed(long seq, long sourceOffset, String errorCode,
                SourcePosition errorAt)
        {
            this(seq, sourceOffset, errorCode, errorAt, -1, null, null);
        }
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
                .select(rowColumns(table))
                .build())
        {
            for (Record row : rows)
            {
                Row read = row(row);
                if (read.correctionOf() == null)
                {
                    found.add(read);
                }
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
     * Reads every committed record of an item, in sequence order: one row
     * per record, the first copy if a record was committed twice.
     *
     * @param table  the table
     * @param itemId the item's id
     * @return the records
     * @throws UncheckedIOException if the table can't be read
     */
    public static List<Row> all(Table table, UUID itemId)
    {
        Map<Long, Row> found = new TreeMap<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(ITEM_ID, itemId),
                        Expressions.equal(CHUNK_INDEX, 0)))
                .select(rowColumns(table))
                .build())
        {
            for (Record row : rows)
            {
                Row read = row(row);
                if (read.correctionOf() == null)
                {
                    found.putIfAbsent(read.seq(), read);
                }
            }
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
        return new ArrayList<>(found.values());
    }

    /**
     * Reads the records of an item made from operators' corrections
     * (STAGES.md §15.6c), which {@link #all} leaves out: one row per
     * record, by record number then record ID.
     *
     * @param table  the table
     * @param itemId the item's id
     * @return the records
     * @throws UncheckedIOException if the table can't be read
     */
    public static List<Row> corrections(Table table, UUID itemId)
    {
        if (table.schema().findField(CORRECTION_OF) == null)
        {
            return List.of();
        }
        Map<String, Row> found = new TreeMap<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(ITEM_ID, itemId),
                        Expressions.equal(CHUNK_INDEX, 0),
                        Expressions.notNull(CORRECTION_OF)))
                .select(rowColumns(table))
                .build())
        {
            for (Record row : rows)
            {
                Row read = row(row);
                found.putIfAbsent(String.format("%020d", read.seq()) + "/"
                        + read.recordId(), read);
            }
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
        return new ArrayList<>(found.values());
    }

    /**
     * Finds the items with records committed in a span of time, and those
     * records' IDs: for a reconciler, to find records whose messages were
     * lost.
     *
     * @param table the table
     * @param from  the start of the span, inclusive
     * @param to    its end, exclusive
     * @return each item's record IDs
     * @throws UncheckedIOException if the table can't be read
     */
    public static Map<UUID, Set<String>> committedBetween(Table table,
            Instant from, Instant to)
    {
        Map<UUID, Set<String>> found = new LinkedHashMap<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(CHUNK_INDEX, 0),
                        Expressions.greaterThanOrEqual(INGESTED_AT,
                                micros(from)),
                        Expressions.lessThan(INGESTED_AT, micros(to))))
                .select(ITEM_ID, RECORD_ID)
                .build())
        {
            for (Record row : rows)
            {
                found.computeIfAbsent((UUID) row.getField(ITEM_ID),
                        item -> new HashSet<>())
                        .add((String) row.getField(RECORD_ID));
            }
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
        return found;
    }

    /**
     * A time as Iceberg's timestamp literal.
     *
     * @param time the time
     * @return microseconds since the epoch
     */
    private static long micros(Instant time)
    {
        return TimeUnit.SECONDS.toMicros(time.getEpochSecond())
                + TimeUnit.NANOSECONDS.toMicros(time.getNano());
    }

    /**
     * Reads a record's first row.
     *
     * @param row the row, with {@link #ROW_COLUMNS}
     * @return the record
     */
    private static Row row(Record row)
    {
        return new Row(
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
                position(row, ERROR_LINE, ERROR_COLUMN),
                (String) row.getField(STAGE_VERSION),
                row.struct().field(CORRECTION_OF) == null ? null
                        : (String) row.getField(CORRECTION_OF));
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
        return failed(table, itemId, null);
    }

    /**
     * Reads an item's committed records that failed, in sequence order,
     * with the queue message each starts in, its broker IDs decrypted
     * (STAGES.md §9). A record committed twice is read once.
     *
     * @param table  the table
     * @param itemId the item
     * @param keys   unwraps the rows' data keys; null to leave the broker
     *               IDs unread
     * @return the records
     * @throws UncheckedIOException if the table can't be read
     * @throws ChunkIntegrityException if the broker IDs don't decrypt or
     *                                 parse
     */
    public static List<Failed> failed(Table table, UUID itemId,
            KeyProvider keys)
    {
        Map<Long, Failed> found = new TreeMap<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(ITEM_ID, itemId),
                        Expressions.equal(CHUNK_INDEX, 0),
                        Expressions.notNull(ERROR_CODE)))
                .select(failedColumns(table))
                .build())
        {
            for (Record row : rows)
            {
                if (row.struct().field(CORRECTION_OF) != null
                        && row.getField(CORRECTION_OF) != null)
                {
                    // A correction that failed: its failure is the
                    // original's (STAGES.md §15.6c).
                    continue;
                }
                if (found.containsKey((Long) row.getField(SEQ)))
                {
                    // A second copy of the record (STAGES.md §6).
                    continue;
                }
                Integer message = (Integer) row.getField(MESSAGE_INDEX);
                List<String> refs = keys == null ? null
                        : messageRefs(row, itemId, keys);
                found.put((Long) row.getField(SEQ), new Failed(
                        (Long) row.getField(SEQ),
                        (Long) row.getField(SOURCE_OFFSET),
                        (String) row.getField(ERROR_CODE),
                        position(row, ERROR_LINE, ERROR_COLUMN),
                        message == null ? -1 : message,
                        refs == null ? null : refs.get(0),
                        refs == null ? null : refs.get(1)));
            }
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
        return new ArrayList<>(found.values());
    }

    /**
     * Decrypts a failed record's broker IDs.
     *
     * @param row    its first row
     * @param itemId its item
     * @param keys   unwraps the row's data key
     * @return the broker ID and the replaced message's, either null; null
     *         if none were kept
     * @throws ChunkIntegrityException if they don't decrypt or parse
     */
    private static List<String> messageRefs(Record row, UUID itemId,
            KeyProvider keys)
    {
        Object encrypted = row.getField(MESSAGE_REFS_ENC);
        if (encrypted == null)
        {
            return null;
        }
        long seq = (Long) row.getField(SEQ);
        byte[] dataKey = keys.unwrap(new WrappedKey(
                (String) row.getField(KEK_REF),
                bytes(row.getField(BDK_WRAPPED))));
        String json = new String(RecordCrypto.decryptSmall(
                RecordCrypto.recordKey(dataKey, itemId, seq), itemId, seq,
                MESSAGE_REFS_CHUNK, 1, MAX_REFS_BYTES, bytes(encrypted)),
                StandardCharsets.UTF_8);
        try
        {
            List<Object> values = JSONArrayUtils.parse(json);
            if (values.size() != 2)
            {
                throw new ChunkIntegrityException("Malformed broker IDs");
            }
            List<String> refs = new ArrayList<>(2);
            for (Object value : values)
            {
                refs.add(value == null ? null : value.toString());
            }
            return refs;
        }
        catch (ParseException malformed)
        {
            throw new ChunkIntegrityException("Malformed broker IDs");
        }
    }

    /**
     * The bytes of a binary field.
     *
     * @param value the field, as Iceberg reads it
     * @return its bytes
     */
    private static byte[] bytes(Object value)
    {
        ByteBuffer buffer = ((ByteBuffer) value).duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
