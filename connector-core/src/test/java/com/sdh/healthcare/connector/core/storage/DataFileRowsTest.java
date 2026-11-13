/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.table.DataFileRows;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.TestCatalogs;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Reading a data file's rows by position (PERFORMANCE.md §3.2): the rows
 * are those a scan finds, across pages and row groups, and only the pages
 * holding them are read.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class DataFileRowsTest
{

    /** The columns a record reader takes. */
    private static final List<String> COLUMNS = List.of(
            RecordsTable.RECORD_ID, RecordsTable.ITEM_ID, RecordsTable.SEQ,
            RecordsTable.CHUNK_INDEX, RecordsTable.CHUNK_COUNT,
            RecordsTable.PLAINTEXT_LENGTH, RecordsTable.CIPHERTEXT,
            RecordsTable.BDK_WRAPPED, RecordsTable.KEK_REF,
            RecordsTable.BATCH_ID, RecordsTable.INGESTED_AT);

    /** The records table, committed to with an epoch fence. */
    private Table table;

    /** The item every record here is in. */
    private final UUID itemId = UUID.randomUUID();

    /** The batch's data file. */
    private DataFile file;

    /** Each record's first row, by sequence number. */
    private final Map<Long, Long> firstRows = new HashMap<>();

    /** Each record's chunk count, by sequence number. */
    private final Map<Long, Integer> chunks = new HashMap<>();

    /**
     * Writes one batch: 40 small records, then five of 3 MiB; with at most
     * two rows a page, many pages.
     *
     * @throws Exception if the batch can't be written
     */
    @BeforeEach
    void writeBatch() throws Exception
    {
        table = IngestTables.records(TestCatalogs.jdbc().newClient(),
                "ns_" + UUID.randomUUID().toString().replace("-", ""));
        table.updateProperties()
                .set(TableProperties.PARQUET_PAGE_ROW_LIMIT, "2")
                .commit();
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return 1;
            }

            @Override
            public void checkHeld()
            {
            }
        });
        committer.establish(table);
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        AesKeyWrapProvider keys = new AesKeyWrapProvider(
                Map.of("kek-1", generator.generateKey()));
        Random random = new Random(7);
        try (RecordBatchWriter writer = new RecordBatchWriter(table, keys,
                "kek-1", new RecordIds(new byte[32]), Instant.now()))
        {
            for (long seq = 0; seq < 45; seq++)
            {
                byte[] plaintext = new byte[seq < 40 ? 2_000 : 3 << 20];
                random.nextBytes(plaintext);
                RecordBatchWriter.WrittenRecord written = writer.add(
                        new RecordBatchWriter.RecordInput(itemId, seq, 0,
                                "ccda", plaintext));
                firstRows.put(seq, writer.firstRow(written));
                chunks.put(seq, written.chunkCount());
            }
            file = writer.finish();
        }
        DataFile appended = file;
        committer.commit(table, transaction -> transaction.newFastAppend()
                .appendFile(appended).commit());
    }

    /**
     * Each record's rows, read by position, are the rows a scan finds.
     *
     * @throws IOException if the file can't be read
     */
    @Test
    void readsTheRowsAScanFinds() throws IOException
    {
        for (long seq : List.of(0L, 1L, 17L, 39L, 40L, 42L, 44L))
        {
            List<Record> read = DataFileRows.read(table.io(), table.schema(),
                    file.location(), file.fileSizeInBytes(),
                    firstRows.get(seq), chunks.get(seq), COLUMNS);
            List<Record> scanned = scan(seq);
            assertThat(read).as("seq %d", seq).hasSize(scanned.size());
            for (int index = 0; index < read.size(); index++)
            {
                for (String column : COLUMNS)
                {
                    assertThat(read.get(index).getField(column))
                            .as("seq %d, %s", seq, column)
                            .isEqualTo(scanned.get(index).getField(column));
                }
            }
        }
    }

    /**
     * A small record is read without the rest of the file: here, its
     * footer, indexes and a page or two, not the 15 MiB of ciphertext.
     *
     * @throws IOException if the file can't be read
     */
    @Test
    void readsOnlyThePagesHoldingTheRows() throws IOException
    {
        CountingIO io = new CountingIO(table.io());
        List<Record> read = DataFileRows.read(io, table.schema(),
                file.location(), file.fileSizeInBytes(), firstRows.get(5L),
                1, COLUMNS);
        assertThat(read).hasSize(1);
        assertThat(read.get(0).getField(RecordsTable.SEQ)).isEqualTo(5L);
        assertThat(io.bytes.get()).isLessThan(256 * 1024);
    }

    /**
     * Past the file's end, there are fewer rows than asked for.
     *
     * @throws IOException if the file can't be read
     */
    @Test
    void stopsAtTheEndOfTheFile() throws IOException
    {
        List<Record> read = DataFileRows.read(table.io(), table.schema(),
                file.location(), file.fileSizeInBytes(),
                file.recordCount() - 1, 5, COLUMNS);
        assertThat(read).hasSize(1);
    }

    /**
     * Rows that cross row groups come back whole and in order, with every
     * kind of value the tables hold converted as the generic reader does.
     *
     * @throws IOException if the file can't be written or read
     */
    @Test
    void readsAcrossRowGroupsEveryKindOfValue() throws IOException
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name",
                        Types.StringType.get()),
                Types.NestedField.optional(3, "uid", Types.UUIDType.get()),
                Types.NestedField.optional(4, "data",
                        Types.BinaryType.get()),
                Types.NestedField.optional(5, "count",
                        Types.IntegerType.get()),
                Types.NestedField.optional(6, "flag",
                        Types.BooleanType.get()),
                Types.NestedField.optional(7, "day", Types.DateType.get()),
                Types.NestedField.optional(8, "at",
                        Types.TimestampType.withZone()),
                Types.NestedField.optional(9, "local",
                        Types.TimestampType.withoutZone()),
                Types.NestedField.optional(10, "offsets",
                        Types.ListType.ofRequired(11,
                                Types.LongType.get())),
                Types.NestedField.optional(12, "refs",
                        Types.ListType.ofOptional(13,
                                Types.StringType.get())),
                Types.NestedField.optional(14, "ratio",
                        Types.DoubleType.get()));
        List<Record> written = new ArrayList<>();
        Random random = new Random(3);
        for (long id = 0; id < 60; id++)
        {
            Record row = GenericRecord.create(schema);
            row.setField("id", id);
            row.setField("name", id % 7 == 0 ? null : "row " + id);
            row.setField("uid", new UUID(random.nextLong(),
                    random.nextLong()));
            byte[] data = new byte[1_000];
            random.nextBytes(data);
            row.setField("data", ByteBuffer.wrap(data));
            row.setField("count", (int) id * 3);
            row.setField("flag", id % 2 == 0);
            row.setField("day", LocalDate.of(2026, 11, 13).plusDays(id));
            row.setField("at", OffsetDateTime.of(2026, 11, 13, 1, 2, 3,
                    456_789_000, ZoneOffset.UTC).plusSeconds(id));
            row.setField("local", LocalDateTime.of(2026, 1, 2, 3, 4, 5)
                    .plusMinutes(id));
            row.setField("offsets", id % 5 == 0 ? List.of()
                    : List.of(id, id + 1, id + 2));
            row.setField("refs", id % 3 == 0 ? null
                    : java.util.Arrays.asList("a" + id, null));
            row.setField("ratio", id / 4.0);
            written.add(row);
        }
        String location = table.location() + "/data/types-"
                + UUID.randomUUID() + ".parquet";
        try (FileAppender<Record> appender = Parquet.write(
                table.io().newOutputFile(location))
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::create)
                .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
                .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
                        "7")
                .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT,
                        "7")
                .set(TableProperties.PARQUET_PAGE_ROW_LIMIT, "2")
                .build())
        {
            appender.addAll(written);
        }
        long size = table.io().newInputFile(location).getLength();
        try (ParquetFileReader footer = ParquetFileReader.open(
                DataFileRows.parquetFile(table.io().newInputFile(location))))
        {
            assertThat(footer.getRowGroups()).hasSizeGreaterThan(3);
        }
        List<String> columns = schema.columns().stream()
                .map(Types.NestedField::name).toList();
        for (int[] range : new int[][] {{0, 1}, {5, 4}, {6, 9}, {13, 30},
                {55, 5}, {0, 60}})
        {
            List<Record> read = DataFileRows.read(table.io(), schema,
                    location, size, range[0], range[1], columns);
            assertThat(read).as("rows %d+%d", range[0], range[1])
                    .hasSize(range[1]);
            for (int index = 0; index < range[1]; index++)
            {
                Record expected = written.get(range[0] + index);
                for (String column : columns)
                {
                    assertThat(read.get(index).getField(column))
                            .as("row %d, %s", range[0] + index, column)
                            .isEqualTo(expected.getField(column));
                }
            }
        }
    }

    /**
     * A record's rows by scan, in chunk order.
     *
     * @param seq the record's sequence number
     * @return its rows
     * @throws IOException if the scan can't be closed
     */
    private List<Record> scan(long seq) throws IOException
    {
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<Record> scan = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal(RecordsTable.ITEM_ID, itemId),
                        Expressions.equal(RecordsTable.SEQ, seq)))
                .select(COLUMNS)
                .build())
        {
            scan.forEach(row -> rows.add(row.copy()));
        }
        rows.sort((left, right) -> Integer.compare(
                (Integer) left.getField(RecordsTable.CHUNK_INDEX),
                (Integer) right.getField(RecordsTable.CHUNK_INDEX)));
        return rows;
    }

    /**
     * A file IO that counts the bytes read through it.
     */
    private static final class CountingIO implements FileIO
    {

        /** The IO counted. */
        private final FileIO delegate;

        /** The bytes read so far. */
        private final AtomicLong bytes = new AtomicLong();

        /**
         * Counts reads through an IO.
         *
         * @param delegate the IO
         */
        CountingIO(FileIO delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public InputFile newInputFile(String path)
        {
            return counted(delegate.newInputFile(path));
        }

        @Override
        public InputFile newInputFile(String path, long length)
        {
            return counted(delegate.newInputFile(path, length));
        }

        @Override
        public OutputFile newOutputFile(String path)
        {
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path)
        {
            delegate.deleteFile(path);
        }

        /**
         * An input file whose streams count what they read.
         *
         * @param file the file
         * @return the counted file
         */
        private InputFile counted(InputFile file)
        {
            return new InputFile()
            {
                @Override
                public long getLength()
                {
                    return file.getLength();
                }

                @Override
                public SeekableInputStream newStream()
                {
                    SeekableInputStream stream = file.newStream();
                    return new SeekableInputStream()
                    {
                        @Override
                        public long getPos() throws IOException
                        {
                            return stream.getPos();
                        }

                        @Override
                        public void seek(long position) throws IOException
                        {
                            stream.seek(position);
                        }

                        @Override
                        public int read() throws IOException
                        {
                            int value = stream.read();
                            if (value >= 0)
                            {
                                bytes.incrementAndGet();
                            }
                            return value;
                        }

                        @Override
                        public int read(byte[] buffer, int offset,
                                int length) throws IOException
                        {
                            int count = stream.read(buffer, offset, length);
                            if (count > 0)
                            {
                                bytes.addAndGet(count);
                            }
                            return count;
                        }

                        @Override
                        public void close() throws IOException
                        {
                            stream.close();
                        }
                    };
                }

                @Override
                public String location()
                {
                    return file.location();
                }

                @Override
                public boolean exists()
                {
                    return file.exists();
                }
            };
        }
    }
}
