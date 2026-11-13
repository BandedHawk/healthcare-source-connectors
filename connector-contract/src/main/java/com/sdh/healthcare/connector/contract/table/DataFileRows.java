/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

/**
 * Reads rows of an Iceberg data file by their positions in it, without
 * planning a scan of the table (PERFORMANCE.md §3.2). A message says which
 * file and rows hold a record; this reads the file's footer and offset
 * index, then only the pages that hold those rows, through the table's
 * {@link FileIO}, so it is the same on every store.
 * <p>
 * The rows come back as Iceberg records of the projected schema, with the
 * values the generic reader would give: UUIDs, strings, byte buffers,
 * dates, timestamps (UTC for those with a zone) and lists of those.
 * Callers check what they read as they would a scanned row: a wrong file
 * or position can only fail that check or decryption, never pass as
 * another record.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class DataFileRows
{

    /** Not instantiated. */
    private DataFileRows()
    {
    }

    /**
     * Reads rows by position.
     *
     * @param io       the table's file IO
     * @param schema   the table's schema
     * @param file     the data file, as {@code DataFile.location()}
     * @param fileSize its size in bytes
     * @param first    the position of the first row wanted, from 0
     * @param count    how many rows, from {@code first}
     * @param columns  the columns wanted, by name
     * @return the rows found, in order; fewer than {@code count} if the file
     *         ends first
     * @throws IOException if the file can't be read; Iceberg's
     *         {@code NotFoundException} if it is gone
     */
    public static List<Record> read(FileIO io, Schema schema, String file,
            long fileSize, long first, int count, Collection<String> columns)
            throws IOException
    {
        Schema projected = schema.select(columns);
        Set<Integer> ids = new HashSet<>();
        for (Types.NestedField field : projected.columns())
        {
            ids.add(field.fieldId());
        }
        long end = first + count;
        List<Record> rows = new ArrayList<>(count);
        ParquetReadOptions options = ParquetReadOptions.builder().build();
        try (ParquetFileReader reader = ParquetFileReader.open(
                new IcebergInput(io.newInputFile(file, fileSize)), options))
        {
            MessageType fileSchema =
                    reader.getFooter().getFileMetaData().getSchema();
            MessageType requested = new MessageType(fileSchema.getName(),
                    fileSchema.getFields().stream()
                            .filter(field -> field.getId() != null
                                    && ids.contains(field.getId().intValue()))
                            .toList());
            reader.setRequestedSchema(requested);
            MessageColumnIO columnIO = new ColumnIOFactory()
                    .getColumnIO(requested, fileSchema);
            List<BlockMetaData> blocks = reader.getRowGroups();
            long start = 0;
            for (int block = 0; block < blocks.size() && start < end;
                    block++)
            {
                BlockMetaData meta = blocks.get(block);
                long groupRows = meta.getRowCount();
                long from = Math.max(first, start);
                long to = Math.min(end, start + groupRows);
                if (from < to)
                {
                    RowRanges ranges = ranges(reader, meta, requested,
                            from - start, to - start);
                    PageReadStore pages =
                            reader.readFilteredRowGroup(block, ranges);
                    read(columnIO, pages, new Materializer(projected,
                            requested), start, from, to, rows);
                }
                start += groupRows;
            }
        }
        return rows;
    }

    /**
     * Which rows of a row group to read: those of the pages of its largest
     * requested column that hold the rows wanted. The other columns' pages
     * holding those rows come with them.
     *
     * @param reader    the file's reader
     * @param block     the row group
     * @param requested the columns read
     * @param from      the first row wanted, within the group
     * @param to        the row after the last wanted, within the group
     * @return the rows to read; the whole group if the file has no offset
     *         index
     * @throws IOException if the offset index can't be read
     */
    private static RowRanges ranges(ParquetFileReader reader,
            BlockMetaData block, MessageType requested, long from, long to)
            throws IOException
    {
        long groupRows = block.getRowCount();
        ColumnChunkMetaData largest = null;
        for (ColumnChunkMetaData column : block.getColumns())
        {
            if (requested.containsPath(column.getPath().toArray())
                    && (largest == null
                            || column.getTotalSize() > largest.getTotalSize()))
            {
                largest = column;
            }
        }
        OffsetIndex index = largest == null ? null
                : reader.readOffsetIndex(largest);
        if (index == null)
        {
            return RowRanges.createSingle(groupRows);
        }
        PrimitiveIterator.OfInt pages = IntStream
                .range(0, index.getPageCount())
                .filter(page -> index.getFirstRowIndex(page) < to
                        && index.getLastRowIndex(page, groupRows) >= from)
                .iterator();
        return RowRanges.create(groupRows, pages, index);
    }

    /**
     * Reads a row group's filtered rows, keeping those wanted.
     *
     * @param columnIO     the columns read
     * @param pages        the row group's pages read
     * @param materializer builds the rows
     * @param groupStart   the file position of the group's first row
     * @param from         the first file position wanted
     * @param to           the file position after the last wanted
     * @param rows         where the rows wanted go
     */
    private static void read(MessageColumnIO columnIO, PageReadStore pages,
            Materializer materializer, long groupStart, long from, long to,
            List<Record> rows)
    {
        RecordReader<Record> records = columnIO.getRecordReader(pages,
                materializer, FilterCompat.NOOP);
        PrimitiveIterator.OfLong indexes = pages.getRowIndexes().orElse(null);
        long read = pages.getRowCount();
        for (long next = 0; next < read; next++)
        {
            Record row = records.read();
            long position = groupStart
                    + (indexes == null ? next : indexes.nextLong());
            if (position >= from && position < to)
            {
                rows.add(row);
            }
        }
    }

    /**
     * A Parquet input over an Iceberg input file, for reading a data file's
     * footer.
     *
     * @param file the Iceberg input file
     * @return the Parquet input
     */
    public static InputFile parquetFile(org.apache.iceberg.io.InputFile file)
    {
        return new IcebergInput(file);
    }

    /**
     * A Parquet input over an Iceberg input file.
     *
     * @param file the Iceberg input file
     */
    private record IcebergInput(org.apache.iceberg.io.InputFile file)
            implements InputFile
    {

        @Override
        public long getLength()
        {
            return file.getLength();
        }

        @Override
        public SeekableInputStream newStream()
        {
            org.apache.iceberg.io.SeekableInputStream stream =
                    file.newStream();
            return new DelegatingSeekableInputStream(stream)
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
            };
        }
    }

    /**
     * Builds Iceberg records from Parquet's record assembly.
     */
    private static final class Materializer extends RecordMaterializer<Record>
    {

        /** The root converter. */
        private final StructConverter root;

        /**
         * Builds converters for the requested columns.
         *
         * @param projected the Iceberg schema of the columns read
         * @param requested the Parquet schema of the same columns
         */
        Materializer(Schema projected, MessageType requested)
        {
            this.root = new StructConverter(projected.asStruct(), requested,
                    null);
        }

        @Override
        public Record getCurrentRecord()
        {
            return root.current;
        }

        @Override
        public GroupConverter getRootConverter()
        {
            return root;
        }
    }

    /** Where a converted value goes. */
    @FunctionalInterface
    private interface Sink
    {

        /**
         * Takes a value.
         *
         * @param value the value
         */
        void accept(Object value);
    }

    /**
     * Builds a struct, the row itself at the root.
     */
    private static final class StructConverter extends GroupConverter
    {

        /** The struct's Iceberg type. */
        private final Types.StructType type;

        /** A converter per Parquet field, in the Parquet schema's order. */
        private final Converter[] converters;

        /** Where the finished struct goes; null at the root. */
        private final Sink parent;

        /** The struct being built. */
        private Record current;

        /**
         * Builds converters for a struct's fields, matched by field ID.
         *
         * @param type   the struct's Iceberg type
         * @param group  its Parquet group
         * @param parent where the finished struct goes; null at the root
         */
        StructConverter(Types.StructType type, GroupType group, Sink parent)
        {
            this.type = type;
            this.parent = parent;
            this.converters = new Converter[group.getFieldCount()];
            for (int index = 0; index < converters.length; index++)
            {
                org.apache.parquet.schema.Type field = group.getType(index);
                Types.NestedField iceberg =
                        type.field(field.getId().intValue());
                String name = iceberg.name();
                converters[index] = converter(iceberg.type(), field,
                        value -> current.setField(name, value));
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return converters[fieldIndex];
        }

        @Override
        public void start()
        {
            current = GenericRecord.create(type);
        }

        @Override
        public void end()
        {
            if (parent != null)
            {
                parent.accept(current);
            }
        }
    }

    /**
     * Builds a list from Parquet's three-level list.
     */
    private static final class ListConverter extends GroupConverter
    {

        /** The repeated group's converter. */
        private final GroupConverter repeated;

        /** Where the finished list goes. */
        private final Sink parent;

        /** The list being built. */
        private List<Object> current;

        /** Whether the element being read had a value. */
        private boolean valued;

        /**
         * Builds converters for a list's elements.
         *
         * @param type   the list's Iceberg type
         * @param group  its Parquet group: {@code list { repeated group
         *               list { element } }}
         * @param parent where the finished list goes
         */
        ListConverter(Types.ListType type, GroupType group, Sink parent)
        {
            this.parent = parent;
            GroupType middle = group.getType(0).asGroupType();
            Converter element = converter(type.elementType(),
                    middle.getType(0), value ->
                    {
                        current.add(value);
                        valued = true;
                    });
            this.repeated = new GroupConverter()
            {
                @Override
                public Converter getConverter(int fieldIndex)
                {
                    return element;
                }

                @Override
                public void start()
                {
                    valued = false;
                }

                @Override
                public void end()
                {
                    // An element without a value is a null in the list.
                    if (!valued)
                    {
                        current.add(null);
                    }
                }
            };
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return repeated;
        }

        @Override
        public void start()
        {
            current = new ArrayList<>();
        }

        @Override
        public void end()
        {
            parent.accept(current);
        }
    }

    /**
     * A converter for a value of an Iceberg type.
     *
     * @param type  the Iceberg type
     * @param field the Parquet field
     * @param sink  where the value goes
     * @return the converter
     * @throws UnsupportedOperationException for a map, or a type no table
     *         here uses
     */
    private static Converter converter(Type type,
            org.apache.parquet.schema.Type field, Sink sink)
    {
        return switch (type.typeId())
        {
            case STRUCT -> new StructConverter(type.asStructType(),
                    field.asGroupType(), sink);
            case LIST -> new ListConverter(type.asListType(),
                    field.asGroupType(), sink);
            case MAP -> throw new UnsupportedOperationException(
                    "Maps aren't read by position");
            default -> new ValueConverter(type, sink);
        };
    }

    /**
     * Converts a primitive value as Iceberg's generic reader would.
     */
    private static final class ValueConverter extends PrimitiveConverter
    {

        /** The value's Iceberg type. */
        private final Type type;

        /** Where the value goes. */
        private final Sink sink;

        /**
         * Builds the converter.
         *
         * @param type the value's Iceberg type
         * @param sink where the value goes
         */
        ValueConverter(Type type, Sink sink)
        {
            this.type = type;
            this.sink = sink;
        }

        @Override
        public void addBinary(Binary value)
        {
            sink.accept(switch (type.typeId())
            {
                case STRING -> value.toStringUsingUTF8();
                case UUID ->
                {
                    ByteBuffer bytes = value.toByteBuffer();
                    yield new UUID(bytes.getLong(bytes.position()),
                            bytes.getLong(bytes.position() + 8));
                }
                default -> ByteBuffer.wrap(value.getBytes());
            });
        }

        @Override
        public void addInt(int value)
        {
            sink.accept(switch (type.typeId())
            {
                case DATE -> LocalDate.ofEpochDay(value);
                case LONG -> (long) value;
                default -> value;
            });
        }

        @Override
        public void addLong(long value)
        {
            sink.accept(switch (type.typeId())
            {
                case TIMESTAMP -> timestamp((Types.TimestampType) type,
                        TimeUnit.MICROSECONDS, value);
                case TIMESTAMP_NANO -> timestamp(
                        (Types.TimestampNanoType) type, value);
                default -> value;
            });
        }

        @Override
        public void addBoolean(boolean value)
        {
            sink.accept(value);
        }

        @Override
        public void addFloat(float value)
        {
            sink.accept(type.typeId() == Type.TypeID.DOUBLE
                    ? (double) value : value);
        }

        @Override
        public void addDouble(double value)
        {
            sink.accept(value);
        }
    }

    /**
     * A timestamp in microseconds, as the generic reader gives it.
     *
     * @param type  its type
     * @param unit  the stored unit
     * @param value the stored value
     * @return an {@link OffsetDateTime} in UTC with a zone, a
     *         {@link LocalDateTime} without
     */
    private static Object timestamp(Types.TimestampType type, TimeUnit unit,
            long value)
    {
        long micros = unit.toMicros(value);
        Instant instant = Instant.ofEpochSecond(
                Math.floorDiv(micros, 1_000_000L),
                Math.floorMod(micros, 1_000_000L) * 1_000L);
        return type.shouldAdjustToUTC()
                ? instant.atOffset(ZoneOffset.UTC)
                : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    /**
     * A timestamp in nanoseconds, as the generic reader gives it.
     *
     * @param type  its type
     * @param nanos the stored value
     * @return an {@link OffsetDateTime} in UTC with a zone, a
     *         {@link LocalDateTime} without
     */
    private static Object timestamp(Types.TimestampNanoType type, long nanos)
    {
        Instant instant = Instant.ofEpochSecond(
                Math.floorDiv(nanos, 1_000_000_000L),
                Math.floorMod(nanos, 1_000_000_000L));
        return type.shouldAdjustToUTC()
                ? instant.atOffset(ZoneOffset.UTC)
                : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
