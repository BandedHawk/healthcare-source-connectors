/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

import com.sdh.healthcare.connector.contract.position.SectionOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.convert.ConversionException;
import com.sdh.healthcare.connector.core.convert.ParquetRowJson;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.Section;
import com.sdh.healthcare.connector.core.split.SectionSplitter;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.split.SplitSession;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.schema.MessageType;

/**
 * One section per row of a Parquet file (design §6.2), as a JSON object
 * ({@link ParquetRowJson}, mapping {@value ParquetRowJson#MAPPING}). This
 * is how the data files of an Iceberg table intake are split (§5.3.18).
 * <p>
 * Rows are read with Iceberg's generic Parquet reader. A file written by
 * Iceberg carries field IDs; one without them gets fallback IDs, by
 * position, so any Parquet file can be read. Columns keep the file's own
 * names.
 * <p>
 * A row isn't a stretch of the file's bytes, so each section is the row's
 * JSON alone. Its source offset is the start of its row group, and its
 * origin is its row number, counted from 1 across the file, as the line,
 * with column 1. A section's resume point is the start of its row group
 * and how many of the group's rows are done; after a group's last row, the
 * start of the next group (or the file's size after the last). A resume
 * reads from that row group and skips the rows done.
 * <p>
 * Errors: {@code PARSE_PARQUET_MALFORMED} for a file that can't be read,
 * {@code PARSE_PARQUET_UNSUPPORTED_TYPE} for a variant, geometry or
 * geography column, and {@code RESUME_INVALID} for a resume point that
 * isn't a row of the file. A failure to read the item itself is an
 * {@link IOException}, so it is tried again.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ParquetSplitter implements SectionSplitter
{

    /**
     * Creates the splitter; the service loader calls this.
     */
    public ParquetSplitter()
    {
    }

    @Override
    public String format()
    {
        return "parquet";
    }

    @Override
    public SplitSession open(SeekableByteSource src, ResumePoint from,
            SplitConfig cfg) throws IOException, SplitException
    {
        long size = src.size();
        List<BlockMetaData> groups = new ArrayList<>();
        MessageType fileType;
        try (ParquetFileReader footer = ParquetFileReader.open(
                new ParquetFile(src, size)))
        {
            fileType = footer.getFileMetaData().getSchema();
            for (BlockMetaData group : footer.getRowGroups())
            {
                if (group.getRowCount() > 0)
                {
                    groups.add(group);
                }
            }
        }
        catch (IOException | RuntimeException failure)
        {
            throw readFailure(failure, 0);
        }
        Schema schema = ParquetSchemaUtil.convert(
                ParquetSchemaUtil.hasIds(fileType) ? fileType
                        : ParquetSchemaUtil.addFallbackIds(fileType));
        if (!ParquetRowJson.check(schema))
        {
            throw new SplitException(
                    ErrorCode.PARSE_PARQUET_UNSUPPORTED_TYPE, 0);
        }
        return new Session(src, size, schema, groups, from);
    }

    /**
     * A failure while reading the file: the item's own read failure is
     * passed on to be tried again; anything else means the file is broken.
     *
     * @param failure what was thrown
     * @param offset  where, for the error
     * @return the exception to throw
     * @throws IOException if the item couldn't be read
     */
    static SplitException readFailure(Exception failure, long offset)
            throws IOException
    {
        for (Throwable cause = failure; cause != null;
                cause = cause.getCause())
        {
            if (cause instanceof SourceReadException read)
            {
                throw read.failure();
            }
        }
        return new SplitException(ErrorCode.PARSE_PARQUET_MALFORMED, offset);
    }

    /** The rows of one file, from a resume point. */
    private static final class Session implements SplitSession
    {

        /** The file. */
        private final SeekableByteSource src;

        /** Its size. */
        private final long size;

        /** Its schema, with IDs. */
        private final Schema schema;

        /** Its row groups that hold rows, in file order. */
        private final List<BlockMetaData> groups;

        /** The row group being read; past the last when done. */
        private int group;

        /** The rows of it already done. */
        private long done;

        /** The rows of the file already done, before this group's. */
        private long rowNumber;

        /** The rows, once opened. */
        private CloseableIterable<Record> rows;

        /** Their iterator, once opened. */
        private Iterator<Record> iterator;

        /**
         * Finds the row to start at.
         *
         * @param src    the file
         * @param size   its size
         * @param schema its schema, with IDs
         * @param groups its row groups that hold rows
         * @param from   where to start
         * @throws SplitException if the resume point isn't a row of it
         */
        Session(SeekableByteSource src, long size, Schema schema,
                List<BlockMetaData> groups, ResumePoint from)
                throws SplitException
        {
            this.src = src;
            this.size = size;
            this.schema = schema;
            this.groups = groups;
            if (from.offset() == 0 && from.context().isEmpty())
            {
                return;
            }
            if (from.offset() >= size)
            {
                group = groups.size();
                return;
            }
            while (group < groups.size()
                    && groups.get(group).getStartingPos() != from.offset())
            {
                rowNumber += groups.get(group).getRowCount();
                group++;
            }
            done = from.context().isEmpty() ? 0 : from.context().get(0);
            if (group == groups.size() || from.context().size() > 1
                    || done < 0 || done >= groups.get(group).getRowCount())
            {
                throw new SplitException(ErrorCode.RESUME_INVALID,
                        from.offset());
            }
        }

        @Override
        public Optional<Section> next() throws IOException, SplitException
        {
            if (group >= groups.size())
            {
                return Optional.empty();
            }
            long start = groups.get(group).getStartingPos();
            byte[] json;
            try
            {
                if (iterator == null)
                {
                    open(start);
                }
                if (!iterator.hasNext())
                {
                    // The footer counted more rows than the groups hold.
                    throw new SplitException(
                            ErrorCode.PARSE_PARQUET_MALFORMED, start);
                }
                json = ParquetRowJson.write(iterator.next());
            }
            catch (ConversionException unsupported)
            {
                throw new SplitException(unsupported.code(), start);
            }
            catch (RuntimeException failure)
            {
                throw readFailure(failure, start);
            }
            done++;
            long row = rowNumber + done;
            ResumePoint resume;
            if (done < groups.get(group).getRowCount())
            {
                resume = ResumePoint.at(start, done);
            }
            else
            {
                rowNumber += done;
                done = 0;
                group++;
                resume = ResumePoint.at(group < groups.size()
                        ? groups.get(group).getStartingPos() : size);
            }
            return Optional.of(new Section(start, 0,
                    List.of(new Section.Literal(json)), resume,
                    new SectionOrigin(SourcePosition.START,
                            new SourcePosition(row, 1))));
        }

        /**
         * Opens the rows from a row group, and skips those done.
         *
         * @param start where the group starts
         * @throws IOException if the item can't be read
         */
        private void open(long start) throws IOException
        {
            rows = Parquet.read(new IcebergFile(src, size))
                    .project(schema)
                    .createReaderFunc(fileType -> GenericParquetReaders
                            .buildReader(schema, fileType))
                    .split(start, size - start)
                    .build();
            iterator = rows.iterator();
            for (long skipped = 0; skipped < done && iterator.hasNext();
                    skipped++)
            {
                iterator.next();
            }
        }

        @Override
        public void close()
        {
            if (rows != null)
            {
                try
                {
                    rows.close();
                }
                catch (IOException ignored)
                {
                    // Nothing to keep; the item is only read.
                }
            }
        }
    }

    /**
     * A failure to read the item, carried through the Parquet libraries,
     * which wrap what they are given.
     */
    static final class SourceReadException extends IOException
    {

        /** For serialization. */
        private static final long serialVersionUID = 1L;

        /** What the item's read threw. */
        private final transient IOException failure;

        /**
         * Wraps a read failure.
         *
         * @param failure what the read threw
         */
        SourceReadException(IOException failure)
        {
            super(failure.getClass().getSimpleName());
            this.failure = failure;
        }

        /**
         * What the read threw.
         *
         * @return the failure
         */
        IOException failure()
        {
            return failure;
        }
    }

    /** The item's bytes, from a position, for Iceberg's reader. */
    static final class Stream extends org.apache.iceberg.io.SeekableInputStream
    {

        /** The item. */
        private final SeekableByteSource src;

        /** Its size. */
        private final long size;

        /** The next byte to read. */
        private long position;

        /**
         * Opens the item at its start.
         *
         * @param src  the item
         * @param size its size
         */
        Stream(SeekableByteSource src, long size)
        {
            this.src = src;
            this.size = size;
        }

        @Override
        public long getPos()
        {
            return position;
        }

        @Override
        public void seek(long newPosition) throws IOException
        {
            if (newPosition < 0 || newPosition > size)
            {
                throw new EOFException("Seek outside the file");
            }
            position = newPosition;
        }

        @Override
        public int read() throws IOException
        {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            if (length == 0)
            {
                return 0;
            }
            if (position >= size)
            {
                return -1;
            }
            int read;
            try
            {
                read = src.read(position, ByteBuffer.wrap(buffer, offset,
                        (int) Math.min(length, size - position)));
            }
            catch (IOException failure)
            {
                throw new SourceReadException(failure);
            }
            if (read > 0)
            {
                position += read;
            }
            return read;
        }
    }

    /**
     * The item, for Iceberg's reader.
     *
     * @param source the item
     * @param size   its size, in bytes
     */
    private record IcebergFile(SeekableByteSource source, long size)
            implements InputFile
    {

        @Override
        public long getLength()
        {
            return size;
        }

        @Override
        public org.apache.iceberg.io.SeekableInputStream newStream()
        {
            return new Stream(source, size);
        }

        @Override
        public String location()
        {
            return "landed-item.parquet";
        }

        @Override
        public boolean exists()
        {
            return true;
        }
    }

    /**
     * The item, for Parquet's footer reader.
     *
     * @param source the item
     * @param size   its size, in bytes
     */
    private record ParquetFile(SeekableByteSource source, long size)
            implements org.apache.parquet.io.InputFile
    {

        @Override
        public long getLength()
        {
            return size;
        }

        @Override
        public org.apache.parquet.io.SeekableInputStream newStream()
        {
            Stream stream = new Stream(source, size);
            return new DelegatingSeekableInputStream(stream)
            {
                @Override
                public long getPos()
                {
                    return stream.getPos();
                }

                @Override
                public void seek(long newPosition) throws IOException
                {
                    stream.seek(newPosition);
                }
            };
        }
    }
}
