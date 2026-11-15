/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.table;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.RangeReadable;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * A data file for Parquet to read a few rows from, in as few requests as
 * the store allows (docs/PERFORMANCE.md §10). Its tail, which holds the
 * footer and the page indexes, is read once, in one request, and kept.
 * Before it, a read takes {@value #READ_AHEAD} bytes from where it starts,
 * kept until a read falls outside them: Parquet reads a row's columns one
 * after another, a page or two each, near each other in the file, so a
 * few rows' pages come in one request. A read longer than that is a
 * request of its own.
 * <p>
 * Read through a plain stream, a seek back (to the footer, the offset
 * index, then the pages) is a new request each time, and on S3 each one
 * abandons the request before it, and with it the connection. Through
 * {@link RangeReadable}, which Iceberg's object store streams are, a
 * request asks for a closed range and reads it to the end, so the
 * connection is kept for the next. A stream that isn't is read by seek, as
 * before. A file gone is Iceberg's {@link NotFoundException} either way, as
 * its plain stream gives, so a reader can find the rows elsewhere.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class RangedInputFile implements InputFile
{

    /**
     * How much of the end of a file is read at once: several times the
     * footer and page indexes of a landing group's file (a few hundred
     * rows, a few dozen columns). Any of them before it is read by range,
     * as the pages are.
     */
    static final int TAIL = 128 * 1024;

    /** How much is read at once before the tail. */
    static final int READ_AHEAD = 256 * 1024;

    /** The file. */
    private final org.apache.iceberg.io.InputFile file;

    /** Its size, in bytes. */
    private final long length;

    /**
     * Wraps a file.
     *
     * @param file   the file
     * @param length its size, in bytes
     */
    RangedInputFile(org.apache.iceberg.io.InputFile file, long length)
    {
        this.file = file;
        this.length = length;
    }

    @Override
    public long getLength()
    {
        return length;
    }

    @Override
    public SeekableInputStream newStream()
    {
        return new Stream(file.newStream(), length);
    }

    /**
     * Reads a file's tail from memory, the rest by range.
     */
    static final class Stream extends SeekableInputStream
    {

        /** The file's stream. */
        private final org.apache.iceberg.io.SeekableInputStream in;

        /** The file's size. */
        private final long length;

        /** Where the tail starts. */
        private final long tailStart;

        /** The tail; null until read. */
        private byte[] tail;

        /** The bytes last read ahead; null if none. */
        private byte[] block;

        /** Where they start. */
        private long blockStart;

        /** The position read from next. */
        private long position;

        /**
         * Wraps a file's stream.
         *
         * @param in     the stream
         * @param length the file's size
         */
        Stream(org.apache.iceberg.io.SeekableInputStream in, long length)
        {
            this.in = in;
            this.length = length;
            this.tailStart = Math.max(0, length - TAIL);
        }

        @Override
        public long getPos()
        {
            return position;
        }

        @Override
        public void seek(long newPosition) throws IOException
        {
            if (newPosition < 0)
            {
                throw new EOFException("Seek before the start of the file");
            }
            position = newPosition;
        }

        @Override
        public long skip(long count)
        {
            long skipped = Math.max(0, Math.min(count, length - position));
            position += skipped;
            return skipped;
        }

        @Override
        public int read() throws IOException
        {
            if (position >= length)
            {
                return -1;
            }
            byte[] one = new byte[1];
            fetch(position, one, 0, 1);
            position++;
            return one[0] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int count)
                throws IOException
        {
            if (count == 0)
            {
                return 0;
            }
            if (position >= length)
            {
                return -1;
            }
            int read = (int) Math.min(count, length - position);
            fetch(position, buffer, offset, read);
            position += read;
            return read;
        }

        @Override
        public void readFully(byte[] buffer) throws IOException
        {
            readFully(buffer, 0, buffer.length);
        }

        @Override
        public void readFully(byte[] buffer, int offset, int count)
                throws IOException
        {
            if (position + count > length)
            {
                throw new EOFException("Read past the end of the file");
            }
            fetch(position, buffer, offset, count);
            position += count;
        }

        @Override
        public int read(ByteBuffer buffer) throws IOException
        {
            if (!buffer.hasRemaining())
            {
                return 0;
            }
            if (position >= length)
            {
                return -1;
            }
            int read = (int) Math.min(buffer.remaining(), length - position);
            put(buffer, read);
            return read;
        }

        @Override
        public void readFully(ByteBuffer buffer) throws IOException
        {
            if (position + buffer.remaining() > length)
            {
                throw new EOFException("Read past the end of the file");
            }
            put(buffer, buffer.remaining());
        }

        @Override
        public void close() throws IOException
        {
            tail = null;
            block = null;
            in.close();
        }

        /**
         * Reads bytes from the position into a buffer, moving both on.
         *
         * @param buffer the buffer
         * @param count  how many; no more than are left in the file
         * @throws IOException if the file can't be read
         */
        private void put(ByteBuffer buffer, int count) throws IOException
        {
            if (buffer.hasArray())
            {
                fetch(position, buffer.array(),
                        buffer.arrayOffset() + buffer.position(), count);
                buffer.position(buffer.position() + count);
            }
            else
            {
                byte[] bytes = new byte[count];
                fetch(position, bytes, 0, count);
                buffer.put(bytes);
            }
            position += count;
        }

        /**
         * Reads bytes at a position: from the tail where they are in it,
         * the rest from what was read ahead, or a request.
         *
         * @param at     the position
         * @param buffer where they go
         * @param offset where in it
         * @param count  how many; all are in the file
         * @throws IOException if the file can't be read
         */
        private void fetch(long at, byte[] buffer, int offset, int count)
                throws IOException
        {
            int before = (int) Math.max(0, Math.min(count, tailStart - at));
            if (before > 0)
            {
                ahead(at, buffer, offset, before);
            }
            if (before < count)
            {
                byte[] kept = tail();
                System.arraycopy(kept, (int) (at + before - tailStart),
                        buffer, offset + before, count - before);
            }
        }

        /**
         * Reads bytes before the tail: from what was read ahead if they
         * are in it; otherwise, if few, reads ahead from them; if many,
         * reads just them.
         *
         * @param at     the position
         * @param buffer where they go
         * @param offset where in it
         * @param count  how many; all before the tail
         * @throws IOException if the file can't be read
         */
        private void ahead(long at, byte[] buffer, int offset, int count)
                throws IOException
        {
            if (block == null || at < blockStart
                    || at + count > blockStart + block.length)
            {
                if (count >= READ_AHEAD)
                {
                    ranged(at, buffer, offset, count);
                    return;
                }
                int size = (int) Math.min(READ_AHEAD, tailStart - at);
                byte[] bytes = new byte[size];
                ranged(at, bytes, 0, size);
                block = bytes;
                blockStart = at;
            }
            System.arraycopy(block, (int) (at - blockStart), buffer, offset,
                    count);
        }

        /**
         * The tail, read in one request the first time.
         *
         * @return its bytes
         * @throws IOException if the file can't be read
         */
        private byte[] tail() throws IOException
        {
            if (tail == null)
            {
                byte[] bytes = new byte[(int) (length - tailStart)];
                if (in instanceof RangeReadable ranges)
                {
                    int read;
                    try
                    {
                        read = ranges.readTail(bytes, 0, bytes.length);
                    }
                    catch (RuntimeException failed)
                    {
                        throw gone(failed);
                    }
                    if (read != bytes.length)
                    {
                        throw new EOFException("File shorter than its size");
                    }
                }
                else
                {
                    ranged(tailStart, bytes, 0, bytes.length);
                }
                tail = bytes;
            }
            return tail;
        }

        /**
         * Reads bytes at a position in one request, or by seek if the
         * stream can't read a range.
         *
         * @param at     the position
         * @param buffer where they go
         * @param offset where in it
         * @param count  how many
         * @throws IOException if the file can't be read
         */
        private void ranged(long at, byte[] buffer, int offset, int count)
                throws IOException
        {
            if (in instanceof RangeReadable ranges)
            {
                try
                {
                    ranges.readFully(at, buffer, offset, count);
                }
                catch (RuntimeException failed)
                {
                    throw gone(failed);
                }
                return;
            }
            in.seek(at);
            int done = 0;
            while (done < count)
            {
                int read = in.read(buffer, offset + done, count - done);
                if (read < 0)
                {
                    throw new EOFException("File shorter than its size");
                }
                done += read;
            }
        }

        /**
         * What a failed range read means: a missing object (the store's
         * {@code NoSuchKeyException}, known by name, so this needs no store
         * library) as Iceberg's {@link NotFoundException}; anything else as
         * it came.
         *
         * @param failed what the read threw
         * @return the exception to throw
         */
        static RuntimeException gone(RuntimeException failed)
        {
            if (failed instanceof NotFoundException
                    || !"NoSuchKeyException".equals(
                            failed.getClass().getSimpleName()))
            {
                return failed;
            }
            return new NotFoundException(failed, "Data file not found");
        }
    }
}
