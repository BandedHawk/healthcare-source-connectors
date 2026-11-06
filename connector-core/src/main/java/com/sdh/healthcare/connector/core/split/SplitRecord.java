/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

import com.sdh.healthcare.connector.contract.position.RecordOrigin;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * One self-contained record, described as byte ranges of the source plus
 * any bytes the splitter made up (design §6.1). A record never holds the
 * source bytes themselves, so a splitter needs almost no memory however large
 * the record is.
 *
 * @param sourceOffset where the record starts in the source file
 * @param sourceLength how many source bytes it spans
 * @param parts        the record's plaintext, in order: literal bytes (such
 *                     as a CSV header or an X12 envelope) and source ranges
 * @param resume       where a new session starts to produce the records
 *                     after this one
 * @param origin       where the record's text came from in the file, as
 *                     lines and columns; null until the pipeline works it out
 *                     ({@code RecordOrigins}), as splitters don't count
 *                     lines
 * @param failure      for the rest of a message in a batch that couldn't be
 *                     split, why; it is stored as split, unconverted, and
 *                     the next message carries on (design §5.3.16); null
 *                     for every other record
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record SplitRecord(long sourceOffset, long sourceLength,
        List<Part> parts, ResumePoint resume, RecordOrigin origin,
        Failure failure)
{

    /**
     * Why part of a message couldn't be split.
     *
     * @param code   the splitter's error code
     * @param offset where in the item the error was found
     */
    public record Failure(ErrorCode code, long offset)
    {
    }

    /** A piece of a record's plaintext. */
    public sealed interface Part permits Literal, SourceRange
    {

        /**
         * How many bytes the piece is.
         *
         * @return its length
         */
        long length();
    }

    /**
     * Bytes the splitter made up, such as a CSV header repeated before each
     * row. They aren't copied, so callers must not change the array.
     *
     * @param bytes the bytes
     */
    public record Literal(byte[] bytes) implements Part
    {

        /**
         * {@inheritDoc}
         *
         * @return the number of bytes
         */
        @Override
        public long length()
        {
            return bytes.length;
        }
    }

    /**
     * A range of the source file's bytes.
     *
     * @param offset where the range starts in the file
     * @param length how many bytes it covers
     */
    public record SourceRange(long offset, long length) implements Part
    {
    }

    /**
     * Keeps an unchangeable copy of the parts.
     */
    public SplitRecord
    {
        parts = List.copyOf(parts);
    }

    /**
     * A record as a splitter makes it, without an origin.
     *
     * @param sourceOffset where the record starts in the source file
     * @param sourceLength how many source bytes it spans
     * @param parts        the record's plaintext, in order
     * @param resume       where a new session starts to produce the records
     *                     after this one
     */
    public SplitRecord(long sourceOffset, long sourceLength, List<Part> parts,
            ResumePoint resume)
    {
        this(sourceOffset, sourceLength, parts, resume, null, null);
    }

    /**
     * A record with its origin and no failure.
     *
     * @param sourceOffset where the record starts in the source file
     * @param sourceLength how many source bytes it spans
     * @param parts        the record's plaintext, in order
     * @param resume       where a new session starts to produce the records
     *                     after this one
     * @param origin       where the record's text came from in the file
     */
    public SplitRecord(long sourceOffset, long sourceLength, List<Part> parts,
            ResumePoint resume, RecordOrigin origin)
    {
        this(sourceOffset, sourceLength, parts, resume, origin, null);
    }

    /**
     * The same record with its origin.
     *
     * @param origin where the record's text came from in the file
     * @return a new record with that origin
     */
    public SplitRecord withOrigin(RecordOrigin origin)
    {
        return new SplitRecord(sourceOffset, sourceLength, parts, resume,
                origin, failure);
    }

    /**
     * A record that is a single range of the source, with nothing added.
     *
     * @param offset where it starts in the file
     * @param length how many bytes it covers
     * @param resume where a new session starts to produce the records after
     *               this one
     * @return the record
     */
    public static SplitRecord ofRange(long offset, long length,
            ResumePoint resume)
    {
        return new SplitRecord(offset, length,
                List.of(new SourceRange(offset, length)), resume);
    }

    /**
     * The length of the record's plaintext: of all its parts together.
     *
     * @return the length, in bytes
     */
    public long plaintextLength()
    {
        long total = 0;
        for (Part part : parts)
        {
            total += part.length();
        }
        return total;
    }

    /**
     * Streams the record's plaintext, reading its ranges from the source as
     * they are needed.
     *
     * @param source the file the record came from
     * @return a stream of the plaintext
     */
    public InputStream open(SeekableByteSource source)
    {
        return new RecordStream(source, parts);
    }

    /**
     * Reads the whole record into memory. For small records and tests.
     *
     * @param source the file the record came from
     * @return the plaintext
     * @throws IOException if the record is too large for an array, or the
     *                     source can't be read or ends inside the record
     */
    public byte[] readAll(SeekableByteSource source) throws IOException
    {
        long length = plaintextLength();
        if (length > Integer.MAX_VALUE - 8)
        {
            throw new IOException("SplitRecord too large to read into memory");
        }
        try (InputStream input = open(source))
        {
            return input.readNBytes((int) length);
        }
    }

    /**
     * The plaintext of a record as a stream: its parts one after another,
     * literal bytes copied and source ranges read as they come.
     */
    private static final class RecordStream extends InputStream
    {

        /** The file the source ranges are read from. */
        private final SeekableByteSource source;

        /** The record's parts, in order. */
        private final List<Part> parts;

        /** The index of the part being read; -1 before the first. */
        private int index = -1;

        /** The part being read; null once all have been read. */
        private Part current;

        /** How many bytes of the current part have been read. */
        private long done;

        /**
         * Starts a stream at the first part that isn't empty.
         *
         * @param source the file the source ranges are read from
         * @param parts  the record's parts, in order
         */
        RecordStream(SeekableByteSource source, List<Part> parts)
        {
            this.source = source;
            this.parts = parts;
            advance();
        }

        /** Moves to the next part that isn't empty, or past the last. */
        private void advance()
        {
            do
            {
                index++;
                current = index < parts.size() ? parts.get(index) : null;
                done = 0;
            }
            while (current != null && current.length() == 0);
        }

        /**
         * Reads one byte.
         *
         * @return the byte, from 0 to 255; -1 at the end of the record
         * @throws IOException if the source can't be read or ends inside the
         *                     record
         */
        @Override
        public int read() throws IOException
        {
            byte[] one = new byte[1];
            int count = read(one, 0, 1);
            return count < 0 ? -1 : one[0] & 0xff;
        }

        /**
         * Reads up to {@code length} bytes, never past the end of the
         * current part.
         *
         * @param buffer where the bytes go
         * @param offset where in the buffer they start
         * @param length the most to read
         * @return how many bytes were read; -1 at the end of the record
         * @throws IOException if the source can't be read or ends inside the
         *                     record
         */
        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            if (length == 0)
            {
                return 0;
            }
            if (current == null)
            {
                return -1;
            }
            int count = (int) Math.min(length, current.length() - done);
            switch (current)
            {
                case Literal literal -> System.arraycopy(literal.bytes(),
                        (int) done, buffer, offset, count);
                case SourceRange range ->
                {
                    int read = source.read(range.offset() + done,
                            ByteBuffer.wrap(buffer, offset, count));
                    if (read < 0)
                    {
                        throw new IOException("Source ended inside a record");
                    }
                    count = read;
                }
            }
            done += count;
            if (done == current.length())
            {
                advance();
            }
            return count;
        }
    }
}
