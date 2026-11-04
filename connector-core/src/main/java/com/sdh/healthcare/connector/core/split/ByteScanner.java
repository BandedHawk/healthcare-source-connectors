/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Buffered forward reader over a {@link SeekableByteSource}, with a small
 * lookahead. Splitters use it to walk a file byte by byte while knowing the
 * exact source offset of every byte.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ByteScanner
{

    /**
     * Smallest buffer allowed; it bounds the lookahead ({@link #lookingAt},
     * {@link #peek(int)}).
     */
    public static final int MIN_BUFFER = 64;

    /** The file. */
    private final SeekableByteSource source;

    /** The bytes read ahead. */
    private final byte[] buffer;

    /** The buffer, for reading into. */
    private final ByteBuffer view;

    /** The source offset of the buffer's first byte. */
    private long bufferStart;

    /** The index in the buffer of the next byte. */
    private int cursor;

    /** The index in the buffer after the last byte read in. */
    private int limit;

    /** Whether the source has ended. */
    private boolean ended;

    /**
     * Creates a scanner.
     *
     * @param source     the file
     * @param start      the source offset to start at
     * @param bufferSize the buffer size; at least {@link #MIN_BUFFER} is
     *                   used
     */
    public ByteScanner(SeekableByteSource source, long start, int bufferSize)
    {
        this.source = source;
        this.buffer = new byte[Math.max(MIN_BUFFER, bufferSize)];
        this.view = ByteBuffer.wrap(buffer);
        this.bufferStart = start;
    }

    /**
     * Source offset of the next byte {@link #read()} returns.
     *
     * @return the offset
     */
    public long position()
    {
        return bufferStart + cursor;
    }

    /**
     * Makes up to {@code wanted} bytes available from the current position.
     *
     * @param wanted how many bytes
     * @return how many are available
     * @throws IOException if the source can't be read
     */
    private int fill(int wanted) throws IOException
    {
        if (limit - cursor >= wanted)
        {
            return wanted;
        }
        if (wanted > buffer.length)
        {
            throw new IllegalArgumentException("Lookahead larger than buffer");
        }
        if (cursor > 0)
        {
            System.arraycopy(buffer, cursor, buffer, 0, limit - cursor);
            bufferStart += cursor;
            limit -= cursor;
            cursor = 0;
        }
        while (limit < wanted && !ended)
        {
            view.limit(buffer.length).position(limit);
            int count = source.read(bufferStart + limit, view);
            if (count < 0)
            {
                ended = true;
            }
            else
            {
                limit += count;
            }
        }
        return Math.min(wanted, limit - cursor);
    }

    /**
     * Next byte without consuming it.
     *
     * @return the byte (0–255), or -1 at the end
     * @throws IOException if the source can't be read
     */
    public int peek() throws IOException
    {
        return fill(1) < 1 ? -1 : buffer[cursor] & 0xff;
    }

    /**
     * The byte {@code ahead} positions after the next one.
     *
     * @param ahead how far ahead; less than the buffer size
     * @return the byte (0–255), or -1 if the source ends first
     * @throws IOException if the source can't be read
     */
    public int peek(int ahead) throws IOException
    {
        return fill(ahead + 1) <= ahead ? -1 : buffer[cursor + ahead] & 0xff;
    }

    /**
     * Consumes and returns the next byte.
     *
     * @return the byte (0–255), or -1 at the end
     * @throws IOException if the source can't be read
     */
    public int read() throws IOException
    {
        if (cursor >= limit && fill(1) < 1)
        {
            return -1;
        }
        return buffer[cursor++] & 0xff;
    }

    /**
     * Tells whether the next bytes equal {@code bytes}. Consumes nothing.
     *
     * @param bytes the bytes to look for; no longer than the buffer
     * @return true if they come next
     * @throws IOException if the source can't be read
     */
    public boolean lookingAt(byte[] bytes) throws IOException
    {
        if (fill(bytes.length) < bytes.length)
        {
            return false;
        }
        for (int index = 0; index < bytes.length; index++)
        {
            if (buffer[cursor + index] != bytes[index])
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Skips {@code count} bytes; positions past the end are allowed and
     * simply read as the end.
     *
     * @param count how many bytes
     */
    public void skip(long count)
    {
        if (count <= limit - cursor)
        {
            cursor += (int) count;
        }
        else
        {
            bufferStart = position() + count;
            cursor = 0;
            limit = 0;
        }
    }

    /**
     * Advances to the next {@code target} without consuming it.
     *
     * @param target the byte to look for
     * @return true if found; false, positioned at the end, if none
     * @throws IOException if the source can't be read
     */
    public boolean skipTo(byte target) throws IOException
    {
        while (true)
        {
            for (int index = cursor; index < limit; index++)
            {
                if (buffer[index] == target)
                {
                    cursor = index;
                    return true;
                }
            }
            cursor = limit;
            if (fill(1) < 1)
            {
                return false;
            }
        }
    }

    /**
     * Advances to the next {@code first} or {@code second} without
     * consuming it.
     *
     * @param first  one byte to look for
     * @param second the other
     * @return the byte found (0–255), or -1 at the end
     * @throws IOException if the source can't be read
     */
    public int skipToAny(byte first, byte second) throws IOException
    {
        while (true)
        {
            for (int index = cursor; index < limit; index++)
            {
                byte value = buffer[index];
                if (value == first || value == second)
                {
                    cursor = index;
                    return value & 0xff;
                }
            }
            cursor = limit;
            if (fill(1) < 1)
            {
                return -1;
            }
        }
    }

    /**
     * Skips past the next occurrence of {@code terminator}.
     *
     * @param terminator the bytes to skip past; not empty
     * @return false if the source ends first
     * @throws IOException if the source can't be read
     */
    public boolean skipPast(byte[] terminator) throws IOException
    {
        while (skipTo(terminator[0]))
        {
            if (lookingAt(terminator))
            {
                skip(terminator.length);
                return true;
            }
            skip(1);
        }
        return false;
    }
}
