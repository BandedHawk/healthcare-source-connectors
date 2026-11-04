/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Random-access, read-only bytes of one source file. Implementations must
 * allow concurrent positional reads.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface SeekableByteSource extends Closeable
{

    /**
     * The file's size.
     *
     * @return the size, in bytes
     * @throws IOException if it can't be read
     */
    long size() throws IOException;

    /**
     * Reads up to {@code target.remaining()} bytes starting at
     * {@code position}.
     *
     * @param position the source offset
     * @param target   where the bytes go
     * @return the count, or -1 at the end
     * @throws IOException if the file can't be read
     */
    int read(long position, ByteBuffer target) throws IOException;

    /**
     * Reads exactly {@code length} bytes starting at {@code position}.
     *
     * @param position the source offset
     * @param length   how many bytes
     * @return the bytes
     * @throws EOFException if the file ends first
     * @throws IOException  if the file can't be read
     */
    default byte[] readFully(long position, int length) throws IOException
    {
        byte[] out = new byte[length];
        ByteBuffer target = ByteBuffer.wrap(out);
        while (target.hasRemaining())
        {
            if (read(position + target.position(), target) < 0)
            {
                throw new EOFException("Source ended before offset "
                        + (position + length));
            }
        }
        return out;
    }

    /**
     * Opens a local file.
     *
     * @param path the file
     * @return its bytes
     * @throws IOException if it can't be opened
     */
    static SeekableByteSource open(Path path) throws IOException
    {
        return new FileChannelSource(
                FileChannel.open(path, StandardOpenOption.READ));
    }

    /**
     * Wraps bytes in memory.
     *
     * @param bytes the bytes; not copied
     * @return a source over them
     */
    static SeekableByteSource of(byte[] bytes)
    {
        return new ByteArraySource(bytes);
    }
}

/**
 * A local file's bytes.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class FileChannelSource implements SeekableByteSource
{

    /** The open file. */
    private final FileChannel channel;

    /**
     * Wraps an open file.
     *
     * @param channel the file; closed with this source
     */
    FileChannelSource(FileChannel channel)
    {
        this.channel = channel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size() throws IOException
    {
        return channel.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(long position, ByteBuffer target) throws IOException
    {
        return channel.read(target, position);
    }

    /**
     * Closes the file.
     *
     * @throws IOException if it can't be closed
     */
    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}

/**
 * Bytes in memory.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ByteArraySource implements SeekableByteSource
{

    /** The bytes. */
    private final byte[] bytes;

    /**
     * Wraps bytes.
     *
     * @param bytes the bytes; not copied
     */
    ByteArraySource(byte[] bytes)
    {
        this.bytes = bytes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size()
    {
        return bytes.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(long position, ByteBuffer target)
    {
        if (position >= bytes.length)
        {
            return -1;
        }
        int count = (int) Math.min(target.remaining(),
                bytes.length - position);
        target.put(bytes, (int) position, count);
        return count;
    }

    /**
     * Does nothing.
     */
    @Override
    public void close()
    {
    }
}
