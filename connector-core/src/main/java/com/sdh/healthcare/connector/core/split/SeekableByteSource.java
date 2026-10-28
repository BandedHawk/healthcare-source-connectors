package com.sdh.healthcare.connector.core.split;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Random-access, read-only bytes of one source file. Implementations must allow concurrent positional reads. */
public interface SeekableByteSource extends Closeable {

    long size() throws IOException;

    /** Reads up to {@code dst.remaining()} bytes starting at {@code position}. Returns the count, or -1 at the end. */
    int read(long position, ByteBuffer dst) throws IOException;

    default byte[] readFully(long position, int length) throws IOException {
        byte[] out = new byte[length];
        ByteBuffer buf = ByteBuffer.wrap(out);
        while (buf.hasRemaining()) {
            if (read(position + buf.position(), buf) < 0) {
                throw new EOFException("Source ended before offset " + (position + length));
            }
        }
        return out;
    }

    static SeekableByteSource open(Path path) throws IOException {
        return new FileChannelSource(FileChannel.open(path, StandardOpenOption.READ));
    }

    static SeekableByteSource of(byte[] bytes) {
        return new ByteArraySource(bytes);
    }
}

final class FileChannelSource implements SeekableByteSource {
    private final FileChannel channel;

    FileChannelSource(FileChannel channel) {
        this.channel = channel;
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public int read(long position, ByteBuffer dst) throws IOException {
        return channel.read(dst, position);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}

final class ByteArraySource implements SeekableByteSource {
    private final byte[] bytes;

    ByteArraySource(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public long size() {
        return bytes.length;
    }

    @Override
    public int read(long position, ByteBuffer dst) {
        if (position >= bytes.length) {
            return -1;
        }
        int n = (int) Math.min(dst.remaining(), bytes.length - position);
        dst.put(bytes, (int) position, n);
        return n;
    }

    @Override
    public void close() {}
}
