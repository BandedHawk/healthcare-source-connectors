package com.sdh.healthcare.connector.core.split;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Buffered forward reader over a {@link SeekableByteSource}, with a small lookahead. Splitters use it to
 * walk a file byte by byte while knowing the exact source offset of every byte.
 */
public final class ByteScanner {
    /** Smallest buffer allowed; it bounds the lookahead ({@link #lookingAt}, {@link #peek(int)}). */
    public static final int MIN_BUFFER = 64;

    private final SeekableByteSource src;
    private final byte[] buf;
    private final ByteBuffer view;
    private long bufStart;
    private int pos;
    private int lim;
    private boolean eof;

    public ByteScanner(SeekableByteSource src, long start, int bufferSize) {
        this.src = src;
        this.buf = new byte[Math.max(MIN_BUFFER, bufferSize)];
        this.view = ByteBuffer.wrap(buf);
        this.bufStart = start;
    }

    /** Source offset of the next byte {@link #read()} returns. */
    public long position() {
        return bufStart + pos;
    }

    /** Makes up to {@code n} bytes available from the current position; returns how many are. */
    private int fill(int n) throws IOException {
        if (lim - pos >= n) {
            return n;
        }
        if (n > buf.length) {
            throw new IllegalArgumentException("Lookahead larger than buffer");
        }
        if (pos > 0) {
            System.arraycopy(buf, pos, buf, 0, lim - pos);
            bufStart += pos;
            lim -= pos;
            pos = 0;
        }
        while (lim < n && !eof) {
            view.limit(buf.length).position(lim);
            int r = src.read(bufStart + lim, view);
            if (r < 0) {
                eof = true;
            } else {
                lim += r;
            }
        }
        return Math.min(n, lim - pos);
    }

    /** Next byte (0–255) without consuming it, or -1 at the end. */
    public int peek() throws IOException {
        return fill(1) < 1 ? -1 : buf[pos] & 0xff;
    }

    /** The byte {@code ahead} positions after the next one, or -1 if the source ends first. */
    public int peek(int ahead) throws IOException {
        return fill(ahead + 1) <= ahead ? -1 : buf[pos + ahead] & 0xff;
    }

    /** Consumes and returns the next byte (0–255), or -1 at the end. */
    public int read() throws IOException {
        if (pos >= lim && fill(1) < 1) {
            return -1;
        }
        return buf[pos++] & 0xff;
    }

    /** True if the next bytes equal {@code s}. Consumes nothing. */
    public boolean lookingAt(byte[] s) throws IOException {
        if (fill(s.length) < s.length) {
            return false;
        }
        for (int i = 0; i < s.length; i++) {
            if (buf[pos + i] != s[i]) {
                return false;
            }
        }
        return true;
    }

    /** Skips {@code n} bytes; positions past the end are allowed and simply read as the end. */
    public void skip(long n) {
        if (n <= lim - pos) {
            pos += (int) n;
        } else {
            bufStart = position() + n;
            pos = 0;
            lim = 0;
        }
    }

    /** Advances to the next {@code b} without consuming it. Returns false (positioned at the end) if none. */
    public boolean skipTo(byte b) throws IOException {
        while (true) {
            for (int i = pos; i < lim; i++) {
                if (buf[i] == b) {
                    pos = i;
                    return true;
                }
            }
            pos = lim;
            if (fill(1) < 1) {
                return false;
            }
        }
    }

    /** Advances to the next {@code a} or {@code b} without consuming it. Returns that byte, or -1 at the end. */
    public int skipToAny(byte a, byte b) throws IOException {
        while (true) {
            for (int i = pos; i < lim; i++) {
                byte c = buf[i];
                if (c == a || c == b) {
                    pos = i;
                    return c & 0xff;
                }
            }
            pos = lim;
            if (fill(1) < 1) {
                return -1;
            }
        }
    }

    /** Skips past the next occurrence of {@code terminator}. Returns false if the source ends first. */
    public boolean skipPast(byte[] terminator) throws IOException {
        while (skipTo(terminator[0])) {
            if (lookingAt(terminator)) {
                skip(terminator.length);
                return true;
            }
            skip(1);
        }
        return false;
    }
}
