/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.convert;

import java.util.Arrays;
import java.util.List;

/**
 * Minimal deterministic JSON writer for the converters: compact (no whitespace), keys in the order written,
 * strings escaped per RFC 8259 with non-ASCII left as UTF-8. The caller keeps the structure valid.
 *
 * <p>It writes UTF-8 bytes straight into one buffer, so a section's JSON is held once while it is written,
 * not as characters and then again as bytes. The bytes are exactly those of
 * {@code String.getBytes(UTF_8)} on the same text, an unpaired surrogate included (written as {@code ?}),
 * which matters because a stored section's ID is a hash of these bytes.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class JsonWriter {
    /** The largest array the JVM reliably allocates. */
    private static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;
    private byte[] buf;
    private int len;
    /** Whether the next value or key in the current container needs a comma before it. */
    private boolean comma;
    /**
     * A high surrogate that ended the last piece of a string, waiting to see whether the next piece starts with
     * its low surrogate; {@code 0} if none.
     */
    private char pendingHigh;

    /**
     * @param capacity the expected size of the JSON in bytes; the buffer grows if it is exceeded, so a close
     *                 estimate only saves copying
     */
    JsonWriter(int capacity) {
        buf = new byte[Math.max(16, capacity)];
    }

    JsonWriter beginObject() {
        separate();
        put('{');
        comma = false;
        return this;
    }

    JsonWriter endObject() {
        put('}');
        comma = true;
        return this;
    }

    JsonWriter beginArray() {
        separate();
        put('[');
        comma = false;
        return this;
    }

    JsonWriter endArray() {
        put(']');
        comma = true;
        return this;
    }

    /** Writes a key; the next call writes its value. */
    JsonWriter key(String k) {
        separate();
        string(k);
        put(':');
        comma = false;
        return this;
    }

    JsonWriter value(String v) {
        separate();
        string(v);
        comma = true;
        return this;
    }

    /**
     * Writes pieces of text as one JSON string, exactly as {@link #value(String)} would write them joined, a
     * surrogate pair split between two pieces included, without joining them into one string first.
     */
    JsonWriter value(List<String> pieces) {
        separate();
        put('"');
        for (String piece : pieces) {
            chars(piece);
        }
        endString();
        comma = true;
        return this;
    }

    /** A literal JSON value (true, false or a number); the caller guarantees it is valid JSON. */
    JsonWriter literal(String v) {
        separate();
        ascii(v);
        comma = true;
        return this;
    }

    private void separate() {
        if (comma) {
            put(',');
        }
    }

    private void string(String s) {
        put('"');
        chars(s);
        endString();
    }

    /** Closes a string, writing a high surrogate left unpaired at its end as {@code ?}. */
    private void endString() {
        if (pendingHigh != 0) {
            pendingHigh = 0;
            put('?');
        }
        put('"');
    }

    /** Writes the escaped UTF-8 of part of a string, carrying a trailing high surrogate over to the next part. */
    private void chars(String s) {
        int n = s.length();
        ensure(n + 1);
        int i = 0;
        if (pendingHigh != 0 && n > 0) {
            char high = pendingHigh;
            pendingHigh = 0;
            if (Character.isLowSurrogate(s.charAt(0))) {
                codePoint(Character.toCodePoint(high, s.charAt(0)));
                i = 1;
            } else {
                put('?');
            }
        }
        for (; i < n; i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> ascii("\\\"");
                case '\\' -> ascii("\\\\");
                case '\n' -> ascii("\\n");
                case '\r' -> ascii("\\r");
                case '\t' -> ascii("\\t");
                case '\b' -> ascii("\\b");
                case '\f' -> ascii("\\f");
                default -> {
                    if (c < 0x20) {
                        ascii(String.format("\\u%04x", (int) c));
                    } else if (c < 0x80) {
                        put(c);
                    } else if (c < 0x800) {
                        ensure(2);
                        buf[len++] = (byte) (0xC0 | c >> 6);
                        buf[len++] = (byte) (0x80 | c & 0x3F);
                    } else if (Character.isHighSurrogate(c) && i + 1 == n) {
                        pendingHigh = c; // its low surrogate may start the next piece
                    } else if (Character.isHighSurrogate(c) && Character.isLowSurrogate(s.charAt(i + 1))) {
                        codePoint(Character.toCodePoint(c, s.charAt(++i)));
                    } else if (Character.isSurrogate(c)) {
                        put('?'); // as String.getBytes(UTF_8) writes an unpaired surrogate
                    } else {
                        ensure(3);
                        buf[len++] = (byte) (0xE0 | c >> 12);
                        buf[len++] = (byte) (0x80 | c >> 6 & 0x3F);
                        buf[len++] = (byte) (0x80 | c & 0x3F);
                    }
                }
            }
        }
    }

    /** Writes a supplementary code point (above U+FFFF) as four UTF-8 bytes. */
    private void codePoint(int cp) {
        ensure(4);
        buf[len++] = (byte) (0xF0 | cp >> 18);
        buf[len++] = (byte) (0x80 | cp >> 12 & 0x3F);
        buf[len++] = (byte) (0x80 | cp >> 6 & 0x3F);
        buf[len++] = (byte) (0x80 | cp & 0x3F);
    }

    /** Text the caller knows is ASCII: JSON punctuation, escapes and literals. */
    private void ascii(String s) {
        ensure(s.length());
        for (int i = 0; i < s.length(); i++) {
            buf[len++] = (byte) s.charAt(i);
        }
    }

    private void put(char c) {
        ensure(1);
        buf[len++] = (byte) c;
    }

    /** Makes room for {@code n} more bytes, growing the buffer by half (or more, if needed). */
    private void ensure(int n) {
        if (n > buf.length - len) {
            long needed = (long) len + n;
            if (needed > MAX_CAPACITY) {
                throw new OutOfMemoryError("JSON larger than the largest array");
            }
            buf = Arrays.copyOf(buf, (int) Math.min(MAX_CAPACITY, Math.max(needed, buf.length + (long) (buf.length >> 1))));
        }
    }

    /** The JSON written, as UTF-8; the buffer itself when it is exactly full, otherwise a copy of what was written. */
    byte[] toBytes() {
        return len == buf.length ? buf : Arrays.copyOf(buf, len);
    }
}
