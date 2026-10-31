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
 * <p>Text that is already UTF-8 (a converter's source bytes, once checked) can be written straight from those
 * bytes ({@link #utf8}), so a large field is never decoded into a string. For large sections, {@link #build} writes
 * the JSON twice: once only to count its bytes, then into an array of exactly that size, so the JSON is never held
 * twice (a growing buffer, then a trimmed copy).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class JsonWriter {
    /** The largest array the JVM reliably allocates. */
    private static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;
    /**
     * The size estimate from which {@link #build} counts first and writes once, rather than growing a buffer and
     * trimming it. Below it the extra pass costs more than the memory it saves.
     */
    static final int EXACT_SIZE_FROM = 1 << 20;
    private byte[] buf;
    private int len;
    /** True when the bytes are only counted: {@link #buf} is then a scratch buffer, reused from the start. */
    private final boolean counting;
    /** Bytes counted before the scratch buffer was last reused. */
    private long counted;
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
        counting = false;
    }

    /** A writer that keeps nothing and only counts the bytes it would write ({@link #size()}). */
    private JsonWriter() {
        buf = new byte[8192];
        counting = true;
    }

    /** Writes some JSON; called once, or twice by {@link #build}, and must write the same both times. */
    @FunctionalInterface
    interface Body {
        void write(JsonWriter w) throws ConversionException;
    }

    /**
     * Writes JSON and returns it in an array of exactly its size. Below {@link #EXACT_SIZE_FROM} it is written once
     * into a buffer of the estimated size; from there, {@code body} is run first on a writer that only counts, then
     * on one of exactly the counted size, so the JSON is held once. Any conversion error is thrown by the counting
     * pass, before anything is kept.
     *
     * @param estimate the expected size of the JSON in bytes (usually the section's size)
     * @param body     writes the JSON; it must write exactly the same each time it is run
     * @return the JSON, as UTF-8
     * @throws ConversionException as thrown by {@code body}
     */
    static byte[] build(int estimate, Body body) throws ConversionException {
        if (estimate < EXACT_SIZE_FROM) {
            JsonWriter w = new JsonWriter(estimate);
            body.write(w);
            return w.toBytes();
        }
        JsonWriter counter = new JsonWriter();
        body.write(counter);
        long size = counter.size();
        if (size > MAX_CAPACITY) {
            throw new OutOfMemoryError("JSON larger than the largest array");
        }
        JsonWriter w = new JsonWriter((int) size);
        body.write(w);
        if (w.len != size) {
            throw new IllegalStateException("JSON written twice came out different sizes");
        }
        return w.buf;
    }

    /** The number of bytes written so far. */
    long size() {
        return counted + len;
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
        closeString();
        comma = true;
        return this;
    }

    /** Starts a string value, whose text follows in calls to {@link #utf8}; {@link #endString()} closes it. */
    JsonWriter beginString() {
        separate();
        put('"');
        return this;
    }

    /**
     * Adds text to a string started by {@link #beginString()}, from bytes that are valid UTF-8 (the caller has
     * checked). They are copied as they are, escaping only what JSON requires, which gives exactly the bytes
     * {@link #value(String)} would write for the decoded text.
     */
    JsonWriter utf8(byte[] b, int from, int to) {
        int run = from;
        for (int i = from; i < to; i++) {
            int c = b[i] & 0xFF;
            if (c < 0x20 || c == '"' || c == '\\') {
                bytes(b, run, i - run);
                escape((char) c);
                run = i + 1;
            }
        }
        bytes(b, run, to - run);
        return this;
    }

    /** Closes a string started by {@link #beginString()}. */
    JsonWriter endString() {
        closeString();
        comma = true;
        return this;
    }

    /** A string value from valid UTF-8 bytes: {@link #beginString()}, {@link #utf8}, {@link #endString()}. */
    JsonWriter valueUtf8(byte[] b, int from, int to) {
        return beginString().utf8(b, from, to).endString();
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
        closeString();
    }

    /** Closes a string, writing a high surrogate left unpaired at its end as {@code ?}. */
    private void closeString() {
        if (pendingHigh != 0) {
            pendingHigh = 0;
            put('?');
        }
        put('"');
    }

    /** Writes the escaped UTF-8 of part of a string, carrying a trailing high surrogate over to the next part. */
    private void chars(String s) {
        int n = s.length();
        reserve(n + 1);
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
            if (c < 0x20 || c == '"' || c == '\\') {
                escape(c);
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

    /** Writes the escape for a character JSON doesn't allow in a string as it is: a quote, backslash or control. */
    private void escape(char c) {
        switch (c) {
            case '"' -> ascii("\\\"");
            case '\\' -> ascii("\\\\");
            case '\n' -> ascii("\\n");
            case '\r' -> ascii("\\r");
            case '\t' -> ascii("\\t");
            case '\b' -> ascii("\\b");
            case '\f' -> ascii("\\f");
            default -> ascii(String.format("\\u%04x", (int) c));
        }
    }

    /** Copies bytes that need no escaping. */
    private void bytes(byte[] b, int from, int n) {
        if (counting) {
            counted += n;
            return;
        }
        ensure(n);
        System.arraycopy(b, from, buf, len, n);
        len += n;
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

    /** Makes room ahead of time for about {@code n} bytes; a hint only, ignored when counting. */
    private void reserve(int n) {
        if (!counting) {
            ensure(n);
        }
    }

    /**
     * Makes room for {@code n} more bytes, growing the buffer by half (or more, if needed). When only counting,
     * the scratch buffer's bytes are counted and it is reused from the start instead.
     */
    private void ensure(int n) {
        if (counting && n > buf.length - len) {
            counted += len;
            len = 0;
            if (n > buf.length) {
                buf = new byte[n];
            }
            return;
        }
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
