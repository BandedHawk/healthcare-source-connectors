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
 * Writes JSON, one piece at a time, for the record converters. It is small and
 * does only what they need, and it always writes the same bytes for the same
 * calls.
 * <p>
 * The JSON it writes is compact: no spaces or line breaks. Keys come in the
 * order they are written. Strings are escaped as the JSON standard (RFC 8259)
 * requires, and only that: a quote, a backslash and the control characters.
 * Every other character, accented letters and emoji included, is written as
 * itself, in UTF-8.
 * <p>
 * The caller decides the structure, by calling {@link #beginObject()},
 * {@link #key}, {@link #value(String)} and so on in the right order. The writer
 * puts in the commas between items, but doesn't check the structure: a key
 * outside an object, or an object that is never closed, gives invalid JSON.
 * <p>
 * Why it is written this way:
 * <ul>
 * <li><b>Memory.</b> It writes UTF-8 bytes straight into one array, so a
 * record's JSON is held once while it is written, not first as characters and
 * then again as bytes. Text that is already UTF-8, such as a field of a CSV
 * record, can be copied from the record's own bytes ({@link #utf8}), so even a
 * large field is never decoded into a Java string. And for a large record,
 * {@link #build} writes the JSON twice, the first time only to count its size,
 * so the result can go straight into an array of exactly that size instead of a
 * growing buffer that is copied when trimmed.</li>
 * <li><b>Exact bytes.</b> The bytes are exactly what
 * {@code String.getBytes(UTF_8)} would give for the same text, including for a
 * broken character (half of a surrogate pair, written as {@code ?}). A stored
 * record's ID is a hash of its bytes, so the same record must always give the
 * same bytes.</li>
 * </ul>
 * A writer is used by one thread for one record's JSON, and then discarded.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 * @see RecordConverter
 */
final class JsonWriter
{

    /**
     * The largest array the JVM can reliably create: a few bytes under the
     * largest {@code int}, as some JVMs keep header words in the array. JSON
     * that would be larger fails with an {@link OutOfMemoryError}.
     */
    private static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;
    /**
     * The estimated size, in bytes, from which {@link #build} writes the JSON
     * twice, first only counting its size, so it can be held once at exactly
     * that size. Smaller JSON is written once into a buffer that is trimmed
     * afterwards: the second pass would cost more time than the memory it
     * saves. 1 MiB.
     */
    static final int EXACT_SIZE_FROM = 1 << 20;
    /**
     * Where the JSON is written. It grows when it fills up. When only counting,
     * it is a small scratch area that is reused from the start whenever it
     * fills up.
     */
    private byte[] buffer;
    /**
     * How many bytes of {@link #buffer} hold JSON written so far.
     */
    private int length;
    /**
     * Whether this writer only counts the bytes it would write, to find the
     * JSON's size, and keeps none of them. {@link #buffer} is then a scratch
     * area, reused from the start.
     */
    private final boolean counting;
    /**
     * When only counting: the bytes counted before the scratch area was last
     * reused. The total so far is this plus {@link #length}
     * ({@link #size()}). Always {@code 0} for a writer that keeps the JSON.
     */
    private long counted;
    /**
     * Whether the next key or value needs a comma before it: {@code true} after
     * a value or a closed object or array, {@code false} at the start of an
     * object or array and right after a key.
     */
    private boolean comma;
    /**
     * The first half of a surrogate pair (a character such as an emoji, which
     * Java holds as two {@code char}s), when it was the last character of one
     * piece of a string and its second half may start the next piece; {@code 0}
     * if there is none. See {@link #value(List)}.
     */
    private char pendingHigh;

    /**
     * Creates a writer that keeps the JSON it writes.
     *
     * @param capacity the expected size of the JSON in bytes; the buffer grows
     * if it turns out bigger, so a close estimate only saves copying. At least
     * 16 bytes are set aside.
     */
    JsonWriter(int capacity)
    {
        buffer = new byte[Math.max(16, capacity)];
        counting = false;
    }

    /**
     * Creates a writer that keeps nothing and only counts the bytes it would
     * write, for the first pass of {@link #build}. The count is
     * {@link #size()}. It needs only an 8 KiB scratch area, whatever the JSON's
     * size.
     */
    private JsonWriter()
    {
        buffer = new byte[8192];
        counting = true;
    }

    /**
     * The code that writes a record's JSON, given to {@link #build}.
     * <p>
     * It may be run twice, once on a writer that only counts and once on one
     * that keeps the JSON, so it must write exactly the same both times: it
     * must start from the beginning of the record each time, and keep nothing
     * from the first run.
     */
    @FunctionalInterface
    interface Body
    {

        /**
         * Writes the JSON.
         *
         * @param w the writer to write it to
         * @throws ConversionException if the record can't be converted
         */
        void write(JsonWriter w) throws ConversionException;
    }

    /**
     * Writes a record's JSON and returns it in an array of exactly its size.
     * This is how converters use the writer.
     * <p>
     * How it does so depends on the estimate:
     * <ul>
     * <li><b>Under {@link #EXACT_SIZE_FROM} (1 MiB):</b> {@code body} is run
     * once, into a buffer of the estimated size, which is then trimmed to the
     * JSON's size if needed.</li>
     * <li><b>From there:</b> {@code body} is run first on a writer that only
     * counts, then again on one of exactly the counted size. The JSON is then
     * held once, with no growing and no trimmed copy. If the record can't be
     * converted, the first run finds it before anything is kept.</li>
     * </ul>
     *
     * @param estimate the expected size of the JSON in bytes, usually about the
     * record's size
     * @param body the code that writes the JSON; it must write exactly the same
     * each time it is run
     * @return the JSON, as UTF-8
     * @throws ConversionException as thrown by {@code body}
     * @throws OutOfMemoryError if the JSON is larger than the largest array
     * Java can create (about 2 GiB)
     * @throws IllegalStateException if {@code body} wrote a different amount
     * the second time, which is a bug in the converter
     */
    static byte[] build(int estimate, Body body) throws ConversionException
    {
        if (estimate < EXACT_SIZE_FROM)
        {
            JsonWriter writer = new JsonWriter(estimate);
            body.write(writer);
            return writer.toBytes();
        }
        // First pass: only count the JSON's size.
        JsonWriter counter = new JsonWriter();
        body.write(counter);
        long size = counter.size();
        if (size > MAX_CAPACITY)
        {
            throw new OutOfMemoryError("JSON larger than the largest array");
        }
        // Second pass: write it into an array of exactly that size.
        JsonWriter writer = new JsonWriter((int) size);
        body.write(writer);
        if (writer.length != size)
        {
            throw new IllegalStateException(
                    "JSON written twice came out different sizes");
        }
        return writer.buffer;
    }

    /**
     * How many bytes of JSON have been written so far, or would have been by a
     * writer that only counts.
     *
     * @return the number of bytes
     */
    long size()
    {
        return counted + length;
    }

    /**
     * Starts a JSON object: writes an opening brace, after a comma if one is
     * needed. Its keys and values follow, and {@link #endObject()} closes it.
     *
     * @return this writer, so calls can be chained
     */
    JsonWriter beginObject()
    {
        separate();
        put('{');
        comma = false;
        return this;
    }

    /**
     * Closes the JSON object started last: writes a closing brace.
     *
     * @return this writer, so calls can be chained
     */
    JsonWriter endObject()
    {
        put('}');
        comma = true;
        return this;
    }

    /**
     * Starts a JSON array: writes {@code [}, after a comma if one is needed.
     * Its values follow, and {@link #endArray()} closes it.
     *
     * @return this writer, so calls can be chained
     */
    JsonWriter beginArray()
    {
        separate();
        put('[');
        comma = false;
        return this;
    }

    /**
     * Closes the JSON array started last: writes {@code ]}.
     *
     * @return this writer, so calls can be chained
     */
    JsonWriter endArray()
    {
        put(']');
        comma = true;
        return this;
    }

    /**
     * Writes a key in an object, as a JSON string followed by a colon, after a
     * comma if one is needed. The next call writes the key's value.
     *
     * @param key the key's text; it is escaped as JSON requires
     * @return this writer, so calls can be chained
     */
    JsonWriter key(String key)
    {
        separate();
        string(key);
        put(':');
        comma = false;
        return this;
    }

    /**
     * Writes a JSON string value, after a comma if one is needed.
     *
     * @param value the string's text; it is escaped as JSON requires
     * @return this writer, so calls can be chained
     */
    JsonWriter value(String value)
    {
        separate();
        string(value);
        comma = true;
        return this;
    }

    /**
     * Writes several pieces of text as one JSON string value, without joining
     * them into one Java string first. The C-CDA converter uses it for long
     * text, which the XML parser hands over in pieces.
     * <p>
     * The bytes are exactly those {@link #value(String)} would write for the
     * pieces joined. That includes a character such as an emoji whose two
     * halves (a surrogate pair) end up in different pieces: the first half is
     * held back until the next piece shows whether its second half follows.
     *
     * @param pieces the string's text, in order; each is escaped as JSON
     * requires
     * @return this writer, so calls can be chained
     */
    JsonWriter value(List<String> pieces)
    {
        separate();
        put('"');
        for (String piece : pieces)
        {
            chars(piece);
        }
        closeString();
        comma = true;
        return this;
    }

    /**
     * Starts a string value whose text is copied from UTF-8 bytes: writes the
     * opening quote, after a comma if one is needed. The text follows in calls
     * to {@link #utf8}, and {@link #endString()} closes it.
     *
     * @return this writer, so calls can be chained
     */
    JsonWriter beginString()
    {
        separate();
        put('"');
        return this;
    }

    /**
     * Adds text to a string started by {@link #beginString()}, copied from
     * bytes that are already UTF-8.
     * <p>
     * The bytes are copied as they are, except for a quote, a backslash or a
     * control character, which are escaped as JSON requires. This gives exactly
     * the bytes {@link #value(String)} would write for the same text decoded,
     * without decoding it. The caller must have checked that the bytes are
     * valid UTF-8; nothing here checks it again.
     *
     * @param bytes the bytes to copy from, such as a whole record
     * @param from  where the text starts in {@code bytes}
     * @param to    where it ends: the position just after its last byte
     * @return this writer, so calls can be chained
     */
    JsonWriter utf8(byte[] bytes, int from, int to)
    {
        // Where the bytes not yet copied start.
        int run = from;
        for (int index = from; index < to; index++)
        {
            // The byte, as a number from 0 to 255.
            int character = bytes[index] & 0xFF;
            if (character < 0x20 || character == '"' || character == '\\')
            {
                bytes(bytes, run, index - run);
                escape((char) character);
                run = index + 1;
            }
        }
        bytes(bytes, run, to - run);
        return this;
    }

    /**
     * Closes a string started by {@link #beginString()}: writes the closing
     * quote.
     *
     * @return this writer, so calls can be chained
     */
    JsonWriter endString()
    {
        closeString();
        comma = true;
        return this;
    }

    /**
     * Writes a whole string value copied from UTF-8 bytes: the same as
     * {@link #beginString()}, {@link #utf8} and {@link #endString()} in turn.
     *
     * @param bytes the bytes to copy from; they must be valid UTF-8
     * @param from  where the text starts in {@code bytes}
     * @param to    where it ends: the position just after its last byte
     * @return this writer, so calls can be chained
     */
    JsonWriter valueUtf8(byte[] bytes, int from, int to)
    {
        return beginString().utf8(bytes, from, to).endString();
    }

    /**
     * Writes a value exactly as given, without quotes: {@code true},
     * {@code false} or a number, after a comma if one is needed.
     *
     * @param value the value; the caller must make sure it is valid JSON and
     * only ASCII, as nothing here checks it
     * @return this writer, so calls can be chained
     */
    JsonWriter literal(String value)
    {
        separate();
        ascii(value);
        comma = true;
        return this;
    }

    /**
     * Writes a comma if the next key or value needs one ({@link #comma}).
     */
    private void separate()
    {
        if (comma)
        {
            put(',');
        }
    }

    /**
     * Writes a complete JSON string: quotes around the text, escaped as JSON
     * requires.
     *
     * @param text the text
     */
    private void string(String text)
    {
        put('"');
        chars(text);
        closeString();
    }

    /**
     * Writes a string's closing quote. If the text ended with the first half of
     * a surrogate pair, and no second half followed, that half is written as
     * {@code ?} first, as {@code String.getBytes(UTF_8)} would write it.
     */
    private void closeString()
    {
        if (pendingHigh != 0)
        {
            pendingHigh = 0;
            put('?');
        }
        put('"');
    }

    /**
     * Writes one piece of a string's text as UTF-8, escaping what JSON
     * requires.
     * <p>
     * Characters are written as {@code String.getBytes(UTF_8)} writes them: one
     * byte for ASCII, two or three for other characters, and four for a
     * character outside the basic range (an emoji, say), which Java holds as a
     * surrogate pair. Half a pair, with no other half next to it, is written as
     * {@code ?}. A first half at the very end of the piece is held in
     * {@link #pendingHigh}, because its second half may start the next piece.
     *
     * @param string the piece of text
     */
    private void chars(String string)
    {
        // The piece's length, in Java chars.
        int size = string.length();
        reserve(size + 1);
        // The position of the next char to write.
        int position = 0;
        if (pendingHigh != 0 && size > 0)
        {
            // Finish the surrogate pair the last piece ended with, if this
            // piece starts with its second half.
            char high = pendingHigh;
            pendingHigh = 0;
            if (Character.isLowSurrogate(string.charAt(0)))
            {
                codePoint(Character.toCodePoint(high, string.charAt(0)));
                position = 1;
            }
            else
            {
                put('?');
            }
        }
        for (; position < size; position++)
        {
            char character = string.charAt(position);
            if (character < 0x20 || character == '"' || character == '\\')
            {
                escape(character);
            }
            else if (character < 0x80)
            {
                put(character);
            }
            else if (character < 0x800)
            {
                ensure(2);
                buffer[this.length++] = (byte) (0xC0 | character >> 6);
                buffer[this.length++] = (byte) (0x80 | character & 0x3F);
            }
            else if (Character.isHighSurrogate(character)
                    && position + 1 == size)
            {
                // Its second half may start the next piece.
                pendingHigh = character;
            }
            else if (Character.isHighSurrogate(character) && Character.
                    isLowSurrogate(string.charAt(position + 1)))
            {
                codePoint(Character.toCodePoint(character, string.charAt(
                        ++position)));
            }
            else if (Character.isSurrogate(character))
            {
                // Half a pair on its own, written as String.getBytes(UTF_8)
                // writes it.
                put('?');
            }
            else
            {
                ensure(3);
                buffer[this.length++] = (byte) (0xE0 | character >> 12);
                buffer[this.length++] = (byte) (0x80 | character >> 6 & 0x3F);
                buffer[this.length++] = (byte) (0x80 | character & 0x3F);
            }
        }
    }

    /**
     * Writes the escape for a character JSON doesn't allow as itself inside a
     * string: {@code \"} for a quote, {@code \\} for a backslash, the short
     * forms {@code \n}, {@code \r}, {@code \t}, {@code \b} and {@code \f}, and
     * a backslash, the letter {@code u} and four hex digits for any other
     * control character.
     *
     * @param character the quote, backslash or control character
     */
    private void escape(char character)
    {
        switch (character)
        {
            case '"' ->
                ascii("\\\"");
            case '\\' ->
                ascii("\\\\");
            case '\n' ->
                ascii("\\n");
            case '\r' ->
                ascii("\\r");
            case '\t' ->
                ascii("\\t");
            case '\b' ->
                ascii("\\b");
            case '\f' ->
                ascii("\\f");
            default ->
                ascii(String.format("\\u%04x", (int) character));
        }
    }

    /**
     * Copies bytes that need no escaping. When only counting, they are counted
     * and not copied.
     *
     * @param bytes the bytes to copy from
     * @param from  where to start copying
     * @param size  how many bytes to copy
     */
    private void bytes(byte[] bytes, int from, int size)
    {
        if (counting)
        {
            counted += size;
            return;
        }
        ensure(size);
        System.arraycopy(bytes, from, buffer, length, size);
        length += size;
    }

    /**
     * Writes a character outside the basic range (above U+FFFF, such as an
     * emoji) as its four UTF-8 bytes.
     *
     * @param cp the character's Unicode number (its code point)
     */
    private void codePoint(int cp)
    {
        ensure(4);
        buffer[length++] = (byte) (0xF0 | cp >> 18);
        buffer[length++] = (byte) (0x80 | cp >> 12 & 0x3F);
        buffer[length++] = (byte) (0x80 | cp >> 6 & 0x3F);
        buffer[length++] = (byte) (0x80 | cp & 0x3F);
    }

    /**
     * Writes text the caller knows is ASCII, one byte per character, without
     * escaping: JSON punctuation, escapes and literals.
     *
     * @param text the text
     */
    private void ascii(String text)
    {
        ensure(text.length());
        for (int index = 0; index < text.length(); index++)
        {
            buffer[length++] = (byte) text.charAt(index);
        }
    }

    /**
     * Writes one ASCII character as one byte.
     *
     * @param character the character, below 128
     */
    private void put(char character)
    {
        ensure(1);
        buffer[length++] = (byte) character;
    }

    /**
     * Makes room ahead of time for about {@code size} more bytes, so a long
     * string doesn't grow the buffer several times. It is only a hint: each
     * write still makes sure of its own room. Ignored when only counting.
     *
     * @param size about how many bytes are coming
     */
    private void reserve(int size)
    {
        if (!counting)
        {
            ensure(size);
        }
    }

    /**
     * Makes sure there is room in {@link #buffer} for {@code size} more bytes.
     * <p>
     * A writer that keeps the JSON grows the buffer by half, or by more if that
     * isn't enough. A writer that only counts adds what the scratch area holds
     * to {@link #counted} and starts again from its beginning, making it bigger
     * only if {@code size} bytes wouldn't fit even in an empty one.
     *
     * @param size how many bytes are about to be written
     * @throws OutOfMemoryError if the JSON would be larger than the largest
     * array Java can create
     */
    private void ensure(int size)
    {
        if (counting && size > buffer.length - length)
        {
            counted += length;
            length = 0;
            if (size > buffer.length)
            {
                buffer = new byte[size];
            }
            return;
        }
        if (size > buffer.length - length)
        {
            // The size the buffer must at least be.
            long needed = (long) length + size;
            if (needed > MAX_CAPACITY)
            {
                throw new OutOfMemoryError(
                        "JSON larger than the largest array");
            }
            buffer = Arrays.copyOf(buffer, (int) Math.min(MAX_CAPACITY, Math.
                    max(needed, buffer.length + (long) (buffer.length >> 1))));
        }
    }

    /**
     * The JSON written, as UTF-8 bytes. Only for a writer that keeps the JSON.
     *
     * @return the buffer itself if it is exactly full, otherwise a copy of just
     * the bytes written
     */
    byte[] toBytes()
    {
        return length == buffer.length ? buffer : Arrays.copyOf(buffer, length);
    }
}
