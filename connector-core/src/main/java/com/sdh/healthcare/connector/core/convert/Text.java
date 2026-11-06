/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

/**
 * Strict text decoding for the converters. Java's usual decoding replaces a
 * byte that isn't valid in the character set with a replacement character
 * ({@code U+FFFD}), so the text quietly changes. Here an invalid byte fails
 * the record instead, with {@code CONVERT_INVALID_ENCODING} and the byte's
 * position, so the record is stored as split and nothing is lost.
 * <p>
 * A position is always an index into the array given, not a count from the
 * start of the range decoded, so a converter that decodes part of a record
 * gets the byte's position in the record.
 * <p>
 * The class only has static methods and isn't meant to be created.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class Text
{

    /** Not used: the class only has static methods. */
    private Text()
    {
    }

    /**
     * Decodes a range of bytes into text, failing on any byte that isn't valid
     * in the character set.
     *
     * @param bytes   the bytes, such as a record
     * @param from    where the range starts
     * @param length  how many bytes it has
     * @param charset the character set the bytes are in
     * @return the decoded text
     * @throws ConversionException {@code CONVERT_INVALID_ENCODING} if a byte
     *                             isn't valid in the character set, or stands
     *                             for a character Java's text can't hold; at
     *                             the position in {@code bytes} of the first
     *                             such byte
     */
    static String decode(byte[] bytes, int from, int length, Charset charset)
            throws ConversionException
    {
        CharsetDecoder decoder = charset.newDecoder().onMalformedInput(
                CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        // Wrapping keeps the array's indexes, so the buffer's position is the
        // position in the array.
        ByteBuffer input = ByteBuffer.wrap(bytes, from, length);
        // Room for the most characters the bytes can decode into, plus one to
        // spare, so the text is decoded in one go.
        CharBuffer output = CharBuffer.allocate((int) Math.ceil(length
                * (double) decoder.maxCharsPerByte()) + 1);
        CoderResult result = decoder.decode(input, output, true);
        if (result.isError())
        {
            throw new ConversionException(ErrorCode.CONVERT_INVALID_ENCODING,
                    input.position());
        }
        // Some character sets hold back the last characters until flushed.
        result = decoder.flush(output);
        if (result.isError())
        {
            throw new ConversionException(ErrorCode.CONVERT_INVALID_ENCODING,
                    input.position());
        }
        return output.flip().toString();
    }

    /**
     * Checks that a range of bytes is valid in the character set, the same way
     * {@link #decode} does and with the same error position, without keeping
     * the text. The text is decoded a few thousand characters at a time and
     * thrown away, so checking a large record needs little memory.
     *
     * @param bytes   the bytes, such as a record
     * @param from    where the range starts
     * @param length  how many bytes it has
     * @param charset the character set the bytes should be in
     * @throws ConversionException {@code CONVERT_INVALID_ENCODING} if a byte
     *                             isn't valid in the character set, or stands
     *                             for a character Java's text can't hold; at
     *                             the position in {@code bytes} of the first
     *                             such byte
     */
    static void check(byte[] bytes, int from, int length, Charset charset)
            throws ConversionException
    {
        CharsetDecoder decoder = charset.newDecoder().onMalformedInput(
                CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        // Wrapping keeps the array's indexes, so the buffer's position is the
        // position in the array.
        ByteBuffer input = ByteBuffer.wrap(bytes, from, length);
        // Reused for each part of the text; its contents are never read.
        CharBuffer output = CharBuffer.allocate(8192);
        CoderResult result;
        // An overflow only means the buffer is full: empty it and go on.
        while ((result = decoder.decode(input, output, true)).isOverflow())
        {
            output.clear();
        }
        if (result.isError())
        {
            throw new ConversionException(ErrorCode.CONVERT_INVALID_ENCODING,
                    input.position());
        }
        // Some character sets hold back the last characters until flushed.
        while ((result = decoder.flush(output)).isOverflow())
        {
            output.clear();
        }
        if (result.isError())
        {
            throw new ConversionException(ErrorCode.CONVERT_INVALID_ENCODING,
                    input.position());
        }
    }

    /**
     * Tells whether there is a UTF-8 byte order mark at a position: the bytes
     * {@code EF BB BF}, which some programs put at the start of a file.
     *
     * @param bytes the bytes, such as a record
     * @param from  where to look
     * @return the mark's length, 3, if there is one there; 0 if not
     */
    static int bom(byte[] bytes, int from)
    {
        return bytes.length - from >= 3 && (bytes[from] & 0xff) == 0xEF
                && (bytes[from + 1] & 0xff) == 0xBB
                && (bytes[from + 2] & 0xff) == 0xBF ? 3 : 0;
    }
}
