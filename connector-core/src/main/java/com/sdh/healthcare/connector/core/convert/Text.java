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
 * Strict text decoding for converters: invalid bytes fail the section rather than being replaced.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class Text {
    private Text() {}

    /** Decodes {@code len} bytes from {@code from}; on invalid input reports the position of the first bad byte. */
    static String decode(byte[] b, int from, int len, Charset cs) throws ConversionException {
        CharsetDecoder d = cs.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        ByteBuffer in = ByteBuffer.wrap(b, from, len);
        CharBuffer out = CharBuffer.allocate((int) Math.ceil(len * (double) d.maxCharsPerByte()) + 1);
        CoderResult r = d.decode(in, out, true);
        if (r.isError()) {
            throw new ConversionException(ErrorCode.CONVERT_INVALID_ENCODING, in.position());
        }
        r = d.flush(out);
        if (r.isError()) {
            throw new ConversionException(ErrorCode.CONVERT_INVALID_ENCODING, in.position());
        }
        return out.flip().toString();
    }

    /**
     * Checks that {@code len} bytes from {@code from} are valid in {@code cs}, as {@link #decode} would, without
     * keeping the decoded text: it is decoded a few thousand characters at a time and thrown away. The error
     * position is the one {@link #decode} would report.
     */
    static void check(byte[] b, int from, int len, Charset cs) throws ConversionException {
        CharsetDecoder d = cs.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        ByteBuffer in = ByteBuffer.wrap(b, from, len);
        CharBuffer out = CharBuffer.allocate(8192);
        CoderResult r;
        while ((r = d.decode(in, out, true)).isOverflow()) {
            out.clear();
        }
        if (r.isError()) {
            throw new ConversionException(ErrorCode.CONVERT_INVALID_ENCODING, in.position());
        }
        while ((r = d.flush(out)).isOverflow()) {
            out.clear();
        }
        if (r.isError()) {
            throw new ConversionException(ErrorCode.CONVERT_INVALID_ENCODING, in.position());
        }
    }

    /** Length of a UTF-8 byte order mark at {@code from}, or 0. */
    static int bom(byte[] b, int from) {
        return b.length - from >= 3 && (b[from] & 0xff) == 0xEF && (b[from + 1] & 0xff) == 0xBB
                && (b[from + 2] & 0xff) == 0xBF ? 3 : 0;
    }
}
