package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

/** Strict text decoding for converters: invalid bytes fail the section rather than being replaced. */
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

    /** Length of a UTF-8 byte order mark at {@code from}, or 0. */
    static int bom(byte[] b, int from) {
        return b.length - from >= 3 && (b[from] & 0xff) == 0xEF && (b[from + 1] & 0xff) == 0xBB
                && (b[from + 2] & 0xff) == 0xBF ? 3 : 0;
    }
}
