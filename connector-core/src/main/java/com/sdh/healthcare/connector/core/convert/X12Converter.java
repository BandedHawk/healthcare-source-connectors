package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.builtin.X12Splitter;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.nio.charset.StandardCharsets;

/**
 * X12 interchange → JSON (design §6.4, mapping {@code x12/1}), in the shape of common EDI-to-JSON converters:
 * <pre>{"format":"X12","segments":[{"segment_id":"NM1","elements":["41","2","SAMPLE CLINIC","","","","","46","…"]}]}</pre>
 *
 * <p>Every segment of the section is included, envelopes too ({@code ISA}, {@code GS}, {@code ST}…{@code SE},
 * {@code GE}, {@code IEA}). Elements are kept exactly as written, by position: empty elements stay as {@code ""} and
 * spaces are kept (including the ISA's fixed-width padding), so downstream can map by position. Composite elements
 * and repetitions stay as one string ({@code "HC:99213:25"}); the separators are in the ISA (ISA16, ISA11).
 * Delimiters are read from the ISA; line breaks after a segment terminator are not part of the data. Input must be
 * UTF-8 (X12's character sets are subsets of it).
 */
public final class X12Converter implements SectionConverter {
    public static final String MAPPING = "x12/1";

    @Override
    public String format() {
        return X12Splitter.FORMAT;
    }

    @Override
    public String mapping() {
        return MAPPING;
    }

    @Override
    public byte[] convert(byte[] section, SplitConfig cfg) throws ConversionException {
        String s = Text.decode(section, 0, section.length, StandardCharsets.UTF_8);
        int i = skipBreaks(s, 0);
        if (!s.startsWith("ISA", i) || s.length() < i + 4) {
            throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED, i);
        }
        // ISA is fixed-format: element separator after "ISA", terminator right after ISA16 (one character).
        char es = s.charAt(i + 3);
        int p = i + 3;
        for (int seps = 0; seps < 16; p++) {
            if (p >= s.length()) {
                throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED, i);
            }
            if (s.charAt(p) == es) {
                seps++;
            }
        }
        if (p + 1 >= s.length()) {
            throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED, i);
        }
        char term = s.charAt(p + 1);

        JsonWriter w = new JsonWriter(section.length * 2);
        w.beginObject().key("format").value("X12").key("segments").beginArray();
        while (i < s.length()) {
            int end = s.indexOf(term, i);
            if (end < 0) {
                throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED, i);
            }
            // With a newline as terminator, a CRLF file leaves a CR at the end of each segment.
            segment(w, s, i, term == '\n' && end > i && s.charAt(end - 1) == '\r' ? end - 1 : end, es);
            i = skipBreaks(s, end + 1);
        }
        w.endArray().endObject();
        return w.toBytes();
    }

    private static void segment(JsonWriter w, String s, int from, int to, char es) {
        int sep = s.indexOf(es, from);
        if (sep < 0 || sep > to) {
            sep = to;
        }
        w.beginObject().key("segment_id").value(s.substring(from, sep)).key("elements").beginArray();
        while (sep < to) {
            int next = s.indexOf(es, sep + 1);
            if (next < 0 || next > to) {
                next = to;
            }
            w.value(s.substring(sep + 1, next));
            sep = next;
        }
        w.endArray().endObject();
    }

    private static int skipBreaks(String s, int i) {
        while (i < s.length() && (s.charAt(i) == '\r' || s.charAt(i) == '\n')) {
            i++;
        }
        return i;
    }
}
