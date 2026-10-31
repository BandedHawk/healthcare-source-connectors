/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

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
 * UTF-8 (X12's character sets are subsets of it), and the separators single ASCII characters
 * ({@code CONVERT_X12_MALFORMED} otherwise). The section is worked on as bytes and never decoded into a string, so
 * error positions are byte offsets.
 *
 * @author Jonas Barnett
 * @since 0.1.0
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

    /** Measured: the section and its JSON, about 2.4 times its size: 224 MiB of heap for a 64 MB interchange. */
    @Override
    public double memoryFactor() {
        return 3.5;
    }

    @Override
    public byte[] convert(byte[] section, SplitConfig cfg) throws ConversionException {
        Text.check(section, 0, section.length, StandardCharsets.UTF_8);
        int first = skipBreaks(section, 0);
        if (!startsWith(section, first, ISA) || section.length < first + 4) {
            throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED, first);
        }
        // ISA is fixed-format: element separator after "ISA", terminator right after ISA16 (one character).
        byte es = section[first + 3];
        int p = first + 3;
        for (int seps = 0; seps < 16; p++) {
            if (p >= section.length) {
                throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED, first);
            }
            if (section[p] == es) {
                seps++;
            }
        }
        if (p + 1 >= section.length) {
            throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED, first);
        }
        byte term = section[p + 1];
        if (es < 0 || term < 0) {
            // A separator must be one character; a byte of a multi-byte UTF-8 character isn't one.
            throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED, first);
        }
        // About 2.4 times the section: every segment gains its keys and every element its quotes.
        return JsonWriter.build(section.length * 2 + (section.length >> 1), w -> {
            w.beginObject().key("format").value("X12").key("segments").beginArray();
            int i = first;
            while (i < section.length) {
                int end = indexOf(section, term, i);
                if (end < 0) {
                    throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED, i);
                }
                // With a newline as terminator, a CRLF file leaves a CR at the end of each segment.
                segment(w, section, i, term == '\n' && end > i && section[end - 1] == '\r' ? end - 1 : end, es);
                i = skipBreaks(section, end + 1);
            }
            w.endArray().endObject();
        });
    }

    private static final byte[] ISA = {'I', 'S', 'A'};

    /**
     * Writes one segment from its bytes, {@code from} up to its terminator at {@code to}. The section is valid
     * UTF-8 and the separators are ASCII, which never occurs inside a multi-byte character, so the bytes can be
     * cut at separators and copied as they are.
     */
    private static void segment(JsonWriter w, byte[] b, int from, int to, byte es) {
        int sep = indexOf(b, es, from);
        if (sep < 0 || sep > to) {
            sep = to;
        }
        w.beginObject().key("segment_id").valueUtf8(b, from, sep).key("elements").beginArray();
        while (sep < to) {
            int next = indexOf(b, es, sep + 1);
            if (next < 0 || next > to) {
                next = to;
            }
            w.valueUtf8(b, sep + 1, next);
            sep = next;
        }
        w.endArray().endObject();
    }

    private static int indexOf(byte[] b, byte c, int from) {
        for (int i = from; i < b.length; i++) {
            if (b[i] == c) {
                return i;
            }
        }
        return -1;
    }

    private static boolean startsWith(byte[] b, int at, byte[] prefix) {
        if (b.length - at < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (b[at + i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static int skipBreaks(byte[] b, int i) {
        while (i < b.length && (b[i] == '\r' || b[i] == '\n')) {
            i++;
        }
        return i;
    }
}
