/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.builtin.EdifactSplitter;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

/**
 * UN/EDIFACT interchange → JSON (design §6.4, mapping {@code edifact/1}), in the shape of common EDI-to-JSON
 * converters, every element being the list of its components:
 * <pre>{"format":"EDIFACT","segments":[{"segment_id":"DTM","elements":[["137","20261028","102"]]}]}</pre>
 *
 * <p>Every segment of the section is included, envelopes too; the UNA service string advice is not a segment and is
 * applied rather than listed. Values are kept by position: empty elements and components stay as {@code ""}, spaces
 * are kept, and release characters are removed ({@code ?+} → {@code +}). The decimal mark isn't changed.
 *
 * <p>The character set comes from the syntax identifier (UNB S001): UNOA/UNOB ASCII, UNOC–UNOK ISO 8859 parts,
 * UNOW/UNOY UTF-8. Others (e.g. UNOX, ISO 2022) fail with {@code CONVERT_EDIFACT_CHARSET}; bytes that aren't valid in
 * the declared set fail with {@code CONVERT_INVALID_ENCODING}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class EdifactConverter implements SectionConverter {
    public static final String MAPPING = "edifact/1";

    private static final Map<String, Charset> CHARSETS = Map.ofEntries(
            Map.entry("UNOA", StandardCharsets.US_ASCII),
            Map.entry("UNOB", StandardCharsets.US_ASCII),
            Map.entry("UNOC", StandardCharsets.ISO_8859_1),
            Map.entry("UNOD", Charset.forName("ISO-8859-2")),
            Map.entry("UNOE", Charset.forName("ISO-8859-5")),
            Map.entry("UNOF", Charset.forName("ISO-8859-7")),
            Map.entry("UNOG", Charset.forName("ISO-8859-3")),
            Map.entry("UNOH", Charset.forName("ISO-8859-4")),
            Map.entry("UNOI", Charset.forName("ISO-8859-6")),
            Map.entry("UNOJ", Charset.forName("ISO-8859-8")),
            Map.entry("UNOK", Charset.forName("ISO-8859-9")),
            Map.entry("UNOW", StandardCharsets.UTF_8),
            Map.entry("UNOY", StandardCharsets.UTF_8));

    @Override
    public String format() {
        return EdifactSplitter.FORMAT;
    }

    @Override
    public String mapping() {
        return MAPPING;
    }

    /** Measured: the section and its JSON, about 2.6 times its size: 256 MiB of heap for a 64 MB interchange. */
    @Override
    public double memoryFactor() {
        return 4;
    }

    /** The service characters, from the UNA service string advice or the defaults. */
    private record Delimiters(byte component, byte element, byte release, byte terminator) {}

    /** What {@link #walk} reports, in document order. Component bytes still hold their release characters. */
    private interface Sink {
        /**
         * @param element   the element's index in its segment, the tag being element 0
         * @param component the component's index in its element
         * @param from      where the component starts in the section
         * @param to        where it ends (its separator)
         * @param released  whether it holds a release character
         */
        void component(int element, int component, int from, int to, boolean released) throws ConversionException;

        void endElement(int element);

        void endSegment();
    }

    @Override
    public byte[] convert(byte[] section, SplitConfig cfg) throws ConversionException {
        byte component = ':';
        byte element = '+';
        byte release = '?';
        byte terminator = '\'';
        int i = skipBreaks(section, 0);
        if (section.length - i >= 9 && section[i] == 'U' && section[i + 1] == 'N' && section[i + 2] == 'A') {
            component = section[i + 3];
            element = section[i + 4];
            release = section[i + 6];
            terminator = section[i + 8];
            i = skipBreaks(section, i + 9);
        }
        Delimiters d = new Delimiters(component, element, release, terminator);
        int first = i;

        // First the structure, without keeping it: the charset is only known once UNB is read, and a malformed
        // segment anywhere is reported before an encoding error.
        Header header = new Header(section, d);
        walk(section, first, d, header);
        if (header.segments == 0 || !"UNB".equals(header.tag) || header.elements < 2) {
            throw new ConversionException(ErrorCode.CONVERT_EDIFACT_MALFORMED, 0);
        }
        Charset cs = CHARSETS.get(header.syntax);
        if (cs == null) {
            throw new ConversionException(ErrorCode.CONVERT_EDIFACT_CHARSET, header.syntaxPosition);
        }

        // About 2.6 times the section: every segment gains its keys, and every component its quotes and commas.
        return JsonWriter.build(section.length * 2 + (section.length >> 1), w -> {
            w.beginObject().key("format").value("EDIFACT").key("segments").beginArray();
            walk(section, first, d, new Writer(section, d, cs, w));
            w.endArray().endObject();
        });
    }

    /**
     * Reads the section's segments, elements and components in order, reporting each to {@code sink}. Nothing is
     * kept, so it can be run once to check the structure and again to write it.
     *
     * @throws ConversionException {@code CONVERT_EDIFACT_MALFORMED} for a release character at the very end, or
     *                             a last segment without its terminator
     */
    private static void walk(byte[] b, int i, Delimiters d, Sink sink) throws ConversionException {
        while (i < b.length) {
            int element = 0;
            int component = 0;
            int start = i;
            boolean released = false;
            boolean terminated = false;
            while (i < b.length) {
                byte c = b[i++];
                if (c == d.release()) {
                    if (i >= b.length) {
                        throw new ConversionException(ErrorCode.CONVERT_EDIFACT_MALFORMED, i - 1);
                    }
                    i++;
                    released = true;
                } else if (c == d.component() || c == d.element() || c == d.terminator()) {
                    sink.component(element, component, start, i - 1, released);
                    released = false;
                    start = i;
                    if (c == d.component()) {
                        component++;
                    } else {
                        sink.endElement(element);
                        element++;
                        component = 0;
                    }
                    if (c == d.terminator()) {
                        terminated = true;
                        break;
                    }
                }
            }
            if (!terminated) {
                throw new ConversionException(ErrorCode.CONVERT_EDIFACT_MALFORMED, start);
            }
            sink.endSegment();
            i = skipBreaks(b, i);
        }
    }

    /**
     * A component's bytes with its release characters removed ({@code ?+} → {@code +}).
     *
     * @param scratch reused for the result when there are release characters; may be replaced by a larger one
     * @return the bytes, in {@code scratch[0]}; their length
     */
    private static int unreleased(byte[] b, int from, int to, byte release, byte[][] scratch) {
        if (scratch[0].length < to - from) {
            scratch[0] = new byte[to - from];
        }
        byte[] out = scratch[0];
        int n = 0;
        for (int i = from; i < to; i++) {
            if (b[i] == release) {
                i++;
            }
            out[n++] = b[i];
        }
        return n;
    }

    /** Checks the structure and records what the first segment says: its tag, element count and charset. */
    private static final class Header implements Sink {
        private final byte[] b;
        private final Delimiters d;
        private final byte[][] scratch = {new byte[16]};
        int segments;
        int elements;
        String tag;
        String syntax;
        int syntaxPosition;

        Header(byte[] b, Delimiters d) {
            this.b = b;
            this.d = d;
        }

        @Override
        public void component(int element, int component, int from, int to, boolean released) {
            if (segments > 0 || component > 0 || element > 1) {
                return;
            }
            String text = new String(raw(from, to, released), element == 0 ? StandardCharsets.ISO_8859_1
                    : StandardCharsets.US_ASCII);
            if (element == 0) {
                tag = text;
            } else {
                syntax = text;
                syntaxPosition = from;
            }
        }

        private byte[] raw(int from, int to, boolean released) {
            if (!released) {
                return Arrays.copyOfRange(b, from, to);
            }
            int n = unreleased(b, from, to, d.release(), scratch);
            return Arrays.copyOf(scratch[0], n);
        }

        @Override
        public void endElement(int element) {
            if (segments == 0) {
                elements = element + 1;
            }
        }

        @Override
        public void endSegment() {
            segments++;
        }
    }

    /**
     * Writes each segment as {@code {"segment_id":…,"elements":[[…],…]}}. Only the tag's first component is
     * written (and decoded); every component of the other elements is.
     */
    private static final class Writer implements Sink {
        private final byte[] b;
        private final Delimiters d;
        private final Charset cs;
        private final JsonWriter w;
        private final byte[][] scratch = {new byte[256]};

        Writer(byte[] b, Delimiters d, Charset cs, JsonWriter w) {
            this.b = b;
            this.d = d;
            this.cs = cs;
            this.w = w;
        }

        @Override
        public void component(int element, int component, int from, int to, boolean released)
                throws ConversionException {
            if (element == 0) {
                if (component == 0) {
                    w.beginObject().key("segment_id").value(decode(from, to, released)).key("elements")
                            .beginArray();
                }
                return;
            }
            if (component == 0) {
                w.beginArray();
            }
            w.value(decode(from, to, released));
        }

        /** Decodes a component, reporting an invalid byte at its position in the section's own numbering. */
        private String decode(int from, int to, boolean released) throws ConversionException {
            if (!released) {
                return Text.decode(b, from, to - from, cs); // positions are already the section's
            }
            int n = unreleased(b, from, to, d.release(), scratch);
            try {
                return Text.decode(scratch[0], 0, n, cs);
            } catch (ConversionException e) {
                throw new ConversionException(e.code(), from + e.position());
            }
        }

        @Override
        public void endElement(int element) {
            if (element > 0) {
                w.endArray();
            }
        }

        @Override
        public void endSegment() {
            w.endArray().endObject();
        }
    }

    private static int skipBreaks(byte[] b, int i) {
        while (i < b.length && (b[i] == '\r' || b[i] == '\n')) {
            i++;
        }
        return i;
    }
}
