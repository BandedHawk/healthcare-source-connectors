package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.builtin.EdifactSplitter;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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

    /** A component's raw bytes (release characters removed) and where it starts in the section. */
    private record Raw(byte[] bytes, int position) {}

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

        // Segments → elements → components, still as bytes: the charset is only known once UNB is read.
        List<List<List<Raw>>> segments = new ArrayList<>();
        while (i < section.length) {
            List<List<Raw>> elements = new ArrayList<>();
            List<Raw> components = new ArrayList<>();
            ByteArrayOutputStream cur = new ByteArrayOutputStream(32);
            int curStart = i;
            boolean terminated = false;
            while (i < section.length) {
                byte c = section[i++];
                if (c == release) {
                    if (i >= section.length) {
                        throw new ConversionException(ErrorCode.CONVERT_EDIFACT_MALFORMED, i - 1);
                    }
                    cur.write(section[i++]);
                } else if (c == component || c == element || c == terminator) {
                    components.add(new Raw(cur.toByteArray(), curStart));
                    cur.reset();
                    curStart = i;
                    if (c != component) {
                        elements.add(components);
                        components = new ArrayList<>();
                    }
                    if (c == terminator) {
                        terminated = true;
                        break;
                    }
                } else {
                    cur.write(c);
                }
            }
            if (!terminated) {
                throw new ConversionException(ErrorCode.CONVERT_EDIFACT_MALFORMED, curStart);
            }
            segments.add(elements);
            i = skipBreaks(section, i);
        }
        if (segments.isEmpty() || !tag(segments.getFirst()).equals("UNB") || segments.getFirst().size() < 2) {
            throw new ConversionException(ErrorCode.CONVERT_EDIFACT_MALFORMED, 0);
        }
        Raw syntax = segments.getFirst().get(1).getFirst();
        Charset cs = CHARSETS.get(new String(syntax.bytes(), StandardCharsets.US_ASCII));
        if (cs == null) {
            throw new ConversionException(ErrorCode.CONVERT_EDIFACT_CHARSET, syntax.position());
        }

        JsonWriter w = new JsonWriter(section.length * 3);
        w.beginObject().key("format").value("EDIFACT").key("segments").beginArray();
        for (List<List<Raw>> seg : segments) {
            w.beginObject().key("segment_id").value(decode(seg.getFirst().getFirst(), cs)).key("elements").beginArray();
            for (int e = 1; e < seg.size(); e++) {
                w.beginArray();
                for (Raw r : seg.get(e)) {
                    w.value(decode(r, cs));
                }
                w.endArray();
            }
            w.endArray().endObject();
        }
        w.endArray().endObject();
        return w.toBytes();
    }

    private static String tag(List<List<Raw>> segment) {
        return new String(segment.getFirst().getFirst().bytes(), StandardCharsets.ISO_8859_1);
    }

    private static String decode(Raw r, Charset cs) throws ConversionException {
        try {
            return Text.decode(r.bytes(), 0, r.bytes().length, cs);
        } catch (ConversionException e) {
            throw new ConversionException(e.code(), r.position() + e.position());
        }
    }

    private static int skipBreaks(byte[] b, int i) {
        while (i < b.length && (b[i] == '\r' || b[i] == '\n')) {
            i++;
        }
        return i;
    }
}
