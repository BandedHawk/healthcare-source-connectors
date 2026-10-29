package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.builtin.CsvSplitter;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * CSV record → JSON array of strings, by field position (design §6.4, mapping {@code csv-row/1}):
 * {@code 1,"Doe, Jane",,"two\nlines"} becomes {@code ["1","Doe, Jane","","two\nlines"]}.
 *
 * <p>Parsing follows RFC 4180: fields may be quoted, {@code ""} inside quotes is a quote, quoted fields may hold
 * delimiters and line breaks, and a CRLF or LF ends the record. As in the splitter (and most CSV readers), a quote
 * that doesn't open a field is kept as an ordinary character. Values are kept exactly: no trimming, no type
 * inference, empty fields are {@code ""}.
 *
 * <p>With {@code csv.header=true} the splitter prepends the header to every section; it is dropped here, and the
 * record must have the same number of fields as the header ({@code CONVERT_CSV_FIELD_COUNT} otherwise). Input must
 * be UTF-8 (a leading byte order mark is ignored); anything else fails with {@code CONVERT_INVALID_ENCODING}. Text
 * after a closing quote fails with {@code CONVERT_CSV_MALFORMED}.
 */
public final class CsvConverter implements SectionConverter {
    public static final String MAPPING = "csv-row/1";

    @Override
    public String format() {
        return CsvSplitter.FORMAT;
    }

    @Override
    public String mapping() {
        return MAPPING;
    }

    @Override
    public byte[] convert(byte[] section, SplitConfig cfg) throws ConversionException {
        char delim = CsvSplitter.delimiter(cfg);
        boolean header = cfg.booleanOption("csv.header", false);
        int start = Text.bom(section, 0);
        String text = Text.decode(section, start, section.length - start, StandardCharsets.UTF_8);
        Parser p = new Parser(text, delim, start);
        List<String> first = p.record();
        List<String> record = first;
        if (header) {
            record = p.record();
            if (record == null) {
                throw new ConversionException(ErrorCode.CONVERT_CSV_MALFORMED, section.length);
            }
            if (record.size() != first.size()) {
                throw new ConversionException(ErrorCode.CONVERT_CSV_FIELD_COUNT, p.recordStart);
            }
        }
        if (record == null || p.record() != null) {
            // The splitter hands over exactly one record (after the header); anything else isn't a section of ours.
            throw new ConversionException(ErrorCode.CONVERT_CSV_MALFORMED, p.recordStart);
        }
        JsonWriter w = new JsonWriter(section.length + 16);
        w.beginArray();
        for (String field : record) {
            w.value(field);
        }
        w.endArray();
        return w.toBytes();
    }

    /** RFC 4180 records from decoded text. Positions are reported as (approximate) byte offsets in the section. */
    private static final class Parser {
        private final String s;
        private final char delim;
        private final int base;
        private int i;
        int recordStart;

        Parser(String s, char delim, int base) {
            this.s = s;
            this.delim = delim;
            this.base = base;
        }

        /** The next record, or null at the end. */
        List<String> record() throws ConversionException {
            if (i >= s.length()) {
                return null;
            }
            recordStart = base + i;
            List<String> fields = new ArrayList<>();
            StringBuilder f = new StringBuilder();
            while (true) {
                if (i < s.length() && s.charAt(i) == '"') {
                    i++;
                    while (true) {
                        if (i >= s.length()) {
                            throw new ConversionException(ErrorCode.CONVERT_CSV_MALFORMED, base + i);
                        }
                        char c = s.charAt(i++);
                        if (c == '"') {
                            if (i < s.length() && s.charAt(i) == '"') {
                                f.append('"');
                                i++;
                            } else {
                                break;
                            }
                        } else {
                            f.append(c);
                        }
                    }
                    // After a closing quote only a delimiter or the end of the record may follow.
                    if (i < s.length() && s.charAt(i) != delim && !atEol()) {
                        throw new ConversionException(ErrorCode.CONVERT_CSV_MALFORMED, base + i);
                    }
                } else {
                    while (i < s.length() && s.charAt(i) != delim && !atEol()) {
                        f.append(s.charAt(i++));
                    }
                }
                fields.add(f.toString());
                f.setLength(0);
                if (i >= s.length()) {
                    return fields;
                }
                if (s.charAt(i) == delim) {
                    i++;
                    continue;
                }
                // End of record: CRLF or LF.
                i += s.charAt(i) == '\r' ? 2 : 1;
                return fields;
            }
        }

        private boolean atEol() {
            char c = s.charAt(i);
            return c == '\n' || (c == '\r' && i + 1 < s.length() && s.charAt(i + 1) == '\n');
        }
    }
}
