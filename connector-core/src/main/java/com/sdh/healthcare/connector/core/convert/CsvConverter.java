/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.builtin.CsvSplitter;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.nio.charset.StandardCharsets;

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
 *
 * @author Jonas Barnett
 * @since 0.1.0
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

    /** Measured: the section, its JSON (about the same size) and a little more: 160 MiB of heap for a 64 MB record. */
    @Override
    public double memoryFactor() {
        return 2.5;
    }

    @Override
    public byte[] convert(byte[] section, SplitConfig cfg) throws ConversionException {
        // The delimiter is ASCII (the splitter insists), as are quotes and line breaks, and in valid UTF-8 an
        // ASCII byte is always that character, so the record is parsed on its bytes and never decoded.
        byte delim = (byte) CsvSplitter.delimiter(cfg);
        boolean header = cfg.booleanOption("csv.header", false);
        int start = Text.bom(section, 0);
        Text.check(section, start, section.length - start, StandardCharsets.UTF_8);
        Parser p = new Parser(section, delim, start);
        int first = p.record(null);
        int data = start;
        int record = first;
        if (header) {
            data = p.i;
            record = p.record(null);
            if (record < 0) {
                throw new ConversionException(ErrorCode.CONVERT_CSV_MALFORMED, section.length);
            }
            if (record != first) {
                throw new ConversionException(ErrorCode.CONVERT_CSV_FIELD_COUNT, p.recordStart);
            }
        }
        if (record < 0 || p.record(null) >= 0) {
            // The splitter hands over exactly one record (after the header); anything else isn't a section of ours.
            throw new ConversionException(ErrorCode.CONVERT_CSV_MALFORMED, p.recordStart);
        }
        int dataStart = data;
        return JsonWriter.build(section.length + 16, w -> {
            w.beginArray();
            new Parser(section, delim, dataStart).record(w);
            w.endArray();
        });
    }

    /** RFC 4180 records from a section's bytes. Positions are byte offsets in the section. */
    private static final class Parser {
        private final byte[] b;
        private final byte delim;
        int i;
        int recordStart;

        Parser(byte[] b, byte delim, int start) {
            this.b = b;
            this.delim = delim;
            this.i = start;
        }

        /**
         * Reads the next record, writing each field as a JSON string if {@code w} isn't null.
         *
         * @return the record's number of fields, or -1 at the end of the section
         */
        int record(JsonWriter w) throws ConversionException {
            if (i >= b.length) {
                return -1;
            }
            recordStart = i;
            int fields = 0;
            while (true) {
                if (i < b.length && b[i] == '"') {
                    i++;
                    int run = i;
                    if (w != null) {
                        w.beginString();
                    }
                    while (true) {
                        if (i >= b.length) {
                            throw new ConversionException(ErrorCode.CONVERT_CSV_MALFORMED, i);
                        }
                        if (b[i++] == '"') {
                            if (i < b.length && b[i] == '"') {
                                // "" is a quote: keep the first, skip the second
                                if (w != null) {
                                    w.utf8(b, run, i);
                                }
                                run = ++i;
                            } else {
                                if (w != null) {
                                    w.utf8(b, run, i - 1).endString();
                                }
                                break;
                            }
                        }
                    }
                    // After a closing quote only a delimiter or the end of the record may follow.
                    if (i < b.length && b[i] != delim && !atEol()) {
                        throw new ConversionException(ErrorCode.CONVERT_CSV_MALFORMED, i);
                    }
                } else {
                    int from = i;
                    while (i < b.length && b[i] != delim && !atEol()) {
                        i++;
                    }
                    if (w != null) {
                        w.valueUtf8(b, from, i);
                    }
                }
                fields++;
                if (i >= b.length) {
                    return fields;
                }
                if (b[i] == delim) {
                    i++;
                    continue;
                }
                // End of record: CRLF or LF.
                i += b[i] == '\r' ? 2 : 1;
                return fields;
            }
        }

        private boolean atEol() {
            byte c = b[i];
            return c == '\n' || (c == '\r' && i + 1 < b.length && b[i + 1] == '\n');
        }
    }
}
