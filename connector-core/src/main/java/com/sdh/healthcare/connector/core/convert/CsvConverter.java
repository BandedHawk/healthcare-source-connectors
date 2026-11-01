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
 * Converts one CSV record into JSON: an array of its fields as strings, in the
 * order they appear (design §6.4). For example, the record
 * {@code 1,"Doe, Jane",,"two\nlines"} becomes
 * {@code ["1","Doe, Jane","","two\nlines"]}. The JSON is labelled
 * {@value #MAPPING}, so whatever reads it knows its shape.
 * <p>
 * Each section it is given is one record, as {@link CsvSplitter} cut it from
 * the file. The record is read by the rules of RFC 4180, the usual CSV
 * standard:
 * <ul>
 * <li>Fields are separated by the delimiter: a comma unless
 * {@code csv.delimiter} says otherwise.</li>
 * <li>A field may be put in double quotes. Inside quotes, the delimiter and
 * line breaks are part of the value, and two quotes in a row ({@code ""})
 * stand for one quote.</li>
 * <li>A line break ends the record: CR LF or a lone LF. A lone CR is
 * ordinary text.</li>
 * <li>A quote that doesn't start a field, as in {@code 5" disk}, is kept as
 * an ordinary character, as the splitter (and most CSV readers) treat it.</li>
 * </ul>
 * Values are kept exactly as written: nothing is trimmed, nothing is turned
 * into a number or a date, and an empty field is {@code ""}.
 * <p>
 * With {@code csv.header=true}, the splitter puts the file's header record in
 * front of every section, so each one can be understood on its own. This
 * converter checks the record against it and then leaves it out of the JSON:
 * the record must have as many fields as the header.
 * <p>
 * A section that can't be converted fails with a {@link ConversionException}
 * giving the byte position of the problem, and is then stored as split:
 * <ul>
 * <li>{@code CONVERT_INVALID_ENCODING}: the bytes aren't valid UTF-8. (A
 * UTF-8 byte order mark at the start is allowed and skipped.)</li>
 * <li>{@code CONVERT_CSV_FIELD_COUNT}: the record doesn't have as many fields
 * as the header.</li>
 * <li>{@code CONVERT_CSV_MALFORMED}: a quoted field is never closed, text
 * follows a closing quote, or the section isn't exactly one record (after the
 * header, if there is one).</li>
 * </ul>
 * The record is read straight from its bytes and never decoded into a Java
 * string, which keeps memory low ({@link #memoryFactor()}).
 * <p>
 * The converter keeps no state between sections, so one instance can convert
 * several sections at once on different threads.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 * @see CsvSplitter
 * @see SectionConverter
 */
public final class CsvConverter implements SectionConverter
{

    /**
     * The name and version of the JSON this converter produces, returned by
     * {@link #mapping()}: an array of the record's fields by position. It
     * travels with every converted section, so whatever reads the JSON knows
     * its shape. The version changes only if the shape changes in a way
     * existing readers would get wrong.
     */
    public static final String MAPPING = "csv-row/1";

    /**
     * Creates the converter. It has no settings of its own: the delimiter and
     * header option are read from the split settings each section is
     * converted with. {@link ConverterRegistry#load()} creates it through
     * Java's {@link java.util.ServiceLoader}, which needs this public
     * constructor without parameters.
     */
    public CsvConverter()
    {
    }

    /**
     * The format this converter handles: {@code "csv"}, the same name as
     * {@link CsvSplitter}'s, so it is given the sections that splitter makes.
     *
     * @return {@code "csv"}
     */
    @Override
    public String format()
    {
        return CsvSplitter.FORMAT;
    }

    /**
     * The name and version of the JSON this converter produces.
     *
     * @return {@value #MAPPING}
     */
    @Override
    public String mapping()
    {
        return MAPPING;
    }

    /**
     * The most memory a conversion needs, as a multiple of the section's size:
     * the section itself, its JSON (about the same size) and a little more.
     * Measured at 160 MiB of heap for a 64 MB record.
     *
     * @return 2.5
     */
    @Override
    public double memoryFactor()
    {
        return 2.5;
    }

    /**
     * Converts one record, with the header in front of it if
     * {@code csv.header} is on, into a JSON array of its fields.
     * <p>
     * The section is read twice. The first time checks it and nothing is kept:
     * the bytes must be valid UTF-8, the section must hold exactly one record
     * (after the header), and that record must have as many fields as the
     * header. Only then is it read again to write the JSON, so a section that
     * fails never has JSON built for it.
     *
     * @param section the section's bytes exactly as split; not changed
     * @param config  the connector's split settings, for {@code csv.delimiter}
     *                and {@code csv.header}
     * @return the record's fields as a JSON array of strings, in UTF-8, in a
     *         new array
     * @throws ConversionException if the section isn't valid UTF-8, isn't
     *                             exactly one well-formed record, or has a
     *                             different number of fields from the
     *                             header; it gives the byte position of the
     *                             problem and the matching error code (see
     *                             the class description)
     */
    @Override
    public byte[] convert(byte[] section, SplitConfig config) throws
            ConversionException
    {
        // The delimiter is ASCII (the splitter insists), as are quotes and
        // line breaks, and in valid UTF-8 an ASCII byte is always that
        // character, so the record is parsed on its bytes and never decoded.
        // The field separator, such as ',' (one ASCII character).
        byte delim = (byte) CsvSplitter.delimiter(config);
        // Whether the section starts with the file's header record.
        boolean header = config.booleanOption("csv.header", false);
        // Where the text starts: after a byte order mark, if there is one.
        int start = Text.bom(section, 0);
        Text.check(section, start, section.length - start,
                StandardCharsets.UTF_8);
        // First reading: check the section, without writing anything.
        Parser parser = new Parser(section, delim, start);
        // How many fields the first record has: the header's, if there is one.
        int first = parser.record(null);
        // Where the record to convert starts: after the header, if there is
        // one.
        int data = start;
        // How many fields the record to convert has; -1 if there isn't one.
        int record = first;
        if (header)
        {
            data = parser.position;
            record = parser.record(null);
            if (record < 0)
            {
                throw new ConversionException(ErrorCode.CONVERT_CSV_MALFORMED,
                        section.length);
            }
            if (record != first)
            {
                throw new ConversionException(ErrorCode.CONVERT_CSV_FIELD_COUNT,
                        parser.recordStart);
            }
        }
        if (record < 0 || parser.record(null) >= 0)
        {
            // The splitter hands over exactly one record (after the header);
            // anything else isn't a section of ours.
            throw new ConversionException(ErrorCode.CONVERT_CSV_MALFORMED,
                    parser.recordStart);
        }
        // Second reading: write the record's fields. A fresh parser each time,
        // because JsonWriter.build may run this twice (first only to count the
        // JSON's size), and a final copy of where the record starts, for the
        // lambda.
        int dataStart = data;
        return JsonWriter.build(section.length + 16, writer -> 
                {
                    writer.beginArray();
                    new Parser(section, delim, dataStart).record(writer);
                    writer.endArray();
        });
    }

    /**
     * Reads CSV records, one after another, straight from a section's bytes,
     * by the rules in the class description. It either only checks and counts
     * the fields, or also writes them as JSON.
     * <p>
     * Working on bytes, without decoding them into characters, is safe
     * because the section has already been checked to be valid UTF-8, and
     * everything the parser looks for (the delimiter, quotes and line breaks)
     * is ASCII. In UTF-8 an ASCII byte value never appears inside another
     * character, so it always means that character.
     * <p>
     * All positions are byte offsets from the start of the section.
     */
    private static final class Parser
    {

        /** The section's bytes. */
        private final byte[] bytes;
        /** The field separator, such as {@code ','}. */
        private final byte delim;
        /** The position of the next byte to read. */
        int position;
        /**
         * Where the record read last starts, to report as the position of a
         * problem with the record as a whole.
         */
        int recordStart;

        /**
         * Creates a parser that starts reading at the given position.
         *
         * @param bytes the section's bytes
         * @param delim the field separator
         * @param start where to start reading: the start of a record
         */
        Parser(byte[] bytes, byte delim, int start)
        {
            this.bytes = bytes;
            this.delim = delim;
            this.position = start;
        }

        /**
         * Reads the next record, and moves on to the start of the one after
         * it. If {@code writer} isn't {@code null}, each field is also written
         * to it as a JSON string; the caller writes the array's brackets
         * around them.
         * <p>
         * A field in quotes ends at its closing quote, with each {@code ""}
         * inside written as one quote. Any other field ends at the next
         * delimiter or line break. An empty line is a record of one empty
         * field.
         *
         * @param writer where to write the fields, or {@code null} to only
         *               check and count them
         * @return how many fields the record has, or -1 if there are no more
         *         records in the section
         * @throws ConversionException with {@code CONVERT_CSV_MALFORMED} if a
         *                             quoted field is never closed, or a
         *                             closing quote is followed by anything
         *                             other than a delimiter or the end of
         *                             the record
         */
        int record(JsonWriter writer) throws ConversionException
        {
            if (position >= bytes.length)
            {
                return -1;
            }
            recordStart = position;
            int fields = 0;
            while (true)
            {
                if (position < bytes.length && bytes[position] == '"')
                {
                    position++;
                    // Where the text not yet written starts.
                    int run = position;
                    if (writer != null)
                    {
                        writer.beginString();
                    }
                    while (true)
                    {
                        if (position >= bytes.length)
                        {
                            throw new ConversionException(
                                    ErrorCode.CONVERT_CSV_MALFORMED, position);
                        }
                        if (bytes[position++] == '"')
                        {
                            if (position < bytes.length
                                    && bytes[position] == '"')
                            {
                                // "" is a quote: keep the first, skip the
                                // second
                                if (writer != null)
                                {
                                    writer.utf8(bytes, run, position);
                                }
                                run = ++position;
                            }
                            else
                            {
                                if (writer != null)
                                {
                                    writer.utf8(bytes, run, position - 1)
                                            .endString();
                                }
                                break;
                            }
                        }
                    }
                    // After a closing quote only a delimiter or the end of
                    // the record may follow.
                    if (position < bytes.length && bytes[position] != delim
                            && !atEol())
                    {
                        throw new ConversionException(
                                ErrorCode.CONVERT_CSV_MALFORMED, position);
                    }
                }
                else
                {
                    // Where this unquoted field starts.
                    int from = position;
                    while (position < bytes.length && bytes[position] != delim
                            && !atEol())
                    {
                        position++;
                    }
                    if (writer != null)
                    {
                        writer.valueUtf8(bytes, from, position);
                    }
                }
                fields++;
                if (position >= bytes.length)
                {
                    return fields;
                }
                if (bytes[position] == delim)
                {
                    position++;
                    continue;
                }
                // End of record: CRLF or LF.
                position += bytes[position] == '\r' ? 2 : 1;
                return fields;
            }
        }

        /**
         * Whether a line break, and so the end of the record, starts at the
         * current position: an LF, or a CR followed by an LF. A CR on its own
         * is ordinary text.
         *
         * @return {@code true} if the record ends here
         */
        private boolean atEol()
        {
            byte current = bytes[position];
            return current == '\n' || (current == '\r'
                    && position + 1 < bytes.length
                    && bytes[position + 1] == '\n');
        }
    }
}
