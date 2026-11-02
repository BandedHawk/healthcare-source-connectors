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
 * Converts one X12 interchange into JSON (design §6.4). The JSON is labeled
 * {@value #MAPPING}, so whatever reads it knows its shape.
 * <p>
 * An X12 interchange is a run of segments, such as
 * {@code NM1*41*2*SAMPLE CLINIC*****46*12345~}. Each segment starts with its
 * ID ({@code NM1}) and holds elements, separated by {@code *}. A segment ends
 * with {@code ~}. These are the usual characters, but each interchange chooses
 * its own and says which in its first segment, {@code ISA}. The JSON follows
 * the shape common EDI-to-JSON converters use: each segment is an object with
 * its ID and its elements, in order:
 * <pre>
 * {"format":"X12","segments":[{"segment_id":"NM1",
 *   "elements":["41","2","SAMPLE CLINIC","","","","","46","12345"]}]}
 * </pre>
 * (shown on two lines here; the JSON itself has no spaces or line breaks).
 * <p>
 * What is kept, and how:
 * <ul>
 * <li><b>Every segment</b> of the section is written, in order, the envelope
 * segments ({@code ISA}, {@code GS}, {@code ST} to {@code SE}, {@code GE} and
 * {@code IEA}) included.</li>
 * <li><b>Elements are kept by position and exactly as written,</b> so whatever
 * reads the JSON can find an element by its position: an empty element stays
 * as {@code ""}, and spaces are kept, including the padding of the
 * {@code ISA} segment's fixed-width elements.</li>
 * <li><b>A composite element or a repeated element stays one string,</b> such
 * as {@code "HC:99213:25"}. The characters that separate their parts are in
 * the {@code ISA} segment: the component separator is ISA16 and the
 * repetition separator ISA11.</li>
 * <li><b>Line breaks after a segment's terminator</b> are ignored, as many
 * senders add one after every segment.</li>
 * </ul>
 * <p>
 * The section must be UTF-8. X12's character sets are all part of UTF-8, so
 * this accepts any interchange that keeps to them.
 * <p>
 * A section that can't be converted fails with a {@link ConversionException}
 * giving the byte position of the problem, and is then stored as split:
 * <ul>
 * <li>{@code CONVERT_INVALID_ENCODING}: the section isn't valid UTF-8.</li>
 * <li>{@code CONVERT_X12_MALFORMED}: the section doesn't start with an
 * {@code ISA} segment from which the separators can be read, a separator
 * isn't a single ASCII character, or the last segment has no
 * terminator.</li>
 * </ul>
 * <p>
 * The section is worked on as bytes and never decoded into one large string.
 * <p>
 * The converter keeps no state between sections, so one instance can convert
 * several sections at once on different threads.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 * @see X12Splitter
 * @see SectionConverter
 */
public final class X12Converter implements SectionConverter
{

    /**
     * The name and version of the JSON this converter produces, returned by
     * {@link #mapping()}: the interchange's segments, each with its ID and its
     * elements as strings. It travels with every converted section, so
     * whatever reads the JSON knows its shape. The version changes only if the
     * shape changes in a way existing readers would get wrong.
     */
    public static final String MAPPING = "x12/1";

    /** The ID of an interchange's first segment, as bytes: {@code ISA}. */
    private static final byte[] ISA = {'I', 'S', 'A'};

    /**
     * Creates the converter. It has no settings of its own.
     * {@link ConverterRegistry#load()} creates it through Java's
     * {@link java.util.ServiceLoader}, which needs this public constructor
     * without parameters.
     */
    public X12Converter()
    {
    }

    /**
     * The format this converter handles: {@code "x12"}, the same name as
     * {@link X12Splitter}'s, so it is given the sections that splitter makes.
     *
     * @return {@code "x12"}
     */
    @Override
    public String format()
    {
        return X12Splitter.FORMAT;
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
     * the section itself and its JSON, which is about 2.4 times the section's
     * size, as every segment gains its keys and every element its quotes and
     * comma. Measured at 224 MiB of heap for a 64 MB interchange.
     *
     * @return 3.5
     */
    @Override
    public double memoryFactor()
    {
        return 3.5;
    }

    /**
     * Converts one interchange into JSON, as the class description says.
     * <p>
     * It first checks that the whole section is valid UTF-8. Then it reads the
     * separators from the {@code ISA} segment. That segment has a fixed
     * layout: the character right after {@code ISA} separates its elements;
     * ISA16, the last element, is a single character; and the character after
     * that ends the segment. Then each segment is written in turn.
     *
     * @param section the section's bytes exactly as split; not changed
     * @param config  the connector's split settings; this converter needs none
     *                of them
     * @return the interchange as JSON, in UTF-8, in a new array
     * @throws ConversionException {@code CONVERT_INVALID_ENCODING} if the
     *                             section isn't valid UTF-8, at the first
     *                             invalid byte; {@code CONVERT_X12_MALFORMED}
     *                             if the separators can't be read from the
     *                             {@code ISA} segment (at its start) or the
     *                             last segment has no terminator (at that
     *                             segment's start)
     */
    @Override
    public byte[] convert(byte[] section, SplitConfig config) throws
            ConversionException
    {
        Text.check(section, 0, section.length, StandardCharsets.UTF_8);
        // Where the ISA segment starts, after any line breaks.
        int isaStart = skipBreaks(section, 0);
        if (!startsWith(section, isaStart, ISA)
                || section.length < isaStart + 4)
        {
            throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED,
                    isaStart);
        }
        // The ISA segment has a fixed layout: the element separator is right
        // after "ISA", and the terminator right after ISA16, which is one
        // character.
        byte elementSeparator = section[isaStart + 3];
        // Finds the 16th element separator; ISA16 is the byte after it.
        int position = isaStart + 3;
        for (int separators = 0; separators < 16; position++)
        {
            if (position >= section.length)
            {
                throw new ConversionException(
                        ErrorCode.CONVERT_X12_MALFORMED, isaStart);
            }
            if (section[position] == elementSeparator)
            {
                separators++;
            }
        }
        if (position + 1 >= section.length)
        {
            throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED,
                    isaStart);
        }
        byte terminator = section[position + 1];
        if (elementSeparator < 0 || terminator < 0)
        {
            // A separator must be one character; a byte of a multi-byte UTF-8
            // character isn't one.
            throw new ConversionException(ErrorCode.CONVERT_X12_MALFORMED,
                    isaStart);
        }
        // The estimate is 2.5 times the section: every segment gains its keys
        // and every element its quotes. JsonWriter.build may run this twice
        // (first only to count the JSON's size), so it starts from the ISA
        // segment each time.
        return JsonWriter.build(section.length * 2 + (section.length >> 1),
                writer ->
        {
            writer.beginObject().key("format").value("X12").key("segments")
                    .beginArray();
            int segmentStart = isaStart;
            while (segmentStart < section.length)
            {
                int segmentEnd = indexOf(section, terminator, segmentStart);
                if (segmentEnd < 0)
                {
                    throw new ConversionException(
                            ErrorCode.CONVERT_X12_MALFORMED, segmentStart);
                }
                // With a newline as terminator, a file with CRLF line endings
                // leaves a CR at the end of each segment, which isn't data.
                segment(writer, section, segmentStart,
                        terminator == '\n' && segmentEnd > segmentStart
                                && section[segmentEnd - 1] == '\r'
                                ? segmentEnd - 1 : segmentEnd,
                        elementSeparator);
                segmentStart = skipBreaks(section, segmentEnd + 1);
            }
            writer.endArray().endObject();
        });
    }

    /**
     * Writes one segment as a JSON object: its ID, then its elements as
     * strings, in order.
     * <p>
     * The bytes are copied as they are, without decoding. That is safe because
     * the section is valid UTF-8 and the separators are ASCII characters,
     * which never occur inside a character of more than one byte, so cutting
     * at separators never splits a character.
     *
     * @param writer           where the JSON is written
     * @param section          the section's bytes
     * @param from             where the segment starts
     * @param to               where it ends: the position of its terminator,
     *                         or of a CR just before it
     * @param elementSeparator the character between elements
     */
    private static void segment(JsonWriter writer, byte[] section, int from,
            int to, byte elementSeparator)
    {
        // The separator before the next element, or the segment's end.
        int separator = indexOf(section, elementSeparator, from);
        if (separator < 0 || separator > to)
        {
            separator = to;
        }
        writer.beginObject().key("segment_id")
                .valueUtf8(section, from, separator).key("elements")
                .beginArray();
        while (separator < to)
        {
            int nextSeparator = indexOf(section, elementSeparator,
                    separator + 1);
            if (nextSeparator < 0 || nextSeparator > to)
            {
                nextSeparator = to;
            }
            writer.valueUtf8(section, separator + 1, nextSeparator);
            separator = nextSeparator;
        }
        writer.endArray().endObject();
    }

    /**
     * Finds the first occurrence of a byte, from a position to the end of the
     * section.
     *
     * @param section the section's bytes
     * @param target  the byte to find
     * @param from    where to start looking
     * @return the byte's position, or -1 if it doesn't occur
     */
    private static int indexOf(byte[] section, byte target, int from)
    {
        for (int position = from; position < section.length; position++)
        {
            if (section[position] == target)
            {
                return position;
            }
        }
        return -1;
    }

    /**
     * Tells whether the section has the given bytes at a position.
     *
     * @param section  the section's bytes
     * @param position where the bytes should start
     * @param prefix   the bytes to look for
     * @return true if they are all there, false if not or if the section ends
     *         first
     */
    private static boolean startsWith(byte[] section, int position,
            byte[] prefix)
    {
        if (section.length - position < prefix.length)
        {
            return false;
        }
        for (int index = 0; index < prefix.length; index++)
        {
            if (section[position + index] != prefix[index])
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Skips any line breaks (CR or LF) at a position, as senders often put one
     * after each segment.
     *
     * @param section  the section's bytes
     * @param position where to start
     * @return the position of the first byte that isn't a line break, or the
     *         section's length if only line breaks remain
     */
    private static int skipBreaks(byte[] section, int position)
    {
        while (position < section.length
                && (section[position] == '\r' || section[position] == '\n'))
        {
            position++;
        }
        return position;
    }
}
