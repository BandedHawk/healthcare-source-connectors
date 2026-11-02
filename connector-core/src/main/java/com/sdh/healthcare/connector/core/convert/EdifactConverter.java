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
 * Converts one UN/EDIFACT interchange into JSON (design §6.4). The JSON is
 * labeled {@value #MAPPING}, so whatever reads it knows its shape.
 * <p>
 * An EDIFACT interchange is a run of segments, such as
 * {@code DTM+137:20261028:102'}. Each segment starts with a three-letter tag
 * ({@code DTM}) and holds elements, separated by {@code +}. An element holds
 * one or more components, separated by {@code :}. A segment ends with
 * {@code '}. The JSON follows the shape common EDI-to-JSON converters use:
 * each segment is an object with its tag and its elements, and each element
 * is the list of its components:
 * <pre>
 * {"format":"EDIFACT","segments":[
 *   {"segment_id":"DTM","elements":[["137","20261028","102"]]}]}
 * </pre>
 * (shown on two lines here; the JSON itself has no spaces or line breaks).
 * <p>
 * What is kept, and how:
 * <ul>
 * <li><b>Every segment</b> of the section is written, in order, the envelope
 * segments ({@code UNB}, {@code UNH}, {@code UNT}, {@code UNZ} and so on)
 * included.</li>
 * <li><b>The UNA segment</b>, if the interchange starts with one, is not
 * written. It isn't data: it says which characters the interchange uses as
 * separators, and the converter uses them. Without it, the standard ones
 * are used: {@code :} between components, {@code +} between elements,
 * {@code ?} as the release character and {@code '} at the end of a
 * segment.</li>
 * <li><b>Values are kept by position and exactly as written:</b> an empty
 * element or component stays as {@code ""}, spaces are kept, and the decimal
 * mark is left as it is. The release character, which lets a separator
 * appear inside a value, is removed: {@code ?+} becomes {@code +}.</li>
 * <li><b>Line breaks between segments</b> are ignored, as many senders add one
 * after every {@code '}.</li>
 * </ul>
 * <p>
 * The text is decoded in the character set the interchange declares in its
 * syntax identifier, the first component of the {@code UNB} segment's first
 * element:
 * <ul>
 * <li>{@code UNOA} and {@code UNOB}: ASCII;</li>
 * <li>{@code UNOC} to {@code UNOK}: the parts of ISO 8859 listed in
 * {@link #CHARSETS};</li>
 * <li>{@code UNOW} and {@code UNOY}: UTF-8.</li>
 * </ul>
 * <p>
 * A section that can't be converted fails with a {@link ConversionException}
 * giving the byte position of the problem, and is then stored as split:
 * <ul>
 * <li>{@code CONVERT_EDIFACT_MALFORMED}: the section doesn't start with a
 * {@code UNB} segment that has a syntax identifier, its last segment has no
 * {@code '}, or it ends in a release character with nothing after it.</li>
 * <li>{@code CONVERT_EDIFACT_CHARSET}: the syntax identifier names a
 * character set not listed above, such as {@code UNOX} (ISO 2022).</li>
 * <li>{@code CONVERT_INVALID_ENCODING}: the bytes aren't valid in the declared
 * character set.</li>
 * </ul>
 * <p>
 * The section is read twice ({@link #walk}): first to check its structure and
 * find its character set, then to write the JSON. Nothing is decoded until the
 * second reading, one component at a time, so the section is never decoded
 * into one large string.
 * <p>
 * The converter keeps no state between sections, so one instance can convert
 * several sections at once on different threads.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 * @see EdifactSplitter
 * @see SectionConverter
 */
public final class EdifactConverter implements SectionConverter
{

    /**
     * The name and version of the JSON this converter produces, returned by
     * {@link #mapping()}: the interchange's segments, each with its tag and its
     * elements as lists of components. It travels with every converted
     * section, so whatever reads the JSON knows its shape. The version changes
     * only if the shape changes in a way existing readers would get wrong.
     */
    public static final String MAPPING = "edifact/1";

    /**
     * The character set for each syntax identifier the converter supports, by
     * the identifier's code. The identifier is the first component of the
     * {@code UNB} segment's first element, such as {@code UNOC} in
     * {@code UNB+UNOC:3+…}. A code not listed here fails the section with
     * {@code CONVERT_EDIFACT_CHARSET}.
     */
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

    /**
     * Creates the converter. It has no settings of its own.
     * {@link ConverterRegistry#load()} creates it through Java's
     * {@link java.util.ServiceLoader}, which needs this public constructor
     * without parameters.
     */
    public EdifactConverter()
    {
    }

    /**
     * The format this converter handles: {@code "edifact"}, the same name as
     * {@link EdifactSplitter}'s, so it is given the sections that splitter
     * makes.
     *
     * @return {@code "edifact"}
     */
    @Override
    public String format()
    {
        return EdifactSplitter.FORMAT;
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
     * the section itself and its JSON, which is about 2.6 times the section's
     * size, as every segment gains its keys and every component its quotes and
     * comma. Measured at 256 MiB of heap for a 64 MB interchange.
     *
     * @return 4
     */
    @Override
    public double memoryFactor()
    {
        return 4;
    }

    /**
     * The four characters that give an interchange its structure. They come
     * from the UNA segment if there is one, and are otherwise the standard
     * ones.
     *
     * @param component  the character between components, normally {@code :}
     * @param element    the character between elements, normally {@code +}
     * @param release    the release character, normally {@code ?}: the
     *                   character after it is part of the value, even if it
     *                   is one of these four
     * @param terminator the character that ends a segment, normally
     *                   {@code '}
     */
    private record Delimiters(byte component, byte element, byte release,
            byte terminator)
    {
    }

    /**
     * Receives the pieces of an interchange from {@link #walk}, in the order
     * they appear. There are two: {@link Header} checks the structure, and
     * {@link SegmentWriter} writes the JSON.
     * <p>
     * A component is given as a range of the section's bytes, with any release
     * characters still in it, so nothing is copied or decoded unless the
     * receiver needs it.
     */
    private interface Sink
    {

        /**
         * Receives one component.
         *
         * @param element   the number of its element within the segment,
         *                  counting the tag as element 0
         * @param component the number of the component within its element,
         *                  from 0
         * @param from      where the component starts in the section
         * @param to        where it ends: the position of the separator after
         *                  it
         * @param released  whether it holds a release character, and so needs
         *                  them removed before use
         * @throws ConversionException if the receiver can't use the component,
         *                             such as bytes that aren't valid in the
         *                             character set
         */
        void component(int element, int component, int from, int to,
                boolean released) throws ConversionException;

        /**
         * Marks the end of an element: its last component has been given.
         *
         * @param element the number of the element within its segment,
         *                counting the tag as element 0
         */
        void endElement(int element);

        /** Marks the end of a segment: its last element has been given. */
        void endSegment();
    }

    /**
     * Converts one interchange into JSON, as the class description says.
     * <p>
     * It first reads the separators from the UNA segment, if there is one.
     * Then the section is read twice. The first reading checks the structure
     * and finds the syntax identifier, and keeps nothing else. It comes first
     * because the character set is only known once the {@code UNB} segment has
     * been read, and so that a malformed segment anywhere in the section is
     * reported before any encoding error. The second reading writes the JSON.
     *
     * @param section the section's bytes exactly as split; not changed
     * @param config  the connector's split settings; this converter needs none
     *                of them
     * @return the interchange as JSON, in UTF-8, in a new array
     * @throws ConversionException if the section isn't a well-formed
     *                             interchange, declares an unsupported
     *                             character set, or has bytes that aren't
     *                             valid in it; it gives the byte position of
     *                             the problem and the matching error code
     *                             (see the class description)
     */
    @Override
    public byte[] convert(byte[] section, SplitConfig config) throws
            ConversionException
    {
        // The standard separators, used unless a UNA segment says otherwise.
        byte component = ':';
        byte element = '+';
        byte release = '?';
        byte terminator = '\'';
        // Where reading starts: the first segment, after any line breaks.
        int position = skipBreaks(section, 0);
        // A UNA segment is "UNA" and six characters: the component separator,
        // the element separator, the decimal mark, the release character, a
        // reserved character and the segment terminator. The decimal mark and
        // the reserved character aren't needed.
        if (section.length - position >= 9 && section[position] == 'U'
                && section[position + 1] == 'N'
                && section[position + 2] == 'A')
        {
            component = section[position + 3];
            element = section[position + 4];
            release = section[position + 6];
            terminator = section[position + 8];
            position = skipBreaks(section, position + 9);
        }
        Delimiters delimiters = new Delimiters(component, element, release,
                terminator);
        // Where the first segment after UNA starts, for both readings.
        int firstSegment = position;

        // First reading: check the structure and find the syntax identifier,
        // keeping nothing else.
        Header header = new Header(section, delimiters);
        walk(section, firstSegment, delimiters, header);
        if (header.segmentCount == 0 || !"UNB".equals(header.tag)
                || header.elementCount < 2)
        {
            throw new ConversionException(ErrorCode.CONVERT_EDIFACT_MALFORMED,
                    0);
        }
        Charset charset = CHARSETS.get(header.syntax);
        if (charset == null)
        {
            throw new ConversionException(ErrorCode.CONVERT_EDIFACT_CHARSET,
                    header.syntaxPosition);
        }

        // Second reading: write the JSON. JsonWriter.build may run this twice
        // (first only to count the JSON's size), so it starts from the first
        // segment each time. The estimate is 2.5 times the section's size.
        return JsonWriter.build(section.length * 2 + (section.length >> 1),
                writer ->
        {
            writer.beginObject().key("format").value("EDIFACT")
                    .key("segments").beginArray();
            walk(section, firstSegment, delimiters,
                    new SegmentWriter(section, delimiters, charset, writer));
            writer.endArray().endObject();
        });
    }

    /**
     * Reads the section's segments, elements and components in order, from a
     * given position to the end, and gives each to {@code sink}. It keeps
     * nothing itself, so it can be run once to check the structure and again
     * to write the JSON.
     * <p>
     * A release character makes the character after it part of the value,
     * even if it is a separator. Line breaks between segments are skipped.
     *
     * @param section   the section's bytes
     * @param position  where the first segment to read starts
     * @param delimiters the interchange's separators
     * @param sink      what receives each component, element end and segment
     *                  end
     * @throws ConversionException {@code CONVERT_EDIFACT_MALFORMED} if the
     *                             section ends in a release character with
     *                             nothing after it (at the release
     *                             character), or its last segment has no
     *                             terminator (at the start of its last
     *                             component); or whatever {@code sink}
     *                             throws
     */
    private static void walk(byte[] section, int position,
            Delimiters delimiters, Sink sink) throws ConversionException
    {
        while (position < section.length)
        {
            // Where the reader is in this segment.
            int element = 0;
            int component = 0;
            // Where the current component starts.
            int componentStart = position;
            // Whether the current component holds a release character.
            boolean released = false;
            // Whether the segment's terminator has been found.
            boolean terminated = false;
            while (position < section.length)
            {
                byte current = section[position++];
                if (current == delimiters.release())
                {
                    if (position >= section.length)
                    {
                        throw new ConversionException(
                                ErrorCode.CONVERT_EDIFACT_MALFORMED,
                                position - 1);
                    }
                    // Skip the released character: it is part of the value.
                    position++;
                    released = true;
                }
                else if (current == delimiters.component()
                        || current == delimiters.element()
                        || current == delimiters.terminator())
                {
                    sink.component(element, component, componentStart,
                            position - 1, released);
                    released = false;
                    componentStart = position;
                    if (current == delimiters.component())
                    {
                        component++;
                    }
                    else
                    {
                        sink.endElement(element);
                        element++;
                        component = 0;
                    }
                    if (current == delimiters.terminator())
                    {
                        terminated = true;
                        break;
                    }
                }
            }
            if (!terminated)
            {
                throw new ConversionException(
                        ErrorCode.CONVERT_EDIFACT_MALFORMED, componentStart);
            }
            sink.endSegment();
            position = skipBreaks(section, position);
        }
    }

    /**
     * Copies a component's bytes with its release characters taken out, so
     * {@code ?+} becomes {@code +} and {@code ??} becomes {@code ?}.
     * <p>
     * The result goes into a buffer the caller keeps and passes in each time,
     * so most components need no new array. The buffer is held in a
     * one-element array, so that this method can replace it with a bigger one
     * when a component doesn't fit.
     *
     * @param section the section's bytes
     * @param from    where the component starts
     * @param to      where it ends: the position of the separator after it
     * @param release the release character
     * @param scratch holds the buffer, in {@code scratch[0]}; the buffer is
     *                replaced by a bigger one if the component doesn't fit
     * @return how many bytes of {@code scratch[0]} hold the result
     */
    private static int unreleased(byte[] section, int from, int to,
            byte release, byte[][] scratch)
    {
        if (scratch[0].length < to - from)
        {
            scratch[0] = new byte[to - from];
        }
        byte[] result = scratch[0];
        // How many bytes of the result have been written.
        int length = 0;
        for (int position = from; position < to; position++)
        {
            if (section[position] == release)
            {
                // Keep the character after the release character instead.
                position++;
            }
            result[length++] = section[position];
        }
        return length;
    }

    /**
     * Finds where a byte of a component's copy without release characters
     * ({@link #unreleased}) came from in the section. Used to report an
     * invalid byte at its real position.
     * <p>
     * It walks the component the same way {@link #unreleased} does, counting
     * the bytes it would copy: a release character is skipped and the byte
     * after it is the one copied.
     *
     * @param section   the section's bytes
     * @param from      where the component starts
     * @param to        where it ends: the position of the separator after it
     * @param release   the release character
     * @param copyIndex the byte's position in the copy, from 0
     * @return the byte's position in the section; {@code to} if the copy has
     *         no byte at {@code copyIndex}, as when an error is found at the
     *         very end of the copy
     */
    private static int sectionPosition(byte[] section, int from, int to,
            byte release, long copyIndex)
    {
        // How many bytes of the copy come before the current position.
        long copied = 0;
        for (int position = from; position < to; position++)
        {
            if (section[position] == release)
            {
                // The byte copied is the one after the release character.
                position++;
            }
            if (copied == copyIndex)
            {
                return position;
            }
            copied++;
        }
        return to;
    }

    /**
     * The first reading of a section: checks its structure (through
     * {@link #walk}, which does the checking) and records what the first
     * segment says. That is all {@link #convert} needs before it can write
     * the JSON: whether the first segment is {@code UNB} with at least one
     * element, and the syntax identifier that names the character set.
     * <p>
     * It looks only at the first segment's tag and the first component of its
     * first element, and only counts the rest.
     */
    private static final class Header implements Sink
    {

        /** The section's bytes. */
        private final byte[] section;
        /** The interchange's separators. */
        private final Delimiters delimiters;
        /**
         * Holds the buffer {@link #unreleased} copies into, in
         * {@code scratch[0]}. Starts small: only the tag and syntax
         * identifier are ever copied.
         */
        private final byte[][] scratch =
        {
            new byte[16]
        };
        /** How many segments have been read. */
        int segmentCount;
        /**
         * How many elements the first segment has, counting its tag: at least
         * 2 for a {@code UNB} segment with a syntax identifier.
         */
        int elementCount;
        /** The first segment's tag, which must be {@code "UNB"}. */
        String tag;
        /** The syntax identifier code, such as {@code "UNOC"}. */
        String syntax;
        /**
         * Where the syntax identifier starts in the section, to report if its
         * character set isn't supported.
         */
        int syntaxPosition;

        /**
         * Creates the first reading for a section.
         *
         * @param section    the section's bytes
         * @param delimiters the interchange's separators
         */
        Header(byte[] section, Delimiters delimiters)
        {
            this.section = section;
            this.delimiters = delimiters;
        }

        /**
         * Records the first segment's tag and syntax identifier, and ignores
         * every other component.
         * <p>
         * The tag is decoded as ISO 8859-1, in which every byte is a
         * character, so even a broken tag gives a string to compare (and
         * fail). The syntax identifier is decoded as ASCII, which every
         * supported code is.
         */
        @Override
        public void component(int element, int component, int from, int to,
                boolean released)
        {
            if (segmentCount > 0 || component > 0 || element > 1)
            {
                return;
            }
            String text = new String(raw(from, to, released),
                    element == 0 ? StandardCharsets.ISO_8859_1
                            : StandardCharsets.US_ASCII);
            if (element == 0)
            {
                tag = text;
            }
            else
            {
                syntax = text;
                syntaxPosition = from;
            }
        }

        /**
         * A component's bytes, in an array of their own, with any release
         * characters taken out.
         *
         * @param from     where the component starts
         * @param to       where it ends
         * @param released whether it holds a release character
         * @return the component's bytes
         */
        private byte[] raw(int from, int to, boolean released)
        {
            if (!released)
            {
                return Arrays.copyOfRange(section, from, to);
            }
            int length = unreleased(section, from, to, delimiters.release(),
                    scratch);
            return Arrays.copyOf(scratch[0], length);
        }

        /** Counts the first segment's elements. */
        @Override
        public void endElement(int element)
        {
            if (segmentCount == 0)
            {
                elementCount = element + 1;
            }
        }

        /** Counts the segments. */
        @Override
        public void endSegment()
        {
            segmentCount++;
        }
    }

    /**
     * The second reading of a section: writes each segment as
     * {@code {"segment_id":…,"elements":[[…],…]}}.
     * <p>
     * Of the tag (element 0), only its first component is written, as
     * {@code segment_id}. Every component of every other element is decoded
     * and written, each element as an array of strings.
     */
    private static final class SegmentWriter implements Sink
    {

        /** The section's bytes. */
        private final byte[] section;
        /** The interchange's separators. */
        private final Delimiters delimiters;
        /** The character set the interchange declares. */
        private final Charset charset;
        /** Where the JSON is written. */
        private final JsonWriter writer;
        /**
         * Holds the buffer {@link #unreleased} copies into, in
         * {@code scratch[0]}. It grows if a component with release characters
         * is bigger.
         */
        private final byte[][] scratch =
        {
            new byte[256]
        };

        /**
         * Creates the second reading for a section.
         *
         * @param section    the section's bytes
         * @param delimiters the interchange's separators
         * @param charset    the character set to decode components with
         * @param writer     where to write the JSON
         */
        SegmentWriter(byte[] section, Delimiters delimiters, Charset charset,
                JsonWriter writer)
        {
            this.section = section;
            this.delimiters = delimiters;
            this.charset = charset;
            this.writer = writer;
        }

        /**
         * Writes one component. The tag's first component starts the
         * segment's object, with the tag as {@code segment_id}, and opens its
         * {@code elements} array. The first component of any other element
         * opens that element's array. Other components of the tag are
         * skipped.
         *
         * @throws ConversionException {@code CONVERT_INVALID_ENCODING} if the
         *                             component's bytes aren't valid in the
         *                             character set
         */
        @Override
        public void component(int element, int component, int from, int to,
                boolean released)
                throws ConversionException
        {
            if (element == 0)
            {
                if (component == 0)
                {
                    writer.beginObject().key("segment_id").value(decode(from,
                            to, released)).key("elements")
                            .beginArray();
                }
                return;
            }
            if (component == 0)
            {
                writer.beginArray();
            }
            writer.value(decode(from, to, released));
        }

        /**
         * Decodes a component into text, with any release characters taken
         * out.
         * <p>
         * An invalid byte is reported at its position in the section. A
         * component with release characters is decoded from a copy without
         * them, so the position found in the copy is mapped back to the
         * section ({@link #sectionPosition}).
         *
         * @param from     where the component starts
         * @param to       where it ends
         * @param released whether it holds a release character
         * @return the component's text
         * @throws ConversionException {@code CONVERT_INVALID_ENCODING} if its
         *                             bytes aren't valid in the character set
         */
        private String decode(int from, int to, boolean released) throws
                ConversionException
        {
            if (!released)
            {
                // Text.decode reports positions in the section already.
                return Text.decode(section, from, to - from, charset);
            }
            int length = unreleased(section, from, to, delimiters.release(),
                    scratch);
            try
            {
                return Text.decode(scratch[0], 0, length, charset);
            }
            catch (ConversionException error)
            {
                throw new ConversionException(error.code(),
                        sectionPosition(section, from, to,
                                delimiters.release(), error.position()));
            }
        }

        /**
         * Closes an element's array. The tag (element 0) has none of its own:
         * its end leaves the segment's {@code elements} array open for the
         * elements that follow.
         */
        @Override
        public void endElement(int element)
        {
            if (element > 0)
            {
                writer.endArray();
            }
        }

        /** Closes the segment's {@code elements} array and its object. */
        @Override
        public void endSegment()
        {
            writer.endArray().endObject();
        }
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
