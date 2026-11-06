/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

import com.sdh.healthcare.connector.core.split.ByteScanner;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.SplitRecord;
import com.sdh.healthcare.connector.core.split.RecordSplitter;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.split.SplitSession;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * X12 splitter (design §6.2). Delimiters are read from each ISA segment,
 * and every record is a valid standalone interchange.
 * <p>
 * Boundary:
 * <ul>
 * <li>{@code ST} (default): one transaction set per record, as
 * {@code ISA + GS + ST…SE + GE + IEA}, with GE01 and IEA01 rewritten to
 * 1</li>
 * <li>{@code GS}: one functional group per record, as
 * {@code ISA + GS…GE + IEA}, with IEA01 rewritten to 1</li>
 * <li>{@code ISA}: one whole interchange per record, unchanged</li>
 * </ul>
 * A TA1 acknowledgement between ISA and GS becomes its own section,
 * {@code ISA + TA1 + IEA} with IEA01 = 0. Control numbers are checked
 * (ST02/SE02, GS06/GE02, ISA13/IEA02) to catch truncated or spliced input.
 * BIN segments carrying raw binary are not supported.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class X12Splitter implements RecordSplitter
{

    /** The format's name. */
    public static final String FORMAT = "x12";

    /** The start of an interchange. */
    private static final byte[] ISA = "ISA".getBytes(ISO_8859_1);

    /** The envelope segments, whose content is kept. */
    private static final Set<String> ENVELOPE = Set.of("ISA", "IEA", "GS",
            "GE", "ST", "SE", "TA1");

    /** The longest envelope segment allowed, in bytes. */
    private static final int MAX_ENVELOPE_SEGMENT = 1024;

    /**
     * Creates the splitter; the service loader calls this.
     */
    public X12Splitter()
    {
    }

    /** What one record holds. */
    enum Level
    {
        /** One transaction set. */
        ST,

        /** One functional group. */
        GS,

        /** One interchange. */
        ISA
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String format()
    {
        return FORMAT;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the boundary isn't ST, GS or ISA
     */
    @Override
    public void validate(SplitConfig config)
    {
        level(config);
    }

    /**
     * The configured boundary.
     *
     * @param config the split settings
     * @return the level; ST by default
     * @throws IllegalArgumentException if it isn't ST, GS or ISA
     */
    private static Level level(SplitConfig config)
    {
        return Level.valueOf(
                config.boundaryOr("ST").toUpperCase(Locale.ROOT));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SplitSession open(SeekableByteSource source, ResumePoint from,
            SplitConfig config)
            throws IOException, SplitException
    {
        Session session = new Session(source, level(config), config);
        session.resume(from);
        return session;
    }

    /**
     * One segment.
     *
     * @param id      the segment ID, e.g. {@code ST}
     * @param start   its source offset
     * @param end     the offset after the terminator and any CR/LF
     *                following it
     * @param content its bytes, without the terminator, for envelope
     *                segments; null for others
     */
    private record Segment(String id, long start, long end, byte[] content)
    {

        /**
         * One element of an envelope segment.
         *
         * @param index     the element's index; 0 is the segment ID
         * @param separator the element separator
         * @return the element, trimmed; empty if there is no such element
         */
        String element(int index, byte separator)
        {
            int field = 0;
            int from = 0;
            for (int position = 0; position <= content.length; position++)
            {
                if (position == content.length
                        || content[position] == separator)
                {
                    if (field == index)
                    {
                        return new String(content, from, position - from,
                                ISO_8859_1).trim();
                    }
                    field++;
                    from = position + 1;
                }
            }
            return "";
        }
    }

    /**
     * An open ISA or GS.
     *
     * @param start   its source offset
     * @param raw     its bytes, for the prefix of a record
     * @param control its control number
     */
    private record Envelope(long start, byte[] raw, String control)
    {
    }

    /** Splits one file. */
    private static final class Session implements SplitSession
    {

        /** The file. */
        private final SeekableByteSource source;

        /** What one record holds. */
        private final Level level;

        /** The split settings. */
        private final SplitConfig config;

        /** The largest record allowed, in bytes. */
        private final long max;

        /** Reads the file. */
        private ByteScanner scanner;

        /** The element separator, from the ISA. */
        private byte elementSeparator;

        /** The segment terminator, from the ISA; 0 before the first ISA. */
        private byte terminator;

        /** The line break after the ISA's terminator, if any. */
        private byte[] eol = new byte[0];

        /** The open interchange; null between interchanges. */
        private Envelope isa;

        /** The open functional group; null between groups. */
        private Envelope gs;

        /**
         * Creates a session.
         *
         * @param source the file
         * @param level  what one record holds
         * @param config the split settings
         */
        Session(SeekableByteSource source, Level level, SplitConfig config)
        {
            this.source = source;
            this.level = level;
            this.config = config;
            this.max = config.limits().maxRecordBytes();
        }

        /**
         * Positions the session at a resume point, reading its open
         * envelopes again.
         *
         * @param from where to resume
         * @throws IOException    if the file can't be read
         * @throws SplitException if the point doesn't fit the file
         */
        void resume(ResumePoint from) throws IOException, SplitException
        {
            List<Long> context = from.context();
            int expected = switch (level)
            {
                case ISA ->
                    0;
                case GS ->
                    1;
                case ST ->
                    2;
            };
            if (context.size() > expected)
            {
                throw new SplitException(ErrorCode.RESUME_INVALID,
                        from.offset());
            }
            if (!context.isEmpty())
            {
                scanner = new ByteScanner(source, context.get(0), 4096);
                Segment interchange = nextSegment();
                if (interchange == null || !interchange.id().equals("ISA")
                        || interchange.start() != context.get(0))
                {
                    throw new SplitException(ErrorCode.RESUME_INVALID,
                            from.offset());
                }
                isa = envelope(interchange, 13);
                if (context.size() == 2)
                {
                    scanner = new ByteScanner(source, context.get(1), 4096);
                    Segment group = nextSegment();
                    if (group == null || !group.id().equals("GS")
                            || group.start() != context.get(1))
                    {
                        throw new SplitException(ErrorCode.RESUME_INVALID,
                                from.offset());
                    }
                    gs = envelope(group, 6);
                }
            }
            scanner = new ByteScanner(source, from.offset(),
                    config.readBufferBytes());
        }

        /**
         * Reads an envelope segment's bytes.
         *
         * @param segment        the segment
         * @param controlElement the index of its control number
         * @return the envelope
         * @throws IOException if the file can't be read
         */
        private Envelope envelope(Segment segment, int controlElement)
                throws IOException
        {
            byte[] raw = source.readFully(segment.start(),
                    (int) (segment.end() - segment.start()));
            return new Envelope(segment.start(), raw,
                    segment.element(controlElement, elementSeparator));
        }

        // ---- segment reading ----

        /**
         * Skips any CR and LF bytes.
         *
         * @throws IOException if the file can't be read
         */
        private void skipLineBreaks() throws IOException
        {
            int next;
            while ((next = scanner.peek()) == '\r' || next == '\n')
            {
                scanner.read();
            }
        }

        /**
         * Reads the next segment.
         *
         * @return the segment, or null at the end
         * @throws IOException    if the file can't be read
         * @throws SplitException if the segment is malformed or unended
         */
        private Segment nextSegment() throws IOException, SplitException
        {
            skipLineBreaks();
            long start = scanner.position();
            if (scanner.peek() < 0)
            {
                return null;
            }
            if (scanner.lookingAt(ISA)
                    && !Character.isLetterOrDigit(scanner.peek(3)))
            {
                return readIsa(start);
            }
            if (terminator == 0)
            {
                throw new SplitException(ErrorCode.PARSE_X12_INVALID_ENVELOPE,
                        start);
            }
            StringBuilder id = new StringBuilder(3);
            int next;
            while ((next = scanner.peek()) >= 0
                    && next != (elementSeparator & 0xff)
                    && next != (terminator & 0xff))
            {
                if (id.length() == 3)
                {
                    throw new SplitException(
                            ErrorCode.PARSE_X12_UNEXPECTED_SEGMENT, start);
                }
                id.append((char) scanner.read());
            }
            String segmentId = id.toString();
            byte[] content = null;
            if (ENVELOPE.contains(segmentId))
            {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream(64);
                bytes.writeBytes(segmentId.getBytes(ISO_8859_1));
                while ((next = scanner.read()) >= 0
                        && next != (terminator & 0xff))
                {
                    if (bytes.size() >= MAX_ENVELOPE_SEGMENT)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_X12_INVALID_ENVELOPE, start);
                    }
                    bytes.write(next);
                }
                content = bytes.toByteArray();
            }
            else if (scanner.skipTo(terminator))
            {
                next = scanner.read();
            }
            else
            {
                next = -1;
            }
            if (next < 0)
            {
                throw new SplitException(ErrorCode.PARSE_X12_UNBALANCED,
                        start);
            }
            skipLineBreaks();
            return new Segment(segmentId, start, scanner.position(), content);
        }

        /**
         * Reads an ISA, which is fixed-format: element separator at byte 3,
         * terminator right after ISA16. Sets the delimiters.
         *
         * @param start its source offset
         * @return the segment
         * @throws IOException    if the file can't be read
         * @throws SplitException if it is malformed
         */
        private Segment readIsa(long start)
                throws IOException, SplitException
        {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(110);
            for (int index = 0; index < 3; index++)
            {
                bytes.write(scanner.read());
            }
            int separator = scanner.read();
            bytes.write(separator);
            int separators = 1;
            while (separators < 16)
            {
                int next = scanner.read();
                if (next < 0 || bytes.size() > 200)
                {
                    throw new SplitException(
                            ErrorCode.PARSE_X12_INVALID_ENVELOPE, start);
                }
                bytes.write(next);
                if (next == separator)
                {
                    separators++;
                }
            }
            int component = scanner.read();
            int segmentEnd = scanner.read();
            if (component < 0 || segmentEnd < 0 || segmentEnd == separator
                    || Character.isLetterOrDigit(segmentEnd))
            {
                throw new SplitException(ErrorCode.PARSE_X12_INVALID_ENVELOPE,
                        start);
            }
            bytes.write(component);
            elementSeparator = (byte) separator;
            terminator = (byte) segmentEnd;
            long afterTerminator = scanner.position();
            skipLineBreaks();
            eol = source.readFully(afterTerminator, (int) Math.min(2,
                    scanner.position() - afterTerminator));
            return new Segment("ISA", start, scanner.position(),
                    bytes.toByteArray());
        }

        // ---- structure ----

        /**
         * {@inheritDoc}
         *
         * @throws SplitException if the envelopes don't balance, a control
         *                        number doesn't match, or a record is too
         *                        large
         */
        @Override
        public Optional<SplitRecord> next() throws IOException, SplitException
        {
            while (true)
            {
                Segment segment = nextSegment();
                if (segment == null)
                {
                    if (isa != null)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_X12_UNBALANCED,
                                scanner.position());
                    }
                    return Optional.empty();
                }
                if (isa == null)
                {
                    if (!segment.id().equals("ISA"))
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_X12_INVALID_ENVELOPE,
                                segment.start());
                    }
                    if (level == Level.ISA)
                    {
                        Segment iea = scanUntil(segment, "IEA",
                                Set.of("ISA"));
                        checkControl(iea, 2,
                                segment.element(13, elementSeparator));
                        return Optional.of(SplitRecord.ofRange(segment.start(),
                                iea.end() - segment.start(),
                                ResumePoint.at(iea.end())));
                    }
                    isa = envelope(segment, 13);
                    continue;
                }
                if (gs == null)
                {
                    switch (segment.id())
                    {
                        case "IEA" ->
                        {
                            checkControl(segment, 2, isa.control());
                            isa = null;
                        }
                        case "TA1" ->
                        {
                            return Optional.of(record(segment.start(),
                                    segment.end(), concat(isa.raw()),
                                    concat(iea(0)),
                                    ResumePoint.at(segment.end(),
                                            isa.start())));
                        }
                        case "GS" ->
                        {
                            if (level == Level.GS)
                            {
                                Segment ge = scanUntil(segment, "GE",
                                        Set.of("ISA", "IEA", "GS"));
                                checkControl(ge, 2,
                                        segment.element(6, elementSeparator));
                                return Optional.of(record(segment.start(),
                                        ge.end(), concat(isa.raw()),
                                        concat(iea(1)),
                                        ResumePoint.at(ge.end(),
                                                isa.start())));
                            }
                            gs = envelope(segment, 6);
                        }
                        default ->
                            throw new SplitException(
                                    ErrorCode.PARSE_X12_UNEXPECTED_SEGMENT,
                                    segment.start());
                    }
                    continue;
                }
                switch (segment.id())
                {
                    case "GE" ->
                    {
                        checkControl(segment, 2, gs.control());
                        gs = null;
                    }
                    case "ST" ->
                    {
                        Segment se = scanUntil(segment, "SE", Set.of("ISA",
                                "IEA", "GS", "GE", "ST"));
                        checkControl(se, 2,
                                segment.element(2, elementSeparator));
                        byte[] ge = segment("GE", "1", gs.control());
                        return Optional.of(record(segment.start(), se.end(),
                                concat(isa.raw(), gs.raw()),
                                concat(ge, iea(1)), ResumePoint.at(se.end(),
                                        isa.start(), gs.start())));
                    }
                    default ->
                        throw new SplitException(
                                ErrorCode.PARSE_X12_UNEXPECTED_SEGMENT,
                                segment.start());
                }
            }
        }

        /**
         * Reads segments after {@code first} until {@code endId}; any of
         * {@code forbidden} first means unbalanced.
         *
         * @param first     the opening segment
         * @param endId     the closing segment's ID
         * @param forbidden IDs that may not come before it
         * @return the closing segment
         * @throws IOException    if the file can't be read
         * @throws SplitException if it is unbalanced or too large
         */
        private Segment scanUntil(Segment first, String endId,
                Set<String> forbidden)
                throws IOException, SplitException
        {
            while (true)
            {
                Segment segment = nextSegment();
                if (segment == null || forbidden.contains(segment.id()))
                {
                    throw new SplitException(ErrorCode.PARSE_X12_UNBALANCED,
                            first.start());
                }
                if (segment.end() - first.start() > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_RECORD_SIZE,
                            first.start());
                }
                if (segment.id().equals(endId))
                {
                    return segment;
                }
            }
        }

        /**
         * Checks a closing segment's control number.
         *
         * @param segment  the closing segment
         * @param element  the index of its control number
         * @param expected the opening segment's control number
         * @throws SplitException if they differ
         */
        private void checkControl(Segment segment, int element,
                String expected) throws SplitException
        {
            if (!segment.element(element, elementSeparator).equals(expected))
            {
                throw new SplitException(ErrorCode.PARSE_X12_CONTROL_MISMATCH,
                        segment.start());
            }
        }

        /**
         * An IEA closing the open interchange.
         *
         * @param groups the group count to give
         * @return the segment's bytes
         */
        private byte[] iea(int groups)
        {
            return segment("IEA", Integer.toString(groups), isa.control());
        }

        /**
         * A segment made of elements, with this file's delimiters.
         *
         * @param elements the elements, the segment ID first
         * @return the segment's bytes, with the terminator and line break
         */
        private byte[] segment(String... elements)
        {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(32);
            for (int index = 0; index < elements.length; index++)
            {
                if (index > 0)
                {
                    bytes.write(elementSeparator);
                }
                bytes.writeBytes(elements[index].getBytes(ISO_8859_1));
            }
            bytes.write(terminator);
            bytes.writeBytes(eol);
            return bytes.toByteArray();
        }

        /**
         * A record of a source range between a prefix and a suffix.
         *
         * @param start  the range's source offset
         * @param end    the offset after it
         * @param prefix bytes before it
         * @param suffix bytes after it
         * @param resume where splitting continues
         * @return the record
         */
        private static SplitRecord record(long start, long end, byte[] prefix,
                byte[] suffix, ResumePoint resume)
        {
            List<SplitRecord.Part> parts = new ArrayList<>(3);
            parts.add(new SplitRecord.Literal(prefix));
            parts.add(new SplitRecord.SourceRange(start, end - start));
            parts.add(new SplitRecord.Literal(suffix));
            return new SplitRecord(start, end - start, parts, resume);
        }

        /**
         * Joins byte arrays.
         *
         * @param parts the arrays
         * @return them joined
         */
        private static byte[] concat(byte[]... parts)
        {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            for (byte[] part : parts)
            {
                bytes.writeBytes(part);
            }
            return bytes.toByteArray();
        }
    }
}
