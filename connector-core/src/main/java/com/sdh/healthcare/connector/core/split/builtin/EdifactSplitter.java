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
import com.sdh.healthcare.connector.core.split.Section;
import com.sdh.healthcare.connector.core.split.SectionSplitter;
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
 * UN/EDIFACT splitter (design §6.2). Delimiters come from each
 * interchange's UNA service string advice, or the defaults
 * ({@code :+.? '}) without one; the release character ({@code ?}) is
 * honoured everywhere. Every section is a valid standalone interchange.
 * <p>
 * Boundary:
 * <ul>
 * <li>{@code UNH} (default): one message per section, as
 * {@code [UNA] UNB [UNG] UNH…UNT [UNE] UNZ}, with the UNE and UNZ counts
 * rewritten to 1</li>
 * <li>{@code UNG}: one functional group per section, as
 * {@code [UNA] UNB UNG…UNE UNZ}; messages outside a group are one section
 * each, as for {@code UNH}</li>
 * <li>{@code UNB}: one whole interchange per section, unchanged</li>
 * </ul>
 * Control references are checked (UNH/UNT 0062, UNG/UNE 0048, UNB/UNZ
 * 0020) to catch truncated or spliced input. Segment counts (UNT 0074)
 * aren't. Line breaks between segments are allowed.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class EdifactSplitter implements SectionSplitter
{

    /** The format's name. */
    public static final String FORMAT = "edifact";

    /** The start of a service string advice. */
    private static final byte[] UNA = "UNA".getBytes(ISO_8859_1);

    /** The longest service segment allowed, in bytes. */
    private static final int MAX_SERVICE_SEGMENT = 1024;

    /**
     * Creates the splitter; the service loader calls this.
     */
    public EdifactSplitter()
    {
    }

    /** What one section holds. */
    enum Level
    {
        /** One message. */
        UNH,

        /** One functional group. */
        UNG,

        /** One interchange. */
        UNB
    }

    /**
     * Service string advice; defaults per ISO 9735 when there is no UNA.
     *
     * @param component  the component separator
     * @param element    the element separator
     * @param release    the release (escape) character
     * @param terminator the segment terminator
     */
    record Delimiters(byte component, byte element, byte release,
            byte terminator)
    {

        /** The ISO 9735 defaults: {@code : + ? '}. */
        static final Delimiters DEFAULT = new Delimiters((byte) ':',
                (byte) '+', (byte) '?', (byte) '\'');
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
     * @throws IllegalArgumentException if the boundary isn't UNH, UNG or
     *                                  UNB
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
     * @return the level; UNH by default
     * @throws IllegalArgumentException if it isn't UNH, UNG or UNB
     */
    private static Level level(SplitConfig config)
    {
        return Level.valueOf(
                config.boundaryOr("UNH").toUpperCase(Locale.ROOT));
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
     * @param id      the segment tag, e.g. {@code UNH}
     * @param start   its source offset
     * @param end     the offset after its terminator and any line break
     * @param content the raw bytes before the terminator, for service
     *                segments; null for others
     */
    private record Segment(String id, long start, long end, byte[] content)
    {

        /**
         * First component of an element, with release characters removed.
         *
         * @param index      the element's index; 0 is the tag
         * @param delimiters the interchange's delimiters
         * @return the component; empty if there is no such element
         */
        String element(int index, Delimiters delimiters)
        {
            int elementIndex = 0;
            boolean inFirstComponent = true;
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(16);
            for (int position = 0; position < content.length; position++)
            {
                byte value = content[position];
                if (value == delimiters.release()
                        && position + 1 < content.length)
                {
                    if (elementIndex == index && inFirstComponent)
                    {
                        bytes.write(content[position + 1]);
                    }
                    position++;
                }
                else if (value == delimiters.element())
                {
                    if (elementIndex == index)
                    {
                        break;
                    }
                    elementIndex++;
                    inFirstComponent = true;
                }
                else if (value == delimiters.component())
                {
                    inFirstComponent = false;
                }
                else if (elementIndex == index && inFirstComponent)
                {
                    bytes.write(value);
                }
            }
            return bytes.toString(ISO_8859_1);
        }
    }

    /**
     * An open interchange or group.
     *
     * @param start   its source offset
     * @param raw     its bytes: UNA, if any, through UNB for an
     *                interchange; the UNG for a group
     * @param control its control reference
     */
    private record Envelope(long start, byte[] raw, String control)
    {
    }

    /** Splits one file. */
    private static final class Session implements SplitSession
    {

        /** The file. */
        private final SeekableByteSource source;

        /** What one section holds. */
        private final Level level;

        /** The split settings. */
        private final SplitConfig config;

        /** The largest section allowed, in bytes. */
        private final long max;

        /** Reads the file. */
        private ByteScanner scanner;

        /** The current interchange's delimiters. */
        private Delimiters delimiters = Delimiters.DEFAULT;

        /** The line break after the UNB's terminator, if any. */
        private byte[] eol = new byte[0];

        /** The open interchange; null between interchanges. */
        private Envelope unb;

        /** The open functional group; null outside a group. */
        private Envelope ung;

        /**
         * Creates a session.
         *
         * @param source the file
         * @param level  what one section holds
         * @param config the split settings
         */
        Session(SeekableByteSource source, Level level, SplitConfig config)
        {
            this.source = source;
            this.level = level;
            this.config = config;
            this.max = config.limits().maxSectionBytes();
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
            int allowed = switch (level)
            {
                case UNB ->
                    0;
                case UNG ->
                    1;
                case UNH ->
                    2;
            };
            if (context.size() > allowed)
            {
                throw new SplitException(ErrorCode.RESUME_INVALID,
                        from.offset());
            }
            if (!context.isEmpty())
            {
                scanner = new ByteScanner(source, context.get(0), 4096);
                Segment first = nextSegment();
                if (first == null || first.start() != context.get(0))
                {
                    throw new SplitException(ErrorCode.RESUME_INVALID,
                            from.offset());
                }
                unb = interchange(first);
                if (unb == null)
                {
                    throw new SplitException(ErrorCode.RESUME_INVALID,
                            from.offset());
                }
                if (context.size() == 2)
                {
                    scanner = new ByteScanner(source, context.get(1), 4096);
                    Segment group = nextSegment();
                    if (group == null || !group.id().equals("UNG")
                            || group.start() != context.get(1))
                    {
                        throw new SplitException(ErrorCode.RESUME_INVALID,
                                from.offset());
                    }
                    ung = envelope(group, group.start(), 5);
                }
            }
            scanner = new ByteScanner(source, from.offset(),
                    config.readBufferBytes());
        }

        /**
         * Opens an interchange from its first segment (UNA or UNB).
         *
         * @param first the first segment
         * @return the interchange; null if it isn't one
         * @throws IOException    if the file can't be read
         * @throws SplitException if the next segment is malformed
         */
        private Envelope interchange(Segment first)
                throws IOException, SplitException
        {
            Segment segment = first;
            if (segment.id().equals("UNA"))
            {
                segment = nextSegment();
            }
            else
            {
                delimiters = Delimiters.DEFAULT;
            }
            if (segment == null || !segment.id().equals("UNB"))
            {
                return null;
            }
            return envelope(segment, first.start(), 5);
        }

        /**
         * Reads an envelope's bytes.
         *
         * @param segment        its opening segment
         * @param from           its source offset (the UNA, if any)
         * @param controlElement the index of its control reference
         * @return the envelope
         * @throws IOException if the file can't be read
         */
        private Envelope envelope(Segment segment, long from,
                int controlElement) throws IOException
        {
            byte[] raw = source.readFully(from, (int) (segment.end() - from));
            return new Envelope(from, raw,
                    segment.element(controlElement, delimiters));
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
            if (scanner.lookingAt(UNA))
            {
                return readUna(start);
            }
            StringBuilder id = new StringBuilder(3);
            int next;
            while ((next = scanner.peek()) >= 0
                    && next != (delimiters.element() & 0xff)
                    && next != (delimiters.terminator() & 0xff))
            {
                if (id.length() == 3)
                {
                    throw new SplitException(
                            ErrorCode.PARSE_EDIFACT_UNEXPECTED_SEGMENT, start);
                }
                id.append((char) scanner.read());
            }
            String segmentId = id.toString();
            boolean service = segmentId.startsWith("UN");
            ByteArrayOutputStream bytes =
                    service ? new ByteArrayOutputStream(64) : null;
            if (bytes != null)
            {
                bytes.writeBytes(segmentId.getBytes(ISO_8859_1));
            }
            boolean terminated = false;
            while ((next = scanner.read()) >= 0)
            {
                if (next == (delimiters.release() & 0xff))
                {
                    int escaped = scanner.read();
                    if (escaped < 0)
                    {
                        break;
                    }
                    if (bytes != null)
                    {
                        bytes.write(next);
                        bytes.write(escaped);
                    }
                    continue;
                }
                if (next == (delimiters.terminator() & 0xff))
                {
                    terminated = true;
                    break;
                }
                if (bytes != null)
                {
                    if (bytes.size() >= MAX_SERVICE_SEGMENT)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_EDIFACT_INVALID_ENVELOPE,
                                start);
                    }
                    bytes.write(next);
                }
            }
            if (!terminated)
            {
                throw new SplitException(ErrorCode.PARSE_EDIFACT_UNBALANCED,
                        start);
            }
            long afterTerminator = scanner.position();
            skipLineBreaks();
            if (segmentId.equals("UNB"))
            {
                eol = source.readFully(afterTerminator, (int) Math.min(2,
                        scanner.position() - afterTerminator));
            }
            return new Segment(segmentId, start, scanner.position(),
                    bytes == null ? null : bytes.toByteArray());
        }

        /**
         * Reads a UNA: the tag plus six service characters (component,
         * element, decimal mark, release, reserved, terminator). Sets the
         * delimiters.
         *
         * @param start its source offset
         * @return the segment
         * @throws IOException    if the file can't be read
         * @throws SplitException if it is short or its delimiters clash
         */
        private Segment readUna(long start)
                throws IOException, SplitException
        {
            byte[] raw = new byte[9];
            for (int index = 0; index < 9; index++)
            {
                int next = scanner.read();
                if (next < 0)
                {
                    throw new SplitException(
                            ErrorCode.PARSE_EDIFACT_INVALID_ENVELOPE, start);
                }
                raw[index] = (byte) next;
            }
            delimiters = new Delimiters(raw[3], raw[4], raw[6], raw[8]);
            if (delimiters.component() == delimiters.element()
                    || delimiters.element() == delimiters.terminator()
                    || delimiters.component() == delimiters.terminator())
            {
                throw new SplitException(
                        ErrorCode.PARSE_EDIFACT_INVALID_ENVELOPE, start);
            }
            skipLineBreaks();
            return new Segment("UNA", start, scanner.position(), raw);
        }

        // ---- structure ----

        /**
         * {@inheritDoc}
         *
         * @throws SplitException if the envelopes don't balance, a control
         *                        reference doesn't match, or a section is
         *                        too large
         */
        @Override
        public Optional<Section> next() throws IOException, SplitException
        {
            while (true)
            {
                Segment segment = nextSegment();
                if (segment == null)
                {
                    if (unb != null)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_EDIFACT_UNBALANCED,
                                scanner.position());
                    }
                    return Optional.empty();
                }
                if (unb == null)
                {
                    long start = segment.start();
                    Envelope opened = interchange(segment);
                    if (opened == null)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_EDIFACT_INVALID_ENVELOPE,
                                start);
                    }
                    if (level == Level.UNB)
                    {
                        Segment unz = scanUntil(start, "UNZ",
                                Set.of("UNA", "UNB"));
                        checkControl(unz, 2, opened.control());
                        delimiters = Delimiters.DEFAULT;
                        return Optional.of(Section.ofRange(start,
                                unz.end() - start,
                                ResumePoint.at(unz.end())));
                    }
                    unb = opened;
                    continue;
                }
                if (ung == null)
                {
                    switch (segment.id())
                    {
                        case "UNZ" ->
                        {
                            checkControl(segment, 2, unb.control());
                            unb = null;
                            // The next interchange has its own UNA, or the
                            // defaults.
                            delimiters = Delimiters.DEFAULT;
                        }
                        case "UNG" ->
                        {
                            if (level == Level.UNG)
                            {
                                Segment une = scanUntil(segment.start(),
                                        "UNE", Set.of("UNA", "UNB", "UNZ",
                                                "UNG"));
                                checkControl(une, 2,
                                        segment.element(5, delimiters));
                                return Optional.of(section(segment.start(),
                                        une.end(), unb.raw(), unz(),
                                        ResumePoint.at(une.end(),
                                                unb.start())));
                            }
                            ung = envelope(segment, segment.start(), 5);
                        }
                        case "UNH" ->
                        {
                            Segment unt = scanUntil(segment.start(), "UNT",
                                    Set.of("UNA", "UNB", "UNZ", "UNG", "UNE",
                                            "UNH"));
                            checkControl(unt, 2,
                                    segment.element(1, delimiters));
                            return Optional.of(section(segment.start(),
                                    unt.end(), unb.raw(), unz(),
                                    ResumePoint.at(unt.end(), unb.start())));
                        }
                        default ->
                            throw new SplitException(
                                    ErrorCode.PARSE_EDIFACT_UNEXPECTED_SEGMENT,
                                    segment.start());
                    }
                    continue;
                }
                switch (segment.id())
                {
                    case "UNE" ->
                    {
                        checkControl(segment, 2, ung.control());
                        ung = null;
                    }
                    case "UNH" ->
                    {
                        Segment unt = scanUntil(segment.start(), "UNT",
                                Set.of("UNA", "UNB", "UNZ", "UNG", "UNE",
                                        "UNH"));
                        checkControl(unt, 2, segment.element(1, delimiters));
                        return Optional.of(section(segment.start(),
                                unt.end(), concat(unb.raw(), ung.raw()),
                                concat(segment("UNE", "1", ung.control()),
                                        unz()),
                                ResumePoint.at(unt.end(), unb.start(),
                                        ung.start())));
                    }
                    default ->
                        throw new SplitException(
                                ErrorCode.PARSE_EDIFACT_UNEXPECTED_SEGMENT,
                                segment.start());
                }
            }
        }

        /**
         * Reads segments until {@code endId}; any of {@code forbidden}
         * first means unbalanced.
         *
         * @param from      the source offset of the opening segment
         * @param endId     the closing segment's tag
         * @param forbidden tags that may not come before it
         * @return the closing segment
         * @throws IOException    if the file can't be read
         * @throws SplitException if it is unbalanced or too large
         */
        private Segment scanUntil(long from, String endId,
                Set<String> forbidden) throws IOException, SplitException
        {
            while (true)
            {
                Segment segment = nextSegment();
                if (segment == null || forbidden.contains(segment.id()))
                {
                    throw new SplitException(
                            ErrorCode.PARSE_EDIFACT_UNBALANCED, from);
                }
                if (segment.end() - from > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_SECTION_SIZE,
                            from);
                }
                if (segment.id().equals(endId))
                {
                    return segment;
                }
            }
        }

        /**
         * Checks a closing segment's control reference.
         *
         * @param segment  the closing segment
         * @param element  the index of its control reference
         * @param expected the opening segment's control reference
         * @throws SplitException if they differ
         */
        private void checkControl(Segment segment, int element,
                String expected) throws SplitException
        {
            if (!segment.element(element, delimiters).equals(expected))
            {
                throw new SplitException(
                        ErrorCode.PARSE_EDIFACT_CONTROL_MISMATCH,
                        segment.start());
            }
        }

        /**
         * A UNZ closing the open interchange, with a count of 1.
         *
         * @return the segment's bytes
         */
        private byte[] unz()
        {
            return segment("UNZ", "1", unb.control());
        }

        /**
         * A synthesised service segment; its values are control references,
         * which need no escaping.
         *
         * @param elements the elements, the tag first
         * @return the segment's bytes, with the terminator and line break
         */
        private byte[] segment(String... elements)
        {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(32);
            for (int index = 0; index < elements.length; index++)
            {
                if (index > 0)
                {
                    bytes.write(delimiters.element());
                }
                bytes.writeBytes(elements[index].getBytes(ISO_8859_1));
            }
            bytes.write(delimiters.terminator());
            bytes.writeBytes(eol);
            return bytes.toByteArray();
        }

        /**
         * A section of a source range between a prefix and a suffix.
         *
         * @param start  the range's source offset
         * @param end    the offset after it
         * @param prefix bytes before it
         * @param suffix bytes after it
         * @param resume where splitting continues
         * @return the section
         */
        private static Section section(long start, long end, byte[] prefix,
                byte[] suffix, ResumePoint resume)
        {
            List<Section.Part> parts = new ArrayList<>(3);
            parts.add(new Section.Literal(prefix));
            parts.add(new Section.SourceRange(start, end - start));
            parts.add(new Section.Literal(suffix));
            return new Section(start, end - start, parts, resume);
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
