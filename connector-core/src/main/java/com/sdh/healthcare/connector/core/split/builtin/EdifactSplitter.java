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
 * UN/EDIFACT splitter (design §6.2). Delimiters come from each interchange's
 * UNA service string advice, or the defaults ({@code :+.? '}) without one; the
 * release character ({@code ?}) is honoured everywhere. Every section is a
 * valid standalone interchange.
 *
 * <p>
 * Boundary:
 * <ul>
 * <li>{@code UNH} (default): one message per section, as
 * {@code [UNA] UNB [UNG] UNH…UNT [UNE] UNZ}, with the UNE and UNZ counts
 * rewritten to 1</li>
 * <li>{@code UNG}: one functional group per section, as
 * {@code [UNA] UNB UNG…UNE UNZ}; messages outside a group are one section each,
 * as for {@code UNH}</li>
 * <li>{@code UNB}: one whole interchange per section, unchanged</li>
 * </ul>
 * Control references are checked (UNH/UNT 0062, UNG/UNE 0048, UNB/UNZ 0020) to
 * catch truncated or spliced input. Segment counts (UNT 0074) aren't. Line
 * breaks between segments are allowed.
 */
public final class EdifactSplitter implements SectionSplitter
{

    public static final String FORMAT = "edifact";

    private static final byte[] UNA = "UNA".getBytes(ISO_8859_1);
    private static final int MAX_SERVICE_SEGMENT = 1024;

    enum Level
    {
        UNH, UNG, UNB
    }

    /**
     * Service string advice; defaults per ISO 9735 when there is no UNA.
     */
    record Delimiters(byte component, byte element, byte release,
            byte terminator)
    {

        static final Delimiters DEFAULT = new Delimiters((byte) ':', (byte) '+',
                (byte) '?', (byte) '\'');
    }

    @Override
    public String format()
    {
        return FORMAT;
    }

    @Override
    public void validate(SplitConfig cfg)
    {
        level(cfg);
    }

    private static Level level(SplitConfig cfg)
    {
        return Level.valueOf(cfg.boundaryOr("UNH").toUpperCase(Locale.ROOT));
    }

    @Override
    public SplitSession open(SeekableByteSource src, ResumePoint from,
            SplitConfig cfg)
            throws IOException, SplitException
    {
        Session s = new Session(src, level(cfg), cfg);
        s.resume(from);
        return s;
    }

    /**
     * One segment; {@code content} (service segments only) is the raw bytes
     * before the terminator.
     */
    private record Segment(String id, long start, long end, byte[] content)
    {

        /**
         * First component of element {@code i} (0 is the tag), with release
         * characters removed.
         */
        String element(int i, Delimiters d)
        {
            int el = 0;
            boolean inFirstComponent = true;
            ByteArrayOutputStream b = new ByteArrayOutputStream(16);
            for (int p = 0; p < content.length; p++)
            {
                byte c = content[p];
                if (c == d.release() && p + 1 < content.length)
                {
                    if (el == i && inFirstComponent)
                    {
                        b.write(content[p + 1]);
                    }
                    p++;
                } else if (c == d.element())
                {
                    if (el == i)
                    {
                        break;
                    }
                    el++;
                    inFirstComponent = true;
                } else if (c == d.component())
                {
                    inFirstComponent = false;
                } else if (el == i && inFirstComponent)
                {
                    b.write(c);
                }
            }
            return b.toString(ISO_8859_1);
        }
    }

    /**
     * An open interchange (raw: UNA, if any, through UNB) or group (raw: UNG),
     * and its control reference.
     */
    private record Envelope(long start, byte[] raw, String control) {}

    private static final class Session implements SplitSession
    {

        private final SeekableByteSource src;
        private final Level level;
        private final SplitConfig cfg;
        private final long max;
        private ByteScanner sc;
        private Delimiters d = Delimiters.DEFAULT;
        private byte[] eol = new byte[0];
        private Envelope unb;
        private Envelope ung;

        Session(SeekableByteSource src, Level level, SplitConfig cfg)
        {
            this.src = src;
            this.level = level;
            this.cfg = cfg;
            this.max = cfg.limits().maxSectionBytes();
        }

        void resume(ResumePoint from) throws IOException, SplitException
        {
            List<Long> ctx = from.context();
            int allowed = switch (level)
            {
                case UNB ->
                    0;
                case UNG ->
                    1;
                case UNH ->
                    2;
            };
            if (ctx.size() > allowed)
            {
                throw new SplitException(ErrorCode.RESUME_INVALID, from.offset());
            }
            if (!ctx.isEmpty())
            {
                sc = new ByteScanner(src, ctx.get(0), 4096);
                Segment first = nextSegment();
                if (first == null || first.start() != ctx.get(0))
                {
                    throw new SplitException(ErrorCode.RESUME_INVALID, from.
                            offset());
                }
                unb = interchange(first);
                if (unb == null)
                {
                    throw new SplitException(ErrorCode.RESUME_INVALID, from.
                            offset());
                }
                if (ctx.size() == 2)
                {
                    sc = new ByteScanner(src, ctx.get(1), 4096);
                    Segment g = nextSegment();
                    if (g == null || !g.id().equals("UNG") || g.start() != ctx.
                            get(1))
                    {
                        throw new SplitException(ErrorCode.RESUME_INVALID, from.
                                offset());
                    }
                    ung = envelope(g, g.start(), 5);
                }
            }
            sc = new ByteScanner(src, from.offset(), cfg.readBufferBytes());
        }

        /**
         * Opens an interchange from its first segment (UNA or UNB); null if it
         * isn't one.
         */
        private Envelope interchange(Segment first) throws IOException,
                SplitException
        {
            Segment s = first;
            if (s.id().equals("UNA"))
            {
                s = nextSegment();
            } else
            {
                d = Delimiters.DEFAULT;
            }
            if (s == null || !s.id().equals("UNB"))
            {
                return null;
            }
            return envelope(s, first.start(), 5);
        }

        private Envelope envelope(Segment s, long from, int controlElement)
                throws IOException
        {
            byte[] raw = src.readFully(from, (int) (s.end() - from));
            return new Envelope(from, raw, s.element(controlElement, d));
        }

        // ---- segment reading ----
        private void skipLineBreaks() throws IOException
        {
            int c;
            while ((c = sc.peek()) == '\r' || c == '\n')
            {
                sc.read();
            }
        }

        private Segment nextSegment() throws IOException, SplitException
        {
            skipLineBreaks();
            long start = sc.position();
            if (sc.peek() < 0)
            {
                return null;
            }
            if (sc.lookingAt(UNA))
            {
                return readUna(start);
            }
            StringBuilder id = new StringBuilder(3);
            int c;
            while ((c = sc.peek()) >= 0 && c != (d.element() & 0xff) && c != (d.
                    terminator() & 0xff))
            {
                if (id.length() == 3)
                {
                    throw new SplitException(
                            ErrorCode.PARSE_EDIFACT_UNEXPECTED_SEGMENT, start);
                }
                id.append((char) sc.read());
            }
            String sid = id.toString();
            boolean service = sid.startsWith("UN");
            ByteArrayOutputStream b = service ? new ByteArrayOutputStream(64) : null;
            if (b != null)
            {
                b.writeBytes(sid.getBytes(ISO_8859_1));
            }
            boolean terminated = false;
            while ((c = sc.read()) >= 0)
            {
                if (c == (d.release() & 0xff))
                {
                    int escaped = sc.read();
                    if (escaped < 0)
                    {
                        break;
                    }
                    if (b != null)
                    {
                        b.write(c);
                        b.write(escaped);
                    }
                    continue;
                }
                if (c == (d.terminator() & 0xff))
                {
                    terminated = true;
                    break;
                }
                if (b != null)
                {
                    if (b.size() >= MAX_SERVICE_SEGMENT)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_EDIFACT_INVALID_ENVELOPE, start);
                    }
                    b.write(c);
                }
            }
            if (!terminated)
            {
                throw new SplitException(ErrorCode.PARSE_EDIFACT_UNBALANCED,
                        start);
            }
            long afterTerm = sc.position();
            skipLineBreaks();
            if (sid.equals("UNB"))
            {
                eol = src.readFully(afterTerm, (int) Math.min(2,
                        sc.position() - afterTerm));
            }
            return new Segment(sid, start, sc.position(), b == null ? null : b.
                    toByteArray());
        }

        /**
         * UNA plus six service characters: component, element, decimal mark,
         * release, reserved, terminator.
         */
        private Segment readUna(long start) throws IOException, SplitException
        {
            byte[] raw = new byte[9];
            for (int i = 0; i < 9; i++)
            {
                int c = sc.read();
                if (c < 0)
                {
                    throw new SplitException(
                            ErrorCode.PARSE_EDIFACT_INVALID_ENVELOPE, start);
                }
                raw[i] = (byte) c;
            }
            d = new Delimiters(raw[3], raw[4], raw[6], raw[8]);
            if (d.component() == d.element() || d.element() == d.terminator() || d.
                    component() == d.terminator())
            {
                throw new SplitException(
                        ErrorCode.PARSE_EDIFACT_INVALID_ENVELOPE, start);
            }
            skipLineBreaks();
            return new Segment("UNA", start, sc.position(), raw);
        }

        // ---- structure ----
        @Override
        public Optional<Section> next() throws IOException, SplitException
        {
            while (true)
            {
                Segment s = nextSegment();
                if (s == null)
                {
                    if (unb != null)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_EDIFACT_UNBALANCED, sc.
                                        position());
                    }
                    return Optional.empty();
                }
                if (unb == null)
                {
                    long start = s.start();
                    Envelope opened = interchange(s);
                    if (opened == null)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_EDIFACT_INVALID_ENVELOPE, start);
                    }
                    if (level == Level.UNB)
                    {
                        Segment unz = scanUntil(start, "UNZ", Set.of("UNA",
                                "UNB"));
                        checkControl(unz, 2, opened.control());
                        d = Delimiters.DEFAULT;
                        return Optional.of(Section.ofRange(start,
                                unz.end() - start, ResumePoint.at(unz.end())));
                    }
                    unb = opened;
                    continue;
                }
                if (ung == null)
                {
                    switch (s.id())
                    {
                        case "UNZ" ->
                        {
                            checkControl(s, 2, unb.control());
                            unb = null;
                            d = Delimiters.DEFAULT; // the next interchange has its own UNA, or the defaults
                        }
                        case "UNG" ->
                        {
                            if (level == Level.UNG)
                            {
                                Segment une = scanUntil(s.start(), "UNE", Set.
                                        of("UNA", "UNB", "UNZ", "UNG"));
                                checkControl(une, 2, s.element(5, d));
                                return Optional.of(section(s.start(), une.end(),
                                        unb.raw(), unz(),
                                        ResumePoint.at(une.end(), unb.start())));
                            }
                            ung = envelope(s, s.start(), 5);
                        }
                        case "UNH" ->
                        {
                            Segment unt = scanUntil(s.start(), "UNT", Set.of(
                                    "UNA", "UNB", "UNZ", "UNG", "UNE", "UNH"));
                            checkControl(unt, 2, s.element(1, d));
                            return Optional.of(section(s.start(), unt.end(),
                                    unb.raw(), unz(),
                                    ResumePoint.at(unt.end(), unb.start())));
                        }
                        default ->
                            throw new SplitException(
                                    ErrorCode.PARSE_EDIFACT_UNEXPECTED_SEGMENT,
                                    s.start());
                    }
                    continue;
                }
                switch (s.id())
                {
                    case "UNE" ->
                    {
                        checkControl(s, 2, ung.control());
                        ung = null;
                    }
                    case "UNH" ->
                    {
                        Segment unt = scanUntil(s.start(), "UNT", Set.of("UNA",
                                "UNB", "UNZ", "UNG", "UNE", "UNH"));
                        checkControl(unt, 2, s.element(1, d));
                        return Optional.of(section(s.start(), unt.end(), concat(
                                unb.raw(), ung.raw()),
                                concat(segment("UNE", "1", ung.control()), unz()),
                                ResumePoint.at(unt.end(), unb.start(), ung.
                                        start())));
                    }
                    default ->
                        throw new SplitException(
                                ErrorCode.PARSE_EDIFACT_UNEXPECTED_SEGMENT, s.
                                        start());
                }
            }
        }

        /**
         * Reads segments until {@code endId}; any of {@code forbidden} first
         * means unbalanced.
         */
        private Segment scanUntil(long from, String endId, Set<String> forbidden)
                throws IOException, SplitException
        {
            while (true)
            {
                Segment s = nextSegment();
                if (s == null || forbidden.contains(s.id()))
                {
                    throw new SplitException(ErrorCode.PARSE_EDIFACT_UNBALANCED,
                            from);
                }
                if (s.end() - from > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_SECTION_SIZE, from);
                }
                if (s.id().equals(endId))
                {
                    return s;
                }
            }
        }

        private void checkControl(Segment s, int element, String expected)
                throws SplitException
        {
            if (!s.element(element, d).equals(expected))
            {
                throw new SplitException(
                        ErrorCode.PARSE_EDIFACT_CONTROL_MISMATCH, s.start());
            }
        }

        private byte[] unz()
        {
            return segment("UNZ", "1", unb.control());
        }

        /**
         * A synthesised service segment; its values are control references,
         * which need no escaping.
         */
        private byte[] segment(String... elements)
        {
            ByteArrayOutputStream b = new ByteArrayOutputStream(32);
            for (int i = 0; i < elements.length; i++)
            {
                if (i > 0)
                {
                    b.write(d.element());
                }
                b.writeBytes(elements[i].getBytes(ISO_8859_1));
            }
            b.write(d.terminator());
            b.writeBytes(eol);
            return b.toByteArray();
        }

        private static Section section(long start, long end, byte[] prefix,
                byte[] suffix, ResumePoint resume)
        {
            List<Section.Part> parts = new ArrayList<>(3);
            parts.add(new Section.Literal(prefix));
            parts.add(new Section.SourceRange(start, end - start));
            parts.add(new Section.Literal(suffix));
            return new Section(start, end - start, parts, resume);
        }

        private static byte[] concat(byte[]... parts)
        {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            for (byte[] p : parts)
            {
                b.writeBytes(p);
            }
            return b.toByteArray();
        }
    }
}
