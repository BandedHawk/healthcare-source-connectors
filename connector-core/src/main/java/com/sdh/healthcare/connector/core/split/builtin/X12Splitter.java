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
 * X12 splitter (design §6.2). Delimiters are read from each ISA segment, and
 * every section is a valid standalone interchange.
 *
 * <p>
 * Boundary:
 * <ul>
 * <li>{@code ST} (default): one transaction set per section, as
 * {@code ISA + GS + ST…SE + GE + IEA}, with GE01 and IEA01 rewritten to 1</li>
 * <li>{@code GS}: one functional group per section, as
 * {@code ISA + GS…GE + IEA}, with IEA01 rewritten to 1</li>
 * <li>{@code ISA}: one whole interchange per section, unchanged</li>
 * </ul>
 * A TA1 acknowledgement between ISA and GS becomes its own section,
 * {@code ISA + TA1 + IEA} with IEA01 = 0. Control numbers are checked
 * (ST02/SE02, GS06/GE02, ISA13/IEA02) to catch truncated or spliced input. BIN
 * segments carrying raw binary are not supported.
 */
public final class X12Splitter implements SectionSplitter
{

    public static final String FORMAT = "x12";

    private static final byte[] ISA = "ISA".getBytes(ISO_8859_1);
    private static final Set<String> ENVELOPE = Set.of("ISA", "IEA", "GS", "GE",
            "ST", "SE", "TA1");
    private static final int MAX_ENVELOPE_SEGMENT = 1024;

    enum Level
    {
        ST, GS, ISA
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
        return Level.valueOf(cfg.boundaryOr("ST").toUpperCase(Locale.ROOT));
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
     * One segment. {@code end} is after the terminator and any CR/LF following
     * it.
     */
    private record Segment(String id, long start, long end, byte[] content)
    {

        String element(int i, byte es)
        {
            int field = 0;
            int from = 0;
            for (int p = 0; p <= content.length; p++)
            {
                if (p == content.length || content[p] == es)
                {
                    if (field == i)
                    {
                        return new String(content, from, p - from, ISO_8859_1).
                                trim();
                    }
                    field++;
                    from = p + 1;
                }
            }
            return "";
        }
    }

    /**
     * An open ISA or GS: its raw bytes (for the prefix) and its control number.
     */
    private record Envelope(long start, byte[] raw, String control) {}

    private static final class Session implements SplitSession
    {

        private final SeekableByteSource src;
        private final Level level;
        private final SplitConfig cfg;
        private final long max;
        private ByteScanner sc;
        private byte es;
        private byte term;
        private byte[] eol = new byte[0];
        private Envelope isa;
        private Envelope gs;

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
            int expected = switch (level)
            {
                case ISA ->
                    0;
                case GS ->
                    1;
                case ST ->
                    2;
            };
            if (ctx.size() > expected)
            {
                throw new SplitException(ErrorCode.RESUME_INVALID, from.offset());
            }
            if (!ctx.isEmpty())
            {
                sc = new ByteScanner(src, ctx.get(0), 4096);
                Segment s = nextSegment();
                if (s == null || !s.id().equals("ISA") || s.start() != ctx.
                        get(0))
                {
                    throw new SplitException(ErrorCode.RESUME_INVALID, from.
                            offset());
                }
                isa = envelope(s, 13);
                if (ctx.size() == 2)
                {
                    sc = new ByteScanner(src, ctx.get(1), 4096);
                    Segment g = nextSegment();
                    if (g == null || !g.id().equals("GS") || g.start() != ctx.
                            get(1))
                    {
                        throw new SplitException(ErrorCode.RESUME_INVALID, from.
                                offset());
                    }
                    gs = envelope(g, 6);
                }
            }
            sc = new ByteScanner(src, from.offset(), cfg.readBufferBytes());
        }

        private Envelope envelope(Segment s, int controlElement) throws
                IOException
        {
            byte[] raw = src.readFully(s.start(), (int) (s.end() - s.start()));
            return new Envelope(s.start(), raw, s.element(controlElement, es));
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
            if (sc.lookingAt(ISA) && !Character.isLetterOrDigit(sc.peek(3)))
            {
                return readIsa(start);
            }
            if (term == 0)
            {
                throw new SplitException(ErrorCode.PARSE_X12_INVALID_ENVELOPE,
                        start);
            }
            StringBuilder id = new StringBuilder(3);
            int c;
            while ((c = sc.peek()) >= 0 && c != (es & 0xff) && c != (term & 0xff))
            {
                if (id.length() == 3)
                {
                    throw new SplitException(
                            ErrorCode.PARSE_X12_UNEXPECTED_SEGMENT, start);
                }
                id.append((char) sc.read());
            }
            String sid = id.toString();
            byte[] content = null;
            if (ENVELOPE.contains(sid))
            {
                ByteArrayOutputStream b = new ByteArrayOutputStream(64);
                b.writeBytes(sid.getBytes(ISO_8859_1));
                while ((c = sc.read()) >= 0 && c != (term & 0xff))
                {
                    if (b.size() >= MAX_ENVELOPE_SEGMENT)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_X12_INVALID_ENVELOPE, start);
                    }
                    b.write(c);
                }
                content = b.toByteArray();
            } else if (sc.skipTo(term))
            {
                c = sc.read();
            } else
            {
                c = -1;
            }
            if (c < 0)
            {
                throw new SplitException(ErrorCode.PARSE_X12_UNBALANCED, start);
            }
            skipLineBreaks();
            return new Segment(sid, start, sc.position(), content);
        }

        /**
         * ISA is fixed-format: element separator at byte 3, terminator right
         * after ISA16.
         */
        private Segment readIsa(long start) throws IOException, SplitException
        {
            ByteArrayOutputStream b = new ByteArrayOutputStream(110);
            for (int i = 0; i < 3; i++)
            {
                b.write(sc.read());
            }
            int sep = sc.read();
            b.write(sep);
            int seps = 1;
            while (seps < 16)
            {
                int c = sc.read();
                if (c < 0 || b.size() > 200)
                {
                    throw new SplitException(
                            ErrorCode.PARSE_X12_INVALID_ENVELOPE, start);
                }
                b.write(c);
                if (c == sep)
                {
                    seps++;
                }
            }
            int component = sc.read();
            int terminator = sc.read();
            if (component < 0 || terminator < 0 || terminator == sep || Character.
                    isLetterOrDigit(terminator))
            {
                throw new SplitException(ErrorCode.PARSE_X12_INVALID_ENVELOPE,
                        start);
            }
            b.write(component);
            es = (byte) sep;
            term = (byte) terminator;
            long afterTerm = sc.position();
            skipLineBreaks();
            eol = src.readFully(afterTerm, (int) Math.min(2,
                    sc.position() - afterTerm));
            return new Segment("ISA", start, sc.position(), b.toByteArray());
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
                    if (isa != null)
                    {
                        throw new SplitException(ErrorCode.PARSE_X12_UNBALANCED,
                                sc.position());
                    }
                    return Optional.empty();
                }
                if (isa == null)
                {
                    if (!s.id().equals("ISA"))
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_X12_INVALID_ENVELOPE, s.start());
                    }
                    if (level == Level.ISA)
                    {
                        Segment iea = scanUntil(s, "IEA", Set.of("ISA"));
                        checkControl(iea, 2, s.element(13, es));
                        return Optional.of(Section.ofRange(s.start(),
                                iea.end() - s.start(),
                                ResumePoint.at(iea.end())));
                    }
                    isa = envelope(s, 13);
                    continue;
                }
                if (gs == null)
                {
                    switch (s.id())
                    {
                        case "IEA" ->
                        {
                            checkControl(s, 2, isa.control());
                            isa = null;
                        }
                        case "TA1" ->
                        {
                            return Optional.of(section(s.start(), s.end(),
                                    concat(isa.raw()), concat(iea(0)),
                                    ResumePoint.at(s.end(), isa.start())));
                        }
                        case "GS" ->
                        {
                            if (level == Level.GS)
                            {
                                Segment ge = scanUntil(s, "GE", Set.of("ISA",
                                        "IEA", "GS"));
                                checkControl(ge, 2, s.element(6, es));
                                return Optional.of(section(s.start(), ge.end(),
                                        concat(isa.raw()),
                                        concat(iea(1)), ResumePoint.at(ge.end(),
                                        isa.start())));
                            }
                            gs = envelope(s, 6);
                        }
                        default ->
                            throw new SplitException(
                                    ErrorCode.PARSE_X12_UNEXPECTED_SEGMENT, s.
                                            start());
                    }
                    continue;
                }
                switch (s.id())
                {
                    case "GE" ->
                    {
                        checkControl(s, 2, gs.control());
                        gs = null;
                    }
                    case "ST" ->
                    {
                        Segment se = scanUntil(s, "SE", Set.of("ISA", "IEA",
                                "GS", "GE", "ST"));
                        checkControl(se, 2, s.element(2, es));
                        byte[] ge = segment("GE", "1", gs.control());
                        return Optional.of(section(s.start(), se.end(), concat(
                                isa.raw(), gs.raw()),
                                concat(ge, iea(1)), ResumePoint.at(se.end(),
                                isa.start(), gs.start())));
                    }
                    default ->
                        throw new SplitException(
                                ErrorCode.PARSE_X12_UNEXPECTED_SEGMENT, s.
                                        start());
                }
            }
        }

        /**
         * Reads segments after {@code first} until {@code endId}; any of
         * {@code forbidden} first means unbalanced.
         */
        private Segment scanUntil(Segment first, String endId,
                Set<String> forbidden)
                throws IOException, SplitException
        {
            while (true)
            {
                Segment s = nextSegment();
                if (s == null || forbidden.contains(s.id()))
                {
                    throw new SplitException(ErrorCode.PARSE_X12_UNBALANCED,
                            first.start());
                }
                if (s.end() - first.start() > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_SECTION_SIZE,
                            first.start());
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
            if (!s.element(element, es).equals(expected))
            {
                throw new SplitException(ErrorCode.PARSE_X12_CONTROL_MISMATCH,
                        s.start());
            }
        }

        private byte[] iea(int groups)
        {
            return segment("IEA", Integer.toString(groups), isa.control());
        }

        private byte[] segment(String... elements)
        {
            ByteArrayOutputStream b = new ByteArrayOutputStream(32);
            for (int i = 0; i < elements.length; i++)
            {
                if (i > 0)
                {
                    b.write(es);
                }
                b.writeBytes(elements[i].getBytes(ISO_8859_1));
            }
            b.write(term);
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
