package com.sdh.healthcare.connector.core.split.builtin;

import static java.nio.charset.StandardCharsets.UTF_8;

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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Streaming, byte-level XML splitter (design §6.2). A section runs from a
 * boundary start tag to its matching end tag. Comments, CDATA, processing
 * instructions and DOCTYPE are skipped, so a boundary name inside them is
 * ignored. A boundary element nested inside another belongs to the outer
 * section.
 *
 * <p>
 * Every section is a standalone document: the file's XML declaration is
 * prepended, and namespace declarations in scope from ancestor elements are
 * copied onto the section's root start tag.
 *
 * <p>
 * Settings: boundary = element local name (required); option {@code namespace}
 * = the element's namespace URI, or {@code *} (default) for any. Input must be
 * in an ASCII-compatible encoding such as UTF-8; UTF-16 is rejected. Entities
 * declared in a DOCTYPE internal subset are not carried into sections.
 */
public class XmlSplitter implements SectionSplitter
{

    public static final String FORMAT = "xml";

    private static final byte[] COMMENT = "<!--".getBytes(UTF_8);
    private static final byte[] COMMENT_END = "-->".getBytes(UTF_8);
    private static final byte[] CDATA = "<![CDATA[".getBytes(UTF_8);
    private static final byte[] CDATA_END = "]]>".getBytes(UTF_8);
    private static final byte[] PI_END = "?>".getBytes(UTF_8);
    private static final byte[] XML_DECL = "<?xml".getBytes(UTF_8);
    private static final int MAX_CONTEXT_TAG = 1 << 20;

    @Override
    public String format()
    {
        return FORMAT;
    }

    protected String defaultBoundary()
    {
        return null;
    }

    protected String defaultNamespace()
    {
        return "*";
    }

    @Override
    public void validate(SplitConfig cfg)
    {
        if (cfg.boundaryOr(defaultBoundary()) == null)
        {
            throw new IllegalArgumentException(
                    "An element name boundary is required for format " + format());
        }
    }

    @Override
    public SplitSession open(SeekableByteSource src, ResumePoint from,
            SplitConfig cfg)
            throws IOException, SplitException
    {
        validate(cfg);
        Session s = new Session(src, cfg.boundaryOr(defaultBoundary()), cfg.
                option("namespace", defaultNamespace()),
                cfg);
        s.start(from);
        return s;
    }

    /**
     * {@code raw} is the attribute exactly as written, e.g.
     * {@code xmlns:v3="urn:hl7-org:v3"}.
     */
    record NsDecl(String prefix, String uri, byte[] raw) {}

    /**
     * A parsed start tag; {@code bytes} is the whole tag from {@code <} to
     * {@code >}.
     */
    record StartTag(long offset, byte[] bytes, int nameLength,
            boolean selfClosing, List<NsDecl> decls)
    {

        String qname()
        {
            return new String(bytes, 1, nameLength, UTF_8);
        }

        byte[] name()
        {
            return Arrays.copyOfRange(bytes, 1, 1 + nameLength);
        }
    }

    private static final class Session implements SplitSession
    {

        private final SeekableByteSource src;
        private final String boundary;
        private final String namespace;
        private final SplitConfig cfg;
        private final long max;
        private final List<StartTag> outer = new ArrayList<>();
        private byte[] decl = new byte[0];
        private ByteScanner sc;

        Session(SeekableByteSource src, String boundary, String namespace,
                SplitConfig cfg)
        {
            this.src = src;
            this.boundary = boundary;
            this.namespace = namespace;
            this.cfg = cfg;
            this.max = cfg.limits().maxSectionBytes();
        }

        void start(ResumePoint from) throws IOException, SplitException
        {
            sc = new ByteScanner(src, 0, Math.min(cfg.readBufferBytes(),
                    64 << 10));
            int b0 = sc.peek(), b1 = sc.peek(1);
            if ((b0 == 0xFE && b1 == 0xFF) || (b0 == 0xFF && b1 == 0xFE) || (b0 == 0 || b1 == 0) && b0 >= 0)
            {
                throw new SplitException(
                        ErrorCode.PARSE_XML_UNSUPPORTED_ENCODING, 0);
            }
            if (b0 == 0xEF && b1 == 0xBB && sc.peek(2) == 0xBF)
            {
                sc.skip(3);
            }
            if (sc.lookingAt(XML_DECL) && isSpace(sc.peek(XML_DECL.length)))
            {
                long s = sc.position();
                if (!sc.skipPast(PI_END) || sc.position() - s > 4096)
                {
                    throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, s);
                }
                byte[] raw = src.readFully(s, (int) (sc.position() - s));
                decl = Arrays.copyOf(raw, raw.length + 1);
                decl[raw.length] = '\n';
            }
            long startAt = sc.position();
            if (from.offset() > 0)
            {
                for (long tagOffset : from.context())
                {
                    ByteScanner t = new ByteScanner(src, tagOffset, 4096);
                    if (t.read() != '<' || isNameEnd(t.peek()) || t.peek() == '/' || t.
                            peek() == '!'
                            || t.peek() == '?')
                    {
                        throw new SplitException(ErrorCode.RESUME_INVALID, from.
                                offset());
                    }
                    StartTag tag = readStartTag(t, tagOffset, MAX_CONTEXT_TAG);
                    if (tag.selfClosing())
                    {
                        throw new SplitException(ErrorCode.RESUME_INVALID, from.
                                offset());
                    }
                    outer.add(tag);
                }
                startAt = from.offset();
            } else if (!from.context().isEmpty())
            {
                throw new SplitException(ErrorCode.RESUME_INVALID, 0);
            }
            sc = new ByteScanner(src, startAt, cfg.readBufferBytes());
        }

        @Override
        public Optional<Section> next() throws IOException, SplitException
        {
            while (true)
            {
                if (!sc.skipTo((byte) '<'))
                {
                    if (!outer.isEmpty())
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_UNBALANCED_ELEMENT, sc.
                                        position());
                    }
                    return Optional.empty();
                }
                long at = sc.position();
                if (skipMarkup(at))
                {
                    continue;
                }
                sc.read();
                if (sc.peek() == '/')
                {
                    sc.read();
                    byte[] name = readEndTag(at);
                    if (outer.isEmpty() || !Arrays.
                            equals(outer.getLast().name(), name))
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_UNBALANCED_ELEMENT, at);
                    }
                    outer.removeLast();
                    continue;
                }
                StartTag tag = readStartTag(sc, at, (int) Math.min(max,
                        MAX_CONTEXT_TAG * 64L));
                if (isBoundary(tag))
                {
                    return Optional.of(section(tag));
                }
                if (!tag.selfClosing())
                {
                    outer.add(tag);
                }
            }
        }

        /**
         * Skips a comment, CDATA, PI or DOCTYPE at {@code at}. Returns false if
         * it's an element tag.
         */
        private boolean skipMarkup(long at) throws IOException, SplitException
        {
            byte[] end;
            if (sc.lookingAt(COMMENT))
            {
                end = COMMENT_END;
            } else if (sc.lookingAt(CDATA))
            {
                end = CDATA_END;
            } else if (sc.peek(1) == '?')
            {
                end = PI_END;
            } else if (sc.peek(1) == '!')
            {
                skipDeclaration(at);
                return true;
            } else
            {
                return false;
            }
            sc.skip(2);
            if (!sc.skipPast(end))
            {
                throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
            }
            return true;
        }

        /**
         * {@code <!DOCTYPE ...>} including an internal subset in brackets.
         */
        private void skipDeclaration(long at) throws IOException, SplitException
        {
            sc.skip(2);
            int brackets = 0;
            int quote = 0;
            while (true)
            {
                int c = sc.read();
                if (c < 0)
                {
                    throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
                }
                if (quote != 0)
                {
                    if (c == quote)
                    {
                        quote = 0;
                    }
                } else if (c == '"' || c == '\'')
                {
                    quote = c;
                } else if (c == '[')
                {
                    brackets++;
                } else if (c == ']')
                {
                    brackets--;
                } else if (c == '>' && brackets <= 0)
                {
                    return;
                }
            }
        }

        /**
         * After {@code </}: reads the name and the closing {@code >}.
         */
        private byte[] readEndTag(long at) throws IOException, SplitException
        {
            ByteArrayOutputStream name = new ByteArrayOutputStream(32);
            int c;
            while (!isNameEnd(c = sc.peek()))
            {
                name.write(sc.read());
                if (name.size() > 4096)
                {
                    throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
                }
            }
            while (isSpace(sc.peek()))
            {
                sc.read();
            }
            if (sc.read() != '>' || name.size() == 0)
            {
                throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
            }
            return name.toByteArray();
        }

        private boolean isBoundary(StartTag tag) throws SplitException
        {
            String qname = tag.qname();
            int colon = qname.indexOf(':');
            String local = colon < 0 ? qname : qname.substring(colon + 1);
            if (!local.equals(boundary))
            {
                return false;
            }
            if (namespace.equals("*"))
            {
                return true;
            }
            String prefix = colon < 0 ? "" : qname.substring(0, colon);
            String uri = resolve(prefix, tag);
            if (uri == null && !prefix.isEmpty())
            {
                throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, tag.
                        offset());
            }
            return namespace.equals(uri == null ? "" : uri);
        }

        private String resolve(String prefix, StartTag tag)
        {
            for (NsDecl d : tag.decls())
            {
                if (d.prefix().equals(prefix))
                {
                    return d.uri();
                }
            }
            for (int i = outer.size() - 1; i >= 0; i--)
            {
                for (NsDecl d : outer.get(i).decls())
                {
                    if (d.prefix().equals(prefix))
                    {
                        return d.uri();
                    }
                }
            }
            return null;
        }

        private Section section(StartTag tag) throws IOException, SplitException
        {
            long start = tag.offset();
            long end = tag.selfClosing() ? sc.position() : scanToEnd(tag);
            long[] ctx = new long[outer.size()];
            for (int i = 0; i < ctx.length; i++)
            {
                ctx[i] = outer.get(i).offset();
            }
            ResumePoint resume = ResumePoint.at(end, ctx);

            Map<String, NsDecl> inScope = new LinkedHashMap<>();
            for (StartTag t : outer)
            {
                for (NsDecl d : t.decls())
                {
                    inScope.remove(d.prefix());
                    inScope.put(d.prefix(), d);
                }
            }
            for (NsDecl d : tag.decls())
            {
                inScope.remove(d.prefix());
            }
            List<Section.Part> parts = new ArrayList<>(3);
            if (decl.length > 0)
            {
                parts.add(new Section.Literal(decl));
            }
            if (inScope.isEmpty())
            {
                parts.add(new Section.SourceRange(start, end - start));
            } else
            {
                byte[] t = tag.bytes();
                ByteArrayOutputStream b = new ByteArrayOutputStream(
                        t.length + 64 * inScope.size());
                b.write(t, 0, 1 + tag.nameLength());
                for (NsDecl d : inScope.values())
                {
                    b.write(' ');
                    b.writeBytes(d.raw());
                }
                b.
                        write(t, 1 + tag.nameLength(), t.length - 1 - tag.
                                nameLength());
                parts.add(new Section.Literal(b.toByteArray()));
                long bodyStart = start + t.length;
                if (end > bodyStart)
                {
                    parts.add(
                            new Section.SourceRange(bodyStart, end - bodyStart));
                }
            }
            return new Section(start, end - start, parts, resume);
        }

        /**
         * Scans from after the boundary start tag to just after its matching
         * end tag.
         */
        private long scanToEnd(StartTag root) throws IOException, SplitException
        {
            long start = root.offset();
            ArrayDeque<byte[]> names = new ArrayDeque<>();
            names.push(root.name());
            while (true)
            {
                if (!sc.skipTo((byte) '<'))
                {
                    throw new SplitException(ErrorCode.PARSE_UNBALANCED_ELEMENT,
                            start);
                }
                long at = sc.position();
                if (at - start > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_SECTION_SIZE, start);
                }
                if (skipMarkup(at))
                {
                    continue;
                }
                sc.read();
                if (sc.peek() == '/')
                {
                    sc.read();
                    if (!Arrays.equals(names.pop(), readEndTag(at)))
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_UNBALANCED_ELEMENT, at);
                    }
                    if (names.isEmpty())
                    {
                        return sc.position();
                    }
                } else
                {
                    byte[] name = skipStartTag(at);
                    if (name != null)
                    {
                        names.push(name);
                    }
                }
            }
        }

        /**
         * After {@code <} inside a section: skips the tag; returns its name, or
         * null if self-closing.
         */
        private byte[] skipStartTag(long at) throws IOException, SplitException
        {
            ByteArrayOutputStream name = new ByteArrayOutputStream(32);
            while (!isNameEnd(sc.peek()))
            {
                name.write(sc.read());
                if (name.size() > 4096)
                {
                    throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
                }
            }
            if (name.size() == 0)
            {
                throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
            }
            int quote = 0;
            int last = 0;
            while (true)
            {
                int c = sc.read();
                if (c < 0)
                {
                    throw new SplitException(ErrorCode.PARSE_UNBALANCED_ELEMENT,
                            at);
                }
                if (quote != 0)
                {
                    if (c == quote)
                    {
                        quote = 0;
                    }
                } else if (c == '"' || c == '\'')
                {
                    quote = c;
                } else if (c == '>')
                {
                    return last == '/' ? null : name.toByteArray();
                } else if (!isSpace(c))
                {
                    last = c;
                }
            }
        }
    }

    /**
     * After {@code <}: reads a whole start tag into memory and parses its
     * namespace declarations.
     */
    static StartTag readStartTag(ByteScanner sc, long at, int cap) throws
            IOException, SplitException
    {
        ByteArrayOutputStream b = new ByteArrayOutputStream(128);
        b.write('<');
        int quote = 0;
        while (true)
        {
            int c = sc.read();
            if (c < 0)
            {
                throw new SplitException(ErrorCode.PARSE_UNBALANCED_ELEMENT, at);
            }
            b.write(c);
            if (b.size() > cap)
            {
                throw new SplitException(ErrorCode.LIMIT_SECTION_SIZE, at);
            }
            if (quote != 0)
            {
                if (c == quote)
                {
                    quote = 0;
                }
            } else if (c == '"' || c == '\'')
            {
                quote = c;
            } else if (c == '>')
            {
                break;
            }
        }
        return parseStartTag(b.toByteArray(), at);
    }

    static StartTag parseStartTag(byte[] t, long at) throws SplitException
    {
        int p = 1;
        while (p < t.length && !isNameEnd(t[p] & 0xff))
        {
            p++;
        }
        int nameLength = p - 1;
        if (nameLength == 0)
        {
            throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
        }
        List<NsDecl> decls = new ArrayList<>(2);
        boolean selfClosing = false;
        while (true)
        {
            while (isSpace(t[p] & 0xff))
            {
                p++;
            }
            if (t[p] == '>')
            {
                break;
            }
            if (t[p] == '/' && t[p + 1] == '>')
            {
                selfClosing = true;
                break;
            }
            int attrStart = p;
            while (!isNameEnd(t[p] & 0xff) && t[p] != '=')
            {
                p++;
            }
            String attr = new String(t, attrStart, p - attrStart, UTF_8);
            while (isSpace(t[p] & 0xff))
            {
                p++;
            }
            if (attr.isEmpty() || t[p] != '=')
            {
                throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
            }
            p++;
            while (isSpace(t[p] & 0xff))
            {
                p++;
            }
            byte q = t[p];
            if (q != '"' && q != '\'')
            {
                throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
            }
            int valueStart = ++p;
            while (t[p] != q)
            {
                p++;
            }
            p++;
            if (attr.equals("xmlns") || attr.startsWith("xmlns:"))
            {
                String prefix = attr.equals("xmlns") ? "" : attr.substring(6);
                String uri = new String(t, valueStart, p - 1 - valueStart, UTF_8);
                byte[] raw = Arrays.copyOfRange(t, attrStart, p);
                decls.add(new NsDecl(prefix, uri, raw));
            }
        }
        return new StartTag(at, t, nameLength, selfClosing, List.copyOf(decls));
    }

    static boolean isSpace(int c)
    {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    }

    static boolean isNameEnd(int c)
    {
        return c < 0 || isSpace(c) || c == '>' || c == '/' || c == '=';
    }
}
