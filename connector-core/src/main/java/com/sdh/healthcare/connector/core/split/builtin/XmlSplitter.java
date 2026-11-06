/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

import static java.nio.charset.StandardCharsets.UTF_8;

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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Streaming, byte-level XML splitter (design §6.2). A record runs from a
 * boundary start tag to its matching end tag. Comments, CDATA, processing
 * instructions and DOCTYPE are skipped, so a boundary name inside them is
 * ignored. A boundary element nested inside another belongs to the outer
 * record.
 * <p>
 * Every record is a standalone document: the file's XML declaration is
 * prepended, and namespace declarations in scope from ancestor elements
 * are copied onto the record's root start tag.
 * <p>
 * Settings: boundary = element local name (required); option
 * {@code namespace} = the element's namespace URI, or {@code *} (default)
 * for any. Input must be in an ASCII-compatible encoding such as UTF-8;
 * UTF-16 is rejected. Entities declared in a DOCTYPE internal subset are
 * not carried into records.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class XmlSplitter implements RecordSplitter
{

    /** The format's name. */
    public static final String FORMAT = "xml";

    /** The start of a comment. */
    private static final byte[] COMMENT = "<!--".getBytes(UTF_8);

    /** The end of a comment. */
    private static final byte[] COMMENT_END = "-->".getBytes(UTF_8);

    /** The start of a CDATA section. */
    private static final byte[] CDATA = "<![CDATA[".getBytes(UTF_8);

    /** The end of a CDATA section. */
    private static final byte[] CDATA_END = "]]>".getBytes(UTF_8);

    /** The end of a processing instruction. */
    private static final byte[] PI_END = "?>".getBytes(UTF_8);

    /** The start of the XML declaration. */
    private static final byte[] XML_DECL = "<?xml".getBytes(UTF_8);

    /** The largest ancestor start tag kept, in bytes. */
    private static final int MAX_CONTEXT_TAG = 1 << 20;

    /**
     * The most elements a boundary element may be nested in. Every open
     * ancestor is held in memory (its start tag, for the namespaces it
     * declares) and its offset goes into each record's resume point, which
     * is saved in the checkpoint, so without a limit a file of nested
     * wrappers could exhaust memory or make the checkpoint huge. Real
     * documents nest their boundary one to three deep. Deeper nesting fails
     * the file with {@code PARSE_XML_MALFORMED}, as the FHIR splitter does
     * for JSON.
     */
    static final int MAX_DEPTH = 128;

    /**
     * Creates the splitter; the service loader calls this.
     */
    public XmlSplitter()
    {
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
     * The boundary used when none is configured.
     *
     * @return the element's local name; null, so one must be configured
     */
    protected String defaultBoundary()
    {
        return null;
    }

    /**
     * The namespace used when none is configured.
     *
     * @return the namespace URI; {@code *} for any
     */
    protected String defaultNamespace()
    {
        return "*";
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if there is no boundary
     */
    @Override
    public void validate(SplitConfig config)
    {
        if (config.boundaryOr(defaultBoundary()) == null)
        {
            throw new IllegalArgumentException("An element name boundary is"
                    + " required for format " + format());
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if there is no boundary
     */
    @Override
    public SplitSession open(SeekableByteSource source, ResumePoint from,
            SplitConfig config)
            throws IOException, SplitException
    {
        validate(config);
        Session session = new Session(source,
                config.boundaryOr(defaultBoundary()),
                config.option("namespace", defaultNamespace()), config);
        session.start(from);
        return session;
    }

    /**
     * A namespace declaration.
     *
     * @param prefix the prefix; empty for the default namespace
     * @param uri    the namespace URI
     * @param raw    the attribute exactly as written, e.g.
     *               {@code xmlns:v3="urn:hl7-org:v3"}
     */
    record NsDecl(String prefix, String uri, byte[] raw)
    {
    }

    /**
     * A parsed start tag.
     *
     * @param offset      its source offset
     * @param bytes       the whole tag, from {@code <} to {@code >}
     * @param nameLength  the length of its name, in bytes
     * @param selfClosing whether it ends with {@code />}
     * @param decls       the namespaces it declares
     */
    record StartTag(long offset, byte[] bytes, int nameLength,
            boolean selfClosing, List<NsDecl> decls)
    {

        /**
         * The element's qualified name.
         *
         * @return the name, with any prefix
         */
        String qname()
        {
            return new String(bytes, 1, nameLength, UTF_8);
        }

        /**
         * The element's qualified name, as bytes.
         *
         * @return a copy of the name's bytes
         */
        byte[] name()
        {
            return Arrays.copyOfRange(bytes, 1, 1 + nameLength);
        }
    }

    /** Splits one file. */
    private static final class Session implements SplitSession
    {

        /** The file. */
        private final SeekableByteSource source;

        /** The boundary element's local name. */
        private final String boundary;

        /** The boundary element's namespace; {@code *} for any. */
        private final String namespace;

        /** The split settings. */
        private final SplitConfig config;

        /** The largest record allowed, in bytes. */
        private final long max;

        /** The open ancestors of the current position, outermost first. */
        private final List<StartTag> outer = new ArrayList<>();

        /** The file's XML declaration and a newline; empty if none. */
        private byte[] decl = new byte[0];

        /** Reads the file. */
        private ByteScanner scanner;

        /**
         * Creates a session.
         *
         * @param source    the file
         * @param boundary  the boundary element's local name
         * @param namespace its namespace; {@code *} for any
         * @param config    the split settings
         */
        Session(SeekableByteSource source, String boundary, String namespace,
                SplitConfig config)
        {
            this.source = source;
            this.boundary = boundary;
            this.namespace = namespace;
            this.config = config;
            this.max = config.limits().maxRecordBytes();
        }

        /**
         * Reads the XML declaration and, when resuming, the open ancestors,
         * then positions the session.
         *
         * @param from where to start
         * @throws IOException    if the file can't be read
         * @throws SplitException if the encoding is unsupported, the
         *                        declaration is malformed, or the point
         *                        doesn't fit the file
         */
        void start(ResumePoint from) throws IOException, SplitException
        {
            scanner = new ByteScanner(source, 0,
                    Math.min(config.readBufferBytes(), 64 << 10));
            int first = scanner.peek();
            int second = scanner.peek(1);
            if ((first == 0xFE && second == 0xFF)
                    || (first == 0xFF && second == 0xFE)
                    || ((first == 0 || second == 0) && first >= 0))
            {
                throw new SplitException(
                        ErrorCode.PARSE_XML_UNSUPPORTED_ENCODING, 0);
            }
            if (first == 0xEF && second == 0xBB && scanner.peek(2) == 0xBF)
            {
                scanner.skip(3);
            }
            if (scanner.lookingAt(XML_DECL)
                    && isSpace(scanner.peek(XML_DECL.length)))
            {
                long declStart = scanner.position();
                if (!scanner.skipPast(PI_END)
                        || scanner.position() - declStart > 4096)
                {
                    throw new SplitException(ErrorCode.PARSE_XML_MALFORMED,
                            declStart);
                }
                byte[] raw = source.readFully(declStart,
                        (int) (scanner.position() - declStart));
                decl = Arrays.copyOf(raw, raw.length + 1);
                decl[raw.length] = '\n';
            }
            long startAt = scanner.position();
            if (from.offset() > 0)
            {
                for (long tagOffset : from.context())
                {
                    ByteScanner tagScanner =
                            new ByteScanner(source, tagOffset, 4096);
                    if (tagScanner.read() != '<'
                            || isNameEnd(tagScanner.peek())
                            || tagScanner.peek() == '/'
                            || tagScanner.peek() == '!'
                            || tagScanner.peek() == '?')
                    {
                        throw new SplitException(ErrorCode.RESUME_INVALID,
                                from.offset());
                    }
                    StartTag tag = readStartTag(tagScanner, tagOffset,
                            MAX_CONTEXT_TAG);
                    if (tag.selfClosing())
                    {
                        throw new SplitException(ErrorCode.RESUME_INVALID,
                                from.offset());
                    }
                    outer.add(tag);
                }
                startAt = from.offset();
            }
            else if (!from.context().isEmpty())
            {
                throw new SplitException(ErrorCode.RESUME_INVALID, 0);
            }
            if (from.context().size() > MAX_DEPTH)
            {
                throw new SplitException(ErrorCode.RESUME_INVALID,
                        from.offset());
            }
            scanner = new ByteScanner(source, startAt,
                    config.readBufferBytes());
        }

        /**
         * {@inheritDoc}
         *
         * @throws SplitException if the XML is malformed or unbalanced,
         *                        nested too deeply, or a record is too
         *                        large
         */
        @Override
        public Optional<SplitRecord> next() throws IOException, SplitException
        {
            while (true)
            {
                if (!scanner.skipTo((byte) '<'))
                {
                    if (!outer.isEmpty())
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_UNBALANCED_ELEMENT,
                                scanner.position());
                    }
                    return Optional.empty();
                }
                long at = scanner.position();
                if (skipMarkup(at))
                {
                    continue;
                }
                scanner.read();
                if (scanner.peek() == '/')
                {
                    scanner.read();
                    byte[] name = readEndTag(at);
                    if (outer.isEmpty()
                            || !Arrays.equals(outer.getLast().name(), name))
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_UNBALANCED_ELEMENT, at);
                    }
                    outer.removeLast();
                    continue;
                }
                StartTag tag = readStartTag(scanner, at,
                        (int) Math.min(max, MAX_CONTEXT_TAG * 64L));
                if (isBoundary(tag))
                {
                    return Optional.of(record(tag));
                }
                if (!tag.selfClosing())
                {
                    if (outer.size() >= MAX_DEPTH)
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_XML_MALFORMED, at);
                    }
                    if (tag.bytes().length > MAX_CONTEXT_TAG)
                    {
                        // A resume re-reads ancestors only up to this size,
                        // so a larger one would fail every restart instead.
                        throw new SplitException(
                                ErrorCode.LIMIT_RECORD_SIZE, at);
                    }
                    outer.add(tag);
                }
            }
        }

        /**
         * Skips a comment, CDATA, PI or DOCTYPE.
         *
         * @param at the offset of its {@code <}
         * @return false if it's an element tag, and nothing was skipped
         * @throws IOException    if the file can't be read
         * @throws SplitException if it is never closed
         */
        private boolean skipMarkup(long at)
                throws IOException, SplitException
        {
            byte[] end;
            if (scanner.lookingAt(COMMENT))
            {
                end = COMMENT_END;
            }
            else if (scanner.lookingAt(CDATA))
            {
                end = CDATA_END;
            }
            else if (scanner.peek(1) == '?')
            {
                end = PI_END;
            }
            else if (scanner.peek(1) == '!')
            {
                skipDeclaration(at);
                return true;
            }
            else
            {
                return false;
            }
            scanner.skip(2);
            if (!scanner.skipPast(end))
            {
                throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
            }
            return true;
        }

        /**
         * Skips {@code <!DOCTYPE ...>}, including an internal subset in
         * brackets.
         *
         * @param at the offset of its {@code <}
         * @throws IOException    if the file can't be read
         * @throws SplitException if it is never closed
         */
        private void skipDeclaration(long at)
                throws IOException, SplitException
        {
            scanner.skip(2);
            int brackets = 0;
            int quote = 0;
            while (true)
            {
                int next = scanner.read();
                if (next < 0)
                {
                    throw new SplitException(ErrorCode.PARSE_XML_MALFORMED,
                            at);
                }
                if (quote != 0)
                {
                    if (next == quote)
                    {
                        quote = 0;
                    }
                }
                else if (next == '"' || next == '\'')
                {
                    quote = next;
                }
                else if (next == '[')
                {
                    brackets++;
                }
                else if (next == ']')
                {
                    brackets--;
                }
                else if (next == '>' && brackets <= 0)
                {
                    return;
                }
            }
        }

        /**
         * After {@code </}: reads the name and the closing {@code >}.
         *
         * @param at the offset of its {@code <}
         * @return the name's bytes
         * @throws IOException    if the file can't be read
         * @throws SplitException if it is malformed
         */
        private byte[] readEndTag(long at) throws IOException, SplitException
        {
            ByteArrayOutputStream name = new ByteArrayOutputStream(32);
            while (!isNameEnd(scanner.peek()))
            {
                name.write(scanner.read());
                if (name.size() > 4096)
                {
                    throw new SplitException(ErrorCode.PARSE_XML_MALFORMED,
                            at);
                }
            }
            while (isSpace(scanner.peek()))
            {
                scanner.read();
            }
            if (scanner.read() != '>' || name.size() == 0)
            {
                throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
            }
            return name.toByteArray();
        }

        /**
         * Tells whether a start tag opens a boundary element.
         *
         * @param tag the start tag
         * @return true if its local name, and namespace if one is required,
         *         match
         * @throws SplitException if its prefix isn't declared
         */
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
                throw new SplitException(ErrorCode.PARSE_XML_MALFORMED,
                        tag.offset());
            }
            return namespace.equals(uri == null ? "" : uri);
        }

        /**
         * The namespace a prefix stands for at a start tag.
         *
         * @param prefix the prefix; empty for the default namespace
         * @param tag    the start tag
         * @return the namespace URI; null if it isn't declared
         */
        private String resolve(String prefix, StartTag tag)
        {
            for (NsDecl declaration : tag.decls())
            {
                if (declaration.prefix().equals(prefix))
                {
                    return declaration.uri();
                }
            }
            for (int index = outer.size() - 1; index >= 0; index--)
            {
                for (NsDecl declaration : outer.get(index).decls())
                {
                    if (declaration.prefix().equals(prefix))
                    {
                        return declaration.uri();
                    }
                }
            }
            return null;
        }

        /**
         * Makes the record a boundary start tag opens: the XML declaration,
         * then the element, with the namespaces in scope from its ancestors
         * added to its start tag.
         *
         * @param tag the boundary start tag
         * @return the record
         * @throws IOException    if the file can't be read
         * @throws SplitException if the element is malformed, unbalanced or
         *                        too large
         */
        private SplitRecord record(StartTag tag)
                throws IOException, SplitException
        {
            long start = tag.offset();
            long end = tag.selfClosing() ? scanner.position() : scanToEnd(tag);
            long[] context = new long[outer.size()];
            for (int index = 0; index < context.length; index++)
            {
                context[index] = outer.get(index).offset();
            }
            ResumePoint resume = ResumePoint.at(end, context);

            Map<String, NsDecl> inScope = new LinkedHashMap<>();
            for (StartTag ancestor : outer)
            {
                for (NsDecl declaration : ancestor.decls())
                {
                    inScope.remove(declaration.prefix());
                    inScope.put(declaration.prefix(), declaration);
                }
            }
            for (NsDecl declaration : tag.decls())
            {
                inScope.remove(declaration.prefix());
            }
            List<SplitRecord.Part> parts = new ArrayList<>(3);
            if (decl.length > 0)
            {
                parts.add(new SplitRecord.Literal(decl));
            }
            if (inScope.isEmpty())
            {
                parts.add(new SplitRecord.SourceRange(start, end - start));
            }
            else
            {
                byte[] tagBytes = tag.bytes();
                ByteArrayOutputStream rewritten = new ByteArrayOutputStream(
                        tagBytes.length + 64 * inScope.size());
                rewritten.write(tagBytes, 0, 1 + tag.nameLength());
                for (NsDecl declaration : inScope.values())
                {
                    rewritten.write(' ');
                    rewritten.writeBytes(declaration.raw());
                }
                rewritten.write(tagBytes, 1 + tag.nameLength(),
                        tagBytes.length - 1 - tag.nameLength());
                parts.add(new SplitRecord.Literal(rewritten.toByteArray()));
                long bodyStart = start + tagBytes.length;
                if (end > bodyStart)
                {
                    parts.add(new SplitRecord.SourceRange(bodyStart,
                            end - bodyStart));
                }
            }
            return new SplitRecord(start, end - start, parts, resume);
        }

        /**
         * Scans from after the boundary start tag to just after its
         * matching end tag.
         *
         * @param root the boundary start tag
         * @return the offset after its end tag
         * @throws IOException    if the file can't be read
         * @throws SplitException if the element is malformed, unbalanced or
         *                        too large
         */
        private long scanToEnd(StartTag root)
                throws IOException, SplitException
        {
            long start = root.offset();
            ArrayDeque<byte[]> names = new ArrayDeque<>();
            names.push(root.name());
            while (true)
            {
                if (!scanner.skipTo((byte) '<'))
                {
                    throw new SplitException(
                            ErrorCode.PARSE_UNBALANCED_ELEMENT, start);
                }
                long at = scanner.position();
                if (at - start > max)
                {
                    throw new SplitException(ErrorCode.LIMIT_RECORD_SIZE,
                            start);
                }
                if (skipMarkup(at))
                {
                    continue;
                }
                scanner.read();
                if (scanner.peek() == '/')
                {
                    scanner.read();
                    if (!Arrays.equals(names.pop(), readEndTag(at)))
                    {
                        throw new SplitException(
                                ErrorCode.PARSE_UNBALANCED_ELEMENT, at);
                    }
                    if (names.isEmpty())
                    {
                        return scanner.position();
                    }
                }
                else
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
         * After {@code <} inside a record: skips the tag.
         *
         * @param at the offset of its {@code <}
         * @return its name, or null if self-closing
         * @throws IOException    if the file can't be read
         * @throws SplitException if it is malformed or never closed
         */
        private byte[] skipStartTag(long at)
                throws IOException, SplitException
        {
            ByteArrayOutputStream name = new ByteArrayOutputStream(32);
            while (!isNameEnd(scanner.peek()))
            {
                name.write(scanner.read());
                if (name.size() > 4096)
                {
                    throw new SplitException(ErrorCode.PARSE_XML_MALFORMED,
                            at);
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
                int next = scanner.read();
                if (next < 0)
                {
                    throw new SplitException(
                            ErrorCode.PARSE_UNBALANCED_ELEMENT, at);
                }
                if (quote != 0)
                {
                    if (next == quote)
                    {
                        quote = 0;
                    }
                }
                else if (next == '"' || next == '\'')
                {
                    quote = next;
                }
                else if (next == '>')
                {
                    return last == '/' ? null : name.toByteArray();
                }
                else if (!isSpace(next))
                {
                    last = next;
                }
            }
        }
    }

    /**
     * After {@code <}: reads a whole start tag into memory and parses its
     * namespace declarations.
     *
     * @param scanner reads the file
     * @param at      the offset of its {@code <}
     * @param cap     the largest tag allowed, in bytes
     * @return the tag
     * @throws IOException    if the file can't be read
     * @throws SplitException if it is malformed, never closed or too large
     */
    static StartTag readStartTag(ByteScanner scanner, long at, int cap)
            throws IOException, SplitException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(128);
        bytes.write('<');
        int quote = 0;
        while (true)
        {
            int next = scanner.read();
            if (next < 0)
            {
                throw new SplitException(ErrorCode.PARSE_UNBALANCED_ELEMENT,
                        at);
            }
            bytes.write(next);
            if (bytes.size() > cap)
            {
                throw new SplitException(ErrorCode.LIMIT_RECORD_SIZE, at);
            }
            if (quote != 0)
            {
                if (next == quote)
                {
                    quote = 0;
                }
            }
            else if (next == '"' || next == '\'')
            {
                quote = next;
            }
            else if (next == '>')
            {
                break;
            }
        }
        return parseStartTag(bytes.toByteArray(), at);
    }

    /**
     * Parses a start tag's name and namespace declarations.
     *
     * @param tag the whole tag, from {@code <} to {@code >}
     * @param at  its source offset
     * @return the tag
     * @throws SplitException if it is malformed
     */
    static StartTag parseStartTag(byte[] tag, long at) throws SplitException
    {
        int position = 1;
        while (position < tag.length && !isNameEnd(tag[position] & 0xff))
        {
            position++;
        }
        int nameLength = position - 1;
        if (nameLength == 0)
        {
            throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
        }
        List<NsDecl> decls = new ArrayList<>(2);
        boolean selfClosing = false;
        while (true)
        {
            while (isSpace(tag[position] & 0xff))
            {
                position++;
            }
            if (tag[position] == '>')
            {
                break;
            }
            if (tag[position] == '/' && tag[position + 1] == '>')
            {
                selfClosing = true;
                break;
            }
            int attributeStart = position;
            while (!isNameEnd(tag[position] & 0xff) && tag[position] != '=')
            {
                position++;
            }
            String attribute = new String(tag, attributeStart,
                    position - attributeStart, UTF_8);
            while (isSpace(tag[position] & 0xff))
            {
                position++;
            }
            if (attribute.isEmpty() || tag[position] != '=')
            {
                throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
            }
            position++;
            while (isSpace(tag[position] & 0xff))
            {
                position++;
            }
            byte quote = tag[position];
            if (quote != '"' && quote != '\'')
            {
                throw new SplitException(ErrorCode.PARSE_XML_MALFORMED, at);
            }
            int valueStart = ++position;
            while (tag[position] != quote)
            {
                position++;
            }
            position++;
            if (attribute.equals("xmlns") || attribute.startsWith("xmlns:"))
            {
                String prefix = attribute.equals("xmlns")
                        ? "" : attribute.substring(6);
                String uri = new String(tag, valueStart,
                        position - 1 - valueStart, UTF_8);
                byte[] raw = Arrays.copyOfRange(tag, attributeStart,
                        position);
                decls.add(new NsDecl(prefix, uri, raw));
            }
        }
        return new StartTag(at, tag, nameLength, selfClosing,
                List.copyOf(decls));
    }

    /**
     * Tells whether a byte is XML whitespace.
     *
     * @param value the byte, or -1
     * @return true for space, tab, LF and CR
     */
    static boolean isSpace(int value)
    {
        return value == ' ' || value == '\t' || value == '\n'
                || value == '\r';
    }

    /**
     * Tells whether a byte ends a name.
     *
     * @param value the byte, or -1 at the end
     * @return true at the end, on whitespace, or on {@code > / =}
     */
    static boolean isNameEnd(int value)
    {
        return value < 0 || isSpace(value) || value == '>' || value == '/'
                || value == '=';
    }
}
