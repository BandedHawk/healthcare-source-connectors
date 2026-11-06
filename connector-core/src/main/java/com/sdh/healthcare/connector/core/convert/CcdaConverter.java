/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import com.ctc.wstx.stax.WstxInputFactory;
import com.sdh.healthcare.connector.contract.position.LineCounter;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.builtin.CcdaSplitter;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.ByteArrayInputStream;
import java.io.CharConversionException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamReader;
import org.codehaus.stax2.XMLStreamReader2;

/**
 * C-CDA document → JSON in the style of Google's Healthcare Data Harmonization
 * XmlToJson tool (design §6.4, mapping {@code ccda/2}), which binds the
 * document to the CDA schema and writes it as nested JSON:
 * <ul>
 * <li>The document is {@code {"ClinicalDocument": {…}}}; every element is a key
 * with its local name, and every attribute a key with its name (no
 * prefix).</li>
 * <li>An element the CDA schema allows to repeat is always an array, even with
 * one occurrence; others are single values. So the shape doesn't depend on how
 * many there happen to be.</li>
 * <li>{@code xsi:type} becomes {@code "type"} (e.g. {@code "PQ"}) and selects
 * the element's type for its children.</li>
 * <li>An element with only text becomes a string, kept exactly. With attributes
 * or child elements, its text goes under {@code "value"}: an array of text
 * parts for mixed-content types (the narrative block, ED, names), a string
 * otherwise. Whitespace-only text between child elements is dropped.</li>
 * <li>Values are strings, except attributes the schema types as boolean
 * ({@code negationInd}, {@code inversionInd}, BL {@code value}…) or integer
 * (INT {@code value}, {@code qty}…), which are JSON booleans and numbers. A
 * value that isn't valid for its type stays a string.</li>
 * </ul>
 * Where this deliberately differs from Google's tool, so that nothing is lost
 * (design §6.4):
 * <ul>
 * <li>{@code nullFlavor} is kept, and empty objects are kept.</li>
 * <li>Elements and attributes the schema doesn't know are kept (unknown
 * elements as arrays), instead of being dropped by schema binding.</li>
 * <li>SDTC extension elements and attributes keep their prefix
 * ({@code "sdtc:raceCode"}), as they can sit beside a CDA element of the same
 * name. Other foreign-namespace attributes use their local name unless that
 * collides. {@code xmlns} declarations and {@code xsi:schemaLocation} are not
 * data and are left out.</li>
 * <li>Keys come in document order, so the output is deterministic.</li>
 * <li><b>Every element written as an object says where it starts in the
 * record:</b> {@code "#line"} and {@code "#col"}, numbers from 1, the column
 * in characters, are its first two keys, giving the position of the
 * {@code <} of its start tag. A mapping reports problems there, so an operator
 * can find them ({@code RecordOrigin} turns the position into one in the
 * file). No XML name starts with {@code #}, so the keys never clash with an
 * attribute or a child. An element written as a plain string, or as an empty
 * {@code {}} (which a mapping reads as absent), has no position: its parent's
 * is the nearest. This is what changed from {@code ccda/1}.</li>
 * </ul>
 * Mixed content loses the interleaving of text and child elements, as in
 * Google's output: text parts are listed under {@code "value"} and child
 * elements under their own keys.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 * @see RecordConverter
 * @see CdaModel
 * @see CcdaSplitter
 */
public final class CcdaConverter implements RecordConverter
{

    /**
     * The name and version of the JSON this converter produces, returned by
     * {@link #mapping()}. It travels with every converted record, so whatever
     * reads the JSON (such as the US Core 6.1 Whistle mapping) knows its shape.
     * The version changes only when the JSON shape changes in a way existing
     * readers would get wrong.
     */
    public static final String MAPPING = "ccda/2";
    /**
     * The key of the line where an element starts, from 1, in every element
     * written as an object. The Whistle runtime's check functions read it
     * (its {@code SourceLocation}), by this name.
     */
    static final String LINE_KEY = "#line";
    /**
     * The key of the column where an element starts, from 1, in characters;
     * see {@link #LINE_KEY}.
     */
    static final String COLUMN_KEY = "#col";
    /**
     * The namespace of HL7's SDTC extensions to CDA, the elements and
     * attributes US realm documents add to the base standard (such as
     * {@code sdtc:raceCode}). Names in it are given an {@code sdtc:} prefix in
     * the JSON, whatever prefix the document uses, as the CDA model names them.
     */
    private static final String SDTC = "urn:hl7-org:sdtc";
    /**
     * The XML Schema instance namespace, the one {@code xsi:type} and
     * {@code xsi:schemaLocation} belong to. Used to find an element's
     * {@code xsi:type}, which can change its schema type, and to leave out
     * {@code xsi:schemaLocation}, which isn't data.
     */
    private static final String XSI =
            XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI;
    /**
     * The XML parser factory every document is read with, configured by
     * {@link #factory()}. It is created once and shared, because creating one
     * means searching the class path for a parser. The factory isn't
     * guaranteed to be thread-safe, so {@link #convert} only creates a
     * reader from it while holding its lock; each document then has a reader
     * of its own.
     */
    private static final XMLInputFactory FACTORY = factory();
    /**
     * The deepest element nesting the converter accepts, counting
     * {@code ClinicalDocument} as 1. Real C-CDA documents nest about 17 deep,
     * so this leaves ample room. A document nested deeper is refused as
     * {@code CONVERT_XML_MALFORMED} as soon as it goes past the limit.
     * <p>
     * Without a limit, a small document of nothing but nested elements could
     * build a huge tree and overflow the stack when {@link #emit} writes it
     * out, since that walks the tree recursively. The parsers have depth
     * limits of their own (Woodstox 1000, the JDK 100 through its
     * {@code jdk.xml.maxElementDepth} setting), but they are defaults that
     * this code doesn't set and that can change; this limit holds whichever
     * parser is in use.
     */
    static final int MAX_DEPTH = 200;

    /**
     * Creates the converter. It has no settings of its own.
     * {@link ConverterRegistry#load()} creates it through Java's
     * {@link java.util.ServiceLoader}, which needs this public constructor
     * without parameters.
     */
    public CcdaConverter()
    {
    }

    @Override
    public String format()
    {
        return CcdaSplitter.FORMAT;
    }

    @Override
    public String mapping()
    {
        return MAPPING;
    }

    /**
     * Measured on 16 and 64 MB documents: the document, the tree and the
     * JSON. A document of structured entries needs 2.8 to 4.0 times its size,
     * because it has many elements, and up to about 5.5 if none of its
     * attribute values repeat (real ones repeat most of theirs, which are then
     * shared); one that is mostly an embedded file, such as a base64 PDF,
     * needs about 3.8 times. The structured figure with nothing shared is the
     * one to cover. The element positions of {@code ccda/2} added 6 to 11%
     * (a 64 MB Epic document: 206 to 229 MiB).
     *
     * @return 6
     */
    @Override
    public double memoryFactor()
    {
        return 6;
    }

    /**
     * Turns the XML parser's error into the converter's own
     * {@link ConversionException}, which is how {@link #convert} reports a
     * document it can't read. The record is then stored unconverted, with the
     * error recorded, and the rest of the file carries on.
     * <p>
     * Only two things are kept from the parser's error:
     * <ul>
     * <li><b>What went wrong</b>, as a stable error code:
     * {@code CONVERT_INVALID_ENCODING} when the bytes aren't valid in the
     * document's character encoding (such as invalid UTF-8, see
     * {@link #isEncodingError}), and {@code CONVERT_XML_MALFORMED} for
     * anything else (broken tags, a {@code <!DOCTYPE>}, an undeclared entity
     * such as {@code &amp;nbsp;}).</li>
     * <li><b>Where</b>: the byte position in the record of the point the
     * parser noticed the problem, which can be a few characters after the
     * actual mistake, or {@code 0} if the parser doesn't say. The parser gives
     * a line and column, which {@link #bytePosition} turns into the byte
     * position.</li>
     * </ul>
     * The parser's message is deliberately dropped: it can quote the document,
     * which is patient data, and a {@code ConversionException} must be safe to
     * log.
     *
     * @param exception the parser's error
     * @param record    the record's bytes, to find the byte position in
     * @return the exception for {@link #convert} to throw
     */
    private static ConversionException malformed(XMLStreamException exception,
            byte[] record)
    {
        int at = bytePosition(exception.getLocation(), record);
        return new ConversionException(
                isEncodingError(exception.getNestedException())
                ? ErrorCode.CONVERT_INVALID_ENCODING
                : ErrorCode.CONVERT_XML_MALFORMED,
                at);
    }

    /**
     * Turns a place the parser reports, a line and column, into a byte
     * position in the record, as a {@link ConversionException} gives it.
     * <p>
     * The column is counted in characters, so the record is read as
     * {@link LineCounter} reads it: as UTF-8. For a document in a single-byte
     * character set with characters beyond ASCII on the same line, the
     * position can be early.
     * <p>
     * Woodstox gives column 0 when the document ends early (at the end of
     * its last line); that is taken as the line's start, which is where the
     * document ends when its last line is complete.
     *
     * @param location the parser's location, or {@code null} if it gave none
     * @param record   the record's bytes
     * @return the byte position; 0 if the location is missing or unusable
     */
    private static int bytePosition(Location location, byte[] record)
    {
        if (location == null || location.getLineNumber() < 1)
        {
            return 0;
        }
        return LineCounter.offsetOf(record, new SourcePosition(
                location.getLineNumber(),
                Math.max(1, location.getColumnNumber())));
    }

    /**
     * Whether the parser failed because the bytes aren't valid in the
     * document's character encoding (such as invalid UTF-8), rather than
     * because the XML itself is malformed. The parser reports both as an
     * {@link XMLStreamException}; an encoding failure carries the decoder's
     * exception as its cause.
     *
     * @param cause the parser exception's cause, or {@code null} if it has none
     * @return {@code true} for an encoding failure
     */
    private static boolean isEncodingError(Throwable cause)
    {
        return cause instanceof CharConversionException
                || cause instanceof CharacterCodingException;
    }

    /**
     * Creates and configures the XML parser factory the converter reads every
     * document with. It is made once, when the class loads, and kept in
     * {@link #FACTORY}; {@link #convert} creates a new reader from it for each
     * document. The parser is always Woodstox, chosen by name rather than
     * found on the class path, so the connector and the command-line tools
     * read a document the same way and report the same positions: Woodstox
     * gives the start of each start tag ({@link #startOf}), which the JDK's
     * own parser doesn't. The settings are:
     * <ul>
     * <li><b>Namespace aware.</b> The parser reports each element's and
     * attribute's namespace, which is how CDA, SDTC and {@code xsi} names are
     * told apart ({@link #elementKey}, {@link #attributes}, {@link #type}),
     * whatever prefixes the document uses.</li>
     * <li><b>No DTDs, no external entities.</b> For safety: a document can't
     * make the parser read local files or URLs (an XML external entity
     * attack, such as {@code <!ENTITY e SYSTEM "file:///etc/passwd">}) or
     * expand entities without limit. A {@code <!DOCTYPE>} is skipped: nothing
     * it declares or points to is read, so an external DTD is never fetched.
     * Only XML's built-in entities ({@code &amp;amp;}, {@code &amp;lt;} …) and
     * character references ({@code &amp;#160;}) are understood; any other
     * entity reference, whether declared in a DOCTYPE or an HTML one such as
     * {@code &amp;nbsp;}, makes the document malformed.</li>
     * <li><b>Not coalescing.</b> The parser hands over long text in pieces
     * (Woodstox up to 4,000 characters, the JDK's parser often fewer) rather
     * than as one string. The converter puts adjacent pieces back together
     * into runs itself ({@link Node#text}), so the JSON is the same as if the
     * parser had joined them, but a large run, such as a base64 body, is
     * never held as one string or in one parser buffer: coalescing made that
     * text take about five times its size in memory.</li>
     * </ul>
     *
     * @return the configured factory
     */
    private static XMLInputFactory factory()
    {
        final XMLInputFactory factory = new WstxInputFactory();
        factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES,
                false);
        factory.setProperty(XMLInputFactory.IS_COALESCING, false);
        return factory;
    }

    /**
     * One element of the document, held in memory between reading the XML and
     * writing the JSON.
     * <p>
     * The conversion has two stages. {@link CcdaConverter#convert} first
     * reads the whole document into a tree of {@code Node}s, one per element,
     * and only then does {@link CcdaConverter#emit} write the tree out as
     * JSON. The JSON can't be written while the XML is still being read,
     * because how an element is written depends on things only known once it
     * has been read completely:
     * <ul>
     * <li>whether a child name occurs more than once, which makes it an
     * array;</li>
     * <li>which children share a name, since they are grouped under one key
     * even when other elements come between them;</li>
     * <li>whether an attribute or child already uses {@code "value"}, in which
     * case the element's text goes under {@code "#text"}.</li>
     * </ul>
     * A {@code Node} holds what was read and decided about its element: its
     * JSON key and schema type, its attributes, its children and its text.
     * Values are kept exactly as written; typing them, and grouping the
     * children by key, is left to the writing stage.
     * <p>
     * A document of structured entries has an element for every 60 or so
     * bytes, so a node holds as little as it can: plain arrays instead of
     * lists and maps, nothing at all for what an element doesn't have, and
     * the text run being read kept by the {@link Reading}, not by every node.
     */
    private static final class Node
    {

        /** The element's JSON key, from {@link CcdaConverter#elementKey}. */
        final String key;
        /** The line of the {@code <} of the element's start tag, from 1. */
        final int line;
        /** The column of that {@code <}, from 1, in characters. */
        final int column;
        /**
         * The element's schema type in the model, from
         * {@link CcdaConverter#type}, or {@code null} if the model doesn't
         * know it.
         */
        final String type;
        /**
         * The attributes as key, value, key, value, and so on, in the order
         * {@link CcdaConverter#attributes} records them, with values exactly
         * as written; {@link #NO_ATTRIBUTES} when there are none.
         */
        String[] attributes = NO_ATTRIBUTES;
        /**
         * The child elements in document order, in the first
         * {@link #childCount} places; {@code null} when there are none. Once
         * the element ends, the array is exactly that long.
         */
        Node[] children;
        /** How many child elements have been read. */
        int childCount;
        /**
         * The element's own text, in document order, as runs: a run is the
         * text (character data, CDATA and whitespace) between two child
         * elements, comments or processing instructions. A run is a
         * {@code String}, or a {@code String[]} of the pieces the parser
         * delivered it in if it is longer than
         * {@link CcdaConverter#JOIN_LIMIT}.
         * This field is {@code null} with no runs, the run itself with one, and
         * a {@link Runs} with more.
         */
        Object text;

        /**
         * Starts a node for an element whose start tag has just been read. The
         * key and type are fixed from the start tag; the attributes, children
         * and text start empty and are filled in as the rest of the element is
         * read.
         *
         * @param key    the element's JSON key, from
         *               {@link CcdaConverter#elementKey}
         * @param type   the element's schema type in the model, from
         *               {@link CcdaConverter#type}, or {@code null} if the
         *               model doesn't know it
         * @param start  where the element's start tag starts in the record
         */
        Node(String key, String type, Location start)
        {
            this.key = key;
            this.type = type;
            this.line = Math.max(1, start.getLineNumber());
            this.column = Math.max(1, start.getColumnNumber());
        }

        /**
         * Adds a child element, after those read so far.
         *
         * @param child the child, whose start tag has just been read
         */
        void add(Node child)
        {
            if (children == null)
            {
                children = new Node[2];
            }
            else if (childCount == children.length)
            {
                children = Arrays.copyOf(children, childCount * 2);
            }
            children[childCount++] = child;
        }

        /**
         * Adds a finished run of text, after those read so far.
         *
         * @param run the run: a {@code String}, or a {@code String[]} of pieces
         */
        void addRun(Object run)
        {
            if (text == null)
            {
                text = run;
            }
            else if (text instanceof Runs runs)
            {
                runs.add(run);
            }
            else
            {
                Runs runs = new Runs();
                runs.add(text);
                runs.add(run);
                text = runs;
            }
        }

        /**
         * Trims the children array to its length, once the element's end tag
         * has been read and no more children can come.
         */
        void end()
        {
            if (children != null && childCount < children.length)
            {
                children = Arrays.copyOf(children, childCount);
            }
        }
    }

    /** What {@link Node#attributes} holds for an element without attributes. */
    private static final String[] NO_ATTRIBUTES = new String[0];

    /**
     * The runs of an element that has more than one, in document order: each
     * a {@code String} or a {@code String[]}, as {@link Node#text} describes.
     * A class of its own, so a {@code Node} can tell several runs from one
     * run held in pieces.
     */
    private static final class Runs extends ArrayList<Object>
    {

        /** The version of the serialized form; a list's, never changed. */
        private static final long serialVersionUID = 1L;

        /** Starts empty, with room for the two runs it is made with. */
        Runs()
        {
            super(2);
        }
    }

    /**
     * What {@link CcdaConverter#read} keeps while reading one document, apart
     * from the stack of open elements: the run of text being read, and the
     * whitespace runs and attribute values seen so far.
     * <p>
     * Only the innermost open element can be in a run, because anything other
     * than text ends it, so the run is kept here once instead of in every
     * node.
     * <p>
     * Indented documents have a whitespace run between every two tags, and
     * the same few runs (a line break and some spaces) over and over.
     * Attribute values repeat too: code systems, template IDs, class and mood
     * codes, about three in four values in the test documents. Each distinct
     * short one is kept once and shared, so a repeat costs a reference, not a
     * string of its own. Both tables have a limit, so a hostile document can't
     * grow them without end; past it, values are simply kept unshared.
     */
    private static final class Reading
    {

        /** The longest whitespace run, in characters, that is shared. */
        private static final int SHARED_LENGTH = 256;
        /**
         * The most distinct whitespace runs shared per document. An indented
         * document has a few dozen.
         */
        private static final int SHARED_COUNT = 1024;
        /** The longest attribute value, in characters, that is shared. */
        private static final int VALUE_LENGTH = 64;
        /**
         * The most distinct attribute values shared per document. A 0.5 MB
         * test document has about 1,000 to 1,400; the values that repeat most
         * (code systems, template IDs) are among the first seen.
         */
        private static final int VALUE_COUNT = 16_384;

        /** The element the run belongs to, or {@code null} with no run. */
        private Node owner;
        /** The run's pieces, as the parser delivered them. */
        private final List<String> pieces = new ArrayList<>();
        /** The whitespace runs shared so far, each mapped to itself. */
        private final Map<String, String> whitespace = new HashMap<>();
        /** The attribute values shared so far, each mapped to itself. */
        private final Map<String, String> values = new HashMap<>();

        /** Starts a document: no run, nothing shared yet. */
        Reading()
        {
        }

        /**
         * Adds a piece of text read inside an element, to the current run if
         * the last thing read was text in that element, otherwise as a new
         * run.
         *
         * @param node  the innermost open element
         * @param piece the text as the parser delivered it
         */
        void text(Node node, String piece)
        {
            if (owner != node)
            {
                endRun();
                owner = node;
            }
            pieces.add(piece);
        }

        /**
         * Ends the current run, if there is one, because something other than
         * text has been read, and gives it to its element. A run of several
         * pieces that is no longer than {@link CcdaConverter#JOIN_LIMIT} is
         * joined into one string; a longer one is kept in pieces.
         */
        void endRun()
        {
            if (owner != null)
            {
                Object run;
                if (pieces.size() == 1)
                {
                    run = shared(pieces.getFirst());
                }
                else if (length(pieces) <= JOIN_LIMIT)
                {
                    run = shared(String.join("", pieces));
                }
                else
                {
                    run = pieces.toArray(String[]::new);
                }
                owner.addRun(run);
                owner = null;
                pieces.clear();
            }
        }

        /**
         * The string to keep for a run: the shared copy if it is whitespace
         * that has been seen before (or is now shared), otherwise the run
         * itself.
         *
         * @param run the run, as one string
         * @return the run, or an equal string already kept
         */
        private String shared(String run)
        {
            if (run.length() > SHARED_LENGTH || !run.isBlank())
            {
                return run;
            }
            String kept = whitespace.get(run);
            if (kept == null && whitespace.size() < SHARED_COUNT)
            {
                whitespace.put(run, run);
                kept = run;
            }
            return kept == null ? run : kept;
        }

        /**
         * The string to keep for an attribute value: an equal one already
         * kept, if there is one.
         *
         * @param value the value as the parser gave it
         * @return the value, or an equal string already kept
         */
        String value(String value)
        {
            if (value.length() > VALUE_LENGTH)
            {
                return value;
            }
            String kept = values.get(value);
            if (kept == null && values.size() < VALUE_COUNT)
            {
                values.put(value, value);
                kept = value;
            }
            return kept == null ? value : kept;
        }
    }

    /**
     * The longest run of text, in characters, that is joined into one string
     * once read. Shorter runs are joined so the tree holds one string per run,
     * not one per parser piece; longer ones stay in pieces so they are never
     * copied into one large string.
     */
    static final int JOIN_LIMIT = 65_536;

    @Override
    public byte[] convert(byte[] record, SplitConfig config) throws
            ConversionException
    {
        CdaModel model = CdaModel.INSTANCE;
        Node root = read(model, record);
        // About the size of the document: JSON drops the end tags, and the
        // sixteenth covers escaping (a large base64 body adds two bytes per
        // line for its \n). A large document is written twice, first only to
        // count, so the JSON is held once, in an array of exactly its size.
        return JsonWriter.build(record.length + (record.length >> 4) + 64,
                writer -> write(model, root, writer));
    }

    /**
     * Reads a record's XML into a tree of {@link Node}s, the first stage of
     * {@link #convert}.
     *
     * @param model  the CDA model, to look up each element's type
     * @param record the record's bytes, as given to {@link #convert}
     * @return the document element, with everything inside it
     * @throws ConversionException if the XML is malformed, nested more than
     *                             {@link #MAX_DEPTH} deep, or has no element
     */
    private static Node read(CdaModel model, byte[] record) throws
            ConversionException
    {
        Node root = null;
        XMLStreamReader reader = null;
        try
        {
            synchronized (FACTORY)
            {
                reader = FACTORY.createXMLStreamReader(new ByteArrayInputStream(
                        record));
            }
            Deque<Node> stack = new ArrayDeque<>();
            Reading reading = new Reading();
            while (reader.hasNext())
            {
                int event = reader.next();
                if (event != XMLStreamConstants.CHARACTERS
                        && event != XMLStreamConstants.CDATA
                        && event != XMLStreamConstants.SPACE)
                {
                    reading.endRun();
                }
                switch (event)
                {
                    case XMLStreamConstants.START_ELEMENT ->
                    {
                        Location start = startOf(reader);
                        if (stack.size() >= MAX_DEPTH)
                        {
                            throw new ConversionException(
                                    ErrorCode.CONVERT_XML_MALFORMED,
                                    bytePosition(start, record));
                        }
                        Node parent = stack.peek();
                        String key = elementKey(reader.getNamespaceURI(),
                                reader.
                                        getLocalName(), reader.getPrefix());
                        String xsiType = reader.getAttributeValue(XSI, "type");
                        Node node = new Node(key, type(model, parent, key,
                                xsiType, reader), start);
                        attributes(reader, node, reading);
                        if (parent != null)
                        {
                            parent.add(node);
                        }
                        stack.push(node);
                    }
                    case XMLStreamConstants.CHARACTERS,
                            XMLStreamConstants.CDATA,
                            XMLStreamConstants.SPACE ->
                    {
                        if (!stack.isEmpty())
                        {
                            reading.text(stack.peek(), reader.getText());
                        }
                    }
                    case XMLStreamConstants.END_ELEMENT ->
                    {
                        Node done = stack.pop();
                        done.end();
                        if (stack.isEmpty())
                        {
                            root = done;
                        }
                    }
                    default ->
                    {
                        // comments and processing instructions aren't data
                    }
                }
            }
        }
        catch (XMLStreamException exception)
        {
            throw malformed(exception, record);
        }
        catch (RuntimeException exception)
        {
            // Woodstox reports some errors only when the text is read, wrapped
            // in an unchecked WstxLazyException: an undeclared entity such as
            // &nbsp;, for one. Without this it escaped as an
            // unknown error and failed the whole file instead of storing this
            // one record unconverted (found by the demo, design §15.3).
            if (exception.getCause() instanceof XMLStreamException xmlException)
            {
                throw malformed(xmlException, record);
            }
            throw exception;
        }
        finally
        {
            if (reader != null)
            {
                try
                {
                    reader.close();
                }
                catch (XMLStreamException ignored)
                {
                    // nothing held
                }
            }
        }
        if (root == null)
        {
            throw new ConversionException(ErrorCode.CONVERT_XML_MALFORMED, 0);
        }
        return root;
    }

    /**
     * Where the event the reader is on starts: for a start tag, the position
     * of its {@code <}. Woodstox, the parser {@link #factory()} makes, keeps
     * where each event starts as well as where it ends.
     *
     * @param reader the reader, on a start tag
     * @return the line and column where it starts
     */
    private static Location startOf(XMLStreamReader reader)
    {
        return ((XMLStreamReader2) reader).getLocationInfo().getStartLocation();
    }

    /**
     * Writes a tree read by {@link #read} as JSON, the second stage of
     * {@link #convert}: one object with the document element under its key.
     *
     * @param model  the CDA model, to look up each element's type
     * @param root   the document element
     * @param writer where the JSON is written
     */
    private static void write(CdaModel model, Node root, JsonWriter writer)
    {
        writer.beginObject().key(root.key);
        emit(model, root, writer);
        writer.endObject();
    }

    /**
     * Works out the JSON key an element is written under, from its namespace
     * and name. The key is used three ways: as the element's name in the
     * JSON, to group repeated elements of the same name under one key, and to
     * look the element up in its parent's schema type ({@link #type}), so it
     * must match how the CDA model names elements.
     * <p>
     * The key depends on the element's namespace, not on the prefix the
     * document happens to use for it:
     * <ul>
     * <li><b>CDA's own namespace</b> ({@code urn:hl7-org:v3}), or no
     * namespace: the plain name. {@code <observation>} and
     * {@code <hl7:observation>} both become {@code "observation"}.</li>
     * <li><b>The SDTC extension namespace</b>: the name with an
     * {@code sdtc:} prefix, whatever prefix the document uses for SDTC, as the
     * model names SDTC elements. An SDTC element can sit beside a CDA element
     * of the same name, so the prefix keeps them apart:
     * {@code <raceCode>} becomes {@code "raceCode"} and
     * {@code <sdtc:raceCode>} becomes {@code "sdtc:raceCode"}.</li>
     * <li><b>Any other namespace</b> (a vendor's extension): the name with the
     * prefix the document wrote ({@code "acme:flag"}), or the plain name if it
     * was written without a prefix. Such elements are unknown to the model, so
     * they are written with safe defaults.</li>
     * </ul>
     *
     * @param namespace the element's namespace URI, or {@code null} or empty
     *                  if it has none
     * @param local     the element's name without any prefix
     * @param prefix    the namespace prefix written in the document, or
     *                  {@code null} or empty if there is none
     * @return the element's JSON key
     */
    private static String elementKey(String namespace, String local,
            String prefix)
    {
        if (namespace == null || namespace.isEmpty() || namespace.equals(
                "urn:hl7-org:v3"))
        {
            return local;
        }
        if (namespace.equals(SDTC))
        {
            return "sdtc:" + local;
        }
        return prefix == null || prefix.isEmpty()
                ? local : prefix + ":" + local;
    }

    /**
     * Works out which CDA schema type an element is, as its start tag is read.
     * The answer is kept on the element's {@link Node} and drives how it is
     * written: which of its children repeat ({@link #repeats}), which
     * attributes are booleans or numbers ({@link #attributeValue}), how its
     * text is written, and, in turn, the types of its own children.
     * <p>
     * The type comes from the first of these that applies:
     * <ol>
     * <li><b>Its {@code xsi:type}</b>, when it has one. The document uses it
     * to say exactly what an element is where the schema allows several types:
     * an observation's {@code <value>} is declared as {@code ANY}, and
     * {@code xsi:type="PQ"} makes it a physical quantity. The namespace prefix
     * is removed ({@code "hl7:PQ"} becomes {@code "PQ"}), except that a type
     * from the SDTC namespace is named {@code "sdtc:…"}, as the model names
     * SDTC's types.</li>
     * <li><b>What its parent's type declares</b> for a child of that name: an
     * {@code <observation>} inside an {@code <entry>} is a
     * {@code POCD_MT000040.Observation}.</li>
     * <li><b>For the root element</b>: {@code ClinicalDocument} is
     * {@code POCD_MT000040.ClinicalDocument}.</li>
     * </ol>
     * Otherwise the answer is {@code null}: the element isn't in the schema
     * (an extension), its parent's type is unknown, or the root isn't
     * {@code ClinicalDocument}. An element of unknown type is still converted,
     * with safe defaults (its children as arrays, its text kept), and so is
     * everything inside it, which is also of unknown type unless it has its
     * own {@code xsi:type}. An {@code xsi:type} the model doesn't know is
     * returned as written, and then treated the same way.
     *
     * @param model   the CDA model, generated from HL7's schemas
     * @param parent  the enclosing element, already typed; {@code null} for the
     *                root element
     * @param key     the element's JSON key: its name, {@code sdtc:}-prefixed
     *                for an SDTC element
     * @param xsiType the element's {@code xsi:type} attribute as written
     *                (perhaps with a prefix), or {@code null} if it has none
     * @param reader  the XML reader, positioned on the element's start tag;
     *                used to find which namespace the {@code xsi:type} prefix
     *                stands for
     * @return the type's name in the model, or {@code null} if unknown
     */
    private static String type(CdaModel model, Node parent, String key,
            String xsiType, XMLStreamReader reader)
    {
        if (xsiType != null)
        {
            int colon = xsiType.indexOf(':');
            String local = colon < 0 ? xsiType : xsiType.substring(colon + 1);
            var ns = reader.getNamespaceContext().getNamespaceURI(
                    colon < 0 ? "" : xsiType.substring(0, colon));
            return SDTC.equals(ns) ? "sdtc:" + local : local;
        }
        if (parent != null)
        {
            CdaModel.Type type = model.type(parent.type);
            CdaModel.Child child =
                    type == null ? null : type.children().get(key);
            return child == null ? null : child.type();
        }
        return key.equals(model.rootElement) ? model.rootType : null;
    }

    /**
     * Records the attributes of the element the reader is on, each under the
     * JSON key it will have, with its value exactly as written. Called once for
     * each element as it is read; the values are typed (boolean, number or
     * string) later, by {@link #attributeValue}, when the JSON is written.
     * <p>
     * The key depends on the attribute's namespace:
     * <ul>
     * <li><b>No namespace</b>, as almost all CDA attributes are
     * ({@code classCode}, {@code code}, {@code value}): its name.</li>
     * <li><b>SDTC extensions</b>: its name with an {@code sdtc:} prefix
     * ({@code "sdtc:valueSet"}), because an SDTC attribute can sit beside a CDA
     * attribute of the same name.</li>
     * <li><b>The XML namespace</b>: an {@code xml:} prefix
     * ({@code "xml:lang"}).</li>
     * <li><b>{@code xsi:type}</b>: {@code "type"}, with any namespace prefix
     * removed from its value ({@code "hl7:PQ"} becomes {@code "PQ"}), as
     * Google's tool writes it. Other {@code xsi} attributes keep their
     * name.</li>
     * <li><b>{@code xsi:schemaLocation}</b> and
     * {@code xsi:noNamespaceSchemaLocation}: left out. They say where to
     * find the schema and are not clinical data. ({@code xmlns} namespace
     * declarations never reach this method: the parser handles them.)</li>
     * <li><b>Any other namespace</b> (a vendor's extension): its name without
     * the prefix, unless another attribute already has that name, in which case
     * it keeps the prefix written in the document
     * ({@code "acme:status"}). These are handled after all the others, so a
     * CDA attribute always keeps its plain name, whichever order the
     * attributes come in.</li>
     * </ul>
     * Attributes are recorded in document order, except that those from other
     * namespaces come last.
     *
     * @param reader  the XML reader, positioned on the element's start tag
     * @param node    the element being built; its attributes are stored in
     *                {@link Node#attributes} as key, value, key, value, and so
     *                on
     * @param reading the document being read, whose short repeated values are
     *               shared ({@link Reading#value})
     */
    private static void attributes(XMLStreamReader reader, Node node,
            Reading reading)
    {
        int count = reader.getAttributeCount();
        if (count == 0)
        {
            return;
        }
        String[] attributes = new String[2 * count];
        int size = 0;
        Set<String> used = new HashSet<>();
        List<String[]> foreignList = new ArrayList<>();
        for (int index = 0; index < count; index++)
        {
            String nameSpace = reader.getAttributeNamespace(index);
            String local = reader.getAttributeLocalName(index);
            String value = reading.value(reader.getAttributeValue(index));
            String key;
            if (XSI.equals(nameSpace))
            {
                if (local.equals("schemaLocation") || local.equals(
                        "noNamespaceSchemaLocation"))
                {
                    continue;
                }
                key = local;
                if (local.equals("type"))
                {
                    int colon = value.indexOf(':');
                    value = colon < 0 ? value : value.substring(colon + 1);
                }
            }
            else if (nameSpace == null || nameSpace.isEmpty())
            {
                key = local;
            }
            else if (nameSpace.equals(SDTC))
            {
                key = "sdtc:" + local;
            }
            else if (nameSpace.equals(XMLConstants.XML_NS_URI))
            {
                key = "xml:" + local;
            }
            else
            {
                // Other namespaces: local name, unless it collides
                // (resolved below, after the plain attributes).
                foreignList.add(new String[]
                {
                    local, reader.getAttributePrefix(index) + ":" + local, value
                });
                continue;
            }
            used.add(key);
            attributes[size++] = key;
            attributes[size++] = value;
        }
        for (String[] foreign : foreignList)
        {
            final String key = used.add(foreign[0]) ? foreign[0] : foreign[1];
            used.add(key);
            attributes[size++] = key;
            attributes[size++] = foreign[2];
        }
        // Smaller only when xsi:schemaLocation was left out.
        node.attributes = size == 0 ? NO_ATTRIBUTES
                : size < attributes.length ? Arrays.copyOf(attributes, size)
                : attributes;
    }

    /**
     * Writes one element, and everything inside it, as a JSON value. This is
     * the second half of the conversion: the document has already been read
     * into a tree of {@link Node}s, and {@code emit} walks that tree, starting
     * from {@code ClinicalDocument}, to produce the JSON.
     * <p>
     * It looks up the element's type in the CDA model (the type the schema
     * gives it in its parent, or its {@code xsi:type}), because the type
     * decides how the element is written, then hands the element to one of
     * two methods:
     * <ul>
     * <li>{@link #emitTextOnly} when it has no attributes and no child
     * elements: a plain string such as {@code "Summary"}, or {@code {}}.</li>
     * <li>{@link #emitObject} otherwise: a JSON object of its attributes,
     * child elements and text. That method calls {@code emit} again for each
     * child, so the whole subtree is written.</li>
     * </ul>
     * Only the value is written: the caller has already written the element's
     * key ({@code "ClinicalDocument"}, or the child's name), and decided
     * whether it is one item of an array.
     *
     * @param model  the CDA model: each element's type, children and attribute
     *               types, from HL7's schemas
     * @param node   the element to write, as read from the document; its type
     *               is {@code null} when the model doesn't know the element
     * @param writer where the JSON value is written
     */
    private static void emit(CdaModel model, Node node, JsonWriter writer)
    {
        CdaModel.Type type = model.type(node.type);
        if (node.attributes.length == 0 && node.childCount == 0)
        {
            emitTextOnly(type, node, writer);
        }
        else
        {
            emitObject(model, type, node, writer);
        }
    }

    /**
     * An element's text runs, in document order.
     *
     * @param node the element
     * @return its runs, each a {@code String} or a {@code String[]}; empty if
     *         it has no text
     */
    private static List<Object> runs(Node node)
    {
        return node.text == null ? List.of()
                : node.text instanceof Runs runs ? runs
                : List.of(node.text);
    }

    /**
     * One text run as a list of pieces.
     *
     * @param run the run, a {@code String} or a {@code String[]}
     * @return its pieces
     */
    private static List<String> pieces(Object run)
    {
        return run instanceof String piece ? List.of(piece)
                : Arrays.asList((String[]) run);
    }

    /**
     * All of an element's text, as one list of pieces in document order, to be
     * written as one string. The pieces are not joined, so a large text is
     * never copied into one string.
     *
     * @param node the element
     * @return the pieces of all its runs; empty if it has no text
     */
    private static List<String> allText(Node node)
    {
        return node.text instanceof Runs runs
                ? runs.stream().flatMap(run -> pieces(run).stream()).toList()
                : node.text == null ? List.of() : pieces(node.text);
    }

    /**
     * An element's children grouped by key, in the order each key first
     * appears, each group in document order: the children as they are written,
     * all occurrences of a name under one key.
     *
     * @param node the element, with at least one child
     * @return its children by key
     */
    private static Map<String, List<Node>> groups(Node node)
    {
        Map<String, List<Node>> groups = new LinkedHashMap<>();
        for (Node child : node.children)
        {
            groups.computeIfAbsent(child.key, key -> new ArrayList<>(1))
                    .add(child);
        }
        return groups;
    }

    /**
     * The total length of some pieces of text.
     *
     * @param pieces the pieces
     * @return their combined length in characters
     */
    private static long length(List<String> pieces)
    {
        long length = 0;
        for (String piece : pieces)
        {
            length += piece.length();
        }
        return length;
    }

    /**
     * Whether some pieces of text, taken together, are empty or only
     * whitespace, as {@link String#isBlank()} would say of them joined.
     *
     * @param pieces the pieces
     * @return {@code true} if every piece is blank
     */
    private static boolean isBlank(List<String> pieces)
    {
        for (String piece : pieces)
        {
            if (!piece.isBlank())
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Writes an element that has no attributes and no child elements, only text
     * or nothing at all. It becomes a plain JSON string or an empty object,
     * never an object with a {@code "value"} key:
     * <ul>
     * <li><b>A type that carries text</b> (a title, a table cell, a name part),
     * or a type the model doesn't know: its text as one string, exactly as
     * written, whitespace included. {@code <title>Summary</title>} becomes
     * {@code "Summary"}. An empty one, such as {@code <td/>}, becomes
     * {@code ""}, so it is still there and a table keeps its cell
     * positions.</li>
     * <li><b>A type that carries no text</b> (a structural element such as
     * {@code <patient/>}), when empty or holding only whitespace: {@code {}}.
     * It is kept rather than dropped, as an object, the shape that element
     * always has, so a mapping can still see that it was there.</li>
     * <li><b>A type that carries no text but has some anyway</b> (an invalid
     * document, such as {@code <patient>unknown</patient>}): the text as a
     * string, so nothing in the document is lost.</li>
     * </ul>
     * The text is all of the element's text (character data, CDATA and
     * whitespace) as one string.
     *
     * @param type the element's type in the model, or {@code null} if the model
     * doesn't know it (treated as carrying text)
     * @param node   the element, with its text pieces as read from the document
     * @param writer where the JSON value is written
     */
    private static void emitTextOnly(CdaModel.Type type, Node node,
            JsonWriter writer)
    {
        boolean text = type == null || type.text();
        List<String> all = allText(node);
        if (!text && isBlank(all))
        {
            writer.beginObject().endObject();
        }
        else
        {
            writer.value(all);
        }
    }

    /**
     * Writes an element that has attributes or child elements as a JSON object.
     * The object's keys come in a fixed order, so the same document always
     * gives the same JSON:
     * <ol>
     * <li><b>Attributes</b>, in document order, each under its own name, with
     * its value typed by {@link #attributeValue} (a boolean, a number or a
     * string). If a child element has the same name, the attribute's key gets
     * an {@code @} prefix ({@code "@code"}), so neither is overwritten.</li>
     * <li><b>Child elements</b>, under their names, in the order each name
     * first appears. All occurrences of one name are grouped under one key: as
     * an array when there is more than one or the schema lets that element
     * repeat ({@link #repeats}), otherwise as a single value. Each child is
     * written by {@link #emit}, so the whole subtree is converted
     * recursively.</li>
     * <li><b>The element's own text</b>, if it has any, under {@code "value"}.
     * If an attribute or child already uses {@code "value"} (as in
     * {@code <value value="…">}), the text goes under {@code "#text"}. How the
     * text is written depends on the element's type:
     * <ul>
     * <li>Types that mix text with elements (the narrative block, ED, names),
     * types that carry no text, and types the model doesn't know: an array of
     * the text runs ({@link Node#text}), each as one string. Whitespace-only
     * runs (the indentation between child elements) are dropped.</li>
     * <li>Other types: all the text joined into one string, kept exactly,
     * whitespace included.</li>
     * </ul>
     * Nothing is written when there is no text.</li>
     * </ol>
     * In mixed content, the interleaving of text and elements is lost: text
     * runs are listed under {@code "value"} and elements under their own
     * keys, as in Google's XmlToJson output.
     *
     * @param model the CDA model, passed on to {@link #emit} so each child can
     * look up its own type
     * @param type this element's type in the model, or {@code null} if the
     * model doesn't know it (unknown elements are treated as mixed content and
     * their children as repeating)
     * @param node the element: its attributes, its children,
     * and its text pieces, as read from the document
     * @param writer where the JSON object is written
     */
    private static void emitObject(CdaModel model, CdaModel.Type type,
            Node node, JsonWriter writer)
    {
        boolean mixed = type == null || type.mixed();
        boolean text = type == null || type.text();
        writer.beginObject();
        writer.key(LINE_KEY).literal(Integer.toString(node.line));
        writer.key(COLUMN_KEY).literal(Integer.toString(node.column));
        Map<String, List<Node>> children = node.childCount == 0 ? Map.of()
                : groups(node);
        Set<String> used = new HashSet<>();
        String[] attributes = node.attributes;
        for (int index = 0; index < attributes.length; index += 2)
        {
            String name = attributes[index];
            String key = children.containsKey(name) || used.contains(name)
                    ? "@" + name : name;
            used.add(key);
            writer.key(key);
            attributeValue(type == null ? null : type.attributes().get(name),
                    attributes[index + 1], writer);
        }
        for (Map.Entry<String, List<Node>> entry : children.entrySet())
        {
            var key = used.contains(entry.getKey())
                    ? "@" + entry.getKey() : entry.getKey();
            used.add(key);
            writer.key(key);
            List<Node> list = entry.getValue();
            if (list.size() > 1 || repeats(type, entry.getKey()))
            {
                writer.beginArray();
                for (Node child : list)
                {
                    emit(model, child, writer);
                }
                writer.endArray();
            }
            else
            {
                emit(model, list.getFirst(), writer);
            }
        }
        var textKey = used.contains("value") ? "#text" : "value";
        if (mixed || !text)
        {
            List<List<String>> parts = runs(node).stream()
                    .map(CcdaConverter::pieces)
                    .filter(run -> !isBlank(run))
                    .toList();
            if (!parts.isEmpty())
            {
                writer.key(textKey).beginArray();
                parts.forEach(writer::value);
                writer.endArray();
            }
        }
        else
        {
            List<String> all = allText(node);
            if (length(all) > 0)
            {
                writer.key(textKey).value(all);
            }
        }
        writer.endObject();
    }

    /**
     * The attribute values {@link #attributeValue} writes as a JSON number when
     * the schema types the attribute as an integer (INT {@code value},
     * {@code qty} and the like): an optional {@code +} or {@code -} followed by
     * 1 to 18 digits, and nothing else. It is used with
     * {@link java.util.regex.Matcher#matches()}, so it must match the whole
     * value.
     * <p>
     * This is the lexical form XML Schema allows for {@code xs:integer} (sign,
     * digits, leading zeros allowed), with two deliberate limits:
     * <ul>
     * <li><b>At most 18 digits.</b> The largest {@code long} has 19 digits
     * (9,223,372,036,854,775,807), so any 18-digit value fits and
     * {@link Long#parseLong} can never overflow or throw. A longer value is
     * valid in the schema but stays a JSON string rather than being converted
     * with a risk of error or lost precision. No real CDA count or quantity is
     * that large.</li>
     * <li><b>No surrounding whitespace.</b> XML Schema would trim {@code " 5"}
     * to {@code 5}; here it stays the string {@code " 5"}, so the value is kept
     * exactly as the document has it.</li>
     * </ul>
     * Anything that doesn't match
     * ({@code "3.5"}, {@code "1e3"}, {@code "five"}, an empty value) is written
     * as a string, so an invalid value is kept rather than lost.
     */
    private static final Pattern INTEGER =
            Pattern.compile("[+-]?[0-9]{1,18}");

    /**
     * Writes one attribute's value as JSON, as a JSON Boolean or number when
     * the CDA schema says the attribute is one, and as a string otherwise.
     * <p>
     * In XML every attribute value is text: {@code negationInd="true"} and
     * {@code value="3"} are strings like any other. Google's XmlToJson tool
     * binds the document to the CDA schema first, so attributes the schema
     * types as {@code xs:boolean} or {@code xs:integer} come out as real JSON
     * values, and mappings written for it compare them as such
     * ({@code negationInd == true}, not {@code == "true"}). This does the same,
     * using the attribute's {@link CdaModel.Kind} from the model:
     * <ul>
     * <li>{@code BOOLEAN}: {@code "true"} or {@code "1"} is written as
     * {@code true}, and {@code "false"} or {@code "0"} as {@code false} (the
     * four spellings XML Schema allows).</li>
     * <li>{@code INTEGER}: an optional sign and up to 18 digits is written as a
     * number in its plain form, so {@code "+007"} becomes {@code 7}. 18 digits
     * always fit in a {@code long}.</li>
     * <li>Anything else, including every attribute the schema types as text or
     * doesn't know, is written as a string, exactly as it appears in the
     * document.</li>
     * </ul>
     * A value that isn't valid for its type, such as {@code negationInd="yes"}
     * or {@code value="3.5"} on an integer, also stays a string rather than
     * being dropped or guessed at. The document is wrong, but nothing in it is
     * lost, and a mapping can still see it and report it.
     *
     * @param kind the attribute's type in the CDA schema, or {@code null} if it
     * isn't boolean or integer, or the model doesn't know it
     * @param rawValue the attribute's value exactly as written in the document
     * (after the XML parser has resolved entities such as {@code &amp;amp;}):
     * always text, whatever the schema type
     * @param writer where the JSON value is written
     */
    private static void attributeValue(CdaModel.Kind kind, String rawValue,
            JsonWriter writer)
    {
        switch (kind)
        {
            case BOOLEAN ->
            {
                switch (rawValue)
                {
                    case "true", "1" ->
                        writer.literal("true");
                    case "false", "0" ->
                        writer.literal("false");
                    default ->
                        writer.value(rawValue);
                }
            }
            case INTEGER ->
            {
                if (INTEGER.matcher(rawValue).matches())
                {
                    // parseLong accepts a leading '+'; toString drops it and
                    // any leading zeros.
                    writer.literal(Long.toString(Long.parseLong(rawValue)));
                }
                else
                {
                    writer.value(rawValue);
                }
            }
            case null ->
                writer.value(rawValue);
        }
    }

    /**
     * Whether a child element is written as a JSON array, according to the CDA
     * schema rather than to how many times it occurs in this document.
     * <p>
     * An element the schema lets repeat ({@code maxOccurs > 1}) is always an
     * array, even with a single occurrence: {@code ClinicalDocument/templateId}
     * becomes {@code "templateId": [{…}]} whether the document has one or five,
     * while {@code code}, which occurs at most once, becomes
     * {@code "code": {…}}. So the JSON shape of an element is fixed by its
     * place in the schema, and a mapping can address it the same way in every
     * document (a Whistle mapping iterates {@code templateId[]} without first
     * checking whether it got an object or an array).
     * <p>
     * When the schema can't say, the answer is {@code true}: the parent's type
     * isn't in {@link CdaModel} (an element the model doesn't know, or an
     * unrecognised {@code xsi:type}), or the type has no child of that name (a
     * non-CDA extension). An array keeps every occurrence whatever the count,
     * so nothing is lost, and the shape of an unknown element still doesn't
     * change from document to document.
     * <p>
     * Only consulted for a single occurrence: {@link #emit} writes an array
     * whenever an element occurs more than once, whatever the schema says, so a
     * document that breaks the schema loses nothing either.
     *
     * @param parent the parent element's type in the model, or {@code null} if
     * the model doesn't know it
     * @param key the child element's JSON key: its local name, or
     * {@code sdtc:}-prefixed for an SDTC extension
     * @return {@code true} to write the child as an array; {@code false} for a
     * single value
     */
    private static boolean repeats(final CdaModel.Type parent, final String key)
    {
        var repeats = true;
        if (parent != null)
        {
            final var child = parent.children().get(key);
            repeats = child == null || child.repeats();
        }
        return repeats;
    }
}
