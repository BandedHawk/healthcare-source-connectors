package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.builtin.CcdaSplitter;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.ByteArrayInputStream;
import java.io.CharConversionException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * C-CDA document → JSON in the style of Google's Healthcare Data Harmonization
 * XmlToJson tool (design §6.4, mapping {@code ccda/1}), which binds the
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
 * </ul>
 * Mixed content loses the interleaving of text and child elements, as in
 * Google's output: text parts are listed under {@code "value"} and child
 * elements under their own keys.
 */
public final class CcdaConverter implements SectionConverter
{

    public static final String MAPPING = "ccda/1";
    private static final String SDTC = "urn:hl7-org:sdtc";
    private static final String XSI = XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI;
    private static final XMLInputFactory FACTORY = factory();

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

    private static ConversionException malformed(XMLStreamException exception)
    {
        int at = exception.getLocation() == null ? 0 : Math.max(0, exception.
                getLocation().
                getCharacterOffset());
        boolean encoding = exception.getNestedException() instanceof CharConversionException
                || exception.getNestedException() instanceof CharacterCodingException;
        return new ConversionException(
                encoding ? ErrorCode.CONVERT_INVALID_ENCODING : ErrorCode.CONVERT_XML_MALFORMED,
                at);
    }

    private static XMLInputFactory factory()
    {
        final var factory = XMLInputFactory.newFactory();
        factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES,
                false);
        factory.setProperty(XMLInputFactory.IS_COALESCING, true);
        return factory;
    }

    /**
     * One element while the document is read.
     */
    private static final class Node
    {

        final String key;
        final String type;
        final List<String[]> attrs = new ArrayList<>(4);
        final Map<String, List<Node>> children = new LinkedHashMap<>();
        final List<String> texts = new ArrayList<>(1);

        Node(String key, String type)
        {
            this.key = key;
            this.type = type;
        }
    }

    @Override
    public byte[] convert(byte[] section, SplitConfig config) throws
            ConversionException
    {
        CdaModel model = CdaModel.INSTANCE;
        Node root = null;
        XMLStreamReader reader = null;
        try
        {
            synchronized (FACTORY)
            {
                reader = FACTORY.createXMLStreamReader(new ByteArrayInputStream(
                        section));
            }
            Deque<Node> stack = new ArrayDeque<>();
            while (reader.hasNext())
            {
                switch (reader.next())
                {
                    case XMLStreamConstants.START_ELEMENT ->
                    {
                        Node parent = stack.peek();
                        String key = elementKey(reader.getNamespaceURI(),
                                reader.
                                        getLocalName(), reader.getPrefix());
                        String xsiType = reader.getAttributeValue(XSI, "type");
                        Node node = new Node(key, type(model, parent, key,
                                xsiType,
                                reader));
                        attributes(reader, node);
                        if (parent != null)
                        {
                            parent.children.computeIfAbsent(key,
                                    k -> new ArrayList<>(1)).add(node);
                        }
                        stack.push(node);
                    }
                    case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA, XMLStreamConstants.SPACE ->
                    {
                        if (!stack.isEmpty())
                        {
                            stack.peek().texts.add(reader.getText());
                        }
                    }
                    case XMLStreamConstants.END_ELEMENT ->
                    {
                        Node done = stack.pop();
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
        catch (XMLStreamException e)
        {
            throw malformed(e);
        }
        catch (RuntimeException e)
        {
            // Woodstox (on the classpath for Hadoop's Configuration, so it's the StAX implementation found) reports
            // some errors only when the text is read, wrapped in an unchecked WstxLazyException: an undeclared
            // entity such as &nbsp;, for one. Without this it escaped as an unknown error and failed the whole file
            // instead of storing this one section unconverted (found by the demo, design §15.3).
            if (e.getCause() instanceof XMLStreamException x)
            {
                throw malformed(x);
            }
            throw e;
        } finally
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
        JsonWriter writer = new JsonWriter(section.length * 2);
        writer.beginObject().key(root.key);
        emit(model, root, writer);
        writer.endObject();
        return writer.toBytes();
    }

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
        return prefix == null || prefix.isEmpty() ? local : prefix + ":" + local;
    }

    /**
     * The element's schema type: its xsi:type, else what the parent's type
     * declares for it; null if unknown.
     */
    private static String type(CdaModel model, Node parent, String key,
            String xsiType, XMLStreamReader reader)
    {
        if (xsiType != null)
        {
            int colon = xsiType.indexOf(':');
            String local = colon < 0 ? xsiType : xsiType.substring(colon + 1);
            String ns = reader.getNamespaceContext().getNamespaceURI(
                    colon < 0 ? "" : xsiType.substring(0, colon));
            return SDTC.equals(ns) ? "sdtc:" + local : local;
        }
        if (parent == null)
        {
            return key.equals(model.rootElement) ? model.rootType : null;
        }
        CdaModel.Type type = model.type(parent.type);
        CdaModel.Child child = type == null ? null : type.children().get(key);
        return child == null ? null : child.type();
    }

    private static void attributes(XMLStreamReader reader, Node node)
    {
        Set<String> used = new HashSet<>();
        List<String[]> foreign = new ArrayList<>();
        for (int i = 0; i < reader.getAttributeCount(); i++)
        {
            String ns = reader.getAttributeNamespace(i);
            String local = reader.getAttributeLocalName(i);
            String value = reader.getAttributeValue(i);
            String key;
            if (XSI.equals(ns))
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
            else if (ns == null || ns.isEmpty())
            {
                key = local;
            }
            else if (ns.equals(SDTC))
            {
                key = "sdtc:" + local;
            }
            else if (ns.equals(XMLConstants.XML_NS_URI))
            {
                key = "xml:" + local;
            }
            else
            {
                // Other namespaces: local name, unless it collides (resolved below, after the plain attributes).
                foreign.add(new String[]
                {
                    local, reader.getAttributePrefix(i) + ":" + local, value
                });
                continue;
            }
            used.add(key);
            node.attrs.add(new String[]
            {
                key, value
            });
        }
        for (String[] f : foreign)
        {
            final String key = used.add(f[0]) ? f[0] : f[1];
            used.add(key);
            node.attrs.add(new String[]
            {
                key, f[2]
            });
        }
    }

    private static void emit(CdaModel model, Node node, JsonWriter writer)
    {
        CdaModel.Type type = model.type(node.type);
        boolean mixed = type == null || type.mixed();
        boolean text = type == null || type.text();
        if (node.attrs.isEmpty() && node.children.isEmpty())
        {
            String all = String.join("", node.texts);
            if (!text && all.isBlank())
            {
                writer.beginObject().endObject();
            }
            else
            {
                writer.value(all);
            }
            return;
        }
        writer.beginObject();
        Set<String> used = new HashSet<>();
        for (String[] a : node.attrs)
        {
            String key = node.children.containsKey(a[0]) || used.contains(a[0]) ? "@" + a[0] : a[0];
            used.add(key);
            writer.key(key);
            attributeValue(type == null ? null : type.attributes().get(a[0]),
                    a[1], writer);
        }
        for (Map.Entry<String, List<Node>> e : node.children.entrySet())
        {
            String key = used.contains(e.getKey()) ? "@" + e.getKey() : e.
                    getKey();
            used.add(key);
            writer.key(key);
            List<Node> list = e.getValue();
            if (list.size() > 1 || repeats(type, e.getKey()))
            {
                writer.beginArray();
                for (Node c : list)
                {
                    emit(model, c, writer);
                }
                writer.endArray();
            }
            else
            {
                emit(model, list.getFirst(), writer);
            }
        }
        String textKey = used.contains("value") ? "#text" : "value";
        if (mixed || !text)
        {
            List<String> parts = node.texts.stream().filter(s -> !s.isBlank()).
                    toList();
            if (!parts.isEmpty())
            {
                writer.key(textKey).beginArray();
                parts.forEach(writer::value);
                writer.endArray();
            }
        }
        else
        {
            String all = String.join("", node.texts);
            if (!all.isEmpty())
            {
                writer.key(textKey).value(all);
            }
        }
        writer.endObject();
    }

    private static final java.util.regex.Pattern INTEGER = java.util.regex.Pattern.
            compile("[+-]?[0-9]{1,18}");

    /**
     * Writes one attribute's value as JSON, as a JSON boolean or number when the
     * CDA schema says the attribute is one, and as a string otherwise.
     * <p>
     * In XML every attribute value is text: {@code negationInd="true"} and
     * {@code value="3"} are strings like any other. Google's XmlToJson tool
     * binds the document to the CDA schema first, so attributes the schema types
     * as {@code xs:boolean} or {@code xs:integer} come out as real JSON values,
     * and mappings written for it compare them as such
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
     * @param kind     the attribute's type in the CDA schema, or {@code null} if
     *                 it isn't boolean or integer, or the model doesn't know it
     * @param rawValue the attribute's value exactly as written in the document
     *                 (after the XML parser has resolved entities such as
     *                 {@code &amp;amp;}): always text, whatever the schema type
     * @param writer   where the JSON value is written
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
                    case "true", "1" -> writer.literal("true");
                    case "false", "0" -> writer.literal("false");
                    default -> writer.value(rawValue);
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
            case null -> writer.value(rawValue);
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
     * {@code "code": {…}}. So the JSON shape of an element is fixed by its place
     * in the schema, and a mapping can address it the same way in every
     * document (a Whistle mapping iterates {@code templateId[]} without first
     * checking whether it got an object or an array).
     * <p>
     * When the schema can't say, the answer is {@code true}: the parent's type
     * isn't in {@link CdaModel} (an element the model doesn't know, or an
     * unrecognised {@code xsi:type}), or the type has no child of that name (a
     * non-CDA extension). An array keeps every occurrence whatever the count, so
     * nothing is lost, and the shape of an unknown element still doesn't change
     * from document to document.
     * <p>
     * Only consulted for a single occurrence: {@link #emit} writes an array
     * whenever an element occurs more than once, whatever the schema says, so a
     * document that breaks the schema loses nothing either.
     *
     * @param parent the parent element's type in the model, or {@code null} if
     *               the model doesn't know it
     * @param key    the child element's JSON key: its local name, or
     *               {@code sdtc:}-prefixed for an SDTC extension
     * @return {@code true} to write the child as an array; {@code false} for a
     *         single value
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
