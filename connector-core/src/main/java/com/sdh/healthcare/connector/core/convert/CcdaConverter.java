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
 * C-CDA document → JSON in the style of Google's Healthcare Data Harmonization XmlToJson tool (design §6.4, mapping
 * {@code ccda/1}), which binds the document to the CDA schema and writes it as nested JSON:
 * <ul>
 *   <li>The document is {@code {"ClinicalDocument": {…}}}; every element is a key with its local name, and every
 *       attribute a key with its name (no prefix).</li>
 *   <li>An element the CDA schema allows to repeat is always an array, even with one occurrence; others are single
 *       values. So the shape doesn't depend on how many there happen to be.</li>
 *   <li>{@code xsi:type} becomes {@code "type"} (e.g. {@code "PQ"}) and selects the element's type for its
 *       children.</li>
 *   <li>An element with only text becomes a string, kept exactly. With attributes or child elements, its text goes
 *       under {@code "value"}: an array of text parts for mixed-content types (the narrative block, ED, names), a
 *       string otherwise. Whitespace-only text between child elements is dropped.</li>
 *   <li>Values are strings, except attributes the schema types as boolean ({@code negationInd},
 *       {@code inversionInd}, BL {@code value}…) or integer (INT {@code value}, {@code qty}…), which are JSON
 *       booleans and numbers. A value that isn't valid for its type stays a string.</li>
 * </ul>
 * Where this deliberately differs from Google's tool, so that nothing is lost (design §6.4):
 * <ul>
 *   <li>{@code nullFlavor} is kept, and empty objects are kept.</li>
 *   <li>Elements and attributes the schema doesn't know are kept (unknown elements as arrays), instead of being
 *       dropped by schema binding.</li>
 *   <li>SDTC extension elements and attributes keep their prefix ({@code "sdtc:raceCode"}), as they can sit beside
 *       a CDA element of the same name. Other foreign-namespace attributes use their local name unless that
 *       collides. {@code xmlns} declarations and {@code xsi:schemaLocation} are not data and are left out.</li>
 *   <li>Keys come in document order, so the output is deterministic.</li>
 * </ul>
 * Mixed content loses the interleaving of text and child elements, as in Google's output: text parts are listed
 * under {@code "value"} and child elements under their own keys.
 */
public final class CcdaConverter implements SectionConverter {
    public static final String MAPPING = "ccda/1";

    private static final String SDTC = "urn:hl7-org:sdtc";
    private static final String XSI = XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI;

    private static final XMLInputFactory FACTORY = factory();

    @Override
    public String format() {
        return CcdaSplitter.FORMAT;
    }

    @Override
    public String mapping() {
        return MAPPING;
    }

    private static ConversionException malformed(XMLStreamException e) {
        int at = e.getLocation() == null ? 0 : Math.max(0, e.getLocation().getCharacterOffset());
        boolean encoding = e.getNestedException() instanceof CharConversionException
                || e.getNestedException() instanceof CharacterCodingException;
        return new ConversionException(encoding ? ErrorCode.CONVERT_INVALID_ENCODING : ErrorCode.CONVERT_XML_MALFORMED,
                at);
    }

    private static XMLInputFactory factory() {
        XMLInputFactory f = XMLInputFactory.newFactory();
        f.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);
        f.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        f.setProperty(XMLInputFactory.IS_COALESCING, true);
        return f;
    }

    /** One element while the document is read. */
    private static final class Node {
        final String key;
        final String type;
        final List<String[]> attrs = new ArrayList<>(4);
        final Map<String, List<Node>> children = new LinkedHashMap<>();
        final List<String> texts = new ArrayList<>(1);

        Node(String key, String type) {
            this.key = key;
            this.type = type;
        }
    }

    @Override
    public byte[] convert(byte[] section, SplitConfig cfg) throws ConversionException {
        CdaModel model = CdaModel.INSTANCE;
        Node root = null;
        XMLStreamReader r = null;
        try {
            synchronized (FACTORY) {
                r = FACTORY.createXMLStreamReader(new ByteArrayInputStream(section));
            }
            Deque<Node> stack = new ArrayDeque<>();
            while (r.hasNext()) {
                switch (r.next()) {
                    case XMLStreamConstants.START_ELEMENT -> {
                        Node parent = stack.peek();
                        String key = elementKey(r.getNamespaceURI(), r.getLocalName(), r.getPrefix());
                        String xsiType = r.getAttributeValue(XSI, "type");
                        Node n = new Node(key, type(model, parent, key, xsiType, r));
                        attributes(r, n);
                        if (parent != null) {
                            parent.children.computeIfAbsent(key, k -> new ArrayList<>(1)).add(n);
                        }
                        stack.push(n);
                    }
                    case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA, XMLStreamConstants.SPACE -> {
                        if (!stack.isEmpty()) {
                            stack.peek().texts.add(r.getText());
                        }
                    }
                    case XMLStreamConstants.END_ELEMENT -> {
                        Node done = stack.pop();
                        if (stack.isEmpty()) {
                            root = done;
                        }
                    }
                    default -> {
                        // comments and processing instructions aren't data
                    }
                }
            }
        } catch (XMLStreamException e) {
            throw malformed(e);
        } catch (RuntimeException e) {
            // Woodstox (on the classpath for Hadoop's Configuration, so it's the StAX implementation found) reports
            // some errors only when the text is read, wrapped in an unchecked WstxLazyException: an undeclared
            // entity such as &nbsp;, for one. Without this it escaped as an unknown error and failed the whole file
            // instead of storing this one section unconverted (found by the demo, design §15.3).
            if (e.getCause() instanceof XMLStreamException x) {
                throw malformed(x);
            }
            throw e;
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (XMLStreamException ignored) {
                    // nothing held
                }
            }
        }
        if (root == null) {
            throw new ConversionException(ErrorCode.CONVERT_XML_MALFORMED, 0);
        }
        JsonWriter w = new JsonWriter(section.length * 2);
        w.beginObject().key(root.key);
        emit(model, root, w);
        w.endObject();
        return w.toBytes();
    }

    private static String elementKey(String ns, String local, String prefix) {
        if (ns == null || ns.isEmpty() || ns.equals("urn:hl7-org:v3")) {
            return local;
        }
        if (ns.equals(SDTC)) {
            return "sdtc:" + local;
        }
        return prefix == null || prefix.isEmpty() ? local : prefix + ":" + local;
    }

    /** The element's schema type: its xsi:type, else what the parent's type declares for it; null if unknown. */
    private static String type(CdaModel model, Node parent, String key, String xsiType, XMLStreamReader r) {
        if (xsiType != null) {
            int colon = xsiType.indexOf(':');
            String local = colon < 0 ? xsiType : xsiType.substring(colon + 1);
            String ns = r.getNamespaceContext().getNamespaceURI(colon < 0 ? "" : xsiType.substring(0, colon));
            return SDTC.equals(ns) ? "sdtc:" + local : local;
        }
        if (parent == null) {
            return key.equals(model.rootElement) ? model.rootType : null;
        }
        CdaModel.Type pt = model.type(parent.type);
        CdaModel.Child c = pt == null ? null : pt.children().get(key);
        return c == null ? null : c.type();
    }

    private static void attributes(XMLStreamReader r, Node n) {
        Set<String> used = new HashSet<>();
        List<String[]> foreign = new ArrayList<>();
        for (int i = 0; i < r.getAttributeCount(); i++) {
            String ns = r.getAttributeNamespace(i);
            String local = r.getAttributeLocalName(i);
            String value = r.getAttributeValue(i);
            String key;
            if (XSI.equals(ns)) {
                if (local.equals("schemaLocation") || local.equals("noNamespaceSchemaLocation")) {
                    continue;
                }
                key = local;
                if (local.equals("type")) {
                    int colon = value.indexOf(':');
                    value = colon < 0 ? value : value.substring(colon + 1);
                }
            } else if (ns == null || ns.isEmpty()) {
                key = local;
            } else if (ns.equals(SDTC)) {
                key = "sdtc:" + local;
            } else if (ns.equals(XMLConstants.XML_NS_URI)) {
                key = "xml:" + local;
            } else {
                // Other namespaces: local name, unless it collides (resolved below, after the plain attributes).
                foreign.add(new String[] {local, r.getAttributePrefix(i) + ":" + local, value});
                continue;
            }
            used.add(key);
            n.attrs.add(new String[] {key, value});
        }
        for (String[] f : foreign) {
            String key = used.add(f[0]) ? f[0] : f[1];
            used.add(key);
            n.attrs.add(new String[] {key, f[2]});
        }
    }

    private static void emit(CdaModel model, Node n, JsonWriter w) {
        CdaModel.Type t = model.type(n.type);
        boolean mixed = t == null || t.mixed();
        boolean text = t == null || t.text();
        if (n.attrs.isEmpty() && n.children.isEmpty()) {
            String all = String.join("", n.texts);
            if (!text && all.isBlank()) {
                w.beginObject().endObject();
            } else {
                w.value(all);
            }
            return;
        }
        w.beginObject();
        Set<String> used = new HashSet<>();
        for (String[] a : n.attrs) {
            String key = n.children.containsKey(a[0]) || used.contains(a[0]) ? "@" + a[0] : a[0];
            used.add(key);
            w.key(key);
            attributeValue(t == null ? null : t.attributes().get(a[0]), a[1], w);
        }
        for (Map.Entry<String, List<Node>> e : n.children.entrySet()) {
            String key = used.contains(e.getKey()) ? "@" + e.getKey() : e.getKey();
            used.add(key);
            w.key(key);
            List<Node> list = e.getValue();
            if (list.size() > 1 || repeats(model, t, e.getKey())) {
                w.beginArray();
                for (Node c : list) {
                    emit(model, c, w);
                }
                w.endArray();
            } else {
                emit(model, list.getFirst(), w);
            }
        }
        String textKey = used.contains("value") ? "#text" : "value";
        if (mixed || !text) {
            List<String> parts = n.texts.stream().filter(s -> !s.isBlank()).toList();
            if (!parts.isEmpty()) {
                w.key(textKey).beginArray();
                parts.forEach(w::value);
                w.endArray();
            }
        } else {
            String all = String.join("", n.texts);
            if (!all.isEmpty()) {
                w.key(textKey).value(all);
            }
        }
        w.endObject();
    }

    private static final java.util.regex.Pattern INTEGER = java.util.regex.Pattern.compile("[+-]?[0-9]{1,18}");

    /**
     * Schema-typed attributes as Google's binding writes them: xs:boolean as true/false, xs:integer as a number.
     * A value that isn't valid for its type stays a string, so nothing is lost.
     */
    private static void attributeValue(CdaModel.Kind kind, String v, JsonWriter w) {
        if (kind == CdaModel.Kind.BOOLEAN && (v.equals("true") || v.equals("false"))) {
            w.literal(v);
        } else if (kind == CdaModel.Kind.BOOLEAN && (v.equals("1") || v.equals("0"))) {
            w.literal(v.equals("1") ? "true" : "false");
        } else if (kind == CdaModel.Kind.INTEGER && INTEGER.matcher(v).matches()) {
            w.literal(Long.toString(Long.parseLong(v.startsWith("+") ? v.substring(1) : v)));
        } else {
            w.value(v);
        }
    }

    /** Unknown parents or children repeat: an array keeps every occurrence whatever the count. */
    private static boolean repeats(CdaModel model, CdaModel.Type parent, String key) {
        if (parent == null) {
            return true;
        }
        CdaModel.Child c = parent.children().get(key);
        return c == null || c.repeats();
    }
}
