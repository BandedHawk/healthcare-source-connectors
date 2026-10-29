package com.sdh.healthcare.connector.core.convert;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * Real-world C-CDA documents (synthetic or de-identified; see src/test/resources/ccda/README.md): each converts, and
 * nothing is lost. Every attribute value and every piece of text in the XML must appear in the JSON as often as in
 * the XML, apart from namespace declarations and xsi:schemaLocation, which aren't data.
 */
class CcdaSamplesTest {

    @ParameterizedTest
    @ValueSource(strings = {"uscdi-v3-all-mappings.xml", "master-ccd.xml", "alice-newman-uscdi-v3.xml",
        "epic-inpatient-ccd-deidentified.xml", "google-synthea-sample1.xml"})
    void convertsWithoutLosingAnyValue(String name) throws Exception {
        byte[] xml = CcdaConverterTest.resource(name);
        JsonNode json = CcdaConverterTest.convert(xml);
        assertThat(json.fieldNames().next()).isEqualTo("ClinicalDocument");

        Map<String, Integer> expected = new HashMap<>();
        DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
        f.setNamespaceAware(true);
        f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        collect(f.newDocumentBuilder().parse(new ByteArrayInputStream(xml)).getDocumentElement(), expected);
        Map<String, Integer> actual = new HashMap<>();
        collect(json, actual);

        Map<String, Integer> missing = new HashMap<>();
        expected.forEach((v, n) -> {
            int have = actual.getOrDefault(v, 0);
            if (have < n) {
                missing.put(v.length() > 60 ? v.substring(0, 60) + "…" : v, n - have);
            }
        });
        assertThat(missing).as("values in the XML but not in the JSON").isEmpty();
        assertThat(count(json, "nullFlavor")).isEqualTo(countAttributes(xml, "nullFlavor"));
    }

    /** XML values: attributes (typed ones as JSON would write them), whole text of leaf elements, mixed text parts. */
    private static void collect(Element e, Map<String, Integer> out) {
        NamedNodeMap attrs = e.getAttributes();
        for (int i = 0; i < attrs.getLength(); i++) {
            Attr a = (Attr) attrs.item(i);
            String ns = a.getNamespaceURI();
            if ("http://www.w3.org/2000/xmlns/".equals(ns)
                    || ("http://www.w3.org/2001/XMLSchema-instance".equals(ns) && a.getLocalName().endsWith("chemaLocation"))) {
                continue;
            }
            String v = a.getValue();
            if (a.getLocalName().equals("type") && v.contains(":")) {
                v = v.substring(v.indexOf(':') + 1);
            }
            add(out, normalise(v));
        }
        boolean hasChildElements = false;
        StringBuilder leafText = new StringBuilder();
        for (Node c = e.getFirstChild(); c != null; c = c.getNextSibling()) {
            if (c instanceof Element ce) {
                hasChildElements = true;
                collect(ce, out);
            } else if (c.getNodeType() == Node.TEXT_NODE || c.getNodeType() == Node.CDATA_SECTION_NODE) {
                leafText.append(c.getNodeValue());
            }
        }
        if (hasChildElements) {
            for (Node c = e.getFirstChild(); c != null; c = c.getNextSibling()) {
                if ((c.getNodeType() == Node.TEXT_NODE || c.getNodeType() == Node.CDATA_SECTION_NODE)
                        && !c.getNodeValue().isBlank()) {
                    add(out, c.getNodeValue());
                }
            }
        } else if (!leafText.isEmpty() && !leafText.toString().isBlank()) {
            add(out, leafText.toString());
        }
    }

    /** JSON leaf values, as strings. */
    private static void collect(JsonNode n, Map<String, Integer> out) {
        if (n.isObject() || n.isArray()) {
            n.forEach(c -> collect(c, out));
        } else {
            add(out, normalise(n.asText()));
        }
    }

    /** Booleans and integers are typed in JSON; compare them by value. */
    private static String normalise(String v) {
        if (v.equals("1") || v.equals("0")) {
            return v;
        }
        return v.matches("\\+[0-9]+") ? v.substring(1) : v;
    }

    private static void add(Map<String, Integer> m, String v) {
        m.merge(v, 1, Integer::sum);
    }

    private static int count(JsonNode n, String key) {
        int c = 0;
        if (n.isObject()) {
            for (Iterator<Map.Entry<String, JsonNode>> it = n.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                c += (e.getKey().equals(key) ? 1 : 0) + count(e.getValue(), key);
            }
        } else if (n.isArray()) {
            for (JsonNode e : n) {
                c += count(e, key);
            }
        }
        return c;
    }

    private static int countAttributes(byte[] xml, String name) throws Exception {
        DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
        f.setNamespaceAware(true);
        org.w3c.dom.NodeList all = f.newDocumentBuilder().parse(new ByteArrayInputStream(xml))
                .getElementsByTagNameNS("*", "*");
        int c = 0;
        for (int i = 0; i < all.getLength(); i++) {
            c += ((Element) all.item(i)).hasAttribute(name) ? 1 : 0;
        }
        return c;
    }
}
