/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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
import org.w3c.dom.NodeList;

/**
 * Real-world C-CDA documents (synthetic or de-identified; see
 * src/test/resources/ccda/README.md): each converts, and nothing is lost.
 * Every attribute value and every piece of text in the XML must appear in
 * the JSON as often as in the XML, apart from namespace declarations and
 * xsi:schemaLocation, which aren't data.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class CcdaSamplesTest
{

    /**
     * A sample converts, and every value in it appears in the JSON.
     *
     * @param name the sample's resource name
     * @throws Exception if the test fails to run
     */
    @ParameterizedTest
    @ValueSource(strings = {"uscdi-v3-all-mappings.xml", "master-ccd.xml",
        "alice-newman-uscdi-v3.xml", "epic-inpatient-ccd-deidentified.xml",
        "google-synthea-sample1.xml"})
    void convertsWithoutLosingAnyValue(String name) throws Exception
    {
        byte[] xml = CcdaConverterTest.resource(name);
        JsonNode json = CcdaConverterTest.convert(xml);
        assertThat(json.fieldNames().next()).isEqualTo("ClinicalDocument");

        Map<String, Integer> expected = new HashMap<>();
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature(
                "http://apache.org/xml/features/disallow-doctype-decl", true);
        collect(factory.newDocumentBuilder()
                .parse(new ByteArrayInputStream(xml)).getDocumentElement(),
                expected);
        Map<String, Integer> actual = new HashMap<>();
        collect(json, actual);

        Map<String, Integer> missing = new HashMap<>();
        expected.forEach((value, count) ->
        {
            int have = actual.getOrDefault(value, 0);
            if (have < count)
            {
                missing.put(value.length() > 60
                        ? value.substring(0, 60) + "…" : value,
                        count - have);
            }
        });
        assertThat(missing).as("values in the XML but not in the JSON")
                .isEmpty();
        assertThat(count(json, "nullFlavor"))
                .isEqualTo(countAttributes(xml, "nullFlavor"));
    }

    /**
     * Collects XML values: attributes (typed ones as JSON would write
     * them), the whole text of leaf elements, and mixed text parts.
     *
     * @param element the element
     * @param out     counts by value
     */
    private static void collect(Element element, Map<String, Integer> out)
    {
        NamedNodeMap attributes = element.getAttributes();
        for (int index = 0; index < attributes.getLength(); index++)
        {
            Attr attribute = (Attr) attributes.item(index);
            String namespace = attribute.getNamespaceURI();
            if ("http://www.w3.org/2000/xmlns/".equals(namespace)
                    || ("http://www.w3.org/2001/XMLSchema-instance"
                            .equals(namespace)
                            && attribute.getLocalName()
                                    .endsWith("chemaLocation")))
            {
                continue;
            }
            String value = attribute.getValue();
            if (attribute.getLocalName().equals("type")
                    && value.contains(":"))
            {
                value = value.substring(value.indexOf(':') + 1);
            }
            add(out, normalise(value));
        }
        boolean hasChildElements = false;
        StringBuilder leafText = new StringBuilder();
        for (Node child = element.getFirstChild(); child != null;
                child = child.getNextSibling())
        {
            if (child instanceof Element childElement)
            {
                hasChildElements = true;
                collect(childElement, out);
            }
            else if (child.getNodeType() == Node.TEXT_NODE
                    || child.getNodeType() == Node.CDATA_SECTION_NODE)
            {
                leafText.append(child.getNodeValue());
            }
        }
        if (hasChildElements)
        {
            for (Node child = element.getFirstChild(); child != null;
                    child = child.getNextSibling())
            {
                if ((child.getNodeType() == Node.TEXT_NODE
                        || child.getNodeType() == Node.CDATA_SECTION_NODE)
                        && !child.getNodeValue().isBlank())
                {
                    add(out, child.getNodeValue());
                }
            }
        }
        else if (!leafText.isEmpty() && !leafText.toString().isBlank())
        {
            add(out, leafText.toString());
        }
    }

    /**
     * Collects JSON leaf values, as strings.
     *
     * @param node the node
     * @param out  counts by value
     */
    private static void collect(JsonNode node, Map<String, Integer> out)
    {
        if (node.isObject() || node.isArray())
        {
            node.forEach(child -> collect(child, out));
        }
        else
        {
            add(out, normalise(node.asText()));
        }
    }

    /**
     * Booleans and integers are typed in JSON; compare them by value.
     *
     * @param value the value
     * @return it, without a leading {@code +} on an integer
     */
    private static String normalise(String value)
    {
        if (value.equals("1") || value.equals("0"))
        {
            return value;
        }
        return value.matches("\\+[0-9]+") ? value.substring(1) : value;
    }

    /**
     * Counts a value.
     *
     * @param counts counts by value
     * @param value  the value
     */
    private static void add(Map<String, Integer> counts, String value)
    {
        counts.merge(value, 1, Integer::sum);
    }

    /**
     * How many times a key appears in JSON.
     *
     * @param node the JSON
     * @param key  the key
     * @return the count
     */
    private static int count(JsonNode node, String key)
    {
        int count = 0;
        if (node.isObject())
        {
            for (Iterator<Map.Entry<String, JsonNode>> fields =
                    node.fields(); fields.hasNext(); )
            {
                Map.Entry<String, JsonNode> field = fields.next();
                count += (field.getKey().equals(key) ? 1 : 0)
                        + count(field.getValue(), key);
            }
        }
        else if (node.isArray())
        {
            for (JsonNode element : node)
            {
                count += count(element, key);
            }
        }
        return count;
    }

    /**
     * How many elements in XML have an attribute.
     *
     * @param xml  the XML
     * @param name the attribute's name
     * @return the count
     * @throws Exception if the XML can't be parsed
     */
    private static int countAttributes(byte[] xml, String name)
            throws Exception
    {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        NodeList all = factory.newDocumentBuilder()
                .parse(new ByteArrayInputStream(xml))
                .getElementsByTagNameNS("*", "*");
        int count = 0;
        for (int index = 0; index < all.getLength(); index++)
        {
            count += ((Element) all.item(index)).hasAttribute(name) ? 1 : 0;
        }
        return count;
    }
}
