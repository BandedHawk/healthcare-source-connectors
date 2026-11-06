/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sun.net.httpserver.HttpServer;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.xml.stream.XMLInputFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Tests for {@link CcdaConverter}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class CcdaConverterTest
{

    /** Reads the converter's output and builds the expected JSON. */
    static final ObjectMapper JSON = new ObjectMapper();

    /** The converter under test. */
    static final CcdaConverter CONVERTER = new CcdaConverter();

    /** The default C-CDA settings. */
    static final SplitConfig CONFIG = SplitConfig.defaults("ccda");

    /**
     * The start of a {@code ClinicalDocument} tag that declares the HL7, XML
     * Schema instance and XInclude namespaces. The tag is left open, so a
     * test can add attributes before closing it with {@code >}.
     */
    static final String ROOT = "<ClinicalDocument xmlns=\"urn:hl7-org:v3\""
            + " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
            + " xmlns:xi=\"http://www.w3.org/2001/XInclude\"";

    /** Creates the tests; JUnit runs each test on a new instance. */
    CcdaConverterTest()
    {
    }

    /**
     * Converts a document and returns the shape of the JSON: the converter's
     * output without the element positions, which tests of shape ignore.
     *
     * @param xml the document
     * @return the JSON, without positions
     * @throws Exception if the document can't be converted
     */
    static JsonNode convert(byte[] xml) throws Exception
    {
        return withoutPositions(convertWithPositions(xml));
    }

    /**
     * Converts a document and returns the converter's output as it is,
     * element positions included.
     *
     * @param xml the document
     * @return the JSON
     * @throws Exception if the document can't be converted
     */
    static JsonNode convertWithPositions(byte[] xml) throws Exception
    {
        return JSON.readTree(CONVERTER.convert(xml, CONFIG));
    }

    /**
     * Copies JSON without its {@code #line} and {@code #col} keys.
     *
     * @param node the JSON
     * @return a copy without the position keys, at any depth
     */
    static JsonNode withoutPositions(JsonNode node)
    {
        if (node instanceof ObjectNode object)
        {
            ObjectNode copy = JSON.createObjectNode();
            object.fields().forEachRemaining(field ->
            {
                if (!field.getKey().equals(CcdaConverter.LINE_KEY)
                        && !field.getKey().equals(CcdaConverter.COLUMN_KEY))
                {
                    copy.set(field.getKey(),
                            withoutPositions(field.getValue()));
                }
            });
            return copy;
        }
        if (node instanceof ArrayNode array)
        {
            ArrayNode copy = JSON.createArrayNode();
            array.forEach(element -> copy.add(withoutPositions(element)));
            return copy;
        }
        return node;
    }

    /**
     * Reads a test document from {@code src/test/resources/ccda}.
     *
     * @param name the file's name
     * @return its bytes
     * @throws Exception if the file can't be read
     */
    static byte[] resource(String name) throws Exception
    {
        try (InputStream input = CcdaConverterTest.class
                .getResourceAsStream("/ccda/" + name))
        {
            assertThat(input).as(name).isNotNull();
            return input.readAllBytes();
        }
    }

    /**
     * What Google's tool would make of our output: {@code nullFlavor}
     * deleted, then empty objects and arrays removed. Our output writes an
     * empty element ({@code <td/>}) as {@code ""}, to keep table cell
     * positions. Google's binding makes it an empty object, which its
     * post-processor then removes, so {@code ""} is removed here too.
     * Everything else must already match Google's output.
     *
     * @param node our JSON
     * @return the JSON as Google's tool would write it
     */
    static JsonNode googleView(JsonNode node)
    {
        if (node instanceof ObjectNode object)
        {
            ObjectNode copy = JSON.createObjectNode();
            for (Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
                    fields.hasNext(); )
            {
                Map.Entry<String, JsonNode> field = fields.next();
                if (field.getKey().equals("nullFlavor"))
                {
                    continue;
                }
                JsonNode value = googleView(field.getValue());
                if (!isEmptyContainer(value))
                {
                    copy.set(field.getKey(), value);
                }
            }
            return copy;
        }
        if (node instanceof ArrayNode array)
        {
            ArrayNode copy = JSON.createArrayNode();
            for (JsonNode element : array)
            {
                JsonNode value = googleView(element);
                if (!isEmptyContainer(value))
                {
                    copy.add(value);
                }
            }
            return copy;
        }
        return node;
    }

    /**
     * Tells whether Google's post-processor would remove a value.
     *
     * @param node the value
     * @return true for an empty object, array or string
     */
    static boolean isEmptyContainer(JsonNode node)
    {
        return ((node.isObject() || node.isArray()) && node.isEmpty())
                || (node.isTextual() && node.asText().isEmpty());
    }

    /**
     * Finds the first path where two trees differ, for a readable failure.
     *
     * @param ours   our JSON
     * @param theirs Google's JSON
     * @param path   the path to both trees, as {@code $.a.b[0]}
     * @return a description of the first difference; null if they are equal
     */
    static String firstDifference(JsonNode ours, JsonNode theirs, String path)
    {
        if (ours.equals(theirs))
        {
            return null;
        }
        if (ours.isObject() && theirs.isObject())
        {
            List<String> keys = new ArrayList<>();
            ours.fieldNames().forEachRemaining(keys::add);
            theirs.fieldNames().forEachRemaining(key ->
            {
                if (!keys.contains(key))
                {
                    keys.add(key);
                }
            });
            for (String key : keys)
            {
                if (!ours.has(key) || !theirs.has(key))
                {
                    return path + "." + key
                            + (ours.has(key) ? " only in ours: "
                                    : " only in Google's: ")
                            + abbreviate(ours.has(key) ? ours.get(key)
                                    : theirs.get(key));
                }
                String difference = firstDifference(ours.get(key),
                        theirs.get(key), path + "." + key);
                if (difference != null)
                {
                    return difference;
                }
            }
        }
        if (ours.isArray() && theirs.isArray()
                && ours.size() == theirs.size())
        {
            for (int index = 0; index < ours.size(); index++)
            {
                String difference = firstDifference(ours.get(index),
                        theirs.get(index), path + "[" + index + "]");
                if (difference != null)
                {
                    return difference;
                }
            }
        }
        return path + ": ours " + abbreviate(ours) + " vs Google's "
                + abbreviate(theirs);
    }

    /**
     * Writes JSON as text, cut short if it is long.
     *
     * @param node the JSON
     * @return its text, at most 300 characters and an ellipsis
     */
    static String abbreviate(JsonNode node)
    {
        String text = node.toString();
        return text.length() > 300 ? text.substring(0, 300) + "…" : text;
    }

    /**
     * Asserts that our output, as Google's tool would write it, matches
     * Google's output for the same document.
     *
     * @param xml        the document
     * @param googleJson Google's JSON for it
     * @throws Exception if the document can't be converted or the JSON read
     */
    static void assertMatchesGoogle(byte[] xml, byte[] googleJson)
            throws Exception
    {
        JsonNode ours = googleView(convert(xml));
        JsonNode theirs = JSON.readTree(googleJson);
        assertThat(firstDifference(ours, theirs, "$")).isNull();
    }

    /**
     * Asserts that a document fails to convert as malformed XML.
     *
     * @param xml the document
     */
    static void assertMalformed(String xml)
    {
        assertThatThrownBy(() -> CONVERTER.convert(xml.getBytes(UTF_8), CONFIG))
                .isInstanceOfSatisfying(ConversionException.class,
                        error -> assertThat(error.code())
                                .isEqualTo(ErrorCode.CONVERT_XML_MALFORMED));
    }

    /**
     * Makes a document whose elements nest {@code depth} deep.
     *
     * @param depth how deep, counting {@code ClinicalDocument} as 1
     * @return the document
     */
    static String nested(int depth)
    {
        return ROOT + ">" + "<a>".repeat(depth - 1) + "x"
                + "</a>".repeat(depth - 1) + "</ClinicalDocument>";
    }

    /**
     * Reads an element's position from its {@code #line} and {@code #col}
     * keys.
     *
     * @param element the element's JSON object
     * @return the position, as {@code line:column}
     */
    private static String position(JsonNode element)
    {
        return element.get(CcdaConverter.LINE_KEY).asInt() + ":"
                + element.get(CcdaConverter.COLUMN_KEY).asInt();
    }

    /**
     * Matches Google's own test pair: a synthetic Synthea patient, under
     * Apache 2.0 (see {@code src/test/resources/ccda/README.md}).
     *
     * @throws Exception if a file can't be read or converted
     */
    @Test
    void matchesGooglesXmlToJsonOnItsOwnSample() throws Exception
    {
        assertMatchesGoogle(resource("google-synthea-sample1.xml"),
                resource("google-synthea-sample1.json"));
    }

    /**
     * Matches all of Google's published pairs, from a local directory of
     * {@code sampleN.xml} and {@code sampleN.json} files given by
     * {@code -Dccda.google.dir=…}. Not run by default, as they are 33 MB.
     *
     * @throws Exception if a file can't be read or converted
     */
    @Test
    @EnabledIfSystemProperty(named = "ccda.google.dir", matches = ".+")
    void matchesGooglesXmlToJsonOnAllItsSamples() throws Exception
    {
        Path directory = Path.of(System.getProperty("ccda.google.dir"));
        List<Path> xmlFiles;
        try (Stream<Path> files = Files.list(directory))
        {
            xmlFiles = files.filter(file -> file.toString().endsWith(".xml"))
                    .sorted().toList();
        }
        assertThat(xmlFiles).isNotEmpty();
        for (Path xmlFile : xmlFiles)
        {
            Path jsonFile = xmlFile.resolveSibling(xmlFile.getFileName()
                    .toString().replace(".xml", ".json"));
            JsonNode ours = googleView(convert(Files.readAllBytes(xmlFile)));
            assertThat(firstDifference(ours,
                    JSON.readTree(Files.readAllBytes(jsonFile)), "$"))
                    .as(xmlFile.getFileName().toString())
                    .isNull();
        }
    }

    /**
     * Keeps what Google's tool drops: {@code nullFlavor}, a qualifier on a
     * name part, an {@code sdtc} extension next to the HL7 element of the
     * same name, and a vendor's own element. Only the schema location goes.
     *
     * @throws Exception if the document can't be converted
     */
    @Test
    void keepsWhatGoogleDrops() throws Exception
    {
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <ClinicalDocument xmlns="urn:hl7-org:v3"
                    xmlns:sdtc="urn:hl7-org:sdtc"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="urn:hl7-org:v3 CDA.xsd">
                  <recordTarget><patientRole>
                    <id nullFlavor="NI"/>
                    <telecom nullFlavor="UNK"/>
                    <patient>
                      <name><given qualifier="CL">Ann</given>
                        <family>Doe</family></name>
                      <raceCode code="2106-3"/>
                      <sdtc:raceCode code="2028-9"/>
                      <vendorExtension flag="1">x</vendorExtension>
                    </patient>
                  </patientRole></recordTarget>
                </ClinicalDocument>""";
        JsonNode document = convert(xml.getBytes(UTF_8))
                .get("ClinicalDocument");
        assertThat(document.has("schemaLocation")).isFalse();
        JsonNode role = document.get("recordTarget").get(0).get("patientRole");
        assertThat(role.get("id").get(0).get("nullFlavor").asText())
                .isEqualTo("NI");
        assertThat(role.get("telecom").get(0).get("nullFlavor").asText())
                .isEqualTo("UNK");
        JsonNode patient = role.get("patient");
        assertThat(patient.get("name").get(0).get("given").get(0).toString())
                .isEqualTo("{\"qualifier\":\"CL\",\"value\":[\"Ann\"]}");
        assertThat(patient.get("raceCode").get("code").asText())
                .isEqualTo("2106-3");
        assertThat(patient.get("sdtc:raceCode").get(0).get("code").asText())
                .isEqualTo("2028-9");
        assertThat(patient.get("vendorExtension").toString())
                .isEqualTo("[{\"flag\":\"1\",\"value\":[\"x\"]}]");
    }

    /**
     * An {@code xsi:type} attribute chooses the element's data type, and is
     * written as a {@code type} key.
     *
     * @throws Exception if the document can't be converted
     */
    @Test
    void xsiTypeSelectsTheTypeAndBecomesTypeKey() throws Exception
    {
        String xml = """
                <ClinicalDocument xmlns="urn:hl7-org:v3"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                  <component><structuredBody><component><section><entry>
                  <observation classCode="OBS" moodCode="EVN">
                    <effectiveTime xsi:type="IVL_TS">
                      <low value="2015"/><high value="2016"/>
                    </effectiveTime>
                    <value xsi:type="PQ" value="12.5" unit="mg"/>
                  </observation></entry></section></component>
                  </structuredBody></component>
                </ClinicalDocument>""";
        JsonNode observation = convert(xml.getBytes(UTF_8))
                .at("/ClinicalDocument/component/structuredBody/component/0"
                        + "/section/entry/0/observation");
        assertThat(observation.get("effectiveTime").toString())
                .isEqualTo("{\"type\":\"IVL_TS\","
                        + "\"low\":{\"value\":\"2015\"},"
                        + "\"high\":{\"value\":\"2016\"}}");
        assertThat(observation.get("value").get(0).toString())
                .isEqualTo("{\"type\":\"PQ\",\"value\":\"12.5\","
                        + "\"unit\":\"mg\"}");
    }

    /**
     * Tests each branch of {@code CcdaConverter.attributeValue}. Attributes
     * the schema types as boolean (BL {@code value}, {@code negationInd}) or
     * integer (INT {@code value}) become JSON booleans and numbers, in all
     * their valid spellings. Invalid values, and attributes typed as text (PQ
     * {@code value}, CD {@code code}), stay strings exactly as written.
     * {@code toString()} keeps the JSON type visible: {@code "\"1\""} is a
     * string, {@code "1"} a number.
     *
     * @throws Exception if the document can't be converted
     */
    @Test
    void typesBooleanAndIntegerAttributesAndKeepEverythingElseAsWritten()
            throws Exception
    {
        String xml = """
                <ClinicalDocument xmlns="urn:hl7-org:v3"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                  <component><structuredBody><component><section>
                    <entry><observation classCode="OBS" moodCode="EVN"
                        negationInd="1">
                      <value xsi:type="BL" value="true"/>
                      <value xsi:type="BL" value="false"/>
                      <value xsi:type="BL" value="1"/>
                      <value xsi:type="BL" value="0"/>
                      <value xsi:type="BL" value="yes"/>
                      <value xsi:type="BL" value="TRUE"/>
                      <value xsi:type="INT" value="42"/>
                      <value xsi:type="INT" value="+007"/>
                      <value xsi:type="INT" value="-12"/>
                      <value xsi:type="INT" value="-0"/>
                      <value xsi:type="INT" value="999999999999999999"/>
                      <value xsi:type="INT" value="1234567890123456789"/>
                      <value xsi:type="INT" value="3.5"/>
                      <value xsi:type="INT" value=" 5"/>
                      <value xsi:type="PQ" value="+007" unit="mg"/>
                      <value xsi:type="CD" code="1"/>
                    </observation></entry>
                    <entry><observation classCode="OBS" moodCode="EVN"
                        negationInd="false"/></entry>
                    <entry><observation classCode="OBS" moodCode="EVN"
                        negationInd="yes"/></entry>
                  </section></component></structuredBody></component>
                </ClinicalDocument>""";
        JsonNode entries = convert(xml.getBytes(UTF_8))
                .at("/ClinicalDocument/component/structuredBody/component/0"
                        + "/section/entry");
        JsonNode values = entries.at("/0/observation/value");
        List<String> written = new ArrayList<>();
        for (int index = 0; index < 14; index++)
        {
            written.add(values.get(index).get("value").toString());
        }
        assertThat(written).containsExactly(
                // BL value: the four XML Schema spellings, then two that
                // aren't valid booleans.
                "true", "false", "true", "false", "\"yes\"", "\"TRUE\"",
                // INT value: plain form (sign and leading zeros dropped), up
                // to 18 digits.
                "42", "7", "-12", "0", "999999999999999999",
                // INT value: too long for the pattern, not an integer, or not
                // exactly digits.
                "\"1234567890123456789\"", "\"3.5\"", "\" 5\"");
        // Attributes the schema types as text stay strings, even when they
        // look like a number.
        assertThat(values.get(14).get("value").toString())
                .isEqualTo("\"+007\"");
        assertThat(values.get(15).get("code").toString()).isEqualTo("\"1\"");
        // negationInd on an observation is a schema boolean: valid spellings
        // typed, an invalid one kept.
        assertThat(entries.at("/0/observation/negationInd").toString())
                .isEqualTo("true");
        assertThat(entries.at("/1/observation/negationInd").toString())
                .isEqualTo("false");
        assertThat(entries.at("/2/observation/negationInd").toString())
                .isEqualTo("\"yes\"");
        // classCode is not typed: always a string.
        assertThat(entries.at("/0/observation/classCode").toString())
                .isEqualTo("\"OBS\"");
    }

    /**
     * Converting the same document twice gives the same bytes.
     *
     * @throws Exception if the document can't be read or converted
     */
    @Test
    void outputIsDeterministic() throws Exception
    {
        byte[] xml = resource("google-synthea-sample1.xml");
        assertThat(CONVERTER.convert(xml, CONFIG))
                .isEqualTo(CONVERTER.convert(xml, CONFIG));
    }

    /**
     * A document with a DTD is refused, and a document that isn't well
     * formed is reported as malformed.
     */
    @Test
    void refusesDtdsAndReportsMalformedXml()
    {
        String dtd = "<?xml version=\"1.0\"?>"
                + "<!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]>"
                + "<ClinicalDocument xmlns=\"urn:hl7-org:v3\">"
                + "<title>&e;</title></ClinicalDocument>";
        assertThatThrownBy(() -> CONVERTER.convert(dtd.getBytes(UTF_8), CONFIG))
                .isInstanceOfSatisfying(ConversionException.class,
                        error -> assertThat(error.code())
                                .isEqualTo(ErrorCode.CONVERT_XML_MALFORMED));
        assertThatThrownBy(() -> CONVERTER.convert(
                "<ClinicalDocument><a></b></ClinicalDocument>"
                        .getBytes(UTF_8), CONFIG))
                .isInstanceOfSatisfying(ConversionException.class,
                        error -> assertThat(error.code())
                                .isEqualTo(ErrorCode.CONVERT_XML_MALFORMED));
    }

    /**
     * Nothing a document points to is ever fetched or read: not an external
     * DTD, an external entity or parameter entity, a schema location or an
     * XInclude. A local HTTP server serves all of them and must get no
     * request. Documents that only point at something are converted; those
     * that use an entity it would declare are refused.
     *
     * @throws Exception if the server can't start or a document can't be
     *                   converted
     */
    @Test
    void neverFetchesDtdsEntitiesSchemasOrIncludes() throws Exception
    {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(
                InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange ->
        {
            requests.incrementAndGet();
            byte[] body = "<!ENTITY ext \"FETCHED\">".getBytes(UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try
        {
            String url = "http://127.0.0.1:" + server.getAddress().getPort();
            JsonNode pointsOnly = convert(("<!DOCTYPE ClinicalDocument SYSTEM"
                    + " \"" + url + "/cda.dtd\">" + ROOT
                    + " xsi:schemaLocation=\"urn:hl7-org:v3 " + url
                    + "/CDA.xsd\">"
                    + "<title>t<xi:include href=\"" + url
                    + "/include\" parse=\"text\"/></title>"
                    + "</ClinicalDocument>").getBytes(UTF_8))
                    .get("ClinicalDocument");
            assertThat(pointsOnly.has("schemaLocation")).isFalse();
            assertThat(pointsOnly.get("title").get("xi:include").get(0)
                    .get("href").asText()).isEqualTo(url + "/include");
            JsonNode parameterEntity = convert(("<!DOCTYPE ClinicalDocument"
                    + " [<!ENTITY % p SYSTEM \"" + url + "/param\">"
                    + " %p;]>" + ROOT + "><title>t</title></ClinicalDocument>")
                    .getBytes(UTF_8));
            assertThat(parameterEntity.toString()).doesNotContain("FETCHED");
            assertMalformed("<!DOCTYPE ClinicalDocument SYSTEM \"" + url
                    + "/evil.dtd\">" + ROOT
                    + "><title>&ext;</title></ClinicalDocument>");
            assertMalformed("<!DOCTYPE ClinicalDocument [<!ENTITY e SYSTEM \""
                    + url + "/entity\">]>" + ROOT
                    + "><title>&e;</title></ClinicalDocument>");
            assertThat(requests.get()).as("requests to the server").isZero();
        }
        finally
        {
            server.stop(0);
        }
    }

    /**
     * No entity is expanded, so neither a declared one nor "billion laughs",
     * where each level is ten of the one before.
     */
    @Test
    void refusesEntityExpansion()
    {
        assertMalformed("<!DOCTYPE ClinicalDocument [<!ENTITY e \"expanded\">]>"
                + ROOT + "><title>&e;</title></ClinicalDocument>");
        StringBuilder laughs = new StringBuilder("<!ENTITY lol0 \"lol\">");
        for (int level = 1; level <= 9; level++)
        {
            laughs.append("<!ENTITY lol").append(level).append(" \"")
                    .append(("&lol" + (level - 1) + ";").repeat(10))
                    .append("\">");
        }
        assertMalformed("<!DOCTYPE ClinicalDocument [" + laughs + "]>" + ROOT
                + "><title>&lol9;</title></ClinicalDocument>");
    }

    /**
     * Nesting beyond {@link CcdaConverter#MAX_DEPTH} is refused, whichever
     * parser is in use, and even absurd nesting fails as a conversion error,
     * not by overflowing the stack. Real documents nest about 17 deep.
     *
     * @throws Exception if the allowed document can't be converted
     */
    @Test
    void refusesNestingDeeperThanTheLimit() throws Exception
    {
        assertThat(convert(nested(50).getBytes(UTF_8)).toString())
                .contains("\"x\"");
        assertMalformed(nested(CcdaConverter.MAX_DEPTH + 1));
        assertMalformed(nested(100_000));
    }

    /**
     * With Woodstox, the connector's parser, whose own limit (1000) is far
     * higher, the limit is the converter's: the deepest nesting it allows
     * converts, and one more level doesn't. The JDK's parser stops at 100 by
     * default.
     *
     * @throws Exception if the allowed document can't be converted
     */
    @Test
    void theDepthLimitIsTheConvertersOwn() throws Exception
    {
        assumeTrue(XMLInputFactory.newFactory().getClass().getName()
                .startsWith("com.ctc.wstx"),
                "needs Woodstox, whose own depth limit is above the"
                        + " converter's");
        assertThat(convert(nested(CcdaConverter.MAX_DEPTH).getBytes(UTF_8))
                .toString()).contains("\"x\"");
        assertMalformed(nested(CcdaConverter.MAX_DEPTH + 1));
    }

    /**
     * The parser hands long text over in pieces (Woodstox's are 4,000
     * characters), and the converter keeps large runs in pieces rather than
     * joining them. The JSON must still be the whole text: a run longer than
     * {@link CcdaConverter#JOIN_LIMIT}, an emoji whose surrogate pair
     * straddles the first piece boundary, and, in mixed content, a comment
     * still splitting the text into separate runs.
     *
     * @throws Exception if the document can't be converted
     */
    @Test
    void longTextReadInPiecesIsWrittenWhole() throws Exception
    {
        String title = "a".repeat(3999) + "\uD83D\uDE00" + "b".repeat(10_000);
        String body = ("QUJD".repeat(19) + "\n")
                .repeat(CcdaConverter.JOIN_LIMIT / 76 * 3);
        String xml = "<ClinicalDocument xmlns=\"urn:hl7-org:v3\"><title>"
                + title + "</title>"
                + "<component><nonXMLBody><text mediaType=\"application/pdf\""
                + " representation=\"B64\">" + body
                + "</text></nonXMLBody></component>"
                + "<component><structuredBody><component><section>"
                + "<text>x <!--c--> y<content>z</content> w"
                + "</text></section></component></structuredBody></component>"
                + "</ClinicalDocument>";
        JsonNode document = convert(xml.getBytes(UTF_8))
                .get("ClinicalDocument");
        assertThat(document.get("title").asText()).isEqualTo(title);
        JsonNode components = document.get("component");
        // ED is mixed content, so its value is an array.
        JsonNode encoded = components.get(0).get("nonXMLBody").get("text")
                .get("value");
        assertThat(encoded).hasSize(1);
        assertThat(encoded.get(0).asText()).isEqualTo(body);
        JsonNode narrative = components.get(1).get("structuredBody")
                .get("component").get(0).get("section").get("text");
        assertThat(narrative.get("value"))
                .hasToString("[\"x \",\" y\",\" w\"]");
    }

    /**
     * Every element written as an object starts with the line and column of
     * its start tag's {@code <}: across CRLF line ends, a tab, a character of
     * two bytes and a start tag split over lines. One written as a string
     * has none, nor has an empty structural element, which stays {@code {}}.
     *
     * @throws Exception if the document can't be converted
     */
    @Test
    void elementsSayWhereTheirStartTagStarts() throws Exception
    {
        String xml = "<?xml version=\"1.0\"?>\r\n"
                + "<ClinicalDocument xmlns=\"urn:hl7-org:v3\">\r\n"
                + "\t<title>\u00e9t\u00e9</title><code\r\n"
                + "    code=\"34133-9\"/>\r\n"
                + "  <recordTarget><patientRole><patient/></patientRole>"
                + "</recordTarget>\r\n"
                + "</ClinicalDocument>";
        JsonNode document = convertWithPositions(xml.getBytes(UTF_8))
                .get("ClinicalDocument");
        assertThat(position(document)).isEqualTo("2:1");
        assertThat(document.get("title").isTextual()).isTrue();
        // After a tab, "<title>", "été" (five bytes, three characters) and
        // "</title>".
        assertThat(position(document.get("code"))).isEqualTo("3:20");
        JsonNode recordTarget = document.get("recordTarget").get(0);
        assertThat(position(recordTarget)).isEqualTo("5:3");
        assertThat(position(recordTarget.get("patientRole")))
                .isEqualTo("5:17");
        assertThat(recordTarget.get("patientRole").get("patient").toString())
                .isEqualTo("{}");
        // The keys come first.
        assertThat(document.fieldNames().next())
                .isEqualTo(CcdaConverter.LINE_KEY);
    }

    /**
     * A malformed document is reported at a byte position, not a character
     * count, when multi-byte text comes before the error.
     */
    @Test
    void malformedXmlIsReportedAtItsBytePosition()
    {
        String xml = "<ClinicalDocument xmlns=\"urn:hl7-org:v3\">\n"
                + "<title>\u00e9\u00e9\u00e9</title><a></b></ClinicalDocument>";
        byte[] bytes = xml.getBytes(UTF_8);
        assertThatThrownBy(() -> CONVERTER.convert(bytes, CONFIG))
                .isInstanceOfSatisfying(ConversionException.class, error ->
                {
                    assertThat(error.code())
                            .isEqualTo(ErrorCode.CONVERT_XML_MALFORMED);
                    // The parser notices in the bad end tag. Counted in
                    // bytes, each "é" is two, so the end tag starts three
                    // bytes later than its character index.
                    long endTag = new String(bytes, 0, bytes.length, UTF_8)
                            .indexOf("</b>") + 3;
                    assertThat(error.position())
                            .isBetween(endTag, endTag + "</b>".length());
                });
    }

    /** A document that ends too early is reported at its last byte. */
    @Test
    void aDocumentThatEndsEarlyIsReportedAtItsEnd()
    {
        byte[] bytes = ("<?xml version=\"1.0\"?>\n"
                + "<ClinicalDocument xmlns=\"urn:hl7-org:v3\"><title>cut off\n")
                .getBytes(UTF_8);
        assertThatThrownBy(() -> CONVERTER.convert(bytes, CONFIG))
                .isInstanceOfSatisfying(ConversionException.class, error ->
                {
                    assertThat(error.code())
                            .isEqualTo(ErrorCode.CONVERT_XML_MALFORMED);
                    assertThat(error.position()).isEqualTo(bytes.length);
                });
    }

    /**
     * An undeclared entity is only reported when the text is read, and
     * Woodstox does that with an unchecked exception. It must still be a
     * conversion failure (the record is stored unconverted), not an unknown
     * error that fails the whole file. Found by the demo (design §15.3).
     */
    @Test
    void undeclaredEntityIsAConversionFailure()
    {
        String nbsp = "<ClinicalDocument xmlns=\"urn:hl7-org:v3\">"
                + "<title>Summary&nbsp;(amended)</title></ClinicalDocument>";
        assertThatThrownBy(() -> CONVERTER.convert(nbsp.getBytes(UTF_8),
                CONFIG))
                .isInstanceOfSatisfying(ConversionException.class,
                        error -> assertThat(error.code())
                                .isEqualTo(ErrorCode.CONVERT_XML_MALFORMED));
    }
}
