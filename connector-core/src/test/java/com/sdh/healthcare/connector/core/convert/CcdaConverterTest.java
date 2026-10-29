package com.sdh.healthcare.connector.core.convert;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

class CcdaConverterTest {
    static final ObjectMapper JSON = new ObjectMapper();
    static final CcdaConverter CONVERTER = new CcdaConverter();
    static final SplitConfig CFG = SplitConfig.defaults("ccda");

    static JsonNode convert(byte[] xml) throws Exception {
        return JSON.readTree(CONVERTER.convert(xml, CFG));
    }

    static byte[] resource(String name) throws Exception {
        try (InputStream in = CcdaConverterTest.class.getResourceAsStream("/ccda/" + name)) {
            assertThat(in).as(name).isNotNull();
            return in.readAllBytes();
        }
    }

    /**
     * What Google's tool would make of our output: nullFlavor deleted, then empty objects and arrays removed. An
     * empty element ({@code <td/>}) is {@code ""} in ours, to keep table cell positions; Google's binding makes it an
     * empty object, which its post-processor then removes, so {@code ""} is removed here too. Everything else must
     * already match Google's output.
     */
    static JsonNode googleView(JsonNode n) {
        if (n instanceof ObjectNode o) {
            ObjectNode out = JSON.createObjectNode();
            for (Iterator<Map.Entry<String, JsonNode>> it = o.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                if (e.getKey().equals("nullFlavor")) {
                    continue;
                }
                JsonNode v = googleView(e.getValue());
                if (!isEmptyContainer(v)) {
                    out.set(e.getKey(), v);
                }
            }
            return out;
        }
        if (n instanceof ArrayNode a) {
            ArrayNode out = JSON.createArrayNode();
            for (JsonNode e : a) {
                JsonNode v = googleView(e);
                if (!isEmptyContainer(v)) {
                    out.add(v);
                }
            }
            return out;
        }
        return n;
    }

    static boolean isEmptyContainer(JsonNode n) {
        return ((n.isObject() || n.isArray()) && n.isEmpty()) || (n.isTextual() && n.asText().isEmpty());
    }

    /** The first path where two trees differ, for a readable failure. */
    static String firstDifference(JsonNode ours, JsonNode theirs, String path) {
        if (ours.equals(theirs)) {
            return null;
        }
        if (ours.isObject() && theirs.isObject()) {
            List<String> keys = new ArrayList<>();
            ours.fieldNames().forEachRemaining(keys::add);
            theirs.fieldNames().forEachRemaining(k -> {
                if (!keys.contains(k)) {
                    keys.add(k);
                }
            });
            for (String k : keys) {
                if (!ours.has(k) || !theirs.has(k)) {
                    return path + "." + k + (ours.has(k) ? " only in ours: " : " only in Google's: ")
                            + abbreviate(ours.has(k) ? ours.get(k) : theirs.get(k));
                }
                String d = firstDifference(ours.get(k), theirs.get(k), path + "." + k);
                if (d != null) {
                    return d;
                }
            }
        }
        if (ours.isArray() && theirs.isArray() && ours.size() == theirs.size()) {
            for (int i = 0; i < ours.size(); i++) {
                String d = firstDifference(ours.get(i), theirs.get(i), path + "[" + i + "]");
                if (d != null) {
                    return d;
                }
            }
        }
        return path + ": ours " + abbreviate(ours) + " vs Google's " + abbreviate(theirs);
    }

    static String abbreviate(JsonNode n) {
        String s = n.toString();
        return s.length() > 300 ? s.substring(0, 300) + "…" : s;
    }

    static void assertMatchesGoogle(byte[] xml, byte[] googleJson) throws Exception {
        JsonNode ours = googleView(convert(xml));
        JsonNode theirs = JSON.readTree(googleJson);
        assertThat(firstDifference(ours, theirs, "$")).isNull();
    }

    /** Google's own test pair (synthetic Synthea patient, Apache 2.0; see src/test/resources/ccda/README.md). */
    @Test
    void matchesGooglesXmlToJsonOnItsOwnSample() throws Exception {
        assertMatchesGoogle(resource("google-synthea-sample1.xml"), resource("google-synthea-sample1.json"));
    }

    /**
     * All of Google's published pairs, from a local directory of sampleN.xml / sampleN.json:
     * {@code -Dccda.google.dir=…}. Not run by default (33 MB).
     */
    @Test
    @EnabledIfSystemProperty(named = "ccda.google.dir", matches = ".+")
    void matchesGooglesXmlToJsonOnAllItsSamples() throws Exception {
        Path dir = Path.of(System.getProperty("ccda.google.dir"));
        List<Path> xmls;
        try (Stream<Path> s = Files.list(dir)) {
            xmls = s.filter(p -> p.toString().endsWith(".xml")).sorted().toList();
        }
        assertThat(xmls).isNotEmpty();
        for (Path x : xmls) {
            Path j = x.resolveSibling(x.getFileName().toString().replace(".xml", ".json"));
            JsonNode ours = googleView(convert(Files.readAllBytes(x)));
            assertThat(firstDifference(ours, JSON.readTree(Files.readAllBytes(j)), "$")).as(x.getFileName().toString())
                    .isNull();
        }
    }

    @Test
    void keepsWhatGoogleDrops() throws Exception {
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:sdtc="urn:hl7-org:sdtc"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="urn:hl7-org:v3 CDA.xsd">
                  <recordTarget><patientRole>
                    <id nullFlavor="NI"/>
                    <telecom nullFlavor="UNK"/>
                    <patient>
                      <name><given qualifier="CL">Ann</given><family>Doe</family></name>
                      <raceCode code="2106-3"/>
                      <sdtc:raceCode code="2028-9"/>
                      <vendorExtension flag="1">x</vendorExtension>
                    </patient>
                  </patientRole></recordTarget>
                </ClinicalDocument>""";
        JsonNode j = convert(xml.getBytes(UTF_8)).get("ClinicalDocument");
        assertThat(j.has("schemaLocation")).isFalse();
        JsonNode role = j.get("recordTarget").get(0).get("patientRole");
        assertThat(role.get("id").get(0).get("nullFlavor").asText()).isEqualTo("NI");
        assertThat(role.get("telecom").get(0).get("nullFlavor").asText()).isEqualTo("UNK");
        JsonNode patient = role.get("patient");
        assertThat(patient.get("name").get(0).get("given").get(0).toString())
                .isEqualTo("{\"qualifier\":\"CL\",\"value\":[\"Ann\"]}");
        assertThat(patient.get("raceCode").get("code").asText()).isEqualTo("2106-3");
        assertThat(patient.get("sdtc:raceCode").get(0).get("code").asText()).isEqualTo("2028-9");
        assertThat(patient.get("vendorExtension").toString()).isEqualTo("[{\"flag\":\"1\",\"value\":[\"x\"]}]");
    }

    @Test
    void xsiTypeSelectsTheTypeAndBecomesTypeKey() throws Exception {
        String xml = """
                <ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                  <component><structuredBody><component><section><entry><observation classCode="OBS" moodCode="EVN">
                    <effectiveTime xsi:type="IVL_TS"><low value="2015"/><high value="2016"/></effectiveTime>
                    <value xsi:type="PQ" value="12.5" unit="mg"/>
                  </observation></entry></section></component></structuredBody></component>
                </ClinicalDocument>""";
        JsonNode obs = convert(xml.getBytes(UTF_8)).at("/ClinicalDocument/component/structuredBody/component/0"
                + "/section/entry/0/observation");
        assertThat(obs.get("effectiveTime").toString())
                .isEqualTo("{\"type\":\"IVL_TS\",\"low\":{\"value\":\"2015\"},\"high\":{\"value\":\"2016\"}}");
        assertThat(obs.get("value").get(0).toString()).isEqualTo("{\"type\":\"PQ\",\"value\":\"12.5\",\"unit\":\"mg\"}");
    }

    @Test
    void outputIsDeterministic() throws Exception {
        byte[] xml = resource("google-synthea-sample1.xml");
        assertThat(CONVERTER.convert(xml, CFG)).isEqualTo(CONVERTER.convert(xml, CFG));
    }

    @Test
    void refusesDtdsAndReportsMalformedXml() {
        String dtd = "<?xml version=\"1.0\"?><!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]>"
                + "<ClinicalDocument xmlns=\"urn:hl7-org:v3\"><title>&e;</title></ClinicalDocument>";
        assertThatThrownBy(() -> CONVERTER.convert(dtd.getBytes(UTF_8), CFG))
                .isInstanceOfSatisfying(ConversionException.class,
                        e -> assertThat(e.code()).isEqualTo(ErrorCode.CONVERT_XML_MALFORMED));
        assertThatThrownBy(() -> CONVERTER.convert("<ClinicalDocument><a></b></ClinicalDocument>".getBytes(UTF_8), CFG))
                .isInstanceOfSatisfying(ConversionException.class,
                        e -> assertThat(e.code()).isEqualTo(ErrorCode.CONVERT_XML_MALFORMED));
    }
}
