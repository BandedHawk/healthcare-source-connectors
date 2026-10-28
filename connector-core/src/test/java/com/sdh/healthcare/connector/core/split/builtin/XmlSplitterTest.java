package com.sdh.healthcare.connector.core.split.builtin;

import static com.sdh.healthcare.connector.core.split.SplitTesting.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.ByteArrayInputStream;
import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

class XmlSplitterTest {
    private final CcdaSplitter ccda = new CcdaSplitter();

    static final String DOC1 = "<ClinicalDocument><id root=\"1\"/><title>One &amp; <b>only</b></title>"
            + "<component><ClinicalDocument>nested stays inside</ClinicalDocument></component></ClinicalDocument>";
    static final String DOC2 = "<v3:ClinicalDocument xmlns:sdtc=\"urn:hl7-org:sdtc\" a='x>y'>"
            + "<v3:id root=\"2\"/><sdtc:raceCode code=\"1\"/><![CDATA[</v3:ClinicalDocument>]]></v3:ClinicalDocument>";
    static final String DOC3 = "<ClinicalDocument\n   xmlns=\"urn:hl7-org:v3\"/>";

    static final String FILE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<!DOCTYPE batch [ <!ENTITY x \"<ClinicalDocument>\"> ]>\n"
            + "<batch xmlns=\"urn:hl7-org:v3\" xmlns:v3='urn:hl7-org:v3' xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
            + "  <!-- <ClinicalDocument> in a comment is ignored -->\n"
            + "  <?pi <ClinicalDocument>?>\n"
            + "  <group id=\"g1\">\n    " + DOC1 + "\n  </group>\n"
            + "  <other><ClinicalDocument xmlns=\"urn:other\"/></other>\n"
            + "  <group>" + DOC2 + DOC3 + "</group>\n"
            + "</batch>\n";

    @Test
    void sectionsAreStandaloneDocumentsWithInheritedNamespaces() throws Exception {
        List<String> s = splitAndCheckResume(ccda, FILE, cfg("ccda"));
        String decl = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
        String inherited = " xmlns=\"urn:hl7-org:v3\" xmlns:v3='urn:hl7-org:v3'"
                + " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";
        assertThat(s).containsExactly(
                decl + DOC1.replaceFirst("<ClinicalDocument", "<ClinicalDocument" + inherited),
                decl + DOC2.replaceFirst("<v3:ClinicalDocument", "<v3:ClinicalDocument" + inherited),
                decl + "<ClinicalDocument xmlns:v3='urn:hl7-org:v3' xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                        + "\n   xmlns=\"urn:hl7-org:v3\"/>");
        for (String doc : s) {
            var f = DocumentBuilderFactory.newInstance();
            f.setNamespaceAware(true);
            Element root = f.newDocumentBuilder().parse(new ByteArrayInputStream(doc.getBytes(UTF_8)))
                    .getDocumentElement();
            assertThat(root.getLocalName()).isEqualTo("ClinicalDocument");
            assertThat(root.getNamespaceURI()).isEqualTo("urn:hl7-org:v3");
        }
    }

    @Test
    void anyNamespaceAndPlainXml() throws Exception {
        List<String> s = splitAndCheckResume(new XmlSplitter(), FILE,
                cfg("xml").withBoundary("ClinicalDocument"));
        assertThat(s).hasSize(4);
        assertThat(splitAndCheckResume(new XmlSplitter(), "<r><item a=\"1\">x</item><item/></r>",
                cfg("xml").withBoundary("item"))).containsExactly("<item a=\"1\">x</item>", "<item/>");
    }

    @Test
    void wholeFileIsOneDocument() throws Exception {
        String doc = "<ClinicalDocument xmlns=\"urn:hl7-org:v3\"><x/></ClinicalDocument>\n<!-- end -->\n";
        assertThat(splitAndCheckResume(ccda, doc, cfg("ccda")))
                .containsExactly("<ClinicalDocument xmlns=\"urn:hl7-org:v3\"><x/></ClinicalDocument>");
    }

    @Test
    void truncatedAndMismatchedInput() {
        String head = FILE.substring(0, FILE.indexOf("</title>"));
        assertThat(splitError(ccda, head, cfg("ccda")).code()).isEqualTo(ErrorCode.PARSE_UNBALANCED_ELEMENT);
        assertThat(splitError(ccda, "<b xmlns=\"urn:hl7-org:v3\"><ClinicalDocument><a></b></ClinicalDocument></b>",
                cfg("ccda")).code()).isEqualTo(ErrorCode.PARSE_UNBALANCED_ELEMENT);
        assertThat(splitError(ccda, "<b><!-- never closed", cfg("ccda")).code())
                .isEqualTo(ErrorCode.PARSE_XML_MALFORMED);
        String afterFirst = FILE.substring(0, FILE.indexOf("</group>"));
        assertThat(splitError(ccda, afterFirst, cfg("ccda")).code()).isEqualTo(ErrorCode.PARSE_UNBALANCED_ELEMENT);
    }

    @Test
    void utf16IsRejected() throws Exception {
        byte[] utf16 = "<?xml version=\"1.0\" encoding=\"UTF-16\"?><a/>".getBytes(java.nio.charset.StandardCharsets.UTF_16);
        try {
            sections(ccda, utf16, cfg("ccda"), com.sdh.healthcare.connector.core.split.ResumePoint.START);
            throw new AssertionError("expected failure");
        } catch (com.sdh.healthcare.connector.core.split.SplitException e) {
            assertThat(e.code()).isEqualTo(ErrorCode.PARSE_XML_UNSUPPORTED_ENCODING);
        }
    }
}
