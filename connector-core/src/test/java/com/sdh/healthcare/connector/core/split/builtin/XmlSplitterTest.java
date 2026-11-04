/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

import static com.sdh.healthcare.connector.core.split.SplitTesting.config;
import static com.sdh.healthcare.connector.core.split.SplitTesting.sections;
import static com.sdh.healthcare.connector.core.split.SplitTesting.splitAndCheckResume;
import static com.sdh.healthcare.connector.core.split.SplitTesting.splitError;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

/**
 * Tests for {@link XmlSplitter}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class XmlSplitterTest
{

    /** The XML Schema instance namespace declaration. */
    static final String XSI =
            " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";

    /** A document with a nested document, which stays inside it. */
    static final String DOC1 = "<ClinicalDocument><id root=\"1\"/>"
            + "<title>One &amp; <b>only</b></title><component>"
            + "<ClinicalDocument>nested stays inside</ClinicalDocument>"
            + "</component></ClinicalDocument>";

    /** A prefixed document, with a {@code >} in an attribute and CDATA. */
    static final String DOC2 = "<v3:ClinicalDocument"
            + " xmlns:sdtc=\"urn:hl7-org:sdtc\" a='x>y'>"
            + "<v3:id root=\"2\"/><sdtc:raceCode code=\"1\"/>"
            + "<![CDATA[</v3:ClinicalDocument>]]></v3:ClinicalDocument>";

    /** A self-closing document that declares its own namespace. */
    static final String DOC3 = "<ClinicalDocument\n"
            + "   xmlns=\"urn:hl7-org:v3\"/>";

    /** A batch of the documents, with markup that must be skipped. */
    static final String FILE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<!DOCTYPE batch [ <!ENTITY x \"<ClinicalDocument>\"> ]>\n"
            + "<batch xmlns=\"urn:hl7-org:v3\" xmlns:v3='urn:hl7-org:v3'"
            + XSI + ">\n"
            + "  <!-- <ClinicalDocument> in a comment is ignored -->\n"
            + "  <?pi <ClinicalDocument>?>\n"
            + "  <group id=\"g1\">\n    " + DOC1 + "\n  </group>\n"
            + "  <other><ClinicalDocument xmlns=\"urn:other\"/></other>\n"
            + "  <group>" + DOC2 + DOC3 + "</group>\n"
            + "</batch>\n";

    /** The splitter under test. */
    private final CcdaSplitter ccda = new CcdaSplitter();

    /**
     * Each section has the XML declaration and the namespaces in scope,
     * and parses as a C-CDA document; one in another namespace is skipped.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void sectionsAreStandaloneDocumentsWithInheritedNamespaces()
            throws Exception
    {
        List<String> sections = splitAndCheckResume(ccda, FILE,
                config("ccda"));
        String decl = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
        String inherited = " xmlns=\"urn:hl7-org:v3\""
                + " xmlns:v3='urn:hl7-org:v3'" + XSI;
        assertThat(sections).containsExactly(
                decl + DOC1.replaceFirst("<ClinicalDocument",
                        "<ClinicalDocument" + inherited),
                decl + DOC2.replaceFirst("<v3:ClinicalDocument",
                        "<v3:ClinicalDocument" + inherited),
                decl + "<ClinicalDocument xmlns:v3='urn:hl7-org:v3'" + XSI
                        + "\n   xmlns=\"urn:hl7-org:v3\"/>");
        for (String document : sections)
        {
            DocumentBuilderFactory factory =
                    DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            Element root = factory.newDocumentBuilder()
                    .parse(new ByteArrayInputStream(document.getBytes(UTF_8)))
                    .getDocumentElement();
            assertThat(root.getLocalName()).isEqualTo("ClinicalDocument");
            assertThat(root.getNamespaceURI()).isEqualTo("urn:hl7-org:v3");
        }
    }

    /**
     * Without a namespace every boundary element counts, and plain XML
     * splits too.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void anyNamespaceAndPlainXml() throws Exception
    {
        List<String> sections = splitAndCheckResume(new XmlSplitter(), FILE,
                config("xml").withBoundary("ClinicalDocument"));
        assertThat(sections).hasSize(4);
        assertThat(splitAndCheckResume(new XmlSplitter(),
                "<r><item a=\"1\">x</item><item/></r>",
                config("xml").withBoundary("item")))
                .containsExactly("<item a=\"1\">x</item>", "<item/>");
    }

    /**
     * A file that is one document gives one section, without what follows
     * it.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void wholeFileIsOneDocument() throws Exception
    {
        String document = "<ClinicalDocument xmlns=\"urn:hl7-org:v3\">"
                + "<x/></ClinicalDocument>\n<!-- end -->\n";
        assertThat(splitAndCheckResume(ccda, document, config("ccda")))
                .containsExactly("<ClinicalDocument xmlns=\"urn:hl7-org:v3\">"
                        + "<x/></ClinicalDocument>");
    }

    /** Truncated, mismatched and unclosed input fails the file. */
    @Test
    void truncatedAndMismatchedInput()
    {
        String head = FILE.substring(0, FILE.indexOf("</title>"));
        assertThat(splitError(ccda, head, config("ccda")).code())
                .isEqualTo(ErrorCode.PARSE_UNBALANCED_ELEMENT);
        assertThat(splitError(ccda, "<b xmlns=\"urn:hl7-org:v3\">"
                + "<ClinicalDocument><a></b></ClinicalDocument></b>",
                config("ccda")).code())
                .isEqualTo(ErrorCode.PARSE_UNBALANCED_ELEMENT);
        assertThat(splitError(ccda, "<b><!-- never closed",
                config("ccda")).code())
                .isEqualTo(ErrorCode.PARSE_XML_MALFORMED);
        String afterFirst = FILE.substring(0, FILE.indexOf("</group>"));
        assertThat(splitError(ccda, afterFirst, config("ccda")).code())
                .isEqualTo(ErrorCode.PARSE_UNBALANCED_ELEMENT);
    }

    /**
     * Boundary elements may sit {@link XmlSplitter#MAX_DEPTH} elements
     * deep, and no deeper: every open ancestor is held in memory and saved
     * in each resume point. A file of nested wrappers fails instead of
     * growing them.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void nestingIsLimited() throws Exception
    {
        String document = "<ClinicalDocument xmlns=\"urn:hl7-org:v3\"/>";
        String deepest = "<a>".repeat(XmlSplitter.MAX_DEPTH) + document
                + "</a>".repeat(XmlSplitter.MAX_DEPTH);
        assertThat(splitAndCheckResume(ccda, deepest, config("ccda")))
                .containsExactly(document);
        String deeper = "<a>".repeat(XmlSplitter.MAX_DEPTH + 1) + document
                + "</a>".repeat(XmlSplitter.MAX_DEPTH + 1);
        assertThat(splitError(ccda, deeper, config("ccda")).code())
                .isEqualTo(ErrorCode.PARSE_XML_MALFORMED);
        assertThat(splitError(ccda, "<a>".repeat(1_000_000),
                config("ccda")).code())
                .isEqualTo(ErrorCode.PARSE_XML_MALFORMED);
    }

    /**
     * An ancestor start tag too large to be re-read on resume fails the
     * file at once, not on every restart.
     */
    @Test
    void ancestorsMustBeSmallEnoughToResume()
    {
        String big = "<a x=\"" + "y".repeat(1 << 20) + "\">"
                + "<ClinicalDocument xmlns=\"urn:hl7-org:v3\"/></a>";
        assertThat(splitError(ccda, big, config("ccda")).code())
                .isEqualTo(ErrorCode.LIMIT_SECTION_SIZE);
    }

    /**
     * UTF-16 input is rejected.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void utf16IsRejected() throws Exception
    {
        byte[] utf16 = "<?xml version=\"1.0\" encoding=\"UTF-16\"?><a/>"
                .getBytes(StandardCharsets.UTF_16);
        try
        {
            sections(ccda, utf16, config("ccda"), ResumePoint.START);
            throw new AssertionError("expected failure");
        }
        catch (SplitException expected)
        {
            assertThat(expected.code())
                    .isEqualTo(ErrorCode.PARSE_XML_UNSUPPORTED_ENCODING);
        }
    }
}
