package com.sdh.healthcare.connector.file.demo;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * The demo's input: C-CDA files built from the synthetic and de-identified samples in
 * {@code connector-core/src/test/resources/ccda/} (no PHI). Some hold one document, some a batch of several, and three
 * are broken in the ways the demo shows.
 */
final class DemoFiles {

    /**
     * One file to deliver, and what the connector should do with it: the final status ({@code COMPLETED} or
     * {@code FAILED}), how many sections it should store, how many of those can't be converted, and why.
     */
    record DemoFile(String name, byte[] content, String status, int sections, int failedSections, String expected) {}

    static final Path SAMPLES = Path.of("..", "connector-core", "src", "test", "resources", "ccda");

    private DemoFiles() {}

    static List<DemoFile> build() throws IOException {
        String synthea = document("google-synthea-sample1.xml");
        String alice = document("alice-newman-uscdi-v3.xml");
        String master = document("master-ccd.xml");
        String uscdi = document("uscdi-v3-all-mappings.xml");
        String epic = document("epic-inpatient-ccd-deidentified.xml");
        // Each file gets its own copy of a sample, tagged in its title. Section IDs are keyed hashes of the content
        // (design §7.3), so the same document in two files would be one section to a downstream consumer, which
        // isn't what this demo is about.
        String synthea4 = tagged(synthea, "file 4");
        String alice4 = tagged(alice, "file 4");
        String uscdi4 = tagged(uscdi, "file 4");
        String epic5 = tagged(epic, "file 5");
        String epic7 = tagged(epic, "file 7");
        // Splits fine (the splitter only finds element boundaries) but isn't well-formed XML: &nbsp; is an HTML
        // entity that a C-CDA document can't use without declaring it, so the converter rejects it.
        String unconvertible = alice4.replaceFirst("</title>", "&nbsp;(amended)</title>");
        // Cut off part-way through, after a tag, so the batch and the document never close.
        String truncated = master.substring(0, master.indexOf('>', master.length() / 2) + 1);
        return List.of(
                new DemoFile("01-synthea-patient.xml", single(synthea), "COMPLETED", 1, 0,
                        "completes: 1 document converted to JSON"),
                new DemoFile("02-alice-newman.xml", single(alice), "COMPLETED", 1, 0,
                        "completes: 1 document converted to JSON"),
                new DemoFile("03-clinic-batch.xml", batch(master, uscdi, epic), "COMPLETED", 3, 0,
                        "completes: a batch of 3 documents, each its own section"),
                new DemoFile("04-batch-with-bad-document.xml", batch(synthea4, unconvertible, uscdi4), "COMPLETED", 3, 1,
                        "completes with 1 failed section: the middle document has an undeclared &nbsp; entity, so it"
                                + " is stored in its original form (CONVERT_XML_MALFORMED) and the other two convert"),
                new DemoFile("05-truncated-batch.xml", unclosedBatch(epic5, truncated), "FAILED", 1, 0,
                        "fails: the file stops part-way through its second document (PARSE_UNBALANCED_ELEMENT); the"
                                + " first document was already stored, and the original goes to failed_files"),
                new DemoFile("06-utf16-document.xml", utf16(alice), "FAILED", 0, 0,
                        "fails at once: UTF-16 isn't supported (PARSE_XML_UNSUPPORTED_ENCODING)"),
                new DemoFile("07-epic-inpatient.xml", single(epic7), "COMPLETED", 1, 0,
                        "completes: the connector carries on after failures"));
    }

    /** A sample document without its XML declaration, ready to stand alone or sit inside a batch. */
    static String document(String file) throws IOException {
        String s = Files.readString(SAMPLES.resolve(file), UTF_8);
        if (s.startsWith("﻿")) {
            s = s.substring(1);
        }
        if (s.startsWith("<?xml")) {
            s = s.substring(s.indexOf("?>") + 2).stripLeading();
        }
        return s;
    }

    /** The document with a tag added to its title, so its content (and section ID) differs from the sample's. */
    static String tagged(String doc, String tag) {
        return doc.replaceFirst("</title>", " [demo " + tag + "]</title>");
    }

    static byte[] single(String doc) {
        return ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + doc).getBytes(UTF_8);
    }

    /** Several documents in one file, inside a wrapper element: each ClinicalDocument becomes a section. */
    static byte[] batch(String... docs) {
        StringBuilder b = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<batch xmlns=\"urn:hl7-org:v3\">\n");
        for (String d : docs) {
            b.append(d.strip()).append('\n');
        }
        return b.append("</batch>\n").toString().getBytes(UTF_8);
    }

    static byte[] unclosedBatch(String complete, String cutOff) {
        return ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<batch xmlns=\"urn:hl7-org:v3\">\n" + complete.strip()
                + "\n" + cutOff).getBytes(UTF_8);
    }

    static byte[] utf16(String doc) {
        return ("﻿<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n" + doc).getBytes(StandardCharsets.UTF_16LE);
    }
}
