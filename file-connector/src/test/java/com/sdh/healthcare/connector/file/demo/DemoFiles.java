/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.demo;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * The demo's input: C-CDA files built from the synthetic and de-identified
 * samples in {@code connector-core/src/test/resources/ccda/} (no PHI).
 * Some hold one document, some a batch of several, and three are broken in
 * the ways the splitter refuses. The files go through the directory
 * pipeline; the pushes, some of which the landing node refuses, through
 * the HTTP push pipeline.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class DemoFiles
{

    /**
     * One item to deliver, and what the pipeline should do with it.
     *
     * @param name          the file name
     * @param content       the item's bytes
     * @param status        the splitter's final status, {@code COMPLETED}
     *                      or {@code FAILED}; for a push the landing node
     *                      refuses, its HTTP status, such as {@code 413}
     * @param records       how many records it should store
     * @param failedRecords how many of those can't be converted
     * @param expected      what should happen, and why
     * @param replaces      the earlier item's file name, if this one is a
     *                      resubmission of it; null if not
     * @param duplicateOf   the earlier item with the same bytes, which the
     *                      landing node warns of; null if none
     * @param repeats       how many of its records an earlier item
     *                      already sent: the same content has the same
     *                      record ID (§7.3), so the same message ID, and
     *                      downstream gets it once
     */
    record DemoFile(String name, byte[] content, String status, int records,
            int failedRecords, String expected, String replaces,
            String duplicateOf, int repeats)
    {

        /**
         * An item with no earlier item.
         *
         * @param name          the file name
         * @param content       the item's bytes
         * @param status        the final status
         * @param records       how many records it should store
         * @param failedRecords how many of those can't be converted
         * @param expected      what should happen, and why
         */
        DemoFile(String name, byte[] content, String status, int records,
                int failedRecords, String expected)
        {
            this(name, content, status, records, failedRecords, expected,
                    null, null);
        }

        /**
         * An item none of whose records an earlier item sent.
         *
         * @param name          the file name
         * @param content       the item's bytes
         * @param status        the final status
         * @param records       how many records it should store
         * @param failedRecords how many of those can't be converted
         * @param expected      what should happen, and why
         * @param replaces      the earlier item it resubmits; null if none
         * @param duplicateOf   the earlier item with the same bytes; null
         *                      if none
         */
        DemoFile(String name, byte[] content, String status, int records,
                int failedRecords, String expected, String replaces,
                String duplicateOf)
        {
            this(name, content, status, records, failedRecords, expected,
                    replaces, duplicateOf, 0);
        }

        /**
         * Whether the landing node should refuse it: a push answered with
         * an HTTP error.
         *
         * @return true if its status is an HTTP status
         */
        boolean refused()
        {
            return Character.isDigit(status.charAt(0));
        }
    }

    /**
     * A push, and who sends it.
     *
     * @param file     the item and what should become of it
     * @param listed   whether the client's certificate subject is one the
     *                 node lists; false for a stranger with a certificate
     *                 from the same CA
     * @param replaces what its {@code Replaces} header names: the file name
     *                 of an earlier push, {@code unknown} for an ID the
     *                 node never landed, or null for none
     */
    record Push(DemoFile file, boolean listed, String replaces)
    {
    }

    /** The largest push the HTTP intake takes, as the demo sets it. */
    static final int MAX_PUSH_BYTES = 2 << 20;

    /** The sample documents. */
    static final Path SAMPLES =
            Path.of("..", "connector-core", "src", "test", "resources", "ccda");

    /** The XML declaration of a UTF-8 file. */
    private static final String UTF8_DECLARATION =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

    /** Opens a batch of documents. */
    private static final String BATCH_START =
            "<batch xmlns=\"urn:hl7-org:v3\">\n";

    /** Not instantiable. */
    private DemoFiles()
    {
    }

    /**
     * Builds the demo's files.
     *
     * @return the files, in delivery order
     * @throws IOException if a sample can't be read
     */
    static List<DemoFile> build() throws IOException
    {
        String synthea = document("google-synthea-sample1.xml");
        String alice = document("alice-newman-uscdi-v3.xml");
        String master = document("master-ccd.xml");
        String uscdi = document("uscdi-v3-all-mappings.xml");
        String epic = document("epic-inpatient-ccd-deidentified.xml");
        // Each file gets its own copy of a sample, tagged in its title.
        // Record IDs are keyed hashes of the content (design §7.3), so the
        // same document in two files would be one record to a downstream
        // consumer, which isn't what this demo is about.
        String synthea4 = tagged(synthea, "file 4");
        String alice4 = tagged(alice, "file 4");
        String uscdi4 = tagged(uscdi, "file 4");
        String epic5 = tagged(epic, "file 5");
        String epic7 = tagged(epic, "file 7");
        // Splits fine (the splitter only finds element boundaries) but
        // isn't well-formed XML: &nbsp; is an HTML entity that a C-CDA
        // document can't use without declaring it, so the converter rejects
        // it.
        String unconvertible =
                alice4.replaceFirst("</title>", "&nbsp;(amended)</title>");
        // Cut off part-way through, after a tag, so the batch and the
        // document never close.
        String truncated = master.substring(0,
                master.indexOf('>', master.length() / 2) + 1);
        return List.of(
                new DemoFile("01-synthea-patient.xml", single(synthea),
                        "COMPLETED", 1, 0,
                        "completes: 1 document converted to JSON"),
                new DemoFile("02-alice-newman.xml", single(alice),
                        "COMPLETED", 1, 0,
                        "completes: 1 document converted to JSON"),
                new DemoFile("03-clinic-batch.xml",
                        batch(master, uscdi, epic), "COMPLETED", 3, 0,
                        "completes: a batch of 3 documents, each its own"
                                + " record"),
                new DemoFile("04-batch-with-bad-document.xml",
                        batch(synthea4, unconvertible, uscdi4), "COMPLETED",
                        3, 1,
                        "completes with 1 failed record: the middle"
                                + " document has an undeclared &nbsp;"
                                + " entity, so it is stored in its original"
                                + " form (CONVERT_XML_MALFORMED) and the"
                                + " other two convert"),
                new DemoFile("05-truncated-batch.xml",
                        unclosedBatch(epic5, truncated), "FAILED", 1, 0,
                        "fails: the file stops part-way through its second"
                                + " document (PARSE_UNBALANCED_ELEMENT); the"
                                + " first document was already stored, and"
                                + " the original stays in landing"),
                new DemoFile("06-utf16-document.xml", utf16(alice), "FAILED",
                        0, 0,
                        "fails at once: UTF-16 isn't supported"
                                + " (PARSE_XML_UNSUPPORTED_ENCODING)"),
                new DemoFile("07-epic-inpatient.xml", single(epic7),
                        "COMPLETED", 1, 0,
                        "completes: the connector carries on after"
                                + " failures"),
                new DemoFile("08-synthea-patient-again.xml", single(synthea),
                        "COMPLETED", 1, 0,
                        "the same bytes as file 1, sent again: landed as an"
                                + " item of its own, with an"
                                + " item.duplicate_content warning naming"
                                + " file 1's item; nothing is dropped, and"
                                + " it splits to the same record ID, so"
                                + " downstream gets that record once",
                        null, "01-synthea-patient.xml", 1),
                new DemoFile("09-truncated-batch-corrected.xml",
                        batch(epic5, tagged(master, "file 9")), "COMPLETED",
                        2, 0,
                        "file 5 corrected and sent again, naming file 5's"
                                + " item in a .sdh-replaces file: landed and"
                                + " linked to it, never in its place; both"
                                + " documents complete, and the first,"
                                + " unchanged, has file 5's record ID",
                        "05-truncated-batch.xml", null, 1));
    }

    /**
     * Builds the pushes, for the HTTP push pipeline.
     *
     * @return the pushes, in order
     * @throws IOException if a sample can't be read
     */
    static List<Push> pushes() throws IOException
    {
        String alice = tagged(document("alice-newman-uscdi-v3.xml"),
                "push 1");
        String synthea = tagged(document("google-synthea-sample1.xml"),
                "push 2");
        String epic = tagged(document("epic-inpatient-ccd-deidentified.xml"),
                "push 2");
        String uscdi = tagged(document("uscdi-v3-all-mappings.xml"),
                "push 6");
        StringBuilder large = new StringBuilder(new String(single(alice),
                UTF_8));
        while (large.length() <= MAX_PUSH_BYTES)
        {
            large.append(" ".repeat(64 * 1024));
        }
        return List.of(
                new Push(new DemoFile("p1-referral.xml", single(alice),
                        "COMPLETED", 1, 0,
                        "202 Accepted with a receipt, then completes"),
                        true, null),
                new Push(new DemoFile("p2-discharge-batch.xml",
                        batch(synthea, epic), "COMPLETED", 2, 0,
                        "202 Accepted, then completes: 2 documents"),
                        true, null),
                new Push(new DemoFile("p3-unlisted-client.xml",
                        single(alice), "403", 0, 0,
                        "403 Forbidden: the client's certificate is from"
                                + " the trusted CA, but its subject isn't"
                                + " listed; nothing is landed"),
                        false, null),
                new Push(new DemoFile("p4-too-large.xml",
                        large.toString().getBytes(UTF_8), "413", 0, 0,
                        "413 Content Too Large: over the intake's 2 MiB"
                                + " limit (push.too_large); nothing is"
                                + " landed"),
                        true, null),
                new Push(new DemoFile("p5-replaces-unknown.xml",
                        single(alice), "400", 0, 0,
                        "400 Bad Request: its Replaces header names an item"
                                + " this intake never landed"
                                + " (push.unknown_replaces); nothing is"
                                + " landed"),
                        true, "unknown"),
                new Push(new DemoFile("p6-referral-corrected.xml",
                        single(uscdi), "COMPLETED", 1, 0,
                        "202 Accepted: a correction of push 1, naming its"
                                + " item in the Replaces header; linked to"
                                + " it, never in its place",
                        "p1-referral.xml", null),
                        true, "p1-referral.xml"),
                new Push(new DemoFile("p7-utf16-document.xml",
                        utf16(document("alice-newman-uscdi-v3.xml")),
                        "FAILED", 0, 0,
                        "202 Accepted: landing takes any bytes; the"
                                + " splitter then fails it"
                                + " (PARSE_XML_UNSUPPORTED_ENCODING)"),
                        true, null));
    }

    /**
     * A sample document without its byte order mark or XML declaration,
     * ready to stand alone or sit inside a batch.
     *
     * @param file the sample's file name
     * @return the document
     * @throws IOException if the sample can't be read
     */
    static String document(String file) throws IOException
    {
        String text = Files.readString(SAMPLES.resolve(file), UTF_8);
        if (text.startsWith("﻿"))
        {
            text = text.substring(1);
        }
        if (text.startsWith("<?xml"))
        {
            text = text.substring(text.indexOf("?>") + 2).stripLeading();
        }
        return text;
    }

    /**
     * The document with a tag added to its title, so its content (and
     * record ID) differs from the sample's.
     *
     * @param document the document
     * @param tag      the tag
     * @return the tagged document
     */
    static String tagged(String document, String tag)
    {
        return document.replaceFirst("</title>",
                " [demo " + tag + "]</title>");
    }

    /**
     * One document as a UTF-8 file.
     *
     * @param document the document
     * @return the file's bytes
     */
    static byte[] single(String document)
    {
        return (UTF8_DECLARATION + document).getBytes(UTF_8);
    }

    /**
     * Several documents in one file, inside a wrapper element: each
     * ClinicalDocument becomes a record.
     *
     * @param documents the documents
     * @return the file's bytes
     */
    static byte[] batch(String... documents)
    {
        StringBuilder text = new StringBuilder(UTF8_DECLARATION + BATCH_START);
        for (String document : documents)
        {
            text.append(document.strip()).append('\n');
        }
        return text.append("</batch>\n").toString().getBytes(UTF_8);
    }

    /**
     * A batch whose second document is cut off, so it never closes.
     *
     * @param complete the whole first document
     * @param cutOff   the cut-off second document
     * @return the file's bytes
     */
    static byte[] unclosedBatch(String complete, String cutOff)
    {
        return (UTF8_DECLARATION + BATCH_START + complete.strip() + "\n"
                + cutOff).getBytes(UTF_8);
    }

    /**
     * One document as a UTF-16 file with a byte order mark.
     *
     * @param document the document
     * @return the file's bytes
     */
    static byte[] utf16(String document)
    {
        return ("﻿<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n"
                + document).getBytes(StandardCharsets.UTF_16LE);
    }
}
