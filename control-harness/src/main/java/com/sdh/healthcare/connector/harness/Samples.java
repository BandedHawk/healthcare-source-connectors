/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * The scenarios' documents: the synthetic and de-identified C-CDA samples
 * in {@code connector-core/src/test/resources/ccda/} (HL7's
 * CCDAtoFHIRSamples and others; no PHI), each given a tag of its own in
 * its title, since a record's ID is a keyed hash of its content; and the
 * malformed ones the failure scenarios need.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Samples
{

    /** A sample with one patient. */
    public static final String SYNTHEA = "google-synthea-sample1.xml";

    /** A sample with USCDI v3's data. */
    public static final String ALICE = "alice-newman-uscdi-v3.xml";

    /** HL7's master CCD. */
    public static final String MASTER = "master-ccd.xml";

    /** A de-identified inpatient CCD. */
    public static final String EPIC = "epic-inpatient-ccd-deidentified.xml";

    /** The XML declaration each item starts with. */
    private static final String DECLARATION =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

    /** A batch's opening element. */
    private static final String BATCH = "<batch xmlns=\"urn:hl7-org:v3\">\n";

    /** Where the samples are. */
    private final Path directory;

    /**
     * Reads the samples from the repository.
     *
     * @param root the repository's root
     */
    public Samples(Path root)
    {
        directory = root.resolve("connector-core").resolve("src")
                .resolve("test").resolve("resources").resolve("ccda");
    }

    /**
     * A sample, tagged in its title, without its XML declaration.
     *
     * @param sample the sample's file
     * @param tag    what makes it a document of its own
     * @return the document
     */
    public String document(String sample, String tag)
    {
        try
        {
            String text = Files.readString(directory.resolve(sample), UTF_8);
            if (text.startsWith("﻿"))
            {
                text = text.substring(1);
            }
            if (text.startsWith("<?xml"))
            {
                text = text.substring(text.indexOf("?>") + 2).stripLeading();
            }
            return text.replaceFirst("</title>", " [harness " + tag
                    + "]</title>");
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
    }

    /**
     * A tag no other run has.
     *
     * @param what what it is for
     * @return the tag
     */
    public static String tag(String what)
    {
        return what + " " + UUID.randomUUID();
    }

    /**
     * One document as an item.
     *
     * @param document the document
     * @return the item's bytes
     */
    public static byte[] single(String document)
    {
        return (DECLARATION + document).getBytes(UTF_8);
    }

    /**
     * Documents as one item, a batch.
     *
     * @param documents the documents
     * @return the item's bytes
     */
    public static byte[] batch(String... documents)
    {
        StringBuilder text = new StringBuilder(DECLARATION + BATCH);
        for (String document : documents)
        {
            text.append(document.strip()).append('\n');
        }
        return text.append("</batch>\n").toString().getBytes(UTF_8);
    }

    /**
     * A document that splits but doesn't convert: an undeclared HTML
     * entity makes it XML that isn't well-formed.
     *
     * @param document the document
     * @return the broken document
     */
    public static String unconvertible(String document)
    {
        return document.replaceFirst("</title>", "&nbsp;(amended)</title>");
    }

    /**
     * An item cut off part-way through its second document, which the
     * split stage fails.
     *
     * @param complete the first, complete document
     * @param cutOff   the second, to cut off
     * @return the item's bytes
     */
    public static byte[] truncated(String complete, String cutOff)
    {
        return (DECLARATION + BATCH + complete.strip() + "\n"
                + cutOff.substring(0, cutOff.indexOf('>',
                        cutOff.length() / 2) + 1)).getBytes(UTF_8);
    }
}
