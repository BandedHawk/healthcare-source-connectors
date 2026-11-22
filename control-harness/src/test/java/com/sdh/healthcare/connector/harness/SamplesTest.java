/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link Samples}: synthetic documents tagged, batched, broken and
 * cut off as the scenarios need.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SamplesTest
{

    /** The samples, from the repository the module is in. */
    static final Samples SAMPLES = new Samples(Path.of("..")
            .toAbsolutePath().normalize());

    /** Creates the test. */
    SamplesTest()
    {
    }

    /** A document is tagged in its title, without a declaration. */
    @Test
    void documentIsTagged()
    {
        String document = SAMPLES.document(Samples.SYNTHEA, "t-1");

        assertThat(document).startsWith("<").doesNotStartWith("<?xml")
                .contains("[harness t-1]</title>");
    }

    /** A batch holds each document once, in one declaration. */
    @Test
    void batchHoldsEachDocument()
    {
        String batch = new String(Samples.batch(SAMPLES.document(
                Samples.ALICE, "a"), SAMPLES.document(Samples.EPIC, "b")),
                UTF_8);

        assertThat(batch).startsWith("<?xml").contains("[harness a]",
                "[harness b]");
        assertThat(batch.split("<\\?xml version", -1)).hasSize(2);
    }

    /** An unconvertible document has an undeclared entity. */
    @Test
    void unconvertibleHasAnUndeclaredEntity()
    {
        assertThat(Samples.unconvertible(SAMPLES.document(Samples.MASTER,
                "m"))).contains("&nbsp;(amended)</title>");
    }

    /** A cut-off item ends part-way through its second document. */
    @Test
    void truncatedEndsPartWay()
    {
        String second = SAMPLES.document(Samples.EPIC, "c");
        String item = new String(Samples.truncated(SAMPLES.document(
                Samples.ALICE, "f"), second), UTF_8);

        assertThat(item).contains("[harness f]").doesNotEndWith(
                second.strip());
        assertThat(item.length()).isLessThan(second.length() * 2);
    }

    /** Tags differ run to run. */
    @Test
    void tagsDiffer()
    {
        assertThat(Samples.tag("x")).isNotEqualTo(Samples.tag("x"));
    }
}
