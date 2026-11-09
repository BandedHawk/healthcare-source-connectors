/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

import static com.sdh.healthcare.connector.core.split.SplitTesting.config;
import static com.sdh.healthcare.connector.core.split.SplitTesting.splitAndCheckResume;
import static com.sdh.healthcare.connector.core.split.SplitTesting.splitError;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FhirSplitter}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class FhirSplitterTest
{

    /** A Bundle with brackets and an escaped quote inside a string. */
    static final String B1 = "{\"resourceType\":\"Bundle\",\"id\":\"1\","
            + "\"entry\":[{\"resource\":{\"resourceType\":\"Patient\","
            + "\"name\":[{\"text\":\"a } ] \\\" { [\"}]}}]}";

    /** A Bundle whose type comes last, after a nested resourceType. */
    static final String B2 = "{ \"id\" : \"2\", \"meta\": {\"resourceType\":"
            + " \"Nope\"}, \"resourceType\" : \"Bundle\" }";

    /** A Bundle with an escaped backslash at the end of a string. */
    static final String B3 = "{\"resourceType\":\"Bundle\",\"note\":\"\\\\\"}";

    /** The splitter under test. */
    private final FhirSplitter splitter = new FhirSplitter();

    /**
     * An array of Bundles, after a byte order mark, splits into them.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void arrayOfBundles() throws Exception
    {
        String input = "﻿ [\n  " + B1 + " ,\n" + B2 + ",\n" + B3
                + "\n]\n";
        assertThat(splitAndCheckResume(splitter, input, config("fhir")))
                .containsExactly(B1, B2, B3);
    }

    /**
     * NDJSON, a single Bundle, and empty input.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void ndjsonAndSingleBundle() throws Exception
    {
        assertThat(splitAndCheckResume(splitter,
                B1 + "\n" + B2 + "\r\n" + B3 + "\n", config("fhir")))
                .containsExactly(B1, B2, B3);
        assertThat(splitAndCheckResume(splitter, B2, config("fhir")))
                .containsExactly(B2);
        // An empty array holds nothing; blank is nothing sent (STAGES.md
        // §16).
        assertThat(splitError(splitter, "[ ]", config("fhir")).code())
                .isEqualTo(ErrorCode.SPLIT_NO_RECORDS);
        assertThat(splitError(splitter, "  \n", config("fhir")).code())
                .isEqualTo(ErrorCode.SPLIT_EMPTY);
    }

    /**
     * Resources are grouped; groups from an array are wrapped in brackets.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void groupedBulkExportNdjson() throws Exception
    {
        String patient1 = "{\"resourceType\":\"Patient\",\"id\":\"1\"}";
        String patient2 = "{\"resourceType\":\"Patient\",\"id\":\"2\"}";
        String patient3 = "{\"resourceType\":\"Patient\",\"id\":\"3\"}";
        SplitConfig settings = config("fhir")
                .withOption("fhir.ndjson-group-size", "2");
        assertThat(splitAndCheckResume(splitter,
                patient1 + "\n" + patient2 + "\n" + patient3 + "\n",
                settings))
                .containsExactly(patient1 + "\n" + patient2, patient3);
        assertThat(splitAndCheckResume(splitter,
                "[" + patient1 + "," + patient2 + "," + patient3 + "]",
                settings))
                .containsExactly("[" + patient1 + "," + patient2 + "]",
                        "[" + patient3 + "]");
    }

    /** A resource other than the boundary fails the file at it. */
    @Test
    void wrongResourceTypeIsRejected()
    {
        String patient = "{\"resourceType\":\"Patient\"}";
        SplitException error = splitError(splitter, B1 + "\n" + patient,
                config("fhir"));
        assertThat(error.code())
                .isEqualTo(ErrorCode.PARSE_FHIR_UNEXPECTED_RESOURCE);
        assertThat(error.offset()).isEqualTo(B1.length() + 1);
        assertThat(splitError(splitter, "[1,2]", config("fhir")).code())
                .isEqualTo(ErrorCode.PARSE_FHIR_UNEXPECTED_RESOURCE);
    }

    /** Malformed JSON fails the file. */
    @Test
    void malformedJson()
    {
        assertThat(splitError(splitter, "[" + B1 + " " + B2 + "]",
                config("fhir")).code())
                .isEqualTo(ErrorCode.PARSE_JSON_MALFORMED);
        assertThat(splitError(splitter,
                "{\"resourceType\":\"Bundle\",\"x\":[}",
                config("fhir")).code())
                .isEqualTo(ErrorCode.PARSE_JSON_MALFORMED);
        assertThat(splitError(splitter, "[" + B1, config("fhir")).code())
                .isEqualTo(ErrorCode.PARSE_JSON_MALFORMED);
        assertThat(splitError(splitter, "[" + B1 + "] junk",
                config("fhir")).code())
                .isEqualTo(ErrorCode.PARSE_JSON_MALFORMED);
        assertThat(splitError(splitter, "{\"resourceType\":\"Bundle",
                config("fhir")).code())
                .isEqualTo(ErrorCode.PARSE_JSON_MALFORMED);
    }
}
