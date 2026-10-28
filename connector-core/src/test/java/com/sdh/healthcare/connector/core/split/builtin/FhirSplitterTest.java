package com.sdh.healthcare.connector.core.split.builtin;

import static com.sdh.healthcare.connector.core.split.SplitTesting.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import org.junit.jupiter.api.Test;

class FhirSplitterTest {
    private final FhirSplitter splitter = new FhirSplitter();

    static final String B1 = "{\"resourceType\":\"Bundle\",\"id\":\"1\",\"entry\":[{\"resource\":{\"resourceType\":\"Patient\","
            + "\"name\":[{\"text\":\"a } ] \\\" { [\"}]}}]}";
    static final String B2 = "{ \"id\" : \"2\", \"meta\": {\"resourceType\": \"Nope\"}, \"resourceType\" : \"Bundle\" }";
    static final String B3 = "{\"resourceType\":\"Bundle\",\"note\":\"\\\\\"}";

    @Test
    void arrayOfBundles() throws Exception {
        String in = "﻿ [\n  " + B1 + " ,\n" + B2 + ",\n" + B3 + "\n]\n";
        assertThat(splitAndCheckResume(splitter, in, cfg("fhir"))).containsExactly(B1, B2, B3);
    }

    @Test
    void ndjsonAndSingleBundle() throws Exception {
        assertThat(splitAndCheckResume(splitter, B1 + "\n" + B2 + "\r\n" + B3 + "\n", cfg("fhir")))
                .containsExactly(B1, B2, B3);
        assertThat(splitAndCheckResume(splitter, B2, cfg("fhir"))).containsExactly(B2);
        assertThat(splitAndCheckResume(splitter, "[ ]", cfg("fhir"))).isEmpty();
        assertThat(splitAndCheckResume(splitter, "  \n", cfg("fhir"))).isEmpty();
    }

    @Test
    void groupedBulkExportNdjson() throws Exception {
        String p1 = "{\"resourceType\":\"Patient\",\"id\":\"1\"}";
        String p2 = "{\"resourceType\":\"Patient\",\"id\":\"2\"}";
        String p3 = "{\"resourceType\":\"Patient\",\"id\":\"3\"}";
        var cfg = cfg("fhir").withOption("fhir.ndjson-group-size", "2");
        assertThat(splitAndCheckResume(splitter, p1 + "\n" + p2 + "\n" + p3 + "\n", cfg))
                .containsExactly(p1 + "\n" + p2, p3);
        assertThat(splitAndCheckResume(splitter, "[" + p1 + "," + p2 + "," + p3 + "]", cfg))
                .containsExactly("[" + p1 + "," + p2 + "]", "[" + p3 + "]");
    }

    @Test
    void wrongResourceTypeIsRejected() {
        String patient = "{\"resourceType\":\"Patient\"}";
        var e = splitError(splitter, B1 + "\n" + patient, cfg("fhir"));
        assertThat(e.code()).isEqualTo(ErrorCode.PARSE_FHIR_UNEXPECTED_RESOURCE);
        assertThat(e.offset()).isEqualTo(B1.length() + 1);
        assertThat(splitError(splitter, "[1,2]", cfg("fhir")).code())
                .isEqualTo(ErrorCode.PARSE_FHIR_UNEXPECTED_RESOURCE);
    }

    @Test
    void malformedJson() {
        assertThat(splitError(splitter, "[" + B1 + " " + B2 + "]", cfg("fhir")).code())
                .isEqualTo(ErrorCode.PARSE_JSON_MALFORMED);
        assertThat(splitError(splitter, "{\"resourceType\":\"Bundle\",\"x\":[}", cfg("fhir")).code())
                .isEqualTo(ErrorCode.PARSE_JSON_MALFORMED);
        assertThat(splitError(splitter, "[" + B1, cfg("fhir")).code()).isEqualTo(ErrorCode.PARSE_JSON_MALFORMED);
        assertThat(splitError(splitter, "[" + B1 + "] junk", cfg("fhir")).code())
                .isEqualTo(ErrorCode.PARSE_JSON_MALFORMED);
        assertThat(splitError(splitter, "{\"resourceType\":\"Bundle", cfg("fhir")).code())
                .isEqualTo(ErrorCode.PARSE_JSON_MALFORMED);
    }
}
