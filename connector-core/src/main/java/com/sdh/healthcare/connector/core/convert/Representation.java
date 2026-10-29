package com.sdh.healthcare.connector.core.convert;

import java.util.Map;

/**
 * What the stored bytes of a section are: a media type, plus the JSON mapping when the section was converted
 * (design §6.4). Stored with every section and carried in its message.
 *
 * @param mapping the converter's mapping name and version (e.g. {@code "csv-row/1"}); null when not converted
 */
public record Representation(String contentType, String mapping) {
    public static final String JSON = "application/json";

    private static final Map<String, String> RAW = Map.of(
            "csv", "text/csv",
            "ccda", "application/xml",
            "xml", "application/xml",
            "x12", "application/edi-x12",
            "edifact", "application/edifact",
            "fhir", "application/fhir+json");

    /** A converted section. */
    public static Representation json(String mapping) {
        return new Representation(JSON, mapping);
    }

    /** A section stored as split, in its original form. */
    public static Representation raw(String format) {
        return new Representation(RAW.getOrDefault(format, "application/octet-stream"), null);
    }
}
