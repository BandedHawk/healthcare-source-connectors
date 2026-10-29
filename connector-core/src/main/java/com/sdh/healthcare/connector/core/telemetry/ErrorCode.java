package com.sdh.healthcare.connector.core.telemetry;

/** Stable, PHI-free error codes used in logs, telemetry and failure records (design §7.5). */
public enum ErrorCode {
    LEASE_HELD,
    LEASE_LOST,
    LEASE_FENCED,
    COMMIT_CONFLICT,
    CHECKPOINT_TAMPERED,
    LIMIT_FILE_SIZE,
    LIMIT_SECTION_SIZE,
    LIMIT_SECTION_COUNT,

    // File failures that aren't about content (design §11)
    SOURCE_READ_FAILED,
    STORAGE_FAILED,
    PUBLISH_FAILED,

    /** A saved resume point doesn't fit the file (wrong file, or a checkpoint from another splitter). */
    RESUME_INVALID,

    // Splitting (design §6)
    PARSE_UNTERMINATED_QUOTE,
    PARSE_UNBALANCED_ELEMENT,
    PARSE_XML_MALFORMED,
    PARSE_XML_UNSUPPORTED_ENCODING,
    PARSE_X12_INVALID_ENVELOPE,
    PARSE_X12_UNBALANCED,
    PARSE_X12_CONTROL_MISMATCH,
    PARSE_X12_UNEXPECTED_SEGMENT,
    PARSE_JSON_MALFORMED,
    PARSE_FHIR_UNEXPECTED_RESOURCE,
    PARSE_EDIFACT_INVALID_ENVELOPE,
    PARSE_EDIFACT_UNBALANCED,
    PARSE_EDIFACT_CONTROL_MISMATCH,
    PARSE_EDIFACT_UNEXPECTED_SEGMENT,

    // Converting a section to JSON (design §6.4). These fail the section only: it is stored in its original form.
    /** Bytes that aren't valid in the section's character encoding. */
    CONVERT_INVALID_ENCODING,
    /** A CSV record with a different number of fields from the header. */
    CONVERT_CSV_FIELD_COUNT,
    /** A CSV record that breaks RFC 4180 quoting (text after a closing quote). */
    CONVERT_CSV_MALFORMED,
    CONVERT_X12_MALFORMED,
    CONVERT_EDIFACT_MALFORMED,
    /** An EDIFACT syntax identifier (UNB S001) whose character set isn't supported. */
    CONVERT_EDIFACT_CHARSET,
    CONVERT_XML_MALFORMED,
    /** A section over {@code split.convert-max-bytes}: converting it would need too much memory. */
    CONVERT_TOO_LARGE
}
