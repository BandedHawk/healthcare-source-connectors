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
    PARSE_FHIR_UNEXPECTED_RESOURCE
}
