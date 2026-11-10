/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.telemetry;

import com.sdh.healthcare.connector.core.pipeline.IngestPipeline;

/**
 * The reasons a file, a record or the connector itself can fail, as they
 * appear in logs, telemetry and failure records (design §7.5). The names stay
 * the same from release to release, so alerts and dashboards can rely on
 * them, and they hold no PHI: a code says what went wrong, never what the
 * data was.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum ErrorCode
{

    // The lease and the checkpoint (design §9)

    /**
     * Another running instance holds the lease, so this one may not start
     * processing. The service logs the code and exits.
     */
    LEASE_HELD,

    /**
     * This instance lost the lease while running: it couldn't renew it in
     * time, or the lease store shows another holder. It stops and exits.
     */
    LEASE_LOST,

    /**
     * This instance lost the lease because a table records a newer epoch:
     * a successor has taken over and fenced it out. It stops and exits.
     */
    LEASE_FENCED,

    /** The inbound directory belongs to another connector (design §9.5). */
    INBOUND_DIR_CLAIMED,

    /**
     * A read-write intake's source can't be written, so items couldn't be
     * deleted, moved or renamed after landing (design §5.3). The landing
     * node won't start.
     */
    INTAKE_NOT_WRITABLE,

    /**
     * An intake's source can't be reached or listed, such as a network
     * share that is down. The landing node keeps trying, and reports when
     * the source is back.
     */
    INTAKE_UNREACHABLE,

    /**
     * The landing ledger is too large for the heap: its index would take
     * more than half of it (design §5.3.2). The landing node won't start
     * until its memory is raised.
     */
    LEDGER_TOO_LARGE,

    /**
     * An {@code item.landed} message the splitter won't take: it can't be
     * opened or verified, isn't {@code item.landed}, or names another
     * intake or landing table. It is dropped, and the landing node's
     * ledger still has the item.
     */
    MESSAGE_REJECTED,

    /**
     * A commit that conflicts with another writer. Not raised at present:
     * conflicting commits are retried (design §9).
     */
    COMMIT_CONFLICT,

    /**
     * A checkpoint that fails its integrity check. The service stops rather
     * than guess where to carry on.
     */
    CHECKPOINT_TAMPERED,

    // Limits (design §6.3)

    /**
     * A file larger than {@code max-item-bytes}. It is rejected before
     * ingestion and kept only in the encrypted local {@code failed/} folder.
     */
    LIMIT_ITEM_SIZE,

    /** A record larger than {@code max-record-bytes}. */
    LIMIT_RECORD_SIZE,

    /** A file with more records than {@code max-records-per-item}. */
    LIMIT_RECORD_COUNT,

    // File failures that aren't about content (design §11)

    /** The file couldn't be read. */
    SOURCE_READ_FAILED,

    /** The records or the archived file couldn't be written to storage. */
    STORAGE_FAILED,

    /** The broker didn't acknowledge a message after all retries. */
    PUBLISH_FAILED,

    /**
     * A saved resume point doesn't fit the file: it is a different file, or
     * the checkpoint came from another splitter.
     */
    RESUME_INVALID,

    /**
     * Splitting was resumed {@link IngestPipeline#FAIL_AFTER_RESTARTS}
     * times in a row without committing anything. Something in the file keeps
     * stopping the connector, such as a record that runs it out of memory
     * even unconverted (design §9.2).
     */
    RESUME_NO_PROGRESS,

    // Splitting (design §6)

    /**
     * A record the splitter couldn't parse, with a code this version
     * doesn't know: sent by a newer splitter. The record is stored as split
     * and reported as failed like any other (STAGES.md §9).
     */
    PARSE_UNRECOGNISED,

    /**
     * An item, or a message of a batch, that is 0 bytes or blank: only
     * spaces, tabs, CR and LF, after a byte-order mark. Nothing was sent, so
     * it fails as a whole for an operator to decide (STAGES.md §16).
     */
    SPLIT_EMPTY,

    /**
     * An item, or a message of a batch, that the splitter read as its format
     * and found to hold nothing to cut: a CSV header alone, an X12 or
     * EDIFACT envelope with nothing inside, a FHIR {@code []}, a Parquet
     * file with no rows. It fails as a whole for an operator to decide
     * (STAGES.md §16).
     */
    SPLIT_NO_RECORDS,

    /** A CSV quoted field that is never closed. */
    PARSE_UNTERMINATED_QUOTE,

    /**
     * An XML file that ends with elements still open, or closes one it never
     * opened.
     */
    PARSE_UNBALANCED_ELEMENT,

    /** An XML file that isn't well-formed. */
    PARSE_XML_MALFORMED,

    /** An XML file in a character encoding that isn't supported. */
    PARSE_XML_UNSUPPORTED_ENCODING,

    /** An X12 interchange header (ISA) or envelope segment that is invalid. */
    PARSE_X12_INVALID_ENVELOPE,

    /**
     * An X12 file whose envelopes don't pair up: one that ends before its
     * trailer, or a trailer without its header.
     */
    PARSE_X12_UNBALANCED,

    /**
     * An X12 trailer whose control number or count doesn't match its
     * header.
     */
    PARSE_X12_CONTROL_MISMATCH,

    /** An X12 segment where the envelope structure doesn't allow one. */
    PARSE_X12_UNEXPECTED_SEGMENT,

    /** A JSON file that isn't well-formed. */
    PARSE_JSON_MALFORMED,

    /**
     * A Parquet file that can't be read: no footer, a broken footer or
     * page, or a row group the footer doesn't describe.
     */
    PARSE_PARQUET_MALFORMED,

    /**
     * A Parquet column of a type with no JSON form here, such as a variant
     * or a geometry.
     */
    PARSE_PARQUET_UNSUPPORTED_TYPE,

    /**
     * A FHIR file holding something other than the resources expected, such
     * as an item that isn't an object or has the wrong resource type.
     */
    PARSE_FHIR_UNEXPECTED_RESOURCE,

    /** An EDIFACT interchange header (UNA, UNB) that is invalid. */
    PARSE_EDIFACT_INVALID_ENVELOPE,

    /**
     * An EDIFACT file whose envelopes don't pair up: one that ends before its
     * trailer, or a trailer without its header.
     */
    PARSE_EDIFACT_UNBALANCED,

    /**
     * An EDIFACT trailer whose control reference or count doesn't match its
     * header.
     */
    PARSE_EDIFACT_CONTROL_MISMATCH,

    /** An EDIFACT segment where the envelope structure doesn't allow one. */
    PARSE_EDIFACT_UNEXPECTED_SEGMENT,

    // Converting a record to JSON (design §6.4). These fail the record
    // only: it is stored in its original form.

    /** Bytes that aren't valid in the record's character encoding. */
    CONVERT_INVALID_ENCODING,

    /** A CSV row with a different number of fields from the header. */
    CONVERT_CSV_FIELD_COUNT,

    /**
     * A CSV row that breaks RFC 4180 quoting, such as text after a closing
     * quote.
     */
    CONVERT_CSV_MALFORMED,

    /**
     * An X12 record that doesn't start with an interchange header, or whose
     * segments can't be read.
     */
    CONVERT_X12_MALFORMED,

    /**
     * An EDIFACT record that doesn't start with an interchange header, or
     * whose segments can't be read.
     */
    CONVERT_EDIFACT_MALFORMED,

    /**
     * An EDIFACT syntax identifier (UNB S001) whose character set isn't
     * supported.
     */
    CONVERT_EDIFACT_CHARSET,

    /** An XML record that isn't well-formed, or uses a DTD or entity. */
    CONVERT_XML_MALFORMED,

    /**
     * A record larger than {@code sdh.convert.max-bytes}: converting it
     * would need too much memory. It is stored as split (STAGES.md §9).
     */
    CONVERT_TOO_LARGE,

    /**
     * Not converted, because two convert workers stopped part-way through
     * this record (it is on its third claim), and converting it may be what
     * stops them (by running them out of memory, say). It is stored as
     * split with this code (STAGES.md §9).
     */
    CONVERT_SKIPPED,

    /**
     * The converter failed in a way it doesn't report itself: an unexpected
     * exception, a bug in the converter rather than a problem it recognised
     * in the record. The record is stored in its original form like any
     * other conversion failure, and the file carries on (design §6.4).
     */
    CONVERT_FAILED,

    /**
     * A stage had already stored a record, and produced different output
     * for it when it came again: the converter or the mapping changed
     * between the two. No longer raised: since 2026-11-08 a record received
     * again is a duplicate whatever its version (STAGES.md §14.3), and a
     * forced reprocess stores a different outcome as a new object (§15.4).
     * Kept so events and messages from before still read.
     */
    RECORD_CONFLICT,

    /**
     * A record failed at a stage whose processor gave no code of its own;
     * recorded so in the stage's fail store (STAGES.md §15.3).
     */
    STAGE_FAILED,

    /**
     * An operator's correction of a piece the splitter couldn't parse
     * wasn't cut into exactly one record: a correction is one document
     * (STAGES.md §15.1, rule 6; §15.6d).
     */
    CORRECTION_RECORD_COUNT,

    /**
     * An operator's correction of an item that couldn't be split to the end
     * would change a record that was already stored: only what failed is
     * corrected (STAGES.md §15.1, rule 5; §15.6d). No longer raised: a
     * correction of the item as a whole is its next generation (§15.8.5);
     * kept to read failures recorded before.
     */
    CORRECTION_CHANGES_PASSED,

    /**
     * An operator's correction or retry of an item as a whole wasn't cut,
     * since a newer generation of the item came first (STAGES.md §15.8.5):
     * the edit is kept, and no failure is added.
     */
    CORRECTION_SUPERSEDED,

    /**
     * A stage's batch couldn't be finished (storage, the registry or the
     * broker down, or a processor's unexpected error), and is tried again
     * in place (STAGES.md §7).
     */
    STAGE_BATCH_FAILED
}
