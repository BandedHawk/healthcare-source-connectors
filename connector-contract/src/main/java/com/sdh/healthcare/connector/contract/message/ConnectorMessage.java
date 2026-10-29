package com.sdh.healthcare.connector.contract.message;

import java.time.Instant;
import java.util.UUID;

/** A message published by a connector. The payload is always sealed (signed, then encrypted). */
public sealed interface ConnectorMessage permits ConnectorMessage.FileStarted, ConnectorMessage.SectionInStorage,
        ConnectorMessage.FileCompleted {

    MessageType type();

    String messageId();

    UUID fileId();

    /** A file was picked up, or resumed in a new run. */
    record FileStarted(UUID fileId, String format, long fileSize, Instant startedAt, String connectorId)
            implements ConnectorMessage {
        @Override
        public MessageType type() {
            return MessageType.FILE_STARTED;
        }

        @Override
        public String messageId() {
            return "fs-" + fileId;
        }
    }

    /**
     * A section in storage, readable with connector-queue-reader: stored ({@link SectionStored}) or kept in its
     * original form after failing conversion ({@link SectionFailed}). {@code sourceOffset} and {@code sourceLength}
     * locate it in the original, unsplit file.
     */
    sealed interface SectionInStorage extends ConnectorMessage permits SectionStored, SectionFailed {
        String sectionId();

        long seq();

        long sourceOffset();

        long sourceLength();

        String format();

        /** What the stored bytes are, e.g. {@code application/json}, {@code text/csv}. */
        String contentType();

        long plaintextLength();

        int chunkCount();

        StorageRef storage();
    }

    /**
     * A section was committed to storage and can be read with connector-queue-reader.
     *
     * @param mapping the JSON mapping the section was converted with (e.g. {@code csv-row/1}, {@code ccda/1}); null
     *                when it was stored as split (FHIR, generic XML, delimited)
     */
    record SectionStored(
            String sectionId,
            UUID fileId,
            long seq,
            long sourceOffset,
            long sourceLength,
            String format,
            String contentType,
            String mapping,
            long plaintextLength,
            int chunkCount,
            StorageRef storage)
            implements SectionInStorage {
        @Override
        public MessageType type() {
            return MessageType.SECTION_STORED;
        }

        @Override
        public String messageId() {
            return sectionId;
        }
    }

    /**
     * A section couldn't be converted to JSON (design §6.4). It is stored as split, in its original format, for
     * remediation; the rest of the file carries on. Read it like any section.
     *
     * @param errorCode why conversion failed, e.g. {@code CONVERT_CSV_FIELD_COUNT}
     */
    record SectionFailed(
            String sectionId,
            UUID fileId,
            long seq,
            long sourceOffset,
            long sourceLength,
            String format,
            String contentType,
            String errorCode,
            long plaintextLength,
            int chunkCount,
            StorageRef storage)
            implements SectionInStorage {
        @Override
        public MessageType type() {
            return MessageType.SECTION_FAILED;
        }

        @Override
        public String messageId() {
            return sectionId;
        }
    }

    /**
     * Every section of a file was stored and published, and the original was archived.
     *
     * @param sectionCount       all sections, failed ones included
     * @param failedSectionCount sections that failed conversion and were stored in their original form
     */
    record FileCompleted(UUID fileId, long sectionCount, long failedSectionCount, long fileSize, String fileDigest,
            Instant startedAt, Instant completedAt)
            implements ConnectorMessage {
        @Override
        public MessageType type() {
            return MessageType.FILE_COMPLETED;
        }

        @Override
        public String messageId() {
            return "fc-" + fileId;
        }
    }

    /** Where a section is stored. {@code ingestedDate} is used for partition pruning. */
    record StorageRef(
            String catalog,
            String namespace,
            String table,
            java.time.LocalDate ingestedDate,
            long snapshotId,
            String kekRef) {}
}
