package com.sdh.healthcare.connector.contract.message;

import java.time.Instant;
import java.util.UUID;

/** A message published by a connector. The payload is always sealed (signed, then encrypted). */
public sealed interface ConnectorMessage
        permits ConnectorMessage.FileStarted, ConnectorMessage.SectionStored, ConnectorMessage.FileCompleted {

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

    /** A section was committed to storage and can be read with connector-queue-reader. */
    record SectionStored(
            String sectionId,
            UUID fileId,
            long seq,
            long sourceOffset,
            long sourceLength,
            String format,
            long plaintextLength,
            int chunkCount,
            StorageRef storage)
            implements ConnectorMessage {
        @Override
        public MessageType type() {
            return MessageType.SECTION_STORED;
        }

        @Override
        public String messageId() {
            return sectionId;
        }
    }

    /** Every section of a file was stored and published, and the original was archived. */
    record FileCompleted(
            UUID fileId, long sectionCount, long fileSize, String fileDigest, Instant startedAt, Instant completedAt)
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
