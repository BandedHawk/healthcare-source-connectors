/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.message;

import com.sdh.healthcare.connector.contract.position.SectionOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import java.time.Instant;
import java.util.UUID;

/**
 * A message a connector publishes (design §10.1): a file was started, a
 * section is in storage, or a file was completed. A landing node publishes
 * one more, to its splitter only: an item was landed (design §5.3.6). The
 * payload is always sealed: signed, then encrypted.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public sealed interface ConnectorMessage permits ConnectorMessage.FileStarted,
        ConnectorMessage.SectionInStorage, ConnectorMessage.FileCompleted,
        ConnectorMessage.ItemLanded
{

    /**
     * The kind of message.
     *
     * @return the message type
     */
    MessageType type();

    /**
     * The message's id, the same each time the same message is sent, so a
     * reader can drop repeats.
     *
     * @return the id
     */
    String messageId();

    /**
     * The id of the file the message is about.
     *
     * @return the file id
     */
    UUID fileId();

    /**
     * A file was picked up, or resumed in a new run.
     *
     * @param fileId      the file's id
     * @param format      the name of the file's format, such as {@code csv}
     * @param fileSize    the file's size, in bytes
     * @param startedAt   when the connector started on the file
     * @param connectorId the id of the connector that picked it up
     */
    record FileStarted(UUID fileId, String format, long fileSize,
            Instant startedAt, String connectorId)
            implements ConnectorMessage
    {

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#FILE_STARTED}
         */
        @Override
        public MessageType type()
        {
            return MessageType.FILE_STARTED;
        }

        /**
         * {@inheritDoc}
         *
         * @return "fs-" and the file id
         */
        @Override
        public String messageId()
        {
            return "fs-" + fileId;
        }
    }

    /**
     * A section in storage, readable with connector-queue-reader: stored
     * ({@link SectionStored}) or kept in its original form after failing
     * conversion ({@link SectionFailed}).
     */
    sealed interface SectionInStorage extends ConnectorMessage
            permits SectionStored, SectionFailed
    {

        /**
         * The section's id.
         *
         * @return the section id
         */
        String sectionId();

        /**
         * The section's number in its file, from 0.
         *
         * @return the sequence number
         */
        long seq();

        /**
         * Where the section starts in the original, unsplit file.
         *
         * @return the offset, in bytes
         */
        long sourceOffset();

        /**
         * How many bytes of the original file the section covers.
         *
         * @return the length, in bytes
         */
        long sourceLength();

        /**
         * The name of the file's format, such as {@code csv}.
         *
         * @return the format
         */
        String format();

        /**
         * What the stored bytes are, such as {@code application/json} or
         * {@code text/csv}.
         *
         * @return the media type
         */
        String contentType();

        /**
         * The stored section's length before compression and encryption.
         *
         * @return the length, in bytes
         */
        long plaintextLength();

        /**
         * How many encrypted chunks the section is stored in.
         *
         * @return the number of chunks
         */
        int chunkCount();

        /**
         * Where the section is stored.
         *
         * @return the storage reference
         */
        StorageRef storage();

        /**
         * Where the section's text came from in the file, as lines and
         * columns, to turn a position in the section (such as one a mapping
         * reports) into one in the file ({@link SectionOrigin#toFile}).
         *
         * @return the origin; null in messages from connectors before
         *         contract 2.1
         */
        SectionOrigin origin();
    }

    /**
     * A section was committed to storage and can be read with
     * connector-queue-reader.
     *
     * @param sectionId       the section's id
     * @param fileId          the id of its file
     * @param seq             its number in the file, from 0
     * @param sourceOffset    where it starts in the file, in bytes
     * @param sourceLength    how many bytes of the file it covers
     * @param format          the name of the file's format
     * @param contentType     what the stored bytes are, such as
     *                        {@code application/json}
     * @param mapping         the JSON mapping the section was converted with,
     *                        such as {@code csv-row/1} or {@code ccda/2};
     *                        null when it was stored as split (FHIR, generic
     *                        XML, delimited)
     * @param plaintextLength the stored length before compression and
     *                        encryption
     * @param chunkCount      how many encrypted chunks it is stored in
     * @param storage         where it is stored
     * @param origin          where the section's text came from in the file;
     *                        null before contract 2.1
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
            StorageRef storage,
            SectionOrigin origin)
            implements SectionInStorage
    {

        /**
         * A message without an origin, as connectors before contract 2.1
         * sent.
         *
         * @param sectionId       the section's id
         * @param fileId          the id of its file
         * @param seq             its number in the file, from 0
         * @param sourceOffset    where it starts in the file, in bytes
         * @param sourceLength    how many bytes of the file it covers
         * @param format          the name of the file's format
         * @param contentType     what the stored bytes are
         * @param mapping         the JSON mapping, or null when stored as
         *                        split
         * @param plaintextLength the stored length before compression and
         *                        encryption
         * @param chunkCount      how many encrypted chunks it is stored in
         * @param storage         where it is stored
         */
        public SectionStored(String sectionId, UUID fileId, long seq,
                long sourceOffset, long sourceLength, String format,
                String contentType, String mapping, long plaintextLength,
                int chunkCount, StorageRef storage)
        {
            this(sectionId, fileId, seq, sourceOffset, sourceLength, format,
                    contentType, mapping, plaintextLength, chunkCount, storage,
                    null);
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#SECTION_STORED}
         */
        @Override
        public MessageType type()
        {
            return MessageType.SECTION_STORED;
        }

        /**
         * {@inheritDoc}
         *
         * @return the section id
         */
        @Override
        public String messageId()
        {
            return sectionId;
        }
    }

    /**
     * A section couldn't be converted to JSON (design §6.4). It is stored as
     * split, in its original format, for remediation, and the rest of the
     * file carries on. Read it like any section.
     *
     * @param sectionId       the section's id
     * @param fileId          the id of its file
     * @param seq             its number in the file, from 0
     * @param sourceOffset    where it starts in the file, in bytes
     * @param sourceLength    how many bytes of the file it covers
     * @param format          the name of the file's format
     * @param contentType     the media type of the original format, such as
     *                        {@code text/csv}
     * @param errorCode       why conversion failed, such as
     *                        {@code CONVERT_CSV_FIELD_COUNT}
     * @param plaintextLength the stored length before compression and
     *                        encryption
     * @param chunkCount      how many encrypted chunks it is stored in
     * @param storage         where it is stored
     * @param origin          where the section's text came from in the file;
     *                        null before contract 2.1
     * @param errorAt         where in the file conversion failed, as a line
     *                        and column; null before contract 2.1
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
            StorageRef storage,
            SectionOrigin origin,
            SourcePosition errorAt)
            implements SectionInStorage
    {

        /**
         * A message without positions, as connectors before contract 2.1
         * sent.
         *
         * @param sectionId       the section's id
         * @param fileId          the id of its file
         * @param seq             its number in the file, from 0
         * @param sourceOffset    where it starts in the file, in bytes
         * @param sourceLength    how many bytes of the file it covers
         * @param format          the name of the file's format
         * @param contentType     the media type of the original format
         * @param errorCode       why conversion failed
         * @param plaintextLength the stored length before compression and
         *                        encryption
         * @param chunkCount      how many encrypted chunks it is stored in
         * @param storage         where it is stored
         */
        public SectionFailed(String sectionId, UUID fileId, long seq,
                long sourceOffset, long sourceLength, String format,
                String contentType, String errorCode, long plaintextLength,
                int chunkCount, StorageRef storage)
        {
            this(sectionId, fileId, seq, sourceOffset, sourceLength, format,
                    contentType, errorCode, plaintextLength, chunkCount,
                    storage, null, null);
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#SECTION_FAILED}
         */
        @Override
        public MessageType type()
        {
            return MessageType.SECTION_FAILED;
        }

        /**
         * {@inheritDoc}
         *
         * @return the section id
         */
        @Override
        public String messageId()
        {
            return sectionId;
        }
    }

    /**
     * Every section of a file was stored and published. The original is
     * the landed item with the same ID (design §5.3.6).
     *
     * @param fileId             the file's id
     * @param sectionCount       all its sections, failed ones included
     * @param failedSectionCount the sections that failed conversion and were
     *                           stored in their original form
     * @param fileSize           the file's size, in bytes
     * @param fileDigest         the keyed digest of the original, as
     *                           landed
     * @param startedAt          when the connector started on the file
     * @param completedAt        when it finished
     */
    record FileCompleted(UUID fileId, long sectionCount,
            long failedSectionCount, long fileSize, String fileDigest,
            Instant startedAt, Instant completedAt)
            implements ConnectorMessage
    {

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#FILE_COMPLETED}
         */
        @Override
        public MessageType type()
        {
            return MessageType.FILE_COMPLETED;
        }

        /**
         * {@inheritDoc}
         *
         * @return "fc-" and the file id
         */
        @Override
        public String messageId()
        {
            return "fc-" + fileId;
        }
    }

    /**
     * An item was landed: stored exactly as received, encrypted, in the
     * landing tables (design §5.3). Sent by a landing node to its splitter
     * only, never to downstream readers. The splitter processes the item
     * under its item id, so the item id is also the file id of every
     * section and message that comes from it.
     *
     * @param fileId     the item's id
     * @param intake     the name of the intake it came in through
     * @param kind       what it is, such as {@code file} or
     *                   {@code message-batch}
     * @param size       its size, in bytes
     * @param itemDigest its keyed digest, as {@code file.completed} reports
     *                   a file's
     * @param landedAt   when its landing was committed
     * @param storage    where it is stored; the date is the day it was
     *                   landed, the table's partition
     */
    record ItemLanded(UUID fileId, String intake, String kind, long size,
            String itemDigest, Instant landedAt, StorageRef storage)
            implements ConnectorMessage
    {

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#ITEM_LANDED}
         */
        @Override
        public MessageType type()
        {
            return MessageType.ITEM_LANDED;
        }

        /**
         * {@inheritDoc}
         *
         * @return "il-" and the item id
         */
        @Override
        public String messageId()
        {
            return "il-" + fileId;
        }
    }

    /**
     * Where a section is stored.
     *
     * @param catalog      the Iceberg catalog's name
     * @param namespace    the table's namespace
     * @param table        the table's name
     * @param ingestedDate the day the section was written, the table's
     *                     partition, so a reader can prune to it
     * @param snapshotId   the Iceberg snapshot the section was committed in
     * @param kekRef       which key encryption key wrapped the section's
     *                     data key
     */
    record StorageRef(
            String catalog,
            String namespace,
            String table,
            java.time.LocalDate ingestedDate,
            long snapshotId,
            String kekRef)
    {
    }
}
