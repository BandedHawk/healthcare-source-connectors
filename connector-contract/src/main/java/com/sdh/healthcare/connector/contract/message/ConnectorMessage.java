/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.message;

import com.sdh.healthcare.connector.contract.position.RecordOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/**
 * A message a connector publishes (design §10.1): a record is in storage,
 * an item was split or converted, or a record's output conflicts with what
 * was stored (STAGES.md §4). A landing node publishes one more, to the
 * split stage only: an item was landed (design §5.3.6). Two
 * go to an intake's producer only, on its results queue (design §5.3.8):
 * an item was received, and what came of it. The payload is always
 * sealed: signed, then encrypted.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public sealed interface ConnectorMessage permits
        ConnectorMessage.RecordInStorage, ConnectorMessage.ItemLanded,
        ConnectorMessage.ItemReceived, ConnectorMessage.ItemResult,
        ConnectorMessage.RecordConflict, ConnectorMessage.ItemEnd,
        ConnectorMessage.ItemProgressed, ConnectorMessage.RecordReprocess,
        ConnectorMessage.ItemReprocess
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
     * What a message ID gains for an item's later completions (STAGES.md
     * §15.6e): nothing for the first, so IDs from before completions were
     * numbered stay the same.
     *
     * @param completion the completion's number
     * @param first      the number that adds nothing: 1 for an
     *                   item-complete message, 0 for {@code item.progress}
     * @return {@code ~c<n>}, or nothing
     */
    static String completionSuffix(int completion, int first)
    {
        return completion <= first ? "" : "~c" + completion;
    }

    /**
     * What an item message's ID gains for a later generation of the item
     * (STAGES.md §15.8.2): nothing for the first, so IDs from before
     * generations were numbered stay the same.
     *
     * @param generation the generation, from 1
     * @return {@code ~g<n>}, or nothing
     */
    static String generationSuffix(int generation)
    {
        return generation <= 1 ? "" : "~g" + generation;
    }

    /**
     * The id of the file the message is about.
     *
     * @return the file id
     */
    UUID itemId();

    /**
     * The key the broker orders and partitions by (STAGES.md §4): the
     * record's id for a message about a record, so an item's records
     * spread over partitions and workers, and the item's id otherwise. A
     * record id is a keyed hash, and is the message id of a record's
     * message already, so it shows nothing more.
     *
     * @return the key
     */
    default String orderingKey()
    {
        return itemId().toString();
    }

    /**
     * A record a stage stored and sends on, readable with
     * connector-queue-reader: as the split stage cut it
     * ({@link RecordSplit}), as the convert stage made it
     * ({@link RecordStored}), or as the transform stage mapped it
     * ({@link RecordTransformed}). A record a stage failed is kept in its
     * fail store and not sent (STAGES.md §15.1).
     */
    sealed interface RecordInStorage extends ConnectorMessage
            permits RecordStored, RecordSplit, RecordTransformed
    {

        /**
         * The record's id.
         *
         * @return the record id
         */
        String recordId();

        /**
         * {@inheritDoc}
         *
         * @return the record's id
         */
        @Override
        default String orderingKey()
        {
            return recordId();
        }

        /**
         * The record's number in its file, from 0.
         *
         * @return the sequence number
         */
        long seq();

        /**
         * Where the record starts in the original, unsplit file.
         *
         * @return the offset, in bytes
         */
        long sourceOffset();

        /**
         * How many bytes of the original file the record covers.
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
         * The stored record's length before compression and encryption.
         *
         * @return the length, in bytes
         */
        long plaintextLength();

        /**
         * How many encrypted chunks the record is stored in.
         *
         * @return the number of chunks
         */
        int chunkCount();

        /**
         * Where the record is stored.
         *
         * @return the storage reference
         */
        StorageRef storage();

        /**
         * Where the record is, for a reader.
         *
         * @return its location
         */
        default RecordLocation location()
        {
            return new RecordLocation(recordId(), itemId(), seq(),
                    plaintextLength(), chunkCount(), storage());
        }

        /**
         * Where the record's text came from in the file, as lines and
         * columns, to turn a position in the record (such as one a mapping
         * reports) into one in the file ({@link RecordOrigin#toFile}).
         *
         * @return the origin; null if the splitter gave none
         */
        RecordOrigin origin();

        /**
         * The version of the stage that made the record (STAGES.md §14):
         * its build, the implementation that made the output, and a digest
         * of the settings that change output.
         *
         * @return the version; null for a record made before versions were
         *         sent
         */
        String version();

        /**
         * Where the record stands in its line of processing (STAGES.md
         * §15.8.2): its item's generation and a revision per stage.
         *
         * @return the lineage; {@link Lineage#FIRST} for the first of
         *         everything
         */
        Lineage lineage();

        /**
         * Whether the stage that sent it already held something newer at
         * the same place when it stored it (STAGES.md §15.8.3): it is sent
         * on for its lineage, and a reader may leave it.
         *
         * @return true if superseded on arrival
         */
        boolean superseded();

        /**
         * The same message with another lineage and superseded flag.
         *
         * @param lineage    the lineage
         * @param superseded whether superseded on arrival
         * @return the message
         */
        RecordInStorage withLineage(Lineage lineage, boolean superseded);

        /**
         * For a record made by an operator's correction or retry, the
         * failure it resolves (STAGES.md §15.6e).
         *
         * @return the failed record's id, or for a correction of an item as
         *         a whole the item's id; null otherwise
         */
        String resolves();

        /**
         * Why the record was made again (STAGES.md §15.8.7).
         *
         * @return the cause; null when first made
         */
        Cause cause();

        /**
         * The same message, made again for a reason: what a stage sends for
         * the output of a reprocess or a correction (STAGES.md §15.8.7).
         *
         * @param cause why it was made again; null when first made
         * @return the message, with the cause
         */
        RecordInStorage withCause(Cause cause);
    }

    /**
     * A stage finished an item: every record of it the stage will make is
     * stored and announced, and none of its failures is open (STAGES.md
     * §5, §15.6). It carries the producer's references on from the stage
     * before, so whichever stage sends the producer's result has them
     * (STAGES.md §17.4).
     */
    sealed interface ItemEnd extends ConnectorMessage
            permits ItemSplit, ItemConverted, ItemTransformed
    {

        /**
         * The intake the item came in through.
         *
         * @return the intake's name
         */
        String intake();

        /**
         * The item's records at this stage, failed ones included.
         *
         * @return the count
         */
        long recordCount();

        /**
         * Those of its records that failed and were declined, or for the
         * split stage the pieces it couldn't parse.
         *
         * @return the count
         */
        long failedRecordCount();

        /**
         * When the stage finished it.
         *
         * @return the time
         */
        Instant completedAt();

        /**
         * The stage's versions the item's records were made under, in the
         * order first used.
         *
         * @return the versions; empty if not known
         */
        List<String> versions();

        /**
         * Which completion of the item at the stage this is, from 1
         * (STAGES.md §15.6e), counted within its generation.
         *
         * @return the completion
         */
        int completion();

        /**
         * The item's generation it is the end of, from 1 (STAGES.md
         * §15.8).
         *
         * @return the generation
         */
        int generation();

        /**
         * The earlier item of the intake with the same content, the
         * earliest still landed (STAGES.md §15.5a).
         *
         * @return its id; null if none
         */
        UUID sameAs();

        /**
         * The producer's own reference for the item, from landing.
         *
         * @return the reference; null if none, or not carried on
         */
        String producerRef();

        /**
         * The item the producer said this one replaces.
         *
         * @return its id; null if none
         */
        UUID replaces();
    }

    /**
     * A record was committed to storage and can be read with
     * connector-queue-reader.
     *
     * @param recordId        the record's id
     * @param itemId          the id of its file
     * @param seq             its number in the file, from 0
     * @param sourceOffset    where it starts in the file, in bytes
     * @param sourceLength    how many bytes of the file it covers
     * @param format          the name of the file's format
     * @param contentType     what the stored bytes are, such as
     *                        {@code application/json}
     * @param mapping         the JSON mapping the record was converted with,
     *                        such as {@code csv-row/1} or {@code ccda/2};
     *                        null when it was stored as split (FHIR, generic
     *                        XML, delimited)
     * @param plaintextLength the stored length before compression and
     *                        encryption
     * @param chunkCount      how many encrypted chunks it is stored in
     * @param storage         where it is stored
     * @param origin          where the record's text came from in the file;
     *                        null if the splitter gave none
     * @param version         the convert stage's version that made it; null
     *                        if not known
     * @param resolves        for a record made by an operator's correction
     *                        or retry, the failed record it resolves
     *                        (STAGES.md §15.6e); null otherwise
     * @param lineage         where it stands in its line (STAGES.md
     *                        §15.8.2); null for the first of everything
     * @param superseded      whether superseded on arrival (§15.8.3)
     * @param cause           why it was made again, for an output of a
     *                        reprocess or correction (§15.8.7); null when
     *                        first made
     */
    record RecordStored(
            String recordId,
            UUID itemId,
            long seq,
            long sourceOffset,
            long sourceLength,
            String format,
            String contentType,
            String mapping,
            long plaintextLength,
            int chunkCount,
            StorageRef storage,
            RecordOrigin origin,
            String version,
            String resolves,
            Lineage lineage,
            boolean superseded,
            Cause cause)
            implements RecordInStorage
    {

        /**
         * Reads no lineage as the first.
         */
        public RecordStored
        {
            lineage = Lineage.orFirst(lineage);
        }

        /**
         * A message first made, with no cause.
         *
         * @param recordId        the record's id
         * @param itemId          the id of its file
         * @param seq             its number in the file, from 0
         * @param sourceOffset    where it starts in the file, in bytes
         * @param sourceLength    how many bytes of the file it covers
         * @param format          the name of the file's format
         * @param contentType     what the stored bytes are
         * @param mapping         the JSON mapping it was converted with; null
         *                        when stored as split
         * @param plaintextLength the stored length before compression and
         *                        encryption
         * @param chunkCount      how many encrypted chunks it is stored in
         * @param storage         where it is stored
         * @param origin          where the record's text came from in the
         *                        file; null if the splitter gave none
         * @param version         the convert stage's version that made it;
         *                        null if not known
         * @param resolves        the failed record a correction or retry
         *                        resolves; null otherwise
         * @param lineage         where it stands in its line; null for the
         *                        first of everything
         * @param superseded      whether superseded on arrival
         */
        public RecordStored(String recordId, UUID itemId, long seq,
                long sourceOffset, long sourceLength, String format,
                String contentType, String mapping, long plaintextLength,
                int chunkCount, StorageRef storage, RecordOrigin origin,
                String version, String resolves, Lineage lineage,
                boolean superseded)
        {
            this(recordId, itemId, seq, sourceOffset, sourceLength, format,
                    contentType, mapping, plaintextLength, chunkCount, storage,
                    origin, version, resolves, lineage, superseded, null);
        }

        /**
         * A message of the first lineage, not superseded.
         *
         * @param recordId        the record's id
         * @param itemId          the id of its file
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
         * @param origin          where the record's text came from
         * @param version         the convert stage's version, or null
         * @param resolves        the failed record it resolves, or null
         */
        public RecordStored(String recordId, UUID itemId, long seq,
                long sourceOffset, long sourceLength, String format,
                String contentType, String mapping, long plaintextLength,
                int chunkCount, StorageRef storage, RecordOrigin origin,
                String version, String resolves)
        {
            this(recordId, itemId, seq, sourceOffset, sourceLength, format,
                    contentType, mapping, plaintextLength, chunkCount, storage,
                    origin, version, resolves, null, false);
        }

        /**
         * A message that resolves no failure.
         *
         * @param recordId        the record's id
         * @param itemId          the id of its file
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
         * @param origin          where the record's text came from
         * @param version         the convert stage's version, or null
         */
        public RecordStored(String recordId, UUID itemId, long seq,
                long sourceOffset, long sourceLength, String format,
                String contentType, String mapping, long plaintextLength,
                int chunkCount, StorageRef storage, RecordOrigin origin,
                String version)
        {
            this(recordId, itemId, seq, sourceOffset, sourceLength, format,
                    contentType, mapping, plaintextLength, chunkCount, storage,
                    origin, version, null);
        }

        /**
         * A message with no version.
         *
         * @param recordId        the record's id
         * @param itemId          the id of its file
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
         * @param origin          where the record's text came from
         */
        public RecordStored(String recordId, UUID itemId, long seq,
                long sourceOffset, long sourceLength, String format,
                String contentType, String mapping, long plaintextLength,
                int chunkCount, StorageRef storage, RecordOrigin origin)
        {
            this(recordId, itemId, seq, sourceOffset, sourceLength, format,
                    contentType, mapping, plaintextLength, chunkCount, storage,
                    origin, null);
        }

        /**
         * A message without an origin, for a record the splitter gave
         * none.
         *
         * @param recordId        the record's id
         * @param itemId          the id of its file
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
        public RecordStored(String recordId, UUID itemId, long seq,
                long sourceOffset, long sourceLength, String format,
                String contentType, String mapping, long plaintextLength,
                int chunkCount, StorageRef storage)
        {
            this(recordId, itemId, seq, sourceOffset, sourceLength, format,
                    contentType, mapping, plaintextLength, chunkCount, storage,
                    null, null);
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#RECORD_STORED}
         */
        @Override
        public MessageType type()
        {
            return MessageType.RECORD_STORED;
        }

        /**
         * {@inheritDoc}
         *
         * @return "rec-", the item id and the record id: the same content
         *         in another item is a record of its own there; then the
         *         lineage's suffix ({@link Lineage#suffix})
         */
        @Override
        public String messageId()
        {
            return "rec-" + itemId + "-" + recordId + lineage.suffix();
        }

        /**
         * {@inheritDoc}
         *
         * @param lineage    {@inheritDoc}
         * @param superseded {@inheritDoc}
         * @return {@inheritDoc}
         */
        @Override
        public RecordStored withLineage(Lineage lineage, boolean superseded)
        {
            return new RecordStored(recordId, itemId, seq, sourceOffset,
                    sourceLength, format, contentType, mapping,
                    plaintextLength, chunkCount, storage, origin, version,
                    resolves, lineage, superseded, cause);
        }

        /**
         * {@inheritDoc}
         *
         * @param cause {@inheritDoc}
         * @return {@inheritDoc}
         */
        @Override
        public RecordStored withCause(Cause cause)
        {
            return new RecordStored(recordId, itemId, seq, sourceOffset,
                    sourceLength, format, contentType, mapping,
                    plaintextLength, chunkCount, storage, origin, version,
                    resolves, lineage, superseded, cause);
        }
    }

    /**
     * The transform stage mapped a record and stored the output in its
     * sink, such as a FHIR bundle in gold (STAGES.md §17.6). Read it with
     * connector-queue-reader, as any record.
     *
     * @param recordId        the output's id
     * @param itemId          the id of its item
     * @param seq             its number in the item, from 0
     * @param sourceOffset    where its source starts in the item, in bytes
     * @param sourceLength    how many bytes of the item it covers
     * @param format          the name of the item's format
     * @param contentType     what the stored bytes are, such as
     *                        {@code application/fhir+json}
     * @param mapping         the WSTL set it was mapped with, with its
     *                        digest
     * @param plaintextLength the stored length before compression and
     *                        encryption
     * @param chunkCount      how many encrypted chunks it is stored in
     * @param storage         where it is stored
     * @param origin          where its source's text came from in the item;
     *                        null if the splitter gave none
     * @param version         the transform stage's version that made it;
     *                        null if not known
     * @param resolves        for an output of an operator's correction or
     *                        retry, the failed record it resolves
     *                        (STAGES.md §15.6e); null otherwise
     * @param lineage         where it stands in its line (STAGES.md
     *                        §15.8.2); null for the first of everything
     * @param superseded      whether superseded on arrival (§15.8.3)
     * @param cause           why it was made again, for an output of a
     *                        reprocess or correction (§15.8.7); null when
     *                        first made
     */
    record RecordTransformed(
            String recordId,
            UUID itemId,
            long seq,
            long sourceOffset,
            long sourceLength,
            String format,
            String contentType,
            String mapping,
            long plaintextLength,
            int chunkCount,
            StorageRef storage,
            RecordOrigin origin,
            String version,
            String resolves,
            Lineage lineage,
            boolean superseded,
            Cause cause)
            implements RecordInStorage
    {

        /**
         * Reads no lineage as the first.
         */
        public RecordTransformed
        {
            lineage = Lineage.orFirst(lineage);
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#RECORD_TRANSFORMED}
         */
        @Override
        public MessageType type()
        {
            return MessageType.RECORD_TRANSFORMED;
        }

        /**
         * {@inheritDoc}
         *
         * @return "tr-", the item id and the record id; then the lineage's
         *         suffix ({@link Lineage#suffix})
         */
        @Override
        public String messageId()
        {
            return "tr-" + itemId + "-" + recordId + lineage.suffix();
        }

        /**
         * {@inheritDoc}
         *
         * @param lineage    {@inheritDoc}
         * @param superseded {@inheritDoc}
         * @return {@inheritDoc}
         */
        @Override
        public RecordTransformed withLineage(Lineage lineage,
                boolean superseded)
        {
            return new RecordTransformed(recordId, itemId, seq, sourceOffset,
                    sourceLength, format, contentType, mapping,
                    plaintextLength, chunkCount, storage, origin, version,
                    resolves, lineage, superseded, cause);
        }

        /**
         * {@inheritDoc}
         *
         * @param cause {@inheritDoc}
         * @return {@inheritDoc}
         */
        @Override
        public RecordTransformed withCause(Cause cause)
        {
            return new RecordTransformed(recordId, itemId, seq, sourceOffset,
                    sourceLength, format, contentType, mapping,
                    plaintextLength, chunkCount, storage, origin, version,
                    resolves, lineage, superseded, cause);
        }
    }

    /**
     * An item was landed: stored exactly as received, encrypted, in the
     * landing tables (design §5.3). Sent by a landing node to its splitter
     * only, never to downstream readers. The splitter processes the item
     * under its item id, so the item id is also the file id of every
     * record and message that comes from it.
     *
     * @param itemId     the item's id
     * @param intake     the name of the intake it came in through
     * @param kind       what it is, such as {@code file} or
     *                   {@code message-batch}
     * @param size       its size, in bytes
     * @param itemDigest its keyed digest, as {@code item.split} reports
     *                   it
     * @param landedAt   when its landing was committed
     * @param storage    where it is stored; the date is the day it was
     *                   landed, the table's partition
     * @param replaces   the earlier item its producer said it replaces, a
     *                   link only: both are kept in full (design §5.3.8);
     *                   null if none, or before contract 2.3
     * @param sameAs     the earlier item of the intake with the same
     *                   content, the earliest still landed, if any
     *                   (STAGES.md §15.5a); null if none
     */
    record ItemLanded(UUID itemId, String intake, String kind, long size,
            String itemDigest, Instant landedAt, StorageRef storage,
            UUID replaces, UUID sameAs)
            implements ConnectorMessage
    {

        /**
         * The message with no {@code sameAs}: an item that is no copy.
         *
         * @param itemId     as in the record, above
         * @param intake     as in the record, above
         * @param kind       as in the record, above
         * @param size       as in the record, above
         * @param itemDigest as in the record, above
         * @param landedAt   as in the record, above
         * @param storage    as in the record, above
         * @param replaces   as in the record, above
         */
        public ItemLanded(UUID itemId, String intake, String kind, long size,
                String itemDigest, Instant landedAt, StorageRef storage,
                UUID replaces)
        {
            this(itemId, intake, kind, size, itemDigest, landedAt, storage,
                    replaces, null);
        }

        /**
         * An item that replaces nothing.
         *
         * @param itemId     the item's id
         * @param intake     the intake it came in through
         * @param kind       what it is
         * @param size       its size, in bytes
         * @param itemDigest its keyed digest
         * @param landedAt   when its landing was committed
         * @param storage    where it is stored
         */
        public ItemLanded(UUID itemId, String intake, String kind, long size,
                String itemDigest, Instant landedAt, StorageRef storage)
        {
            this(itemId, intake, kind, size, itemDigest, landedAt, storage,
                    null);
        }

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
            return "il-" + itemId;
        }
    }

    /**
     * An item was received and landed: the receipt a producer gets on its
     * results queue (design §5.3.8). It is sealed to the producer's key, as
     * it carries the producer's own reference, which can be PHI.
     *
     * @param itemId          the item's id, which every later message about it
     *                        carries
     * @param intake          the intake it came in through
     * @param kind            what it is, such as {@code file} or
     *                        {@code message-batch}
     * @param size            its size, in bytes
     * @param sha256          the SHA-256 of its bytes as received, in
     *                        lower-case hex; null if it was resumed part-way
     *                        after a restart
     * @param landedAt        when its landing was committed
     * @param producerRef     the producer's own reference: the path or key for
     *                        a file or object, the data file's location for a
     *                        table, the {@code X-Correlation-Id} of a push;
     *                        null if there is none
     * @param replaces        the earlier item the producer said this one
     *                        replaces, recorded as a link; null if none
     * @param messageIds      for a message batch, each message's broker ID in
     *                        order (null for a message without one); otherwise
     *                        null
     * @param messageReplaces for a message batch, the earlier message each
     *                        message replaces, as its producer said, as
     * @param sameAs          the earlier item of the intake with the same
     *                        content, the earliest still landed, if any
     *                        (STAGES.md §15.5a); null if none
     *                        {@code <itemId>/<messageId>} (null for one that
     *                        replaces none); null if none does, and for other
     *                        items
     */
    record ItemReceived(UUID itemId, String intake, String kind, long size,
            String sha256, Instant landedAt, String producerRef, UUID replaces,
            List<String> messageIds, List<String> messageReplaces, UUID sameAs)
            implements ConnectorMessage
    {

        /**
         * The message with no {@code sameAs}: an item that is no copy.
         *
         * @param itemId          as in the record, above
         * @param intake          as in the record, above
         * @param kind            as in the record, above
         * @param size            as in the record, above
         * @param sha256          as in the record, above
         * @param landedAt        as in the record, above
         * @param producerRef     as in the record, above
         * @param replaces        as in the record, above
         * @param messageIds      as in the record, above
         * @param messageReplaces as in the record, above
         */
        public ItemReceived(UUID itemId, String intake, String kind, long size,
                String sha256, Instant landedAt, String producerRef,
                UUID replaces, List<String> messageIds,
                List<String> messageReplaces)
        {
            this(itemId, intake, kind, size, sha256, landedAt, producerRef,
                    replaces, messageIds, messageReplaces, null);
        }

        /**
         * Copies the message IDs, which may hold nulls.
         *
         * @param itemId          the item's id
         * @param intake          the intake
         * @param kind            what it is
         * @param size            its size
         * @param sha256          its SHA-256, or null
         * @param landedAt        when it was landed
         * @param producerRef     the producer's reference, or null
         * @param replaces        the item it replaces, or null
         * @param messageIds      the message IDs, or null
         * @param messageReplaces the messages' links, or null
         * @param sameAs          the earlier item of the intake with the same
         *                        content, the earliest still landed, if any
         *                        (STAGES.md §15.5a); null if none
         */
        public ItemReceived
        {
            messageIds = messageIds == null ? null
                    : java.util.Collections.unmodifiableList(
                            new java.util.ArrayList<>(messageIds));
            messageReplaces = messageReplaces == null ? null
                    : java.util.Collections.unmodifiableList(
                            new java.util.ArrayList<>(messageReplaces));
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#ITEM_RECEIVED}
         */
        @Override
        public MessageType type()
        {
            return MessageType.ITEM_RECEIVED;
        }

        /**
         * {@inheritDoc}
         *
         * @return "rc-" and the item id
         */
        @Override
        public String messageId()
        {
            return "rc-" + itemId;
        }
    }

    /**
     * What came of an item: the producer's final result, on its results
     * queue (design §5.3.8). A long list of problems is sent in parts; each
     * part carries everything but its share of the problems, so any one of
     * them says how the item ended.
     *
     * @param itemId            the item's id
     * @param intake            the intake it came in through
     * @param status            {@code completed} (every record stored,
     *                          some perhaps failed), {@code failed} (the
     *                          item couldn't be split) or {@code incomplete}
     *                          (failures still open, STAGES.md §15.6)
     * @param recordCount       the records stored, failed ones included
     * @param failedRecordCount the records that failed conversion
     * @param failure           why the item failed; null if it completed
     * @param problemCount      how many problems there are in all, across
     *                          every part
     * @param part              this part's number, from 1
     * @param partCount         how many parts there are
     * @param problems          this part's problems, in record order
     * @param producerRef       the producer's own reference, as in
     *                          {@link ItemReceived}; null if none
     * @param replaces          the item it replaces; null if none
     * @param finishedAt        when the item was finished
     * @param completion        for a {@link #COMPLETED} result, which
     *                          completion of the item it reports, from 1;
     *                          otherwise how many came before it, so a
     *                          result after an operator reopened a failure
     *                          isn't taken for a repeat (STAGES.md §15.6e)
     * @param sameAs            the earlier item of the intake with the same
     *                          content, the earliest still landed, if any
     *                          (STAGES.md §15.5a); null if none
     */
    record ItemResult(UUID itemId, String intake, String status,
            long recordCount, long failedRecordCount, Problem failure,
            long problemCount, int part, int partCount, List<Problem> problems,
            String producerRef, UUID replaces, Instant finishedAt,
            int completion, UUID sameAs)
            implements ConnectorMessage
    {

        /**
         * The message with no {@code sameAs}: an item that is no copy.
         *
         * @param itemId            as in the record, above
         * @param intake            as in the record, above
         * @param status            as in the record, above
         * @param recordCount       as in the record, above
         * @param failedRecordCount as in the record, above
         * @param failure           as in the record, above
         * @param problemCount      as in the record, above
         * @param part              as in the record, above
         * @param partCount         as in the record, above
         * @param problems          as in the record, above
         * @param producerRef       as in the record, above
         * @param replaces          as in the record, above
         * @param finishedAt        as in the record, above
         * @param completion        as in the record, above
         */
        public ItemResult(UUID itemId, String intake, String status,
                long recordCount, long failedRecordCount, Problem failure,
                long problemCount, int part, int partCount,
                List<Problem> problems, String producerRef, UUID replaces,
                Instant finishedAt, int completion)
        {
            this(itemId, intake, status, recordCount, failedRecordCount,
                    failure, problemCount, part, partCount, problems,
                    producerRef, replaces, finishedAt, completion, null);
        }

        /** The status of an item whose records were all stored. */
        public static final String COMPLETED = "completed";

        /** The status of an item that couldn't be split. */
        public static final String FAILED = "failed";

        /**
         * The status of an item with failures still open: not every record
         * has passed or been declined (STAGES.md §15.6). Another result
         * follows when that changes.
         */
        public static final String INCOMPLETE = "incomplete";

        /**
         * Copies the problems.
         *
         * @param itemId            the item's id
         * @param intake            the intake
         * @param status            completed or failed
         * @param recordCount       the records stored
         * @param failedRecordCount the records that failed
         * @param failure           why it failed, or null
         * @param problemCount      the problems in all
         * @param part              this part, from 1
         * @param partCount         the parts
         * @param problems          this part's problems
         * @param producerRef       the producer's reference, or null
         * @param replaces          the item it replaces, or null
         * @param finishedAt        when it was finished
         * @param completion        its completion's number, or those
         *                          before it
         * @param sameAs            the earlier item of the intake with the same
         *                          content, the earliest still landed, if any
         *                          (STAGES.md §15.5a); null if none
         */
        public ItemResult
        {
            problems = List.copyOf(problems);
            completion = Math.max(COMPLETED.equals(status) ? 1 : 0,
                    completion);
        }

        /**
         * A result of an item's first completion, or before it.
         *
         * @param itemId            the item's id
         * @param intake            the intake
         * @param status            completed, incomplete or failed
         * @param recordCount       the records stored
         * @param failedRecordCount the records that failed
         * @param failure           why it failed, or null
         * @param problemCount      the problems in all
         * @param part              this part, from 1
         * @param partCount         the parts
         * @param problems          this part's problems
         * @param producerRef       the producer's reference, or null
         * @param replaces          the item it replaces, or null
         * @param finishedAt        when it was finished
         */
        public ItemResult(UUID itemId, String intake, String status,
                long recordCount, long failedRecordCount, Problem failure,
                long problemCount, int part, int partCount,
                List<Problem> problems, String producerRef, UUID replaces,
                Instant finishedAt)
        {
            this(itemId, intake, status, recordCount, failedRecordCount,
                    failure, problemCount, part, partCount, problems,
                    producerRef, replaces, finishedAt, 0);
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#ITEM_RESULT}
         */
        @Override
        public MessageType type()
        {
            return MessageType.ITEM_RESULT;
        }

        /**
         * {@inheritDoc}
         *
         * @return "rs-", the item id, "-" and the part; for an
         *         {@link #INCOMPLETE} result, with its counts before the
         *         part, so a result whose counts changed isn't taken for a
         *         repeat of the one before
         */
        @Override
        public String messageId()
        {
            if (COMPLETED.equals(status))
            {
                return "rs-" + itemId + "-" + part
                        + ConnectorMessage.completionSuffix(completion, 1);
            }
            if (INCOMPLETE.equals(status))
            {
                return "rs-" + itemId + "-incomplete-" + recordCount + "-"
                        + failedRecordCount + "-" + problemCount + "-" + part
                        + ConnectorMessage.completionSuffix(completion, 0);
            }
            return "rs-" + itemId + "-" + part;
        }
    }

    /**
     * A stage had already stored a record, and has now produced different
     * output for it: the converter or the mapping changed between the two
     * deliveries (STAGES.md §6). The first output is kept; this tells the
     * stage's dead-letter queue about the second, which is not stored. It
     * holds no content, only digests.
     *
     * @param recordId     the record's id
     * @param itemId       the id of the item it came from
     * @param stage        the stage, such as {@code convert}
     * @param storedDigest the digest of the output stored first
     * @param newDigest    the digest of the output now produced
     * @param foundAt      when the conflict was found
     */
    record RecordConflict(String recordId, UUID itemId, String stage,
            String storedDigest, String newDigest, Instant foundAt)
            implements ConnectorMessage
    {

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#RECORD_CONFLICT}
         */
        @Override
        public MessageType type()
        {
            return MessageType.RECORD_CONFLICT;
        }

        /**
         * {@inheritDoc}
         *
         * @return "rc-" and a SHA-256 of the stage, the item id, the record
         *         id and the new digest, so each different output is
         *         reported once, in an id short enough for every broker
         */
        @Override
        public String messageId()
        {
            try
            {
                MessageDigest sha = MessageDigest.getInstance("SHA-256");
                byte[] hash = sha.digest(String.join("\n", stage,
                        itemId.toString(), recordId, newDigest)
                        .getBytes(StandardCharsets.UTF_8));
                return "rc-" + HexFormat.of().formatHex(hash);
            }
            catch (NoSuchAlgorithmException missing)
            {
                throw new IllegalStateException(missing);
            }
        }

        /**
         * {@inheritDoc}
         *
         * @return the record's id
         */
        @Override
        public String orderingKey()
        {
            return recordId;
        }
    }

    /**
     * The split stage stored a record as it was cut from its item, in the
     * split records table, for the convert stage (STAGES.md §4). A piece the
     * splitter couldn't parse is stored too, as it was, with the error that
     * stopped it.
     *
     * @param recordId        the record's id
     * @param itemId          the id of its item
     * @param seq             its number in the item, from 0
     * @param sourceOffset    where it starts in the item, in bytes
     * @param sourceLength    how many bytes of the item it covers
     * @param format          the name of the item's format
     * @param contentType     what the stored bytes are
     * @param plaintextLength the stored length before compression and
     *                        encryption
     * @param chunkCount      how many encrypted chunks it is stored in
     * @param storage         where it is stored
     * @param origin          where its text came from in the item
     * @param errorCode       why the splitter couldn't parse it; null if it
     *                        could
     * @param errorAt         where in the item that was; null if unknown or
     *                        none
     * @param message         for an item that is a batch of queue messages,
     *                        the number of the message the record starts
     *                        in, from 0; -1 otherwise
     * @param messageRef      that message's broker ID; null if not a batch
     *                        or not kept
     * @param messageReplaces the earlier message it replaces, as its
     *                        producer said; null if none
     * @param version         the split stage's version that cut it; null if
     *                        not known
     * @param resolves        for a record made by an operator's correction
     *                        or retry, the failed piece it resolves, or for
     *                        a correction of the item as a whole the item's
     *                        id (STAGES.md §15.6e); null otherwise
     * @param lineage         where it stands in its line (STAGES.md
     *                        §15.8.2); null for the first of everything
     * @param superseded      whether superseded on arrival (§15.8.3)
     * @param cause           why it was cut again, for a generation made by
     *                        a reprocess or an operator's correction
     *                        (STAGES.md §15.8.7); null for a first cut
     */
    record RecordSplit(
            String recordId,
            UUID itemId,
            long seq,
            long sourceOffset,
            long sourceLength,
            String format,
            String contentType,
            long plaintextLength,
            int chunkCount,
            StorageRef storage,
            RecordOrigin origin,
            String errorCode,
            SourcePosition errorAt,
            int message,
            String messageRef,
            String messageReplaces,
            String version,
            String resolves,
            Lineage lineage,
            boolean superseded,
            Cause cause)
            implements RecordInStorage
    {

        /**
         * Reads no lineage as the first.
         */
        public RecordSplit
        {
            lineage = Lineage.orFirst(lineage);
        }

        /**
         * A message of a first cut.
         *
         * @param recordId        the record's id
         * @param itemId          the id of its item
         * @param seq             its number in the item, from 0
         * @param sourceOffset    where it starts in the item, in bytes
         * @param sourceLength    how many bytes of the item it covers
         * @param format          the name of the item's format
         * @param contentType     what the stored bytes are
         * @param plaintextLength the stored length before compression and
         *                        encryption
         * @param chunkCount      how many encrypted chunks it is stored in
         * @param storage         where it is stored
         * @param origin          where its text came from in the item
         * @param errorCode       why the splitter couldn't parse it, or null
         * @param errorAt         where in the item that was, or null
         * @param message         its message in a batch, or -1
         * @param messageRef      that message's broker ID, or null
         * @param messageReplaces the message it replaces, or null
         * @param version         the split stage's version, or null
         * @param resolves        the failure it resolves, or null
         * @param lineage         where it stands in its line, or null
         * @param superseded      whether superseded on arrival
         */
        public RecordSplit(String recordId, UUID itemId, long seq,
                long sourceOffset, long sourceLength, String format,
                String contentType, long plaintextLength, int chunkCount,
                StorageRef storage, RecordOrigin origin, String errorCode,
                SourcePosition errorAt, int message, String messageRef,
                String messageReplaces, String version, String resolves,
                Lineage lineage, boolean superseded)
        {
            this(recordId, itemId, seq, sourceOffset, sourceLength, format,
                    contentType, plaintextLength, chunkCount, storage, origin,
                    errorCode, errorAt, message, messageRef, messageReplaces,
                    version, resolves, lineage, superseded, null);
        }

        /**
         * A message of the first lineage, not superseded.
         *
         * @param recordId        the record's id
         * @param itemId          the id of its item
         * @param seq             its number in the item, from 0
         * @param sourceOffset    where it starts in the item, in bytes
         * @param sourceLength    how many bytes of the item it covers
         * @param format          the name of the item's format
         * @param contentType     what the stored bytes are
         * @param plaintextLength the stored length before compression and
         *                        encryption
         * @param chunkCount      how many encrypted chunks it is stored in
         * @param storage         where it is stored
         * @param origin          where its text came from in the item
         * @param errorCode       why the splitter couldn't parse it, or null
         * @param errorAt         where in the item that was, or null
         * @param message         its message in a batch, or -1
         * @param messageRef      that message's broker ID, or null
         * @param messageReplaces the message it replaces, or null
         * @param version         the split stage's version, or null
         * @param resolves        the failure it resolves, or null
         */
        public RecordSplit(String recordId, UUID itemId, long seq,
                long sourceOffset, long sourceLength, String format,
                String contentType, long plaintextLength, int chunkCount,
                StorageRef storage, RecordOrigin origin, String errorCode,
                SourcePosition errorAt, int message, String messageRef,
                String messageReplaces, String version, String resolves)
        {
            this(recordId, itemId, seq, sourceOffset, sourceLength, format,
                    contentType, plaintextLength, chunkCount, storage, origin,
                    errorCode, errorAt, message, messageRef, messageReplaces,
                    version, resolves, null, false);
        }

        /**
         * A message that resolves no failure.
         *
         * @param recordId        the record's id
         * @param itemId          the id of its item
         * @param seq             its number in the item, from 0
         * @param sourceOffset    where it starts in the item, in bytes
         * @param sourceLength    how many bytes of the item it covers
         * @param format          the name of the item's format
         * @param contentType     what the stored bytes are
         * @param plaintextLength the stored length before compression and
         *                        encryption
         * @param chunkCount      how many encrypted chunks it is stored in
         * @param storage         where it is stored
         * @param origin          where its text came from in the item
         * @param errorCode       why the splitter couldn't parse it, or null
         * @param errorAt         where in the item that was, or null
         * @param message         its message in a batch, or -1
         * @param messageRef      that message's broker ID, or null
         * @param messageReplaces the message it replaces, or null
         * @param version         the split stage's version, or null
         */
        public RecordSplit(String recordId, UUID itemId, long seq,
                long sourceOffset, long sourceLength, String format,
                String contentType, long plaintextLength, int chunkCount,
                StorageRef storage, RecordOrigin origin, String errorCode,
                SourcePosition errorAt, int message, String messageRef,
                String messageReplaces, String version)
        {
            this(recordId, itemId, seq, sourceOffset, sourceLength, format,
                    contentType, plaintextLength, chunkCount, storage, origin,
                    errorCode, errorAt, message, messageRef, messageReplaces,
                    version, null);
        }

        /**
         * A message with no version.
         *
         * @param recordId        the record's id
         * @param itemId          the id of its item
         * @param seq             its number in the item, from 0
         * @param sourceOffset    where it starts in the item, in bytes
         * @param sourceLength    how many bytes of the item it covers
         * @param format          the name of the item's format
         * @param contentType     what the stored bytes are
         * @param plaintextLength the stored length before compression and
         *                        encryption
         * @param chunkCount      how many encrypted chunks it is stored in
         * @param storage         where it is stored
         * @param origin          where its text came from in the item
         * @param errorCode       why the splitter couldn't parse it, or null
         * @param errorAt         where in the item that was, or null
         * @param message         its message in a batch, or -1
         * @param messageRef      that message's broker ID, or null
         * @param messageReplaces the message it replaces, or null
         */
        public RecordSplit(String recordId, UUID itemId, long seq,
                long sourceOffset, long sourceLength, String format,
                String contentType, long plaintextLength, int chunkCount,
                StorageRef storage, RecordOrigin origin, String errorCode,
                SourcePosition errorAt, int message, String messageRef,
                String messageReplaces)
        {
            this(recordId, itemId, seq, sourceOffset, sourceLength, format,
                    contentType, plaintextLength, chunkCount, storage, origin,
                    errorCode, errorAt, message, messageRef, messageReplaces,
                    null);
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#RECORD_SPLIT}
         */
        @Override
        public MessageType type()
        {
            return MessageType.RECORD_SPLIT;
        }

        /**
         * {@inheritDoc}
         *
         * @return "sp-", the item id and the record id: the same content
         *         in another item is a record of its own there; then the
         *         lineage's suffix ({@link Lineage#suffix})
         */
        @Override
        public String messageId()
        {
            return "sp-" + itemId + "-" + recordId + lineage.suffix();
        }

        /**
         * {@inheritDoc}
         *
         * @param lineage    {@inheritDoc}
         * @param superseded {@inheritDoc}
         * @return {@inheritDoc}
         */
        @Override
        public RecordSplit withLineage(Lineage lineage, boolean superseded)
        {
            return new RecordSplit(recordId, itemId, seq, sourceOffset,
                    sourceLength, format, contentType, plaintextLength,
                    chunkCount, storage, origin, errorCode, errorAt, message,
                    messageRef, messageReplaces, version, resolves, lineage,
                    superseded, cause);
        }

        /**
         * {@inheritDoc}
         *
         * @param why {@inheritDoc}
         * @return {@inheritDoc}
         */
        @Override
        public RecordSplit withCause(Cause why)
        {
            return new RecordSplit(recordId, itemId, seq, sourceOffset,
                    sourceLength, format, contentType, plaintextLength,
                    chunkCount, storage, origin, errorCode, errorAt, message,
                    messageRef, messageReplaces, version, resolves, lineage,
                    superseded, why);
        }
    }

    /**
     * The split stage finished an item: every record it was cut into is
     * stored and announced (STAGES.md §5).
     *
     * @param itemId            the item's id
     * @param intake            the intake it came in through
     * @param recordCount       its records, failed ones included
     * @param failedRecordCount the pieces the splitter couldn't parse,
     *                          stored as they were
     * @param itemSize          the item's size, in bytes
     * @param itemDigest        its keyed digest, as landed
     * @param producerRef       the producer's own reference for it, from
     *                          landing; null if none
     * @param replaces          the item it replaces; null if none
     * @param failure           why the item as a whole couldn't be split
     *                          further, with the records before it kept,
     *                          placed in its message for a batch; null if
     *                          it was split to the end
     * @param startedAt         when the split stage started on it
     * @param completedAt       when it finished
     * @param versions          the split stage's versions its records were
     *                          cut under, in the order first used; empty if
     *                          not known
     * @param completion        which completion of the item at the split
     *                          stage this is, from 1: a later one follows
     *                          an operator's reopening it (STAGES.md
     *                          §15.6e), counted within its generation
     * @param generation        the item's generation it is the end of, from
     *                          1 (STAGES.md §15.8)
     * @param interrupted       whether the generation was stopped part way
     *                          for a newer one (STAGES.md §15.8.6): its
     *                          records so far, a total still open
     * @param sameAs            the earlier item of the intake with the same
     *                          content, the earliest still landed, if any
     *                          (STAGES.md §15.5a); null if none
     */
    record ItemSplit(UUID itemId, String intake, long recordCount,
            long failedRecordCount, long itemSize, String itemDigest,
            String producerRef, UUID replaces, Problem failure,
            Instant startedAt, Instant completedAt, List<String> versions,
            int completion, int generation, boolean interrupted, UUID sameAs)
            implements ItemEnd
    {

        /**
         * The message with no {@code sameAs}: an item that is no copy.
         *
         * @param itemId            as in the record, above
         * @param intake            as in the record, above
         * @param recordCount       as in the record, above
         * @param failedRecordCount as in the record, above
         * @param itemSize          as in the record, above
         * @param itemDigest        as in the record, above
         * @param producerRef       as in the record, above
         * @param replaces          as in the record, above
         * @param failure           as in the record, above
         * @param startedAt         as in the record, above
         * @param completedAt       as in the record, above
         * @param versions          as in the record, above
         * @param completion        as in the record, above
         * @param generation        as in the record, above
         * @param interrupted       as in the record, above
         */
        public ItemSplit(UUID itemId, String intake, long recordCount,
                long failedRecordCount, long itemSize, String itemDigest,
                String producerRef, UUID replaces, Problem failure,
                Instant startedAt, Instant completedAt, List<String> versions,
                int completion, int generation, boolean interrupted)
        {
            this(itemId, intake, recordCount, failedRecordCount, itemSize,
                    itemDigest, producerRef, replaces, failure, startedAt,
                    completedAt, versions, completion, generation, interrupted,
                    null);
        }

        /**
         * Copies the versions; a completion or generation not given is the
         * first.
         */
        public ItemSplit
        {
            versions = versions == null ? List.of() : List.copyOf(versions);
            completion = Math.max(1, completion);
            generation = Math.max(1, generation);
        }

        /**
         * The end of a generation split to its end, or failed.
         *
         * @param itemId            the item's id
         * @param intake            the intake it came in through
         * @param recordCount       its records, failed ones included
         * @param failedRecordCount the pieces the splitter couldn't parse
         * @param itemSize          the item's size, in bytes
         * @param itemDigest        its keyed digest, as landed
         * @param producerRef       the producer's reference, or null
         * @param replaces          the item it replaces, or null
         * @param failure           why it couldn't be split further, or null
         * @param startedAt         when the split stage started on it
         * @param completedAt       when it finished
         * @param versions          the versions, in the order first used
         * @param completion        which completion this is, from 1
         * @param generation        the generation, from 1
         */
        public ItemSplit(UUID itemId, String intake, long recordCount,
                long failedRecordCount, long itemSize, String itemDigest,
                String producerRef, UUID replaces, Problem failure,
                Instant startedAt, Instant completedAt, List<String> versions,
                int completion, int generation)
        {
            this(itemId, intake, recordCount, failedRecordCount, itemSize,
                    itemDigest, producerRef, replaces, failure, startedAt,
                    completedAt, versions, completion, generation, false);
        }

        /**
         * The end of the item's first generation.
         *
         * @param itemId            the item's id
         * @param intake            the intake it came in through
         * @param recordCount       its records, failed ones included
         * @param failedRecordCount the pieces the splitter couldn't parse
         * @param itemSize          the item's size, in bytes
         * @param itemDigest        its keyed digest, as landed
         * @param producerRef       the producer's reference, or null
         * @param replaces          the item it replaces, or null
         * @param failure           why it couldn't be split further, or null
         * @param startedAt         when the split stage started on it
         * @param completedAt       when it finished
         * @param versions          the versions, in the order first used
         * @param completion        which completion this is, from 1
         */
        public ItemSplit(UUID itemId, String intake, long recordCount,
                long failedRecordCount, long itemSize, String itemDigest,
                String producerRef, UUID replaces, Problem failure,
                Instant startedAt, Instant completedAt, List<String> versions,
                int completion)
        {
            this(itemId, intake, recordCount, failedRecordCount, itemSize,
                    itemDigest, producerRef, replaces, failure, startedAt,
                    completedAt, versions, completion, 1);
        }

        /**
         * The item's first completion.
         *
         * @param itemId            the item's id
         * @param intake            the intake it came in through
         * @param recordCount       its records, failed ones included
         * @param failedRecordCount the pieces the splitter couldn't parse
         * @param itemSize          the item's size, in bytes
         * @param itemDigest        its keyed digest, as landed
         * @param producerRef       the producer's reference, or null
         * @param replaces          the item it replaces, or null
         * @param failure           why it couldn't be split further, or null
         * @param startedAt         when the split stage started on it
         * @param completedAt       when it finished
         * @param versions          the versions, in the order first used
         */
        public ItemSplit(UUID itemId, String intake, long recordCount,
                long failedRecordCount, long itemSize, String itemDigest,
                String producerRef, UUID replaces, Problem failure,
                Instant startedAt, Instant completedAt, List<String> versions)
        {
            this(itemId, intake, recordCount, failedRecordCount, itemSize,
                    itemDigest, producerRef, replaces, failure, startedAt,
                    completedAt, versions, 1);
        }

        /**
         * A message with no versions.
         *
         * @param itemId            the item's id
         * @param intake            the intake it came in through
         * @param recordCount       its records, failed ones included
         * @param failedRecordCount the pieces the splitter couldn't parse
         * @param itemSize          the item's size, in bytes
         * @param itemDigest        its keyed digest, as landed
         * @param producerRef       the producer's reference, or null
         * @param replaces          the item it replaces, or null
         * @param failure           why it couldn't be split further, or null
         * @param startedAt         when the split stage started on it
         * @param completedAt       when it finished
         */
        public ItemSplit(UUID itemId, String intake, long recordCount,
                long failedRecordCount, long itemSize, String itemDigest,
                String producerRef, UUID replaces, Problem failure,
                Instant startedAt, Instant completedAt)
        {
            this(itemId, intake, recordCount, failedRecordCount, itemSize,
                    itemDigest, producerRef, replaces, failure, startedAt,
                    completedAt, List.of());
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#ITEM_SPLIT}
         */
        @Override
        public MessageType type()
        {
            return MessageType.ITEM_SPLIT;
        }

        /**
         * {@inheritDoc}
         *
         * @return "is-" and the item id; from its second generation
         *         {@code ~g} and the generation's number; and from its
         *         second completion {@code ~c} and the completion's number
         */
        @Override
        public String messageId()
        {
            return "is-" + itemId + ConnectorMessage.generationSuffix(
                    generation) + ConnectorMessage.completionSuffix(
                    completion, 1);
        }
    }

    /**
     * The convert stage stored every record of an item (STAGES.md §5).
     *
     * @param itemId            the item's id
     * @param intake            the intake it came in through
     * @param recordCount       its records, failed ones included
     * @param failedRecordCount those stored in their original form, after
     *                          failing to split or to convert
     * @param completedAt       when the last was stored
     * @param versions          the convert stage's versions its records
     *                          were made under, in the order first used;
     *                          empty if not known
     * @param completion        which completion of the item at the convert
     *                          stage this is, from 1 (STAGES.md §15.6e),
     *                          counted within its generation
     * @param generation        the item's generation it is the end of, from
     *                          1 (STAGES.md §15.8)
     * @param sameAs            the earlier item of the intake with the same
     *                          content, the earliest still landed, if any
     *                          (STAGES.md §15.5a); null if none
     * @param producerRef       the producer's own reference for it, carried
     *                          on from the item's end at the split stage
     *                          (STAGES.md §17.4); null if none
     * @param replaces          the item it replaces, carried on the same
     *                          way; null if none
     */
    record ItemConverted(UUID itemId, String intake, long recordCount,
            long failedRecordCount, Instant completedAt, List<String> versions,
            int completion, int generation, UUID sameAs, String producerRef,
            UUID replaces)
            implements ItemEnd
    {

        /**
         * The message without the producer's references.
         *
         * @param itemId            as in the record, above
         * @param intake            as in the record, above
         * @param recordCount       as in the record, above
         * @param failedRecordCount as in the record, above
         * @param completedAt       as in the record, above
         * @param versions          as in the record, above
         * @param completion        as in the record, above
         * @param generation        as in the record, above
         * @param sameAs            as in the record, above
         */
        public ItemConverted(UUID itemId, String intake, long recordCount,
                long failedRecordCount, Instant completedAt,
                List<String> versions, int completion, int generation,
                UUID sameAs)
        {
            this(itemId, intake, recordCount, failedRecordCount, completedAt,
                    versions, completion, generation, sameAs, null, null);
        }

        /**
         * The message with no {@code sameAs}: an item that is no copy.
         *
         * @param itemId            as in the record, above
         * @param intake            as in the record, above
         * @param recordCount       as in the record, above
         * @param failedRecordCount as in the record, above
         * @param completedAt       as in the record, above
         * @param versions          as in the record, above
         * @param completion        as in the record, above
         * @param generation        as in the record, above
         */
        public ItemConverted(UUID itemId, String intake, long recordCount,
                long failedRecordCount, Instant completedAt,
                List<String> versions, int completion, int generation)
        {
            this(itemId, intake, recordCount, failedRecordCount, completedAt,
                    versions, completion, generation, null);
        }

        /**
         * Copies the versions; a completion or generation not given is the
         * first.
         */
        public ItemConverted
        {
            versions = versions == null ? List.of() : List.copyOf(versions);
            completion = Math.max(1, completion);
            generation = Math.max(1, generation);
        }

        /**
         * The end of the item's first generation.
         *
         * @param itemId            the item's id
         * @param intake            the intake it came in through
         * @param recordCount       its records, failed ones included
         * @param failedRecordCount those stored in their original form
         * @param completedAt       when the last was stored
         * @param versions          the versions, in the order first used
         * @param completion        which completion this is, from 1
         */
        public ItemConverted(UUID itemId, String intake, long recordCount,
                long failedRecordCount, Instant completedAt,
                List<String> versions, int completion)
        {
            this(itemId, intake, recordCount, failedRecordCount, completedAt,
                    versions, completion, 1);
        }

        /**
         * The item's first completion.
         *
         * @param itemId            the item's id
         * @param intake            the intake it came in through
         * @param recordCount       its records, failed ones included
         * @param failedRecordCount those stored in their original form
         * @param completedAt       when the last was stored
         * @param versions          the versions, in the order first used
         */
        public ItemConverted(UUID itemId, String intake, long recordCount,
                long failedRecordCount, Instant completedAt,
                List<String> versions)
        {
            this(itemId, intake, recordCount, failedRecordCount, completedAt,
                    versions, 1);
        }

        /**
         * A message with no versions.
         *
         * @param itemId            the item's id
         * @param intake            the intake it came in through
         * @param recordCount       its records, failed ones included
         * @param failedRecordCount those stored in their original form
         * @param completedAt       when the last was stored
         */
        public ItemConverted(UUID itemId, String intake, long recordCount,
                long failedRecordCount, Instant completedAt)
        {
            this(itemId, intake, recordCount, failedRecordCount, completedAt,
                    List.of());
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#ITEM_CONVERTED}
         */
        @Override
        public MessageType type()
        {
            return MessageType.ITEM_CONVERTED;
        }

        /**
         * {@inheritDoc}
         *
         * @return "ic-convert-" and the item id; from its second
         *         generation {@code ~g} and its number; and from its second
         *         completion {@code ~c} and the completion's number
         */
        @Override
        public String messageId()
        {
            return "ic-convert-" + itemId + ConnectorMessage.generationSuffix(
                    generation) + ConnectorMessage.completionSuffix(
                    completion, 1);
        }
    }

    /**
     * The transform stage stored every record of an item (STAGES.md §5,
     * §17).
     *
     * @param itemId            the item's id
     * @param intake            the intake it came in through
     * @param recordCount       its records, failed ones included
     * @param failedRecordCount those that failed and were declined
     * @param completedAt       when the last was stored
     * @param versions          the transform stage's versions its records
     *                          were made under, in the order first used;
     *                          empty if not known
     * @param completion        which completion of the item at the
     *                          transform stage this is, from 1 (STAGES.md
     *                          §15.6e), counted within its generation
     * @param generation        the item's generation it is the end of, from
     *                          1 (STAGES.md §15.8)
     * @param sameAs            the earlier item of the intake with the same
     *                          content, the earliest still landed, if any
     *                          (STAGES.md §15.5a); null if none
     * @param producerRef       the producer's own reference for it, carried
     *                          on from {@code item.converted}; null if none
     * @param replaces          the item it replaces; null if none
     */
    record ItemTransformed(UUID itemId, String intake, long recordCount,
            long failedRecordCount, Instant completedAt, List<String> versions,
            int completion, int generation, UUID sameAs, String producerRef,
            UUID replaces)
            implements ItemEnd
    {

        /**
         * Copies the versions; a completion or generation not given is the
         * first.
         */
        public ItemTransformed
        {
            versions = versions == null ? List.of() : List.copyOf(versions);
            completion = Math.max(1, completion);
            generation = Math.max(1, generation);
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#ITEM_TRANSFORMED}
         */
        @Override
        public MessageType type()
        {
            return MessageType.ITEM_TRANSFORMED;
        }

        /**
         * {@inheritDoc}
         *
         * @return "ic-transform-" and the item id; from its second
         *         generation {@code ~g} and its number; and from its second
         *         completion {@code ~c} and the completion's number
         */
        @Override
        public String messageId()
        {
            return "ic-transform-" + itemId
                    + ConnectorMessage.generationSuffix(generation)
                    + ConnectorMessage.completionSuffix(completion, 1);
        }
    }

    /**
     * A stage has handled every record of an item once, and some failed and
     * are open: neither passed nor declined (STAGES.md §15.6). It is sent in
     * place of the item-complete message ({@code item.split},
     * {@code item.converted}, {@code item.transformed}), and again whenever
     * its counts change, until nothing is open; failures themselves are not
     * sent downstream (§15.1).
     * A later stage takes it as a total still to grow: it handles the
     * records passed so far, and completes only on the item-complete
     * message.
     *
     * @param itemId        the item's id
     * @param intake        the intake it came in through
     * @param stage         the stage reporting, such as {@code split}
     * @param recordCount   the records the stage has handled: passed,
     *                      failed and declined
     * @param passedCount   those that passed, and were sent on
     * @param openCount     those that failed and are open
     * @param declinedCount those an operator declined to correct
     * @param failure       why the item as a whole couldn't be split
     *                      further, as in {@link ItemSplit}; null if none
     * @param producerRef   the producer's own reference for it; null if
     *                      none
     * @param replaces      the item it replaces; null if none
     * @param reportedAt    when the stage reported it
     * @param versions      the stage's versions its records were made under,
     *                      in the order first used; empty if not known
     * @param completion    how many times the stage has completed the item
     *                      before this: 0 until its first completion; from
     *                      1, it reopens that completion, after an operator
     *                      reopened a failure (STAGES.md §15.6e), and a
     *                      stage after that took it reopens its own;
     *                      counted within its generation
     * @param generation    the item's generation it reports on, from 1
     *                      (STAGES.md §15.8)
     * @param sameAs        the earlier item of the intake with the same
     *                      content, the earliest still landed, if any
     *                      (STAGES.md §15.5a); null if none
     */
    record ItemProgressed(UUID itemId, String intake, String stage,
            long recordCount, long passedCount, long openCount,
            long declinedCount, Problem failure, String producerRef,
            UUID replaces, Instant reportedAt, List<String> versions,
            int completion, int generation, UUID sameAs)
            implements ConnectorMessage
    {

        /**
         * The message with no {@code sameAs}: an item that is no copy.
         *
         * @param itemId        as in the record, above
         * @param intake        as in the record, above
         * @param stage         as in the record, above
         * @param recordCount   as in the record, above
         * @param passedCount   as in the record, above
         * @param openCount     as in the record, above
         * @param declinedCount as in the record, above
         * @param failure       as in the record, above
         * @param producerRef   as in the record, above
         * @param replaces      as in the record, above
         * @param reportedAt    as in the record, above
         * @param versions      as in the record, above
         * @param completion    as in the record, above
         * @param generation    as in the record, above
         */
        public ItemProgressed(UUID itemId, String intake, String stage,
                long recordCount, long passedCount, long openCount,
                long declinedCount, Problem failure, String producerRef,
                UUID replaces, Instant reportedAt, List<String> versions,
                int completion, int generation)
        {
            this(itemId, intake, stage, recordCount, passedCount, openCount,
                    declinedCount, failure, producerRef, replaces, reportedAt,
                    versions, completion, generation, null);
        }

        /**
         * Copies the versions; a generation not given is the first.
         */
        public ItemProgressed
        {
            versions = versions == null ? List.of() : List.copyOf(versions);
            completion = Math.max(0, completion);
            generation = Math.max(1, generation);
        }

        /**
         * Progress of the item's first generation.
         *
         * @param itemId        the item's id
         * @param intake        the intake it came in through
         * @param stage         the stage reporting
         * @param recordCount   the records the stage has handled
         * @param passedCount   those that passed
         * @param openCount     those that failed and are open
         * @param declinedCount those declined
         * @param failure       why the item as a whole couldn't be split
         *                      further, or null
         * @param producerRef   the producer's reference, or null
         * @param replaces      the item it replaces, or null
         * @param reportedAt    when the stage reported it
         * @param versions      the versions, in the order first used
         * @param completion    the completions before it
         */
        public ItemProgressed(UUID itemId, String intake, String stage,
                long recordCount, long passedCount, long openCount,
                long declinedCount, Problem failure, String producerRef,
                UUID replaces, Instant reportedAt, List<String> versions,
                int completion)
        {
            this(itemId, intake, stage, recordCount, passedCount, openCount,
                    declinedCount, failure, producerRef, replaces,
                    reportedAt, versions, completion, 1);
        }

        /**
         * Progress before the item's first completion.
         *
         * @param itemId        the item's id
         * @param intake        the intake it came in through
         * @param stage         the stage reporting
         * @param recordCount   the records the stage has handled
         * @param passedCount   those that passed
         * @param openCount     those that failed and are open
         * @param declinedCount those declined
         * @param failure       why the item as a whole couldn't be split
         *                      further, or null
         * @param producerRef   the producer's reference, or null
         * @param replaces      the item it replaces, or null
         * @param reportedAt    when the stage reported it
         * @param versions      the versions, in the order first used
         */
        public ItemProgressed(UUID itemId, String intake, String stage,
                long recordCount, long passedCount, long openCount,
                long declinedCount, Problem failure, String producerRef,
                UUID replaces, Instant reportedAt, List<String> versions)
        {
            this(itemId, intake, stage, recordCount, passedCount, openCount,
                    declinedCount, failure, producerRef, replaces,
                    reportedAt, versions, 0);
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#ITEM_PROGRESS}
         */
        @Override
        public MessageType type()
        {
            return MessageType.ITEM_PROGRESS;
        }

        /**
         * {@inheritDoc}
         *
         * @return "ip-", the stage, the item id and the counts: the same
         *         counts are the same message, and a change is a new one;
         *         from its second generation, {@code ~g} and its number;
         *         after a completion, {@code ~c} and its number, so the
         *         counts before it and after a reopening differ
         */
        @Override
        public String messageId()
        {
            return "ip-" + stage + "-" + itemId + "-" + recordCount + "-"
                    + passedCount + "-" + openCount + "-" + declinedCount
                    + ConnectorMessage.generationSuffix(generation)
                    + ConnectorMessage.completionSuffix(completion, 0);
        }
    }

    /**
     * A problem found in an item, with where it is, so the producer can
     * find it in their own data. It holds no content.
     *
     * @param code            the error code, such as
     *                        {@code CONVERT_CSV_FIELD_COUNT}
     * @param stage           where it was found: {@code split} or
     *                        {@code convert} for the splitter, {@code landing}
     *                        if the item couldn't be read
     * @param seq             the record's number in the item, from 0; -1 for a
     *                        problem with the item as a whole
     * @param offset          the byte offset in the item where it is
     * @param at              the line and column in the item; null if unknown
     * @param message         for a message batch, the message's index in the
     *                        batch, from 0; otherwise -1
     * @param messageRef      that message's broker ID; null if none
     * @param messageReplaces the earlier message that message replaces, as its
     *                        producer said, as {@code <itemId>/<messageId>};
     *                        null if none
     */
    record Problem(String code, String stage, long seq, long offset,
            SourcePosition at, int message, String messageRef,
            String messageReplaces)
    {
    }

    /**
     * Where a record is stored.
     *
     * @param catalog      the Iceberg catalog's name
     * @param namespace    the table's namespace
     * @param table        the table's name
     * @param ingestedDate the day the record was written, the table's
     *                     partition, so a reader can prune to it
     * @param snapshotId   the Iceberg snapshot the record was committed in
     * @param kekRef       which key encryption key wrapped the record's
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

    /**
     * Where one record is, and what a reader checks it against: all that
     * connector-queue-reader needs to read it (STAGES.md §17.4). Every
     * record message gives one ({@link RecordInStorage#location}); a stage
     * makes one from a stored row to read a record no message announced,
     * such as a failed one an operator exports.
     *
     * @param recordId        the record's id
     * @param itemId          the id of its item
     * @param seq             its number in the item, from 0
     * @param plaintextLength the stored length before compression and
     *                        encryption
     * @param chunkCount      how many encrypted chunks it is stored in
     * @param storage         where it is stored
     */
    record RecordLocation(String recordId, UUID itemId, long seq,
            long plaintextLength, int chunkCount, StorageRef storage)
    {
    }

    /**
     * Why an output was made again (STAGES.md §15.8.7): by a reprocess,
     * with the run that asked for it, or by an operator's correction; and
     * the record it supersedes at its place, when the stage held one.
     *
     * @param kind       {@link #REPROCESS} or {@link #CORRECTION}
     * @param runId      the reprocess run; null for a correction
     * @param revisionOf the record ID of the object it supersedes at the
     *                   same place; null if the stage held none
     */
    record Cause(String kind, UUID runId, String revisionOf)
    {

        /** Made again by a reprocess, or a retry. */
        public static final String REPROCESS = "reprocess";

        /** Made from an operator's correction. */
        public static final String CORRECTION = "correction";

        /**
         * Checks the kind.
         *
         * @throws IllegalArgumentException if it isn't one of the two
         */
        public Cause
        {
            if (!REPROCESS.equals(kind) && !CORRECTION.equals(kind))
            {
                throw new IllegalArgumentException("Unknown cause: " + kind);
            }
        }
    }

    /**
     * A record to process again at a stage (STAGES.md §15.8.4), sent by
     * the stage to its own input, signed by itself: the stage's input as
     * it was stored (a {@code record.split} at the convert stage, a
     * {@code record.stored} at the transformer), at the line the reprocess
     * gives it, and the run that asked for it. It is partitioned as the
     * record's own messages are.
     *
     * @param record the input record, at its new line
     * @param runId  the reprocess run
     */
    record RecordReprocess(RecordInStorage record, UUID runId)
            implements ConnectorMessage
    {

        /**
         * Checks both are given.
         *
         * @throws NullPointerException if either is null
         */
        public RecordReprocess
        {
            java.util.Objects.requireNonNull(record, "record");
            java.util.Objects.requireNonNull(runId, "runId");
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#RECORD_REPROCESS}
         */
        @Override
        public MessageType type()
        {
            return MessageType.RECORD_REPROCESS;
        }

        /**
         * {@inheritDoc}
         *
         * @return "rp-", the item id, the record id and the line's suffix:
         *         one per revision of the place
         */
        @Override
        public String messageId()
        {
            return "rp-" + record.itemId() + "-" + record.recordId()
                    + record.lineage().suffix();
        }

        /**
         * {@inheritDoc}
         *
         * @return the record's item
         */
        @Override
        public UUID itemId()
        {
            return record.itemId();
        }

        /**
         * {@inheritDoc}
         *
         * @return the record's id, as for its other messages
         */
        @Override
        public String orderingKey()
        {
            return record.recordId();
        }
    }

    /**
     * An item to split again, as its next generation (STAGES.md §15.8.4),
     * sent by the split stage to its own input, signed by itself: the
     * item's {@code item.landed} as landing kept it, the generation the
     * reprocess gives it, and the run that asked for it. It is partitioned
     * as the item's {@code item.landed} is.
     *
     * @param landed     the item, as landed
     * @param generation the generation to cut, from 2
     * @param runId      the reprocess run
     */
    record ItemReprocess(ItemLanded landed, int generation, UUID runId)
            implements ConnectorMessage
    {

        /**
         * Checks each is given, and the generation is past the first.
         *
         * @throws NullPointerException     if the item or run is null
         * @throws IllegalArgumentException if the generation is below 2
         */
        public ItemReprocess
        {
            java.util.Objects.requireNonNull(landed, "landed");
            java.util.Objects.requireNonNull(runId, "runId");
            if (generation < 2)
            {
                throw new IllegalArgumentException(
                        "A reprocess makes generation 2 or later");
            }
        }

        /**
         * {@inheritDoc}
         *
         * @return {@link MessageType#ITEM_REPROCESS}
         */
        @Override
        public MessageType type()
        {
            return MessageType.ITEM_REPROCESS;
        }

        /**
         * {@inheritDoc}
         *
         * @return "ri-", the item id and the generation's suffix: one per
         *         generation of the item
         */
        @Override
        public String messageId()
        {
            return "ri-" + landed.itemId()
                    + ConnectorMessage.generationSuffix(generation);
        }

        /**
         * {@inheritDoc}
         *
         * @return the item's id
         */
        @Override
        public UUID itemId()
        {
            return landed.itemId();
        }
    }
}
