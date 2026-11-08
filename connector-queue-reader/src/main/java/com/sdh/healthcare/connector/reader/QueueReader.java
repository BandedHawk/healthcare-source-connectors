/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.reader;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.PrivateKey;
import java.security.interfaces.ECPublicKey;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * The one entry point for systems that consume connector messages.
 *
 * <pre>{@code
 * QueueReader reader = QueueReader.builder()
 *     .recipientKey("downstream-2026", myPrivateKey)
 *     .trustedSigner("connector-2026", connectorPublicKey)
 *     .catalog(icebergCatalog)
 *     // Needs unwrap rights on the records KEK only.
 *     .keyProvider(kmsKeyProvider)
 *     .build();
 *
 * ConnectorMessage message = reader.decode(brokerMessageBody);
 * switch (message) {
 *     // JSON when stored.mapping() isn't null (csv-row/1, ccda/2, x12/1,
 *     // edifact/1).
 *     case RecordStored stored -> {
 *         try (InputStream input = reader.openRecord(stored)) { ... }
 *     }
 *     // Kept in its original format (failed.contentType()), for
 *     // remediation.
 *     case RecordFailed failed -> { ... }
 *     default -> { }
 * }
 * }</pre>
 * <p>
 * The message envelope, encryption, chunk layout and table layout are all
 * handled here. Consumers see only the message records and the record
 * bytes. Thread-safe.
 * <p>
 * Delivery is at least once: a stage sends a message again after a crash,
 * and an operator's replay sends an item's messages again (STAGES.md
 * §13.2), each with the same contract {@code messageId} (for a record,
 * its item and record IDs). {@link #decodeNew} drops a message whose ID
 * this reader has decoded recently, the last
 * {@link Builder#repeatMemory} IDs; {@link #decode} returns every
 * message. The memory is the process's own, so a consumer that must never
 * process a record twice, across its restarts too, keeps the record IDs it
 * has processed itself.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class QueueReader implements AutoCloseable
{

    /**
     * Decrypts messages and checks their signatures; null for a reader
     * built without message keys, which only reads records.
     */
    private final MessageOpener opener;

    /** The catalog the records' tables are in. */
    private final Catalog catalog;

    /** The only catalog name messages may refer to; null for any. */
    private final String expectedCatalogName;

    /** The batch data keys unwrapped so far. */
    private final BatchKeyCache keys;

    /**
     * The message IDs decoded most recently by {@link #decodeNew}, oldest
     * first; guarded by itself.
     */
    private final Map<String, Boolean> seen;

    /** The tables loaded so far, by identifier. */
    private final Map<TableIdentifier, Table> tables =
            new ConcurrentHashMap<>();

    /**
     * Creates a reader from a builder's settings.
     *
     * @param builder the settings
     * @throws NullPointerException if the catalog or key provider is missing
     */
    private QueueReader(Builder builder)
    {
        this.opener = builder.recipients.isEmpty()
                && builder.signers.isEmpty() ? null
                : new MessageOpener(builder.recipients, builder.signers);
        this.catalog = Objects.requireNonNull(builder.catalog, "catalog");
        this.expectedCatalogName = builder.expectedCatalogName;
        this.keys = new BatchKeyCache(
                Objects.requireNonNull(builder.keyProvider, "keyProvider"),
                builder.keyCacheSize);
        int memory = builder.repeatMemory;
        this.seen = new LinkedHashMap<>(16, 0.75f, true)
        {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(
                    Map.Entry<String, Boolean> eldest)
            {
                return size() > memory;
            }
        };
    }

    /**
     * Starts building a reader.
     *
     * @return a new builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Decrypts the broker message body, checks the connector's signature and
     * parses the payload. If the message is rejected, the exception's
     * {@link MessageRejectedException#reason()} says why.
     *
     * @param body the broker message's body
     * @return the message
     * @throws MessageRejectedException if the message can't be trusted or
     *                                  uses an unsupported contract major
     *                                  version
     * @throws IllegalStateException    if the reader was built without
     *                                  message keys
     */
    public ConnectorMessage decode(byte[] body) throws MessageRejectedException
    {
        if (opener == null)
        {
            throw new IllegalStateException(
                    "This reader was built to read records only");
        }
        return opener.open(body);
    }

    /**
     * Decodes a message as {@link #decode} does, but drops it if this
     * reader has decoded one with the same contract {@code messageId}
     * recently: a message sent again after a stage's crash, or by a replay
     * (STAGES.md §13.2), under the same ID. A record's message ID is its
     * item's and record's IDs, so a record is returned once.
     *
     * @param body the broker message's body
     * @return the message; empty if it repeats one decoded recently
     * @throws MessageRejectedException if the message can't be trusted or
     *                                  uses an unsupported contract major
     *                                  version
     * @throws IllegalStateException    if the reader was built without
     *                                  message keys
     */
    public Optional<ConnectorMessage> decodeNew(byte[] body)
            throws MessageRejectedException
    {
        ConnectorMessage message = decode(body);
        synchronized (seen)
        {
            if (seen.put(message.messageId(), Boolean.TRUE) != null)
            {
                return Optional.empty();
            }
        }
        return Optional.of(message);
    }

    /**
     * Streams the plaintext of a stored record. Every chunk is authenticated
     * as it is read, and the stream fails if any chunk is missing,
     * duplicated, out of place or of the wrong length. It holds at most one
     * decrypted chunk (8 MiB) in memory, plus any chunks the scan returns
     * ahead of order.
     *
     * @param message the message announcing the record
     * @return a stream of the record's plaintext
     * @throws RecordNotFoundException if the record is not (or no longer)
     *                                  in the table
     */
    public InputStream openRecord(RecordInStorage message)
    {
        Table table = table(message.storage());
        return RecordInputStream.open(table, message, keys);
    }

    /**
     * Reads a whole record into memory.
     *
     * @param message  the message announcing the record
     * @param maxBytes the largest record to read
     * @return the record's plaintext
     * @throws IllegalArgumentException if the record is larger than
     *                                  {@code maxBytes}
     * @throws UncheckedIOException     if the record can't be read
     */
    public byte[] readRecord(RecordInStorage message, int maxBytes)
    {
        if (message.plaintextLength() > maxBytes)
        {
            throw new IllegalArgumentException(
                    "SplitRecord is larger than maxBytes");
        }
        try (InputStream input = openRecord(message))
        {
            return input.readAllBytes();
        }
        catch (IOException error)
        {
            throw new UncheckedIOException(error);
        }
    }

    /**
     * Finds the table a message refers to, loading it the first time.
     *
     * @param ref where the message says its record is stored
     * @return the table, refreshed if it hasn't seen the message's snapshot
     * @throws IllegalArgumentException if the message names a catalog other
     *                                  than the expected one
     */
    private Table table(StorageRef ref)
    {
        if (expectedCatalogName != null
                && !expectedCatalogName.equals(ref.catalog()))
        {
            throw new IllegalArgumentException(
                    "Message refers to a different catalog");
        }
        TableIdentifier id = TableIdentifier.of(ref.namespace(), ref.table());
        Table table = tables.computeIfAbsent(id, catalog::loadTable);
        if (table.snapshot(ref.snapshotId()) == null)
        {
            // The message can arrive before our cached metadata has seen the
            // commit.
            table.refresh();
        }
        return table;
    }

    /** Forgets the unwrapped keys and the loaded tables. */
    @Override
    public void close()
    {
        keys.clear();
        tables.clear();
    }

    /** Collects a reader's keys, catalog and settings. */
    public static final class Builder
    {

        /** The private keys messages are encrypted to, by key id. */
        private final Map<String, PrivateKey> recipients = new HashMap<>();

        /** The connector signing keys to trust, by key id. */
        private final Map<String, ECPublicKey> signers = new HashMap<>();

        /** The catalog the records' tables are in. */
        private Catalog catalog;

        /** The only catalog name messages may refer to; null for any. */
        private String expectedCatalogName;

        /** Unwraps batch data keys. */
        private KeyProvider keyProvider;

        /** How many unwrapped batch keys are kept in memory. */
        private int keyCacheSize = 256;

        /** How many message IDs {@link QueueReader#decodeNew} remembers. */
        private int repeatMemory = 100_000;

        /**
         * Creates a builder with no keys and the default cache size.
         * {@link QueueReader#builder()} does the same.
         */
        public Builder()
        {
        }

        /**
         * Adds a private key messages are encrypted to. Add several during a
         * key rotation.
         *
         * @param kid the key's id
         * @param key the private key
         * @return this builder
         */
        public Builder recipientKey(String kid, PrivateKey key)
        {
            recipients.put(kid, key);
            return this;
        }

        /**
         * Adds a connector signing key to trust. Add several during a key
         * rotation.
         *
         * @param kid the key's id
         * @param key the public key
         * @return this builder
         */
        public Builder trustedSigner(String kid, ECPublicKey key)
        {
            signers.put(kid, key);
            return this;
        }

        /**
         * Sets the catalog the records' tables are in. Required.
         *
         * @param catalog the Iceberg catalog
         * @return this builder
         */
        public Builder catalog(Catalog catalog)
        {
            this.catalog = catalog;
            return this;
        }

        /**
         * Rejects messages naming another catalog. Optional.
         *
         * @param name the only catalog name to accept
         * @return this builder
         */
        public Builder expectedCatalogName(String name)
        {
            this.expectedCatalogName = name;
            return this;
        }

        /**
         * Sets what unwraps batch data keys. It needs decrypt rights on the
         * records KEK only. Required.
         *
         * @param provider the key provider
         * @return this builder
         */
        public Builder keyProvider(KeyProvider provider)
        {
            this.keyProvider = provider;
            return this;
        }

        /**
         * Sets how many unwrapped batch keys are kept in memory (default
         * 256).
         *
         * @param size how many
         * @return this builder
         */
        public Builder keyCacheSize(int size)
        {
            this.keyCacheSize = size;
            return this;
        }

        /**
         * Sets how many message IDs {@link QueueReader#decodeNew} remembers
         * to drop repeats by (default 100,000, about 15 MiB): the oldest is
         * forgotten first. Set it above the most messages a stage may send
         * again at once, such as an item's records for a replay.
         *
         * @param size how many; at least 1
         * @return this builder
         * @throws IllegalArgumentException if it is less than 1
         */
        public Builder repeatMemory(int size)
        {
            if (size < 1)
            {
                throw new IllegalArgumentException(
                        "repeatMemory must be at least 1");
            }
            this.repeatMemory = size;
            return this;
        }

        /**
         * Builds the reader. Without a recipient key or trusted signer it
         * only reads records, for a stage whose own worker opens the
         * messages; {@link QueueReader#decode} then refuses.
         *
         * @return the reader
         * @throws NullPointerException     if the catalog or key provider
         *                                  is missing
         * @throws IllegalArgumentException if only one of a recipient key
         *                                  and a trusted signer is given
         */
        public QueueReader build()
        {
            return new QueueReader(this);
        }
    }
}
