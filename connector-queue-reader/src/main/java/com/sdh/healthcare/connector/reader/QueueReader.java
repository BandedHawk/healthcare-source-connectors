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
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.PrivateKey;
import java.security.interfaces.ECPublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
 *     // Needs unwrap rights on the sections KEK only.
 *     .keyProvider(kmsKeyProvider)
 *     .build();
 *
 * ConnectorMessage message = reader.decode(brokerMessageBody);
 * switch (message) {
 *     // JSON when stored.mapping() isn't null (csv-row/1, ccda/2, x12/1,
 *     // edifact/1).
 *     case SectionStored stored -> {
 *         try (InputStream input = reader.openSection(stored)) { ... }
 *     }
 *     // Kept in its original format (failed.contentType()), for
 *     // remediation.
 *     case SectionFailed failed -> { ... }
 *     default -> { }
 * }
 * }</pre>
 * <p>
 * The message envelope, encryption, chunk layout and table layout are all
 * handled here. Consumers see only the message records and the section
 * bytes. Thread-safe.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class QueueReader implements AutoCloseable
{

    /** Decrypts messages and checks their signatures. */
    private final MessageOpener opener;

    /** The catalog the sections' tables are in. */
    private final Catalog catalog;

    /** The only catalog name messages may refer to; null for any. */
    private final String expectedCatalogName;

    /** The batch data keys unwrapped so far. */
    private final BatchKeyCache keys;

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
        this.opener = new MessageOpener(builder.recipients, builder.signers);
        this.catalog = Objects.requireNonNull(builder.catalog, "catalog");
        this.expectedCatalogName = builder.expectedCatalogName;
        this.keys = new BatchKeyCache(
                Objects.requireNonNull(builder.keyProvider, "keyProvider"),
                builder.keyCacheSize);
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
     */
    public ConnectorMessage decode(byte[] body) throws MessageRejectedException
    {
        return opener.open(body);
    }

    /**
     * Streams the plaintext of a stored section. Every chunk is authenticated
     * as it is read, and the stream fails if any chunk is missing,
     * duplicated, out of place or of the wrong length. It holds at most one
     * decrypted chunk (8 MiB) in memory, plus any chunks the scan returns
     * ahead of order.
     *
     * @param message the message announcing the section
     * @return a stream of the section's plaintext
     * @throws SectionNotFoundException if the section is not (or no longer)
     *                                  in the table
     */
    public InputStream openSection(SectionInStorage message)
    {
        Table table = table(message.storage());
        return SectionInputStream.open(table, message, keys);
    }

    /**
     * Reads a whole section into memory.
     *
     * @param message  the message announcing the section
     * @param maxBytes the largest section to read
     * @return the section's plaintext
     * @throws IllegalArgumentException if the section is larger than
     *                                  {@code maxBytes}
     * @throws UncheckedIOException     if the section can't be read
     */
    public byte[] readSection(SectionInStorage message, int maxBytes)
    {
        if (message.plaintextLength() > maxBytes)
        {
            throw new IllegalArgumentException(
                    "Section is larger than maxBytes");
        }
        try (InputStream input = openSection(message))
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
     * @param ref where the message says its section is stored
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

        /** The catalog the sections' tables are in. */
        private Catalog catalog;

        /** The only catalog name messages may refer to; null for any. */
        private String expectedCatalogName;

        /** Unwraps batch data keys. */
        private KeyProvider keyProvider;

        /** How many unwrapped batch keys are kept in memory. */
        private int keyCacheSize = 256;

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
         * Sets the catalog the sections' tables are in. Required.
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
         * sections KEK only. Required.
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
         * Builds the reader.
         *
         * @return the reader
         * @throws NullPointerException if the catalog or key provider is
         *                              missing
         */
        public QueueReader build()
        {
            return new QueueReader(this);
        }
    }
}
