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
 *     .keyProvider(kmsKeyProvider)          // needs unwrap rights on the sections KEK only
 *     .build();
 *
 * ConnectorMessage m = reader.decode(brokerMessageBody);
 * switch (m) {
 *     case SectionStored s -> {           // JSON when s.mapping() != null (csv-row/1, ccda/1, x12/1, edifact/1)
 *         try (InputStream in = reader.openSection(s)) { ... }
 *     }
 *     case SectionFailed f -> { ... }     // kept in its original format (f.contentType()) for remediation
 *     default -> { }
 * }
 * }</pre>
 *
 * <p>The message envelope, encryption, chunk layout and table layout are all handled here. Consumers
 * see only the message records and the section bytes. Thread-safe.
 */
public final class QueueReader implements AutoCloseable {
    private final MessageOpener opener;
    private final Catalog catalog;
    private final String expectedCatalogName;
    private final BatchKeyCache keys;
    private final Map<TableIdentifier, Table> tables = new ConcurrentHashMap<>();

    private QueueReader(Builder b) {
        this.opener = new MessageOpener(b.recipients, b.signers);
        this.catalog = Objects.requireNonNull(b.catalog, "catalog");
        this.expectedCatalogName = b.expectedCatalogName;
        this.keys = new BatchKeyCache(Objects.requireNonNull(b.keyProvider, "keyProvider"), b.keyCacheSize);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Decrypts the broker message body, checks the connector's signature and parses the payload.
     *
     * @throws MessageRejectedException if the message cannot be trusted or uses an unsupported contract
     *     major version. {@link MessageRejectedException#reason()} says which.
     */
    public ConnectorMessage decode(byte[] body) throws MessageRejectedException {
        return opener.open(body);
    }

    /**
     * Streams the plaintext of a stored section. Every chunk is authenticated as it is read, and the
     * stream fails if any chunk is missing, duplicated, out of place or of the wrong length. Holds at most
     * one decrypted chunk (8 MiB) in memory, plus any chunks the scan returns ahead of order.
     *
     * @throws SectionNotFoundException if the section is not (or no longer) in the table
     */
    public InputStream openSection(SectionInStorage message) {
        Table table = table(message.storage());
        return SectionInputStream.open(table, message, keys);
    }

    /** Reads a whole section into memory. Refuses sections larger than {@code maxBytes}. */
    public byte[] readSection(SectionInStorage message, int maxBytes) {
        if (message.plaintextLength() > maxBytes) {
            throw new IllegalArgumentException("Section is larger than maxBytes");
        }
        try (InputStream in = openSection(message)) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Table table(StorageRef ref) {
        if (expectedCatalogName != null && !expectedCatalogName.equals(ref.catalog())) {
            throw new IllegalArgumentException("Message refers to a different catalog");
        }
        TableIdentifier id = TableIdentifier.of(ref.namespace(), ref.table());
        Table t = tables.computeIfAbsent(id, catalog::loadTable);
        if (t.snapshot(ref.snapshotId()) == null) {
            // The message can arrive before our cached metadata has seen the commit.
            t.refresh();
        }
        return t;
    }

    @Override
    public void close() {
        keys.clear();
        tables.clear();
    }

    public static final class Builder {
        private final Map<String, PrivateKey> recipients = new HashMap<>();
        private final Map<String, ECPublicKey> signers = new HashMap<>();
        private Catalog catalog;
        private String expectedCatalogName;
        private KeyProvider keyProvider;
        private int keyCacheSize = 256;

        /** A private key messages are encrypted to. Add several during a key rotation. */
        public Builder recipientKey(String kid, PrivateKey key) {
            recipients.put(kid, key);
            return this;
        }

        /** A connector signing key to trust. Add several during a key rotation. */
        public Builder trustedSigner(String kid, ECPublicKey key) {
            signers.put(kid, key);
            return this;
        }

        public Builder catalog(Catalog catalog) {
            this.catalog = catalog;
            return this;
        }

        /** Optional: reject messages naming another catalog. */
        public Builder expectedCatalogName(String name) {
            this.expectedCatalogName = name;
            return this;
        }

        /** Unwraps batch data keys. Needs decrypt rights on the sections KEK only. */
        public Builder keyProvider(KeyProvider provider) {
            this.keyProvider = provider;
            return this;
        }

        /** Number of unwrapped batch keys kept in memory (default 256). */
        public Builder keyCacheSize(int size) {
            this.keyCacheSize = size;
            return this;
        }

        public QueueReader build() {
            return new QueueReader(this);
        }
    }
}
