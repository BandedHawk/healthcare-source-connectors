/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDHEncrypter;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.ContractVersion;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemConverted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.RecordInput;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.WrittenRecord;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link QueueReader}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class QueueReaderTest
{

    /** The namespace the records table is in. */
    static final String NS = "sdh_ingest";

    /** The connector's signing key pair. */
    KeyPair signer;

    /** Downstream's key pair, which messages are encrypted to. */
    KeyPair recipient;

    /** A local key provider holding the records KEK. */
    AesKeyWrapProvider kms;

    /** The catalog, named "prod". */
    InMemoryCatalog catalog;

    /** The records table. */
    Table table;

    /** Seals messages as the connector does. */
    MessageSealer sealer;

    /**
     * Creates keys, a catalog with the records table, and a sealer.
     *
     * @throws Exception if a key can't be generated
     */
    @BeforeEach
    void setUp() throws Exception
    {
        signer = ec();
        recipient = ec();
        KeyGenerator aesGenerator = KeyGenerator.getInstance("AES");
        aesGenerator.init(256);
        kms = new AesKeyWrapProvider(
                Map.of("records-kek/1", aesGenerator.generateKey()));
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        table = IngestTables.records(catalog, NS);
        sealer = new MessageSealer(
                new ECDSASigner((ECPrivateKey) signer.getPrivate()),
                "connector-2026", recipient.getPublic(), "downstream-2026");
    }

    /**
     * A reader with downstream's key, trusting the connector's.
     *
     * @return the reader
     */
    QueueReader reader()
    {
        return QueueReader.builder()
                .recipientKey("downstream-2026", recipient.getPrivate())
                .trustedSigner("connector-2026",
                        (ECPublicKey) signer.getPublic())
                .catalog(catalog)
                .expectedCatalogName("prod")
                .keyProvider(kms)
                .build();
    }

    /**
     * What the connector does for one batch: write, fenced commit, then one
     * message per record.
     *
     * @param itemId  the file the records come from
     * @param records the records' plaintext
     * @return the sealed messages, one per record
     */
    List<byte[]> ingest(UUID itemId, byte[]... records)
    {
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
        {
            public long epoch()
            {
                return 1;
            }

            public void checkHeld()
            {
            }
        });
        committer.establish(table);
        RecordBatchWriter writer = new RecordBatchWriter(table, kms,
                "records-kek/1", new RecordIds(new byte[32]), Instant.now());
        long offset = 0;
        for (int index = 0; index < records.length; index++)
        {
            writer.add(new RecordInput(itemId, index, offset, "ccda",
                    records[index]));
            offset += records[index].length;
        }
        DataFile dataFile = writer.finish();
        Snapshot snapshot = committer.commit(table, transaction ->
                transaction.newFastAppend().appendFile(dataFile).commit());
        return writer.written().stream()
                .map(record -> sealer.seal(message(record, writer,
                        snapshot)))
                .toList();
    }

    /**
     * The message the connector sends for a written record.
     *
     * @param record   the record
     * @param writer   the writer that wrote it
     * @param snapshot the commit that made it visible
     * @return the message
     */
    static RecordStored message(WrittenRecord record,
            RecordBatchWriter writer, Snapshot snapshot)
    {
        return new RecordStored(record.recordId(), record.itemId(),
                record.seq(), record.sourceOffset(), record.sourceLength(),
                record.format(), record.representation().contentType(),
                record.representation().mapping(), record.plaintextLength(),
                record.chunkCount(),
                new StorageRef("prod", NS, RecordsTable.DEFAULT_NAME,
                        writer.ingestedDate(), snapshot.snapshotId(),
                        writer.kekRef()));
    }

    /**
     * Messages decode, and a small, a multi-chunk and an empty record
     * read back intact.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void decodesMessagesAndStreamsRecords() throws Exception
    {
        byte[] small = ("<ClinicalDocument xmlns=\"urn:hl7-org:v3\">"
                + "<id root=\"1\"/></ClinicalDocument>")
                .getBytes(StandardCharsets.UTF_8);
        // Three chunks.
        byte[] large = new byte[2 * RecordCrypto.CHUNK_SIZE + 12345];
        new Random(7).nextBytes(large);
        byte[] empty = new byte[0];
        UUID itemId = UUID.randomUUID();
        List<byte[]> bodies = ingest(itemId, small, large, empty);

        try (QueueReader reader = reader())
        {
            RecordStored smallMessage =
                    (RecordStored) reader.decode(bodies.get(0));
            RecordStored largeMessage =
                    (RecordStored) reader.decode(bodies.get(1));
            RecordStored emptyMessage =
                    (RecordStored) reader.decode(bodies.get(2));
            assertThat(largeMessage.chunkCount()).isEqualTo(3);
            assertThat(reader.readRecord(smallMessage, 1 << 20))
                    .isEqualTo(small);
            try (InputStream input = reader.openRecord(largeMessage))
            {
                assertThat(input.readAllBytes()).isEqualTo(large);
            }
            assertThat(reader.readRecord(emptyMessage, 10)).isEmpty();
        }
    }

    /**
     * {@code decodeNew} returns a record's message once: the same body
     * again, or the message sealed again as a stage sends it after a crash,
     * is dropped, while {@code decode} returns every one. With room for one
     * ID, the first is forgotten once another is decoded.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void decodeNewDropsARepeatedMessage() throws Exception
    {
        UUID itemId = UUID.randomUUID();
        List<byte[]> bodies = ingest(itemId, "one".getBytes(
                StandardCharsets.UTF_8), "two".getBytes(
                StandardCharsets.UTF_8));
        try (QueueReader reader = reader())
        {
            ConnectorMessage first = reader.decodeNew(bodies.get(0))
                    .orElseThrow();
            assertThat(reader.decodeNew(bodies.get(0))).isEmpty();
            assertThat(reader.decodeNew(sealer.seal(first))).isEmpty();
            assertThat(reader.decodeNew(bodies.get(1))).isPresent();
            assertThat(reader.decode(bodies.get(0)))
                    .isEqualTo(first);
        }
        try (QueueReader small = QueueReader.builder()
                .recipientKey("downstream-2026", recipient.getPrivate())
                .trustedSigner("connector-2026",
                        (ECPublicKey) signer.getPublic())
                .catalog(catalog).keyProvider(kms).repeatMemory(1).build())
        {
            assertThat(small.decodeNew(bodies.get(0))).isPresent();
            assertThat(small.decodeNew(bodies.get(1))).isPresent();
            assertThat(small.decodeNew(bodies.get(0))).isPresent();
        }
        assertThatThrownBy(() -> QueueReader.builder().repeatMemory(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * A message whose record ID doesn't match the rows is refused, and one
     * for a record with no rows isn't found.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void messageThatDoesNotMatchStoredRowsIsRejected() throws Exception
    {
        UUID itemId = UUID.randomUUID();
        List<byte[]> bodies =
                ingest(itemId, "abc".getBytes(StandardCharsets.UTF_8));
        try (QueueReader reader = reader())
        {
            RecordStored real = (RecordStored) reader.decode(bodies.get(0));
            RecordStored wrongId = new RecordStored("not-the-id",
                    real.itemId(), real.seq(), real.sourceOffset(),
                    real.sourceLength(), real.format(), real.contentType(),
                    real.mapping(), real.plaintextLength(), real.chunkCount(),
                    real.storage());
            assertThatThrownBy(() -> reader.readRecord(wrongId, 100))
                    .isInstanceOf(ChunkIntegrityException.class);
            RecordStored missing = new RecordStored(real.recordId(),
                    real.itemId(), 99, 0, 3, "ccda", "application/xml", null,
                    3, 1, real.storage());
            assertThatThrownBy(() -> reader.readRecord(missing, 100))
                    .isInstanceOf(RecordNotFoundException.class);
        }
    }

    /**
     * A record committed twice, by two stage workers (STAGES.md §6), reads
     * back once, from one copy, small or of several chunks; a reader built
     * without message keys reads it too, and refuses to decode.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void readsOneCopyOfARecordCommittedTwice() throws Exception
    {
        byte[] small = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] large = new byte[RecordCrypto.CHUNK_SIZE + 99];
        new Random(3).nextBytes(large);
        UUID itemId = UUID.randomUUID();
        List<byte[]> first = ingest(itemId, small, large);
        ingest(itemId, small, large);
        try (QueueReader reader = reader();
                QueueReader recordsOnly = QueueReader.builder()
                        .catalog(catalog)
                        .keyProvider(kms)
                        .build())
        {
            RecordStored smallMessage =
                    (RecordStored) reader.decode(first.get(0));
            RecordStored largeMessage =
                    (RecordStored) reader.decode(first.get(1));
            assertThat(reader.readRecord(smallMessage, 1 << 20))
                    .isEqualTo(small);
            assertThat(recordsOnly.readRecord(largeMessage, 1 << 24))
                    .isEqualTo(large);
            assertThatThrownBy(() -> recordsOnly.decode(first.get(0)))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    /**
     * A message from a newer contract major version is refused, saying
     * why.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void newerContractMajorVersionIsRejectedWithClearReason() throws Exception
    {
        JWSObject jws = new JWSObject(
                new JWSHeader.Builder(JWSAlgorithm.ES256)
                        .keyID("connector-2026")
                        .type(MessageSealer.JWS_TYPE).build(),
                new Payload(Map.of("v", (ContractVersion.MAJOR + 1) + ".0",
                        "type", "item.landed",
                        "messageId", "x",
                        "itemId", UUID.randomUUID().toString())));
        jws.sign(new ECDSASigner((ECPrivateKey) signer.getPrivate()));
        JWEObject jwe = new JWEObject(
                new JWEHeader.Builder(JWEAlgorithm.ECDH_ES_A256KW,
                        EncryptionMethod.A256GCM)
                        .keyID("downstream-2026")
                        .contentType(MessageSealer.JWE_CONTENT_TYPE).build(),
                new Payload(jws));
        jwe.encrypt(new ECDHEncrypter((ECPublicKey) recipient.getPublic()));

        try (QueueReader reader = reader())
        {
            assertThatThrownBy(() -> reader.decode(
                    jwe.serialize().getBytes(StandardCharsets.US_ASCII)))
                    .isInstanceOf(MessageRejectedException.class)
                    .extracting(thrown ->
                            ((MessageRejectedException) thrown).reason())
                    .isEqualTo(MessageRejectedException.Reason
                            .UNSUPPORTED_VERSION);
        }
    }

    /**
     * Message types other than records decode too.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void otherMessageTypesDecode() throws Exception
    {
        UUID itemId = UUID.randomUUID();
        byte[] body = sealer.seal(new ItemConverted(itemId, "claims", 3, 0,
                Instant.EPOCH.plusSeconds(5)));
        try (QueueReader reader = reader())
        {
            assertThat(reader.decode(body))
                    .isInstanceOf(ItemConverted.class)
                    .extracting(ConnectorMessage::messageId)
                    .isEqualTo("ic-convert-" + itemId);
        }
    }

    /**
     * A new P-256 key pair.
     *
     * @return the key pair
     * @throws Exception if it can't be generated
     */
    static KeyPair ec() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }
}
