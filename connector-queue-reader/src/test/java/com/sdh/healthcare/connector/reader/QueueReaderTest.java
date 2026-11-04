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
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.table.SectionsTable;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.storage.SectionBatchWriter;
import com.sdh.healthcare.connector.core.storage.SectionBatchWriter.SectionInput;
import com.sdh.healthcare.connector.core.storage.SectionBatchWriter.WrittenSection;
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

    /** The namespace the sections table is in. */
    static final String NS = "sdh_ingest";

    /** The connector's signing key pair. */
    KeyPair signer;

    /** Downstream's key pair, which messages are encrypted to. */
    KeyPair recipient;

    /** A local key provider holding the sections KEK. */
    AesKeyWrapProvider kms;

    /** The catalog, named "prod". */
    InMemoryCatalog catalog;

    /** The sections table. */
    Table table;

    /** Seals messages as the connector does. */
    MessageSealer sealer;

    /**
     * Creates keys, a catalog with the sections table, and a sealer.
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
                Map.of("sections-kek/1", aesGenerator.generateKey()));
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        table = IngestTables.sections(catalog, NS);
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
     * message per section.
     *
     * @param fileId the file the sections come from
     * @param sections the sections' plaintext
     * @return the sealed messages, one per section
     */
    List<byte[]> ingest(UUID fileId, byte[]... sections)
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
        SectionBatchWriter writer = new SectionBatchWriter(table, kms,
                "sections-kek/1", new SectionIds(new byte[32]), Instant.now());
        long offset = 0;
        for (int index = 0; index < sections.length; index++)
        {
            writer.add(new SectionInput(fileId, index, offset, "ccda",
                    sections[index]));
            offset += sections[index].length;
        }
        DataFile dataFile = writer.finish();
        Snapshot snapshot = committer.commit(table, transaction ->
                transaction.newFastAppend().appendFile(dataFile).commit());
        return writer.written().stream()
                .map(section -> sealer.seal(message(section, writer,
                        snapshot)))
                .toList();
    }

    /**
     * The message the connector sends for a written section.
     *
     * @param section the section
     * @param writer the writer that wrote it
     * @param snapshot the commit that made it visible
     * @return the message
     */
    static SectionStored message(WrittenSection section,
            SectionBatchWriter writer, Snapshot snapshot)
    {
        return new SectionStored(section.sectionId(), section.fileId(),
                section.seq(), section.sourceOffset(), section.sourceLength(),
                section.format(), section.representation().contentType(),
                section.representation().mapping(), section.plaintextLength(),
                section.chunkCount(),
                new StorageRef("prod", NS, SectionsTable.DEFAULT_NAME,
                        writer.ingestedDate(), snapshot.snapshotId(),
                        writer.kekRef()));
    }

    /**
     * Messages decode, and a small, a multi-chunk and an empty section
     * read back intact.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void decodesMessagesAndStreamsSections() throws Exception
    {
        byte[] small = ("<ClinicalDocument xmlns=\"urn:hl7-org:v3\">"
                + "<id root=\"1\"/></ClinicalDocument>")
                .getBytes(StandardCharsets.UTF_8);
        // Three chunks.
        byte[] large = new byte[2 * SectionCrypto.CHUNK_SIZE + 12345];
        new Random(7).nextBytes(large);
        byte[] empty = new byte[0];
        UUID fileId = UUID.randomUUID();
        List<byte[]> bodies = ingest(fileId, small, large, empty);

        try (QueueReader reader = reader())
        {
            SectionStored smallMessage =
                    (SectionStored) reader.decode(bodies.get(0));
            SectionStored largeMessage =
                    (SectionStored) reader.decode(bodies.get(1));
            SectionStored emptyMessage =
                    (SectionStored) reader.decode(bodies.get(2));
            assertThat(largeMessage.chunkCount()).isEqualTo(3);
            assertThat(reader.readSection(smallMessage, 1 << 20))
                    .isEqualTo(small);
            try (InputStream input = reader.openSection(largeMessage))
            {
                assertThat(input.readAllBytes()).isEqualTo(large);
            }
            assertThat(reader.readSection(emptyMessage, 10)).isEmpty();
        }
    }

    /**
     * A message whose section ID doesn't match the rows is refused, and one
     * for a section with no rows isn't found.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void messageThatDoesNotMatchStoredRowsIsRejected() throws Exception
    {
        UUID fileId = UUID.randomUUID();
        List<byte[]> bodies =
                ingest(fileId, "abc".getBytes(StandardCharsets.UTF_8));
        try (QueueReader reader = reader())
        {
            SectionStored real = (SectionStored) reader.decode(bodies.get(0));
            SectionStored wrongId = new SectionStored("not-the-id",
                    real.fileId(), real.seq(), real.sourceOffset(),
                    real.sourceLength(), real.format(), real.contentType(),
                    real.mapping(), real.plaintextLength(), real.chunkCount(),
                    real.storage());
            assertThatThrownBy(() -> reader.readSection(wrongId, 100))
                    .isInstanceOf(ChunkIntegrityException.class);
            SectionStored missing = new SectionStored(real.sectionId(),
                    real.fileId(), 99, 0, 3, "ccda", "application/xml", null,
                    3, 1, real.storage());
            assertThatThrownBy(() -> reader.readSection(missing, 100))
                    .isInstanceOf(SectionNotFoundException.class);
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
                new Payload(Map.of("v", "3.0", "type", "file.started",
                        "messageId", "x",
                        "fileId", UUID.randomUUID().toString())));
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
     * Message types other than sections decode too.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void otherMessageTypesDecode() throws Exception
    {
        UUID fileId = UUID.randomUUID();
        byte[] body = sealer.seal(new FileCompleted(fileId, 3, 0, 999,
                "digest", Instant.EPOCH, Instant.EPOCH.plusSeconds(5)));
        try (QueueReader reader = reader())
        {
            assertThat(reader.decode(body))
                    .isInstanceOf(FileCompleted.class)
                    .extracting(ConnectorMessage::messageId)
                    .isEqualTo("fc-" + fileId);
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
