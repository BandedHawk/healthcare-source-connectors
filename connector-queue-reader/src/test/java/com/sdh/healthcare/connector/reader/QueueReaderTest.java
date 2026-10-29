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
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.table.SectionsTable;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.storage.SectionBatchWriter;
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

class QueueReaderTest {
    static final String NS = "sdh_ingest";

    KeyPair signer;
    KeyPair recipient;
    AesKeyWrapProvider kms;
    InMemoryCatalog catalog;
    Table table;
    MessageSealer sealer;

    @BeforeEach
    void setUp() throws Exception {
        signer = ec();
        recipient = ec();
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        kms = new AesKeyWrapProvider(Map.of("sections-kek/1", kg.generateKey()));
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "memory://wh-" + UUID.randomUUID()));
        table = IngestTables.sections(catalog, NS);
        sealer = new MessageSealer(new ECDSASigner((ECPrivateKey) signer.getPrivate()), "connector-2026",
                recipient.getPublic(), "downstream-2026");
    }

    QueueReader reader() {
        return QueueReader.builder()
                .recipientKey("downstream-2026", recipient.getPrivate())
                .trustedSigner("connector-2026", (ECPublicKey) signer.getPublic())
                .catalog(catalog)
                .expectedCatalogName("prod")
                .keyProvider(kms)
                .build();
    }

    /** What the connector does for one batch: write, fenced commit, then one message per section. */
    List<byte[]> ingest(UUID fileId, byte[]... sections) {
        FencedCommitter committer = new FencedCommitter(new LeaseToken() {
            public long epoch() { return 1; }
            public void checkHeld() {}
        });
        committer.establish(table);
        SectionBatchWriter w = new SectionBatchWriter(table, kms, "sections-kek/1", new SectionIds(new byte[32]),
                Instant.now());
        long offset = 0;
        for (int i = 0; i < sections.length; i++) {
            w.add(new SectionBatchWriter.SectionInput(fileId, i, offset, "ccda", sections[i]));
            offset += sections[i].length;
        }
        DataFile df = w.finish();
        Snapshot snap = committer.commit(table, tx -> tx.newFastAppend().appendFile(df).commit());
        return w.written().stream().map(s -> sealer.seal(message(s, w, snap))).toList();
    }

    static SectionStored message(WrittenSection s, SectionBatchWriter w, Snapshot snap) {
        return new SectionStored(s.sectionId(), s.fileId(), s.seq(), s.sourceOffset(), s.sourceLength(), s.format(),
                s.representation().contentType(), s.representation().mapping(), s.plaintextLength(), s.chunkCount(),
                new StorageRef("prod", NS, SectionsTable.DEFAULT_NAME, w.ingestedDate(), snap.snapshotId(), w.kekRef()));
    }

    @Test
    void decodesMessagesAndStreamsSections() throws Exception {
        byte[] small = "<ClinicalDocument xmlns=\"urn:hl7-org:v3\"><id root=\"1\"/></ClinicalDocument>"
                .getBytes(StandardCharsets.UTF_8);
        byte[] large = new byte[2 * SectionCrypto.CHUNK_SIZE + 12345]; // three chunks
        new Random(7).nextBytes(large);
        byte[] empty = new byte[0];
        UUID fileId = UUID.randomUUID();
        List<byte[]> bodies = ingest(fileId, small, large, empty);

        try (QueueReader reader = reader()) {
            SectionStored m0 = (SectionStored) reader.decode(bodies.get(0));
            SectionStored m1 = (SectionStored) reader.decode(bodies.get(1));
            SectionStored m2 = (SectionStored) reader.decode(bodies.get(2));
            assertThat(m1.chunkCount()).isEqualTo(3);
            assertThat(reader.readSection(m0, 1 << 20)).isEqualTo(small);
            try (InputStream in = reader.openSection(m1)) {
                assertThat(in.readAllBytes()).isEqualTo(large);
            }
            assertThat(reader.readSection(m2, 10)).isEmpty();
        }
    }

    @Test
    void messageThatDoesNotMatchStoredRowsIsRejected() throws Exception {
        UUID fileId = UUID.randomUUID();
        List<byte[]> bodies = ingest(fileId, "abc".getBytes(StandardCharsets.UTF_8));
        try (QueueReader reader = reader()) {
            SectionStored real = (SectionStored) reader.decode(bodies.get(0));
            SectionStored wrongId = new SectionStored("not-the-id", real.fileId(), real.seq(), real.sourceOffset(),
                    real.sourceLength(), real.format(), real.contentType(), real.mapping(), real.plaintextLength(),
                    real.chunkCount(), real.storage());
            assertThatThrownBy(() -> reader.readSection(wrongId, 100))
                    .isInstanceOf(com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException.class);
            SectionStored missing = new SectionStored(real.sectionId(), real.fileId(), 99, 0, 3, "ccda",
                    "application/xml", null, 3, 1, real.storage());
            assertThatThrownBy(() -> reader.readSection(missing, 100)).isInstanceOf(SectionNotFoundException.class);
        }
    }

    @Test
    void newerContractMajorVersionIsRejectedWithClearReason() throws Exception {
        JWSObject jws = new JWSObject(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID("connector-2026")
                .type(MessageSealer.JWS_TYPE).build(),
                new Payload(Map.of("v", "3.0", "type", "file.started", "messageId", "x", "fileId",
                        UUID.randomUUID().toString())));
        jws.sign(new ECDSASigner((ECPrivateKey) signer.getPrivate()));
        JWEObject jwe = new JWEObject(new JWEHeader.Builder(JWEAlgorithm.ECDH_ES_A256KW, EncryptionMethod.A256GCM)
                .keyID("downstream-2026").contentType(MessageSealer.JWE_CONTENT_TYPE).build(), new Payload(jws));
        jwe.encrypt(new ECDHEncrypter((ECPublicKey) recipient.getPublic()));

        try (QueueReader reader = reader()) {
            assertThatThrownBy(() -> reader.decode(jwe.serialize().getBytes(StandardCharsets.US_ASCII)))
                    .isInstanceOf(MessageRejectedException.class)
                    .extracting(e -> ((MessageRejectedException) e).reason())
                    .isEqualTo(MessageRejectedException.Reason.UNSUPPORTED_VERSION);
        }
    }

    @Test
    void otherMessageTypesDecode() throws Exception {
        UUID fileId = UUID.randomUUID();
        byte[] body = sealer.seal(new ConnectorMessage.FileCompleted(fileId, 3, 0, 999, "digest", Instant.EPOCH,
                Instant.EPOCH.plusSeconds(5)));
        try (QueueReader reader = reader()) {
            assertThat(reader.decode(body)).isInstanceOf(ConnectorMessage.FileCompleted.class)
                    .extracting(ConnectorMessage::messageId).isEqualTo("fc-" + fileId);
        }
    }

    static KeyPair ec() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp256r1"));
        return g.generateKeyPair();
    }
}
