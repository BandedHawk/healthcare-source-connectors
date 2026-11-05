/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.position.SectionOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;
import javax.crypto.SecretKey;
import org.junit.jupiter.api.Test;

/**
 * Tests that the connector contract survives a round trip: every message type
 * is written and read back unchanged, a sealed message from an untrusted
 * signer is rejected, and an encrypted chunk is bound to its associated data.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ContractRoundTripTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    ContractRoundTripTest()
    {
    }

    /**
     * Makes an elliptic-curve key pair on P-256, as the connector signs and
     * encrypts messages with.
     *
     * @return a new key pair
     * @throws Exception if the JDK has no such key generator
     */
    static KeyPair ec() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    /**
     * A sealed message opens to the same message with the right keys, and is
     * rejected when someone else signed it under the trusted signer's key id.
     *
     * @throws Exception if a key can't be made or the message can't be sealed
     */
    @Test
    void sealedMessageOpensAndRejectsUntrustedSigner() throws Exception
    {
        KeyPair signer = ec();
        KeyPair recipient = ec();
        KeyPair stranger = ec();
        SectionStored message = new SectionStored("ABC", UUID.randomUUID(),
                7, 100, 50, "ccda", "application/json", "ccda/1", 50, 1,
                new StorageRef("cat", "sdh_ingest", "sections",
                        LocalDate.of(2026, 10, 28), 42L, "kek-1"));

        byte[] body = new MessageSealer(
                new ECDSASigner((ECPrivateKey) signer.getPrivate()), "sig-1",
                recipient.getPublic(), "rcp-1").seal(message);

        MessageOpener opener = new MessageOpener(
                Map.of("rcp-1", recipient.getPrivate()),
                Map.of("sig-1", (ECPublicKey) signer.getPublic()));
        ConnectorMessage opened = opener.open(body);
        assertThat(opened).isEqualTo(message);

        byte[] forged = new MessageSealer(
                new ECDSASigner((ECPrivateKey) stranger.getPrivate()), "sig-1",
                recipient.getPublic(), "rcp-1").seal(message);
        assertThatThrownBy(() -> opener.open(forged))
                .isInstanceOf(MessageRejectedException.class)
                .extracting(rejection ->
                        ((MessageRejectedException) rejection).reason())
                .isEqualTo(MessageRejectedException.Reason.BAD_SIGNATURE);
    }

    /**
     * Every message type survives the payload codec, with and without its
     * optional fields, and a field left out isn't written as null.
     */
    @Test
    void everyMessageTypeRoundTrips()
    {
        UUID fileId = UUID.randomUUID();
        StorageRef ref = new StorageRef("cat", "sdh_ingest", "sections",
                LocalDate.of(2026, 10, 28), 42L, "kek-1");
        for (ConnectorMessage message : java.util.List.of(
                new ConnectorMessage.FileStarted(fileId, "csv", 999,
                        java.time.Instant.EPOCH, "file-connector"),
                new SectionStored("ID1", fileId, 0, 10, 20, "csv",
                        "application/json", "csv-row/1", 30, 1, ref),
                new SectionStored("ID2", fileId, 1, 30, 20, "fhir",
                        "application/fhir+json", null, 20, 1, ref),
                new ConnectorMessage.SectionFailed("ID3", fileId, 2, 50, 20,
                        "csv", "text/csv", "CONVERT_CSV_FIELD_COUNT", 40, 1,
                        ref),
                new SectionStored("ID4", fileId, 3, 70, 20, "ccda",
                        "application/json", "ccda/2", 30, 1, ref,
                        new SectionOrigin(new SourcePosition(2, 1),
                                new SourcePosition(41, 5))),
                new ConnectorMessage.SectionFailed("ID5", fileId, 4, 90, 20,
                        "ccda", "application/xml", "CONVERT_XML_MALFORMED",
                        40, 1, ref,
                        new SectionOrigin(new SourcePosition(2, 1),
                                new SourcePosition(60, 1)),
                        new SourcePosition(75, 12)),
                new ConnectorMessage.FileCompleted(fileId, 3, 1, 999,
                        "digest", java.time.Instant.EPOCH,
                        java.time.Instant.EPOCH),
                new ConnectorMessage.ItemLanded(fileId, "claims-sftp", "file",
                        999, "digest", java.time.Instant.EPOCH,
                        new StorageRef("cat", "sdh_landing", "landed_items",
                                LocalDate.of(2026, 11, 3), 43L, "kek-2")),
                new ConnectorMessage.ItemLanded(fileId, "claims-sftp", "file",
                        999, "digest", java.time.Instant.EPOCH,
                        new StorageRef("cat", "sdh_landing", "landed_items",
                                LocalDate.of(2026, 11, 3), 43L, "kek-2"),
                        UUID.randomUUID()),
                new ConnectorMessage.ItemReceived(fileId, "claims", "file",
                        999, "ab12", java.time.Instant.EPOCH,
                        "/out/claims.csv", UUID.randomUUID(), null, null),
                new ConnectorMessage.ItemReceived(fileId, "adt",
                        "message-batch", 99, null, java.time.Instant.EPOCH,
                        null, null, java.util.Arrays.asList("m-1", null),
                        java.util.Arrays.asList(null, fileId + "/m-0")),
                new ConnectorMessage.ItemResult(fileId, "claims",
                        ConnectorMessage.ItemResult.COMPLETED, 3, 1, null, 1,
                        1, 1, java.util.List.of(new ConnectorMessage.Problem(
                                "CONVERT_CSV_FIELD_COUNT", "convert", 2, 50,
                                new SourcePosition(4, 1), -1, null, null)),
                        "/out/claims.csv", null, java.time.Instant.EPOCH),
                new ConnectorMessage.ItemResult(fileId, "adt",
                        ConnectorMessage.ItemResult.FAILED, 0, 0,
                        new ConnectorMessage.Problem("PARSE_JSON_MALFORMED",
                                "split", -1, 7, null, 2, "m-3",
                                fileId + "/m-1"),
                        3, 2, 2, java.util.List.of(), null,
                        UUID.randomUUID(), java.time.Instant.EPOCH)))
        {
            assertThat(PayloadCodec.fromJson(PayloadCodec.toJson(message)))
                    .isEqualTo(message);
        }
        assertThat(PayloadCodec.toJson(new SectionStored("ID2", fileId, 1, 30,
                20, "fhir", "application/fhir+json", null, 20, 1, ref)))
                .doesNotContainKey("mapping");
        assertThat(PayloadCodec.toJson(new ConnectorMessage.ItemReceived(
                fileId, "claims", "push", 1, null, java.time.Instant.EPOCH,
                null, null, null, null)))
                .doesNotContainKeys("sha256", "producerRef", "replaces",
                        "messageIds", "messageReplaces");
    }

    /**
     * An encrypted chunk decrypts to what was encrypted, and only as the
     * chunk and section it was encrypted as.
     */
    @Test
    void chunkRoundTripAndAadBinding()
    {
        byte[] batchDataKey = new byte[32];
        UUID fileId = UUID.randomUUID();
        SecretKey key = SectionCrypto.sectionKey(batchDataKey, fileId, 3);
        byte[] plaintext = "<ClinicalDocument/>"
                .getBytes(StandardCharsets.UTF_8);

        byte[] stored = SectionCrypto.encryptChunk(key, fileId, 3, 0, 1,
                plaintext);
        assertThat(SectionCrypto.decryptChunk(key, fileId, 3, 0, 1,
                plaintext.length, stored)).isEqualTo(plaintext);

        // The same bytes presented as another chunk position or another
        // section are rejected.
        assertThatThrownBy(() -> SectionCrypto.decryptChunk(key, fileId, 3, 1,
                2, plaintext.length, stored))
                .isInstanceOf(ChunkIntegrityException.class);
        assertThatThrownBy(() -> SectionCrypto.decryptChunk(
                SectionCrypto.sectionKey(batchDataKey, fileId, 4), fileId, 4,
                0, 1, plaintext.length, stored))
                .isInstanceOf(ChunkIntegrityException.class);
    }
}
