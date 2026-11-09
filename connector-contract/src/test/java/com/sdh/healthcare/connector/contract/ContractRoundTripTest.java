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
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.position.RecordOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
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
        RecordStored message = new RecordStored("ABC", UUID.randomUUID(),
                7, 100, 50, "ccda", "application/json", "ccda/1", 50, 1,
                new StorageRef("cat", "sdh_ingest", "records",
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
        UUID itemId = UUID.randomUUID();
        StorageRef ref = new StorageRef("cat", "sdh_ingest", "records",
                LocalDate.of(2026, 10, 28), 42L, "kek-1");
        for (ConnectorMessage message : java.util.List.of(
                new RecordStored("ID1", itemId, 0, 10, 20, "csv",
                        "application/json", "csv-row/1", 30, 1, ref),
                new RecordStored("ID2", itemId, 1, 30, 20, "fhir",
                        "application/fhir+json", null, 20, 1, ref),
                new ConnectorMessage.RecordFailed("ID3", itemId, 2, 50, 20,
                        "csv", "text/csv", "CONVERT_CSV_FIELD_COUNT", 40, 1,
                        ref),
                new RecordStored("ID4", itemId, 3, 70, 20, "ccda",
                        "application/json", "ccda/2", 30, 1, ref,
                        new RecordOrigin(new SourcePosition(2, 1),
                                new SourcePosition(41, 5))),
                new ConnectorMessage.RecordFailed("ID5", itemId, 4, 90, 20,
                        "ccda", "application/xml", "CONVERT_XML_MALFORMED",
                        40, 1, ref,
                        new RecordOrigin(new SourcePosition(2, 1),
                                new SourcePosition(60, 1)),
                        new SourcePosition(75, 12)),
                new ConnectorMessage.ItemLanded(itemId, "claims-sftp", "file",
                        999, "digest", java.time.Instant.EPOCH,
                        new StorageRef("cat", "sdh_landing", "landed_items",
                                LocalDate.of(2026, 11, 3), 43L, "kek-2")),
                new ConnectorMessage.ItemLanded(itemId, "claims-sftp", "file",
                        999, "digest", java.time.Instant.EPOCH,
                        new StorageRef("cat", "sdh_landing", "landed_items",
                                LocalDate.of(2026, 11, 3), 43L, "kek-2"),
                        UUID.randomUUID()),
                new ConnectorMessage.ItemReceived(itemId, "claims", "file",
                        999, "ab12", java.time.Instant.EPOCH,
                        "/out/claims.csv", UUID.randomUUID(), null, null),
                new ConnectorMessage.ItemReceived(itemId, "adt",
                        "message-batch", 99, null, java.time.Instant.EPOCH,
                        null, null, java.util.Arrays.asList("m-1", null),
                        java.util.Arrays.asList(null, itemId + "/m-0")),
                new ConnectorMessage.ItemResult(itemId, "claims",
                        ConnectorMessage.ItemResult.COMPLETED, 3, 1, null, 1,
                        1, 1, java.util.List.of(new ConnectorMessage.Problem(
                                "CONVERT_CSV_FIELD_COUNT", "convert", 2, 50,
                                new SourcePosition(4, 1), -1, null, null)),
                        "/out/claims.csv", null, java.time.Instant.EPOCH),
                new ConnectorMessage.ItemResult(itemId, "adt",
                        ConnectorMessage.ItemResult.FAILED, 0, 0,
                        new ConnectorMessage.Problem("PARSE_JSON_MALFORMED",
                                "split", -1, 7, null, 2, "m-3",
                                itemId + "/m-1"),
                        3, 2, 2, java.util.List.of(), null,
                        UUID.randomUUID(), java.time.Instant.EPOCH),
                new ConnectorMessage.RecordConflict("ID6", itemId, "convert",
                        "d-1", "d-2", java.time.Instant.EPOCH),
                new ConnectorMessage.RecordSplit("ID7", itemId, 5, 110, 20,
                        "ccda", "application/xml", 20, 1, ref,
                        new RecordOrigin(new SourcePosition(2, 1),
                                new SourcePosition(9, 3)), null, null, -1,
                        null, null),
                new ConnectorMessage.RecordSplit("ID8", itemId, 6, 130, 20,
                        "csv", "text/csv", 20, 1, ref, null,
                        "PARSE_UNTERMINATED_QUOTE", new SourcePosition(7, 4),
                        3, "m-4", "m-1"),
                new ConnectorMessage.ItemSplit(itemId, "claims", 7, 1, 999,
                        "digest", "claims-2026-11.csv", UUID.randomUUID(),
                        null, java.time.Instant.EPOCH,
                        java.time.Instant.EPOCH),
                new ConnectorMessage.ItemSplit(itemId, "claims", 2, 0, 999,
                        "digest", null, null, new ConnectorMessage.Problem(
                                "PARSE_XML_MALFORMED", "split", -1, 300,
                                new SourcePosition(12, 1), -1, null, null),
                        java.time.Instant.EPOCH, java.time.Instant.EPOCH),
                new ConnectorMessage.ItemConverted(itemId, "claims", 7, 2,
                        java.time.Instant.EPOCH),
                // With the versions that made them (STAGES.md §14).
                new RecordStored("ID9", itemId, 7, 150, 20, "ccda",
                        "application/json", "ccda/2", 30, 1, ref, null,
                        "0.1.0@2026-11-08T10:00:00Z/ccda/2/3f9a0c1d22b7"),
                new ConnectorMessage.RecordFailed("ID10", itemId, 8, 170, 20,
                        "csv", "text/csv", "CONVERT_CSV_FIELD_COUNT", 40, 1,
                        ref, null, null, "0.1.0@x/csv-row/1/ab"),
                new ConnectorMessage.RecordSplit("ID11", itemId, 9, 190, 20,
                        "ccda", "application/xml", 20, 1, ref, null, null,
                        null, -1, null, null, "0.1.0@x/ccda/1/cd"),
                new ConnectorMessage.ItemSplit(itemId, "claims", 7, 1, 999,
                        "digest", null, null, null, java.time.Instant.EPOCH,
                        java.time.Instant.EPOCH,
                        java.util.List.of("0.1.0@x/ccda/1/cd")),
                new ConnectorMessage.ItemConverted(itemId, "claims", 7, 2,
                        java.time.Instant.EPOCH, java.util.List.of(
                                "0.1.0@x/ccda/2/ab", "0.1.0@y/ccda/2/ab")),
                // Failures still open (STAGES.md §15.6).
                new ConnectorMessage.ItemProgressed(itemId, "claims", "split",
                        7, 5, 2, 0, null, "claims-2026-11.csv",
                        UUID.randomUUID(), java.time.Instant.EPOCH,
                        java.util.List.of("0.1.0@x/ccda/1/cd")),
                new ConnectorMessage.ItemProgressed(itemId, "claims", "split",
                        2, 2, 1, 0, new ConnectorMessage.Problem(
                                "PARSE_XML_MALFORMED", "split", -1, 300,
                                null, -1, null, null),
                        null, null, java.time.Instant.EPOCH, null),
                new ConnectorMessage.ItemResult(itemId, "claims",
                        ConnectorMessage.ItemResult.INCOMPLETE, 7, 2, null, 2,
                        1, 1, java.util.List.of(), null, null,
                        java.time.Instant.EPOCH)))
        {
            assertThat(PayloadCodec.fromJson(PayloadCodec.toJson(message)))
                    .isEqualTo(message);
        }
        assertThat(PayloadCodec.toJson(new RecordStored("ID2", itemId, 1, 30,
                20, "fhir", "application/fhir+json", null, 20, 1, ref)))
                .doesNotContainKey("mapping");
        assertThat(PayloadCodec.toJson(new ConnectorMessage.RecordSplit("ID7",
                itemId, 5, 110, 20, "ccda", "application/xml", 20, 1, ref,
                null, null, null, -1, null, null)))
                .doesNotContainKeys("errorCode", "errorLine", "mapping",
                        "message", "messageRef", "messageReplaces",
                        "version");
        assertThat(PayloadCodec.toJson(new ConnectorMessage.ItemConverted(
                itemId, "claims", 1, 0, java.time.Instant.EPOCH)))
                .doesNotContainKey("versions");
        assertThat(PayloadCodec.toJson(new ConnectorMessage.ItemProgressed(
                itemId, "claims", "convert", 3, 2, 1, 0, null, null, null,
                java.time.Instant.EPOCH, null)))
                .doesNotContainKeys("failure", "producerRef", "replaces",
                        "versions");
        // A change of counts is a new message, the same counts a repeat.
        assertThat(new ConnectorMessage.ItemProgressed(itemId, "claims",
                "convert", 3, 2, 1, 0, null, null, null,
                java.time.Instant.EPOCH, null).messageId())
                .isEqualTo("ip-convert-" + itemId + "-3-2-1-0")
                .isNotEqualTo(new ConnectorMessage.ItemProgressed(itemId,
                        "claims", "convert", 3, 2, 0, 1, null, null, null,
                        java.time.Instant.EPOCH, null).messageId());
        assertThat(new ConnectorMessage.ItemResult(itemId, "claims",
                ConnectorMessage.ItemResult.INCOMPLETE, 3, 1, null, 1, 1, 1,
                java.util.List.of(), null, null, java.time.Instant.EPOCH)
                .messageId()).isNotEqualTo(new ConnectorMessage.ItemResult(
                        itemId, "claims", ConnectorMessage.ItemResult.COMPLETED,
                        3, 1, null, 1, 1, 1, java.util.List.of(), null, null,
                        java.time.Instant.EPOCH).messageId());
        assertThat(new ConnectorMessage.RecordSplit("ID7", itemId, 5, 110, 20,
                "ccda", "application/xml", 20, 1, ref, null, null, null, -1,
                null, null).orderingKey()).isEqualTo("ID7");
        assertThat(new ConnectorMessage.ItemConverted(itemId, "claims", 1, 0,
                java.time.Instant.EPOCH).orderingKey())
                .isEqualTo(itemId.toString());
        assertThat(PayloadCodec.toJson(new ConnectorMessage.ItemReceived(
                itemId, "claims", "push", 1, null, java.time.Instant.EPOCH,
                null, null, null, null)))
                .doesNotContainKeys("sha256", "producerRef", "replaces",
                        "messageIds", "messageReplaces");
    }

    /**
     * An encrypted chunk decrypts to what was encrypted, and only as the
     * chunk and record it was encrypted as.
     */
    @Test
    void chunkRoundTripAndAadBinding()
    {
        byte[] batchDataKey = new byte[32];
        UUID itemId = UUID.randomUUID();
        SecretKey key = RecordCrypto.recordKey(batchDataKey, itemId, 3);
        byte[] plaintext = "<ClinicalDocument/>"
                .getBytes(StandardCharsets.UTF_8);

        byte[] stored = RecordCrypto.encryptChunk(key, itemId, 3, 0, 1,
                plaintext);
        assertThat(RecordCrypto.decryptChunk(key, itemId, 3, 0, 1,
                plaintext.length, stored)).isEqualTo(plaintext);

        // The same bytes presented as another chunk position or another
        // record are rejected.
        assertThatThrownBy(() -> RecordCrypto.decryptChunk(key, itemId, 3, 1,
                2, plaintext.length, stored))
                .isInstanceOf(ChunkIntegrityException.class);
        assertThatThrownBy(() -> RecordCrypto.decryptChunk(
                RecordCrypto.recordKey(batchDataKey, itemId, 4), itemId, 4,
                0, 1, plaintext.length, stored))
                .isInstanceOf(ChunkIntegrityException.class);
    }
}
