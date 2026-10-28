package com.sdh.healthcare.connector.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
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

class ContractRoundTripTest {

    static KeyPair ec() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp256r1"));
        return g.generateKeyPair();
    }

    @Test
    void sealedMessageOpensAndRejectsUntrustedSigner() throws Exception {
        KeyPair signer = ec();
        KeyPair recipient = ec();
        KeyPair stranger = ec();
        SectionStored msg = new SectionStored("ABC", UUID.randomUUID(), 7, 100, 50, "ccda", 50, 1,
                new StorageRef("cat", "sdh_ingest", "sections", LocalDate.of(2026, 10, 28), 42L, "kek-1"));

        byte[] body = new MessageSealer(new ECDSASigner((ECPrivateKey) signer.getPrivate()), "sig-1",
                recipient.getPublic(), "rcp-1").seal(msg);

        MessageOpener opener = new MessageOpener(Map.of("rcp-1", recipient.getPrivate()),
                Map.of("sig-1", (ECPublicKey) signer.getPublic()));
        ConnectorMessage opened = opener.open(body);
        assertThat(opened).isEqualTo(msg);

        byte[] forged = new MessageSealer(new ECDSASigner((ECPrivateKey) stranger.getPrivate()), "sig-1",
                recipient.getPublic(), "rcp-1").seal(msg);
        assertThatThrownBy(() -> opener.open(forged))
                .isInstanceOf(MessageRejectedException.class)
                .extracting(e -> ((MessageRejectedException) e).reason())
                .isEqualTo(MessageRejectedException.Reason.BAD_SIGNATURE);
    }

    @Test
    void chunkRoundTripAndAadBinding() {
        byte[] bdk = new byte[32];
        UUID fileId = UUID.randomUUID();
        SecretKey k = SectionCrypto.sectionKey(bdk, fileId, 3);
        byte[] plain = "<ClinicalDocument/>".getBytes(StandardCharsets.UTF_8);

        byte[] stored = SectionCrypto.encryptChunk(k, fileId, 3, 0, 1, plain);
        assertThat(SectionCrypto.decryptChunk(k, fileId, 3, 0, 1, plain.length, stored)).isEqualTo(plain);

        // Same bytes presented as another chunk position or another section are rejected.
        assertThatThrownBy(() -> SectionCrypto.decryptChunk(k, fileId, 3, 1, 2, plain.length, stored))
                .isInstanceOf(ChunkIntegrityException.class);
        assertThatThrownBy(() -> SectionCrypto.decryptChunk(
                SectionCrypto.sectionKey(bdk, fileId, 4), fileId, 4, 0, 1, plain.length, stored))
                .isInstanceOf(ChunkIntegrityException.class);
    }
}
