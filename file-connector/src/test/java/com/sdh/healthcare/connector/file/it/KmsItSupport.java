package com.sdh.healthcare.connector.file.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.config.KeyMaterial;
import java.nio.file.Files;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.interfaces.ECPublicKey;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Shared checks for the KMS provider integration tests. */
final class KmsItSupport {
    private KmsItSupport() {}

    static TestKeys localKeys() throws Exception {
        return new TestKeys(Files.createTempDirectory("sdh-kms-it"));
    }

    static ConnectorProperties.Crypto crypto(TestKeys k, String provider, String sectionsKek, String sourceKek,
            String failedKek, String signingKeyRef, ConnectorProperties.Aws aws, ConnectorProperties.Vault vault)
            throws Exception {
        return new ConnectorProperties.Crypto(provider, provider,
                new ConnectorProperties.Keystore(null, null),
                new ConnectorProperties.Keys(sectionsKek, sourceKek, failedKek,
                        new ConnectorProperties.Signing("connector-kms", null, null, signingKeyRef),
                        new ConnectorProperties.Recipient("downstream-2026", k.recipientPem,
                                KeyMaterial.fingerprint(k.recipient.getPublic())),
                        k.recoveryPem),
                aws, new ConnectorProperties.Gcp(null), vault);
    }

    /**
     * What the connector relies on from a KMS: KEKs wrap and unwrap (load() already probed each), tampering is
     * refused, and messages signed through the KMS verify with the KMS's public key.
     */
    static void assertUsable(KeyMaterial km, TestKeys k, String kek, PublicKey signingPublic) throws Exception {
        byte[] dek = new byte[32];
        new SecureRandom().nextBytes(dek);
        WrappedKey w = km.wrap().wrap(kek, dek);
        assertThat(km.wrap().unwrap(w)).isEqualTo(dek);
        // Flip the last byte: the authentication tag (AES-GCM) or the end of the RSA-OAEP block. Header bytes (key ID,
        // version) aren't necessarily authenticated, so tampering there proves nothing.
        byte[] bad = w.wrapped().clone();
        bad[bad.length - 1] ^= 1;
        assertThatThrownBy(() -> km.wrap().unwrap(new WrappedKey(w.keyRef(), bad)))
                .isInstanceOf(RuntimeException.class);

        UUID fileId = UUID.randomUUID();
        byte[] body = new MessageSealer(km.signer(), km.signerKid(), km.recipient(), km.recipientKid())
                .seal(new ConnectorMessage.FileStarted(fileId, "csv", 42, Instant.now(), "file-connector"));
        ConnectorMessage opened = new MessageOpener(Map.of("downstream-2026", k.recipient.getPrivate()),
                Map.of("connector-kms", (ECPublicKey) signingPublic)).open(body);
        assertThat(opened.fileId()).isEqualTo(fileId);
    }
}
