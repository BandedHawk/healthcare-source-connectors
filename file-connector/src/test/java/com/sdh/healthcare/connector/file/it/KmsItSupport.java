/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.nio.file.Files;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.interfaces.ECPublicKey;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Shared checks for the KMS provider integration tests.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class KmsItSupport
{

    /** Not instantiable. */
    private KmsItSupport()
    {
    }

    /**
     * Local key files for what stays local: the recipient, inbox and
     * landing signer keys.
     *
     * @return the key files, in a new temporary directory
     * @throws Exception if the keys can't be made
     */
    static TestKeys localKeys() throws Exception
    {
        return new TestKeys(Files.createTempDirectory("sdh-kms-it"));
    }

    /**
     * Key settings with the KEKs and signing key in a KMS.
     *
     * @param keys the local key files
     * @param provider the KMS provider, for both wrapping and signing
     * @param sectionsKek the sections KEK's reference
     * @param landedKek the landed items KEK's reference
     * @param signingKeyRef the signing key's reference
     * @param aws the AWS settings
     * @param vault the Vault settings
     * @return the settings
     * @throws Exception if the recipient key's fingerprint can't be made
     */
    static CommonSettings.Crypto crypto(TestKeys keys, String provider,
            String sectionsKek, String landedKek, String signingKeyRef,
            CommonSettings.Aws aws, CommonSettings.Vault vault)
            throws Exception
    {
        return new CommonSettings.Crypto(provider, provider,
                new CommonSettings.Keystore(null, null),
                new CommonSettings.Keys(sectionsKek, landedKek,
                        new CommonSettings.Signing("connector-kms", null,
                                null, signingKeyRef),
                        new CommonSettings.Recipient("downstream-2026",
                                keys.recipientPem, KeyMaterial.fingerprint(
                                        keys.recipient.getPublic())),
                        new CommonSettings.InboxKey(TestKeys.INBOX_KID,
                                keys.inboxPem, TestKeys.INBOX_PASSPHRASE),
                        new CommonSettings.Signer(TestKeys.LANDING_SIGNER_KID,
                                keys.landingSignerPem)),
                aws, new CommonSettings.Gcp(null), vault);
    }

    /**
     * What the connector relies on from a KMS: KEKs wrap and unwrap (load()
     * already probed each), tampering is refused, and messages signed
     * through the KMS verify with the KMS's public key.
     *
     * @param material the loaded key material
     * @param keys the local key files
     * @param kek the KEK to wrap with
     * @param signingPublic the KMS signing key's public half
     * @throws Exception if a key operation fails unexpectedly
     */
    static void assertUsable(KeyMaterial material, TestKeys keys, String kek,
            PublicKey signingPublic) throws Exception
    {
        byte[] dek = new byte[32];
        new SecureRandom().nextBytes(dek);
        WrappedKey wrapped = material.wrap().wrap(kek, dek);
        assertThat(material.wrap().unwrap(wrapped)).isEqualTo(dek);
        // Flip the last byte: the authentication tag (AES-GCM) or the end of
        // the RSA-OAEP block. Header bytes (key ID, version) aren't
        // necessarily authenticated, so tampering there proves nothing.
        byte[] bad = wrapped.wrapped().clone();
        bad[bad.length - 1] ^= 1;
        assertThatThrownBy(() -> material.wrap().unwrap(
                new WrappedKey(wrapped.keyRef(), bad)))
                .isInstanceOf(RuntimeException.class);

        UUID fileId = UUID.randomUUID();
        byte[] body = new MessageSealer(material.signer(),
                material.signerKid(), material.recipient(),
                material.recipientKid())
                .seal(new ConnectorMessage.FileStarted(fileId, "csv", 42,
                        Instant.now(), "file-connector"));
        ConnectorMessage opened = new MessageOpener(
                Map.of("downstream-2026", keys.recipient.getPrivate()),
                Map.of("connector-kms", (ECPublicKey) signingPublic))
                .open(body);
        assertThat(opened.fileId()).isEqualTo(fileId);
    }
}
