/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.runtime.config.Pem;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.interfaces.ECPrivateKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link KeyMaterial}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class KeyMaterialTest
{

    /** Where the key files are written. */
    @TempDir
    Path dir;

    /**
     * Local key settings.
     *
     * @param keys the key files
     * @param recipientPin the recipient key's pinned fingerprint
     * @param signingPem the signing key
     * @return the settings
     * @throws Exception if the settings can't be made
     */
    static CommonSettings.Crypto crypto(TestKeys keys, String recipientPin,
            Path signingPem) throws Exception
    {
        return crypto(keys, recipientPin, signingPem, "local", "local", null);
    }

    /**
     * Key settings with chosen providers.
     *
     * @param keys the key files
     * @param recipientPin the recipient key's pinned fingerprint
     * @param signingPem the signing key
     * @param wrap the KEK provider
     * @param sign the signing provider
     * @param signingKeyRef the KMS signing key, or null for the local one
     * @return the settings
     */
    static CommonSettings.Crypto crypto(TestKeys keys, String recipientPin,
            Path signingPem, String wrap, String sign, String signingKeyRef)
    {
        return new CommonSettings.Crypto(wrap, sign,
                new CommonSettings.Keystore(keys.keystore,
                        TestKeys.KS_PASSWORD),
                keys.splitterKeys(recipientPin, signingPem, signingKeyRef),
                new CommonSettings.Aws(null, null),
                new CommonSettings.Gcp(null),
                new CommonSettings.Vault(null, null, null, "transit", null));
    }

    /**
     * Every key loads, and the KEK wraps and unwraps.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void loadsAllKeys() throws Exception
    {
        TestKeys keys = new TestKeys(dir);
        KeyMaterial material = KeyMaterial.load(crypto(keys,
                KeyMaterial.fingerprint(keys.recipient.getPublic()),
                keys.signingPem));
        assertThat(material.signerKid()).isEqualTo("connector-2026");
        assertThat(material.recipient()).isEqualTo(keys.recipient.getPublic());
        assertThat(material.inbox()).isNotNull();
        byte[] dek = new byte[32];
        assertThat(material.wrap().unwrap(
                material.wrap().wrap(TestKeys.SECTIONS, dek))).isEqualTo(dek);
    }

    /**
     * A keystore or signing key readable by others or writable by the
     * group is refused.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesWorldReadableOrGroupWritableKeystoreAndSigningKey()
            throws Exception
    {
        TestKeys keys = new TestKeys(dir);
        String pin = KeyMaterial.fingerprint(keys.recipient.getPublic());
        Files.setPosixFilePermissions(keys.keystore,
                PosixFilePermissions.fromString("rw-rw----"));
        assertThatThrownBy(() -> KeyMaterial.load(
                crypto(keys, pin, keys.signingPem)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("others or writable by group");
        Files.setPosixFilePermissions(keys.keystore,
                PosixFilePermissions.fromString("rw-------"));
        Files.setPosixFilePermissions(keys.signingPem,
                PosixFilePermissions.fromString("rw----r--"));
        assertThatThrownBy(() -> KeyMaterial.load(
                crypto(keys, pin, keys.signingPem)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("others or writable by group");
    }

    /**
     * Kubernetes secret volumes with an fsGroup: read-only for owner and
     * group (0440).
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void acceptsGroupReadOnlyKeyFiles() throws Exception
    {
        TestKeys keys = new TestKeys(dir);
        String pin = KeyMaterial.fingerprint(keys.recipient.getPublic());
        Files.setPosixFilePermissions(keys.keystore,
                PosixFilePermissions.fromString("r--r-----"));
        Files.setPosixFilePermissions(keys.signingPem,
                PosixFilePermissions.fromString("r--r-----"));
        try (KeyMaterial material =
                KeyMaterial.load(crypto(keys, pin, keys.signingPem)))
        {
            assertThat(material.signerKid()).isNotBlank();
        }
    }

    /**
     * A wrong recipient pin, an unencrypted signing key and an unknown
     * provider are refused.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesWrongPinUnencryptedSigningKeyAndOtherProviders()
            throws Exception
    {
        TestKeys keys = new TestKeys(dir);
        assertThatThrownBy(() -> KeyMaterial.load(
                crypto(keys, "00".repeat(32), keys.signingPem)))
                .isInstanceOf(GeneralSecurityException.class)
                .hasMessageContaining("pinned");

        Path plain = dir.resolve("plain.pem");
        Files.createFile(plain, PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rw-------")));
        Files.writeString(plain, "-----BEGIN PRIVATE KEY-----\nAAAA\n"
                + "-----END PRIVATE KEY-----\n");
        String pin = KeyMaterial.fingerprint(keys.recipient.getPublic());
        assertThatThrownBy(() -> KeyMaterial.load(crypto(keys, pin, plain)))
                .isInstanceOf(GeneralSecurityException.class)
                .hasMessageContaining("encrypted");

        CommonSettings.Crypto crypto = crypto(keys, pin, keys.signingPem);
        assertThatThrownBy(() -> KeyMaterial.load(new CommonSettings.Crypto(
                "hsm-x", "local", crypto.keystore(), crypto.keys(),
                crypto.aws(), crypto.gcp(), crypto.vault())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown key provider");
    }

    /**
     * A key encrypted by {@code openssl pkcs8 -topk8} reads with the right
     * passphrase only. Skipped without openssl.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void readsAnOpensslEncryptedKey() throws Exception
    {
        Path key = dir.resolve("ossl.pem");
        Process process;
        try
        {
            process = new ProcessBuilder("sh", "-c", "openssl genpkey"
                    + " -algorithm EC -pkeyopt ec_paramgen_curve:P-256 "
                    + "| openssl pkcs8 -topk8 -v2 aes-256-cbc -passout pass:pw"
                    + " -out '" + key + "'")
                    .redirectErrorStream(true).start();
        }
        catch (IOException failure)
        {
            assumeTrue(false, "openssl not available");
            return;
        }
        assumeTrue(process.waitFor() == 0, "openssl failed");
        assertThat(Pem.encryptedPrivateKey(key, "pw".toCharArray()))
                .isInstanceOf(ECPrivateKey.class);
        assertThatThrownBy(() -> Pem.encryptedPrivateKey(key,
                "wrong".toCharArray()))
                .isInstanceOf(GeneralSecurityException.class);
    }
}
