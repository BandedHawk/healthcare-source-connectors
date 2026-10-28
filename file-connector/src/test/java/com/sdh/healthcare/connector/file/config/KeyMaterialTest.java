package com.sdh.healthcare.connector.file.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sdh.healthcare.connector.file.TestKeys;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.interfaces.ECPrivateKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KeyMaterialTest {
    @TempDir
    Path dir;

    static ConnectorProperties.Crypto crypto(TestKeys k, String recipientPin, Path signingPem) throws Exception {
        return new ConnectorProperties.Crypto("local", "local",
                new ConnectorProperties.Keystore(k.keystore, TestKeys.KS_PASSWORD),
                new ConnectorProperties.Keys(TestKeys.SECTIONS, TestKeys.SOURCE, TestKeys.FAILED,
                        new ConnectorProperties.Signing("connector-2026", signingPem, TestKeys.SIGNING_PASSPHRASE),
                        new ConnectorProperties.Recipient("downstream-2026", k.recipientPem, recipientPin),
                        k.recoveryPem));
    }

    @Test
    void loadsAllKeys() throws Exception {
        TestKeys k = new TestKeys(dir);
        KeyMaterial m = KeyMaterial.load(crypto(k, KeyMaterial.fingerprint(k.recipient.getPublic()), k.signingPem));
        assertThat(m.signerKid()).isEqualTo("connector-2026");
        assertThat(m.recipient()).isEqualTo(k.recipient.getPublic());
        assertThat(m.recovery()).isEqualTo(k.recovery.getPublic());
        byte[] dek = new byte[32];
        assertThat(m.wrap().unwrap(m.wrap().wrap(TestKeys.SECTIONS, dek))).isEqualTo(dek);
    }

    @Test
    void refusesGroupReadableKeystoreAndSigningKey() throws Exception {
        TestKeys k = new TestKeys(dir);
        String pin = KeyMaterial.fingerprint(k.recipient.getPublic());
        Files.setPosixFilePermissions(k.keystore, PosixFilePermissions.fromString("rw-r-----"));
        assertThatThrownBy(() -> KeyMaterial.load(crypto(k, pin, k.signingPem)))
                .isInstanceOf(IOException.class).hasMessageContaining("group or others");
        Files.setPosixFilePermissions(k.keystore, PosixFilePermissions.fromString("rw-------"));
        Files.setPosixFilePermissions(k.signingPem, PosixFilePermissions.fromString("rw----r--"));
        assertThatThrownBy(() -> KeyMaterial.load(crypto(k, pin, k.signingPem)))
                .isInstanceOf(IOException.class).hasMessageContaining("group or others");
    }

    @Test
    void refusesWrongPinUnencryptedSigningKeyAndOtherProviders() throws Exception {
        TestKeys k = new TestKeys(dir);
        assertThatThrownBy(() -> KeyMaterial.load(crypto(k, "00".repeat(32), k.signingPem)))
                .isInstanceOf(GeneralSecurityException.class).hasMessageContaining("pinned");

        Path plain = dir.resolve("plain.pem");
        Files.createFile(plain, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        Files.writeString(plain, "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n");
        String pin = KeyMaterial.fingerprint(k.recipient.getPublic());
        assertThatThrownBy(() -> KeyMaterial.load(crypto(k, pin, plain)))
                .isInstanceOf(GeneralSecurityException.class).hasMessageContaining("encrypted");

        ConnectorProperties.Crypto c = crypto(k, pin, k.signingPem);
        assertThatThrownBy(() -> KeyMaterial.load(new ConnectorProperties.Crypto("aws-kms", "local", c.keystore(),
                c.keys()))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("local");
    }

    @Test
    void readsAnOpensslEncryptedKey() throws Exception {
        Path key = dir.resolve("ossl.pem");
        Process p;
        try {
            p = new ProcessBuilder("sh", "-c", "openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 "
                    + "| openssl pkcs8 -topk8 -v2 aes-256-cbc -passout pass:pw -out '" + key + "'")
                    .redirectErrorStream(true).start();
        } catch (IOException e) {
            assumeTrue(false, "openssl not available");
            return;
        }
        assumeTrue(p.waitFor() == 0, "openssl failed");
        assertThat(Pem.encryptedPrivateKey(key, "pw".toCharArray())).isInstanceOf(ECPrivateKey.class);
        assertThatThrownBy(() -> Pem.encryptedPrivateKey(key, "wrong".toCharArray()))
                .isInstanceOf(GeneralSecurityException.class);
    }
}
