package com.sdh.healthcare.connector.file.config;

import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECParameterSpec;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.crypto.SecretKey;

/**
 * Keys for the {@code local} provider (design §7.4): a PKCS#12 keystore with the AES-256 KEKs, an encrypted PKCS#8
 * PEM for the ES256 signing key, the recipient public key (fingerprint pinned) and the recovery public key.
 *
 * <p>Refuses to start if the keystore or signing key is group- or world-accessible, if the signing key isn't
 * encrypted, or if the recipient key doesn't match its pinned fingerprint. Public key files are not checked for
 * permissions. KMS providers are not implemented yet.
 */
public record KeyMaterial(KeyProvider wrap, JWSSigner signer, String signerKid, PublicKey recipient,
        String recipientKid, ECPublicKey recovery) {

    private static final Set<PosixFilePermission> GROUP_OR_OTHER = EnumSet.of(
            PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE);

    public static KeyMaterial load(ConnectorProperties.Crypto c)
            throws IOException, GeneralSecurityException, com.nimbusds.jose.JOSEException {
        if (!"local".equals(c.wrapProvider()) || !"local".equals(c.signProvider())) {
            throw new IllegalArgumentException("Only the 'local' key provider is implemented so far (got wrap="
                    + c.wrapProvider() + ", sign=" + c.signProvider() + ")");
        }
        ConnectorProperties.Keys k = c.keys();

        Path ksPath = require(c.keystore().path(), "sdh.crypto.keystore.path");
        checkPrivate(ksPath);
        char[] ksPassword = require(c.keystore().password(), "sdh.crypto.keystore.password").toCharArray();
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(ksPath)) {
            ks.load(in, ksPassword);
        }
        Map<String, SecretKey> keks = new LinkedHashMap<>();
        for (String ref : new String[] {
                require(k.sectionsKek(), "sdh.crypto.keys.sections-kek"),
                require(k.sourceFilesKek(), "sdh.crypto.keys.source-files-kek"),
                require(k.failedFilesKek(), "sdh.crypto.keys.failed-files-kek")}) {
            if (!(ks.getKey(ref, ksPassword) instanceof SecretKey key) || !"AES".equalsIgnoreCase(key.getAlgorithm())
                    || key.getEncoded().length != 32) {
                throw new GeneralSecurityException("Keystore entry '" + ref + "' must be an AES-256 secret key");
            }
            keks.put(ref, key);
        }
        if (k.sectionsKek().equals(k.sourceFilesKek()) || k.sectionsKek().equals(k.failedFilesKek())) {
            throw new IllegalArgumentException("The sections KEK must differ from the archive KEKs (design §7.1)");
        }

        Path signingPem = require(k.signing().pem(), "sdh.crypto.keys.signing.pem");
        checkPrivate(signingPem);
        PrivateKey signing = Pem.encryptedPrivateKey(signingPem,
                require(k.signing().passphrase(), "sdh.crypto.keys.signing.passphrase").toCharArray());
        if (!(signing instanceof ECPrivateKey ec) || !isP256(ec.getParams())) {
            throw new GeneralSecurityException("Signing key must be EC P-256 (ES256)");
        }

        Path recipientPem = require(k.recipient().pem(), "sdh.crypto.keys.recipient.pem");
        PublicKey recipient = Pem.publicKey(recipientPem);
        String pinned = require(k.recipient().sha256(), "sdh.crypto.keys.recipient.sha256")
                .replace(":", "").toLowerCase(Locale.ROOT);
        if (!fingerprint(recipient).equals(pinned)) {
            throw new GeneralSecurityException("Recipient key does not match its pinned SHA-256 fingerprint");
        }

        PublicKey recovery = Pem.publicKey(require(k.recoveryPublic(), "sdh.crypto.keys.recovery-public"));
        if (!(recovery instanceof ECPublicKey rec)) {
            throw new GeneralSecurityException("Recovery key must be an EC public key");
        }
        return new KeyMaterial(new AesKeyWrapProvider(keks), new ECDSASigner((ECPrivateKey) signing),
                k.signing().kid(), recipient, require(k.recipient().kid(), "sdh.crypto.keys.recipient.kid"), rec);
    }

    /** Lower-case hex SHA-256 of the key's SubjectPublicKeyInfo, as pinned in configuration. */
    public static String fingerprint(PublicKey key) throws GeneralSecurityException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(key.getEncoded()));
    }

    static void checkPrivate(Path file) throws IOException {
        if (!Files.isRegularFile(file)) {
            throw new IOException("Key file not found: " + file.getFileName());
        }
        if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(file);
            perms.retainAll(GROUP_OR_OTHER);
            if (!perms.isEmpty()) {
                throw new IOException("Key file " + file.getFileName()
                        + " must not be accessible by group or others (chmod 600)");
            }
        }
    }

    private static boolean isP256(ECParameterSpec p) {
        return p.getCurve().getField().getFieldSize() == 256 && p.getCofactor() == 1
                && p.getOrder().bitLength() == 256;
    }

    static <T> T require(T value, String name) {
        if (value == null || (value instanceof String s && s.isBlank())) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}
