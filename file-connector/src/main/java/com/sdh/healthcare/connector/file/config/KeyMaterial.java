package com.sdh.healthcare.connector.file.config;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.kms.AwsKmsKeyProvider;
import com.sdh.healthcare.connector.kms.AzureKeyVaultKeyProvider;
import com.sdh.healthcare.connector.kms.GcpKmsKeyProvider;
import com.sdh.healthcare.connector.kms.KmsJwsSigner;
import com.sdh.healthcare.connector.kms.VaultTransitKeyProvider;
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
import java.security.SecureRandom;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECParameterSpec;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.Set;
import javax.crypto.SecretKey;

/**
 * Keys for the {@code local} provider (design §7.4): a PKCS#12 keystore with the AES-256 KEKs, an encrypted PKCS#8
 * PEM for the ES256 signing key, the recipient public key (fingerprint pinned) and the recovery public key.
 *
 * <p>Wrapping and signing can each use {@code local} or a KMS ({@code aws-kms}, {@code gcp-kms}, {@code azure-keyvault},
 * {@code vault-transit}). Every KEK is probed with a wrap/unwrap at startup, and a KMS signing key with a sign/verify.
 * Locally, startup is refused if the keystore or signing key is world-accessible, group-writable or group-executable,
 * or the signing key isn't encrypted. Group read is allowed: Kubernetes secret volumes with an {@code fsGroup} are
 * always at least 0440. The recipient key must match its pinned fingerprint. Public key files are not checked for permissions.
 */
public record KeyMaterial(KeyProvider wrap, JWSSigner signer, String signerKid, PublicKey recipient,
        String recipientKid, ECPublicKey recovery, List<AutoCloseable> resources) implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(KeyMaterial.class);

    /** Group read is allowed: Kubernetes adds it (mode | 0440) to secret files when the pod sets an fsGroup. */
    private static final Set<PosixFilePermission> GROUP_OR_OTHER = EnumSet.of(
            PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE);

    /** Builds a KMS provider by name. Replaceable in tests to point at emulators. */
    @FunctionalInterface
    public interface Providers {
        KeyProvider create(String name, ConnectorProperties.Crypto c);

        Providers DEFAULT = (name, c) -> switch (name) {
            case "aws-kms" -> AwsKmsKeyProvider.create(Optional.ofNullable(c.aws().region()),
                    Optional.ofNullable(c.aws().endpoint()));
            case "gcp-kms" -> GcpKmsKeyProvider.create(Optional.ofNullable(c.gcp().endpoint()));
            case "azure-keyvault" -> AzureKeyVaultKeyProvider.create();
            case "vault-transit" -> VaultTransitKeyProvider.create(
                    require(c.vault().address(), "sdh.crypto.vault.address"), c.vault().mount(), vaultToken(c.vault()),
                    Optional.ofNullable(c.vault().namespace()));
            default -> throw new IllegalArgumentException("Unknown key provider '" + name
                    + "'; one of local, aws-kms, gcp-kms, azure-keyvault, vault-transit");
        };
    }

    static Supplier<String> vaultToken(ConnectorProperties.Vault v) {
        if (v.tokenFile() != null) {
            return () -> {
                try {
                    return Files.readString(v.tokenFile()).trim();
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException("Vault token file unreadable", e);
                }
            };
        }
        String token = require(v.token(), "sdh.crypto.vault.token or sdh.crypto.vault.token-file");
        return () -> token;
    }

    public static KeyMaterial load(ConnectorProperties.Crypto c)
            throws IOException, GeneralSecurityException, com.nimbusds.jose.JOSEException {
        return load(c, Providers.DEFAULT);
    }

    public static KeyMaterial load(ConnectorProperties.Crypto c, Providers providers)
            throws IOException, GeneralSecurityException, com.nimbusds.jose.JOSEException {
        ConnectorProperties.Keys k = c.keys();
        String[] kekRefs = {
                require(k.sectionsKek(), "sdh.crypto.keys.sections-kek"),
                require(k.sourceFilesKek(), "sdh.crypto.keys.source-files-kek"),
                require(k.failedFilesKek(), "sdh.crypto.keys.failed-files-kek")};
        if (k.sectionsKek().equals(k.sourceFilesKek()) || k.sectionsKek().equals(k.failedFilesKek())) {
            throw new IllegalArgumentException("The sections KEK must differ from the archive KEKs (design §7.1)");
        }
        List<AutoCloseable> resources = new ArrayList<>();
        Map<String, KeyProvider> byName = new LinkedHashMap<>();
        try {
            KeyProvider wrap = "local".equals(c.wrapProvider()) ? localKeks(c, kekRefs)
                    : byName.computeIfAbsent(c.wrapProvider(), n -> track(providers.create(n, c), resources));
            for (String ref : kekRefs) {
                probeKek(wrap, ref);
            }

            JWSSigner signer;
            PublicKey signingPublic;
            if ("local".equals(c.signProvider())) {
                Path signingPem = require(k.signing().pem(), "sdh.crypto.keys.signing.pem");
                checkPrivate(signingPem);
                PrivateKey signing = Pem.encryptedPrivateKey(signingPem,
                        require(k.signing().passphrase(), "sdh.crypto.keys.signing.passphrase").toCharArray());
                if (!(signing instanceof ECPrivateKey ec) || !isP256(ec.getParams())) {
                    throw new GeneralSecurityException("Signing key must be EC P-256 (ES256)");
                }
                signer = new ECDSASigner(ec);
                signingPublic = null;
            } else {
                KeyProvider kms = byName.computeIfAbsent(c.signProvider(),
                        n -> track(providers.create(n, c), resources));
                String keyRef = require(k.signing().keyRef(), "sdh.crypto.keys.signing.key-ref");
                signingPublic = kms.publicKey(keyRef);
                if (!(signingPublic instanceof ECPublicKey ec) || !isP256(ec.getParams())) {
                    throw new GeneralSecurityException("Signing key " + keyRef + " must be EC P-256 (ES256)");
                }
                signer = new KmsJwsSigner(kms, keyRef);
                probeSigner(signer, ec);
                LOG.info("crypto.signing_key provider={} kid={} sha256={} (configure readers with this public key)",
                        c.signProvider(), k.signing().kid(), fingerprint(signingPublic));
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
            return new KeyMaterial(wrap, signer, k.signing().kid(), recipient,
                    require(k.recipient().kid(), "sdh.crypto.keys.recipient.kid"), rec, List.copyOf(resources));
        } catch (Exception | Error e) {
            closeAll(resources);
            throw e;
        }
    }

    private static KeyProvider localKeks(ConnectorProperties.Crypto c, String[] refs)
            throws IOException, GeneralSecurityException {
        Path ksPath = require(c.keystore().path(), "sdh.crypto.keystore.path");
        checkPrivate(ksPath);
        char[] ksPassword = require(c.keystore().password(), "sdh.crypto.keystore.password").toCharArray();
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(ksPath)) {
            ks.load(in, ksPassword);
        }
        Map<String, SecretKey> keks = new LinkedHashMap<>();
        for (String ref : refs) {
            if (!(ks.getKey(ref, ksPassword) instanceof SecretKey key) || !"AES".equalsIgnoreCase(key.getAlgorithm())
                    || key.getEncoded().length != 32) {
                throw new GeneralSecurityException("Keystore entry '" + ref + "' must be an AES-256 secret key");
            }
            keks.put(ref, key);
        }
        return new AesKeyWrapProvider(keks);
    }

    /** Wrap and unwrap a throwaway key, so a missing KEK or permission stops startup rather than the first file. */
    static void probeKek(KeyProvider p, String ref) throws GeneralSecurityException {
        byte[] probe = new byte[32];
        new SecureRandom().nextBytes(probe);
        try {
            WrappedKey w = p.wrap(ref, probe);
            if (!MessageDigest.isEqual(probe, p.unwrap(w))) {
                throw new GeneralSecurityException("KEK " + ref + " did not round-trip");
            }
        } catch (RuntimeException e) {
            throw new GeneralSecurityException("KEK " + ref + " is not usable: " + e.getMessage(), e);
        }
    }

    /** Sign a probe through the KMS and verify it with the key's public half. */
    static void probeSigner(JWSSigner signer, ECPublicKey publicKey) throws com.nimbusds.jose.JOSEException,
            GeneralSecurityException {
        JWSObject jws = new JWSObject(new JWSHeader(JWSAlgorithm.ES256), new Payload("sdh-signing-probe"));
        jws.sign(signer);
        if (!jws.verify(new ECDSAVerifier(publicKey))) {
            throw new GeneralSecurityException("KMS signature did not verify with the key's public key");
        }
    }

    private static KeyProvider track(KeyProvider p, List<AutoCloseable> resources) {
        if (p instanceof AutoCloseable c) {
            resources.add(c);
        }
        return p;
    }

    private static void closeAll(List<AutoCloseable> resources) {
        for (AutoCloseable r : resources) {
            try {
                r.close();
            } catch (Exception ignored) {
                // shutting down
            }
        }
    }

    /** Closes KMS clients. */
    @Override
    public void close() {
        closeAll(resources);
    }

    /** Lower-case hex SHA-256 of the key's SubjectPublicKeyInfo, as pinned in configuration. */
    public static String fingerprint(PublicKey key) throws GeneralSecurityException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(key.getEncoded()));
    }

    /**
     * Reads an encrypted PKCS#8 private key under the same rules as the signing key: owner-only (or group-read)
     * permissions and a passphrase. Used by operator tools for the recovery key (design §11.4).
     */
    public static PrivateKey loadPrivateKey(Path pem, char[] passphrase) throws IOException, GeneralSecurityException {
        checkPrivate(pem);
        return Pem.encryptedPrivateKey(pem, passphrase);
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
                        + " must not be accessible by others or writable by group (chmod 600 or 640)");
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
