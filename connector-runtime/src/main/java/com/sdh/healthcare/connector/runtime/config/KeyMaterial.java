/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.runtime.config;

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
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.Set;
import javax.crypto.SecretKey;

/**
 * Keys for the {@code local} provider (design §7.4): a PKCS#12 keystore with the AES-256 KEKs, an encrypted PKCS#8
 * PEM for the ES256 signing key, the recipient public key (fingerprint pinned), and for the splitter the opener of
 * {@code item.landed} messages (its own inbox key and the landing node's signing key, design §5.3.6).
 *
 * <p>Wrapping and signing can each use {@code local} or a KMS ({@code aws-kms}, {@code gcp-kms}, {@code azure-keyvault},
 * {@code vault-transit}). Every KEK is probed with a wrap/unwrap at startup, and a KMS signing key with a sign/verify.
 * Locally, startup is refused if the keystore or signing key is world-accessible, group-writable or group-executable,
 * or the signing key isn't encrypted. Group read is allowed: Kubernetes secret volumes with an {@code fsGroup} are
 * always at least 0440. The recipient key must match its pinned fingerprint. Public key files are not checked for permissions.
 *
 * @param wrap         wraps and unwraps data keys with the KEKs
 * @param signer       signs messages
 * @param signerKid    the signing key's ID
 * @param recipient    the public key messages are sealed to
 * @param recipientKid the recipient key's ID
 * @param inbox        opens {@code item.landed} messages; null for a landing node
 * @param resources    closed with the key material
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record KeyMaterial(KeyProvider wrap, JWSSigner signer, String signerKid, PublicKey recipient,
        String recipientKid, MessageOpener inbox, List<AutoCloseable> resources) implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(KeyMaterial.class);

    /** Group read is allowed: Kubernetes adds it (mode | 0440) to secret files when the pod sets an fsGroup. */
    private static final Set<PosixFilePermission> GROUP_OR_OTHER = EnumSet.of(
            PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE);

    /** Builds a KMS provider by name. Replaceable in tests to point at emulators. */
    @FunctionalInterface
    public interface Providers {
        KeyProvider create(String name, CommonSettings.Crypto c);

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

    static Supplier<String> vaultToken(CommonSettings.Vault v) {
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

    /** The splitter's keys: the sections KEK, the landing KEK (unwrap only), signing, recipient and inbox keys. */
    public static KeyMaterial load(CommonSettings.Crypto c)
            throws IOException, GeneralSecurityException, com.nimbusds.jose.JOSEException {
        return load(c, Providers.DEFAULT);
    }

    /**
     * The splitter's keys, with KMS providers from {@code providers}. The sections KEK is probed with a wrap and
     * unwrap. The landing KEK is only ever unwrapped with, so it isn't probed: reading the landing ID key at startup
     * checks it.
     */
    public static KeyMaterial load(CommonSettings.Crypto c, Providers providers)
            throws IOException, GeneralSecurityException, com.nimbusds.jose.JOSEException {
        CommonSettings.Keys k = c.keys();
        String sections = require(k.sectionsKek(), "sdh.crypto.keys.sections-kek");
        String landed = require(k.landedItemsKek(), "sdh.crypto.keys.landed-items-kek");
        if (sections.equals(landed)) {
            throw new IllegalArgumentException("The sections KEK must differ from the landing KEK (design §7.1)");
        }
        return load(c, providers, List.of(sections), List.of(landed), true);
    }

    /**
     * A landing node's keys (design §5.3): the {@code landed_items} KEK, and the signing and recipient keys for
     * {@code item.landed}. It opens no messages, so {@link #inbox()} is null.
     */
    public static KeyMaterial forLanding(CommonSettings.Crypto c, Providers providers)
            throws IOException, GeneralSecurityException, com.nimbusds.jose.JOSEException {
        String landed = require(c.keys().landedItemsKek(), "sdh.crypto.keys.landed-items-kek");
        return load(c, providers, List.of(landed), List.of(), false);
    }

    /**
     * Loads and checks the keys.
     *
     * @param probed     KEKs wrapped and unwrapped with, probed at startup
     * @param unwrapOnly KEKs only unwrapped with, not probed
     * @param inbox      whether the splitter's inbox key and the landing node's signing key are required
     */
    private static KeyMaterial load(CommonSettings.Crypto c, Providers providers, List<String> probed,
            List<String> unwrapOnly, boolean inbox)
            throws IOException, GeneralSecurityException, com.nimbusds.jose.JOSEException {
        CommonSettings.Keys k = c.keys();
        List<String> all = new ArrayList<>(probed);
        all.addAll(unwrapOnly);
        String[] kekRefs = all.toArray(String[]::new);
        List<AutoCloseable> resources = new ArrayList<>();
        Map<String, KeyProvider> byName = new LinkedHashMap<>();
        try {
            KeyProvider wrap = "local".equals(c.wrapProvider()) ? localKeks(c, kekRefs)
                    : byName.computeIfAbsent(c.wrapProvider(), n -> track(providers.create(n, c), resources));
            for (String ref : probed) {
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

            PublicKey recipient = pinnedKey(k.recipient(), "sdh.crypto.keys.recipient");
            MessageOpener opener = inbox ? inboxOpener(k) : null;
            return new KeyMaterial(wrap, signer, k.signing().kid(), recipient,
                    require(k.recipient().kid(), "sdh.crypto.keys.recipient.kid"), opener, List.copyOf(resources));
        } catch (Exception | Error e) {
            closeAll(resources);
            throw e;
        }
    }

    /**
     * Loads a recipient's public key and checks it against its pinned fingerprint.
     *
     * @param recipient the key's settings
     * @param prefix    the settings' name, for errors
     * @return the key
     * @throws IOException              if the PEM can't be read
     * @throws GeneralSecurityException if the key isn't the one pinned
     */
    public static PublicKey pinnedKey(CommonSettings.Recipient recipient, String prefix)
            throws IOException, GeneralSecurityException {
        PublicKey key = Pem.publicKey(require(recipient.pem(), prefix + ".pem"));
        String pinned = require(recipient.sha256(), prefix + ".sha256").replace(":", "").toLowerCase(Locale.ROOT);
        if (!fingerprint(key).equals(pinned)) {
            throw new GeneralSecurityException("Key " + prefix + " does not match its pinned SHA-256 fingerprint");
        }
        return key;
    }

    /** Opens {@code item.landed}: the splitter's own private key, and the landing node's signing public key. */
    private static MessageOpener inboxOpener(CommonSettings.Keys k) throws IOException, GeneralSecurityException {
        Path pem = require(k.inbox().pem(), "sdh.crypto.keys.inbox.pem");
        checkPrivate(pem);
        PrivateKey key = Pem.encryptedPrivateKey(pem,
                require(k.inbox().passphrase(), "sdh.crypto.keys.inbox.passphrase").toCharArray());
        if (!(key instanceof ECPrivateKey ec) || !isP256(ec.getParams())) {
            throw new GeneralSecurityException("Inbox key must be EC P-256");
        }
        PublicKey signer = Pem.publicKey(require(k.landingSigner().pem(), "sdh.crypto.keys.landing-signer.pem"));
        if (!(signer instanceof ECPublicKey ecSigner) || !isP256(ecSigner.getParams())) {
            throw new GeneralSecurityException("Landing signing key must be EC P-256 (ES256)");
        }
        return new MessageOpener(Map.of(require(k.inbox().kid(), "sdh.crypto.keys.inbox.kid"), key),
                Map.of(require(k.landingSigner().kid(), "sdh.crypto.keys.landing-signer.kid"), ecSigner));
    }

    private static KeyProvider localKeks(CommonSettings.Crypto c, String[] refs)
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
     * permissions and a passphrase. Used by operator tools for the recovery key of old fallback files (design §11.4).
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
