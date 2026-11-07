/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.config;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.core.pipeline.Outage;
import com.sdh.healthcare.connector.kms.AwsKmsKeyProvider;
import com.sdh.healthcare.connector.kms.AzureKeyVaultKeyProvider;
import com.sdh.healthcare.connector.kms.GcpKmsKeyProvider;
import com.sdh.healthcare.connector.kms.KmsJwsSigner;
import com.sdh.healthcare.connector.kms.VaultTransitKeyProvider;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.crypto.SecretKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A node's keys (design §7.4): the AES-256 KEKs, the ES256 signing key, the
 * recipient's public key (fingerprint pinned), and for the splitter the
 * opener of {@code item.landed} messages (its own inbox key and the landing
 * node's signing key, design §5.3.6).
 * <p>
 * Wrapping and signing can each use {@code local} (a PKCS#12 keystore, and
 * an encrypted PKCS#8 PEM) or a KMS ({@code aws-kms}, {@code gcp-kms},
 * {@code azure-keyvault}, {@code vault-transit}). Every KEK is probed with
 * a wrap and unwrap at startup, and a KMS signing key with a sign and
 * verify. Locally, startup is refused if the keystore or signing key is
 * accessible by others, writable or executable by group, or the signing
 * key isn't encrypted. Group read is allowed: Kubernetes secret volumes
 * with an {@code fsGroup} are always at least 0440. The recipient key must
 * match its pinned fingerprint. Public key files are not checked for
 * permissions.
 * <p>
 * A KMS that is down or not answering at startup ({@link Outage}) is
 * waited for, trying again with a growing pause, rather than startup
 * failing: a node started during an outage is then ready as soon as the KMS
 * is back, not after its orchestrator's restart delays (design revision
 * (ca)). Any other failure, a missing key or permission among them, still
 * refuses startup at once.
 *
 * @param wrap         wraps and unwraps data keys with the KEKs
 * @param signer       signs messages
 * @param signerKid    the signing key's ID
 * @param recipient    the public key messages are sealed to
 * @param recipientKid the recipient key's ID
 * @param inbox        opens {@code item.landed} messages; null for a
 *                     landing node
 * @param resources    closed with the key material
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record KeyMaterial(KeyProvider wrap, JWSSigner signer,
        String signerKid, PublicKey recipient, String recipientKid,
        MessageOpener inbox, List<AutoCloseable> resources)
        implements AutoCloseable
{

    /** Logs the KMS signing key's fingerprint, for readers. */
    private static final Logger LOG =
            LoggerFactory.getLogger(KeyMaterial.class);

    /**
     * Permissions a private key file must not have. Group read is allowed:
     * Kubernetes adds it (mode | 0440) to secret files when the pod sets an
     * {@code fsGroup}.
     */
    private static final Set<PosixFilePermission> GROUP_OR_OTHER = EnumSet.of(
            PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE,
            PosixFilePermission.OTHERS_EXECUTE);

    /** Builds a KMS provider by name. Replaceable in tests, for emulators. */
    @FunctionalInterface
    public interface Providers
    {

        /** The real providers. */
        Providers DEFAULT = (name, crypto) -> switch (name)
        {
            case "aws-kms" -> AwsKmsKeyProvider.create(
                    Optional.ofNullable(crypto.aws().region()),
                    Optional.ofNullable(crypto.aws().endpoint()));
            case "gcp-kms" -> GcpKmsKeyProvider.create(
                    Optional.ofNullable(crypto.gcp().endpoint()));
            case "azure-keyvault" -> AzureKeyVaultKeyProvider.create();
            case "vault-transit" -> VaultTransitKeyProvider.create(
                    require(crypto.vault().address(),
                            "sdh.crypto.vault.address"),
                    crypto.vault().mount(), vaultToken(crypto.vault()),
                    Optional.ofNullable(crypto.vault().namespace()));
            default -> throw new IllegalArgumentException("Unknown key"
                    + " provider '" + name + "'; one of local, aws-kms,"
                    + " gcp-kms, azure-keyvault, vault-transit");
        };

        /**
         * Builds a provider.
         *
         * @param name   its name, such as {@code aws-kms}
         * @param crypto the key settings
         * @return the provider
         * @throws IllegalArgumentException if the name is unknown
         */
        KeyProvider create(String name, CommonSettings.Crypto crypto);
    }

    /** Pauses before the keys are tried again. Replaceable in tests. */
    @FunctionalInterface
    public interface Pause
    {

        /** Sleeps. */
        Pause SLEEP = Thread::sleep;

        /**
         * Pauses.
         *
         * @param millis how long
         * @throws InterruptedException if interrupted meanwhile
         */
        void pause(long millis) throws InterruptedException;
    }

    /** The first pause while a KMS is down at startup. */
    static final Duration FIRST_WAIT = Duration.ofSeconds(1);

    /** The longest pause while a KMS is down at startup. */
    static final Duration LONGEST_WAIT = Duration.ofSeconds(30);

    /**
     * Where the Vault token comes from: read from its file at each use, so
     * a renewed token is picked up, or the token set directly.
     *
     * @param vault the Vault settings
     * @return the token's supplier
     * @throws IllegalArgumentException if neither is set
     */
    static Supplier<String> vaultToken(CommonSettings.Vault vault)
    {
        if (vault.tokenFile() != null)
        {
            return () ->
            {
                try
                {
                    return Files.readString(vault.tokenFile()).trim();
                }
                catch (IOException unreadable)
                {
                    throw new UncheckedIOException(
                            "Vault token file unreadable", unreadable);
                }
            };
        }
        String token = require(vault.token(),
                "sdh.crypto.vault.token or sdh.crypto.vault.token-file");
        return () -> token;
    }

    /**
     * The splitter's keys: the records KEK, the landing KEK (unwrap only),
     * and the signing, recipient and inbox keys.
     *
     * @param crypto the key settings
     * @return the keys
     * @throws IOException              if a key file can't be read
     * @throws GeneralSecurityException if a key is wrong or unusable
     * @throws JOSEException            if the KMS signing probe fails
     */
    public static KeyMaterial load(CommonSettings.Crypto crypto)
            throws IOException, GeneralSecurityException, JOSEException
    {
        return load(crypto, Providers.DEFAULT);
    }

    /**
     * The splitter's keys, with KMS providers from {@code providers}. The
     * records KEK is probed with a wrap and unwrap. The landing KEK is only
     * ever unwrapped with, so it isn't probed: reading the landing ID key
     * at startup checks it.
     *
     * @param crypto    the key settings
     * @param providers builds KMS providers
     * @return the keys
     * @throws IOException              if a key file can't be read
     * @throws GeneralSecurityException if a key is wrong or unusable
     * @throws JOSEException            if the KMS signing probe fails
     */
    public static KeyMaterial load(CommonSettings.Crypto crypto,
            Providers providers)
            throws IOException, GeneralSecurityException, JOSEException
    {
        return load(crypto, providers, Pause.SLEEP);
    }

    /**
     * The splitter's keys, with KMS providers from {@code providers}, and
     * {@code pause} between tries while a KMS is down.
     *
     * @param crypto    the key settings
     * @param providers builds KMS providers
     * @param pause     pauses before trying again
     * @return the keys
     * @throws IOException              if a key file can't be read, or the
     *                                  thread is interrupted while waiting
     * @throws GeneralSecurityException if a key is wrong or unusable
     * @throws JOSEException            if the KMS signing probe fails
     */
    public static KeyMaterial load(CommonSettings.Crypto crypto,
            Providers providers, Pause pause)
            throws IOException, GeneralSecurityException, JOSEException
    {
        CommonSettings.Keys keys = crypto.keys();
        String records = require(keys.recordsKek(),
                "sdh.crypto.keys.records-kek");
        String landed = require(keys.landedItemsKek(),
                "sdh.crypto.keys.landed-items-kek");
        if (records.equals(landed))
        {
            throw new IllegalArgumentException("The records KEK must differ"
                    + " from the landing KEK (design §7.1)");
        }
        return waitingFor(crypto, providers, List.of(records),
                List.of(landed), true, pause);
    }

    /**
     * The keys of a stage after the splitter, such as the convert stage
     * (STAGES.md §9): the records KEK, probed, and the signing, recipient
     * and inbox keys. It holds no landing KEK; the inbox key opens the
     * previous stage's messages.
     *
     * @param crypto    the key settings
     * @param providers builds KMS providers
     * @return the keys
     * @throws IOException              if a key file can't be read
     * @throws GeneralSecurityException if a key is wrong or unusable
     * @throws JOSEException            if the KMS signing probe fails
     */
    public static KeyMaterial forStage(CommonSettings.Crypto crypto,
            Providers providers)
            throws IOException, GeneralSecurityException, JOSEException
    {
        String records = require(crypto.keys().recordsKek(),
                "sdh.crypto.keys.records-kek");
        return waitingFor(crypto, providers, List.of(records), List.of(),
                true, Pause.SLEEP);
    }

    /**
     * A landing node's keys (design §5.3): the {@code landed_items} KEK,
     * and the signing and recipient keys for {@code item.landed}. It opens
     * no messages, so {@link #inbox()} is null.
     *
     * @param crypto    the key settings
     * @param providers builds KMS providers
     * @return the keys
     * @throws IOException              if a key file can't be read
     * @throws GeneralSecurityException if a key is wrong or unusable
     * @throws JOSEException            if the KMS signing probe fails
     */
    public static KeyMaterial forLanding(CommonSettings.Crypto crypto,
            Providers providers)
            throws IOException, GeneralSecurityException, JOSEException
    {
        String landed = require(crypto.keys().landedItemsKek(),
                "sdh.crypto.keys.landed-items-kek");
        return waitingFor(crypto, providers, List.of(landed), List.of(),
                false, Pause.SLEEP);
    }

    /**
     * Loads and checks the keys, trying again while a KMS is down
     * ({@link Outage}): the pause doubles from {@link #FIRST_WAIT} to
     * {@link #LONGEST_WAIT}, for as long as it takes. Each try is logged as
     * {@code crypto.kms_waiting}, and the keys loaded after waiting as
     * {@code crypto.kms_ready}. Any other failure is thrown at once.
     *
     * @param crypto     the key settings
     * @param providers  builds KMS providers
     * @param probed     KEKs wrapped and unwrapped with, probed at startup
     * @param unwrapOnly KEKs only unwrapped with, not probed
     * @param inbox      whether the inbox and landing signing keys are
     *                   required
     * @param pause      pauses before trying again
     * @return the keys
     * @throws IOException              if a key file can't be read, or the
     *                                  thread is interrupted while waiting
     * @throws GeneralSecurityException if a key is wrong or unusable
     * @throws JOSEException            if the KMS signing probe fails
     */
    private static KeyMaterial waitingFor(CommonSettings.Crypto crypto,
            Providers providers, List<String> probed, List<String> unwrapOnly,
            boolean inbox, Pause pause)
            throws IOException, GeneralSecurityException, JOSEException
    {
        long start = System.nanoTime();
        for (int attempt = 1;; attempt++)
        {
            try
            {
                KeyMaterial keys = load(crypto, providers, probed,
                        unwrapOnly, inbox);
                if (attempt > 1)
                {
                    LOG.info("crypto.kms_ready attempts={} waited_ms={}",
                            attempt, TimeUnit.NANOSECONDS.toMillis(
                                    System.nanoTime() - start));
                }
                return keys;
            }
            catch (IOException | GeneralSecurityException | JOSEException
                    | RuntimeException failure)
            {
                if (!Outage.caused(failure))
                {
                    throw failure;
                }
                long wait = Math.min(LONGEST_WAIT.toMillis(),
                        FIRST_WAIT.toMillis() << Math.min(attempt - 1, 20));
                LOG.warn("crypto.kms_waiting attempt={} wait_ms={} error={}",
                        attempt, wait, failure.getClass().getSimpleName());
                try
                {
                    pause.pause(wait);
                }
                catch (InterruptedException interrupted)
                {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException(
                            "Interrupted waiting for the KMS");
                }
            }
        }
    }

    /**
     * Loads and checks the keys once. KMS clients opened along the way are
     * closed if anything fails.
     *
     * @param crypto     the key settings
     * @param providers  builds KMS providers
     * @param probed     KEKs wrapped and unwrapped with, probed at startup
     * @param unwrapOnly KEKs only unwrapped with, not probed
     * @param inbox      whether the splitter's inbox key and the landing
     *                   node's signing key are required
     * @return the keys
     * @throws IOException              if a key file can't be read
     * @throws GeneralSecurityException if a key is wrong or unusable
     * @throws JOSEException            if the KMS signing probe fails
     */
    private static KeyMaterial load(CommonSettings.Crypto crypto,
            Providers providers, List<String> probed, List<String> unwrapOnly,
            boolean inbox)
            throws IOException, GeneralSecurityException, JOSEException
    {
        CommonSettings.Keys keys = crypto.keys();
        List<String> all = new ArrayList<>(probed);
        all.addAll(unwrapOnly);
        String[] kekRefs = all.toArray(String[]::new);
        List<AutoCloseable> resources = new ArrayList<>();
        // One client per KMS, shared by wrapping and signing.
        Map<String, KeyProvider> byName = new LinkedHashMap<>();
        try
        {
            KeyProvider wrap = "local".equals(crypto.wrapProvider())
                    ? localKeks(crypto, kekRefs)
                    : byName.computeIfAbsent(crypto.wrapProvider(),
                            name -> track(providers.create(name, crypto),
                                    resources));
            for (String ref : probed)
            {
                probeKek(wrap, ref);
            }

            JWSSigner signer;
            if ("local".equals(crypto.signProvider()))
            {
                Path signingPem = require(keys.signing().pem(),
                        "sdh.crypto.keys.signing.pem");
                checkPrivate(signingPem);
                PrivateKey signing = Pem.encryptedPrivateKey(signingPem,
                        require(keys.signing().passphrase(),
                                "sdh.crypto.keys.signing.passphrase")
                                .toCharArray());
                if (!(signing instanceof ECPrivateKey ecSigning)
                        || !isP256(ecSigning.getParams()))
                {
                    throw new GeneralSecurityException(
                            "Signing key must be EC P-256 (ES256)");
                }
                signer = new ECDSASigner(ecSigning);
            }
            else
            {
                KeyProvider kms = byName.computeIfAbsent(
                        crypto.signProvider(),
                        name -> track(providers.create(name, crypto),
                                resources));
                String keyRef = require(keys.signing().keyRef(),
                        "sdh.crypto.keys.signing.key-ref");
                PublicKey signingPublic = kms.publicKey(keyRef);
                if (!(signingPublic instanceof ECPublicKey ecPublic)
                        || !isP256(ecPublic.getParams()))
                {
                    throw new GeneralSecurityException("Signing key " + keyRef
                            + " must be EC P-256 (ES256)");
                }
                signer = new KmsJwsSigner(kms, keyRef);
                probeSigner(signer, ecPublic);
                LOG.info("crypto.signing_key provider={} kid={} sha256={}"
                        + " (configure readers with this public key)",
                        crypto.signProvider(), keys.signing().kid(),
                        fingerprint(signingPublic));
            }

            PublicKey recipient =
                    pinnedKey(keys.recipient(), "sdh.crypto.keys.recipient");
            MessageOpener opener = inbox ? inboxOpener(keys) : null;
            return new KeyMaterial(wrap, signer, keys.signing().kid(),
                    recipient, require(keys.recipient().kid(),
                            "sdh.crypto.keys.recipient.kid"),
                    opener, List.copyOf(resources));
        }
        catch (Exception | Error failure)
        {
            closeAll(resources);
            throw failure;
        }
    }

    /**
     * Loads a recipient's public key and checks it against its pinned
     * fingerprint.
     *
     * @param recipient the key's settings
     * @param prefix    the settings' name, for errors
     * @return the key
     * @throws IOException              if the PEM can't be read
     * @throws GeneralSecurityException if the key isn't the one pinned
     */
    public static PublicKey pinnedKey(CommonSettings.Recipient recipient,
            String prefix) throws IOException, GeneralSecurityException
    {
        PublicKey key = Pem.publicKey(require(recipient.pem(),
                prefix + ".pem"));
        String pinned = require(recipient.sha256(), prefix + ".sha256")
                .replace(":", "").toLowerCase(Locale.ROOT);
        if (!fingerprint(key).equals(pinned))
        {
            throw new GeneralSecurityException("Key " + prefix
                    + " does not match its pinned SHA-256 fingerprint");
        }
        return key;
    }

    /**
     * Builds the opener of {@code item.landed} messages: the splitter's own
     * private key, and the landing node's signing public key.
     *
     * @param keys the key settings
     * @return the opener
     * @throws IOException              if a key file can't be read
     * @throws GeneralSecurityException if a key isn't EC P-256
     */
    private static MessageOpener inboxOpener(CommonSettings.Keys keys)
            throws IOException, GeneralSecurityException
    {
        Path pem = require(keys.inbox().pem(), "sdh.crypto.keys.inbox.pem");
        checkPrivate(pem);
        PrivateKey key = Pem.encryptedPrivateKey(pem,
                require(keys.inbox().passphrase(),
                        "sdh.crypto.keys.inbox.passphrase").toCharArray());
        if (!(key instanceof ECPrivateKey ecKey) || !isP256(ecKey.getParams()))
        {
            throw new GeneralSecurityException("Inbox key must be EC P-256");
        }
        PublicKey signer = Pem.publicKey(require(keys.landingSigner().pem(),
                "sdh.crypto.keys.landing-signer.pem"));
        if (!(signer instanceof ECPublicKey ecSigner)
                || !isP256(ecSigner.getParams()))
        {
            throw new GeneralSecurityException(
                    "Landing signing key must be EC P-256 (ES256)");
        }
        return new MessageOpener(
                Map.of(require(keys.inbox().kid(),
                        "sdh.crypto.keys.inbox.kid"), key),
                Map.of(require(keys.landingSigner().kid(),
                        "sdh.crypto.keys.landing-signer.kid"), ecSigner));
    }

    /**
     * Loads the KEKs from the local keystore.
     *
     * @param crypto the key settings
     * @param refs   the KEKs' aliases
     * @return a provider wrapping with them
     * @throws IOException              if the keystore can't be read
     * @throws GeneralSecurityException if a KEK is missing or isn't
     *                                  AES-256
     */
    private static KeyProvider localKeks(CommonSettings.Crypto crypto,
            String[] refs) throws IOException, GeneralSecurityException
    {
        Path keystorePath = require(crypto.keystore().path(),
                "sdh.crypto.keystore.path");
        checkPrivate(keystorePath);
        char[] keystorePassword = require(crypto.keystore().password(),
                "sdh.crypto.keystore.password").toCharArray();
        KeyStore keystore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystorePath))
        {
            keystore.load(in, keystorePassword);
        }
        Map<String, SecretKey> keks = new LinkedHashMap<>();
        for (String ref : refs)
        {
            if (!(keystore.getKey(ref, keystorePassword)
                            instanceof SecretKey key)
                    ||!"AES".equalsIgnoreCase(key.getAlgorithm())
                    || key.getEncoded().length != 32)
            {
                throw new GeneralSecurityException("Keystore entry '" + ref
                        + "' must be an AES-256 secret key");
            }
            keks.put(ref, key);
        }
        return new AesKeyWrapProvider(keks);
    }

    /**
     * Wraps and unwraps a throwaway key, so a missing KEK or permission
     * stops startup rather than the first item.
     *
     * @param provider wraps with the KEK
     * @param ref      the KEK
     * @throws GeneralSecurityException if the KEK can't be used
     */
    static void probeKek(KeyProvider provider, String ref)
            throws GeneralSecurityException
    {
        byte[] probe = new byte[32];
        new SecureRandom().nextBytes(probe);
        try
        {
            WrappedKey wrapped = provider.wrap(ref, probe);
            if (!MessageDigest.isEqual(probe, provider.unwrap(wrapped)))
            {
                throw new GeneralSecurityException(
                        "KEK " + ref + " did not round-trip");
            }
        }
        catch (RuntimeException failure)
        {
            throw new GeneralSecurityException("KEK " + ref
                    + " is not usable: " + failure.getMessage(), failure);
        }
    }

    /**
     * Signs a probe through the KMS and verifies it with the key's public
     * half.
     *
     * @param signer    signs through the KMS
     * @param publicKey the key's public half
     * @throws JOSEException            if signing fails
     * @throws GeneralSecurityException if the signature doesn't verify
     */
    static void probeSigner(JWSSigner signer, ECPublicKey publicKey)
            throws JOSEException, GeneralSecurityException
    {
        JWSObject jws = new JWSObject(new JWSHeader(JWSAlgorithm.ES256),
                new Payload("sdh-signing-probe"));
        jws.sign(signer);
        if (!jws.verify(new ECDSAVerifier(publicKey)))
        {
            throw new GeneralSecurityException("KMS signature did not verify"
                    + " with the key's public key");
        }
    }

    /**
     * Notes a provider to close later, if it holds resources.
     *
     * @param provider  the provider
     * @param resources where to note it
     * @return the provider
     */
    private static KeyProvider track(KeyProvider provider,
            List<AutoCloseable> resources)
    {
        if (provider instanceof AutoCloseable closeable)
        {
            resources.add(closeable);
        }
        return provider;
    }

    /**
     * Closes resources, ignoring failures.
     *
     * @param resources the resources
     */
    private static void closeAll(List<AutoCloseable> resources)
    {
        for (AutoCloseable resource : resources)
        {
            try
            {
                resource.close();
            }
            catch (Exception ignored)
            {
                // shutting down
            }
        }
    }

    /** Closes the KMS clients. */
    @Override
    public void close()
    {
        closeAll(resources);
    }

    /**
     * A public key's fingerprint, as pinned in configuration.
     *
     * @param key the key
     * @return the lower-case hex SHA-256 of its SubjectPublicKeyInfo
     * @throws GeneralSecurityException if SHA-256 isn't available
     */
    public static String fingerprint(PublicKey key)
            throws GeneralSecurityException
    {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(key.getEncoded()));
    }

    /**
     * Reads an encrypted PKCS#8 private key under the same rules as the
     * signing key: owner-only (or group-read) permissions and a passphrase.
     * Used by operator tools for the recovery key of old fallback files
     * (design §11.4).
     *
     * @param pem        the key file
     * @param passphrase its passphrase
     * @return the key
     * @throws IOException              if the file can't be read, or is too
     *                                  open
     * @throws GeneralSecurityException if the passphrase is wrong
     */
    public static PrivateKey loadPrivateKey(Path pem, char[] passphrase)
            throws IOException, GeneralSecurityException
    {
        checkPrivate(pem);
        return Pem.encryptedPrivateKey(pem, passphrase);
    }

    /**
     * Checks that a private key file exists and isn't too open.
     *
     * @param file the file
     * @throws IOException if it is missing, accessible by others, or
     *                     writable or executable by group
     */
    static void checkPrivate(Path file) throws IOException
    {
        if (!Files.isRegularFile(file))
        {
            throw new IOException("Key file not found: " + file.getFileName());
        }
        if (FileSystems.getDefault().supportedFileAttributeViews()
                .contains("posix"))
        {
            Set<PosixFilePermission> permissions =
                    Files.getPosixFilePermissions(file);
            permissions.retainAll(GROUP_OR_OTHER);
            if (!permissions.isEmpty())
            {
                throw new IOException("Key file " + file.getFileName()
                        + " must not be accessible by others or writable by"
                        + " group (chmod 600 or 640)");
            }
        }
    }

    /**
     * Tells whether EC parameters are P-256's.
     *
     * @param params the parameters
     * @return true for a 256-bit prime curve with cofactor 1
     */
    private static boolean isP256(ECParameterSpec params)
    {
        return params.getCurve().getField().getFieldSize() == 256
                && params.getCofactor() == 1
                && params.getOrder().bitLength() == 256;
    }

    /**
     * Checks that a setting is there.
     *
     * @param <T>   its type
     * @param value its value
     * @param name  its name, for the message
     * @return the value
     * @throws IllegalArgumentException if it is null or blank
     */
    static <T> T require(T value, String name)
    {
        if (value == null || (value instanceof String text && text.isBlank()))
        {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}
