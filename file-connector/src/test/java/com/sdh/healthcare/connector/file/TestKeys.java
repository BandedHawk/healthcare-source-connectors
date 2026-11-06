/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file;

import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.AlgorithmParameters;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

/**
 * Writes a complete set of local key files, as an operator would provide
 * them.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class TestKeys
{

    /** The KEK keystore's password. */
    public static final String KS_PASSWORD = "ks-secret";

    /** The passphrase of both signing keys. */
    public static final String SIGNING_PASSPHRASE = "sign-secret";

    /** The records KEK's alias. */
    public static final String RECORDS = "records-kek/1";

    /** The landed items KEK's alias. */
    public static final String LANDED = "landed-items-kek/1";

    /** The inbox key's passphrase. */
    public static final String INBOX_PASSPHRASE = "inbox-secret";

    /** The landing node's signing key ID. */
    public static final String LANDING_SIGNER_KID = "landing-2026";

    /** The splitter's inbox key ID. */
    public static final String INBOX_KID = "splitter-2026";

    /** The directory the files are in. */
    public final Path dir;

    /** The PKCS#12 keystore holding both KEKs. */
    public final Path keystore;

    /** The splitter's encrypted signing key. */
    public final Path signingPem;

    /** The downstream recipient's public key. */
    public final Path recipientPem;

    /** The recovery public key. */
    public final Path recoveryPem;

    /**
     * The splitter's own private key, which {@code item.landed} messages
     * are sealed to.
     */
    public final Path inboxPem;

    /**
     * The public half of the splitter's inbox key: the landing node's
     * recipient.
     */
    public final Path inboxPublicPem;

    /** The landing node's encrypted signing key. */
    public final Path landingSigningPem;

    /** The public half of the landing node's signing key, for the splitter. */
    public final Path landingSignerPem;

    /** The splitter's signing key pair. */
    public final KeyPair signing;

    /** The downstream recipient's key pair. */
    public final KeyPair recipient;

    /** The recovery key pair. */
    public final KeyPair recovery;

    /** The splitter's inbox key pair. */
    public final KeyPair inbox;

    /** The landing node's signing key pair. */
    public final KeyPair landingSigning;

    /**
     * Generates every key and writes the files.
     *
     * @param dir the directory to write them in
     * @throws Exception if a key can't be made or a file written
     */
    public TestKeys(Path dir) throws Exception
    {
        this.dir = dir;
        signing = ec();
        recipient = ec();
        recovery = ec();
        inbox = ec();
        landingSigning = ec();

        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        for (String alias : new String[] {RECORDS, LANDED})
        {
            store.setEntry(alias,
                    new KeyStore.SecretKeyEntry(generator.generateKey()),
                    new KeyStore.PasswordProtection(
                            KS_PASSWORD.toCharArray()));
        }
        keystore = privateFile(dir.resolve("keks.p12"));
        try (OutputStream out = Files.newOutputStream(keystore))
        {
            store.store(out, KS_PASSWORD.toCharArray());
        }
        signingPem = privateFile(dir.resolve("signing.pem"));
        Files.writeString(signingPem, pem("ENCRYPTED PRIVATE KEY",
                encrypt(signing.getPrivate(), SIGNING_PASSPHRASE)));
        recipientPem = Files.writeString(dir.resolve("recipient.pem"),
                publicPem(recipient.getPublic()));
        recoveryPem = Files.writeString(dir.resolve("recovery-public.pem"),
                publicPem(recovery.getPublic()));
        inboxPem = encryptedPrivatePem(dir.resolve("inbox.pem"),
                inbox.getPrivate(), INBOX_PASSPHRASE);
        inboxPublicPem = Files.writeString(dir.resolve("inbox-public.pem"),
                publicPem(inbox.getPublic()));
        landingSigningPem = encryptedPrivatePem(
                dir.resolve("landing-signing.pem"),
                landingSigning.getPrivate(), SIGNING_PASSPHRASE);
        landingSignerPem = Files.writeString(
                dir.resolve("landing-signer.pem"),
                publicPem(landingSigning.getPublic()));
    }

    /**
     * The splitter's key settings.
     *
     * @param recipientPin  the downstream recipient key's pinned fingerprint
     * @param signingPem    the splitter's signing key
     * @param signingKeyRef the KMS signing key, or null for the local one
     * @return the settings
     */
    public CommonSettings.Keys splitterKeys(String recipientPin,
            Path signingPem, String signingKeyRef)
    {
        return new CommonSettings.Keys(RECORDS, LANDED,
                new CommonSettings.Signing("connector-2026", signingPem,
                        SIGNING_PASSPHRASE, signingKeyRef),
                new CommonSettings.Recipient("downstream-2026", recipientPem,
                        recipientPin),
                new CommonSettings.InboxKey(INBOX_KID, inboxPem,
                        INBOX_PASSPHRASE),
                new CommonSettings.Signer(LANDING_SIGNER_KID,
                        landingSignerPem));
    }

    /**
     * Writes a key as an encrypted PKCS#8 PEM with owner-only permissions,
     * as an operator would keep it.
     *
     * @param file       the file to write
     * @param key        the private key
     * @param passphrase the passphrase to encrypt it with
     * @return the file
     * @throws Exception if the key can't be encrypted or written
     */
    public static Path encryptedPrivatePem(Path file, PrivateKey key,
            String passphrase) throws Exception
    {
        Files.writeString(privateFile(file),
                pem("ENCRYPTED PRIVATE KEY", encrypt(key, passphrase)));
        return file;
    }

    /**
     * A new P-256 key pair.
     *
     * @return the key pair
     * @throws Exception if the curve isn't available
     */
    public static KeyPair ec() throws Exception
    {
        KeyPairGenerator pairs = KeyPairGenerator.getInstance("EC");
        pairs.initialize(new ECGenParameterSpec("secp256r1"));
        return pairs.generateKeyPair();
    }

    /**
     * Creates an empty file only the owner can read and write, replacing
     * any that exists.
     *
     * @param path the file
     * @return the file
     * @throws Exception if the file can't be created
     */
    static Path privateFile(Path path) throws Exception
    {
        Files.deleteIfExists(path);
        Files.createFile(path, PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rw-------")));
        return path;
    }

    /**
     * Encrypts a private key with PBES2 (PBKDF2-HMAC-SHA256 +
     * AES-256-CBC), the same scheme {@code openssl pkcs8 -topk8} uses by
     * default.
     *
     * @param key        the private key
     * @param passphrase the passphrase
     * @return the DER of the EncryptedPrivateKeyInfo
     * @throws Exception if encryption fails
     */
    static byte[] encrypt(PrivateKey key, String passphrase) throws Exception
    {
        String algorithm = "PBEWithHmacSHA256AndAES_256";
        byte[] salt = new byte[16];
        byte[] iv = new byte[16];
        new SecureRandom().nextBytes(salt);
        new SecureRandom().nextBytes(iv);
        AlgorithmParameters parameters =
                AlgorithmParameters.getInstance(algorithm);
        parameters.init(new PBEParameterSpec(salt, 100_000,
                new IvParameterSpec(iv)));
        Cipher cipher = Cipher.getInstance(algorithm);
        cipher.init(Cipher.ENCRYPT_MODE,
                SecretKeyFactory.getInstance(algorithm).generateSecret(
                        new PBEKeySpec(passphrase.toCharArray())),
                parameters);
        byte[] ciphertext = cipher.doFinal(key.getEncoded());
        // EncryptedPrivateKeyInfo ::= SEQUENCE { AlgorithmIdentifier {
        // id-PBES2, PBES2-params }, OCTET STRING }. Built by hand: the JDK
        // can't map the PBES2 scheme name back to its OID in the other
        // constructor.
        byte[] pbes2Oid = {0x06, 0x09, 0x2A, (byte) 0x86, 0x48, (byte) 0x86,
            (byte) 0xF7, 0x0D, 0x01, 0x05, 0x0D};
        byte[] der = tlv(0x30, tlv(0x30, pbes2Oid, parameters.getEncoded()),
                tlv(0x04, ciphertext));
        // A sanity check: the JDK parses it.
        new EncryptedPrivateKeyInfo(der);
        return der;
    }

    /**
     * A DER tag-length-value, for lengths up to 65535.
     *
     * @param tag   the tag
     * @param parts the value's parts, in order
     * @return the encoding
     */
    static byte[] tlv(int tag, byte[]... parts)
    {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (byte[] part : parts)
        {
            body.writeBytes(part);
        }
        int length = body.size();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(tag);
        if (length < 0x80)
        {
            out.write(length);
        }
        else if (length < 0x100)
        {
            out.write(0x81);
            out.write(length);
        }
        else
        {
            out.write(0x82);
            out.write(length >> 8);
            out.write(length & 0xff);
        }
        out.writeBytes(body.toByteArray());
        return out.toByteArray();
    }

    /**
     * A public key as PEM.
     *
     * @param key the public key
     * @return the PEM text
     */
    public static String publicPem(PublicKey key)
    {
        return pem("PUBLIC KEY", key.getEncoded());
    }

    /**
     * DER as PEM with 64-character lines.
     *
     * @param type the PEM label
     * @param der  the encoding
     * @return the PEM text
     */
    static String pem(String type, byte[] der)
    {
        return "-----BEGIN " + type + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes())
                        .encodeToString(der)
                + "\n-----END " + type + "-----\n";
    }
}
