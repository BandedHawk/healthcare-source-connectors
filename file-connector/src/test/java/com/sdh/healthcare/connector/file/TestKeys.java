/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.file;

import com.sdh.healthcare.connector.runtime.config.CommonSettings;
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
 * Writes a complete set of local key files, as an operator would provide them.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class TestKeys {
    public static final String KS_PASSWORD = "ks-secret";
    public static final String SIGNING_PASSPHRASE = "sign-secret";
    public static final String SECTIONS = "sections-kek/1";
    public static final String LANDED = "landed-items-kek/1";
    public static final String INBOX_PASSPHRASE = "inbox-secret";
    public static final String LANDING_SIGNER_KID = "landing-2026";
    public static final String INBOX_KID = "splitter-2026";

    public final Path dir;
    public final Path keystore;
    public final Path signingPem;
    public final Path recipientPem;
    public final Path recoveryPem;
    /** The splitter's own private key, which {@code item.landed} messages are sealed to. */
    public final Path inboxPem;
    /** The landing node's signing key: the private half for a landing node, the public half for the splitter. */
    public final Path landingSigningPem;
    public final Path landingSignerPem;
    public final KeyPair signing;
    public final KeyPair recipient;
    public final KeyPair recovery;
    public final KeyPair inbox;
    public final KeyPair landingSigning;

    public TestKeys(Path dir) throws Exception {
        this.dir = dir;
        signing = ec();
        recipient = ec();
        recovery = ec();
        inbox = ec();
        landingSigning = ec();

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        for (String alias : new String[] {SECTIONS, LANDED}) {
            ks.setEntry(alias, new KeyStore.SecretKeyEntry(kg.generateKey()),
                    new KeyStore.PasswordProtection(KS_PASSWORD.toCharArray()));
        }
        keystore = privateFile(dir.resolve("keks.p12"));
        try (OutputStream out = Files.newOutputStream(keystore)) {
            ks.store(out, KS_PASSWORD.toCharArray());
        }
        signingPem = privateFile(dir.resolve("signing.pem"));
        Files.writeString(signingPem, pem("ENCRYPTED PRIVATE KEY", encrypt(signing.getPrivate(), SIGNING_PASSPHRASE)));
        recipientPem = Files.writeString(dir.resolve("recipient.pem"), publicPem(recipient.getPublic()));
        recoveryPem = Files.writeString(dir.resolve("recovery-public.pem"), publicPem(recovery.getPublic()));
        inboxPem = encryptedPrivatePem(dir.resolve("inbox.pem"), inbox.getPrivate(), INBOX_PASSPHRASE);
        landingSigningPem = encryptedPrivatePem(dir.resolve("landing-signing.pem"), landingSigning.getPrivate(),
                SIGNING_PASSPHRASE);
        landingSignerPem = Files.writeString(dir.resolve("landing-signer.pem"),
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
    public CommonSettings.Keys splitterKeys(String recipientPin, Path signingPem, String signingKeyRef) {
        return new CommonSettings.Keys(SECTIONS, LANDED,
                new CommonSettings.Signing("connector-2026", signingPem, SIGNING_PASSPHRASE, signingKeyRef),
                new CommonSettings.Recipient("downstream-2026", recipientPem, recipientPin),
                new CommonSettings.InboxKey(INBOX_KID, inboxPem, INBOX_PASSPHRASE),
                new CommonSettings.Signer(LANDING_SIGNER_KID, landingSignerPem));
    }

    /** Writes {@code key} as an encrypted PKCS#8 PEM with owner-only permissions, as an operator would keep it. */
    public static Path encryptedPrivatePem(Path file, PrivateKey key, String passphrase) throws Exception {
        Files.writeString(privateFile(file), pem("ENCRYPTED PRIVATE KEY", encrypt(key, passphrase)));
        return file;
    }

    public static KeyPair ec() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp256r1"));
        return g.generateKeyPair();
    }

    static Path privateFile(Path p) throws Exception {
        Files.deleteIfExists(p);
        Files.createFile(p, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        return p;
    }

    /** PBES2 (PBKDF2-HMAC-SHA256 + AES-256-CBC), the same scheme {@code openssl pkcs8 -topk8} uses by default. */
    static byte[] encrypt(PrivateKey key, String passphrase) throws Exception {
        String alg = "PBEWithHmacSHA256AndAES_256";
        byte[] salt = new byte[16];
        byte[] iv = new byte[16];
        new SecureRandom().nextBytes(salt);
        new SecureRandom().nextBytes(iv);
        AlgorithmParameters params = AlgorithmParameters.getInstance(alg);
        params.init(new PBEParameterSpec(salt, 100_000, new IvParameterSpec(iv)));
        Cipher c = Cipher.getInstance(alg);
        c.init(Cipher.ENCRYPT_MODE, SecretKeyFactory.getInstance(alg).generateSecret(
                new PBEKeySpec(passphrase.toCharArray())), params);
        byte[] ct = c.doFinal(key.getEncoded());
        // EncryptedPrivateKeyInfo ::= SEQUENCE { AlgorithmIdentifier { id-PBES2, PBES2-params }, OCTET STRING }.
        // Built by hand: the JDK can't map the PBES2 scheme name back to its OID in the other constructor.
        byte[] pbes2Oid = {0x06, 0x09, 0x2A, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xF7, 0x0D, 0x01, 0x05, 0x0D};
        byte[] der = tlv(0x30, tlv(0x30, pbes2Oid, params.getEncoded()), tlv(0x04, ct));
        new EncryptedPrivateKeyInfo(der); // sanity check
        return der;
    }

    static byte[] tlv(int tag, byte[]... parts) {
        java.io.ByteArrayOutputStream body = new java.io.ByteArrayOutputStream();
        for (byte[] p : parts) {
            body.writeBytes(p);
        }
        int n = body.size();
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        out.write(tag);
        if (n < 0x80) {
            out.write(n);
        } else if (n < 0x100) {
            out.write(0x81);
            out.write(n);
        } else {
            out.write(0x82);
            out.write(n >> 8);
            out.write(n & 0xff);
        }
        out.writeBytes(body.toByteArray());
        return out.toByteArray();
    }

    public static String publicPem(PublicKey k) {
        return pem("PUBLIC KEY", k.getEncoded());
    }

    static String pem(String type, byte[] der) {
        return "-----BEGIN " + type + "-----\n" + Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(der)
                + "\n-----END " + type + "-----\n";
    }
}
