/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

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
import java.security.SecureRandom;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

/**
 * Key files for a landing node under test, as an operator would keep them:
 * a PKCS#12 keystore with the {@code landed_items} KEK, an encrypted
 * signing key, and the recipient's public key.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class LandingTestKeys
{

    /** The keystore's password. */
    static final String KEYSTORE_PASSWORD = "ks-secret";

    /** The signing key's passphrase. */
    static final String SIGNING_PASSPHRASE = "sign-secret";

    /** The KEK's alias. */
    static final String LANDED_ITEMS = "landed-items-kek/1";

    /** The keystore file. */
    final Path keystore;

    /** The encrypted signing key. */
    final Path signingPem;

    /** The recipient's public key. */
    final Path recipientPem;

    /** The signing key pair. */
    final KeyPair signing;

    /** The recipient's key pair. */
    final KeyPair recipient;

    /**
     * Makes the keys and writes their files.
     *
     * @param directory where to write them
     * @throws Exception if a key can't be made or written
     */
    LandingTestKeys(Path directory) throws Exception
    {
        signing = ec();
        recipient = ec();
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        store.setEntry(LANDED_ITEMS,
                new KeyStore.SecretKeyEntry(generator.generateKey()),
                new KeyStore.PasswordProtection(
                        KEYSTORE_PASSWORD.toCharArray()));
        keystore = privateFile(directory.resolve("keks.p12"));
        try (OutputStream out = Files.newOutputStream(keystore))
        {
            store.store(out, KEYSTORE_PASSWORD.toCharArray());
        }
        signingPem = privateFile(directory.resolve("signing.pem"));
        Files.writeString(signingPem, pem("ENCRYPTED PRIVATE KEY",
                encrypt(signing.getPrivate(), SIGNING_PASSPHRASE)));
        recipientPem = Files.writeString(directory.resolve("recipient.pem"),
                pem("PUBLIC KEY", recipient.getPublic().getEncoded()));
    }

    /**
     * Makes a P-256 key pair.
     *
     * @return the pair
     * @throws Exception if it can't be made
     */
    static KeyPair ec() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }

    /**
     * Creates an empty file only its owner can read.
     *
     * @param file the file
     * @return the file
     * @throws Exception if it can't be created
     */
    static Path privateFile(Path file) throws Exception
    {
        Files.deleteIfExists(file);
        Files.createFile(file, PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rw-------")));
        return file;
    }

    /**
     * Encrypts a private key as PBES2 (PBKDF2-HMAC-SHA256 with
     * AES-256-CBC), the scheme {@code openssl pkcs8 -topk8} uses.
     *
     * @param key        the key
     * @param passphrase the passphrase
     * @return the DER of its EncryptedPrivateKeyInfo
     * @throws Exception if it can't be encrypted
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
        cipher.init(Cipher.ENCRYPT_MODE, SecretKeyFactory.getInstance(
                algorithm).generateSecret(new PBEKeySpec(
                        passphrase.toCharArray())), parameters);
        byte[] ciphertext = cipher.doFinal(key.getEncoded());
        byte[] pbes2 = {0x06, 0x09, 0x2A, (byte) 0x86, 0x48, (byte) 0x86,
            (byte) 0xF7, 0x0D, 0x01, 0x05, 0x0D};
        return tlv(0x30, tlv(0x30, pbes2, parameters.getEncoded()),
                tlv(0x04, ciphertext));
    }

    /**
     * Encodes one DER element.
     *
     * @param tag   its tag
     * @param parts its content, in pieces
     * @return the element
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
     * Wraps DER in PEM armour.
     *
     * @param type the PEM type
     * @param der  the content
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
