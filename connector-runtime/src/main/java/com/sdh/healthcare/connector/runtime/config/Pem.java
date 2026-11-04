/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * Minimal PEM reading with the JDK only: public keys, and encrypted PKCS#8
 * private keys (PBES2, as OpenSSL writes them).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Pem
{

    /** A PEM block: its type, then its base64 body. */
    private static final Pattern BLOCK = Pattern.compile(
            "-----BEGIN ([A-Z ]+)-----([^-]+)-----END \\1-----");

    /** Not instantiated: this class only has static methods. */
    private Pem()
    {
    }

    /**
     * Reads the first block of a type from a PEM file.
     *
     * @param file the file
     * @param type the block's type, such as {@code PUBLIC KEY}
     * @return the block's decoded bytes
     * @throws IOException if the file can't be read or has no such block
     */
    static byte[] block(Path file, String type) throws IOException
    {
        Matcher blocks = BLOCK.matcher(
                Files.readString(file, StandardCharsets.US_ASCII));
        while (blocks.find())
        {
            if (blocks.group(1).equals(type))
            {
                return Base64.getMimeDecoder().decode(blocks.group(2));
            }
        }
        throw new IOException("No " + type + " block in " + file.getFileName());
    }

    /**
     * Reads an EC or RSA public key.
     *
     * @param file a PEM file with a {@code PUBLIC KEY} block
     * @return the key
     * @throws IOException              if the file can't be read
     * @throws GeneralSecurityException if the key is neither EC nor RSA
     */
    public static PublicKey publicKey(Path file)
            throws IOException, GeneralSecurityException
    {
        X509EncodedKeySpec spec =
                new X509EncodedKeySpec(block(file, "PUBLIC KEY"));
        try
        {
            return KeyFactory.getInstance("EC").generatePublic(spec);
        }
        catch (InvalidKeySpecException notEc)
        {
            return KeyFactory.getInstance("RSA").generatePublic(spec);
        }
    }

    /**
     * Reads an encrypted EC private key.
     *
     * @param file       a PEM file with an {@code ENCRYPTED PRIVATE KEY}
     *                   block
     * @param passphrase its passphrase
     * @return the key
     * @throws IOException              if the file can't be read
     * @throws GeneralSecurityException if the key isn't encrypted PKCS#8,
     *                                  or the passphrase is wrong
     */
    public static PrivateKey encryptedPrivateKey(Path file, char[] passphrase)
            throws IOException, GeneralSecurityException
    {
        String text = Files.readString(file, StandardCharsets.US_ASCII);
        if (!text.contains("-----BEGIN ENCRYPTED PRIVATE KEY-----"))
        {
            throw new GeneralSecurityException("Private key "
                    + file.getFileName() + " must be an encrypted PKCS#8 PEM"
                    + " (BEGIN ENCRYPTED PRIVATE KEY)");
        }
        EncryptedPrivateKeyInfo encrypted = new EncryptedPrivateKeyInfo(
                block(file, "ENCRYPTED PRIVATE KEY"));
        AlgorithmParameters params = encrypted.getAlgParameters();
        // For PBES2 the parameters name the real scheme, such as
        // PBEWithHmacSHA256AndAES_256.
        String algorithm = params != null
                ? params.toString() : encrypted.getAlgName();
        SecretKey pbeKey = SecretKeyFactory.getInstance(algorithm)
                .generateSecret(new PBEKeySpec(passphrase));
        Cipher cipher = Cipher.getInstance(algorithm);
        cipher.init(Cipher.DECRYPT_MODE, pbeKey, params);
        PKCS8EncodedKeySpec spec;
        try
        {
            spec = encrypted.getKeySpec(cipher);
        }
        catch (InvalidKeySpecException wrongPassphrase)
        {
            throw new GeneralSecurityException(
                    "Wrong passphrase for " + file.getFileName());
        }
        return KeyFactory.getInstance("EC").generatePrivate(spec);
    }
}
