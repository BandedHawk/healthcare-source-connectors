/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEEncrypter;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDHEncrypter;
import com.nimbusds.jose.crypto.RSAEncrypter;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HexFormat;

/**
 * Encrypts what an operator exports to the operator's own public key
 * (STAGES.md §15.6c), as a JWE: the plaintext never leaves the pod, and
 * only the holder of the private key can read it. EC P-256 keys use
 * {@code ECDH-ES+A256KW}, RSA keys (2048 bits or more) {@code
 * RSA-OAEP-256}; the content is {@code A256GCM}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ExportSealer
{

    /** The longest public key read, in bytes. */
    static final int MAX_KEY_BYTES = 16 * 1024;

    /** The recipient's key. */
    private final PublicKey recipient;

    /**
     * Reads the recipient's key.
     *
     * @param pem the public key, PEM ({@code BEGIN PUBLIC KEY})
     * @throws AdminArgs.UsageException if it isn't an EC or RSA public key
     */
    ExportSealer(byte[] pem)
    {
        this.recipient = parse(new String(pem, StandardCharsets.US_ASCII));
    }

    /**
     * Parses a PEM public key.
     *
     * @param pem the key
     * @return the key
     * @throws AdminArgs.UsageException if it isn't an EC or RSA public key
     */
    private static PublicKey parse(String pem)
    {
        String body = pem.replaceAll("-----(BEGIN|END) PUBLIC KEY-----", "")
                .replaceAll("\\s", "");
        byte[] der;
        try
        {
            der = Base64.getDecoder().decode(body);
        }
        catch (IllegalArgumentException notBase64)
        {
            throw new AdminArgs.UsageException("Give a PEM public key on"
                    + " standard input (BEGIN PUBLIC KEY)");
        }
        for (String algorithm : new String[] {"EC", "RSA"})
        {
            try
            {
                PublicKey key = KeyFactory.getInstance(algorithm)
                        .generatePublic(new X509EncodedKeySpec(der));
                if (key instanceof RSAPublicKey rsa
                        && rsa.getModulus().bitLength() < 2048)
                {
                    throw new AdminArgs.UsageException(
                            "An RSA key must have 2048 bits or more");
                }
                return key;
            }
            catch (GeneralSecurityException notThisKind)
            {
                // try the next
            }
        }
        throw new AdminArgs.UsageException("Give a PEM public key on"
                + " standard input: EC P-256 or RSA");
    }

    /**
     * The key's SHA-256, hex, for the audit log.
     *
     * @return the fingerprint
     */
    String fingerprint()
    {
        try
        {
            return HexFormat.of().formatHex(MessageDigest
                    .getInstance("SHA-256").digest(recipient.getEncoded()));
        }
        catch (GeneralSecurityException missing)
        {
            throw new IllegalStateException(missing);
        }
    }

    /**
     * Encrypts content to the key.
     *
     * @param content the content; not kept
     * @return the JWE compact serialisation
     */
    String seal(byte[] content)
    {
        try
        {
            JWEEncrypter encrypter;
            JWEAlgorithm algorithm;
            switch (recipient)
            {
                case ECPublicKey ec ->
                {
                    encrypter = new ECDHEncrypter(ec);
                    algorithm = JWEAlgorithm.ECDH_ES_A256KW;
                }
                case RSAPublicKey rsa ->
                {
                    encrypter = new RSAEncrypter(rsa);
                    algorithm = JWEAlgorithm.RSA_OAEP_256;
                }
                default -> throw new IllegalStateException(
                        "Not an EC or RSA key");
            }
            JWEObject jwe = new JWEObject(new JWEHeader.Builder(algorithm,
                    EncryptionMethod.A256GCM).keyID(fingerprint()).build(),
                    new Payload(content));
            jwe.encrypt(encrypter);
            return jwe.serialize();
        }
        catch (JOSEException failure)
        {
            throw new IllegalStateException("Could not encrypt the export",
                    failure);
        }
    }
}
