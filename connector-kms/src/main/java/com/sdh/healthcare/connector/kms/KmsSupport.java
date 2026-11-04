/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.kms;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.crypto.impl.ECDSA;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helpers shared by the KMS providers.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class KmsSupport
{

    /**
     * Bound to every wrapped data key where the KMS supports it, so other
     * ciphertexts can't pose as one.
     */
    static final String PURPOSE = "sdh-batch-data-key";

    /** {@link #PURPOSE} as ASCII bytes. */
    static final byte[] PURPOSE_BYTES =
            PURPOSE.getBytes(StandardCharsets.US_ASCII);

    /** A PEM public key; the group is its Base64 body. */
    private static final Pattern PEM = Pattern.compile(
            "-----BEGIN PUBLIC KEY-----([^-]+)-----END PUBLIC KEY-----");

    /**
     * Not instantiated.
     */
    private KmsSupport()
    {
    }

    /**
     * ES256: a DER-encoded ECDSA signature (AWS, GCP) → JOSE raw r‖s, 64
     * bytes.
     *
     * @param der the DER signature
     * @return the raw signature
     * @throws IllegalStateException if the signature is malformed
     */
    static byte[] derToRaw(byte[] der)
    {
        try
        {
            return ECDSA.transcodeSignatureToConcat(der, 64);
        }
        catch (JOSEException malformed)
        {
            throw new IllegalStateException(
                    "KMS returned a malformed ECDSA signature");
        }
    }

    /**
     * Checks that a raw signature is the 64 bytes ES256 needs.
     *
     * @param raw the signature
     * @return the signature
     * @throws IllegalStateException if it is another length
     */
    static byte[] requireRaw(byte[] raw)
    {
        if (raw.length != 64)
        {
            throw new IllegalStateException("KMS returned a " + raw.length
                    + "-byte signature; ES256 needs 64");
        }
        return raw;
    }

    /**
     * Reads an EC or RSA public key from its DER SubjectPublicKeyInfo.
     *
     * @param spki the encoded key
     * @return the key
     * @throws IllegalStateException if it is neither
     */
    static PublicKey publicKeyFromDer(byte[] spki)
    {
        X509EncodedKeySpec spec = new X509EncodedKeySpec(spki);
        try
        {
            return KeyFactory.getInstance("EC").generatePublic(spec);
        }
        catch (InvalidKeySpecException notEc)
        {
            try
            {
                return KeyFactory.getInstance("RSA").generatePublic(spec);
            }
            catch (GeneralSecurityException failure)
            {
                throw new IllegalStateException(
                        "Unreadable public key from KMS", failure);
            }
        }
        catch (GeneralSecurityException failure)
        {
            throw new IllegalStateException("Unreadable public key from KMS",
                    failure);
        }
    }

    /**
     * Reads a public key from the first PEM block in some text.
     *
     * @param pem the text
     * @return the key
     * @throws IllegalStateException if there is no PEM public key, or it is
     *         unreadable
     */
    static PublicKey publicKeyFromPem(String pem)
    {
        Matcher matcher = PEM.matcher(pem);
        if (!matcher.find())
        {
            throw new IllegalStateException("KMS returned no PEM public key");
        }
        return publicKeyFromDer(
                Base64.getMimeDecoder().decode(matcher.group(1)));
    }

    /**
     * An exception for a failed KMS call.
     *
     * @param operation the call, named for operators
     * @param cause what the KMS client threw
     * @return the exception to throw
     */
    static IllegalStateException failed(String operation, Exception cause)
    {
        // The KMS's own message names keys and requests, never data, so it
        // is kept for operators.
        return new IllegalStateException(
                operation + " failed: " + cause.getMessage(), cause);
    }

    /**
     * An I/O exception with a message.
     *
     * @param message the message
     * @return the exception
     */
    static IOException io(String message)
    {
        return new IOException(message);
    }
}
