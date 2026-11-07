/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.kms;

import com.azure.core.exception.HttpResponseException;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.api.gax.rpc.UnavailableException;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.crypto.impl.ECDSA;
import com.sdh.healthcare.connector.contract.crypto.KeyServiceUnavailableException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.amazon.awssdk.core.exception.SdkServiceException;

/**
 * Helpers shared by the KMS providers.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class KmsSupport
{

    /**
     * Statuses for a KMS not serving: rate limited (429), a gateway failing
     * to reach it (502, 504), unavailable (503).
     */
    private static final Set<Integer> NOT_SERVING = Set.of(429, 502, 503,
            504);

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
     * An exception for a failed KMS call: a
     * {@link KeyServiceUnavailableException} if the KMS couldn't be reached
     * or wasn't serving ({@link #notServing}), so the work is tried again
     * later without being held against the item.
     *
     * @param operation the call, named for operators
     * @param cause what the KMS client threw
     * @return the exception to throw
     */
    static RuntimeException failed(String operation, Exception cause)
    {
        // The KMS's own message names keys and requests, never data, so it
        // is kept for operators.
        if (notServing(cause))
        {
            return new KeyServiceUnavailableException(
                    operation + " unavailable: " + cause.getMessage(),
                    new IOException(cause));
        }
        return new IllegalStateException(
                operation + " failed: " + cause.getMessage(), cause);
    }

    /**
     * Tells whether a KMS client's error means the KMS couldn't be reached
     * or wasn't serving, rather than refusing: down its chain of causes, a
     * connection refused, timed out or unresolved; an AWS or Azure answer
     * of 429, 502, 503 or 504; or gRPC's {@code UNAVAILABLE} or
     * {@code DEADLINE_EXCEEDED} from Cloud KMS.
     *
     * @param error the error
     * @return true if so
     */
    static boolean notServing(Throwable error)
    {
        Throwable current = error;
        for (int depth = 0; current != null && depth < 16; depth++)
        {
            if (current instanceof ConnectException
                    || current instanceof NoRouteToHostException
                    || current instanceof UnknownHostException
                    || current instanceof SocketTimeoutException
                    || current instanceof HttpTimeoutException
                    || current instanceof UnavailableException
                    || current instanceof DeadlineExceededException)
            {
                return true;
            }
            if (current instanceof SdkServiceException aws)
            {
                return aws.isThrottlingException()
                        || NOT_SERVING.contains(aws.statusCode());
            }
            if (current instanceof HttpResponseException azure)
            {
                return azure.getResponse() != null && NOT_SERVING
                        .contains(azure.getResponse().getStatusCode());
            }
            if (current.getCause() == current)
            {
                break;
            }
            current = current.getCause();
        }
        return false;
    }
}
