/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.kms;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.jca.JCAContext;
import com.nimbusds.jose.util.Base64URL;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

/**
 * ES256 JWS signer whose private key stays in a KMS (design §7.1): the
 * SHA-256 digest of the signing input is sent to {@link KeyProvider#sign},
 * which returns raw r‖s. Used by {@code MessageSealer} like any Nimbus
 * signer.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class KmsJwsSigner implements JWSSigner
{

    /** The KMS holding the private key. */
    private final KeyProvider kms;

    /** The signing key's reference in the KMS. */
    private final String keyRef;

    /** Nimbus's JCA context; unused, since the KMS does the signing. */
    private final JCAContext jca = new JCAContext();

    /**
     * Creates a signer.
     *
     * @param kms the KMS holding the private key
     * @param keyRef the signing key's reference in the KMS
     */
    public KmsJwsSigner(KeyProvider kms, String keyRef)
    {
        this.kms = kms;
        this.keyRef = keyRef;
    }

    /**
     * Signs with the KMS key.
     *
     * @param header the JWS header, which must name ES256
     * @param signingInput the bytes to sign
     * @return the raw r‖s signature
     * @throws JOSEException if the algorithm isn't ES256 or the KMS fails
     */
    @Override
    public Base64URL sign(JWSHeader header, byte[] signingInput)
            throws JOSEException
    {
        if (!JWSAlgorithm.ES256.equals(header.getAlgorithm()))
        {
            throw new JOSEException("Only ES256 is supported");
        }
        try
        {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(signingInput);
            return Base64URL.encode(
                    KmsSupport.requireRaw(kms.sign(keyRef, digest)));
        }
        catch (NoSuchAlgorithmException failure)
        {
            throw new JOSEException("SHA-256 unavailable", failure);
        }
        catch (RuntimeException failure)
        {
            throw new JOSEException("KMS signing failed", failure);
        }
    }

    /**
     * The algorithms supported: ES256 only.
     *
     * @return ES256
     */
    @Override
    public Set<JWSAlgorithm> supportedJWSAlgorithms()
    {
        return Set.of(JWSAlgorithm.ES256);
    }

    /**
     * Nimbus's JCA context.
     *
     * @return the context
     */
    @Override
    public JCAContext getJCAContext()
    {
        return jca;
    }
}
