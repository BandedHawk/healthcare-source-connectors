/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.jose;

import com.nimbusds.jose.CriticalHeaderParamsAware;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEDecrypter;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.crypto.impl.CriticalHeaderParamsDeferral;
import com.nimbusds.jose.crypto.impl.ECDH;
import com.nimbusds.jose.crypto.impl.ECDHCryptoProvider;
import com.nimbusds.jose.crypto.utils.ECChecks;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.util.Base64URL;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.SecretKey;

/**
 * Opens ECDH-ES+A256KW messages with one private key, agreeing a secret
 * once for each sender's ephemeral key rather than once a message
 * (docs/PERFORMANCE.md §13). It opens any standard message of the
 * algorithm: one whose ephemeral key is new is checked, as the library's
 * own decrypter checks it, to be on the private key's curve, and its
 * agreement kept; the next message with that key reuses it. Each
 * message's content key is still unwrapped and checked, and its content
 * authenticated, on its own. Agreements are kept in memory only, up to
 * {@link #AGREEMENTS_KEPT}; then the lot is forgotten and agreed again
 * as needed.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class SharedAgreementDecrypter extends ECDHCryptoProvider
        implements JWEDecrypter, CriticalHeaderParamsAware
{

    /** The most agreements kept: senders' ephemeral keys in use at once. */
    static final int AGREEMENTS_KEPT = 4096;

    /** The private key. */
    private final ECPrivateKey key;

    /** Critical header parameters: none are understood. */
    private final CriticalHeaderParamsDeferral critical =
            new CriticalHeaderParamsDeferral();

    /** Agreed secrets, by the ephemeral key's coordinates. */
    private final Map<String, SecretKey> agreements =
            new ConcurrentHashMap<>();

    /**
     * Creates the decrypter.
     *
     * @param key the private key, on P-256, P-384 or P-521
     * @throws JOSEException if the key's curve isn't supported
     */
    SharedAgreementDecrypter(ECPrivateKey key) throws JOSEException
    {
        super(Curve.forECParameterSpec(key.getParams()), null);
        this.key = key;
    }

    /**
     * {@inheritDoc}
     *
     * @return P-256, P-384 and P-521
     */
    @Override
    public Set<Curve> supportedEllipticCurves()
    {
        return Set.of(Curve.P_256, Curve.P_384, Curve.P_521);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getProcessedCriticalHeaderParams()
    {
        return critical.getProcessedCriticalHeaderParams();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getDeferredCriticalHeaderParams()
    {
        return critical.getDeferredCriticalHeaderParams();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] decrypt(JWEHeader header, Base64URL encryptedKey,
            Base64URL iv, Base64URL cipherText, Base64URL authTag,
            byte[] aad) throws JOSEException
    {
        critical.ensureHeaderPasses(header);
        JWK epk = header.getEphemeralPublicKey();
        if (!(epk instanceof ECKey ephemeral))
        {
            throw new JOSEException("Missing ephemeral public EC key");
        }
        return decryptWithZ(header, aad, agreement(ephemeral), encryptedKey,
                iv, cipherText, authTag);
    }

    /**
     * The secret agreed with a sender's ephemeral key: the one kept, or a
     * new one once the key is shown to be on the curve.
     *
     * @param ephemeral the sender's ephemeral public key
     * @return the secret
     * @throws JOSEException if the key isn't on the curve, or the
     *                       agreement fails
     */
    private SecretKey agreement(ECKey ephemeral) throws JOSEException
    {
        if (!getCurve().equals(ephemeral.getCurve()))
        {
            throw new JOSEException("Ephemeral key on another curve");
        }
        String id = ephemeral.getX() + "." + ephemeral.getY();
        SecretKey z = agreements.get(id);
        if (z != null)
        {
            return z;
        }
        ECPublicKey point = ephemeral.toECPublicKey();
        if (!ECChecks.isPointOnCurve(point, key))
        {
            throw new JOSEException("Ephemeral key not on the curve");
        }
        z = ECDH.deriveSharedSecret(point, key, JoseCrypto.provider());
        if (agreements.size() >= AGREEMENTS_KEPT)
        {
            agreements.clear();
        }
        agreements.put(id, z);
        return z;
    }
}
