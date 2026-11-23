/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.jose;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWECryptoParts;
import com.nimbusds.jose.JWEEncrypter;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.crypto.impl.AAD;
import com.nimbusds.jose.crypto.impl.ECDH;
import com.nimbusds.jose.crypto.impl.ECDHCryptoProvider;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Provider;
import java.security.interfaces.ECPublicKey;
import java.util.Arrays;
import java.util.Set;
import javax.crypto.SecretKey;

/**
 * ECDH-ES+A256KW to one recipient, with one key agreement for many messages
 * (docs/PERFORMANCE.md §13). Each message is a standard JWE: its own random
 * content key, wrapped with AES key wrap under the key derived from the
 * agreement, its own IV, and the agreement's ephemeral public key in its
 * header, so any reader of the algorithm opens it, one message at a time.
 * What is shared is the ephemeral key: one for up to
 * {@link JoseCrypto.Settings#agreementMessages()} messages or
 * {@link JoseCrypto.Settings#agreementLifetime()}, whichever comes first,
 * then a new one. It is kept in memory only, as the JDK's own encrypter
 * keeps its one for a message.
 * <p>
 * The agreement (ECDH, and the new key pair) was 75% of a message's
 * sealing; with one agreement a message, as with the settings 1, this is
 * the library's own encrypter's work. The key pair and the agreement use
 * {@link JoseCrypto#provider()}; the key wrap and AES-GCM the JDK.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class SharedAgreementEncrypter extends ECDHCryptoProvider
        implements JWEEncrypter
{

    /** The recipient's public key. */
    private final ECPublicKey recipient;

    /** The agreement in use; null before the first message. */
    private Agreement agreement;

    /**
     * An agreement and how far it has been used.
     *
     * @param ephemeral the ephemeral public key, for the header
     * @param z         the shared secret
     * @param expiresAt when it is no longer used, from
     *                  {@link System#nanoTime()}
     * @param uses      the messages it has encrypted
     */
    private record Agreement(ECKey ephemeral, SecretKey z, long expiresAt,
            int uses)
    {
    }

    /**
     * Creates the encrypter.
     *
     * @param recipient the recipient's public key, on P-256, P-384 or P-521
     * @throws JOSEException if the key's curve isn't supported
     */
    SharedAgreementEncrypter(ECPublicKey recipient) throws JOSEException
    {
        super(Curve.forECParameterSpec(recipient.getParams()), null);
        this.recipient = recipient;
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
     * <p>
     * The header gains the agreement's ephemeral key; additional data
     * computed from the header as given is computed again from the header
     * with it, as the library's own encrypter does.
     */
    @Override
    public JWECryptoParts encrypt(JWEHeader header, byte[] clearText,
            byte[] aad) throws JOSEException
    {
        Agreement current = next();
        JWEHeader updated = new JWEHeader.Builder(header)
                .ephemeralPublicKey(current.ephemeral()).build();
        byte[] data = Arrays.equals(AAD.compute(header), aad)
                ? AAD.compute(updated) : aad;
        return encryptWithZ(updated, current.z(), clearText, data);
    }

    /**
     * The agreement for the next message: the one in use, or a new one
     * once it has encrypted its most or lived its longest.
     *
     * @return it, counted as used once more
     * @throws JOSEException if a new one can't be made
     */
    private synchronized Agreement next() throws JOSEException
    {
        JoseCrypto.Settings settings = JoseCrypto.settings();
        long now = System.nanoTime();
        if (agreement == null
                || agreement.uses() >= settings.agreementMessages()
                || now - agreement.expiresAt() >= 0)
        {
            agreement = agree(now, settings);
        }
        agreement = new Agreement(agreement.ephemeral(), agreement.z(),
                agreement.expiresAt(), agreement.uses() + 1);
        return agreement;
    }

    /**
     * Makes a new ephemeral key pair and agrees a secret with the
     * recipient.
     *
     * @param now      the time, from {@link System#nanoTime()}
     * @param settings the limits
     * @return the agreement, unused
     * @throws JOSEException if the key pair or the agreement fails
     */
    private Agreement agree(long now, JoseCrypto.Settings settings)
            throws JOSEException
    {
        Provider provider = JoseCrypto.provider();
        KeyPair pair;
        try
        {
            KeyPairGenerator generator = provider == null
                    ? KeyPairGenerator.getInstance("EC")
                    : KeyPairGenerator.getInstance("EC", provider);
            generator.initialize(recipient.getParams());
            pair = generator.generateKeyPair();
        }
        catch (GeneralSecurityException failure)
        {
            throw new JOSEException("Couldn't make an ephemeral EC key: "
                    + failure.getMessage(), failure);
        }
        SecretKey z = ECDH.deriveSharedSecret(recipient, pair.getPrivate(),
                provider);
        ECKey ephemeral = new ECKey.Builder(getCurve(),
                (ECPublicKey) pair.getPublic()).build();
        return new Agreement(ephemeral, z,
                now + settings.agreementLifetime().toNanos(), 0);
    }
}
