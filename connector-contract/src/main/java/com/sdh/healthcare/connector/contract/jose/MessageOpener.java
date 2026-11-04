/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.jose;

import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEDecrypter;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.ECDHDecrypter;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.crypto.RSADecrypter;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException.Reason;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.text.ParseException;
import java.util.Map;
import java.util.Set;

/**
 * Decrypts and verifies a sealed message. Only the algorithms the
 * connectors produce are accepted, and keys are chosen by {@code kid} from
 * fixed maps, so headers can't pick a weaker algorithm or key.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class MessageOpener
{

    /** The key-management algorithms accepted. */
    private static final Set<JWEAlgorithm> JWE_ALGORITHMS = Set.of(
            JWEAlgorithm.ECDH_ES_A256KW, JWEAlgorithm.RSA_OAEP_256);

    /** This reader's private keys, by key ID. */
    private final Map<String, PrivateKey> recipientKeys;

    /** The trusted signing public keys, by key ID. */
    private final Map<String, ECPublicKey> trustedSigners;

    /**
     * Creates an opener.
     *
     * @param recipientKeys  this reader's private keys by key ID (several
     *                       during a key rotation)
     * @param trustedSigners the trusted signing public keys by key ID
     * @throws IllegalArgumentException if either map is empty
     */
    public MessageOpener(Map<String, PrivateKey> recipientKeys,
            Map<String, ECPublicKey> trustedSigners)
    {
        if (recipientKeys.isEmpty() || trustedSigners.isEmpty())
        {
            throw new IllegalArgumentException("At least one recipient key"
                    + " and one trusted signer are required");
        }
        this.recipientKeys = Map.copyOf(recipientKeys);
        this.trustedSigners = Map.copyOf(trustedSigners);
    }

    /**
     * Decrypts a message, verifies its signature, and reads its payload.
     *
     * @param body the JWE compact serialisation, as ASCII bytes
     * @return the message
     * @throws MessageRejectedException if it can't be trusted or read; the
     *                                  reason says why
     */
    public ConnectorMessage open(byte[] body) throws MessageRejectedException
    {
        JWEObject jwe;
        try
        {
            jwe = JWEObject.parse(new String(body, StandardCharsets.US_ASCII));
        }
        catch (ParseException malformed)
        {
            throw new MessageRejectedException(Reason.MALFORMED, malformed);
        }
        if (!JWE_ALGORITHMS.contains(jwe.getHeader().getAlgorithm())
                || !EncryptionMethod.A256GCM.equals(
                        jwe.getHeader().getEncryptionMethod()))
        {
            throw new MessageRejectedException(Reason.ALGORITHM_NOT_ALLOWED);
        }
        PrivateKey key = recipientKeys.get(jwe.getHeader().getKeyID());
        if (key == null)
        {
            throw new MessageRejectedException(Reason.UNKNOWN_RECIPIENT_KEY);
        }
        try
        {
            jwe.decrypt(decrypter(key, jwe.getHeader().getAlgorithm()));
        }
        catch (JOSEException | IllegalArgumentException failure)
        {
            throw new MessageRejectedException(Reason.DECRYPT_FAILED, failure);
        }

        JWSObject jws = jwe.getPayload().toJWSObject();
        if (jws == null)
        {
            throw new MessageRejectedException(Reason.MALFORMED);
        }
        if (!JWSAlgorithm.ES256.equals(jws.getHeader().getAlgorithm())
                || !MessageSealer.JWS_TYPE.equals(jws.getHeader().getType()))
        {
            throw new MessageRejectedException(Reason.ALGORITHM_NOT_ALLOWED);
        }
        ECPublicKey signerKey =
                trustedSigners.get(jws.getHeader().getKeyID());
        if (signerKey == null)
        {
            throw new MessageRejectedException(Reason.UNKNOWN_SIGNER);
        }
        try
        {
            JWSVerifier verifier = new ECDSAVerifier(signerKey);
            if (!jws.verify(verifier))
            {
                throw new MessageRejectedException(Reason.BAD_SIGNATURE);
            }
        }
        catch (JOSEException failure)
        {
            throw new MessageRejectedException(Reason.BAD_SIGNATURE, failure);
        }

        try
        {
            return PayloadCodec.fromJson(jws.getPayload().toJSONObject());
        }
        catch (RuntimeException failure)
        {
            throw new MessageRejectedException(Reason.BAD_PAYLOAD, failure);
        }
    }

    /**
     * The decrypter for a key, if its type matches the algorithm.
     *
     * @param key       the private key
     * @param algorithm the message's key-management algorithm
     * @return the decrypter
     * @throws JOSEException if the key's type doesn't match the algorithm
     */
    private static JWEDecrypter decrypter(PrivateKey key,
            JWEAlgorithm algorithm) throws JOSEException
    {
        return switch (key)
        {
            case ECPrivateKey ecKey
                    when algorithm.equals(JWEAlgorithm.ECDH_ES_A256KW) ->
                    new ECDHDecrypter(ecKey);
            case RSAPrivateKey rsaKey
                    when algorithm.equals(JWEAlgorithm.RSA_OAEP_256) ->
                    new RSADecrypter(rsaKey);
            default -> throw new JOSEException(
                    "Key type does not match algorithm");
        };
    }
}
