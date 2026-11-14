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
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    /** The IDs of pod statements whose messages are refused. */
    private final Set<String> revoked;

    /** The most verified pod statements kept. */
    private static final int STATEMENTS_KEPT = 4096;

    /**
     * Pod statements already verified, by their compact form, so each is
     * verified once (docs/PERFORMANCE.md §3.5).
     */
    private final Map<String, PodStatement> verified =
            new ConcurrentHashMap<>();

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
        this(recipientKeys, trustedSigners, Set.of());
    }

    /**
     * Creates an opener that also refuses messages signed under revoked
     * pod statements.
     *
     * @param recipientKeys  this reader's private keys by key ID
     * @param trustedSigners the trusted signing public keys by key ID
     * @param revoked        the IDs of revoked pod statements
     * @throws IllegalArgumentException if either map is empty
     */
    public MessageOpener(Map<String, PrivateKey> recipientKeys,
            Map<String, ECPublicKey> trustedSigners, Set<String> revoked)
    {
        this.revoked = Set.copyOf(revoked);
        if (recipientKeys.isEmpty() || trustedSigners.isEmpty())
        {
            throw new IllegalArgumentException("At least one recipient key"
                    + " and one trusted signer are required");
        }
        this.recipientKeys = Map.copyOf(recipientKeys);
        this.trustedSigners = Map.copyOf(trustedSigners);
    }

    /**
     * A message opened, with the key that signed it.
     *
     * @param message   the message
     * @param signerKid the ID of the trusted key that signed it
     */
    public record Opened(ConnectorMessage message, String signerKid)
    {
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
        return openSigned(body).message();
    }

    /**
     * Decrypts a message, verifies its signature, and reads its payload,
     * saying which trusted key signed it: for a reader that trusts more
     * than one, and takes some types from one only (STAGES.md §15.8.4).
     *
     * @param body the JWE compact serialisation, as ASCII bytes
     * @return the message and its signer
     * @throws MessageRejectedException if it can't be trusted or read; the
     *                                  reason says why
     */
    public Opened openSigned(byte[] body) throws MessageRejectedException
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
        String signerKid = jws.getHeader().getKeyID();
        ECPublicKey signerKey = signerKid == null ? null
                : trustedSigners.get(signerKid);
        if (signerKey == null)
        {
            throw new MessageRejectedException(Reason.UNKNOWN_SIGNER);
        }
        Object statement =
                jws.getHeader().getCustomParam(MessageSealer.STATEMENT_PARAM);
        if (statement != null)
        {
            signerKey = podKey(statement, signerKid, signerKey,
                    jws.getHeader().getCustomParam(
                            MessageSealer.SIGNED_AT_PARAM));
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
            return new Opened(PayloadCodec.fromJson(
                    jws.getPayload().toJSONObject()), signerKid);
        }
        catch (RuntimeException failure)
        {
            throw new MessageRejectedException(Reason.BAD_PAYLOAD, failure);
        }
    }

    /**
     * The pod key a message is signed with, once its statement is shown
     * to be the stage's, not revoked, and to cover the time the message
     * says it was signed (docs/PERFORMANCE.md §3.5). The time is the
     * signing time, not now, so a message queued for days still opens.
     *
     * @param statement the statement header parameter
     * @param stageKid  the stage key's ID, as the message names it
     * @param stageKey  that stage key
     * @param signedAt  the signing time header parameter
     * @return the pod's public key
     * @throws MessageRejectedException if any check fails
     */
    private ECPublicKey podKey(Object statement, String stageKid,
            ECPublicKey stageKey, Object signedAt)
            throws MessageRejectedException
    {
        if (!(statement instanceof String compact)
                || !(signedAt instanceof Number seconds))
        {
            throw new MessageRejectedException(Reason.STATEMENT_INVALID);
        }
        PodStatement known = verified.get(compact);
        if (known == null)
        {
            known = PodStatement.verify(compact, stageKid, stageKey);
            if (verified.size() >= STATEMENTS_KEPT)
            {
                verified.clear();
            }
            verified.put(compact, known);
        }
        if (revoked.contains(known.id()))
        {
            throw new MessageRejectedException(Reason.REVOKED_SIGNER);
        }
        if (!known.covers(Instant.ofEpochSecond(seconds.longValue())))
        {
            throw new MessageRejectedException(Reason.STATEMENT_INVALID);
        }
        return known.key();
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
