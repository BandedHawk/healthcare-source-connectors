/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.audit;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.jose.PodSigner;
import com.sdh.healthcare.connector.contract.jose.PodStatement;
import java.security.MessageDigest;
import java.security.interfaces.ECPublicKey;
import java.text.ParseException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A stage's signature over the head of its audit chain (HIPAA.md §3.1,
 * audit controls): a compact JWS, ES256, whose payload names the chain, an
 * entry's number and that entry's hash. The hash covers every entry before
 * it, so a seal vouches for the chain up to it; nobody without the stage's
 * signing key can change those entries and seal them again. Signed as the
 * stage's messages are (DESIGN §10.1): by the stage's key, or by the pod's
 * own key under a statement the stage's key signed (docs/PERFORMANCE.md
 * §3.5), so a seal costs no KMS call while the pod's key lasts.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AuditSeal implements JdbcAuditJournal.Sealer
{

    /** The JWS {@code typ} header of a seal. */
    public static final JOSEObjectType TYPE =
            new JOSEObjectType("sdh-audit-seal+jws");

    /** Signs the seals. */
    private final JWSSigner signer;

    /** The stage key's ID. */
    private final String kid;

    /**
     * Creates a sealer.
     *
     * @param signer signs with the stage's key, or a {@link PodSigner}
     * @param kid    the stage key's ID
     */
    public AuditSeal(JWSSigner signer, String kid)
    {
        this.signer = signer;
        this.kid = kid;
    }

    /**
     * {@inheritDoc}
     *
     * @throws JOSEException if it can't be signed, as when the KMS can't
     *                       be reached
     */
    @Override
    public String seal(UUID chainId, long seq, byte[] hash, Instant at)
            throws JOSEException
    {
        JWSHeader.Builder header = new JWSHeader.Builder(JWSAlgorithm.ES256)
                .keyID(kid).type(TYPE);
        JWSSigner signing = signer;
        if (signer instanceof PodSigner pod)
        {
            PodSigner.Signing current = pod.current();
            header.customParam(MessageSealer.STATEMENT_PARAM,
                    current.statement())
                    .customParam(MessageSealer.SIGNED_AT_PARAM,
                            at.getEpochSecond());
            signing = current.signer();
        }
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("chain", chainId.toString());
        claims.put("seq", seq);
        claims.put("hash", Base64URL.encode(hash).toString());
        claims.put("iat", at.getEpochSecond());
        JWSObject jws = new JWSObject(header.build(),
                new Payload(JSONObjectUtils.toJSONString(claims)));
        jws.sign(signing);
        return jws.serialize();
    }

    /** What checking a seal found. */
    public enum Check
    {
        /** It is the stage's, over this entry. */
        VALID,
        /** Its signature isn't the stage key's, or it is malformed. */
        INVALID,
        /** It is signed, but over another chain, entry or hash. */
        MISMATCH
    }

    /**
     * Checks a seal against the entry it should vouch for.
     *
     * @param compact  the seal
     * @param kid      the stage key's ID
     * @param stageKey the stage key's public half
     * @param chainId  the chain
     * @param seq      the entry's number
     * @param hash     the entry's hash, as the chain gives it
     * @return what the check found
     */
    public static Check verify(String compact, String kid,
            ECPublicKey stageKey, UUID chainId, long seq, byte[] hash)
    {
        try
        {
            JWSObject jws = JWSObject.parse(compact);
            JWSHeader header = jws.getHeader();
            if (!JWSAlgorithm.ES256.equals(header.getAlgorithm())
                    || !TYPE.equals(header.getType())
                    || !kid.equals(header.getKeyID()))
            {
                return Check.INVALID;
            }
            ECPublicKey key = stageKey;
            Object statement =
                    header.getCustomParam(MessageSealer.STATEMENT_PARAM);
            if (statement != null)
            {
                if (!(statement instanceof String text)
                        || !(header.getCustomParam(
                                MessageSealer.SIGNED_AT_PARAM)
                                instanceof Number signedAt))
                {
                    return Check.INVALID;
                }
                PodStatement pod = PodStatement.verify(text, kid, stageKey);
                if (!pod.covers(Instant.ofEpochSecond(signedAt.longValue())))
                {
                    return Check.INVALID;
                }
                key = pod.key();
            }
            if (!jws.verify(new ECDSAVerifier(key)))
            {
                return Check.INVALID;
            }
            Map<String, Object> claims = jws.getPayload().toJSONObject();
            String sealedHash = JSONObjectUtils.getString(claims, "hash");
            boolean same = chainId.toString().equals(
                    JSONObjectUtils.getString(claims, "chain"))
                    && JSONObjectUtils.getLong(claims, "seq") == seq
                    && sealedHash != null
                    && MessageDigest.isEqual(hash,
                            new Base64URL(sealedHash).decode());
            return same ? Check.VALID : Check.MISMATCH;
        }
        catch (ParseException | JOSEException | MessageRejectedException
                | RuntimeException malformed)
        {
            return Check.INVALID;
        }
    }
}
