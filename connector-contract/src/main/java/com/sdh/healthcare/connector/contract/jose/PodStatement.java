/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.jose;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException.Reason;
import java.security.interfaces.ECPublicKey;
import java.text.ParseException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A stage's statement about one pod's signing key (docs/PERFORMANCE.md
 * §3.5): signed by the stage's own key, through its KMS, it says that the
 * pod's public key may sign the stage's messages from {@code notBefore} to
 * {@code notAfter}. Each message the pod signs carries it, so a reader
 * needs nothing but the message and the stage's public key, which it
 * already trusts, even days later.
 * <p>
 * It holds no PHI: a public key, the stage's and pod's names, and times.
 *
 * @param id        the statement's own ID, by which it can be revoked
 * @param subject   the stage the pod signs for
 * @param pod       the pod, for audit
 * @param key       the pod's public signing key, EC P-256
 * @param notBefore the first instant a message may be signed under it
 * @param notAfter  the last instant a message may be signed under it
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record PodStatement(String id, String subject, String pod,
        ECPublicKey key, Instant notBefore, Instant notAfter)
{

    /** The statement's JWS type. */
    public static final JOSEObjectType TYPE =
            new JOSEObjectType("sdh-pod-key+jws");

    /**
     * Signs the statement with the stage's key.
     *
     * @param stage    signs for the stage, such as through its KMS
     * @param stageKid the stage key's ID, which readers trust
     * @return the statement, as a compact JWS
     * @throws IllegalStateException if it can't be signed
     */
    public String sign(JWSSigner stage, String stageKid)
    {
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("jti", id);
        claims.put("sub", subject);
        claims.put("pod", pod);
        claims.put("jwk", new ECKey.Builder(Curve.P_256, key).build()
                .toPublicJWK().toJSONObject());
        claims.put("nbf", notBefore.getEpochSecond());
        claims.put("exp", notAfter.getEpochSecond());
        try
        {
            JWSObject jws = new JWSObject(
                    new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(stageKid)
                            .type(TYPE).build(),
                    new Payload(JSONObjectUtils.toJSONString(claims)));
            jws.sign(stage);
            return jws.serialize();
        }
        catch (JOSEException failure)
        {
            throw new IllegalStateException(
                    "Could not sign the pod's statement", failure);
        }
    }

    /**
     * Reads a statement and checks that a stage trusted under a key ID
     * signed it.
     *
     * @param compact   the statement, as a compact JWS
     * @param stageKid  the key ID the message names, which the statement
     *                  must name too
     * @param stageKey  that stage key, which must have signed it
     * @return the statement
     * @throws MessageRejectedException if it isn't well formed, names
     *                                  another key, or doesn't verify
     */
    public static PodStatement verify(String compact, String stageKid,
            ECPublicKey stageKey) throws MessageRejectedException
    {
        JWSObject jws;
        try
        {
            jws = JWSObject.parse(compact);
        }
        catch (ParseException malformed)
        {
            throw new MessageRejectedException(Reason.STATEMENT_INVALID,
                    malformed);
        }
        if (!JWSAlgorithm.ES256.equals(jws.getHeader().getAlgorithm())
                || !TYPE.equals(jws.getHeader().getType()))
        {
            throw new MessageRejectedException(Reason.ALGORITHM_NOT_ALLOWED);
        }
        if (!stageKid.equals(jws.getHeader().getKeyID()))
        {
            throw new MessageRejectedException(Reason.STATEMENT_INVALID);
        }
        try
        {
            if (!jws.verify(new ECDSAVerifier(stageKey)))
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
            Map<String, Object> claims = jws.getPayload().toJSONObject();
            ECKey jwk = ECKey.parse(
                    JSONObjectUtils.getJSONObject(claims, "jwk"));
            if (!Curve.P_256.equals(jwk.getCurve()) || jwk.isPrivate())
            {
                throw new MessageRejectedException(Reason.STATEMENT_INVALID);
            }
            String id = JSONObjectUtils.getString(claims, "jti");
            String subject = JSONObjectUtils.getString(claims, "sub");
            if (id == null || subject == null)
            {
                throw new MessageRejectedException(Reason.STATEMENT_INVALID);
            }
            Instant notBefore = Instant.ofEpochSecond(
                    JSONObjectUtils.getLong(claims, "nbf"));
            Instant notAfter = Instant.ofEpochSecond(
                    JSONObjectUtils.getLong(claims, "exp"));
            if (notAfter.isBefore(notBefore))
            {
                throw new MessageRejectedException(Reason.STATEMENT_INVALID);
            }
            return new PodStatement(id, subject,
                    JSONObjectUtils.getString(claims, "pod"),
                    jwk.toECPublicKey(), notBefore, notAfter);
        }
        catch (ParseException | JOSEException | RuntimeException malformed)
        {
            throw new MessageRejectedException(Reason.STATEMENT_INVALID,
                    malformed);
        }
    }

    /**
     * Tells whether a message signed at an instant falls under the
     * statement.
     *
     * @param signedAt when the message says it was signed
     * @return true if it is within the statement's validity
     */
    public boolean covers(Instant signedAt)
    {
        return !signedAt.isBefore(notBefore) && !signedAt.isAfter(notAfter);
    }
}
