/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.jose;

import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEEncrypter;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.ECDHEncrypter;
import com.nimbusds.jose.crypto.RSAEncrypter;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Map;

/**
 * Signs a message (JWS, ES256) and encrypts the signed object to one
 * recipient (JWE). EC recipients use ECDH-ES+A256KW, RSA recipients
 * RSA-OAEP-256; the content is always A256GCM.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class MessageSealer
{

    /** The JWS {@code typ} header of every message. */
    public static final JOSEObjectType JWS_TYPE =
            new JOSEObjectType("sdh-msg+jws");

    /** The JWE {@code cty} header: the content is a signed message. */
    public static final String JWE_CONTENT_TYPE = "sdh-msg+jws";

    /** Signs messages. */
    private final JWSSigner signer;

    /** The signing key's ID. */
    private final String signerKid;

    /** Encrypts to the recipient. */
    private final JWEEncrypter encrypter;

    /** The key-management algorithm for the recipient's key type. */
    private final JWEAlgorithm jweAlgorithm;

    /** The recipient key's ID. */
    private final String recipientKid;

    /**
     * Creates a sealer for one signing key and one recipient.
     *
     * @param signer       signs messages
     * @param signerKid    the signing key's ID
     * @param recipient    the recipient's public key, EC or RSA
     * @param recipientKid the recipient key's ID
     * @throws IllegalArgumentException if the recipient key is neither EC
     *                                  nor RSA, or can't be used
     */
    public MessageSealer(JWSSigner signer, String signerKid,
            PublicKey recipient, String recipientKid)
    {
        this.signer = signer;
        this.signerKid = signerKid;
        this.recipientKid = recipientKid;
        try
        {
            switch (recipient)
            {
                case ECPublicKey ecKey ->
                {
                    this.encrypter = new ECDHEncrypter(ecKey);
                    this.jweAlgorithm = JWEAlgorithm.ECDH_ES_A256KW;
                }
                case RSAPublicKey rsaKey ->
                {
                    this.encrypter = new RSAEncrypter(rsaKey);
                    this.jweAlgorithm = JWEAlgorithm.RSA_OAEP_256;
                }
                default -> throw new IllegalArgumentException(
                        "Recipient key must be EC or RSA");
            }
        }
        catch (JOSEException unusable)
        {
            throw new IllegalArgumentException("Unusable recipient key",
                    unusable);
        }
    }

    /**
     * Signs a message, then encrypts it to the recipient.
     *
     * @param message the message
     * @return the JWE compact serialisation, as ASCII bytes
     * @throws IllegalStateException if signing or encryption fails
     */
    public byte[] seal(ConnectorMessage message)
    {
        return seal(PayloadCodec.toJson(message));
    }

    /**
     * Signs a payload as given, then encrypts it to the recipient. A
     * message is sealed with {@link #seal(ConnectorMessage)}; this is for
     * payloads that build couldn't write, such as one of another contract
     * version, to test how readers take them.
     *
     * @param payload the payload's fields
     * @return the JWE compact serialisation, as ASCII bytes
     * @throws IllegalStateException if signing or encryption fails
     */
    public byte[] seal(Map<String, Object> payload)
    {
        try
        {
            JWSObject jws = new JWSObject(
                    new JWSHeader.Builder(JWSAlgorithm.ES256)
                            .keyID(signerKid)
                            .type(JWS_TYPE)
                            .build(),
                    new Payload(JSONObjectUtils.toJSONString(payload)));
            jws.sign(signer);
            JWEObject jwe = new JWEObject(
                    new JWEHeader.Builder(jweAlgorithm,
                            EncryptionMethod.A256GCM)
                            .keyID(recipientKid)
                            .contentType(JWE_CONTENT_TYPE)
                            .build(),
                    new Payload(jws));
            jwe.encrypt(encrypter);
            return jwe.serialize().getBytes(StandardCharsets.US_ASCII);
        }
        catch (JOSEException failure)
        {
            throw new IllegalStateException("Could not seal message", failure);
        }
    }
}
