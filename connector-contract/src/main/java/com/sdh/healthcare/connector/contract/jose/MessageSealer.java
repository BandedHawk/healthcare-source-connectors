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

/**
 * Signs a message (JWS, ES256) and encrypts the signed object to one recipient (JWE).
 * EC recipients use ECDH-ES+A256KW, RSA recipients RSA-OAEP-256; content is always A256GCM.
 */
public final class MessageSealer {
    public static final JOSEObjectType JWS_TYPE = new JOSEObjectType("sdh-msg+jws");
    public static final String JWE_CONTENT_TYPE = "sdh-msg+jws";

    private final JWSSigner signer;
    private final String signerKid;
    private final JWEEncrypter encrypter;
    private final JWEAlgorithm jweAlg;
    private final String recipientKid;

    public MessageSealer(JWSSigner signer, String signerKid, PublicKey recipient, String recipientKid) {
        this.signer = signer;
        this.signerKid = signerKid;
        this.recipientKid = recipientKid;
        try {
            switch (recipient) {
                case ECPublicKey ec -> {
                    this.encrypter = new ECDHEncrypter(ec);
                    this.jweAlg = JWEAlgorithm.ECDH_ES_A256KW;
                }
                case RSAPublicKey rsa -> {
                    this.encrypter = new RSAEncrypter(rsa);
                    this.jweAlg = JWEAlgorithm.RSA_OAEP_256;
                }
                default -> throw new IllegalArgumentException("Recipient key must be EC or RSA");
            }
        } catch (JOSEException e) {
            throw new IllegalArgumentException("Unusable recipient key", e);
        }
    }

    public byte[] seal(ConnectorMessage message) {
        try {
            JWSObject jws = new JWSObject(
                    new JWSHeader.Builder(JWSAlgorithm.ES256)
                            .keyID(signerKid)
                            .type(JWS_TYPE)
                            .build(),
                    new Payload(JSONObjectUtils.toJSONString(PayloadCodec.toJson(message))));
            jws.sign(signer);
            JWEObject jwe = new JWEObject(
                    new JWEHeader.Builder(jweAlg, EncryptionMethod.A256GCM)
                            .keyID(recipientKid)
                            .contentType(JWE_CONTENT_TYPE)
                            .build(),
                    new Payload(jws));
            jwe.encrypt(encrypter);
            return jwe.serialize().getBytes(StandardCharsets.US_ASCII);
        } catch (JOSEException e) {
            throw new IllegalStateException("Could not seal message", e);
        }
    }
}
