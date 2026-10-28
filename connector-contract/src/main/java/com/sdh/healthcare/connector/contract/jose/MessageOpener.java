package com.sdh.healthcare.connector.contract.jose;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEDecrypter;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.crypto.ECDHDecrypter;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.crypto.RSADecrypter;
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
 * Decrypts and verifies a sealed message. Only the algorithms the connector produces are accepted,
 * and keys are chosen by {@code kid} from fixed maps, so headers cannot pick a weaker algorithm or key.
 */
public final class MessageOpener {
    private static final Set<JWEAlgorithm> JWE_ALGS = Set.of(JWEAlgorithm.ECDH_ES_A256KW, JWEAlgorithm.RSA_OAEP_256);

    private final Map<String, PrivateKey> recipientKeys;
    private final Map<String, ECPublicKey> trustedSigners;

    /**
     * @param recipientKeys our private keys by kid (several during a key rotation)
     * @param trustedSigners connector signing public keys by kid
     */
    public MessageOpener(Map<String, PrivateKey> recipientKeys, Map<String, ECPublicKey> trustedSigners) {
        if (recipientKeys.isEmpty() || trustedSigners.isEmpty()) {
            throw new IllegalArgumentException("At least one recipient key and one trusted signer are required");
        }
        this.recipientKeys = Map.copyOf(recipientKeys);
        this.trustedSigners = Map.copyOf(trustedSigners);
    }

    public ConnectorMessage open(byte[] body) throws MessageRejectedException {
        JWEObject jwe;
        try {
            jwe = JWEObject.parse(new String(body, StandardCharsets.US_ASCII));
        } catch (ParseException e) {
            throw new MessageRejectedException(MessageRejectedException.Reason.MALFORMED, e);
        }
        if (!JWE_ALGS.contains(jwe.getHeader().getAlgorithm())
                || !EncryptionMethod.A256GCM.equals(jwe.getHeader().getEncryptionMethod())) {
            throw new MessageRejectedException(MessageRejectedException.Reason.ALGORITHM_NOT_ALLOWED);
        }
        PrivateKey key = recipientKeys.get(jwe.getHeader().getKeyID());
        if (key == null) {
            throw new MessageRejectedException(MessageRejectedException.Reason.UNKNOWN_RECIPIENT_KEY);
        }
        try {
            jwe.decrypt(decrypter(key, jwe.getHeader().getAlgorithm()));
        } catch (JOSEException | IllegalArgumentException e) {
            throw new MessageRejectedException(MessageRejectedException.Reason.DECRYPT_FAILED, e);
        }

        JWSObject jws = jwe.getPayload().toJWSObject();
        if (jws == null) {
            throw new MessageRejectedException(MessageRejectedException.Reason.MALFORMED);
        }
        if (!JWSAlgorithm.ES256.equals(jws.getHeader().getAlgorithm())
                || !MessageSealer.JWS_TYPE.equals(jws.getHeader().getType())) {
            throw new MessageRejectedException(MessageRejectedException.Reason.ALGORITHM_NOT_ALLOWED);
        }
        ECPublicKey signerKey = trustedSigners.get(jws.getHeader().getKeyID());
        if (signerKey == null) {
            throw new MessageRejectedException(MessageRejectedException.Reason.UNKNOWN_SIGNER);
        }
        try {
            JWSVerifier verifier = new ECDSAVerifier(signerKey);
            if (!jws.verify(verifier)) {
                throw new MessageRejectedException(MessageRejectedException.Reason.BAD_SIGNATURE);
            }
        } catch (JOSEException e) {
            throw new MessageRejectedException(MessageRejectedException.Reason.BAD_SIGNATURE, e);
        }

        try {
            return PayloadCodec.fromJson(jws.getPayload().toJSONObject());
        } catch (RuntimeException e) {
            throw new MessageRejectedException(MessageRejectedException.Reason.BAD_PAYLOAD, e);
        }
    }

    private static JWEDecrypter decrypter(PrivateKey key, JWEAlgorithm alg) throws JOSEException {
        return switch (key) {
            case ECPrivateKey ec when alg.equals(JWEAlgorithm.ECDH_ES_A256KW) -> new ECDHDecrypter(ec);
            case RSAPrivateKey rsa when alg.equals(JWEAlgorithm.RSA_OAEP_256) -> new RSADecrypter(rsa);
            default -> throw new JOSEException("Key type does not match algorithm");
        };
    }
}
