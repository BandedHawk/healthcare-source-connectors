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
 * ES256 JWS signer whose private key stays in a KMS (design §7.1): the SHA-256 digest of the signing input is sent to
 * {@link KeyProvider#sign}, which returns raw r‖s. Used by {@code MessageSealer} like any Nimbus signer.
 */
public final class KmsJwsSigner implements JWSSigner {
    private final KeyProvider kms;
    private final String keyRef;
    private final JCAContext jca = new JCAContext();

    public KmsJwsSigner(KeyProvider kms, String keyRef) {
        this.kms = kms;
        this.keyRef = keyRef;
    }

    @Override
    public Base64URL sign(JWSHeader header, byte[] signingInput) throws JOSEException {
        if (!JWSAlgorithm.ES256.equals(header.getAlgorithm())) {
            throw new JOSEException("Only ES256 is supported");
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(signingInput);
            return Base64URL.encode(KmsSupport.requireRaw(kms.sign(keyRef, digest)));
        } catch (NoSuchAlgorithmException e) {
            throw new JOSEException("SHA-256 unavailable", e);
        } catch (RuntimeException e) {
            throw new JOSEException("KMS signing failed", e);
        }
    }

    @Override
    public Set<JWSAlgorithm> supportedJWSAlgorithms() {
        return Set.of(JWSAlgorithm.ES256);
    }

    @Override
    public JCAContext getJCAContext() {
        return jca;
    }
}
