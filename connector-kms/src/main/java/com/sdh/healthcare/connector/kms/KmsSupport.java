package com.sdh.healthcare.connector.kms;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.crypto.impl.ECDSA;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Helpers shared by the KMS providers. */
final class KmsSupport {
    /** Bound to every wrapped data key where the KMS supports it, so other ciphertexts can't pose as one. */
    static final String PURPOSE = "sdh-batch-data-key";
    static final byte[] PURPOSE_BYTES = PURPOSE.getBytes(StandardCharsets.US_ASCII);

    private static final Pattern PEM = Pattern.compile("-----BEGIN PUBLIC KEY-----([^-]+)-----END PUBLIC KEY-----");

    private KmsSupport() {}

    /** ES256: DER-encoded ECDSA signature (AWS, GCP) → JOSE raw r‖s, 64 bytes. */
    static byte[] derToRaw(byte[] der) {
        try {
            return ECDSA.transcodeSignatureToConcat(der, 64);
        } catch (JOSEException e) {
            throw new IllegalStateException("KMS returned a malformed ECDSA signature");
        }
    }

    static byte[] requireRaw(byte[] raw) {
        if (raw.length != 64) {
            throw new IllegalStateException("KMS returned a " + raw.length + "-byte signature; ES256 needs 64");
        }
        return raw;
    }

    static PublicKey publicKeyFromDer(byte[] spki) {
        X509EncodedKeySpec spec = new X509EncodedKeySpec(spki);
        try {
            return KeyFactory.getInstance("EC").generatePublic(spec);
        } catch (InvalidKeySpecException notEc) {
            try {
                return KeyFactory.getInstance("RSA").generatePublic(spec);
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException("Unreadable public key from KMS", e);
            }
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Unreadable public key from KMS", e);
        }
    }

    static PublicKey publicKeyFromPem(String pem) {
        Matcher m = PEM.matcher(pem);
        if (!m.find()) {
            throw new IllegalStateException("KMS returned no PEM public key");
        }
        return publicKeyFromDer(Base64.getMimeDecoder().decode(m.group(1)));
    }

    static IllegalStateException failed(String op, Exception e) {
        // The KMS's own message names keys and requests, never data, so it is kept for operators.
        return new IllegalStateException(op + " failed: " + e.getMessage(), e);
    }

    static IOException io(String msg) {
        return new IOException(msg);
    }
}
