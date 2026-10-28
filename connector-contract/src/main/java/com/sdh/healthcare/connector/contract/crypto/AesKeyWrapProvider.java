package com.sdh.healthcare.connector.contract.crypto;

import java.security.GeneralSecurityException;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * KEK wrapping with AES key wrap (RFC 5649, "AES/KWP/NoPadding") using in-process AES-256 keys.
 * Used by the {@code local} provider (keys loaded from a PKCS#12 keystore) and in tests.
 */
public final class AesKeyWrapProvider implements KeyProvider {
    private static final String ALG = "AES/KWP/NoPadding";

    private final Map<String, SecretKey> keks;

    public AesKeyWrapProvider(Map<String, SecretKey> keks) {
        this.keks = Map.copyOf(keks);
    }

    @Override
    public WrappedKey wrap(String keyRef, byte[] dataKey) {
        try {
            Cipher c = Cipher.getInstance(ALG);
            c.init(Cipher.WRAP_MODE, kek(keyRef));
            return new WrappedKey(keyRef, c.wrap(new SecretKeySpec(dataKey, "AES")));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Key wrap failed", e);
        }
    }

    @Override
    public byte[] unwrap(WrappedKey wrapped) {
        try {
            Cipher c = Cipher.getInstance(ALG);
            c.init(Cipher.UNWRAP_MODE, kek(wrapped.keyRef()));
            return c.unwrap(wrapped.wrapped(), "AES", Cipher.SECRET_KEY).getEncoded();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Key unwrap failed", e);
        }
    }

    private SecretKey kek(String keyRef) {
        SecretKey k = keks.get(keyRef);
        if (k == null) {
            throw new IllegalArgumentException("Unknown KEK reference");
        }
        return k;
    }
}
