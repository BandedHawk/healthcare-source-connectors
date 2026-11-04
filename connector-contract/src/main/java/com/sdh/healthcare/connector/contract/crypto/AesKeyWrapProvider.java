/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.crypto;

import java.security.GeneralSecurityException;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * KEK wrapping with AES key wrap (RFC 5649, {@code AES/KWP/NoPadding})
 * using AES-256 keys held in the process. Used by the {@code local}
 * provider (keys loaded from a PKCS#12 keystore) and in tests.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AesKeyWrapProvider implements KeyProvider
{

    /** The cipher: AES key wrap with padding. */
    private static final String ALGORITHM = "AES/KWP/NoPadding";

    /** The KEKs, by reference. */
    private final Map<String, SecretKey> keks;

    /**
     * Creates a provider.
     *
     * @param keks the KEKs, by reference
     */
    public AesKeyWrapProvider(Map<String, SecretKey> keks)
    {
        this.keks = Map.copyOf(keks);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the KEK is unknown
     * @throws IllegalStateException    if wrapping fails
     */
    @Override
    public WrappedKey wrap(String keyRef, byte[] dataKey)
    {
        try
        {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.WRAP_MODE, kek(keyRef));
            return new WrappedKey(keyRef,
                    cipher.wrap(new SecretKeySpec(dataKey, "AES")));
        }
        catch (GeneralSecurityException failure)
        {
            throw new IllegalStateException("Key wrap failed", failure);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the KEK is unknown
     * @throws IllegalStateException    if unwrapping fails
     */
    @Override
    public byte[] unwrap(WrappedKey wrapped)
    {
        try
        {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.UNWRAP_MODE, kek(wrapped.keyRef()));
            return cipher.unwrap(wrapped.wrapped(), "AES", Cipher.SECRET_KEY)
                    .getEncoded();
        }
        catch (GeneralSecurityException failure)
        {
            throw new IllegalStateException("Key unwrap failed", failure);
        }
    }

    /**
     * Finds a KEK.
     *
     * @param keyRef its reference
     * @return the KEK
     * @throws IllegalArgumentException if it is unknown
     */
    private SecretKey kek(String keyRef)
    {
        SecretKey kek = keks.get(keyRef);
        if (kek == null)
        {
            throw new IllegalArgumentException("Unknown KEK reference");
        }
        return kek;
    }
}
