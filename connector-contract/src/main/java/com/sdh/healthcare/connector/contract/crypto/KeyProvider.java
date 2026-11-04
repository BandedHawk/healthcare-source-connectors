/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.crypto;

import java.security.PublicKey;

/**
 * Key operations backed by a KMS or a local keystore. Implementations:
 * aws-kms, gcp-kms, azure-keyvault, vault-transit, local. Readers only
 * need {@link #unwrap}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface KeyProvider
{

    /**
     * Wraps a data key with a KEK.
     *
     * @param keyRef  the KEK
     * @param dataKey the data key
     * @return the wrapped key
     */
    WrappedKey wrap(String keyRef, byte[] dataKey);

    /**
     * Unwraps a data key with the KEK that wrapped it.
     *
     * @param wrapped the wrapped key
     * @return the data key
     */
    byte[] unwrap(WrappedKey wrapped);

    /**
     * Signs a SHA-256 digest with ES256.
     *
     * @param keyRef the signing key
     * @param digest the digest
     * @return the signature, as raw r‖s, 64 bytes
     * @throws UnsupportedOperationException if the provider doesn't sign
     */
    default byte[] sign(String keyRef, byte[] digest)
    {
        throw new UnsupportedOperationException(
                "This provider does not sign");
    }

    /**
     * The public half of a signing key.
     *
     * @param keyRef the signing key
     * @return its public key
     * @throws UnsupportedOperationException if the provider doesn't expose
     *                                       public keys
     */
    default PublicKey publicKey(String keyRef)
    {
        throw new UnsupportedOperationException(
                "This provider does not expose public keys");
    }
}
