/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.contract.crypto;

import java.security.PublicKey;

/**
 * Key operations backed by a KMS or a local keystore. Implementations: aws-kms, gcp-kms,
 * azure-keyvault, vault-transit, local. Readers only need {@link #unwrap}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface KeyProvider {

    WrappedKey wrap(String keyRef, byte[] dataKey);

    byte[] unwrap(WrappedKey wrapped);

    /** ES256 signature (raw r‖s, 64 bytes) over a SHA-256 digest. */
    default byte[] sign(String keyRef, byte[] digest) {
        throw new UnsupportedOperationException("This provider does not sign");
    }

    default PublicKey publicKey(String keyRef) {
        throw new UnsupportedOperationException("This provider does not expose public keys");
    }
}
