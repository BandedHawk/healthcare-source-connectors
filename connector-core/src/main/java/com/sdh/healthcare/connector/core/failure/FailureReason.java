/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.failure;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.UUID;

/**
 * An operator's reason for a change to a failure, such as declining to
 * correct it (STAGES.md §15.3), encrypted: it may describe the data. Each
 * reason has a data key of its own, wrapped with the stage's KEK, and is
 * bound to its item, its record number and its event, so it can't be moved
 * to another failure.
 *
 * @param sealed  nonce, ciphertext and tag
 * @param key     the data key, wrapped
 * @param keyRef  the KEK it is wrapped with
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record FailureReason(byte[] sealed, byte[] key, String keyRef)
{

    /** The longest reason taken, in bytes. */
    public static final int MAX_BYTES = 4096;

    /** Makes data keys. */
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * Encrypts a reason.
     *
     * @param keys   wraps the data key
     * @param keyRef the stage's KEK
     * @param itemId the failure's item
     * @param seq    its record number
     * @param event  the event's number for the failure, from 1
     * @param text   the reason
     * @return the reason, encrypted
     * @throws IllegalArgumentException if the reason is empty or too long
     */
    public static FailureReason seal(KeyProvider keys, String keyRef,
            UUID itemId, long seq, int event, String text)
    {
        byte[] plain = text.strip().getBytes(StandardCharsets.UTF_8);
        if (plain.length == 0)
        {
            throw new IllegalArgumentException("A reason is needed");
        }
        if (plain.length > MAX_BYTES)
        {
            throw new IllegalArgumentException(
                    "The reason is longer than " + MAX_BYTES + " bytes");
        }
        byte[] dataKey = new byte[32];
        RANDOM.nextBytes(dataKey);
        try
        {
            WrappedKey wrapped = keys.wrap(keyRef, dataKey);
            byte[] sealed = RecordCrypto.encryptChunk(
                    RecordCrypto.recordKey(dataKey, itemId, seq), itemId,
                    seq, event, event + 1, plain);
            return new FailureReason(sealed, wrapped.wrapped(),
                    wrapped.keyRef());
        }
        finally
        {
            Arrays.fill(dataKey, (byte) 0);
            Arrays.fill(plain, (byte) 0);
        }
    }

    /**
     * Decrypts a reason, for an operator entitled to read it.
     *
     * @param keys   unwraps the data key
     * @param itemId the failure's item
     * @param seq    its record number
     * @param event  the event's number for the failure
     * @return the reason
     */
    public String open(KeyProvider keys, UUID itemId, long seq, int event)
    {
        byte[] dataKey = keys.unwrap(new WrappedKey(keyRef, key));
        try
        {
            return new String(RecordCrypto.decryptSmall(
                    RecordCrypto.recordKey(dataKey, itemId, seq), itemId,
                    seq, event, event + 1, MAX_BYTES, sealed),
                    StandardCharsets.UTF_8);
        }
        finally
        {
            Arrays.fill(dataKey, (byte) 0);
        }
    }
}
