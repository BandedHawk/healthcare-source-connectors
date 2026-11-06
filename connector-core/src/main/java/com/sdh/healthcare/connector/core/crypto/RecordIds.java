/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.crypto;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Works out record IDs (design §7.3):
 * {@code recordId = base32(HMAC-SHA-256(idKey, recordPlaintext))}. The
 * same record always gets the same ID, which downstream systems use to
 * spot duplicates, and without the ID key the ID says nothing about the
 * content. Also makes the ID key's other MACs, such as checkpoints'.
 * Thread-safe.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RecordIds
{

    /** The HMAC algorithm. */
    private static final String ALGORITHM = "HmacSHA256";

    /** How long the ID key must be: 32 bytes, 256 bits. */
    private static final int KEY_BYTES = 32;

    /** The 32 characters of base32 (RFC 4648), in lower case. */
    private static final char[] ALPHABET =
            "abcdefghijklmnopqrstuvwxyz234567".toCharArray();

    /** How many bits a base32 character encodes. */
    private static final int BITS_PER_CHARACTER = 5;

    /** Picks the low 5 bits: one base32 character's worth. */
    private static final int CHARACTER_MASK = 0b11111;

    /** The ID key. */
    private final SecretKeySpec idKey;

    /**
     * Creates IDs with an ID key.
     *
     * @param idKey the ID key ({@link IdKeys})
     * @throws IllegalArgumentException if the key isn't 256 bits
     */
    public RecordIds(byte[] idKey)
    {
        if (idKey.length != KEY_BYTES)
        {
            throw new IllegalArgumentException("ID key must be 256 bits");
        }
        this.idKey = new SecretKeySpec(idKey, ALGORITHM);
    }

    /**
     * Makes an HMAC with the ID key, as record IDs use.
     *
     * @return a new HMAC, ready for input
     */
    public Mac newMac()
    {
        try
        {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(idKey);
            return mac;
        }
        catch (GeneralSecurityException unavailable)
        {
            throw new IllegalStateException(unavailable);
        }
    }

    /**
     * Makes an HMAC with the ID key that has already been given a label and
     * a zero byte. It is used for checkpoint MACs and fingerprints, so those
     * values can never be the same as a record ID, which has no label
     * (design §7.3).
     *
     * @param label what the MAC is for, in ASCII
     * @return a new HMAC, ready for the rest of its input
     */
    public Mac newLabelledMac(String label)
    {
        Mac mac = newMac();
        mac.update(label.getBytes(StandardCharsets.US_ASCII));
        mac.update((byte) 0);
        return mac;
    }

    /**
     * Works out a record's ID.
     *
     * @param plaintext the record's plaintext
     * @return its ID: 52 base32 characters
     */
    public String of(byte[] plaintext)
    {
        return encode(newMac().doFinal(plaintext));
    }

    /**
     * Writes bytes in base32 (RFC 4648), in lower case and without padding.
     *
     * @param bytes the bytes
     * @return the base32 text
     */
    public static String encode(byte[] bytes)
    {
        StringBuilder text = new StringBuilder(
                (bytes.length * Byte.SIZE + BITS_PER_CHARACTER - 1)
                        / BITS_PER_CHARACTER);
        // Bits not yet written, at the low end of pending.
        int pending = 0;
        int pendingBits = 0;
        for (byte value : bytes)
        {
            pending = (pending << Byte.SIZE) | (value & 0xff);
            pendingBits += Byte.SIZE;
            while (pendingBits >= BITS_PER_CHARACTER)
            {
                pendingBits -= BITS_PER_CHARACTER;
                text.append(ALPHABET[(pending >>> pendingBits)
                        & CHARACTER_MASK]);
            }
        }
        if (pendingBits > 0)
        {
            // The last few bits, padded with zeros to a whole character.
            text.append(ALPHABET[(pending << (BITS_PER_CHARACTER
                    - pendingBits)) & CHARACTER_MASK]);
        }
        return text.toString();
    }
}
