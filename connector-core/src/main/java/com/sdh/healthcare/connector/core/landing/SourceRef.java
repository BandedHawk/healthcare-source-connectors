/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import com.sdh.healthcare.connector.core.crypto.RecordIds;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Mac;

/**
 * Where an item came from, without PHI (design §5.3.2): a keyed hash of its
 * reference, such as a path, object key or message ID, with the size and
 * version the source showed. Paths and keys often hold patient names or
 * MRNs, so only the hash is ever stored in the clear.
 * <p>
 * Two sightings with the same hash, size and version are the same item, and
 * it is landed only once. The same path with another size or version is a
 * new item.
 *
 * @param hash    the keyed hash of the intake and reference, in base32
 * @param size    the source's size in bytes; -1 if the source doesn't say
 * @param version the source's version, such as a modification time in
 *                milliseconds, an ETag or a version ID; empty if it has
 *                none
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record SourceRef(String hash, long size, String version)
{

    /** The label of the reference hash, so it can't equal any other MAC. */
    private static final String LABEL = "sdh-source-ref";

    /** How many bytes of a ledger key are kept: 128 bits. */
    private static final int KEY_BYTES = 16;

    /**
     * Creates a reference, checking its parts.
     *
     * @param hash    the keyed hash of the intake and reference, in base32
     * @param size    the source's size in bytes; -1 if not known
     * @param version the source's version; empty if it has none
     * @throws IllegalArgumentException if the hash is empty, the size is
     *                                  below -1, or the version is null
     */
    public SourceRef
    {
        if (hash == null || hash.isEmpty() || size < -1 || version == null)
        {
            throw new IllegalArgumentException("Incomplete source reference");
        }
    }

    /**
     * Makes the reference of something an intake found.
     *
     * @param ids       makes the keyed hash, with the ID key
     * @param intake    the intake's name
     * @param reference the item's path, object key or message ID; PHI, so
     *                  it is only ever hashed
     * @param size      its size in bytes; -1 if not known
     * @param version   its version, such as a modification time; empty if
     *                  it has none
     * @return the reference
     */
    public static SourceRef of(RecordIds ids, String intake,
            String reference, long size, String version)
    {
        Mac mac = ids.newLabelledMac(LABEL);
        mac.update(intake.getBytes(StandardCharsets.UTF_8));
        mac.update((byte) 0);
        mac.update(reference.getBytes(StandardCharsets.UTF_8));
        return new SourceRef(RecordIds.encode(mac.doFinal()), size, version);
    }

    /**
     * The ledger's key for this sighting: 128 bits of a hash of the
     * reference hash, size and version. The reference hash is already
     * keyed, so a plain hash is enough here.
     *
     * @return the key
     */
    public LedgerIndex.Key key()
    {
        try
        {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            sha256.update(hash.getBytes(StandardCharsets.US_ASCII));
            sha256.update((byte) 0);
            sha256.update(ByteBuffer.allocate(Long.BYTES).putLong(size)
                    .array());
            sha256.update((byte) 0);
            sha256.update(version.getBytes(StandardCharsets.UTF_8));
            ByteBuffer key = ByteBuffer.wrap(sha256.digest(), 0, KEY_BYTES);
            return new LedgerIndex.Key(key.getLong(), key.getLong());
        }
        catch (NoSuchAlgorithmException unavailable)
        {
            throw new IllegalStateException(unavailable);
        }
    }
}
