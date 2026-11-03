/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

/**
 * The ledger in memory (design §5.3.2): what has been landed, so an intake
 * can tell at once whether something it has found is new. It is loaded from
 * the ledger table at startup and added to as items land.
 * <p>
 * It holds two compact sets, to stay small with tens of millions of items:
 * the 128-bit key of each sighting ({@link SourceRef#key()}), about 24 bytes
 * an item, and 60 bits of each item's digest, about 12 bytes an item. The
 * digest set only says that some earlier item may have the same content; the
 * ledger table says which. Thread-safe.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LedgerIndex
{

    /** How many characters of a base32 digest make the digest prefix. */
    private static final int PREFIX_CHARACTERS = 12;

    /** How many bits a base32 character encodes. */
    private static final int BITS_PER_CHARACTER = 5;

    /** The keys of the sightings landed. */
    private final PairSet keys = new PairSet();

    /** The digest prefixes of the items landed. */
    private final PairSet digests = new PairSet();

    /**
     * A sighting's 128-bit key.
     *
     * @param high the first 64 bits
     * @param low  the last 64 bits
     */
    public record Key(long high, long low)
    {
    }

    /**
     * Records a landed item.
     *
     * @param key    its sighting's key; null if it had no source reference
     * @param digest its digest, in base32
     */
    public synchronized void add(Key key, String digest)
    {
        if (key != null)
        {
            keys.add(key.high(), key.low());
        }
        digests.add(digestPrefix(digest), 0);
    }

    /**
     * Tells whether a sighting has already been landed.
     *
     * @param key the sighting's key
     * @return true if it has
     */
    public synchronized boolean contains(Key key)
    {
        return keys.contains(key.high(), key.low());
    }

    /**
     * Tells whether an earlier item may have this content. A true answer
     * can be wrong, about once in 2<sup>60</sup> / (items landed); the
     * ledger table gives the exact answer.
     *
     * @param digest the item's digest, in base32
     * @return true if an item with the same digest prefix has landed
     */
    public synchronized boolean mayHaveContent(String digest)
    {
        return digests.contains(digestPrefix(digest), 0);
    }

    /**
     * How many sightings the index holds.
     *
     * @return the number of keys
     */
    public synchronized long size()
    {
        return keys.size();
    }

    /**
     * The first 60 bits of a base32 digest, as a number.
     *
     * @param digest the digest, in lower-case base32 (RFC 4648)
     * @return its first 12 characters' bits
     * @throws IllegalArgumentException if it is too short or isn't base32
     */
    static long digestPrefix(String digest)
    {
        if (digest.length() < PREFIX_CHARACTERS)
        {
            throw new IllegalArgumentException("Digest too short");
        }
        long prefix = 0;
        for (int index = 0; index < PREFIX_CHARACTERS; index++)
        {
            char character = digest.charAt(index);
            int value;
            if (character >= 'a' && character <= 'z')
            {
                value = character - 'a';
            }
            else if (character >= '2' && character <= '7')
            {
                value = 26 + character - '2';
            }
            else
            {
                throw new IllegalArgumentException("Digest isn't base32");
            }
            prefix = (prefix << BITS_PER_CHARACTER) | value;
        }
        return prefix;
    }
}
