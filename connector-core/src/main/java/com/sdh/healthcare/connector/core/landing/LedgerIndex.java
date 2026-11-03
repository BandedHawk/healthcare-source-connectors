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

    /**
     * A set of pairs of longs, by open addressing in two arrays, so each
     * entry costs 16 bytes and the table is never more than two-thirds
     * full. The pair (0, 0) marks an empty slot, so it is kept apart.
     */
    private static final class PairSet
    {

        /** How many slots a new set starts with. */
        private static final int INITIAL_CAPACITY = 1 << 10;

        /** The first half of each pair. */
        private long[] highs = new long[INITIAL_CAPACITY];

        /** The second half of each pair. */
        private long[] lows = new long[INITIAL_CAPACITY];

        /** How many pairs are in the arrays. */
        private int count;

        /** Whether the pair (0, 0) is in the set. */
        private boolean hasZero;

        /**
         * Adds a pair.
         *
         * @param high the first half
         * @param low  the second half
         */
        void add(long high, long low)
        {
            if (high == 0 && low == 0)
            {
                hasZero = true;
                return;
            }
            if ((count + 1) * 3L > highs.length * 2L)
            {
                grow();
            }
            if (insert(highs, lows, high, low))
            {
                count++;
            }
        }

        /**
         * Tells whether a pair is in the set.
         *
         * @param high the first half
         * @param low  the second half
         * @return true if it is
         */
        boolean contains(long high, long low)
        {
            if (high == 0 && low == 0)
            {
                return hasZero;
            }
            int mask = highs.length - 1;
            for (int slot = slot(high, low, mask); ;
                    slot = (slot + 1) & mask)
            {
                if (highs[slot] == 0 && lows[slot] == 0)
                {
                    return false;
                }
                if (highs[slot] == high && lows[slot] == low)
                {
                    return true;
                }
            }
        }

        /**
         * How many pairs are in the set.
         *
         * @return the count
         */
        long size()
        {
            return count + (hasZero ? 1 : 0);
        }

        /** Doubles the arrays, and puts every pair in its new slot. */
        private void grow()
        {
            if (highs.length >= 1 << 30)
            {
                throw new IllegalStateException("Ledger index is full");
            }
            long[] newHighs = new long[highs.length * 2];
            long[] newLows = new long[lows.length * 2];
            for (int slot = 0; slot < highs.length; slot++)
            {
                if (highs[slot] != 0 || lows[slot] != 0)
                {
                    insert(newHighs, newLows, highs[slot], lows[slot]);
                }
            }
            highs = newHighs;
            lows = newLows;
        }

        /**
         * Puts a pair in the first free slot from its home slot on.
         *
         * @param targetHighs the first halves
         * @param targetLows  the second halves
         * @param high        the pair's first half
         * @param low         the pair's second half
         * @return false if the pair was already there
         */
        private static boolean insert(long[] targetHighs, long[] targetLows,
                long high, long low)
        {
            int mask = targetHighs.length - 1;
            for (int slot = slot(high, low, mask); ;
                    slot = (slot + 1) & mask)
            {
                if (targetHighs[slot] == 0 && targetLows[slot] == 0)
                {
                    targetHighs[slot] = high;
                    targetLows[slot] = low;
                    return true;
                }
                if (targetHighs[slot] == high && targetLows[slot] == low)
                {
                    return false;
                }
            }
        }

        /**
         * A pair's home slot. The keys are hashes already, but a digest
         * prefix has only 60 bits, so the bits are mixed anyway.
         *
         * @param high the first half
         * @param low  the second half
         * @param mask the number of slots, less one
         * @return the slot
         */
        private static int slot(long high, long low, int mask)
        {
            long mixed = (high ^ Long.rotateLeft(low, 32))
                    * 0x9E3779B97F4A7C15L;
            return (int) (mixed >>> 32) & mask;
        }
    }
}
