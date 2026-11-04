/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

/**
 * A set of pairs of longs, by open addressing in two arrays, so each
 * entry costs 16 bytes and the table is never more than two-thirds
 * full. The pair (0, 0) marks an empty slot, so it is kept apart. Not
 * thread-safe: callers synchronize.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PairSet
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
     * Creates an empty set.
     */
    public PairSet()
    {
    }

    /**
     * Adds a pair.
     *
     * @param high the first half
     * @param low  the second half
     */
    public void add(long high, long low)
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
    public boolean contains(long high, long low)
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
    public long size()
    {
        return count + (hasZero ? 1 : 0);
    }

    /** Doubles the arrays, and puts every pair in its new slot. */
    private void grow()
    {
        if (highs.length >= 1 << 30)
        {
            throw new IllegalStateException("Set is full");
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
