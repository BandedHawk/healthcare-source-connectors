/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

/**
 * A set of pairs of longs, by open addressing in two arrays, so each
 * slot costs 16 bytes and the table is never more than two-thirds full.
 * The pair (0, 0) marks an empty slot, so it is kept apart. Not
 * thread-safe: callers synchronize.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PairSet
{

    /** How many slots a set has at least. */
    private static final int MIN_SLOTS = 1 << 10;

    /** How many slots a set has at most. */
    private static final int MAX_SLOTS = 1 << 30;

    /** The first half of each pair. */
    private long[] highs;

    /** The second half of each pair. */
    private long[] lows;

    /** How many pairs are in the arrays. */
    private int count;

    /** Whether the pair (0, 0) is in the set. */
    private boolean hasZero;

    /**
     * Creates an empty set with room for some pairs before it grows.
     *
     * @param expected how many pairs it should hold without growing
     */
    public PairSet(long expected)
    {
        int slots = slotsFor(expected);
        highs = new long[slots];
        lows = new long[slots];
    }

    /**
     * How many slots hold some entries without growing: the smallest
     * power of two at least half as large again.
     *
     * @param expected how many entries
     * @return the slots
     * @throws IllegalStateException if that is more than a set can have
     */
    static int slotsFor(long expected)
    {
        long slots = MIN_SLOTS;
        while (full(expected, slots))
        {
            if (slots >= MAX_SLOTS)
            {
                throw new IllegalStateException("Set is full");
            }
            slots <<= 1;
        }
        return (int) slots;
    }

    /**
     * Whether some entries are too many for some slots.
     *
     * @param entries how many entries
     * @param slots   how many slots
     * @return true if they would fill more than two-thirds of the slots
     */
    static boolean full(long entries, long slots)
    {
        return entries * 3 > slots * 2;
    }

    /**
     * The next size of a full set's arrays.
     *
     * @param slots its slots now
     * @return twice as many
     * @throws IllegalStateException if the set can't grow
     */
    static int grownLength(int slots)
    {
        if (slots >= MAX_SLOTS)
        {
            throw new IllegalStateException("Set is full");
        }
        return slots * 2;
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
        if (full(count + 1, highs.length))
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

    /**
     * How many bytes the set's arrays take.
     *
     * @return the bytes
     */
    public long bytes()
    {
        return 2L * highs.length * Long.BYTES;
    }

    /** Doubles the arrays, and puts every pair in its new slot. */
    private void grow()
    {
        int slots = grownLength(highs.length);
        long[] newHighs = new long[slots];
        long[] newLows = new long[slots];
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
     * A pair's home slot. The keys are hashes already, but the bits are
     * mixed anyway.
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
