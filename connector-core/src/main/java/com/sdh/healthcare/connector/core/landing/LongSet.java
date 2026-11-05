/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

/**
 * A set of longs, by open addressing in one array, so each slot costs 8
 * bytes and the table is never more than two-thirds full. Zero marks an
 * empty slot, so it is kept apart. Not thread-safe: callers synchronize.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LongSet
{

    /** The values. */
    private long[] values;

    /** How many values are in the array. */
    private int count;

    /** Whether zero is in the set. */
    private boolean hasZero;

    /**
     * Creates an empty set with room for some values before it grows.
     *
     * @param expected how many values it should hold without growing
     */
    public LongSet(long expected)
    {
        values = new long[PairSet.slotsFor(expected)];
    }

    /**
     * Adds a value.
     *
     * @param value the value
     */
    public void add(long value)
    {
        if (value == 0)
        {
            hasZero = true;
            return;
        }
        if (PairSet.full(count + 1, values.length))
        {
            grow();
        }
        if (insert(values, value))
        {
            count++;
        }
    }

    /**
     * Tells whether a value is in the set.
     *
     * @param value the value
     * @return true if it is
     */
    public boolean contains(long value)
    {
        if (value == 0)
        {
            return hasZero;
        }
        int mask = values.length - 1;
        for (int slot = slot(value, mask); ; slot = (slot + 1) & mask)
        {
            if (values[slot] == 0)
            {
                return false;
            }
            if (values[slot] == value)
            {
                return true;
            }
        }
    }

    /**
     * How many values are in the set.
     *
     * @return the count
     */
    public long size()
    {
        return count + (hasZero ? 1 : 0);
    }

    /**
     * How many bytes the set's array takes.
     *
     * @return the bytes
     */
    public long bytes()
    {
        return (long) values.length * Long.BYTES;
    }

    /** Doubles the array, and puts every value in its new slot. */
    private void grow()
    {
        long[] grown = new long[PairSet.grownLength(values.length)];
        for (long value : values)
        {
            if (value != 0)
            {
                insert(grown, value);
            }
        }
        values = grown;
    }

    /**
     * Puts a value in the first free slot from its home slot on.
     *
     * @param target the slots
     * @param value  the value
     * @return false if the value was already there
     */
    private static boolean insert(long[] target, long value)
    {
        int mask = target.length - 1;
        for (int slot = slot(value, mask); ; slot = (slot + 1) & mask)
        {
            if (target[slot] == 0)
            {
                target[slot] = value;
                return true;
            }
            if (target[slot] == value)
            {
                return false;
            }
        }
    }

    /**
     * A value's home slot, with its bits mixed.
     *
     * @param value the value
     * @param mask  the number of slots, less one
     * @return the slot
     */
    private static int slot(long value, int mask)
    {
        return (int) ((value * 0x9E3779B97F4A7C15L) >>> 32) & mask;
    }
}
