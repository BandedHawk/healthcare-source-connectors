/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LongSet} and {@link PairSet}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class CompactSetsTest
{

    /**
     * A set is sized to the smallest power of two that keeps it at most
     * two-thirds full, and never fewer than 1,024 slots.
     */
    @Test
    void sizesForTheEntriesExpected()
    {
        assertThat(PairSet.slotsFor(0)).isEqualTo(1_024);
        assertThat(PairSet.slotsFor(682)).isEqualTo(1_024);
        assertThat(PairSet.slotsFor(683)).isEqualTo(2_048);
        assertThat(PairSet.slotsFor(10_000_000)).isEqualTo(1 << 24);
        assertThat(new PairSet(10_000).bytes())
                .isEqualTo(16L * PairSet.slotsFor(10_000));
        assertThat(new LongSet(10_000).bytes())
                .isEqualTo(8L * PairSet.slotsFor(10_000));
    }

    /** A set sized for its entries holds them without growing. */
    @Test
    void holdsWhatItWasSizedForWithoutGrowing()
    {
        LongSet longs = new LongSet(5_000);
        PairSet pairs = new PairSet(5_000);
        long longBytes = longs.bytes();
        long pairBytes = pairs.bytes();
        for (long value = 1; value <= 5_000; value++)
        {
            longs.add(value * 7);
            pairs.add(value, value * 7);
        }
        assertThat(longs.bytes()).isEqualTo(longBytes);
        assertThat(pairs.bytes()).isEqualTo(pairBytes);
    }

    /** The long set grows and keeps every value, zero and repeats once. */
    @Test
    void theLongSetGrowsAndKeepsEveryValue()
    {
        LongSet set = new LongSet(0);
        int count = 100_000;
        for (long value = 0; value < count; value++)
        {
            set.add(value * 0x10001L);
        }
        set.add(5 * 0x10001L);
        assertThat(set.size()).isEqualTo(count);
        for (long value = 0; value < count; value++)
        {
            assertThat(set.contains(value * 0x10001L)).isTrue();
        }
        assertThat(set.contains(0)).isTrue();
        assertThat(set.contains(1)).isFalse();
        assertThat(set.contains(-1)).isFalse();
        assertThat(set.bytes()).isEqualTo(8L * PairSet.slotsFor(count));
    }

    /** Zero is held apart, and counted once. */
    @Test
    void zeroIsKeptApart()
    {
        LongSet longs = new LongSet(0);
        assertThat(longs.contains(0)).isFalse();
        longs.add(0);
        longs.add(0);
        assertThat(longs.contains(0)).isTrue();
        assertThat(longs.size()).isOne();
        PairSet pairs = new PairSet(0);
        assertThat(pairs.contains(0, 0)).isFalse();
        pairs.add(0, 0);
        assertThat(pairs.contains(0, 0)).isTrue();
        assertThat(pairs.contains(0, 1)).isFalse();
        assertThat(pairs.size()).isOne();
    }
}
