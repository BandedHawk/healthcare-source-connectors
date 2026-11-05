/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.perf;

import java.util.Arrays;

/**
 * Per-item latencies of a run, and their percentiles (design §13.1). Every
 * item is kept, so the percentiles are exact. Thread-safe.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Latencies
{

    /**
     * The percentiles of a run's latencies, in milliseconds.
     *
     * @param count how many items
     * @param p50   the median
     * @param p95   the 95th percentile
     * @param p99   the 99th percentile
     * @param max   the longest
     */
    public record Summary(int count, double p50, double p95, double p99,
            double max)
    {
    }

    /** The latencies so far, in nanoseconds. */
    private long[] nanos = new long[64];

    /** How many there are. */
    private int count;

    /** Creates an empty set of latencies. */
    public Latencies()
    {
    }

    /**
     * Adds one item's latency.
     *
     * @param latencyNanos its latency
     */
    public synchronized void add(long latencyNanos)
    {
        if (count == nanos.length)
        {
            nanos = Arrays.copyOf(nanos, count * 2);
        }
        nanos[count++] = latencyNanos;
    }

    /**
     * How many latencies there are.
     *
     * @return the count
     */
    public synchronized int count()
    {
        return count;
    }

    /**
     * The percentiles, by nearest rank.
     *
     * @return the summary; null if there are no latencies
     */
    public synchronized Summary summary()
    {
        if (count == 0)
        {
            return null;
        }
        long[] sorted = Arrays.copyOf(nanos, count);
        Arrays.sort(sorted);
        return new Summary(count, percentile(sorted, 50),
                percentile(sorted, 95), percentile(sorted, 99),
                sorted[count - 1] / 1e6);
    }

    /**
     * A percentile of sorted latencies, by nearest rank.
     *
     * @param sorted  the latencies, in order
     * @param percent the percentile
     * @return it, in milliseconds
     */
    private static double percentile(long[] sorted, int percent)
    {
        int rank = (int) Math.ceil(percent / 100.0 * sorted.length);
        return sorted[Math.max(0, rank - 1)] / 1e6;
    }
}
