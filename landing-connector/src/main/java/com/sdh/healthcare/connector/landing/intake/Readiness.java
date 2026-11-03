/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Decides when an item is ready to land (design §5.3.4): once the node has
 * seen the same size and version for the settle time. It goes by its own
 * observations and clock, not the source's modification times, so a
 * network share whose clock is wrong can't make an item look settled while
 * it is still being written.
 * <p>
 * Items that drop out of a listing are forgotten when the listing ends, so
 * the map holds only what is in the source now. It lives only in memory:
 * after a restart, every item settles again.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Readiness
{

    /**
     * What was seen of an item.
     *
     * @param size       its size
     * @param version    its version
     * @param sinceNanos when this size and version were first seen
     * @param listing    the listing it was last seen in
     */
    private record Sighting(long size, long version, long sinceNanos,
            long listing)
    {
    }

    /** How long an item must stay unchanged. */
    private final long settleNanos;

    /** Gives the time, in nanoseconds. */
    private final LongSupplier nanoClock;

    /** What was seen of each item, by reference. */
    private final Map<String, Sighting> seen = new HashMap<>();

    /** The number of the listing in progress. */
    private long listing;

    /**
     * Creates a tracker.
     *
     * @param settle    how long an item must stay unchanged; zero makes
     *                  every item ready when first seen
     * @param nanoClock gives the time, in nanoseconds
     */
    public Readiness(Duration settle, LongSupplier nanoClock)
    {
        this.settleNanos = settle.toNanos();
        this.nanoClock = nanoClock;
    }

    /**
     * Records a sighting of an item and tells whether it is ready.
     *
     * @param item the item as listed
     * @return true if it has had this size and version for the settle time
     */
    public synchronized boolean ready(ListedItem item)
    {
        long now = nanoClock.getAsLong();
        Sighting last = seen.get(item.ref());
        long since = last != null && last.size() == item.size()
                && last.version() == item.version() ? last.sinceNanos() : now;
        seen.put(item.ref(),
                new Sighting(item.size(), item.version(), since, listing));
        return now - since >= settleNanos;
    }

    /**
     * Forgets an item, once it is landed or has changed.
     *
     * @param ref the item's reference
     */
    public synchronized void forget(String ref)
    {
        seen.remove(ref);
    }

    /**
     * Ends a listing: forgets items it didn't include.
     */
    public synchronized void listingDone()
    {
        long finished = listing;
        seen.values().removeIf(sighting -> sighting.listing() != finished);
        listing++;
    }

    /**
     * How many items are waiting to settle, for the
     * {@code sdh.landing.items_waiting} gauge.
     *
     * @return the number of items seen and not yet landed
     */
    public synchronized int waiting()
    {
        return seen.size();
    }
}
