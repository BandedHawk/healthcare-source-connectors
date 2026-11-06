/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.split;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.checkpoint.LeaseHeldException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseKeeper;
import com.sdh.healthcare.connector.core.checkpoint.LeaseStore;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandedRefs;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits one landed item at a time on this pod, under the item's lease
 * (STAGES.md §8). Items run in parallel across pods; the lease, named
 * {@code split/<connector>/<item>}, keeps two pods off the same item, such
 * as when a message is delivered again while its first delivery is still
 * being split. Its epoch fences the item's checkpoint, so a pod that lost
 * the lease can't move the checkpoint back.
 * <p>
 * Losing an item's lease stops that item, not the pod: the pipeline finds
 * the lease gone at its next commit and throws, and the caller tries the
 * item again later, from its checkpoint.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ItemSplitter
{

    /** Logs each item's end, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ItemSplitter.class);

    /** Splits, stores and announces items. */
    private final ItemProcessor processor;

    /** The {@code landed_items} table. */
    private final LandedItemsTable landedItems;

    /** Unwraps the landed items' data keys. */
    private final KeyProvider keys;

    /** Where the item leases are kept. */
    private final LeaseStore leases;

    /** The start of each item lease's name. */
    private final String leasePrefix;

    /** Who takes the leases. */
    private final String owner;

    /** The lease timings. */
    private final LeaseKeeper.Settings leaseSettings;

    /** Held while an item is split: one at a time per pod. */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Creates a splitter.
     *
     * @param processor     splits, stores and announces items
     * @param landedItems   the {@code landed_items} table
     * @param keys          unwraps the landed items' data keys
     * @param leases        where the item leases are kept
     * @param connectorId   the split stage's ID, naming its item leases
     * @param leaseSettings the lease timings
     */
    public ItemSplitter(ItemProcessor processor,
            LandedItemsTable landedItems, KeyProvider keys, LeaseStore leases,
            String connectorId, LeaseKeeper.Settings leaseSettings)
    {
        this.processor = processor;
        this.landedItems = landedItems;
        this.keys = keys;
        this.leases = leases;
        this.leasePrefix = "split/" + connectorId + "/";
        this.owner = LeaseKeeper.defaultOwner();
        this.leaseSettings = leaseSettings;
    }

    /**
     * Splits an item, or finishes or announces again one already started.
     * Waits while this pod splits another.
     *
     * @param itemId the item's ID
     * @param digest the item's digest, from its {@code item.landed}
     * @return how it ended
     * @throws LeaseHeldException   if another pod holds the item's lease;
     *                              try again later
     * @throws InterruptedException if interrupted while waiting
     * @throws Exception            if storage or the broker fails, or the
     *                              item's lease was lost; try again later
     */
    public ItemProcessor.Outcome split(UUID itemId, String digest)
            throws Exception
    {
        lock.lockInterruptibly();
        try (LeaseKeeper lease = LeaseKeeper.tryAcquire(leases,
                leasePrefix + itemId, owner, leaseSettings,
                lost -> LOG.warn("split.item_lease_lost reason={}",
                        lost.reason())))
        {
            // What the producer's result names, passed on so later stages
            // need no landing key (STAGES.md §9). Refreshed first: the item
            // landed after the table was last read.
            Table landed = landedItems.get();
            landed.refresh();
            LandedRefs refs = LandedItems.summary(landed, itemId)
                    .map(summary -> LandedRefs.of(summary, keys))
                    .orElse(LandedRefs.NONE);
            ItemProcessor.Outcome outcome = processor.process(itemId, digest,
                    () -> LandedItemSource.open(landedItems.get(), itemId,
                            keys), lease, refs);
            LOG.info("split.item_done result={} records={}",
                    outcome.result(), outcome.recordCount());
            return outcome;
        }
        finally
        {
            lock.unlock();
        }
    }
}
