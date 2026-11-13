/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.split;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.HoldReleases;
import com.sdh.healthcare.connector.core.checkpoint.LeaseHeldException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseKeeper;
import com.sdh.healthcare.connector.core.checkpoint.LeaseStore;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandedRefs;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
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

    /**
     * A permit is held while an item is split, so a pod splits at most so
     * many at once. Each item's own lease keeps it to one split at a time,
     * in this pod or another: a live lease is refused to its own owner too.
     */
    private final Semaphore permits;

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
        this(processor, landedItems, keys, leases, connectorId,
                leaseSettings, 1);
    }

    /**
     * Creates a splitter that splits up to so many items at once.
     *
     * @param processor     splits, stores and announces items
     * @param landedItems   the {@code landed_items} table
     * @param keys          unwraps the landed items' data keys
     * @param leases        where the item leases are kept
     * @param connectorId   the split stage's ID, naming its item leases
     * @param leaseSettings the lease timings
     * @param concurrency   the most items split at once; at least 1
     */
    public ItemSplitter(ItemProcessor processor,
            LandedItemsTable landedItems, KeyProvider keys, LeaseStore leases,
            String connectorId, LeaseKeeper.Settings leaseSettings,
            int concurrency)
    {
        if (concurrency < 1)
        {
            throw new IllegalArgumentException(
                    "concurrency must be at least 1");
        }
        this.permits = new Semaphore(concurrency);
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
     * Waits while this pod splits as many as it may.
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
        return split(itemId, digest, null);
    }

    /**
     * Splits an item as {@link #split(UUID, String)} does; for an exact
     * copy of an earlier item (STAGES.md §15.5a), first takes note of it,
     * which flags the earlier item's open failures resent.
     *
     * @param itemId the item's ID
     * @param digest the item's digest, from its {@code item.landed}
     * @param sameAs the earlier item it is an exact copy of, from its
     *               {@code item.landed}; null if none
     * @return how it ended
     * @throws LeaseHeldException   if another pod holds the item's lease;
     *                              try again later
     * @throws InterruptedException if interrupted while waiting
     * @throws Exception            if storage or the broker fails, or the
     *                              item's lease was lost; try again later
     */
    public ItemProcessor.Outcome split(UUID itemId, String digest,
            UUID sameAs) throws Exception
    {
        return split(itemId, digest, sameAs, null);
    }

    /**
     * Splits an item as {@link #split(UUID, String, UUID)} does, reading
     * its summary row and chunks where its {@code item.landed} says they
     * are, not by scan (PERFORMANCE.md §3.2).
     *
     * @param itemId  the item's ID
     * @param digest  the item's digest, from its {@code item.landed}
     * @param sameAs  the earlier item it is an exact copy of; null if none
     * @param storage where its {@code item.landed} says it is; null to find
     *                it by scan
     * @return how it ended
     * @throws LeaseHeldException   if another pod holds the item's lease;
     *                              try again later
     * @throws InterruptedException if interrupted while waiting
     * @throws Exception            if storage or the broker fails, or the
     *                              item's lease was lost; try again later
     */
    public ItemProcessor.Outcome split(UUID itemId, String digest,
            UUID sameAs, ConnectorMessage.StorageRef storage)
            throws Exception
    {
        permits.acquire();
        try (LeaseKeeper lease = LeaseKeeper.tryAcquire(leases,
                leasePrefix + itemId, owner, leaseSettings,
                lost -> LOG.warn("split.item_lease_lost reason={}",
                        lost.reason())))
        {
            // What the producer's result names, passed on so later stages
            // need no landing key (STAGES.md §9). Refreshed first: the item
            // landed after the table was last read.
            long started = System.nanoTime();
            if (sameAs != null)
            {
                processor.noteCopy(itemId, sameAs);
            }
            Table landed = landedItems.get();
            Optional<LandedItemSource.Summary> located =
                    LandedItemSource.summary(landed, itemId, storage);
            Optional<Record> summary =
                    located.map(LandedItemSource.Summary::row);
            Telemetry telemetry = Telemetry.get();
            telemetry.phaseDuration(ItemProcessor.STAGE, "lookup",
                    System.nanoTime() - started);
            // When it landed, so its time to gold can be measured from the
            // registries (docs/PERFORMANCE.md §10).
            summary.ifPresent(row -> processor.noteLanded(itemId,
                    ((OffsetDateTime) row.getField(LandingTables.LANDED_AT))
                            .toInstant()));
            LandedRefs refs = summary
                    .map(row -> LandedRefs.of(row, keys))
                    .orElse(LandedRefs.NONE)
                    .withSameAs(processor.sameAs(itemId).orElse(null));
            // Read once: the source reads the item from where it says.
            ItemProcessor.Outcome outcome = processor.process(itemId, digest,
                    () -> located.isPresent()
                            ? LandedItemSource.open(landed, itemId, keys,
                                    located.get())
                            : LandedItemSource.open(landedItems.get(),
                                    itemId, keys), lease, refs);
            telemetry.phaseDuration(ItemProcessor.STAGE, "item",
                    System.nanoTime() - started);
            LOG.info("split.item_done result={} records={}",
                    outcome.result(), outcome.recordCount());
            return outcome;
        }
        finally
        {
            permits.release();
        }
    }

    /**
     * Splits an item again as its next generation, for a reprocess run
     * (STAGES.md §15.8.4), under the item's lease: interrupting the
     * generation before if it is part way (§15.8.6), or finishing it if its
     * cut is over; then notes what came of the run's object. Waits while
     * this pod splits as many as it may.
     *
     * @param reprocess the message taken
     * @param runs      notes what came of it; null to note nothing
     * @return how the generation ended
     * @throws LeaseHeldException   if another pod holds the item's lease;
     *                              try again later, once that pod has seen
     *                              the newer generation and stopped
     * @throws InterruptedException if interrupted while waiting
     * @throws Exception            if storage or the broker fails, or the
     *                              item's lease was lost; try again later
     */
    public ItemProcessor.Outcome reprocess(
            ConnectorMessage.ItemReprocess reprocess, SplitReprocess runs)
            throws Exception
    {
        UUID itemId = reprocess.itemId();
        permits.acquire();
        try (LeaseKeeper lease = lease(itemId))
        {
            Landed landed = landed(itemId);
            ItemProcessor.Outcome outcome = processor.reprocess(itemId,
                    landed.digest(), () -> LandedItemSource.open(
                            landedItems.get(), itemId, keys), lease,
                    landed.refs(), reprocess.generation(),
                    reprocess.runId());
            if (runs != null)
            {
                runs.note(reprocess, outcome);
            }
            LOG.info("split.reprocess_done result={} records={}",
                    outcome.result(), outcome.recordCount());
            return outcome;
        }
        finally
        {
            permits.release();
        }
    }

    /**
     * Applies an operator's correction of one of an item's failures
     * (STAGES.md §15.6d), under the item's lease, unless another pod
     * applied it first.
     *
     * @param pending  the correction, as listed
     * @param store    the correction store, read again under the lease
     * @return what it came to; empty if it was no longer pending
     * @throws LeaseHeldException   if another pod holds the item's lease
     * @throws InterruptedException if interrupted while waiting
     * @throws Exception            if storage or the broker fails; it is
     *                              tried again later
     */
    public Optional<CorrectionStore.Outcome> correct(
            CorrectionStore.Pending pending, CorrectionStore store)
            throws Exception
    {
        UUID itemId = pending.itemId();
        permits.acquire();
        try (LeaseKeeper lease = lease(itemId))
        {
            if (!store.stillPending(pending))
            {
                return Optional.empty();
            }
            Landed landed = landed(itemId);
            CorrectionStore.Outcome outcome = processor.correct(pending,
                    landed.digest(), landed.refs(), lease);
            LOG.info("split.correction_done passed={}", outcome.passed());
            return Optional.of(outcome);
        }
        finally
        {
            permits.release();
        }
    }

    /**
     * Applies an operator's request to end the hold on an item held across
     * versions (STAGES.md §15.6e), under the item's lease, unless another
     * pod applied it first; then, for {@code RESUME}, splits the item from
     * its checkpoint under the version running here. A request for an item
     * that isn't held is refused, and noted so.
     *
     * @param request  the request, as listed
     * @param releases the release store, read again under the lease
     * @return what it came to; empty if it was no longer pending
     * @throws LeaseHeldException   if another pod holds the item's lease
     * @throws InterruptedException if interrupted while waiting
     * @throws Exception            if storage or the broker fails; a
     *                              request applied is resumed by the
     *                              reconciler, one not applied is tried
     *                              again later
     */
    public Optional<HoldReleases.State> release(HoldReleases.Request request,
            HoldReleases releases) throws Exception
    {
        UUID itemId = request.itemId();
        Optional<Checkpoint> held;
        permits.acquire();
        try (LeaseKeeper lease = lease(itemId))
        {
            if (!releases.stillPending(request))
            {
                return Optional.empty();
            }
            held = processor.release(itemId, request.action(),
                    request.requestedBy(), lease);
            if (held.isEmpty())
            {
                String state = processor.checkpoint(itemId)
                        .map(checkpoint -> checkpoint.state().name())
                        .orElse("never split");
                releases.finish(request, HoldReleases.State.REFUSED, null,
                        null, "The item is " + state + ", not HELD");
                Telemetry.get().auditWarn("split.release_refused",
                        Fields.of(Attr.ITEM_ID, itemId)
                                .with(Attr.ACTION, request.action().name())
                                .with(Attr.STATE, state));
                return Optional.of(HoldReleases.State.REFUSED);
            }
            releases.finish(request, HoldReleases.State.DONE,
                    held.get().version(),
                    request.action() == HoldReleases.Action.RESUME
                            ? processor.version() : null, null);
        }
        finally
        {
            permits.release();
        }
        if (request.action() == HoldReleases.Action.RESUME)
        {
            // Split now, under a lease of its own; if this pod stops first,
            // the reconciler resumes it as it does any stalled item.
            split(itemId, landed(itemId).digest());
        }
        return Optional.of(HoldReleases.State.DONE);
    }

    /**
     * Sends an item's end again after its failures changed, under its
     * lease.
     *
     * @param itemId the item
     * @throws LeaseHeldException   if another pod holds its lease
     * @throws InterruptedException if interrupted while waiting
     * @throws Exception            if the store or the broker fails
     */
    public void reannounce(UUID itemId) throws Exception
    {
        permits.acquire();
        try (LeaseKeeper lease = lease(itemId))
        {
            Landed landed = landed(itemId);
            processor.reannounce(itemId, landed.digest(), landed.refs());
        }
        finally
        {
            permits.release();
        }
    }

    /**
     * The items whose ends are to be sent again, their failures changed
     * since: see {@link ItemProcessor#changedEnds}.
     *
     * @param limit the most returned
     * @return their ids
     */
    public List<UUID> changedEnds(int limit)
    {
        return processor.changedEnds(limit);
    }

    /**
     * Takes an item's lease.
     *
     * @param itemId the item
     * @return the lease, kept until closed
     * @throws LeaseHeldException if another pod holds it
     */
    private LeaseKeeper lease(UUID itemId) throws LeaseHeldException
    {
        return LeaseKeeper.tryAcquire(leases, leasePrefix + itemId, owner,
                leaseSettings, lost -> LOG.warn(
                        "split.item_lease_lost reason={}", lost.reason()));
    }

    /**
     * What landing kept about an item.
     *
     * @param digest its digest
     * @param refs   what its result names
     */
    private record Landed(String digest, LandedRefs refs)
    {
    }

    /**
     * Reads what landing kept about an item, and the earlier item it is an
     * exact copy of, if noted (STAGES.md §15.5a).
     *
     * @param itemId the item
     * @return its digest and references
     * @throws IllegalStateException if it is no longer in landing
     * @throws java.io.IOException   if its data keys can't be unwrapped
     */
    private Landed landed(UUID itemId) throws java.io.IOException
    {
        Table landed = landedItems.get();
        landed.refresh();
        var summary = LandedItems.summary(landed, itemId)
                .orElseThrow(() -> new IllegalStateException(
                        "The item is no longer in landing"));
        return new Landed((String) summary.getField(
                LandingTables.ITEM_DIGEST), LandedRefs.of(summary, keys)
                        .withSameAs(processor.sameAs(itemId).orElse(null)));
    }
}
