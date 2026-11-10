/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.split;

import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.checkpoint.HoldReleases;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.stage.ReprocessStore;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

/**
 * The split stage's reprocess (STAGES.md §15.8.4): queues the objects of
 * operators' runs, each the next generation of a whole item, and sends
 * each as an {@code item.reprocess} to the stage's own input, sealed to
 * itself and signed by it; notes what came of each once its generation's
 * end is sent; and tells the pipeline when a newer generation is queued
 * for the item it is splitting (§15.8.6).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SplitReprocess
{

    /** The most objects queued in one interval. */
    static final int LIMIT = 100;

    /** Refused: an operator's release of the item held is waiting. */
    public static final String RELEASE_PENDING = "release_pending";

    /** The stage's runs. */
    private final ReprocessStore store;

    /** Seals the messages to the stage itself, signed by it. */
    private final MessageSealer sealer;

    /** Sends them to the stage's own input. */
    private final MessagePublisher publisher;

    /** The ID of the stage's own signing key. */
    private final String selfKid;

    /** The items' checkpoints, for their generations. */
    private final ItemCheckpoints checkpoints;

    /** The {@code landed_items} table. */
    private final LandedItemsTable landedItems;

    /** Operators' requests to release held items; null for none. */
    private final HoldReleases releases;

    /** The landing catalog's name, for the messages. */
    private final String catalogName;

    /** The landing namespace, for the messages. */
    private final String namespace;

    /** How long a send may take. */
    private final Duration sendTimeout;

    /** How long an object queued may go unsent before it is sent again. */
    private final Duration unsentAfter;

    /**
     * The parts of the stage's reprocess.
     *
     * @param store     the stage's runs
     * @param sealer    seals messages to the stage itself, signed by it
     * @param publisher sends them to the stage's own input
     * @param selfKid   the ID of the stage's own signing key
     */
    public record Parts(ReprocessStore store, MessageSealer sealer,
            MessagePublisher publisher, String selfKid)
    {
    }

    /**
     * Where the items are, and the timings.
     *
     * @param checkpoints the items' checkpoints
     * @param landedItems the {@code landed_items} table
     * @param releases    operators' requests to release held items; null
     *                    for none
     * @param catalogName the landing catalog's name
     * @param namespace   the landing namespace
     * @param sendTimeout how long a send may take
     * @param unsentAfter how long an object queued may go unsent before it
     *                    is sent again
     */
    public record Items(ItemCheckpoints checkpoints,
            LandedItemsTable landedItems, HoldReleases releases,
            String catalogName, String namespace, Duration sendTimeout,
            Duration unsentAfter)
    {
    }

    /**
     * Creates the stage's reprocess.
     *
     * @param parts its parts
     * @param items where the items are, and the timings
     */
    public SplitReprocess(Parts parts, Items items)
    {
        this.store = parts.store();
        this.sealer = parts.sealer();
        this.publisher = parts.publisher();
        this.selfKid = parts.selfKid();
        this.checkpoints = items.checkpoints();
        this.landedItems = items.landedItems();
        this.releases = items.releases();
        this.catalogName = items.catalogName();
        this.namespace = items.namespace();
        this.sendTimeout = items.sendTimeout();
        this.unsentAfter = items.unsentAfter();
    }

    /**
     * The ID of the stage's own signing key: the only signer of an
     * {@code item.reprocess} taken, and one no {@code item.landed} is
     * taken from.
     *
     * @return the key's ID
     */
    public String selfKid()
    {
        return selfKid;
    }

    /**
     * Queues and sends a batch of objects: first those queued but never
     * sent (a pod stopped between the two), under the same generations and
     * message IDs, then a batch not yet queued, each checked against the
     * item now; then ends the runs with nothing left. An object is refused
     * {@code superseded} if the item is past the generation chosen,
     * {@code past_retention} if it is no longer in landing, and
     * {@code release_pending} if a release of it held is waiting.
     *
     * @return how many were sent
     * @throws Exception if the database, landing or the broker fails; it
     *                   is tried again at the next interval
     */
    public int queueAndSend() throws Exception
    {
        Table landed = landedItems.get();
        landed.refresh();
        Map<UUID, Optional<Record>> summaries = new HashMap<>();
        List<ReprocessStore.Queued> queued = new ArrayList<>(
                store.unsent(unsentAfter, LIMIT));
        queued.addAll(store.queue(LIMIT,
                chosen -> refusal(chosen, summary(landed, summaries,
                        chosen.itemId()))));
        Snapshot snapshot = landed.currentSnapshot();
        List<CompletableFuture<Void>> sends = new ArrayList<>();
        List<ReprocessStore.Queued> sent = new ArrayList<>();
        for (ReprocessStore.Queued each : queued)
        {
            Optional<Record> summary = summary(landed, summaries,
                    each.chosen().itemId());
            if (summary.isEmpty() || snapshot == null)
            {
                store.refuse(each, ReprocessStore.PAST_RETENTION);
                continue;
            }
            ConnectorMessage message = new ConnectorMessage.ItemReprocess(
                    LandedItems.landedItem(summary.get(),
                            snapshot.snapshotId()).message(catalogName,
                                    namespace),
                    each.line().generation(), each.runId());
            sends.add(publisher.publish(OutboundMessage.of(message,
                    sealer.seal(message))));
            sent.add(each);
        }
        if (!sends.isEmpty())
        {
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                    .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        store.markSent(sent);
        store.finishRuns();
        return sent.size();
    }

    /**
     * Why an object can't be queued now.
     *
     * @param chosen  the object
     * @param summary the item's summary row in landing; empty if gone
     * @return the reason; null if it may be queued
     */
    private String refusal(ReprocessStore.Chosen chosen,
            Optional<Record> summary)
    {
        // The generation the run chose is the item's then; one past it
        // means another run, or a correction, came first.
        Integer now = checkpoints.generations(List.of(chosen.itemId()))
                .get(chosen.itemId());
        if (now != null && now > chosen.input().generation())
        {
            return ReprocessStore.SUPERSEDED;
        }
        if (summary.isEmpty())
        {
            return ReprocessStore.PAST_RETENTION;
        }
        if (releases != null && releases.list(chosen.itemId()).stream()
                .anyMatch(listed -> listed.state()
                        == HoldReleases.State.PENDING))
        {
            return RELEASE_PENDING;
        }
        return null;
    }

    /**
     * An item's summary row in landing, read once an interval.
     *
     * @param landed    the {@code landed_items} table
     * @param summaries what is read so far, by item
     * @param itemId    the item
     * @return its row; empty if it is gone
     * @throws IOException if landing can't be read
     */
    private static Optional<Record> summary(Table landed,
            Map<UUID, Optional<Record>> summaries, UUID itemId)
            throws IOException
    {
        Optional<Record> found = summaries.get(itemId);
        if (found == null)
        {
            found = LandedItems.summary(landed, itemId);
            summaries.put(itemId, found);
        }
        return found;
    }

    /**
     * The newest generation queued for an item, past the one being split.
     *
     * @param itemId     the item
     * @param generation the generation being split
     * @return the newer generation; 0 if none
     */
    public int newer(UUID itemId, int generation)
    {
        return store.newerQueued(itemId, generation);
    }

    /**
     * Notes what came of a generation once its end is sent: {@code stored}
     * if it was split to its end, {@code failed} if it failed as a whole,
     * {@code interrupted} if a newer one stopped it. Any other outcome
     * leaves the object as it is, to be noted when its end is sent.
     *
     * @param reprocess the message taken
     * @param outcome   how the generation ended
     * @return whether it was noted
     */
    public boolean note(ConnectorMessage.ItemReprocess reprocess,
            ItemProcessor.Outcome outcome)
    {
        String noted = switch (outcome.result())
        {
            case COMPLETED -> "stored";
            case FAILED -> "failed";
            case INTERRUPTED -> "interrupted";
            case SUPERSEDED -> ReprocessStore.SUPERSEDED;
            default -> null;
        };
        if (noted == null)
        {
            return false;
        }
        return store.noteGeneration(reprocess.runId(), reprocess.itemId(),
                reprocess.generation(), noted);
    }
}
