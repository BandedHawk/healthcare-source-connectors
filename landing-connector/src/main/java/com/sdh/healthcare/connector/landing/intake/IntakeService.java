/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandingCheckpoint;
import com.sdh.healthcare.connector.core.landing.LandingLedger;
import com.sdh.healthcare.connector.core.landing.LedgerIndex;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.landing.SourceRef;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.InboundDirectoryClaim;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lands an intake's items, one at a time, as its route lists them (design
 * §5.3). For each item listed:
 * <ol>
 *   <li>its source reference is hashed with the ID key, and looked up in
 *       the ledger: an item already landed is only removed from a
 *       read-write source, in case that step was cut short;</li>
 *   <li>otherwise it waits until it is ready, then is landed under a new
 *       item ID;</li>
 *   <li>its ledger entry is written, and an {@code item.duplicate_content}
 *       warning sent if another item had the same content;</li>
 *   <li>its {@code item.landed} message is sent and acknowledged;</li>
 *   <li>a read-write source deletes, moves or renames it.</li>
 * </ol>
 * A crash between any two steps is recovered at startup: the ledger is
 * repaired, the last item's message is sent again, and an unfinished item
 * is resumed if the first full listing finds it unchanged, or given up
 * otherwise. Step 1 then finishes the source side.
 * <p>
 * Errors that mean the node can't safely go on (lease loss, an unknown
 * commit outcome, a tampered checkpoint, a broken invariant, a lost
 * directory claim) stop the process. Anything else (storage or broker
 * down, an unreadable item) is retried on later listings, after a backoff
 * that pauses the route. Item references are never logged.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class IntakeService
{

    /** The longest wait after repeated errors. */
    private static final Duration MAX_BACKOFF = Duration.ofMinutes(5);

    /** Logs progress and errors, never item references. */
    private static final Logger LOG =
            LoggerFactory.getLogger(IntakeService.class);

    /**
     * The landing storage an intake uses.
     *
     * @param lander lands items in {@code landed_items}
     * @param ledger records landed items
     * @param index  the ledger's entries for this intake, in memory
     * @param ids    hashes source references with the ID key
     */
    public record Storage(ItemLander lander, LandingLedger ledger,
            LedgerIndex index, SectionIds ids)
    {
    }

    /** The intake's name. */
    private final String intake;

    /** Where items come from. */
    private final IntakeSource source;

    /** Decides when an item is ready. */
    private final Readiness readiness;

    /** The landing storage. */
    private final Storage storage;

    /** Sends {@code item.landed} messages. */
    private final Announcer announcer;

    /** The read-write source's directory claim; null when read-only. */
    private final InboundDirectoryClaim claim;

    /** Stops the process. */
    private final FatalExit fatal;

    /** Gives the times new item IDs are made from. */
    private final Clock clock;

    /** The wait after the first error in a row. */
    private final Duration initialBackoff;

    /**
     * Held while the intake writes to landing storage, so table maintenance
     * runs only between items.
     */
    private final ReentrantLock writer = new ReentrantLock();

    /** Whether startup recovery has finished. */
    private boolean recovered;

    /** An unfinished item to resume, if a listing finds it; or null. */
    private LandingCheckpoint unfinished;

    /** A landed item whose message isn't acknowledged yet; or null. */
    private LandedItem unannounced;

    /** Whether the source was unreachable at the last listing. */
    private volatile boolean unreachable;

    /** Errors in a row, for the backoff. */
    private int consecutiveErrors;

    /**
     * Creates the service.
     *
     * @param intake         the intake's name
     * @param source         where items come from
     * @param readiness      decides when an item is ready
     * @param storage        the landing storage
     * @param announcer      sends {@code item.landed} messages
     * @param claim          the read-write source's directory claim; null
     *                       when read-only
     * @param fatal          stops the process
     * @param clock          gives the times item IDs are made from
     * @param initialBackoff the wait after the first error in a row
     */
    public IntakeService(String intake, IntakeSource source,
            Readiness readiness, Storage storage, Announcer announcer,
            InboundDirectoryClaim claim, FatalExit fatal, Clock clock,
            Duration initialBackoff)
    {
        this.intake = intake;
        this.source = source;
        this.readiness = readiness;
        this.storage = storage;
        this.announcer = announcer;
        this.claim = claim;
        this.fatal = fatal;
        this.clock = clock;
        this.initialBackoff = initialBackoff;
    }

    /**
     * Recovers from a crash, before the route starts. If storage or the
     * broker is down, recovery is tried again before each item.
     */
    public void recoverAtStartup()
    {
        writer.lock();
        try
        {
            ensureRecovered();
        }
        catch (Throwable failure)
        {
            onError(failure);
        }
        finally
        {
            writer.unlock();
        }
    }

    /**
     * Handles an item a listing found.
     *
     * @param item the item
     */
    public void onListed(ListedItem item)
    {
        writer.lock();
        try
        {
            handle(item);
            consecutiveErrors = 0;
        }
        catch (Throwable failure)
        {
            onError(failure);
        }
        finally
        {
            writer.unlock();
        }
    }

    /**
     * Looks an item up, and lands it if it is new and ready.
     *
     * @param item the item
     * @throws Exception if storage, the broker or the source fails
     */
    private void handle(ListedItem item) throws Exception
    {
        ensureRecovered();
        if (claim != null)
        {
            claim.verify();
        }
        announcePending();
        nowReachable();
        SourceRef ref = SourceRef.of(storage.ids(), intake, item.ref(),
                item.size(), item.versionText());
        if (unfinished != null)
        {
            if (unfinished.source().hash().equals(ref.hash()))
            {
                land(new NewItem(unfinished.itemId(), unfinished.kind(),
                        ref, item.ref(), null), item);
            }
            return;
        }
        if (storage.index().contains(ref.key()))
        {
            source.afterLanding(item);
            readiness.forget(item.ref());
            return;
        }
        if (readiness.ready(item))
        {
            land(new NewItem(UuidV7.next(clock.millis()), ItemKind.FILE,
                    ref, item.ref(), null), item);
        }
    }

    /**
     * Ends a listing. If the source couldn't be listed, reports it once;
     * otherwise forgets items that have gone, and gives up an unfinished
     * item the listing didn't find.
     */
    public void onListingDone()
    {
        writer.lock();
        try
        {
            ensureRecovered();
            if (!source.reachable())
            {
                if (!unreachable)
                {
                    unreachable = true;
                    Telemetry.get().auditError("intake.unreachable",
                            Fields.of(Attr.INTAKE, intake).with(
                                    Attr.ERROR_CODE,
                                    ErrorCode.INTAKE_UNREACHABLE));
                }
                return;
            }
            nowReachable();
            readiness.listingDone();
            if (unfinished != null)
            {
                UUID itemId = unfinished.itemId();
                discard(itemId);
                Telemetry.get().auditWarn("item.abandoned",
                        Fields.of(Attr.ITEM_ID, itemId)
                                .with(Attr.INTAKE, intake));
            }
        }
        catch (Throwable failure)
        {
            onError(failure);
        }
        finally
        {
            writer.unlock();
        }
    }

    /**
     * The lock held while the intake writes to landing storage: table
     * maintenance takes it only when it is free.
     *
     * @return the lock
     */
    public ReentrantLock writerLock()
    {
        return writer;
    }

    /**
     * Tells whether the source could be listed last time, for the
     * {@code sdh.landing.source_reachable} gauge.
     *
     * @return true if it could
     */
    public boolean reachable()
    {
        return !unreachable;
    }

    /**
     * How many listed items are waiting to settle, for the
     * {@code sdh.landing.items_waiting} gauge.
     *
     * @return the number of items seen and not yet landed
     */
    public int waiting()
    {
        return readiness.waiting();
    }

    /**
     * Runs startup recovery, once: repairs the ledger, notes an unfinished
     * item to resume, and sends the last item's message again.
     *
     * @throws Exception if storage or the broker fails
     */
    private void ensureRecovered() throws Exception
    {
        if (recovered)
        {
            return;
        }
        ItemLander lander = storage.lander();
        if (storage.ledger().repair(lander, storage.index()))
        {
            Telemetry.get().audit("ledger.repaired",
                    Fields.of(Attr.INTAKE, intake));
        }
        Optional<LandingCheckpoint> stored = lander.checkpoint();
        if (stored.isPresent() && !stored.get().done())
        {
            if (stored.get().source() == null)
            {
                lander.clear(stored.get().itemId());
            }
            else
            {
                unfinished = stored.get();
            }
        }
        unannounced = lander.lastLanded().orElse(null);
        announcePending();
        recovered = true;
        LOG.info("intake.recovered resume={} ledger_entries={}",
                unfinished != null, storage.index().size());
    }

    /**
     * Lands an item, records it, announces it and finishes it at the
     * source. An item that changed is given up and forgotten, to be
     * landed afresh once it settles again.
     *
     * @param item   the item to land
     * @param listed the item as listed
     * @throws Exception if storage, the broker or the source fails
     */
    private void land(NewItem item, ListedItem listed) throws Exception
    {
        LandedItem landed;
        try
        {
            landed = storage.lander().land(item,
                    offset -> source.open(listed, offset));
        }
        catch (ItemLander.ItemChangedException changed)
        {
            discard(item.itemId());
            readiness.forget(listed.ref());
            Telemetry.get().auditWarn("item.changed",
                    Fields.of(Attr.ITEM_ID, item.itemId())
                            .with(Attr.INTAKE, intake));
            return;
        }
        unfinished = null;
        boolean seenContent =
                storage.index().mayHaveContent(landed.digest());
        storage.ledger().record(List.of(LandingLedger.Entry.of(landed)));
        storage.index().add(landed.source().key(), landed.digest());
        if (seenContent)
        {
            reportDuplicate(landed);
        }
        unannounced = landed;
        announcePending();
        source.afterLanding(listed);
        readiness.forget(listed.ref());
    }

    /**
     * Sends the message of a landed item not yet acknowledged.
     *
     * @throws Exception if the broker doesn't acknowledge it
     */
    private void announcePending() throws Exception
    {
        if (unannounced != null)
        {
            announcer.announce(unannounced);
            unannounced = null;
        }
    }

    /**
     * Warns that a landed item has the same content as items landed
     * before (design §5.3.5). It stays landed: a producer may send the
     * same content again on purpose.
     *
     * @param landed the new item
     * @throws Exception if the ledger can't be read
     */
    private void reportDuplicate(LandedItem landed) throws Exception
    {
        List<UUID> others = storage.ledger().itemsWithDigest(landed.digest())
                .stream().filter(other -> !other.equals(landed.itemId()))
                .toList();
        if (!others.isEmpty())
        {
            Telemetry.get().auditWarn("item.duplicate_content",
                    Fields.of(Attr.ITEM_ID, landed.itemId())
                            .with(Attr.INTAKE, intake)
                            .with(Attr.COUNT, others.size())
                            .with(Attr.REASON, "same content as item "
                                    + others.get(0)));
        }
    }

    /**
     * Removes the checkpoint of an item being given up, if it is still the
     * unfinished one.
     *
     * @param itemId the item
     * @throws Exception if storage fails
     */
    private void discard(UUID itemId) throws Exception
    {
        unfinished = null;
        Optional<LandingCheckpoint> stored = storage.lander().checkpoint();
        if (stored.isPresent() && !stored.get().done()
                && stored.get().itemId().equals(itemId))
        {
            storage.lander().clear(itemId);
        }
    }

    /** Reports that an unreachable source is back. */
    private void nowReachable()
    {
        if (unreachable)
        {
            unreachable = false;
            Telemetry.get().audit("intake.reachable",
                    Fields.of(Attr.INTAKE, intake));
        }
    }

    /**
     * Stops the process for a fatal error; otherwise waits, longer for
     * each error in a row, so the route pauses before its next try.
     *
     * @param failure the error
     */
    private void onError(Throwable failure)
    {
        if (isFatal(failure))
        {
            String reason = reason(failure);
            if (failure instanceof InboundDirectoryClaim.ClaimedException
                    claimed)
            {
                Fields fields = Fields.of(Attr.ERROR_CODE, claimed.code());
                if (claimed.holder() != null)
                {
                    fields = fields.with(Attr.REASON,
                            claimed.holder().describe());
                }
                Telemetry.get().auditError("connector.directory_claimed",
                        fields);
            }
            LOG.error("intake.fatal reason={}", reason);
            fatal.exit(reason);
            return;
        }
        consecutiveErrors++;
        long waitMillis = Math.min(MAX_BACKOFF.toMillis(),
                initialBackoff.toMillis()
                        << Math.min(consecutiveErrors - 1, 20));
        LOG.warn("intake.retry error={} attempt={} backoff_ms={}",
                failure.getClass().getSimpleName(), consecutiveErrors,
                waitMillis);
        try
        {
            Thread.sleep(waitMillis);
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Tells whether an error means the node must stop.
     *
     * @param failure the error
     * @return true if it does
     */
    static boolean isFatal(Throwable failure)
    {
        return failure instanceof Error
                || failure instanceof LeaseLostException
                || failure instanceof CommitStateUnknownException
                || failure instanceof CheckpointTamperedException
                || failure instanceof IllegalStateException
                || failure instanceof InboundDirectoryClaim.ClaimedException;
    }

    /**
     * The reason a fatal error gives, without PHI.
     *
     * @param failure the error
     * @return the reason
     */
    private static String reason(Throwable failure)
    {
        if (failure instanceof LeaseLostException lost)
        {
            return "LEASE_LOST:" + lost.reason();
        }
        if (failure instanceof CheckpointTamperedException)
        {
            return ErrorCode.CHECKPOINT_TAMPERED.name();
        }
        if (failure instanceof CommitStateUnknownException)
        {
            return "COMMIT_STATE_UNKNOWN";
        }
        if (failure instanceof InboundDirectoryClaim.ClaimedException)
        {
            return ErrorCode.INBOUND_DIR_CLAIMED.name();
        }
        return failure.getClass().getSimpleName();
    }
}
