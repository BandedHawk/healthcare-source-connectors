/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandingCheckpoint;
import com.sdh.healthcare.connector.core.landing.LandingLedger;
import com.sdh.healthcare.connector.core.landing.LedgerIndex;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.landing.SourceRef;
import com.sdh.healthcare.connector.core.pipeline.Outage;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.InboundDirectoryClaim;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
public final class IntakeService implements LandingIntake
{

    /** The most a replaces marker is read: an item ID and some spaces. */
    private static final int MAX_REPLACES_BYTES = 256;

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
            LedgerIndex index, RecordIds ids)
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

    /**
     * Whether a landing stopped part-way, so the checkpoint may hold an
     * unfinished item that {@link #unfinished} doesn't know of yet.
     */
    private boolean checkpointStale;

    /** Whether the current listing has found the unfinished item. */
    private boolean unfinishedListed;

    /** A landed item whose message isn't acknowledged yet; or null. */
    private LandedItem unannounced;

    /**
     * The earlier item {@link #unannounced} is an exact copy of (STAGES.md
     * §15.5a); null if none.
     */
    private UUID unannouncedSameAs;

    /** Whether the source is answering, for events and gauges. */
    private final IntakeHealth health;

    /** How many items the last full listing found. */
    private volatile long listedItems;

    /** Errors in a row, for the backoff. */
    private int consecutiveErrors;

    /**
     * Whether the claim is checked once per listing, as on a remote
     * source, where each check is a call to the server, rather than before
     * every item.
     */
    private final boolean claimPerListing;

    /** Whether the claim has been checked in this listing. */
    private boolean claimChecked;

    /**
     * Creates the service.
     *
     * @param intake         the intake's name
     * @param source         where items come from
     * @param readiness      decides when an item is ready
     * @param storage        the landing storage
     * @param announcer      sends {@code item.landed} messages
     * @param claim          the read-write source's claim; null when
     *                       read-only. Checked before every item, or on a
     *                       remote source once per listing.
     * @param fatal          stops the process
     * @param clock          gives the times item IDs are made from
     * @param initialBackoff the wait after the first error in a row
     * @param health         whether the source is answering
     */
    public IntakeService(String intake, IntakeSource source,
            Readiness readiness, Storage storage, Announcer announcer,
            InboundDirectoryClaim claim, FatalExit fatal, Clock clock,
            Duration initialBackoff, IntakeHealth health)
    {
        this.intake = intake;
        this.source = source;
        this.readiness = readiness;
        this.storage = storage;
        this.announcer = announcer;
        this.claim = claim;
        this.claimPerListing = source instanceof RemoteSource;
        this.fatal = fatal;
        this.clock = clock;
        this.initialBackoff = initialBackoff;
        this.health = health;
    }

    /**
     * Recovers from a crash, before the route starts. If storage or the
     * broker is down, recovery is tried again before each item.
     */
    @Override
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
        reloadUnfinished();
        if (claim != null && !(claimPerListing && claimChecked))
        {
            claim.verify();
            claimChecked = true;
        }
        announcePending();
        health.answered();
        SourceRef ref = SourceRef.of(storage.ids(), intake, item.ref(),
                item.size(), item.versionText());
        if (unfinished != null)
        {
            if (unfinished.source().hash().equals(ref.hash()))
            {
                unfinishedListed = true;
                land(new NewItem(unfinished.itemId(), unfinished.kind(),
                        ref, item.ref(), replaces(item)), item);
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
            land(new NewItem(UuidV7.next(clock.millis()), source.kind(),
                    ref, item.ref(), replaces(item)), item);
        }
    }

    /**
     * The earlier item an item names, by a value its source gives or by
     * its replaces marker, if it is one this intake landed (design
     * §5.3.8). The link is only recorded; nothing is
     * replaced. A marker that is malformed or names another item is
     * reported and left out, and the item is landed all the same, so
     * nothing is lost; a marker that can't be read is tried again.
     *
     * @param item the item about to be landed
     * @return the earlier item; null if none
     * @throws IOException if the source can't be read
     */
    private UUID replaces(ListedItem item) throws IOException
    {
        Optional<String> given = source.replacesValue(item);
        if (given.isPresent())
        {
            if (given.get().length() > MAX_REPLACES_BYTES)
            {
                ignoredLink(intake, "MALFORMED");
                return null;
            }
            return linkedItem(given.get());
        }
        Optional<ListedItem> marker = source.replacesMarker(item);
        if (marker.isEmpty())
        {
            return null;
        }
        byte[] text;
        try (InputStream input = source.open(marker.get(), 0))
        {
            text = input.readNBytes(MAX_REPLACES_BYTES + 1);
        }
        catch (ItemLander.ItemChangedException gone)
        {
            return null;
        }
        if (text.length > MAX_REPLACES_BYTES)
        {
            ignoredLink(intake, "MALFORMED");
            return null;
        }
        return linkedItem(new String(text, StandardCharsets.US_ASCII));
    }

    /**
     * The earlier item a producer named, if it is one this intake landed;
     * if not, reports it and gives null.
     *
     * @param text the item ID as the producer gave it
     * @return the earlier item; null if it isn't one
     * @throws IOException if the ledger can't be read
     */
    private UUID linkedItem(String text) throws IOException
    {
        String reason;
        try
        {
            UUID earlier = UUID.fromString(text.strip());
            if (storage.ledger().contains(intake, earlier))
            {
                return earlier;
            }
            reason = "UNKNOWN";
        }
        catch (IllegalArgumentException malformed)
        {
            reason = "MALFORMED";
        }
        ignoredLink(intake, reason);
        return null;
    }

    /**
     * Reports a link to an earlier item, or message, that was left out.
     *
     * @param intake the intake
     * @param reason {@code MALFORMED}, or {@code UNKNOWN} for an item this
     *               intake didn't land
     */
    static void ignoredLink(String intake, String reason)
    {
        Telemetry.get().auditWarn("item.replaces_ignored",
                Fields.of(Attr.INTAKE, intake).with(Attr.REASON, reason));
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
            claimChecked = false;
            ensureRecovered();
            if (!source.reachable())
            {
                health.unreachable((IntakeProblem) null);
                return;
            }
            health.answered();
            readiness.listingDone();
            reloadUnfinished();
            boolean listed = unfinishedListed;
            unfinishedListed = false;
            if (unfinished != null && !listed)
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
     * Called when a remote source couldn't be listed. Reported once, until
     * a listing works again; nothing else changes, so an unfinished item
     * is kept to resume.
     *
     * @param problem what went wrong, with no names or paths
     */
    public void onListingFailed(IntakeProblem problem)
    {
        writer.lock();
        try
        {
            unfinishedListed = false;
            claimChecked = false;
            health.unreachable(problem);
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
    @Override
    public ReentrantLock writerLock()
    {
        return writer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Resubmitter.Landed resubmit(Resubmitter resubmitter,
            ResubmitRequests.Request request) throws Exception
    {
        writer.lockInterruptibly();
        try
        {
            ensureRecovered();
            announcePending();
            Resubmitter.Landed landed;
            try
            {
                landed = resubmitter.land(storage, intake, request);
            }
            catch (Exception | Error failure)
            {
                // Its checkpoint, if storage wouldn't clear it, is given up
                // by the next listing, which doesn't find it.
                checkpointStale = true;
                throw failure;
            }
            LandedItem item = landed.item();
            UUID sameAs;
            if (!landed.recorded())
            {
                boolean seenContent =
                        storage.index().mayHaveContent(item.digest());
                storage.ledger().record(
                        List.of(LandingLedger.Entry.of(item)));
                storage.index().add(item.source().key(), item.digest());
                sameAs = seenContent ? reportDuplicate(item) : null;
            }
            else
            {
                sameAs = sameAs(storage, item);
            }
            unannounced = item;
            unannouncedSameAs = sameAs;
            announcePending();
            return landed;
        }
        finally
        {
            writer.unlock();
        }
    }

    /**
     * Tells whether the source could be listed last time.
     *
     * @return true if it could
     */
    public boolean reachable()
    {
        return health.reachable();
    }

    /**
     * Whether the source is answering, for the intake health gauges.
     *
     * @return the intake's health
     */
    public IntakeHealth health()
    {
        return health;
    }

    /**
     * Records how long a listing took and how many items it found, for
     * {@code sdh.intake.list_duration} and {@code sdh.intake.listed_items}.
     *
     * @param nanos how long the listing took, in nanoseconds
     * @param items how many items it found
     */
    public void listingTook(long nanos, int items)
    {
        listedItems = items;
        Telemetry.get().listed(intake, nanos);
    }

    /**
     * How many items the last listing found, for the
     * {@code sdh.intake.listed_items} gauge.
     *
     * @return the number of items
     */
    public long listedItems()
    {
        return listedItems;
    }

    /**
     * How many listed items are waiting to settle, for the
     * {@code sdh.landing.backlog} gauge.
     *
     * @return the number of items seen and not yet landed
     */
    public int waiting()
    {
        return readiness.waiting();
    }

    /**
     * How long the item that has waited longest has been waiting, for the
     * {@code sdh.landing.oldest_waiting_age} gauge.
     *
     * @return whole seconds; 0 if none is waiting
     */
    public long oldestWaitingSeconds()
    {
        return TimeUnit.NANOSECONDS.toSeconds(readiness.oldestWaitingNanos());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long ledgerItems()
    {
        return storage.index().size();
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
        unannouncedSameAs = sameAs(storage, unannounced);
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
            checkpointStale = false;
            discard(item.itemId());
            readiness.forget(listed.ref());
            Telemetry.get().auditWarn("item.changed",
                    Fields.of(Attr.ITEM_ID, item.itemId())
                            .with(Attr.INTAKE, intake));
            return;
        }
        catch (Exception failure)
        {
            // A read or commit failed part-way: chunks may be committed
            // under a checkpoint, so the item must be resumed, not
            // started again under a new ID.
            checkpointStale = true;
            unfinishedListed = true;
            throw failure;
        }
        unfinished = null;
        boolean seenContent =
                storage.index().mayHaveContent(landed.digest());
        storage.ledger().record(List.of(LandingLedger.Entry.of(landed)));
        storage.index().add(landed.source().key(), landed.digest());
        UUID sameAs = seenContent ? reportDuplicate(landed) : null;
        unannounced = landed;
        unannouncedSameAs = sameAs;
        announcePending();
        source.afterLanding(listed);
        readiness.forget(listed.ref());
    }

    /**
     * After a landing stopped part-way, picks up the unfinished item from
     * the checkpoint, so the next listing resumes it. A storage failure
     * here is retried with the next item, like any other.
     *
     * @throws Exception if the checkpoint can't be read
     */
    private void reloadUnfinished() throws Exception
    {
        if (!checkpointStale)
        {
            return;
        }
        Optional<LandingCheckpoint> stored = storage.lander().checkpoint();
        unfinished = stored.isPresent() && !stored.get().done()
                && stored.get().source() != null ? stored.get() : null;
        checkpointStale = false;
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
            announcer.announce(unannounced, unannouncedSameAs);
            unannounced = null;
            unannouncedSameAs = null;
        }
    }

    /**
     * Warns that a landed item has the same content as items landed
     * before (design §5.3.5). It stays landed: a producer may send the
     * same content again on purpose.
     *
     * @param landed the new item
     * @return the earlier item it is an exact copy of; null if none
     * @throws Exception if the ledger can't be read
     */
    private UUID reportDuplicate(LandedItem landed) throws Exception
    {
        return reportDuplicate(storage, intake, landed);
    }

    /**
     * Warns that a landed item has the same content as items landed
     * before, if the ledger confirms it, and finds the one it is an exact
     * copy of (STAGES.md §15.5a).
     *
     * @param storage the landing storage
     * @param intake  the intake's name
     * @param landed  the new item
     * @return the earliest earlier item with the same content, other than
     *         the one it replaces; null if none
     * @throws Exception if the ledger can't be read
     */
    static UUID reportDuplicate(Storage storage, String intake,
            LandedItem landed) throws Exception
    {
        List<UUID> others = storage.ledger().itemsWithDigest(landed.digest())
                .stream().filter(other -> !other.equals(landed.itemId()))
                .toList();
        UUID sameAs = LandingLedger.earliest(others, landed.itemId(),
                landed.replaces()).orElse(null);
        if (!others.isEmpty())
        {
            Telemetry.get().auditWarn("item.duplicate_content",
                    Fields.of(Attr.ITEM_ID, landed.itemId())
                            .with(Attr.INTAKE, intake)
                            .with(Attr.COUNT, others.size())
                            .with(Attr.REASON, "same content as item "
                                    + (sameAs != null ? sameAs
                                            : others.get(0))));
        }
        return sameAs;
    }

    /**
     * The earlier item a landed item is an exact copy of, from the ledger,
     * for an item whose landing isn't at hand: one found at startup, or
     * recorded before.
     *
     * @param storage the landing storage
     * @param landed  the item; or null
     * @return the earliest earlier item with the same content, other than
     *         the one it replaces; null if none, or if there's no item
     * @throws IOException if the ledger can't be read
     */
    static UUID sameAs(Storage storage, LandedItem landed) throws IOException
    {
        return landed == null ? null
                : storage.ledger().earliestCopy(landed.itemId(),
                        landed.digest(), landed.replaces()).orElse(null);
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
     * Tells whether an error means the node must stop. An
     * {@link IllegalStateException} means a broken invariant, unless a
     * service being down caused it ({@link Outage}): signing a message
     * through a key service that is down, for one, ends in one. Then the
     * node stays up and the work is tried again, as for any other error
     * (design revision (ca)). A lost lease, an unknown commit outcome, a
     * tampered checkpoint and a claimed directory stop it whatever caused
     * them.
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
                || failure instanceof InboundDirectoryClaim.ClaimedException
                || failure instanceof IllegalStateException
                        && !Outage.caused(failure);
    }

    /**
     * The reason a fatal error gives, without PHI.
     *
     * @param failure the error
     * @return the reason
     */
    static String reason(Throwable failure)
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
