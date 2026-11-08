/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandingCheckpoint;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.landing.SourceRef;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

/**
 * Lands an item already landed again, as a new item linked to it with
 * {@code replaces_item_id} (STAGES.md §13.5): what a producer's
 * resubmission does, but from the bytes landing kept, for an item the
 * pipeline failed for a reason that wasn't the item's (an outage, before
 * the split stage recognised one). Both items are kept; nothing is changed
 * in place.
 * <p>
 * The new item has the old one's kind, original name, message index and
 * producer's references, so its receipt and result reach the producer as
 * the first did, naming the item it replaces. Its source reference is the
 * request's, so the ledger tells it apart from the producer's own items,
 * and {@code item.duplicate_content} notes the repeat.
 * <p>
 * An intake service calls {@link #land} under its writer lock, once its own
 * recovery is done, then records and announces the item as it does its
 * own. The new item's ID is noted on the request before landing: tried
 * again, a request whose item was landed finds it, and one whose landing
 * stopped part-way is landed afresh under a new ID, its checkpoint
 * cleared.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Resubmitter
{

    /** Why a request was refused: the item has no summary row. */
    public static final String NOT_LANDED = "NOT_LANDED";

    /** Why a request was refused: another intake landed the item. */
    public static final String OTHER_INTAKE = "OTHER_INTAKE";

    /** What a resubmission's source reference starts with. */
    static final String SOURCE_PREFIX = "resubmit\u0000";

    /**
     * An item landed again.
     *
     * @param item     the new item
     * @param recorded whether its ledger entry was written already, by an
     *                 earlier try
     */
    public record Landed(LandedItem item, boolean recorded)
    {
    }

    /** Thrown when a request can't be carried out, ever. */
    public static final class RefusedException extends Exception
    {

        /** Serialization version. */
        private static final long serialVersionUID = 1L;

        /** Why, such as {@link #NOT_LANDED}. */
        private final String reason;

        /**
         * Creates the exception.
         *
         * @param reason why
         */
        public RefusedException(String reason)
        {
            super("Resubmission refused: " + reason);
            this.reason = reason;
        }

        /**
         * Why it was refused.
         *
         * @return the reason, such as {@link #NOT_LANDED}
         */
        public String reason()
        {
            return reason;
        }
    }

    /**
     * Thrown when the intake has an unfinished item of its own to resume
     * first: the request is tried again later.
     */
    public static final class BusyException extends Exception
    {

        /** Serialization version. */
        private static final long serialVersionUID = 1L;

        /** Creates the exception. */
        public BusyException()
        {
            super("The intake has an unfinished item to resume first");
        }
    }

    /** Landing's {@value LandingTables#LANDED_ITEMS}. */
    private final Table landedItems;

    /** Unwraps the landed items' data keys. */
    private final KeyProvider keys;

    /** Where requests are kept. */
    private final ResubmitRequests requests;

    /** Gives the time for the new item's ID. */
    private final Clock clock;

    /**
     * Creates the resubmitter.
     *
     * @param landedItems landing's {@value LandingTables#LANDED_ITEMS}
     * @param keys        unwraps the landed items' data keys
     * @param requests    where requests are kept
     * @param clock       gives the time
     */
    public Resubmitter(Table landedItems, KeyProvider keys,
            ResubmitRequests requests, Clock clock)
    {
        this.landedItems = Objects.requireNonNull(landedItems);
        this.keys = Objects.requireNonNull(keys);
        this.requests = Objects.requireNonNull(requests);
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * Where requests are kept.
     *
     * @return the request store
     */
    public ResubmitRequests requests()
    {
        return requests;
    }

    /**
     * Lands a request's item again, or finds the new item an earlier try
     * landed. Called under the intake's writer lock, after its recovery.
     *
     * @param storage the intake's landing storage
     * @param intake  the intake's name
     * @param request the request
     * @return the new item
     * @throws RefusedException if the item isn't fully landed, or another
     *                          intake landed it
     * @throws BusyException    if the intake has an unfinished item
     * @throws Exception        if storage or the key service fails: the
     *                          request is tried again later
     */
    Landed land(IntakeService.Storage storage, String intake,
            ResubmitRequests.Request request) throws Exception
    {
        landedItems.refresh();
        if (request.newItemId() != null)
        {
            Optional<Record> landed =
                    LandedItems.summary(landedItems, request.newItemId());
            if (landed.isPresent())
            {
                return new Landed(LandedItems.landedItem(landed.get(),
                        landedItems.currentSnapshot().snapshotId()),
                        storage.ledger().contains(intake,
                                request.newItemId()));
            }
        }
        Record summary = LandedItems.summary(landedItems, request.itemId())
                .orElseThrow(() -> new RefusedException(NOT_LANDED));
        if (!intake.equals(summary.getField(LandingTables.INTAKE)))
        {
            throw new RefusedException(OTHER_INTAKE);
        }
        ItemLander lander = storage.lander();
        Optional<LandingCheckpoint> stored = lander.checkpoint();
        if (stored.isPresent() && !stored.get().done())
        {
            if (!stored.get().itemId().equals(request.newItemId()))
            {
                throw new BusyException();
            }
            // An earlier try stopped part-way: start again under a new ID.
            lander.clear(request.newItemId());
        }

        LandedItem old = LandedItems.landedItem(summary,
                landedItems.currentSnapshot().snapshotId());
        ResubmitRequests.Request assigned =
                requests.assign(request, UuidV7.next(clock.millis()));
        NewItem item = new NewItem(assigned.newItemId(), old.kind(),
                SourceRef.of(storage.ids(), intake,
                        SOURCE_PREFIX + request.requestId(), -1, ""),
                LandedItems.originalName(summary, keys).orElse(null),
                request.itemId(),
                LandedItems.messageIndex(summary).orElse(null),
                LandedItems.messageRefs(summary, keys).orElse(null),
                LandedItems.messageReplaces(summary, keys).orElse(null));
        try (LandedItemSource source =
                LandedItemSource.open(landedItems, request.itemId(), keys))
        {
            return new Landed(lander.land(item,
                    offset -> new SourceStream(source, offset)), false);
        }
        catch (Exception | Error failure)
        {
            clearQuietly(lander, item, failure);
            throw failure;
        }
    }

    /**
     * Clears the checkpoint of a resubmission that stopped part-way, if it
     * is still the unfinished one; if storage won't, the next try does.
     *
     * @param lander  the intake's lander
     * @param item    the item being landed
     * @param failure why it stopped; a failure to clear is added to it
     */
    private static void clearQuietly(ItemLander lander, NewItem item,
            Throwable failure)
    {
        try
        {
            Optional<LandingCheckpoint> stored = lander.checkpoint();
            if (stored.isPresent() && !stored.get().done()
                    && stored.get().itemId().equals(item.itemId()))
            {
                lander.clear(item.itemId());
            }
        }
        catch (Exception uncleared)
        {
            failure.addSuppressed(uncleared);
        }
    }

    /** A landed item's bytes as a stream, from a position. */
    private static final class SourceStream extends InputStream
    {

        /** The item. */
        private final LandedItemSource source;

        /** Where the next read starts. */
        private long position;

        /**
         * Creates the stream.
         *
         * @param source the item
         * @param from   where to start
         */
        SourceStream(LandedItemSource source, long from)
        {
            this.source = source;
            this.position = from;
        }

        @Override
        public int read() throws IOException
        {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            if (length == 0)
            {
                return 0;
            }
            if (position >= source.size())
            {
                return -1;
            }
            int read = source.read(position,
                    ByteBuffer.wrap(buffer, offset, length));
            if (read > 0)
            {
                position += read;
            }
            return read;
        }
    }
}
