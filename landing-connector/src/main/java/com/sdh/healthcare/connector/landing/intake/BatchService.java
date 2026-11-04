/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandingCheckpoint;
import com.sdh.healthcare.connector.core.landing.LandingLedger;
import com.sdh.healthcare.connector.core.landing.LedgerIndex;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.landing.RecordIndex;
import com.sdh.healthcare.connector.core.landing.SourceRef;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.FatalExit;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lands batches of queue messages (design §5.3.16), one batch at a time.
 * Each batch becomes one item: its messages' bodies one after another, with
 * a record index saying where each starts and holding the keyed hash of
 * each message's broker ID. The caller acknowledges the messages only once
 * this says the batch is landed; anything else and the broker delivers them
 * again.
 * <p>
 * A message delivered again after its batch was landed, because the
 * acknowledgement was lost, is recognised by its ID if it came within
 * {@code dedup-window} of the last landed batch, and is acknowledged
 * without being landed again. A message without an ID can't be recognised.
 * <p>
 * Before each batch, whatever the last one left is finished: its ledger
 * entry and its {@code item.landed} message, so no item is announced behind
 * a newer one. A batch that fails part-way has its checkpoint cleared: its
 * messages are delivered again, so it is never resumed. Errors that mean
 * the node can't safely go on stop the process. Nothing about a message's
 * content or ID is logged.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class BatchService implements LandingIntake
{

    /** Logs progress and errors, never content or message IDs. */
    private static final Logger LOG =
            LoggerFactory.getLogger(BatchService.class);

    /**
     * A message as the broker delivered it.
     *
     * @param body      its body
     * @param messageId the broker's ID for it; null if it has none
     */
    public record Message(byte[] body, String messageId)
    {
    }

    /** What happened to a batch. */
    public enum Status
    {

        /** It was landed; acknowledge its messages. */
        LANDED,

        /**
         * Every message in it was landed before; acknowledge them, nothing
         * was landed.
         */
        SKIPPED,

        /**
         * It couldn't be landed now; let the broker deliver its messages
         * again.
         */
        FAILED,

        /**
         * The node is stopping on an error it can't go on from; don't
         * acknowledge, and take no more messages.
         */
        FATAL
    }

    /**
     * What happened to a batch.
     *
     * @param status  what happened
     * @param landed  how many messages were landed
     * @param skipped how many were landed before and skipped
     */
    public record Result(Status status, int landed, int skipped)
    {
    }

    /** The intake's name. */
    private final String intake;

    /** The landing storage. */
    private final IntakeService.Storage storage;

    /** The {@code landed_items} table, to read recent message keys. */
    private final Table landedItems;

    /** Sends {@code item.landed} messages. */
    private final Announcer announcer;

    /** Stops the process. */
    private final FatalExit fatal;

    /** Gives the times item IDs are made from. */
    private final Clock clock;

    /** The keys of messages landed lately. */
    private final RecentKeys recent;

    /** Held while a batch is landed, and by table maintenance. */
    private final ReentrantLock writer = new ReentrantLock();

    /** Whether startup recovery has finished. */
    private boolean recovered;

    /** Whether a batch may have left an unfinished checkpoint. */
    private boolean unfinished;

    /** A landed item whose ledger entry isn't written yet; or null. */
    private LandedItem unrecorded;

    /** A landed item whose message isn't acknowledged yet; or null. */
    private LandedItem unannounced;

    /**
     * Creates the service.
     *
     * @param intake      the intake's name
     * @param storage     the landing storage
     * @param landedItems the {@code landed_items} table
     * @param announcer   sends {@code item.landed} messages
     * @param fatal       stops the process
     * @param clock       gives the times item IDs are made from
     * @param dedupWindow how far back from the last landed batch a message
     *                    delivered again is recognised
     */
    public BatchService(String intake, IntakeService.Storage storage,
            Table landedItems, Announcer announcer, FatalExit fatal,
            Clock clock, Duration dedupWindow)
    {
        this.intake = intake;
        this.storage = storage;
        this.landedItems = landedItems;
        this.announcer = announcer;
        this.fatal = fatal;
        this.clock = clock;
        this.recent = new RecentKeys(dedupWindow);
    }

    @Override
    public void recoverAtStartup()
    {
        writer.lock();
        try
        {
            prepare();
        }
        catch (Throwable failure)
        {
            failed(failure);
        }
        finally
        {
            writer.unlock();
        }
    }

    @Override
    public ReentrantLock writerLock()
    {
        return writer;
    }

    /**
     * How many message keys are held to recognise messages delivered again.
     *
     * @return the count
     */
    public int recentKeys()
    {
        return recent.size();
    }

    /**
     * Lands a batch of messages as one item.
     *
     * @param messages the messages, in the order the broker gave them
     * @return what happened
     * @throws InterruptedException if waiting for maintenance to finish is
     *                              interrupted
     */
    public Result land(List<Message> messages) throws InterruptedException
    {
        writer.lockInterruptibly();
        try
        {
            return landLocked(messages);
        }
        finally
        {
            writer.unlock();
        }
    }

    /**
     * Lands a batch, holding the writer lock.
     *
     * @param messages the messages
     * @return what happened
     */
    private Result landLocked(List<Message> messages)
    {
        UUID itemId = UuidV7.next(clock.millis());
        List<byte[]> bodies = new ArrayList<>();
        List<Long> offsets = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        int skipped = 0;
        long size = 0;
        try
        {
            prepare();
            Set<LedgerIndex.Key> seen = new HashSet<>();
            for (Message message : messages)
            {
                String key = null;
                if (message.messageId() != null)
                {
                    SourceRef ref = SourceRef.of(storage.ids(), intake,
                            "message\u0000" + message.messageId(), -1, "");
                    if (recent.contains(ref.key()) || !seen.add(ref.key()))
                    {
                        skipped++;
                        continue;
                    }
                    key = ref.hash();
                }
                offsets.add(size);
                keys.add(key);
                bodies.add(message.body());
                size += message.body().length;
            }
        }
        catch (Throwable failure)
        {
            return failed(failure);
        }
        if (bodies.isEmpty())
        {
            Telemetry.get().audit("batch.skipped", Fields.of(Attr.INTAKE,
                    intake).with(Attr.COUNT, skipped));
            return new Result(Status.SKIPPED, 0, skipped);
        }

        LandedItem landed;
        try
        {
            SourceRef ref = SourceRef.of(storage.ids(), intake,
                    "batch\u0000" + itemId, -1, "");
            landed = storage.lander().land(new NewItem(itemId,
                    ItemKind.MESSAGE_BATCH, ref, null, null,
                    new RecordIndex(offsets, keys)), offset ->
                    {
                        if (offset != 0)
                        {
                            throw new IOException("A batch can't resume");
                        }
                        return joined(bodies);
                    });
        }
        catch (Throwable failure)
        {
            unfinished = true;
            clearUnfinishedQuietly();
            return failed(failure);
        }

        recent.add(landed.landedAt(), keys);
        unrecorded = landed;
        Telemetry.get().audit("batch.landed", Fields.of(Attr.ITEM_ID, itemId)
                .with(Attr.INTAKE, intake).with(Attr.COUNT, bodies.size()));
        try
        {
            finishLast();
        }
        catch (Throwable failure)
        {
            // The item is committed, so its messages can be acknowledged;
            // its entry and message are finished before the next batch, or
            // at startup.
            if (IntakeService.isFatal(failure))
            {
                return failed(failure);
            }
            LOG.warn("batch.finish_deferred error={}",
                    failure.getClass().getSimpleName());
        }
        return new Result(Status.LANDED, bodies.size(), skipped);
    }

    /**
     * The bodies one after another, as one stream.
     *
     * @param bodies the bodies
     * @return the stream
     */
    private static InputStream joined(List<byte[]> bodies)
    {
        List<InputStream> streams = new ArrayList<>(bodies.size());
        for (byte[] body : bodies)
        {
            streams.add(new ByteArrayInputStream(body));
        }
        return new SequenceInputStream(Collections.enumeration(streams));
    }

    /**
     * Gets the intake ready for a batch: recovers once at startup, clears
     * an unfinished batch, and finishes the last landed one.
     *
     * @throws Exception if storage or the broker fails
     */
    private void prepare() throws Exception
    {
        if (!recovered)
        {
            if (storage.ledger().repair(storage.lander(), storage.index()))
            {
                Telemetry.get().audit("ledger.repaired",
                        Fields.of(Attr.INTAKE, intake));
            }
            unfinished = true;
            Optional<LandedItem> last = storage.lander().lastLanded();
            unannounced = last.orElse(null);
            if (last.isPresent())
            {
                loadRecent(last.get().landedAt());
            }
            recovered = true;
            LOG.info("intake.recovered recent_keys={}", recent.size());
        }
        if (unfinished)
        {
            clearUnfinished();
        }
        finishLast();
    }

    /**
     * Loads the keys of the messages landed within the window before the
     * last batch.
     *
     * @param lastLanded when the last batch was landed
     * @throws IOException if the table can't be read
     */
    private void loadRecent(Instant lastLanded) throws IOException
    {
        Instant since = lastLanded.minus(recent.window());
        recent.add(lastLanded, LandedItems.recordKeys(landedItems, intake,
                since));
    }

    /**
     * Writes the ledger entry and sends the message of the last landed
     * item, whichever are still to do.
     *
     * @throws Exception if storage or the broker fails
     */
    private void finishLast() throws Exception
    {
        if (unrecorded != null)
        {
            LandedItem landed = unrecorded;
            boolean seenContent =
                    storage.index().mayHaveContent(landed.digest());
            storage.ledger().record(List.of(LandingLedger.Entry.of(landed)));
            storage.index().add(landed.source().key(), landed.digest());
            unrecorded = null;
            unannounced = landed;
            if (seenContent)
            {
                IntakeService.reportDuplicate(storage, intake, landed);
            }
        }
        if (unannounced != null)
        {
            announcer.announce(unannounced);
            unannounced = null;
        }
    }

    /**
     * Clears the checkpoint of a batch that stopped part-way. Its committed
     * chunks stay, without a summary row, until retention removes them.
     *
     * @throws Exception if storage fails
     */
    private void clearUnfinished() throws Exception
    {
        ItemLander lander = storage.lander();
        Optional<LandingCheckpoint> stored = lander.checkpoint();
        if (stored.isPresent() && !stored.get().done())
        {
            UUID itemId = stored.get().itemId();
            lander.clear(itemId);
            Telemetry.get().auditWarn("item.abandoned",
                    Fields.of(Attr.ITEM_ID, itemId)
                            .with(Attr.INTAKE, intake));
        }
        unfinished = false;
    }

    /**
     * Clears an unfinished batch straight away, if storage allows; if not,
     * the next batch tries again first.
     */
    private void clearUnfinishedQuietly()
    {
        try
        {
            clearUnfinished();
        }
        catch (Throwable failure)
        {
            if (IntakeService.isFatal(failure))
            {
                failed(failure);
            }
        }
    }

    /**
     * Stops the process for a fatal error; otherwise reports the batch as
     * one to deliver again.
     *
     * @param failure the error
     * @return a {@link Status#FATAL} or {@link Status#FAILED} result
     */
    private Result failed(Throwable failure)
    {
        if (IntakeService.isFatal(failure))
        {
            String reason = IntakeService.reason(failure);
            LOG.error("intake.fatal reason={}", reason);
            fatal.exit(reason);
            return new Result(Status.FATAL, 0, 0);
        }
        LOG.warn("batch.failed error={}", failure.getClass().getSimpleName());
        return new Result(Status.FAILED, 0, 0);
    }

    /**
     * The keys of messages landed lately: those within the window before
     * the last batch. Older ones are forgotten as newer batches land.
     */
    static final class RecentKeys
    {

        /** How far back from the newest batch keys are kept. */
        private final Duration window;

        /** Each batch's keys, oldest first. */
        private final Deque<Landed> batches = new ArrayDeque<>();

        /** Every key held. */
        private final Set<LedgerIndex.Key> keys = new HashSet<>();

        /**
         * A batch's keys.
         *
         * @param at   when it was landed
         * @param keys its keys
         */
        private record Landed(Instant at, List<LedgerIndex.Key> keys)
        {
        }

        /**
         * Creates an empty set.
         *
         * @param window how far back keys are kept
         */
        RecentKeys(Duration window)
        {
            this.window = window;
        }

        /**
         * How far back keys are kept.
         *
         * @return the window
         */
        Duration window()
        {
            return window;
        }

        /**
         * Adds a batch's keys, and forgets those now outside the window.
         *
         * @param at     when the batch was landed
         * @param hashes its keys, as {@link SourceRef#hash()}; nulls are
         *               skipped
         */
        void add(Instant at, List<String> hashes)
        {
            List<LedgerIndex.Key> added = new ArrayList<>();
            for (String hash : hashes)
            {
                if (hash != null)
                {
                    LedgerIndex.Key key = new SourceRef(hash, -1, "").key();
                    if (keys.add(key))
                    {
                        added.add(key);
                    }
                }
            }
            batches.addLast(new Landed(at, added));
            Instant cutoff = at.minus(window);
            while (!batches.isEmpty()
                    && batches.peekFirst().at().isBefore(cutoff))
            {
                batches.removeFirst().keys().forEach(keys::remove);
            }
        }

        /**
         * Whether a key is held.
         *
         * @param key the key
         * @return true if a message with it was landed lately
         */
        boolean contains(LedgerIndex.Key key)
        {
            return keys.contains(key);
        }

        /**
         * How many keys are held.
         *
         * @return the count
         */
        int size()
        {
            return keys.size();
        }
    }
}
