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
import com.sdh.healthcare.connector.core.landing.LandingCheckpoint;
import com.sdh.healthcare.connector.core.landing.LandingLedger;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.landing.SourceRef;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.FatalExit;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lands HTTP pushes (design §5.3.15), one at a time. A push waits up to
 * {@code max-wait} for the one before it, then is turned away. Each push:
 * <ol>
 *   <li>finishes whatever the last push left: its ledger entry and its
 *       {@code item.landed} message, so no item is ever left unannounced
 *       behind a newer one;</li>
 *   <li>with an idempotency key the sender has used before, gets the first
 *       push's receipt back and lands nothing;</li>
 *   <li>otherwise streams its body straight into landing, checking its
 *       length and the size limit as it goes, and gets a receipt once the
 *       item is committed.</li>
 * </ol>
 * A push can't be resumed: a body that breaks off, or an item that can't
 * be committed, has its unfinished checkpoint cleared, and the sender sends
 * it again. The idempotency key is scoped to the sender, so one producer's
 * key never returns another's receipt; it is only ever hashed.
 * <p>
 * Errors that mean the node can't safely go on stop the process, as for a
 * listing intake. Nothing about a push's content or sender is logged.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PushService implements LandingIntake
{

    /** Logs progress and errors, never content or senders. */
    private static final Logger LOG =
            LoggerFactory.getLogger(PushService.class);

    /** What happened to a push. */
    public enum Status
    {

        /** It was landed; the receipt is new. */
        LANDED,

        /** Its idempotency key was used before; the receipt is the first. */
        REPLAYED,

        /** Another push held the intake for longer than the wait. */
        BUSY,

        /**
         * It named, in {@code Replaces}, an item this intake didn't land;
         * nothing was landed, and the producer can send it again.
         */
        UNKNOWN_REPLACES,

        /** Its body was larger than the limit. */
        TOO_LARGE,

        /** Its body broke off, or didn't match its stated length. */
        BROKEN,

        /** Storage or the broker failed: the sender should try later. */
        UNAVAILABLE
    }

    /**
     * What happened to a push.
     *
     * @param status  what happened
     * @param receipt the receipt, for {@link Status#LANDED} and
     *                {@link Status#REPLAYED}; null otherwise
     */
    public record Result(Status status, Receipt receipt)
    {

        /**
         * A result without a receipt.
         *
         * @param status what happened
         * @return the result
         */
        static Result of(Status status)
        {
            return new Result(status, null);
        }
    }

    /**
     * A push, as the route hands it over.
     *
     * @param sender         who sent it, from their authentication
     * @param idempotencyKey the sender's key for it; null if none
     * @param contentLength  the stated length of its body; -1 if not stated
     * @param correlationId  the producer's own reference for it, from
     *                       {@code X-Correlation-Id}, kept encrypted and
     *                       sent back in its receipt and result; null if
     *                       none
     * @param replaces       the earlier item it replaces, from
     *                       {@code Replaces}, recorded as a link; null if
     *                       none
     */
    public record Push(String sender, String idempotencyKey,
            long contentLength, String correlationId, UUID replaces)
    {

        /**
         * A push without a correlation ID or a replaced item.
         *
         * @param sender         who sent it
         * @param idempotencyKey the sender's key for it; null if none
         * @param contentLength  the stated length of its body; -1 if not
         *                       stated
         */
        public Push(String sender, String idempotencyKey, long contentLength)
        {
            this(sender, idempotencyKey, contentLength, null, null);
        }
    }

    /** The intake's name. */
    private final String intake;

    /** The landing storage. */
    private final IntakeService.Storage storage;

    /** Sends {@code item.landed} messages. */
    private final Announcer announcer;

    /** Stops the process. */
    private final FatalExit fatal;

    /** Gives the times item IDs are made from. */
    private final Clock clock;

    /** The largest body accepted; 0 for no limit. */
    private final long maxItemBytes;

    /** How long a push waits for the one before it. */
    private final Duration maxWait;

    /**
     * Held while a push is landed, and by table maintenance, so only one
     * writes at a time.
     */
    private final ReentrantLock writer = new ReentrantLock();

    /** Whether startup recovery has finished. */
    private boolean recovered;

    /**
     * Whether a push stopped part-way, so the checkpoint may hold an
     * unfinished item that must be cleared before the next.
     */
    private boolean unfinished;

    /** A landed item whose ledger entry isn't written yet; or null. */
    private LandedItem unrecorded;

    /** A landed item whose message isn't acknowledged yet; or null. */
    private LandedItem unannounced;

    /**
     * Creates the service.
     *
     * @param intake       the intake's name
     * @param storage      the landing storage
     * @param announcer    sends {@code item.landed} messages
     * @param fatal        stops the process
     * @param clock        gives the times item IDs are made from
     * @param maxItemBytes the largest body accepted; 0 for no limit
     * @param maxWait      how long a push waits for the one before it
     */
    public PushService(String intake, IntakeService.Storage storage,
            Announcer announcer, FatalExit fatal, Clock clock,
            long maxItemBytes, Duration maxWait)
    {
        this.intake = intake;
        this.storage = storage;
        this.announcer = announcer;
        this.fatal = fatal;
        this.clock = clock;
        this.maxItemBytes = maxItemBytes;
        this.maxWait = maxWait;
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
     * Lands a push, or gives back the receipt of the first push with the
     * same idempotency key.
     *
     * @param push the push
     * @param body its body, read once, to the end
     * @return what happened, with the receipt if it was landed
     */
    public Result receive(Push push, InputStream body)
    {
        boolean locked;
        try
        {
            locked = writer.tryLock(maxWait.toNanos(), TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
            return Result.of(Status.UNAVAILABLE);
        }
        if (!locked)
        {
            Telemetry.get().auditWarn("push.busy",
                    Fields.of(Attr.INTAKE, intake));
            return Result.of(Status.BUSY);
        }
        try
        {
            return receiveLocked(push, body);
        }
        finally
        {
            writer.unlock();
        }
    }

    /**
     * Lands a push, holding the writer lock.
     *
     * @param push the push
     * @param body its body
     * @return what happened
     */
    private Result receiveLocked(Push push, InputStream body)
    {
        UUID itemId = UuidV7.next(clock.millis());
        SourceRef ref;
        try
        {
            prepare();
            if (push.idempotencyKey() != null)
            {
                ref = SourceRef.of(storage.ids(), intake, "push\u0000"
                        + push.sender() + "\u0000" + push.idempotencyKey(),
                        -1, "");
                Optional<Receipt> earlier = earlier(ref);
                if (earlier.isPresent())
                {
                    Telemetry.get().audit("push.replayed",
                            Fields.of(Attr.ITEM_ID, earlier.get().itemId())
                                    .with(Attr.INTAKE, intake));
                    return new Result(Status.REPLAYED, earlier.get());
                }
            }
            else
            {
                ref = SourceRef.of(storage.ids(), intake,
                        "push\u0000item\u0000" + itemId, -1, "");
            }
            if (push.replaces() != null
                    && !storage.ledger().contains(intake, push.replaces()))
            {
                Telemetry.get().auditWarn("push.unknown_replaces",
                        Fields.of(Attr.INTAKE, intake));
                return Result.of(Status.UNKNOWN_REPLACES);
            }
        }
        catch (Throwable failure)
        {
            return failed(failure);
        }

        CheckedBody checked =
                new CheckedBody(body, maxItemBytes, push.contentLength());
        LandedItem landed;
        try
        {
            landed = storage.lander().land(
                    new NewItem(itemId, ItemKind.PUSH, ref,
                            push.correlationId(), push.replaces()),
                    offset ->
                    {
                        if (offset != 0)
                        {
                            throw new IOException("A push can't resume");
                        }
                        return checked;
                    });
        }
        catch (Throwable failure)
        {
            unfinished = true;
            clearUnfinishedQuietly();
            if (checked.tooLarge)
            {
                Telemetry.get().auditWarn("push.too_large",
                        Fields.of(Attr.ITEM_ID, itemId)
                                .with(Attr.INTAKE, intake)
                                .with(Attr.BYTES, checked.count));
                return Result.of(Status.TOO_LARGE);
            }
            if (checked.broken)
            {
                Telemetry.get().auditWarn("push.broken",
                        Fields.of(Attr.ITEM_ID, itemId)
                                .with(Attr.INTAKE, intake)
                                .with(Attr.BYTES, checked.count));
                return Result.of(Status.BROKEN);
            }
            return failed(failure);
        }

        unrecorded = landed;
        try
        {
            finishLast();
        }
        catch (Throwable failure)
        {
            // The item is committed, so its receipt holds; its entry and
            // message are finished before the next push, or at startup.
            if (IntakeService.isFatal(failure))
            {
                return failed(failure);
            }
            LOG.warn("push.finish_deferred error={}",
                    failure.getClass().getSimpleName());
        }
        return new Result(Status.LANDED, Receipt.of(landed));
    }

    /**
     * Gets the intake ready for a push: recovers once at startup, clears an
     * unfinished push, and finishes the last landed one.
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
            unannounced = storage.lander().lastLanded().orElse(null);
            recovered = true;
            LOG.info("intake.recovered ledger_entries={}",
                    storage.index().size());
        }
        if (unfinished)
        {
            clearUnfinished();
        }
        finishLast();
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
     * The receipt of an earlier push with the same key, if there was one.
     *
     * @param ref the key's source reference
     * @return the first push's receipt; empty if the key is new
     * @throws IOException if the ledger can't be read
     */
    private Optional<Receipt> earlier(SourceRef ref) throws IOException
    {
        if (!storage.index().contains(ref.key()))
        {
            return Optional.empty();
        }
        return storage.ledger().find(intake, ref).map(Receipt::of);
    }

    /**
     * Clears the checkpoint of a push that stopped part-way. Its committed
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
     * Clears an unfinished push straight away, if storage allows; if not,
     * the next push tries again first.
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
     * Stops the process for a fatal error; otherwise reports the push as
     * one to send again later.
     *
     * @param failure the error
     * @return {@link Status#UNAVAILABLE}
     */
    private Result failed(Throwable failure)
    {
        if (IntakeService.isFatal(failure))
        {
            String reason = IntakeService.reason(failure);
            LOG.error("intake.fatal reason={}", reason);
            fatal.exit(reason);
        }
        else
        {
            LOG.warn("push.failed error={}",
                    failure.getClass().getSimpleName());
        }
        return Result.of(Status.UNAVAILABLE);
    }

    /**
     * A push's body, counted as it is read: it fails once it passes the
     * size limit, or if it ends short of or beyond its stated length, and
     * notes when reading it failed, so a sender's broken connection is told
     * apart from a storage failure.
     */
    private static final class CheckedBody extends FilterInputStream
    {

        /** The largest body accepted; 0 for no limit. */
        private final long limit;

        /** The stated length; -1 if not stated. */
        private final long expected;

        /** The bytes read so far. */
        private long count;

        /** Whether the body passed the limit. */
        private boolean tooLarge;

        /** Whether the body broke off or didn't match its length. */
        private boolean broken;

        /**
         * Wraps a body.
         *
         * @param body     the body
         * @param limit    the largest body accepted; 0 for no limit
         * @param expected the stated length; -1 if not stated
         */
        CheckedBody(InputStream body, long limit, long expected)
        {
            super(body);
            this.limit = limit;
            this.expected = expected;
        }

        @Override
        public int read() throws IOException
        {
            byte[] one = new byte[1];
            int read = read(one, 0, 1);
            return read < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            int read;
            try
            {
                read = super.read(buffer, offset, length);
            }
            catch (IOException failure)
            {
                broken = true;
                throw failure;
            }
            if (read < 0)
            {
                if (expected >= 0 && count != expected)
                {
                    broken = true;
                    throw new EOFException("Body shorter than stated");
                }
                return read;
            }
            count += read;
            if (limit > 0 && count > limit)
            {
                tooLarge = true;
                throw new IOException("Body larger than the limit");
            }
            if (expected >= 0 && count > expected)
            {
                broken = true;
                throw new IOException("Body longer than stated");
            }
            return read;
        }

        /** Leaves the request's stream open for the server to finish. */
        @Override
        public void close()
        {
        }
    }
}
