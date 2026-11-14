/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.split;

import com.sdh.healthcare.connector.broker.receive.BatchHandler;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException.Reason;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemLanded;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemReprocess;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.checkpoint.LeaseHeldException;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.stage.StageFailures;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The split stage's input (STAGES.md §8): takes each {@code item.landed}
 * message, checks it, and splits its item. A message must be
 * {@code item.landed}, sealed to the split stage and signed by the landing
 * node, for the stage's intake, and point at the landing table it reads.
 * Where the stage reprocesses, it takes {@code item.reprocess} too, signed
 * by the stage itself, and splits the item's next generation (STAGES.md
 * §15.8.4); an {@code item.landed} it signed, or an {@code item.reprocess}
 * another did, is refused ({@code wrong_signer}).
 * One that fails a check is refused, audited and acknowledged: delivering
 * it again won't change it. One of a contract major version this stage
 * can't read is held instead, and tried again, so that no item is
 * stranded by a landing node upgraded first.
 * <p>
 * A batch's items are split at once, each on a thread of its own; the
 * batch is acknowledged once each is split. If one can't be split now
 * (storage or the broker down, another pod on the item, its lease lost),
 * the batch is held and tried again; items already split in it are
 * skipped, known by their broker IDs. So a batch as large as
 * {@code sdh.split.concurrency} suits the stage best: a larger one holds
 * finished items' messages behind a slow one. A pod may handle several
 * batches at once, one per Kafka consumer ({@code sdh.split.consumers}),
 * so a slow item holds back only its own batch; the splitter's limit
 * covers them all. A tampered checkpoint, a commit whose outcome isn't
 * known, or an {@link Error} stops the process.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SplitHandler implements BatchHandler
{

    /** Logs retries, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(SplitHandler.class);

    /** Splits the items. */
    private final ItemSplitter splitter;

    /** Decrypts and verifies messages. */
    private final MessageOpener opener;

    /** The intake the stage serves. */
    private final String intake;

    /** The landing catalog's name. */
    private final String catalogName;

    /** The landing namespace. */
    private final String namespace;

    /** Stops the process on a fatal error. */
    private final FatalExit fatal;

    /** The stage's reprocess; null if it doesn't reprocess. */
    private final SplitReprocess reprocess;

    /**
     * The messages of the batch being retried whose items are split, by
     * {@link #key}, for each consumer thread: each hands one batch at a
     * time, the same one again until it is done (as new objects, for a
     * Kafka batch left for the next poll), and a pod may have several
     * ({@code sdh.split.consumers}).
     */
    private final ThreadLocal<Map<Object, Boolean>> retried =
            ThreadLocal.withInitial(HashMap::new);

    /**
     * Creates a handler.
     *
     * @param splitter    splits the items
     * @param opener      decrypts and verifies messages
     * @param intake      the intake the stage serves
     * @param catalogName the landing catalog's name
     * @param namespace   the landing namespace
     * @param fatal       stops the process on a fatal error
     */
    public SplitHandler(ItemSplitter splitter, MessageOpener opener,
            String intake, String catalogName, String namespace,
            FatalExit fatal)
    {
        this(splitter, opener, intake, catalogName, namespace, fatal, null);
    }

    /**
     * Creates a handler that takes {@code item.reprocess} too (STAGES.md
     * §15.8.4).
     *
     * @param splitter    splits the items
     * @param opener      decrypts and verifies messages
     * @param intake      the intake the stage serves
     * @param catalogName the landing catalog's name
     * @param namespace   the landing namespace
     * @param fatal       stops the process on a fatal error
     * @param reprocess   the stage's reprocess; null if it doesn't
     *                    reprocess
     */
    public SplitHandler(ItemSplitter splitter, MessageOpener opener,
            String intake, String catalogName, String namespace,
            FatalExit fatal, SplitReprocess reprocess)
    {
        this.reprocess = reprocess;
        this.splitter = splitter;
        this.opener = opener;
        this.intake = intake;
        this.catalogName = catalogName;
        this.namespace = namespace;
        this.fatal = fatal;
    }

    /**
     * Splits each message's item, all at once: the splitter limits how
     * many run together, and each item's lease keeps two messages for the
     * same item apart.
     *
     * @param messages the messages
     * @return {@code DONE} once every item is split or its message
     *         refused; {@code FATAL} if one stops the process; otherwise
     *         {@code RETRY} if one must be tried again
     * @throws InterruptedException if interrupted while waiting; the
     *                              items still being split are
     *                              interrupted too
     */
    @Override
    public Outcome handle(List<BrokerMessage> messages)
            throws InterruptedException
    {
        if (messages.size() == 1)
        {
            return handle(messages.get(0).body());
        }
        // A batch retried comes again as the same messages: those whose
        // items were split last time are skipped, not split (announced)
        // again while another waits for its lease.
        Map<Object, Boolean> done = retried.get();
        done.keySet().retainAll(keys(messages));
        List<BrokerMessage> left = new ArrayList<>(messages.size());
        for (BrokerMessage message : messages)
        {
            if (!done.containsKey(key(message)))
            {
                left.add(message);
            }
        }
        ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();
        try
        {
            List<Future<Outcome>> splits = new ArrayList<>(left.size());
            for (BrokerMessage message : left)
            {
                splits.add(threads.submit(() -> handle(message.body())));
            }
            Outcome outcome = Outcome.DONE;
            for (int index = 0; index < splits.size(); index++)
            {
                Outcome one = outcome(splits.get(index));
                if (one == Outcome.DONE)
                {
                    done.put(key(left.get(index)), Boolean.TRUE);
                }
                outcome = worse(outcome, one);
            }
            if (outcome == Outcome.DONE)
            {
                done.clear();
            }
            return outcome;
        }
        finally
        {
            // Interrupts any still running if this thread was interrupted;
            // otherwise each has ended.
            threads.shutdownNow();
        }
    }

    /**
     * What a message is known by across a batch's retries: the broker's ID
     * (for Kafka, its topic, partition and offset), or else the message
     * itself, by identity (a record's equality compares its body by
     * reference).
     *
     * @param message the message
     * @return its key
     */
    private static Object key(BrokerMessage message)
    {
        return message.messageId() != null
                ? "id:" + message.messageId() : message;
    }

    /**
     * A batch's messages' keys.
     *
     * @param messages the messages
     * @return their keys
     */
    private static Set<Object> keys(List<BrokerMessage> messages)
    {
        Set<Object> set = new HashSet<>();
        for (BrokerMessage message : messages)
        {
            set.add(key(message));
        }
        return set;
    }

    /**
     * What became of one message's item.
     *
     * @param split its split
     * @return its outcome
     * @throws InterruptedException if interrupted while waiting
     * @throws RuntimeException     what its split threw, if unchecked
     */
    private static Outcome outcome(Future<Outcome> split)
            throws InterruptedException
    {
        try
        {
            return split.get();
        }
        catch (ExecutionException failed)
        {
            // Thrown on as it would be from this thread. An interruption
            // of the item's own thread is tried again.
            switch (failed.getCause())
            {
                case RuntimeException unchecked -> throw unchecked;
                case Error error -> throw error;
                default ->
                {
                    return Outcome.RETRY;
                }
            }
        }
    }

    /**
     * The worse of two outcomes: {@code FATAL}, then {@code RETRY}, then
     * {@code DONE}.
     *
     * @param one   one outcome
     * @param other the other
     * @return the worse
     */
    private static Outcome worse(Outcome one, Outcome other)
    {
        return one == Outcome.FATAL || other == Outcome.FATAL
                ? Outcome.FATAL
                : one == Outcome.RETRY || other == Outcome.RETRY
                        ? Outcome.RETRY
                        : Outcome.DONE;
    }

    /**
     * Opens, checks and splits one message's item.
     *
     * @param body the message as received
     * @return what became of it
     * @throws InterruptedException if interrupted while waiting
     */
    Outcome handle(byte[] body) throws InterruptedException
    {
        MessageOpener.Opened opened;
        try
        {
            opened = opener.openSigned(body);
        }
        catch (MessageRejectedException rejected)
        {
            if (rejected.reason() == Reason.UNSUPPORTED_VERSION)
            {
                // A later version: dropping it would strand the item; it
                // is held until this stage is upgraded. An earlier one,
                // which no upgrade reads, is rejected below, and its item
                // found by the reconciler.
                Telemetry.get().auditError("split.unreadable_version",
                        Fields.of(Attr.ERROR_CODE, ErrorCode.MESSAGE_REJECTED)
                                .with(Attr.INTAKE, intake)
                                .with(Attr.REASON, rejected.reason().name()));
                return Outcome.RETRY;
            }
            return reject(rejected.reason().name());
        }
        ConnectorMessage message = opened.message();
        boolean self = reprocess != null
                && reprocess.selfKid().equals(opened.signerKid());
        if (message instanceof ItemReprocess again)
        {
            // Only the stage itself asks for a reprocess, and an item lands
            // only from another (STAGES.md §15.8.4).
            if (!self)
            {
                return wrongSigner(message);
            }
            if (!intake.equals(again.landed().intake()))
            {
                return reject("OTHER_INTAKE");
            }
            if (!checked(again.landed()))
            {
                return reject("OTHER_STORAGE");
            }
            return split(() -> splitter.reprocess(again, reprocess));
        }
        if (!(message instanceof ItemLanded landed))
        {
            return reject("NOT_ITEM_LANDED");
        }
        if (self)
        {
            return wrongSigner(message);
        }
        if (!intake.equals(landed.intake()))
        {
            return reject("OTHER_INTAKE");
        }
        if (!checked(landed))
        {
            return reject("OTHER_STORAGE");
        }
        return split(() -> splitter.split(landed.itemId(),
                landed.itemDigest(), landed.sameAs(), landed.storage()));
    }

    /**
     * Whether an item's message points at the landing table this stage
     * reads. Its intake is checked by the caller.
     *
     * @param landed the item's message
     * @return whether its storage is the stage's landing table
     */
    private boolean checked(ItemLanded landed)
    {
        StorageRef storage = landed.storage();
        return storage != null && catalogName.equals(storage.catalog())
                && namespace.equals(storage.namespace())
                && LandingTables.LANDED_ITEMS.equals(storage.table());
    }

    /** Splitting an item, as a message asks. */
    @FunctionalInterface
    private interface Split
    {

        /**
         * Does it.
         *
         * @throws Exception if it fails
         */
        void run() throws Exception;
    }

    /**
     * Splits an item, and says what becomes of its message.
     *
     * @param work the splitting
     * @return {@code DONE}, {@code RETRY} or {@code FATAL}
     * @throws InterruptedException if interrupted while waiting
     */
    private Outcome split(Split work) throws InterruptedException
    {
        try
        {
            work.run();
            return Outcome.DONE;
        }
        catch (InterruptedException interrupted)
        {
            throw interrupted;
        }
        catch (LeaseHeldException held)
        {
            LOG.info("split.item_busy");
            return Outcome.RETRY;
        }
        catch (Exception | Error failure)
        {
            Optional<String> reason = fatalReason(failure);
            if (reason.isPresent())
            {
                LOG.error("split.fatal reason={}", reason.get());
                fatal.exit(reason.get());
                return Outcome.FATAL;
            }
            LOG.warn("split.retry error={}",
                    failure.getClass().getSimpleName());
            return Outcome.RETRY;
        }
    }

    /**
     * Tells whether an error must stop the process, and why
     * ({@link StageFailures#fatalReason}). A lost item lease is not one:
     * only that item stops.
     *
     * @param failure the error
     * @return the reason, for the exit; empty if the error can be retried
     */
    public static Optional<String> fatalReason(Throwable failure)
    {
        return StageFailures.fatalReason(failure);
    }

    /**
     * Audits a message refused for its signer, as the other stages do.
     *
     * @param message the message
     * @return {@code DONE}: it is acknowledged
     */
    private Outcome wrongSigner(ConnectorMessage message)
    {
        Telemetry.get().auditError("stage.message_rejected",
                Fields.of(Attr.STAGE, ItemProcessor.STAGE)
                        .with(Attr.ERROR_CODE, ErrorCode.MESSAGE_REJECTED)
                        .with(Attr.INTAKE, intake)
                        .with(Attr.REASON, "wrong_signer")
                        .with(Attr.MESSAGE_TYPE, message.type().wireName()));
        return Outcome.DONE;
    }

    /**
     * Audits a refused message.
     *
     * @param reason why it was refused, without PHI
     * @return {@code DONE}: it is acknowledged
     */
    private Outcome reject(String reason)
    {
        Telemetry.get().auditError("split.rejected",
                Fields.of(Attr.ERROR_CODE, ErrorCode.MESSAGE_REJECTED)
                        .with(Attr.INTAKE, intake)
                        .with(Attr.REASON, reason));
        return Outcome.DONE;
    }
}
