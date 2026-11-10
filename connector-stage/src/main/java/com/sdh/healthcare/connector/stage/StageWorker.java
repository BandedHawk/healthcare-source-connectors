/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.broker.receive.BatchHandler;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stage's worker (STAGES.md §5–§7): handles each batch a receiver from
 * {@code connector-broker} holds, and around the stage's
 * {@link StageProcessor} does everything a stage has in common.
 * <p>
 * For each batch:
 * <ol>
 * <li>each message is opened and verified, and read by the processor; a
 *     message that can't be trusted is audited and dropped, and one of a
 *     contract major version this build can't read holds the batch back
 *     until the stage is upgraded;</li>
 * <li>each record is claimed in the registry, then processed, several at a
 *     time;</li>
 * <li>the records this worker claimed are committed by the processor in one
 *     append, then marked stored with the message each is sent as, and
 *     counted on their items; a record that failed is marked
 *     {@code FAILED} and recorded in the stage's fail store in the same
 *     transaction, and has no message: failures aren't sent on (STAGES.md
 *     §15.1);</li>
 * <li>the messages are sent, and marked sent;</li>
 * <li>a record already stored is a duplicate, whatever version it was
 *     stored under, and is only sent again if its message never was; it is
 *     never processed again (STAGES.md §14.3), and a version that differs
 *     is audited. The first output is kept;</li>
 * <li>a record another worker holds is waited for, after the rest is
 *     stored and sent, until that worker stores it or lets its claim
 *     expire;</li>
 * <li>an item whose records are all stored gets its item-complete message
 *     if none of its failures is open and the stage before has completed
 *     it; otherwise its {@code item.progress}, again whenever its counts
 *     change (§15.6).</li>
 * </ol>
 * Only then is the batch acknowledged. Anything that fails on the way is
 * tried again in place, and what was already stored is recognised.
 *
 * @param <O> what the processor stores for a record
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StageWorker<O> implements BatchHandler, AutoCloseable
{

    /** The most items looked at again in one go. */
    static final int REVISIT_LIMIT = 100;

    /** The most corrections a worker processes in an interval. */
    static final int CORRECTION_LIMIT = 10;

    /** The most reprocess objects a worker queues in an interval. */
    static final int REPROCESS_LIMIT = 100;

    /** Logs the worker's own events; never a body or a record id. */
    private static final Logger LOG = LoggerFactory.getLogger(
            StageWorker.class);

    /** The stage's own work. */
    private final StageProcessor<O> processor;

    /** The claims and item rows. */
    private final StageRegistry registry;

    /** Opens received messages. */
    private final MessageOpener opener;

    /** Seals the messages the stage sends. */
    private final MessageSealer sealer;

    /** Sends the stage's messages. */
    private final MessagePublisher output;

    /** How the worker runs. */
    private final Settings settings;

    /** Queues and takes reprocess runs' objects; null if it doesn't. */
    private final Reprocessing reprocessing;

    /** Processes records, several at a time. */
    private final ExecutorService workers;

    /** Renews this worker's claims. */
    private final ScheduledExecutorService renewer;

    /** Looks again at items whose failures changed (STAGES.md §15.3). */
    private final ScheduledExecutorService revisiter;

    /**
     * Held while the processor commits: batches and corrections commit
     * from different threads, and the processor commits one at a time.
     */
    private final Object commits = new Object();

    /**
     * How a worker runs.
     *
     * @param concurrency   how many records are processed at once
     * @param claimTtl      how long a claim lasts unless renewed
     * @param renewInterval how often claims are renewed; well within the
     *                      claim's life
     * @param busyPoll      how long to wait before looking again at a
     *                      record another worker holds
     * @param sendTimeout   the longest to wait for the broker to take a
     *                      message
     */
    public record Settings(int concurrency, Duration claimTtl,
            Duration renewInterval, Duration busyPoll, Duration sendTimeout)
    {

        /**
         * Checks the settings.
         *
         * @param concurrency   how many records are processed at once
         * @param claimTtl      how long a claim lasts
         * @param renewInterval how often claims are renewed
         * @param busyPoll      how long to wait on another's claim
         * @param sendTimeout   the longest to wait for a send
         * @throws IllegalArgumentException if a value is out of range
         */
        public Settings
        {
            if (concurrency < 1)
            {
                throw new IllegalArgumentException(
                        "concurrency must be at least 1");
            }
            if (renewInterval.isNegative() || renewInterval.isZero()
                    || renewInterval.multipliedBy(2).compareTo(claimTtl) > 0)
            {
                throw new IllegalArgumentException(
                        "the claim renewal interval must be positive and at"
                                + " most half the claim's life");
            }
        }
    }

    /**
     * How a worker reprocesses (STAGES.md §15.8.4): the stage's runs, its
     * input, and how it sends to itself.
     *
     * @param store     the stage's runs
     * @param inputs    the stage's input, as the stage before stored it
     * @param sealer    seals to the stage's own inbox, signed by itself
     * @param publisher sends to the stage's own input
     * @param selfKid   the ID of the stage's own signing key: the only
     *                  signer of a {@code record.reprocess} taken, and one
     *                  no other type is taken from
     */
    public record Reprocessing(ReprocessStore store, ReprocessInputs inputs,
            MessageSealer sealer, MessagePublisher publisher, String selfKid)
    {
    }

    /**
     * A record of the batch, with its message.
     *
     * @param input    what the processor read in it
     * @param message  the message
     * @param brokerId the broker's ID of the message it came in; null if
     *                 none, or if it was rebuilt rather than received
     * @param runId    the reprocess run it was sent for; null if none
     */
    private record Pending(StageInput.RecordInput input,
            ConnectorMessage message, String brokerId, UUID runId)
    {
    }

    /**
     * What came of one record in one round.
     *
     * @param pending  the record
     * @param found    what was found
     * @param output   its output: when claimed, or when processed again
     *                 to compare with what another worker stored
     * @param stored   the registry's row, when stored before
     * @param version  the processor's version for it, when claimed or
     *                 stored before by another worker
     * @param <O>      the processor's output
     */
    private record Result<O>(Pending pending, Found found,
            StageOutput<O> output, StageRegistry.Stored stored,
            String version)
    {
    }

    /** What a round found for a record. */
    private enum Found
    {

        /** Claimed by this worker, and processed. */
        OWNED,

        /** Stored before by this worker. */
        MINE,

        /** Stored before by another, with the same output. */
        DUPLICATE,

        /** Held by another worker. */
        BUSY
    }

    /**
     * Creates a worker. {@link #start} starts renewing its claims.
     *
     * @param processor   the stage's own work
     * @param registry    the claims and item rows
     * @param opener      opens received messages
     * @param sealer      seals the messages the stage sends
     * @param output      sends the stage's messages
     * @param settings    how the worker runs
     */
    public StageWorker(StageProcessor<O> processor, StageRegistry registry,
            MessageOpener opener, MessageSealer sealer,
            MessagePublisher output, Settings settings)
    {
        this(processor, registry, opener, sealer, output, settings, null);
    }

    /**
     * Creates a worker that reprocesses. {@link #start} starts renewing
     * its claims and queueing its runs.
     *
     * @param processor    the stage's own work
     * @param registry     the claims and item rows
     * @param opener       opens received messages; it must trust the
     *                     stage's own signing key if it reprocesses
     * @param sealer       seals the messages the stage sends
     * @param output       sends the stage's messages
     * @param settings     how the worker runs
     * @param reprocessing how it reprocesses; null if it doesn't
     */
    public StageWorker(StageProcessor<O> processor, StageRegistry registry,
            MessageOpener opener, MessageSealer sealer,
            MessagePublisher output, Settings settings,
            Reprocessing reprocessing)
    {
        this.reprocessing = reprocessing;
        this.processor = processor;
        this.registry = registry;
        this.opener = opener;
        this.sealer = sealer;
        this.output = output;
        this.settings = settings;
        this.workers = Executors.newFixedThreadPool(settings.concurrency(),
                Thread.ofPlatform().name("sdh-" + processor.stage() + "-", 0)
                        .daemon().factory());
        this.renewer = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("sdh-" + processor.stage()
                        + "-claims").daemon().factory());
        this.revisiter = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("sdh-" + processor.stage()
                        + "-revisit").daemon().factory());
    }

    /** Starts renewing this worker's claims. */
    public void start()
    {
        long period = settings.renewInterval().toMillis();
        renewer.scheduleWithFixedDelay(this::renew, period, period,
                TimeUnit.MILLISECONDS);
        revisiter.scheduleWithFixedDelay(() ->
        {
            applyCorrections();
            reprocess();
            revisit();
        }, period, period, TimeUnit.MILLISECONDS);
    }

    /**
     * Queues reprocess runs' objects (STAGES.md §15.8.4), if the stage
     * reprocesses: sends again those given their lines but never sent (a
     * pod stopped between the two), under the same lines and message IDs;
     * takes a batch not yet queued, which no other pod takes, checks each
     * against the stage's input now, and gives each its line; sends each as
     * a {@code record.reprocess} to the stage's own input, sealed to itself;
     * and ends the runs with nothing left. An object whose input is gone
     * before it is sent is refused. A failure is logged, and tried again at
     * the next interval. Run each renewal interval once {@link #start}ed;
     * public for a stage's own tests.
     */
    public void reprocess()
    {
        if (reprocessing == null)
        {
            return;
        }
        try
        {
            ReprocessStore store = reprocessing.store();
            Map<UUID, List<ReprocessInputs.Input>> current = new HashMap<>();
            List<ReprocessStore.Queued> queued = new ArrayList<>(
                    store.unsent(settings.claimTtl(), REPROCESS_LIMIT));
            queued.addAll(store.queue(REPROCESS_LIMIT,
                    chosen -> ReprocessInputs.refusal(chosen,
                            inputs(current, chosen.itemId()))));
            List<OutboundMessage> sends = new ArrayList<>();
            List<ReprocessStore.Queued> sent = new ArrayList<>();
            for (ReprocessStore.Queued each : queued)
            {
                inputs(current, each.chosen().itemId());
                ConnectorMessage.RecordInStorage record =
                        ReprocessInputs.rebuild(each, current);
                if (record == null)
                {
                    store.refuse(each, ReprocessStore.PAST_RETENTION);
                    continue;
                }
                ConnectorMessage message = new ConnectorMessage
                        .RecordReprocess(record, each.runId());
                sends.add(OutboundMessage.of(message,
                        reprocessing.sealer().seal(message)));
                sent.add(each);
            }
            send(reprocessing.publisher(), sends);
            store.markSent(sent);
            store.finishRuns();
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
        catch (Exception failure)
        {
            LOG.warn("stage.reprocess_failed stage={} reason={}",
                    processor.stage(), failure.getClass().getSimpleName());
        }
    }

    /**
     * An item's current input, read once an interval.
     *
     * @param current what is read so far, by item
     * @param itemId  the item
     * @return its current objects
     * @throws Exception if the input can't be read
     */
    private List<ReprocessInputs.Input> inputs(
            Map<UUID, List<ReprocessInputs.Input>> current, UUID itemId)
            throws Exception
    {
        List<ReprocessInputs.Input> found = current.get(itemId);
        if (found == null)
        {
            found = reprocessing.inputs().current(itemId);
            current.put(itemId, found);
        }
        return found;
    }

    /**
     * Looks again at the items whose failures changed since their counts
     * were reported (STAGES.md §15.3): an operator declined or reopened
     * one. An item already complete whose failure was reopened is
     * incomplete again (§15.6e). Each completes, or has its new counts
     * reported, as at the end of a batch. Every pod does it; the messages'
     * ids are fixed, so a repeat is dropped. A failure is logged and tried
     * again at the next interval.
     * Run each renewal interval once {@link #start}ed; public for a stage's
     * own tests.
     */
    public void revisit()
    {
        try
        {
            registry.reopenChanged(REVISIT_LIMIT);
            List<UUID> changed = registry.changedItems(REVISIT_LIMIT);
            if (!changed.isEmpty())
            {
                completeItems(new LinkedHashSet<>(changed));
            }
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
        catch (Exception failure)
        {
            LOG.warn("stage.revisit_failed stage={} reason={}",
                    processor.stage(), failure.getClass().getSimpleName());
        }
    }

    /**
     * Processes operators' corrections (STAGES.md §15.6c), oldest first,
     * if the stage does: each claimed in the registry under a name of its
     * own, so one worker processes it; its output committed and stored as
     * a record's is, as the next revision of its place (§15.8.5), with its
     * outcome noted in the same transaction (the correction passed or
     * failed, and its failure superseded either way), and the record sent
     * on if it passed. A
     * correction stored but not sent (a worker stopped between the two) is
     * sent again. The item is then looked at again by {@link #revisit}. A
     * failure is logged, and the correction tried again at the next
     * interval. Run each renewal interval once {@link #start}ed; public
     * for a stage's own tests.
     */
    public void applyCorrections()
    {
        if (!processor.corrects())
        {
            return;
        }
        try
        {
            List<StageRegistry.Unsent> unsent =
                    registry.unsentCorrections(settings.claimTtl());
            if (!unsent.isEmpty())
            {
                send(output, unsent.stream()
                        .map(StageRegistry.Unsent::message).toList());
                registry.markSent(unsent.stream()
                        .map(StageRegistry.Unsent::key).toList());
            }
            for (CorrectionStore.Pending pending : registry
                    .correctionStore().pending(CORRECTION_LIMIT))
            {
                if (registry.claim(pending.claimId(), pending.itemId(),
                        settings.claimTtl()) instanceof StageRegistry.Owned)
                {
                    applyCorrection(pending);
                }
            }
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
        catch (Exception failure)
        {
            LOG.warn("stage.correction_failed stage={} reason={}",
                    processor.stage(), failure.getClass().getSimpleName());
        }
    }

    /**
     * Processes one correction this worker has claimed, stores it as the
     * next revision of its place (STAGES.md §15.8.5), and sends its record
     * on if it passed: flagged superseded if something newer is there
     * already. A revision that failed is a failure of its own.
     *
     * @param pending the correction
     * @throws Exception if it can't be processed, stored or sent; it is
     *                   tried again
     */
    private void applyCorrection(CorrectionStore.Pending pending)
            throws Exception
    {
        StageProcessor.Corrected<O> made = processor.correct(pending);
        StageOutput<O> result = made.output();
        ConnectorMessage message;
        synchronized (commits)
        {
            message = processor.commit(List.of(result)).get(0);
        }
        // Claimed under its place's key; one submitted before revisions
        // were noted, under a name of its own.
        boolean revision = pending.recordId() != null
                && pending.lineage() != null;
        Lineage lineage = revision ? pending.lineage() : null;
        boolean passed = !result.failed() && message != null;
        String recordId = passed
                && message instanceof ConnectorMessage.RecordInStorage record
                        ? record.recordId() : null;
        String code = passed ? null : result.errorCode() == null
                ? ErrorCode.STAGE_FAILED.name() : result.errorCode();
        if (passed && revision
                && message instanceof ConnectorMessage.RecordInStorage record)
        {
            message = record.withLineage(lineage, !registry.supersededAlready(
                    List.of(pending.itemId()), List.of(pending.seq()),
                    List.of(lineage)).isEmpty());
        }
        long offset = message instanceof ConnectorMessage.RecordInStorage
                record ? record.sourceOffset() : 0;
        FailureStore.Failure failure = passed ? null
                : new FailureStore.Failure(pending.itemId(), pending.seq(),
                        pending.recordId(), code, offset, made.version(),
                        lineage);
        StageRegistry.Entry entry = new StageRegistry.Entry(
                revision ? pending.recordId() : pending.claimId(),
                pending.itemId(), result.digest(), made.version(), !passed,
                passed ? outbound(message) : null, failure,
                new CorrectionStore.Outcome(pending, passed, recordId, code,
                        made.version()), pending.seq(), lineage);
        if (registry.store(List.of(entry)).contains(entry.key())
                && entry.message() != null)
        {
            send(output, List.of(entry.message()));
            registry.markSent(List.of(entry.key()));
        }
    }

    /**
     * Renews this worker's claims. A failure is logged and tried again at
     * the next interval; claims last several intervals.
     */
    private void renew()
    {
        try
        {
            registry.renew(settings.claimTtl());
        }
        catch (RuntimeException failure)
        {
            LOG.warn("stage.claims_not_renewed stage={}", processor.stage());
        }
    }

    @Override
    public Outcome handle(List<BrokerMessage> batch)
            throws InterruptedException
    {
        List<Opened> opened = new ArrayList<>(batch.size());
        for (BrokerMessage received : batch)
        {
            try
            {
                MessageOpener.Opened signed =
                        opener.openSigned(received.body());
                if (!signedRightly(signed))
                {
                    // A reprocess comes from the stage itself, and every
                    // other type from the stage before (STAGES.md §15.8.4).
                    Telemetry.get().auditError("stage.message_rejected",
                            stageFields(ErrorCode.MESSAGE_REJECTED)
                                    .with(Attr.REASON, "wrong_signer")
                                    .with(Attr.MESSAGE_TYPE, signed.message()
                                            .type().wireName()));
                    continue;
                }
                opened.add(new Opened(signed.message(), received.body(),
                        received.messageId()));
            }
            catch (MessageRejectedException rejected)
            {
                if (rejected.reason()
                        == MessageRejectedException.Reason.UNSUPPORTED_VERSION)
                {
                    // A later version: dropping it would strand its record;
                    // it waits on the queue until this stage is upgraded.
                    // An earlier one, which no upgrade reads, is rejected
                    // below; a record is found by the reconciler, an item
                    // total replayed (STAGES.md §13.2).
                    Telemetry.get().auditError("stage.unreadable_version",
                            stageFields(ErrorCode.MESSAGE_REJECTED));
                    return Outcome.RETRY;
                }
                Telemetry.get().auditError("stage.message_rejected",
                        stageFields(ErrorCode.MESSAGE_REJECTED)
                                .with(Attr.REASON, rejected.reason().name()));
            }
        }
        return run(opened);
    }

    /**
     * Whether a message was signed by the one it may come from: a
     * {@code record.reprocess} by the stage itself, if it reprocesses, and
     * any other type by another key the opener trusts, the stage before's.
     *
     * @param signed the message and its signer
     * @return true if so
     */
    private boolean signedRightly(MessageOpener.Opened signed)
    {
        boolean self = reprocessing != null
                && reprocessing.selfKid().equals(signed.signerKid());
        return self == signed.message()
                instanceof ConnectorMessage.RecordReprocess;
    }

    /**
     * Handles messages that are already open, as if received: for a
     * reconciler that rebuilds lost record messages from their stored rows
     * (STAGES.md §9). An item total handed in this way keeps no message for
     * the item-complete step.
     *
     * @param messages the messages
     * @return {@code DONE}, or {@code RETRY} if they should be handed in
     *         again later
     * @throws InterruptedException if interrupted
     */
    public Outcome handleOpened(List<ConnectorMessage> messages)
            throws InterruptedException
    {
        List<Opened> opened = new ArrayList<>(messages.size());
        for (ConnectorMessage message : messages)
        {
            opened.add(new Opened(message, null, null));
        }
        return run(opened);
    }

    /**
     * Completes items whose records are all stored, with their total, but
     * which weren't completed: for a reconciler, when the batch that would
     * have completed them was lost (STAGES.md §10). An item not yet due is
     * left; one completed meanwhile by another worker is completed again,
     * its message dropped as a repeat.
     *
     * @param itemIds the items
     * @return {@code DONE}, or {@code RETRY} if they should be tried again
     *         later
     * @throws InterruptedException if interrupted
     */
    public Outcome completeDue(Collection<UUID> itemIds)
            throws InterruptedException
    {
        try
        {
            completeItems(new LinkedHashSet<>(itemIds));
            return Outcome.DONE;
        }
        catch (RuntimeException | ExecutionException | TimeoutException failure)
        {
            Telemetry.get().auditWarn("stage.batch_failed",
                    stageFields(ErrorCode.STAGE_BATCH_FAILED)
                            .with(Attr.REASON,
                                    failure.getClass().getSimpleName()));
            return Outcome.RETRY;
        }
    }

    /**
     * A message, opened, with its body as received.
     *
     * @param message  the message
     * @param body     its sealed body; null if it wasn't received
     * @param brokerId the broker's ID for it; null if none, or if it
     *                 wasn't received
     */
    private record Opened(ConnectorMessage message, byte[] body,
            String brokerId)
    {
    }

    /**
     * Processes a batch of opened messages: claims, processes, commits and
     * sends its records, notes item totals, and completes the items it
     * touched.
     *
     * @param batch the messages
     * @return {@code DONE}, or {@code RETRY} if the batch failed
     * @throws InterruptedException if interrupted
     */
    private Outcome run(List<Opened> batch) throws InterruptedException
    {
        try
        {
            Map<StageRegistry.Key, Pending> records =
                    new LinkedHashMap<>();
            Set<UUID> items = new LinkedHashSet<>();
            for (Opened received : batch)
            {
                ConnectorMessage message = received.message();
                UUID runId = null;
                if (message instanceof ConnectorMessage.RecordReprocess again)
                {
                    // Its input at the line the run gave it, taken as any
                    // record of that line (STAGES.md §15.8.4).
                    runId = again.runId();
                    message = again.record();
                }
                switch (processor.classify(message))
                {
                    case StageInput.RecordInput input ->
                    {
                        // The same record twice in one batch is one record.
                        records.putIfAbsent(input.key(),
                                new Pending(input, message,
                                        received.brokerId(), runId));
                        items.add(input.itemId());
                    }
                    case StageInput.ItemTotal total ->
                    {
                        registry.expect(total.itemId(), total.expected(),
                                received.body(), total.partial(),
                                total.completion(), total.generation());
                        items.add(total.itemId());
                    }
                    case StageInput.Unwanted unwanted ->
                            Telemetry.get().audit("stage.message_unwanted",
                                    Fields.of(Attr.STAGE, processor.stage())
                                            .with(Attr.MESSAGE_TYPE,
                                                    message.type()
                                                            .wireName())
                                            .with(Attr.REASON,
                                                    unwanted.reason()));
                }
            }
            List<Pending> waiting = new ArrayList<>(records.values());
            while (!waiting.isEmpty())
            {
                waiting = round(waiting);
                if (!waiting.isEmpty())
                {
                    Thread.sleep(settings.busyPoll().toMillis());
                }
            }
            completeItems(items);
            return Outcome.DONE;
        }
        catch (RuntimeException | ExecutionException | TimeoutException failure)
        {
            Telemetry.get().auditWarn("stage.batch_failed",
                    stageFields(ErrorCode.STAGE_BATCH_FAILED)
                            .with(Attr.REASON,
                                    failure.getClass().getSimpleName()));
            return Outcome.RETRY;
        }
    }

    /**
     * One round over the records not yet done: claims and processes each,
     * commits and sends what this worker claimed, sends again what was
     * stored but never sent.
     *
     * @param pending the records
     * @return those another worker holds, to look at again
     * @throws InterruptedException if interrupted
     * @throws ExecutionException   if a record's processing failed
     * @throws TimeoutException     if the broker didn't take a message in
     *                              time
     */
    private List<Pending> round(List<Pending> pending)
            throws InterruptedException, ExecutionException,
            TimeoutException
    {
        List<Callable<Result<O>>> tasks = new ArrayList<>();
        for (Pending record : pending)
        {
            tasks.add(() -> claimAndProcess(record));
        }
        List<Result<O>> owned = new ArrayList<>();
        List<OutboundMessage> resend = new ArrayList<>();
        List<StageRegistry.Key> resent = new ArrayList<>();
        List<Pending> busy = new ArrayList<>();
        for (Future<Result<O>> future : workers.invokeAll(tasks))
        {
            Result<O> result = future.get();
            switch (result.found())
            {
                case OWNED -> owned.add(result);
                case MINE ->
                {
                    if (result.stored().pending() != null)
                    {
                        resend.add(result.stored().pending());
                        resent.add(result.pending().input().key());
                    }
                }
                case DUPLICATE ->
                {
                    Telemetry.get().audit("record.duplicate",
                            storedFields(result));
                    if (result.stored().pending() != null)
                    {
                        resend.add(result.stored().pending());
                        resent.add(result.pending().input().key());
                    }
                }
                case BUSY -> busy.add(result.pending());
            }
        }
        List<OutboundMessage> sends = new ArrayList<>(resend);
        List<StageRegistry.Key> sent = new ArrayList<>(resent);
        if (!owned.isEmpty())
        {
            List<StageRegistry.Entry> entries = commit(owned);
            Set<StageRegistry.Key> marked = registry.store(entries);
            for (StageRegistry.Entry entry : entries)
            {
                if (marked.contains(entry.key()))
                {
                    if (entry.message() != null)
                    {
                        sends.add(entry.message());
                        sent.add(entry.key());
                    }
                }
                else
                {
                    // Its claim expired while this worker was stalled; the
                    // worker that took it over stores and sends it.
                    Telemetry.get().auditWarn("record.claim_lost",
                            Fields.of(Attr.STAGE, processor.stage())
                                    .with(Attr.ITEM_ID, entry.itemId()));
                }
            }
        }
        send(output, sends);
        registry.markSent(sent);
        return busy;
    }

    /**
     * Claims a record and, unless another worker holds it or this one
     * stored it already, processes it.
     *
     * @param record the record
     * @return what was found
     * @throws InterruptedException if interrupted
     */
    private Result<O> claimAndProcess(Pending record)
            throws InterruptedException
    {
        StageRegistry.Claim claim = registry.claim(
                record.input().key().recordId(), record.input().itemId(),
                settings.claimTtl());
        return switch (claim)
        {
            case StageRegistry.Owned owned ->
            {
                String version = processor.version(record.message());
                StageOutput<O> output = record.runId() == null
                        ? processor.process(record.message(), owned.attempts())
                        : processor.process(record.message(), owned.attempts(),
                                new ConnectorMessage.Cause(
                                        ConnectorMessage.Cause.REPROCESS,
                                        record.runId(), null));
                yield new Result<>(record, Found.OWNED, output, null,
                        version);
            }
            case StageRegistry.Stored stored when stored.mine() ->
                    new Result<>(record, Found.MINE, null, stored, null);
            case StageRegistry.Stored stored -> compare(record, stored);
            case StageRegistry.Busy busy ->
                    new Result<>(record, Found.BUSY, null, null, null);
        };
    }

    /**
     * Treats a record received again, which another worker stored, as a
     * duplicate whatever its version (STAGES.md §14.3, decided 2026-11-08):
     * the first output stays, and nothing is processed again, so a later
     * version can't stop the worker on a record stored under an earlier one.
     * When the versions differ, that is audited,
     * {@code record.version_differs}, with what an operator needs to find
     * the record, and counted. Processing it again under the new version is
     * an operator's forced reprocess (§15.4).
     *
     * @param record the record
     * @param stored its row
     * @return a duplicate
     */
    private Result<O> compare(Pending record, StageRegistry.Stored stored)
    {
        String version = processor.version(record.message());
        if (!version.equals(stored.version()))
        {
            Telemetry telemetry = Telemetry.get();
            telemetry.versionDiffers(processor.stage());
            telemetry.auditWarn("record.version_differs",
                    storedFields(record, stored, version));
        }
        return new Result<>(record, Found.DUPLICATE, null, stored, version);
    }

    /**
     * Commits the records this worker claimed, and seals the message each
     * is to be sent as.
     *
     * @param owned the records, processed
     * @return what to mark stored
     * @throws InterruptedException if interrupted
     */
    private List<StageRegistry.Entry> commit(List<Result<O>> owned)
            throws InterruptedException
    {
        List<StageOutput<O>> outputs = new ArrayList<>();
        for (Result<O> result : owned)
        {
            outputs.add(result.output());
        }
        List<ConnectorMessage> messages;
        synchronized (commits)
        {
            messages = processor.commit(outputs);
        }
        if (messages.size() != outputs.size())
        {
            throw new IllegalStateException(
                    "The processor made a message for each of "
                            + messages.size() + " of " + outputs.size()
                            + " outputs");
        }
        Set<Integer> late = superseded(owned);
        List<StageRegistry.Entry> entries = new ArrayList<>();
        for (int index = 0; index < owned.size(); index++)
        {
            Result<O> result = owned.get(index);
            StageOutput<O> made = result.output();
            StageInput.RecordInput input = result.pending().input();
            ConnectorMessage message = messages.get(index);
            if (message instanceof ConnectorMessage.RecordInStorage record)
            {
                // Sent on with the lineage it was made under, and marked if
                // something newer was here first (STAGES.md §15.8.3).
                message = record.withLineage(input.lineage(),
                        late.contains(index));
            }
            StageRegistry.Reprocessed again = null;
            UUID runId = result.pending().runId();
            if (runId != null)
            {
                // Says what it is a revision of, and whether its output is
                // the same (STAGES.md §15.8.7).
                StageRegistry.Current older = registry.current(
                        input.itemId(), input.seq(), input.lineage())
                        .orElse(null);
                if (message instanceof ConnectorMessage.RecordStored stored)
                {
                    message = stored.withCause(new ConnectorMessage.Cause(
                            ConnectorMessage.Cause.REPROCESS, runId,
                            older == null ? null : older.messageKey()));
                }
                again = new StageRegistry.Reprocessed(runId, older == null
                        ? null : made.digest().equals(older.digest()));
            }
            entries.add(new StageRegistry.Entry(input.recordId(),
                    input.itemId(), made.digest(), result.version(),
                    made.failed(),
                    made.failed() || message == null ? null
                            : outbound(message),
                    made.failed() ? failure(result) : null, null,
                    input.seq(), input.lineage(), again));
        }
        return entries;
    }

    /**
     * Which of the records this worker processed something newer at the
     * stage already supersedes (STAGES.md §15.8.3), so their messages are
     * sealed marked as such. The registry decides again, as it stores
     * them, whether each is counted; a record found newer only then is
     * still stored late, its message unmarked, and a reader tells by its
     * lineage.
     *
     * @param owned the records, processed
     * @return the indexes of those superseded
     */
    private Set<Integer> superseded(List<Result<O>> owned)
    {
        List<UUID> itemIds = new ArrayList<>();
        List<Long> seqs = new ArrayList<>();
        List<Lineage> lineages = new ArrayList<>();
        for (Result<O> result : owned)
        {
            StageInput.RecordInput input = result.pending().input();
            itemIds.add(input.itemId());
            seqs.add(input.seq());
            lineages.add(input.lineage());
        }
        return registry.supersededAlready(itemIds, seqs, lineages);
    }

    /**
     * The failure to record for a record that failed: named by its item
     * and sequence number, with where it starts in the item, why, and the
     * version that failed it. It holds no content.
     *
     * @param result the record and its output
     * @return the failure
     */
    private FailureStore.Failure failure(Result<O> result)
    {
        StageInput.RecordInput input = result.pending().input();
        long offset = result.pending().message()
                instanceof ConnectorMessage.RecordInStorage record
                        ? record.sourceOffset() : 0;
        String code = result.output().errorCode();
        return new FailureStore.Failure(input.itemId(), input.seq(),
                input.recordId(),
                code == null ? ErrorCode.STAGE_FAILED.name() : code, offset,
                result.version(), input.lineage());
    }

    /**
     * Sends the item-complete message of each item whose records are all
     * stored, and hasn't had it, then, if it is the item's first report
     * here ({@link #firstReport}), runs the processor's
     * {@link StageProcessor#itemCompleted} step. Two workers may both do
     * it; the message's id is fixed, so the repeat is dropped.
     *
     * @param items the items the batch touched
     * @throws InterruptedException if interrupted
     * @throws ExecutionException   if the broker refused a message, or the
     *                              processor's step failed
     * @throws TimeoutException     if the broker didn't take one in time
     */
    private void completeItems(Set<UUID> items)
            throws InterruptedException, ExecutionException,
            TimeoutException
    {
        for (StageRegistry.DueItem due : registry.due(items))
        {
            ItemProgress progress = due.progress().withTotal(total(due));
            boolean first = firstReport(due);
            if (interrupted(progress))
            {
                // An interrupted generation's total is an open total: its
                // records are counted, but no end or result is sent for
                // it; the newer generation follows (STAGES.md §15.8.6).
                if (due.progressChanged())
                {
                    registry.markProgressed(progress.itemId(),
                            progress.progressKey(), due.readAt(), false);
                    Telemetry.get().audit("item.progress",
                            Fields.of(Attr.STAGE, processor.stage())
                                    .with(Attr.ITEM_ID, progress.itemId())
                                    .with(Attr.GENERATION,
                                            progress.generation())
                                    .with(Attr.COUNT, progress.passed())
                                    .with(Attr.RESULT, "INTERRUPTED"));
                }
                continue;
            }
            if (!progress.complete())
            {
                if (due.progressChanged())
                {
                    progressItem(progress, due.readAt(), first);
                }
                else
                {
                    // The same counts: only note they were looked at.
                    registry.markProgressed(progress.itemId(),
                            progress.progressKey(), due.readAt());
                }
                continue;
            }
            send(output, List.of(outbound(processor.itemComplete(progress))));
            if (first)
            {
                runStep(() -> processor.itemCompleted(progress));
            }
            registry.markCompleted(progress.itemId());
            // The same event as the split stage's, so one query over the
            // stages finds an item lost between two (STAGES.md §13.2).
            Telemetry.get().audit("stage.item_completed",
                    Fields.of(Attr.STAGE, processor.stage())
                            .with(Attr.ITEM_ID, progress.itemId())
                            .with(Attr.RESULT, "COMPLETED")
                            .with(Attr.COUNT, progress.done())
                            .with(Attr.FAILED_COUNT, progress.unresolved()));
        }
    }

    /**
     * Whether an item's report is its first here: nothing reported of it
     * before, in any generation. Only then does the processor's step after
     * it run, which sends the producer's result: any later report follows
     * an operator's action (a correction, retry, decline, reopen or
     * reprocess), and none of those sends the producer anything (STAGES.md
     * §15.8.7). An item whose first generation was interrupted before it
     * was reported has its first report in a later one.
     *
     * @param due the item
     * @return true if it is the first
     */
    private static boolean firstReport(StageRegistry.DueItem due)
    {
        ItemProgress progress = due.progress();
        return !due.reported() && due.progressKey() == null
                && progress.completion() == 0;
    }

    /**
     * Whether an item's total is of a generation the stage before
     * interrupted (STAGES.md §15.8.6).
     *
     * @param progress the item, with its total
     * @return whether it is
     */
    private static boolean interrupted(ItemProgress progress)
    {
        return progress.total() instanceof ConnectorMessage.ItemSplit split
                && split.interrupted();
    }

    /**
     * Sends an item's {@code item.progress}: it has handled each of its
     * records once, and a failure here is open, or the stage before has
     * failures open (STAGES.md §15.6). Then, for its first report, runs
     * the processor's {@link StageProcessor#itemProgressed} step, and
     * notes the counts, so it isn't sent again until they change. Its
     * message id is made from the counts, so a repeat is dropped.
     *
     * @param progress the item and its counts
     * @param readAt   when its counts were read
     * @param first    whether it is the item's first report here
     * @throws InterruptedException if interrupted
     * @throws ExecutionException   if the broker refused it, or the
     *                              processor's step failed
     * @throws TimeoutException     if the broker didn't take it in time
     */
    private void progressItem(ItemProgress progress, Instant readAt,
            boolean first)
            throws InterruptedException, ExecutionException,
            TimeoutException
    {
        send(output, List.of(outbound(processor.itemProgress(progress))));
        if (first)
        {
            runStep(() -> processor.itemProgressed(progress));
        }
        registry.markProgressed(progress.itemId(), progress.progressKey(),
                readAt);
        Telemetry.get().audit("item.progress",
                Fields.of(Attr.STAGE, processor.stage())
                        .with(Attr.ITEM_ID, progress.itemId())
                        .with(Attr.COUNT, progress.passed())
                        .with(Attr.OPEN_COUNT, progress.open())
                        .with(Attr.DECLINED_COUNT, progress.declined())
                        .with(Attr.RESULT, progress.upstreamOpen()
                                ? "UPSTREAM_OPEN" : "INCOMPLETE"));
    }

    /** A processor's step after an item's message is sent. */
    @FunctionalInterface
    private interface Step
    {

        /**
         * Runs the step.
         *
         * @throws Exception if it fails
         */
        void run() throws Exception;
    }

    /**
     * Runs a processor's step, as a failure of the batch if it fails.
     *
     * @param step the step
     * @throws InterruptedException if interrupted
     * @throws ExecutionException   if the step failed
     */
    private static void runStep(Step step)
            throws InterruptedException, ExecutionException
    {
        try
        {
            step.run();
        }
        catch (InterruptedException | RuntimeException failure)
        {
            throw failure;
        }
        catch (Exception failure)
        {
            throw new ExecutionException(failure);
        }
    }

    /**
     * Opens the kept message that gave an item's total.
     *
     * @param due the item
     * @return the message; null if none was kept, or it no longer opens
     *         (its key retired), which is audited
     */
    private ConnectorMessage total(StageRegistry.DueItem due)
    {
        if (due.total() == null)
        {
            return null;
        }
        try
        {
            return opener.open(due.total());
        }
        catch (MessageRejectedException rejected)
        {
            Telemetry.get().auditWarn("stage.item_total_unreadable",
                    stageFields(ErrorCode.MESSAGE_REJECTED)
                            .with(Attr.ITEM_ID, due.progress().itemId())
                            .with(Attr.REASON, rejected.reason().name()));
            return null;
        }
    }

    /**
     * Seals a message for sending.
     *
     * @param message the message
     * @return the message to send
     */
    private OutboundMessage outbound(ConnectorMessage message)
    {
        return OutboundMessage.of(message, sealer.seal(message));
    }

    /**
     * Sends messages, and waits until the broker has taken them all.
     *
     * @param publisher where to
     * @param messages  the messages
     * @throws InterruptedException if interrupted
     * @throws ExecutionException   if the broker refused one
     * @throws TimeoutException     if it didn't take them in time
     */
    private void send(MessagePublisher publisher,
            List<OutboundMessage> messages)
            throws InterruptedException, ExecutionException,
            TimeoutException
    {
        if (messages.isEmpty())
        {
            return;
        }
        List<CompletableFuture<Void>> sends = new ArrayList<>();
        for (OutboundMessage message : messages)
        {
            sends.add(publisher.publish(message));
        }
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                .get(settings.sendTimeout().toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    /**
     * The fields of an event about the stage.
     *
     * @param code the error code
     * @return the fields
     */
    private Fields stageFields(ErrorCode code)
    {
        return Fields.of(Attr.STAGE, processor.stage())
                .with(Attr.ERROR_CODE, code);
    }

    /**
     * The fields of an event about a record received again: enough to find
     * it and recover by hand (STAGES.md §6). A record is named by its item
     * and sequence number, never its id.
     *
     * @param result the record, its row and the version now
     * @return the fields
     */
    private Fields storedFields(Result<O> result)
    {
        return storedFields(result.pending(), result.stored(),
                result.version());
    }

    /**
     * The fields of an event about a record received again.
     *
     * @param record  the record
     * @param stored  its row
     * @param version the processor's version for it now
     * @return its stage, item, sequence number, broker ID, both versions,
     *         attempts and the row's state
     */
    private Fields storedFields(Pending record, StageRegistry.Stored stored,
            String version)
    {
        long seq = record.input().seq();
        return Fields.of(Attr.STAGE, processor.stage())
                .with(Attr.ITEM_ID, record.input().itemId())
                .with(Attr.SEQ, seq < 0 ? null : seq)
                .with(Attr.BROKER_MESSAGE_ID, record.brokerId())
                .with(Attr.VERSION, version)
                .with(Attr.STORED_VERSION, stored.version() == null
                        ? "none" : stored.version())
                .with(Attr.ATTEMPTS, stored.attempts())
                .with(Attr.STATE, stored.state());
    }

    /**
     * Stops renewing and processing, then releases the claims this worker
     * holds on records it didn't store, so that another worker takes them
     * over at once rather than after the claim's time to live. Released only
     * once its threads have stopped, so none is storing meanwhile; if they
     * haven't stopped within the claim's time to live, or the database can't
     * be reached, the claims are left to expire, as after a crash.
     */
    @Override
    public void close()
    {
        long start = System.nanoTime();
        renewer.shutdownNow();
        revisiter.shutdownNow();
        workers.shutdownNow();
        try
        {
            if (!workers.awaitTermination(settings.claimTtl().toMillis(),
                    TimeUnit.MILLISECONDS))
            {
                notReleased("busy", start);
                return;
            }
            int released = registry.release();
            LOG.info("stage.claims_released stage={} count={} elapsed_ms={}",
                    processor.stage(), released, sinceMillis(start));
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
            notReleased("interrupted", start);
        }
        catch (RuntimeException failure)
        {
            notReleased("unavailable", start);
        }
    }

    /**
     * Logs that the claims were left to expire.
     *
     * @param reason why
     * @param start  when closing began, from {@link System#nanoTime()}
     */
    private void notReleased(String reason, long start)
    {
        LOG.warn("stage.claims_not_released stage={} reason={} elapsed_ms={}",
                processor.stage(), reason, sinceMillis(start));
    }

    /**
     * The time since a moment.
     *
     * @param start the moment, from {@link System#nanoTime()}
     * @return the milliseconds since
     */
    private static long sinceMillis(long start)
    {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }
}
