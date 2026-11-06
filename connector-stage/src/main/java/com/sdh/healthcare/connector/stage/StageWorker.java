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
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
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
 *     counted on their items;</li>
 * <li>the messages are sent, and marked sent;</li>
 * <li>a record already stored is a duplicate if its output is the same,
 *     and is only sent again if its message never was; with a different
 *     output it is a conflict, reported to the dead-letter queue, and the
 *     first output is kept;</li>
 * <li>a record another worker holds is waited for, after the rest is
 *     stored and sent, until that worker stores it or lets its claim
 *     expire;</li>
 * <li>an item whose records are all stored gets its item-complete message.
 *     </li>
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

    /** Sends conflicts to the dead-letter queue. */
    private final MessagePublisher deadLetters;

    /** How the worker runs. */
    private final Settings settings;

    /** Gives the time conflicts are found at. */
    private final Clock clock;

    /** Processes records, several at a time. */
    private final ExecutorService workers;

    /** Renews this worker's claims. */
    private final ScheduledExecutorService renewer;

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
     * A record of the batch, with its message.
     *
     * @param input   what the processor read in it
     * @param message the message
     */
    private record Pending(StageInput.RecordInput input,
            ConnectorMessage message)
    {
    }

    /**
     * What came of one record in one round.
     *
     * @param pending  the record
     * @param found    what was found
     * @param output   its output: when claimed, or when stored before by
     *                 another worker
     * @param stored   the registry's row, when stored before
     */
    private record Result<O>(Pending pending, Found found,
            StageOutput<O> output, StageRegistry.Stored stored)
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

        /** Stored before by another, with a different output. */
        CONFLICT,

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
     * @param deadLetters sends conflicts to the dead-letter queue
     * @param settings    how the worker runs
     * @param clock       gives the time conflicts are found at
     */
    public StageWorker(StageProcessor<O> processor, StageRegistry registry,
            MessageOpener opener, MessageSealer sealer,
            MessagePublisher output, MessagePublisher deadLetters,
            Settings settings, Clock clock)
    {
        this.processor = processor;
        this.registry = registry;
        this.opener = opener;
        this.sealer = sealer;
        this.output = output;
        this.deadLetters = deadLetters;
        this.settings = settings;
        this.clock = clock;
        this.workers = Executors.newFixedThreadPool(settings.concurrency(),
                Thread.ofPlatform().name("sdh-" + processor.stage() + "-", 0)
                        .daemon().factory());
        this.renewer = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("sdh-" + processor.stage()
                        + "-claims").daemon().factory());
    }

    /** Starts renewing this worker's claims. */
    public void start()
    {
        long period = settings.renewInterval().toMillis();
        renewer.scheduleWithFixedDelay(this::renew, period, period,
                TimeUnit.MILLISECONDS);
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
        try
        {
            Map<String, Pending> records = new LinkedHashMap<>();
            Set<UUID> items = new LinkedHashSet<>();
            for (BrokerMessage received : batch)
            {
                ConnectorMessage message;
                try
                {
                    message = opener.open(received.body());
                }
                catch (MessageRejectedException rejected)
                {
                    if (rejected.reason()
                            == MessageRejectedException.Reason
                                    .UNSUPPORTED_VERSION)
                    {
                        // Dropping it would strand its record; it waits on
                        // the queue until this stage is upgraded.
                        Telemetry.get().auditError("stage.unreadable_version",
                                stageFields(ErrorCode.MESSAGE_REJECTED));
                        return Outcome.RETRY;
                    }
                    Telemetry.get().auditError("stage.message_rejected",
                            stageFields(ErrorCode.MESSAGE_REJECTED)
                                    .with(Attr.REASON,
                                            rejected.reason().name()));
                    continue;
                }
                switch (processor.classify(message))
                {
                    case StageInput.RecordInput input ->
                    {
                        // The same record twice in one batch is one record.
                        records.putIfAbsent(input.recordId(),
                                new Pending(input, message));
                        items.add(input.itemId());
                    }
                    case StageInput.ItemTotal total ->
                    {
                        registry.expect(total.itemId(), total.expected());
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
     * stored but never sent, and reports conflicts.
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
        List<String> resent = new ArrayList<>();
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
                        resent.add(result.pending().input().recordId());
                    }
                }
                case DUPLICATE ->
                {
                    Telemetry.get().audit("record.duplicate",
                            itemFields(result.pending()));
                    if (result.stored().pending() != null)
                    {
                        resend.add(result.stored().pending());
                        resent.add(result.pending().input().recordId());
                    }
                }
                case CONFLICT -> conflict(result);
                case BUSY -> busy.add(result.pending());
            }
        }
        List<OutboundMessage> sends = new ArrayList<>(resend);
        List<String> sent = new ArrayList<>(resent);
        if (!owned.isEmpty())
        {
            List<StageRegistry.Entry> entries = commit(owned);
            Set<String> marked = registry.store(entries);
            for (StageRegistry.Entry entry : entries)
            {
                if (marked.contains(entry.recordId()))
                {
                    sends.add(entry.message());
                    sent.add(entry.recordId());
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
        StageRegistry.Claim claim = registry.claim(record.input().recordId(),
                record.input().itemId(), settings.claimTtl());
        return switch (claim)
        {
            case StageRegistry.Owned owned -> new Result<>(record,
                    Found.OWNED,
                    processor.process(record.message(), owned.attempts()),
                    null);
            case StageRegistry.Stored stored when stored.mine() ->
                    new Result<>(record, Found.MINE, null, stored);
            case StageRegistry.Stored stored ->
            {
                // Processed again only to compare: the same output is a
                // duplicate, a different one a conflict.
                StageOutput<O> again = processor.process(record.message(), 1);
                yield new Result<>(record,
                        again.digest().equals(stored.digest())
                                ? Found.DUPLICATE : Found.CONFLICT,
                        again, stored);
            }
            case StageRegistry.Busy busy ->
                    new Result<>(record, Found.BUSY, null, null);
        };
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
        List<ConnectorMessage> messages = processor.commit(outputs);
        if (messages.size() != outputs.size())
        {
            throw new IllegalStateException(
                    "The processor made a message for each of "
                            + messages.size() + " of " + outputs.size()
                            + " outputs");
        }
        List<StageRegistry.Entry> entries = new ArrayList<>();
        for (int index = 0; index < owned.size(); index++)
        {
            Result<O> result = owned.get(index);
            entries.add(new StageRegistry.Entry(
                    result.pending().input().recordId(),
                    result.pending().input().itemId(),
                    result.output().digest(), result.output().failed(),
                    outbound(messages.get(index))));
        }
        return entries;
    }

    /**
     * Reports a conflict to the dead-letter queue: the record stored first
     * stays, the new output is dropped (STAGES.md §6).
     *
     * @param result the record, its new output and its stored row
     * @throws InterruptedException if interrupted
     * @throws ExecutionException   if the dead-letter queue refused it
     * @throws TimeoutException     if it didn't take it in time
     */
    private void conflict(Result<O> result)
            throws InterruptedException, ExecutionException,
            TimeoutException
    {
        Telemetry.get().auditWarn("record.conflict",
                itemFields(result.pending())
                        .with(Attr.ERROR_CODE, ErrorCode.RECORD_CONFLICT));
        send(deadLetters, List.of(outbound(
                new ConnectorMessage.RecordConflict(
                        result.pending().input().recordId(),
                        result.pending().input().itemId(),
                        processor.stage(), result.stored().digest(),
                        result.output().digest(), clock.instant()))));
    }

    /**
     * Sends the item-complete message of each item whose records are all
     * stored, and hasn't had it. Two workers may both send it; its id is
     * fixed, so the repeat is dropped.
     *
     * @param items the items the batch touched
     * @throws InterruptedException if interrupted
     * @throws ExecutionException   if the broker refused a message
     * @throws TimeoutException     if it didn't take one in time
     */
    private void completeItems(Set<UUID> items)
            throws InterruptedException, ExecutionException,
            TimeoutException
    {
        for (ItemProgress progress : registry.due(items))
        {
            send(output, List.of(outbound(processor.itemComplete(progress))));
            registry.markCompleted(progress.itemId());
            Telemetry.get().audit("stage.item_completed",
                    Fields.of(Attr.STAGE, processor.stage())
                            .with(Attr.ITEM_ID, progress.itemId())
                            .with(Attr.COUNT, progress.done()));
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
     * The fields of an event about a record: its stage and item, never its
     * id.
     *
     * @param record the record
     * @return the fields
     */
    private Fields itemFields(Pending record)
    {
        return Fields.of(Attr.STAGE, processor.stage())
                .with(Attr.ITEM_ID, record.input().itemId());
    }

    /** Stops processing and renewing; held claims expire. */
    @Override
    public void close()
    {
        renewer.shutdownNow();
        workers.shutdownNow();
    }
}
