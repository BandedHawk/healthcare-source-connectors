/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record;

import com.sdh.healthcare.connector.broker.receive.BatchHandler;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.stage.ItemProgress;
import com.sdh.healthcare.connector.stage.StageRegistry;
import com.sdh.healthcare.connector.stage.StageWorker;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds records in a record stage's source table whose messages never
 * reached the stage (STAGES.md §9, §17.3). It looks at the items with
 * records in the source committed between the lookback and the grace ago
 * (or since the registry's watermark, if that is earlier: below) that
 * aren't complete here; any of their records the registry doesn't know
 * has its input message rebuilt from its row and handed to the worker, as
 * if received. A record rebuilt this way doesn't know its queue message,
 * so if it fails, the producer's result names no message for it; the
 * processor audits it, {@code stage.placement_unknown}.
 * <p>
 * An item whose records are all here but whose item end never arrived
 * can't be completed: its producer's references come only with it. It is
 * marked in the registry ({@link StageRegistry#wantTotal}), for the stage
 * before to send its end again ({@link
 * com.sdh.healthcare.connector.stage.ItemEndResender}), and audited, {@code
 * stage.item_total_missing}, once per item while this pod holds the duties
 * (again after a restart) (STAGES.md §13.2). Runs on the pod that holds the
 * duties lease.
 * <p>
 * The registry keeps how far it has checked ({@link StageRegistry#checkedTo}),
 * so that after an outage longer than the lookback, the days in between are
 * checked too, rather than never. A run goes a day at a time, so a day's
 * record IDs are the most it holds, and moves the watermark on after each
 * day, so a run cut short resumes where it stopped. Catching up is audited,
 * {@code stage.reconcile_catching_up} and {@code
 * stage.reconcile_caught_up}, with the days to check.
 * <p>
 * A record that looks lost may only be queued behind a backlog: after an
 * outage, or whenever the queue falls further behind than the grace. So
 * while records older than the grace are still coming off the queue
 * ({@link BacklogWatch}), a run waits rather than rebuild them, up to a
 * limit ({@link Deferral}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RecordReconciler
{

    /** The most rebuilt messages handed to the worker at once. */
    static final int BATCH = 100;

    /** The most expired claims a sweep looks at. */
    static final int SWEEP_LIMIT = 1000;

    /**
     * How long ago a claim must have expired for a sweep to take it up,
     * unless set: longer than the broker takes to deliver a stopped
     * worker's messages again (Kafka's session timeout is 45 seconds), so
     * that they take it over first if they come.
     */
    public static final Duration DEFAULT_CLAIM_AGE = Duration.ofMinutes(1);

    /** The most of the source a run reads at once, by time. */
    static final Duration WINDOW = Duration.ofDays(1);

    /** Logs each run's counts. */
    private static final Logger LOG =
            LoggerFactory.getLogger(RecordReconciler.class);

    /** The source table. */
    private final RecordTable source;

    /** What the stage has claimed, stored and completed. */
    private final StageRegistry registry;

    /** Processes the rebuilt records. */
    private final StageWorker<?> worker;

    /** The worker's processor, told which records are rebuilt. */
    private final RecordStageProcessor processor;

    /** The stage's messages, for the type of its input. */
    private final StageMessages messages;

    /** How long a record may wait for its message. */
    private final Duration grace;

    /** How far back to look. */
    private final Duration lookback;

    /** The time. */
    private final Clock clock;

    /** When to wait for a backlog to drain, and for how long at most. */
    private final Deferral deferral;

    /** How long ago a claim must have expired for a sweep to take it up. */
    private final Duration claimAge;

    /** Whether a run has cleared a wait left by an earlier duties holder. */
    private boolean started;

    /**
     * Whether a run has gone ahead, or seen a backlog: past the wait at
     * startup.
     */
    private boolean warmedUp;

    /**
     * The items audited as missing their total, so each is audited once.
     * Only items still looked at are kept, so it can't grow without end.
     */
    private final Set<UUID> reportedMissing = new HashSet<>();

    /**
     * When the reconciler waits for a backlog to drain (STAGES.md §9).
     * While a record older than the grace has come off the queue, on any
     * pod, within {@code quiet}, the records that look lost are most likely
     * still queued, so a run rebuilds nothing, and the watermark stays
     * where it is. After waiting {@code most}, a run goes ahead anyway, so
     * a queue that is always behind can't hide a real loss for long; then
     * it waits again. At startup, before the pods have received anything,
     * it waits one {@code quiet} whatever; but not when it takes over from
     * a holder that went ahead within the grace and two {@code quiet}s
     * (2026-11-18): the other pods kept receiving, and noting any backlog,
     * up to the hand-over. Times are the database's.
     *
     * @param quiet how long since the last backlog record before a run
     *              goes ahead; zero never to wait
     * @param most  the longest it waits before a run goes ahead anyway
     */
    public record Deferral(Duration quiet, Duration most)
    {

        /** Never waits. */
        public static final Deferral OFF =
                new Deferral(Duration.ZERO, Duration.ZERO);

        /**
         * Whether a run ever waits.
         *
         * @return whether {@code quiet} is above zero
         */
        boolean enabled()
        {
            return quiet.isPositive();
        }
    }

    /**
     * Creates a reconciler that never waits for a backlog.
     *
     * @param source    the source table
     * @param messages  the stage's messages
     * @param registry  the stage's registry
     * @param worker    processes the rebuilt records
     * @param processor the worker's processor, told which records are
     *                  rebuilt
     * @param grace     how long a record may wait for its message
     * @param lookback  how far back to look
     * @param clock     the time
     */
    public RecordReconciler(RecordTable source, StageMessages messages,
            StageRegistry registry, StageWorker<?> worker,
            RecordStageProcessor processor, Duration grace,
            Duration lookback, Clock clock)
    {
        this(source, messages, registry, worker, processor, grace, lookback,
                clock, Deferral.OFF);
    }

    /**
     * Creates a reconciler.
     *
     * @param source    the source table
     * @param messages  the stage's messages
     * @param registry  the stage's registry
     * @param worker    processes the rebuilt records
     * @param processor the worker's processor, told which records are
     *                  rebuilt
     * @param grace     how long a record may wait for its message
     * @param lookback  how far back to look
     * @param clock     the time
     * @param deferral  when to wait for a backlog to drain
     */
    public RecordReconciler(RecordTable source, StageMessages messages,
            StageRegistry registry, StageWorker<?> worker,
            RecordStageProcessor processor, Duration grace,
            Duration lookback, Clock clock, Deferral deferral)
    {
        this(source, messages, registry, worker, processor, grace, lookback,
                clock, deferral, DEFAULT_CLAIM_AGE);
    }

    /**
     * Creates a reconciler that also sweeps expired claims.
     *
     * @param source    the source table
     * @param messages  the stage's messages
     * @param registry  the stage's registry
     * @param worker    processes the rebuilt records
     * @param processor the worker's processor, told which records are
     *                  rebuilt
     * @param grace     how long a record may wait for its message
     * @param lookback  how far back to look
     * @param clock     the time
     * @param deferral  when to wait for a backlog to drain
     * @param claimAge  how long ago a claim must have expired for a sweep
     *                  to take it up ({@link #sweepExpiredClaims})
     */
    public RecordReconciler(RecordTable source, StageMessages messages,
            StageRegistry registry, StageWorker<?> worker,
            RecordStageProcessor processor, Duration grace,
            Duration lookback, Clock clock, Deferral deferral,
            Duration claimAge)
    {
        this.claimAge = claimAge;
        this.deferral = deferral;
        this.source = source;
        this.messages = messages;
        this.registry = registry;
        this.worker = worker;
        this.processor = processor;
        this.grace = grace;
        this.lookback = lookback;
        this.clock = clock;
    }

    /**
     * Looks once: from the earlier of the watermark and the lookback ago,
     * to the grace ago, a day at a time.
     *
     * @return how many records were handed to the worker
     * @throws InterruptedException  if interrupted
     * @throws IllegalStateException if the worker held a batch back, or the
     *                               registry can't be reached; the rest is
     *                               looked at next time, from the watermark
     */
    public int reconcile() throws InterruptedException
    {
        Instant now = clock.instant();
        Table table = source.table();
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null || deferred(now))
        {
            return 0;
        }
        Instant to = now.minus(grace);
        Instant recent = now.minus(lookback);
        // With no watermark yet, nothing older than the lookback is looked
        // at: a stage that has never reconciled, or one upgraded to this.
        Instant from = registry.checkedTo()
                .filter(checked -> checked.isBefore(recent))
                .orElse(recent);
        long days = days(from, to);
        boolean catchingUp = from.isBefore(recent);
        if (catchingUp)
        {
            Telemetry.get().auditWarn("stage.reconcile_catching_up",
                    Fields.of(Attr.STAGE, messages.stage())
                            .with(Attr.COUNT, days));
        }
        Set<UUID> looked = new HashSet<>();
        Tally tally = new Tally();
        for (Instant start = from; start.isBefore(to);)
        {
            Instant end = start.plus(WINDOW).isBefore(to)
                    ? start.plus(WINDOW) : to;
            Map<UUID, Set<String>> committed = CommittedRecords
                    .committedBetween(table, start, end);
            looked.addAll(committed.keySet());
            reconcile(committed, snapshot.snapshotId(), tally);
            registry.markChecked(end);
            start = end;
        }
        reportedMissing.retainAll(looked);
        if (catchingUp)
        {
            Telemetry.get().audit("stage.reconcile_caught_up",
                    Fields.of(Attr.STAGE, messages.stage())
                            .with(Attr.COUNT, days));
        }
        LOG.info("stage.reconciled stage={} checked={} recovered={}"
                + " completed={} totals_missing={} days={}", messages.stage(),
                looked.size(),
                tally.recovered, tally.completed, tally.totalsMissing, days);
        return tally.recovered;
    }

    /**
     * Whether this run waits for a backlog to drain. It waits while a
     * record older than the grace has come off the queue within the quiet
     * time, until it has waited the most it may; and, on this reconciler's
     * first runs, for one quiet time whatever the queue shows, since at
     * startup the pods haven't yet received anything to show a backlog
     * by. The time waited is counted afresh by each reconciler, so from
     * when this pod took the duties. Audited each time it waits
     * ({@code sdh.reason} {@code BACKLOG} or {@code STARTING}), and when it
     * stops waiting because it has waited the most, each with
     * {@code sdh.count}, the minutes it has waited.
     *
     * @param now the time of this run
     * @return whether to rebuild nothing this time
     * @throws IllegalStateException if the registry can't be reached
     */
    private boolean deferred(Instant now)
    {
        if (!deferral.enabled())
        {
            return false;
        }
        if (!started)
        {
            // A wait left by a pod that held the duties before.
            registry.endDeferral();
            started = true;
            warmedUp = handedOver(now);
        }
        Optional<Duration> since = registry.sinceBacklog();
        boolean backlog = since.isPresent()
                && since.get().compareTo(deferral.quiet()) < 0;
        if (!backlog && warmedUp)
        {
            registry.endDeferral();
            return false;
        }
        Duration waited = registry.deferring();
        if (!backlog)
        {
            if (waited.compareTo(deferral.quiet()) < 0)
            {
                return defer("STARTING", waited);
            }
            warmedUp = true;
            registry.endDeferral();
            return false;
        }
        warmedUp = true;
        if (waited.compareTo(deferral.most()) < 0)
        {
            return defer("BACKLOG", waited);
        }
        Telemetry.get().auditWarn("stage.reconcile_deferral_capped",
                Fields.of(Attr.STAGE, messages.stage())
                        .with(Attr.COUNT, waited.toMinutes()));
        registry.endDeferral();
        return false;
    }

    /**
     * Whether this pod took the duties over from a holder that was past its
     * own startup: one whose last run went ahead within the grace and two
     * {@code quiet}s, so moved the watermark to within two {@code quiet}s
     * of now less the grace. The pods kept noting any backlog up to the
     * hand-over, so the wait at startup is not needed. Logged if so.
     *
     * @param now the time of this run
     * @return whether the wait at startup can be skipped
     * @throws IllegalStateException if the registry can't be reached
     */
    private boolean handedOver(Instant now)
    {
        Instant recent = now.minus(grace)
                .minus(deferral.quiet().multipliedBy(2));
        boolean recently = registry.checkedTo()
                .filter(checked -> checked.isAfter(recent)).isPresent();
        if (recently)
        {
            LOG.info("stage.reconcile_handed_over stage={}",
                    messages.stage());
        }
        return recently;
    }

    /**
     * Audits a run that waits.
     *
     * @param reason {@code BACKLOG} or {@code STARTING}
     * @param waited how long it has waited so far
     * @return true
     */
    private boolean defer(String reason, Duration waited)
    {
        Telemetry.get().audit("stage.reconcile_deferred",
                Fields.of(Attr.STAGE, messages.stage())
                        .with(Attr.REASON, reason)
                        .with(Attr.COUNT, waited.toMinutes()));
        LOG.info("stage.reconcile_deferred stage={} reason={} waited_ms={}",
                messages.stage(), reason, waited.toMillis());
        return true;
    }

    /** What a run found, over its windows. */
    private static final class Tally
    {

        /** The records handed to the worker. */
        int recovered;

        /** The items whose total never came. */
        int totalsMissing;

        /** The items found stored in full but not completed, completed. */
        int completed;

        /** Created at the start of a run. */
        Tally()
        {
        }
    }

    /**
     * How many days, or parts of a day, a span covers.
     *
     * @param from its start
     * @param to   its end
     * @return the days; 0 if it is empty
     */
    static long days(Instant from, Instant to)
    {
        long millis = Math.max(0, Duration.between(from, to).toMillis());
        return (millis + WINDOW.toMillis() - 1) / WINDOW.toMillis();
    }

    /**
     * Looks at the items with records committed in one window.
     *
     * @param committed  each item's records committed in the window
     * @param snapshotId the table's snapshot, which holds them all
     * @param tally      what the run has found, added to
     * @throws InterruptedException  if interrupted
     * @throws IllegalStateException if the worker held a batch back
     */
    private void reconcile(Map<UUID, Set<String>> committed, long snapshotId,
            Tally tally) throws InterruptedException
    {
        Set<UUID> completed = registry.completed(committed.keySet());
        reportedMissing.removeAll(completed);
        // The items short of records, with those known: rebuilt below from
        // one scan of the source for them all, not two an item.
        Map<UUID, Set<String>> shortOf = new LinkedHashMap<>();
        for (Map.Entry<UUID, Set<String>> item : committed.entrySet())
        {
            UUID itemId = item.getKey();
            if (completed.contains(itemId))
            {
                continue;
            }
            Set<String> known = registry.knownRecords(itemId);
            if (known.containsAll(item.getValue()))
            {
                Optional<ItemProgress> progress = registry.item(itemId);
                if (progress.isPresent() && progress.get().expected() >= 0)
                {
                    complete(itemId, progress.get(), tally);
                }
                else
                {
                    // For the stage before to send it again (§13.2).
                    registry.wantTotal(itemId);
                    if (reportedMissing.add(itemId))
                    {
                        Telemetry.get().auditWarn(
                                "stage.item_total_missing",
                                Fields.of(Attr.STAGE, messages.stage())
                                        .with(Attr.ITEM_ID, itemId)
                                        .with(Attr.COUNT,
                                                item.getValue().size()));
                    }
                    tally.totalsMissing++;
                }
                continue;
            }
            shortOf.put(itemId, known);
        }
        if (shortOf.isEmpty())
        {
            return;
        }
        Map<UUID, List<CommittedRecords.Row>> rows =
                CommittedRecords.withCorrections(source.table(),
                        shortOf.keySet());
        Recovery recovery = new Recovery("stage.records_recovered");
        for (Map.Entry<UUID, Set<String>> item : shortOf.entrySet())
        {
            UUID itemId = item.getKey();
            Set<String> known = item.getValue();
            recovery.add(itemId, rebuild(itemId, rows.get(itemId),
                    name -> !known.contains(name), snapshotId));
        }
        tally.recovered += recovery.finish();
    }

    /**
     * Rebuilt records handed to the worker several items at a time, in full
     * batches, not an item a batch: each batch waits its turn to commit, so
     * an item a batch took one batch's time an item (on AWS under load, 4 s
     * on average and 18 s at the 95th percentile, run 47, 2026-11-18). Each
     * item is audited once the batch holding its records is in.
     */
    private final class Recovery
    {

        /** The audit event for each item recovered. */
        private final String event;

        /** The records gathered and not yet handed in. */
        private final List<ConnectorMessage> gathered = new ArrayList<>();

        /** How many records of each item gathered are in it. */
        private final Map<UUID, Integer> items = new LinkedHashMap<>();

        /** How many records have been handed in. */
        private int handedIn;

        /**
         * Starts a recovery.
         *
         * @param event the audit event for each item recovered
         */
        Recovery(String event)
        {
            this.event = event;
        }

        /**
         * Gathers an item's rebuilt records, and hands in what is gathered
         * once it fills a batch.
         *
         * @param itemId the item
         * @param lost   its records; none leaves it out
         * @throws InterruptedException  if interrupted
         * @throws IllegalStateException if the worker held a batch back
         */
        void add(UUID itemId, List<ConnectorMessage> lost)
                throws InterruptedException
        {
            if (lost.isEmpty())
            {
                return;
            }
            gathered.addAll(lost);
            items.merge(itemId, lost.size(), Integer::sum);
            if (gathered.size() >= BATCH)
            {
                handInGathered();
            }
        }

        /**
         * Hands in the rest.
         *
         * @return how many records were handed in, in all
         * @throws InterruptedException  if interrupted
         * @throws IllegalStateException if the worker held a batch back
         */
        int finish() throws InterruptedException
        {
            handInGathered();
            return handedIn;
        }

        /**
         * Hands in what is gathered and audits its items.
         *
         * @throws InterruptedException  if interrupted
         * @throws IllegalStateException if the worker held a batch back
         */
        private void handInGathered() throws InterruptedException
        {
            if (gathered.isEmpty())
            {
                return;
            }
            handIn(List.copyOf(gathered));
            for (Map.Entry<UUID, Integer> item : items.entrySet())
            {
                Telemetry.get().auditWarn(event,
                        Fields.of(Attr.STAGE, messages.stage())
                                .with(Attr.ITEM_ID, item.getKey())
                                .with(Attr.COUNT, item.getValue()));
            }
            handedIn += gathered.size();
            gathered.clear();
            items.clear();
        }
    }

    /**
     * Hands rebuilt messages to the worker, a batch at a time, as if
     * received.
     *
     * @param lost the messages
     * @throws InterruptedException  if interrupted
     * @throws IllegalStateException if the worker held a batch back
     */
    private void handIn(List<ConnectorMessage> lost)
            throws InterruptedException
    {
        for (int start = 0; start < lost.size(); start += BATCH)
        {
            List<ConnectorMessage> batch = lost.subList(start,
                    Math.min(lost.size(), start + BATCH));
            BatchHandler.Outcome outcome;
            processor.rebuilding(batch);
            try
            {
                outcome = worker.handleOpened(batch);
            }
            finally
            {
                processor.rebuilt(batch);
            }
            if (outcome != BatchHandler.Outcome.DONE)
            {
                throw new IllegalStateException(
                        "The worker held back reconciled records");
            }
        }
    }

    /**
     * Hands in again the records whose claims expired, at least the claim
     * age ago, with nothing stored (STAGES.md §10, 2026-11-18): their
     * holder stopped part way, and their messages may have been
     * acknowledged with it, so they may never come again. Unlike a
     * {@link #reconcile} run it doesn't wait for a backlog or at startup:
     * a claim expired that long ago is held by no one, and taking it
     * over is what its message, if it did come again, would do. The
     * worker claims each again, so a record also handed in by its message
     * is processed once. Each item's records are rebuilt from their rows
     * in the source, as a run rebuilds them; a claim with no row there,
     * such as an operator's correction's, which its own step takes up
     * again, is left. Audited per item, {@code stage.claims_recovered},
     * with how many; and each sweep that finds any is logged, with how
     * many it found, handed in and left.
     *
     * @return how many records were handed to the worker
     * @throws InterruptedException  if interrupted
     * @throws IllegalStateException if the worker held a batch back, or the
     *                               registry can't be reached; the rest are
     *                               found again next time
     */
    public int sweepExpiredClaims() throws InterruptedException
    {
        List<StageRegistry.Key> expired =
                registry.expiredClaims(claimAge, SWEEP_LIMIT);
        if (expired.isEmpty())
        {
            return 0;
        }
        Table table = source.table();
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null)
        {
            return 0;
        }
        Map<UUID, Set<String>> byItem = new LinkedHashMap<>();
        for (StageRegistry.Key key : expired)
        {
            byItem.computeIfAbsent(key.itemId(), item -> new HashSet<>())
                    .add(key.recordId());
        }
        // One scan of the source for every item, not two an item: on AWS
        // those took about 20 s an item (run 47, 2026-11-18).
        Map<UUID, List<CommittedRecords.Row>> rows =
                CommittedRecords.withCorrections(table, byItem.keySet());
        Recovery recovery = new Recovery("stage.claims_recovered");
        for (Map.Entry<UUID, Set<String>> item : byItem.entrySet())
        {
            recovery.add(item.getKey(), rebuild(item.getKey(),
                    rows.get(item.getKey()), item.getValue()::contains,
                    snapshot.snapshotId()));
        }
        int recovered = recovery.finish();
        LOG.info("stage.claims_swept stage={} expired={} recovered={}"
                + " left={}", messages.stage(), expired.size(), recovered,
                expired.size() - recovered);
        return recovered;
    }

    /**
     * Completes an item whose records are all known and whose total came,
     * if they are all stored: the batch that would have completed it may
     * have been lost (STAGES.md §10). Records still claimed leave it for
     * their worker.
     *
     * @param itemId   the item
     * @param progress its progress
     * @param tally    what the run has found, added to
     * @throws InterruptedException  if interrupted
     * @throws IllegalStateException if the worker couldn't complete it
     */
    private void complete(UUID itemId, ItemProgress progress, Tally tally)
            throws InterruptedException
    {
        // As the registry's due(): done counts failed records too.
        if (progress.done() < progress.expected())
        {
            return;
        }
        if (worker.completeDue(List.of(itemId)) != BatchHandler.Outcome.DONE)
        {
            throw new IllegalStateException(
                    "The worker couldn't complete a reconciled item");
        }
        Telemetry.get().auditWarn("stage.item_completion_recovered",
                Fields.of(Attr.STAGE, messages.stage())
                        .with(Attr.ITEM_ID, itemId)
                        .with(Attr.COUNT, progress.expected()));
        tally.completed++;
    }

    /**
     * Rebuilds the input message of an item's records the registry doesn't
     * know.
     *
     * @param itemId     the item
     * @param rows       its rows in the source, then those made from
     *                   corrections ({@link CommittedRecords#withCorrections})
     * @param wanted     whether to rebuild a record, by its name in the
     *                   registry ({@link StageRegistry.Key})
     * @param snapshotId the table's snapshot, which holds them all
     * @return the messages, in sequence order
     */
    private List<ConnectorMessage> rebuild(UUID itemId,
            List<CommittedRecords.Row> rows, Predicate<String> wanted,
            long snapshotId)
    {
        List<ConnectorMessage> lost = new ArrayList<>();
        // With the records operators' corrections made at the stage before
        // (STAGES.md §15.6d), sent on as any record.
        for (CommittedRecords.Row row : rows)
        {
            if (!wanted.test(StageRegistry.Key.of(itemId, row.recordId(),
                    row.lineage()).recordId()) || row.errorCode() != null)
            {
                // Not wanted, or one the stage before failed: in its fail
                // store, never sent on (STAGES.md §15.1).
                continue;
            }
            lost.add(SourceInputs.rebuild(messages, itemId, row,
                    source.ref(row.ingestedDate(), snapshotId,
                            row.kekRef())));
        }
        return lost;
    }
}
