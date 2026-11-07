/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

import com.sdh.healthcare.connector.broker.receive.BatchHandler;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.IngestTables;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds split records whose {@code record.split} messages never reached
 * the convert stage (STAGES.md §9). It looks at the items with records in
 * {@code split_records} committed between the lookback and the grace ago
 * (or since the registry's watermark, if that is earlier: below) that
 * aren't complete here; any of their records the registry doesn't
 * know has its {@code record.split} rebuilt from its row and handed to the
 * worker, as if received. A record rebuilt this way doesn't know its queue
 * message, so if it fails, the producer's result names no message for it;
 * the processor audits it, {@code convert.placement_unknown}.
 * <p>
 * An item whose records are all here but whose {@code item.split} never
 * arrived can't be completed: its producer's references are known only to
 * landing. It is audited, {@code convert.item_total_missing}, once per
 * item while this pod holds the duties (again after a restart), for an
 * operator to recover by hand (STAGES.md §13.2). Runs on the pod that
 * holds the duties lease.
 * <p>
 * The registry keeps how far it has checked ({@link StageRegistry#checkedTo}),
 * so that after an outage longer than the lookback, the days in between are
 * checked too, rather than never. A run goes a day at a time, so a day's
 * record IDs are the most it holds, and moves the watermark on after each
 * day, so a run cut short resumes where it stopped. Catching up is audited,
 * {@code convert.reconcile_catching_up} and {@code
 * convert.reconcile_caught_up}, with the days to check.
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
public final class ConvertReconciler
{

    /** The most rebuilt messages handed to the worker at once. */
    static final int BATCH = 100;

    /** The most of {@code split_records} a run reads at once, by time. */
    static final Duration WINDOW = Duration.ofDays(1);

    /** Logs each run's counts. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ConvertReconciler.class);

    /** The split stage's table. */
    private final Table splitRecords;

    /** What the stage has claimed, stored and completed. */
    private final StageRegistry registry;

    /** Converts the rebuilt records. */
    private final StageWorker<?> worker;

    /** The worker's processor, told which records are rebuilt. */
    private final ConvertProcessor processor;

    /** The catalog's name, put in the rebuilt messages. */
    private final String catalogName;

    /** The namespace of {@code split_records}. */
    private final String namespace;

    /** How long a record may wait for its message. */
    private final Duration grace;

    /** How far back to look. */
    private final Duration lookback;

    /** The time. */
    private final Clock clock;

    /** When to wait for a backlog to drain, and for how long at most. */
    private final Deferral deferral;

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
     * it waits one {@code quiet} whatever. Times are the database's.
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
     * @param splitRecords the split stage's table
     * @param registry     the stage's registry
     * @param worker       converts the rebuilt records
     * @param processor    the worker's processor, told which records are
     *                     rebuilt
     * @param catalogName  the catalog's name
     * @param namespace    the namespace of {@code split_records}
     * @param grace        how long a record may wait for its message
     * @param lookback     how far back to look
     * @param clock        the time
     */
    public ConvertReconciler(Table splitRecords, StageRegistry registry,
            StageWorker<?> worker, ConvertProcessor processor,
            String catalogName, String namespace, Duration grace,
            Duration lookback, Clock clock)
    {
        this(splitRecords, registry, worker, processor, catalogName,
                namespace, grace, lookback, clock, Deferral.OFF);
    }

    /**
     * Creates a reconciler.
     *
     * @param splitRecords the split stage's table
     * @param registry     the stage's registry
     * @param worker       converts the rebuilt records
     * @param processor    the worker's processor, told which records are
     *                     rebuilt
     * @param catalogName  the catalog's name
     * @param namespace    the namespace of {@code split_records}
     * @param grace        how long a record may wait for its message
     * @param lookback     how far back to look
     * @param clock        the time
     * @param deferral     when to wait for a backlog to drain
     */
    public ConvertReconciler(Table splitRecords, StageRegistry registry,
            StageWorker<?> worker, ConvertProcessor processor,
            String catalogName, String namespace, Duration grace,
            Duration lookback, Clock clock, Deferral deferral)
    {
        this.deferral = deferral;
        this.splitRecords = splitRecords;
        this.registry = registry;
        this.worker = worker;
        this.processor = processor;
        this.catalogName = catalogName;
        this.namespace = namespace;
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
        splitRecords.refresh();
        Snapshot snapshot = splitRecords.currentSnapshot();
        if (snapshot == null || deferred())
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
            Telemetry.get().auditWarn("convert.reconcile_catching_up",
                    Fields.of(Attr.STAGE, ConvertProcessor.STAGE)
                            .with(Attr.COUNT, days));
        }
        Set<UUID> looked = new HashSet<>();
        Tally tally = new Tally();
        for (Instant start = from; start.isBefore(to);)
        {
            Instant end = start.plus(WINDOW).isBefore(to)
                    ? start.plus(WINDOW) : to;
            Map<UUID, Set<String>> committed = CommittedRecords
                    .committedBetween(splitRecords, start, end);
            looked.addAll(committed.keySet());
            reconcile(committed, snapshot.snapshotId(), tally);
            registry.markChecked(end);
            start = end;
        }
        reportedMissing.retainAll(looked);
        if (catchingUp)
        {
            Telemetry.get().audit("convert.reconcile_caught_up",
                    Fields.of(Attr.STAGE, ConvertProcessor.STAGE)
                            .with(Attr.COUNT, days));
        }
        LOG.info("convert.reconciled checked={} recovered={}"
                + " completed={} totals_missing={} days={}", looked.size(),
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
     * @return whether to rebuild nothing this time
     * @throws IllegalStateException if the registry can't be reached
     */
    private boolean deferred()
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
        Telemetry.get().auditWarn("convert.reconcile_deferral_capped",
                Fields.of(Attr.STAGE, ConvertProcessor.STAGE)
                        .with(Attr.COUNT, waited.toMinutes()));
        registry.endDeferral();
        return false;
    }

    /**
     * Audits a run that waits.
     *
     * @param reason {@code BACKLOG} or {@code STARTING}
     * @param waited how long it has waited so far
     * @return true
     */
    private static boolean defer(String reason, Duration waited)
    {
        Telemetry.get().audit("convert.reconcile_deferred",
                Fields.of(Attr.STAGE, ConvertProcessor.STAGE)
                        .with(Attr.REASON, reason)
                        .with(Attr.COUNT, waited.toMinutes()));
        LOG.info("convert.reconcile_deferred reason={} waited_ms={}", reason,
                waited.toMillis());
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
                    if (reportedMissing.add(itemId))
                    {
                        Telemetry.get().auditWarn(
                                "convert.item_total_missing",
                                Fields.of(Attr.STAGE, ConvertProcessor.STAGE)
                                        .with(Attr.ITEM_ID, itemId)
                                        .with(Attr.COUNT,
                                                item.getValue().size()));
                    }
                    tally.totalsMissing++;
                }
                continue;
            }
            List<ConnectorMessage> lost = rebuild(itemId, known, snapshotId);
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
            Telemetry.get().auditWarn("convert.records_recovered",
                    Fields.of(Attr.ITEM_ID, itemId)
                            .with(Attr.COUNT, lost.size()));
            tally.recovered += lost.size();
        }
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
        Telemetry.get().auditWarn("convert.item_completion_recovered",
                Fields.of(Attr.ITEM_ID, itemId)
                        .with(Attr.COUNT, progress.expected()));
        tally.completed++;
    }

    /**
     * Rebuilds the {@code record.split} of an item's records the registry
     * doesn't know.
     *
     * @param itemId     the item
     * @param known      the records the registry knows
     * @param snapshotId the table's snapshot, which holds them all
     * @return the messages, in sequence order
     */
    private List<ConnectorMessage> rebuild(UUID itemId, Set<String> known,
            long snapshotId)
    {
        List<ConnectorMessage> lost = new ArrayList<>();
        for (CommittedRecords.Row row
                : CommittedRecords.all(splitRecords, itemId))
        {
            if (known.contains(row.recordId()))
            {
                continue;
            }
            StorageRef ref = new StorageRef(catalogName, namespace,
                    IngestTables.SPLIT_RECORDS, row.ingestedDate(),
                    snapshotId, row.kekRef());
            lost.add(new RecordSplit(row.recordId(), itemId, row.seq(),
                    row.sourceOffset(), row.sourceLength(), row.format(),
                    row.contentType(), row.plaintextLength(),
                    row.chunkCount(), ref, row.origin(), row.errorCode(),
                    row.errorAt(), -1, null, null));
        }
        return lost;
    }
}
