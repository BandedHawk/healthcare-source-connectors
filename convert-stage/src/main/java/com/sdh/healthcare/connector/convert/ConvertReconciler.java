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
 * that aren't complete here; any of their records the registry doesn't
 * know has its {@code record.split} rebuilt from its row and handed to the
 * worker, as if received. A record rebuilt this way doesn't know its queue
 * message, so if it fails, the producer's result names no message for it.
 * <p>
 * An item whose records are all here but whose {@code item.split} never
 * arrived can't be completed: its producer's references are known only to
 * landing. It is audited, {@code convert.item_total_missing}, for an
 * operator (STAGES.md §12). Runs on the pod that holds the duties lease.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ConvertReconciler
{

    /** The most rebuilt messages handed to the worker at once. */
    static final int BATCH = 100;

    /** Logs each run's counts. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ConvertReconciler.class);

    /** The split stage's table. */
    private final Table splitRecords;

    /** What the stage has claimed, stored and completed. */
    private final StageRegistry registry;

    /** Converts the rebuilt records. */
    private final StageWorker<?> worker;

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

    /**
     * Creates a reconciler.
     *
     * @param splitRecords the split stage's table
     * @param registry     the stage's registry
     * @param worker       converts the rebuilt records
     * @param catalogName  the catalog's name
     * @param namespace    the namespace of {@code split_records}
     * @param grace        how long a record may wait for its message
     * @param lookback     how far back to look
     * @param clock        the time
     */
    public ConvertReconciler(Table splitRecords, StageRegistry registry,
            StageWorker<?> worker, String catalogName, String namespace,
            Duration grace, Duration lookback, Clock clock)
    {
        this.splitRecords = splitRecords;
        this.registry = registry;
        this.worker = worker;
        this.catalogName = catalogName;
        this.namespace = namespace;
        this.grace = grace;
        this.lookback = lookback;
        this.clock = clock;
    }

    /**
     * Looks once.
     *
     * @return how many records were handed to the worker
     * @throws InterruptedException  if interrupted
     * @throws IllegalStateException if the worker held a batch back; the
     *                               rest is looked at next time
     */
    public int reconcile() throws InterruptedException
    {
        Instant now = clock.instant();
        splitRecords.refresh();
        Snapshot snapshot = splitRecords.currentSnapshot();
        if (snapshot == null)
        {
            return 0;
        }
        Map<UUID, Set<String>> committed = CommittedRecords.committedBetween(
                splitRecords, now.minus(lookback), now.minus(grace));
        Set<UUID> completed = registry.completed(committed.keySet());
        int recovered = 0;
        int totalsMissing = 0;
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
                if (progress.isEmpty() || progress.get().expected() < 0)
                {
                    Telemetry.get().auditWarn("convert.item_total_missing",
                            Fields.of(Attr.ITEM_ID, itemId));
                    totalsMissing++;
                }
                continue;
            }
            List<ConnectorMessage> lost = rebuild(itemId, known,
                    snapshot.snapshotId());
            for (int start = 0; start < lost.size(); start += BATCH)
            {
                List<ConnectorMessage> batch = lost.subList(start,
                        Math.min(lost.size(), start + BATCH));
                if (worker.handleOpened(batch) != BatchHandler.Outcome.DONE)
                {
                    throw new IllegalStateException(
                            "The worker held back reconciled records");
                }
            }
            Telemetry.get().auditWarn("convert.records_recovered",
                    Fields.of(Attr.ITEM_ID, itemId)
                            .with(Attr.COUNT, lost.size()));
            recovered += lost.size();
        }
        LOG.info("convert.reconciled checked={} recovered={}"
                + " totals_missing={}", committed.size(), recovered,
                totalsMissing);
        return recovered;
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
