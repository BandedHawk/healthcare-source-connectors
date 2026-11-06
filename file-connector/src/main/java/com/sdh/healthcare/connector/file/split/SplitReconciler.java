/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.split;

import static com.sdh.healthcare.connector.core.landing.LandingTables.CHUNK_INDEX;
import static com.sdh.healthcare.connector.core.landing.LandingTables.INTAKE;
import static com.sdh.healthcare.connector.core.landing.LandingTables.ITEM_DIGEST;
import static com.sdh.healthcare.connector.core.landing.LandingTables.ITEM_ID;
import static com.sdh.healthcare.connector.core.landing.LandingTables.LANDED_AT;
import static com.sdh.healthcare.connector.core.landing.LandingTables.SUMMARY_ROW;

import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.LeaseHeldException;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds landed items whose {@code item.landed} message never reached the
 * split stage, and splits them (docs/DEPLOYMENT.md, STAGES.md §8). The
 * landing node sends the last item's message again after a restart, which
 * covers a crash between landing an item and announcing it. It can't cover
 * a broker that accepted a message and then lost it: a purged queue, a
 * lost broker node, a Kafka retention limit passed while the stage was
 * down. Such an item is safe in {@code landed_items}, but nothing would
 * split it.
 * <p>
 * So the stage compares the two itself, on the one pod that holds the
 * duties lease: every item of its intake fully landed (with a summary row)
 * more than {@code grace} ago and with no checkpoint is split, in the order
 * it was landed, with a {@code split.recovered} warning, so that the
 * broker problem is seen. The grace leaves time for a message on its way.
 * The first run looks at every landed item; later runs only at those
 * landed since the previous run's cutoff, less the grace again, in case
 * the two nodes' clocks differ.
 * <p>
 * An item another pod is splitting is left to it. If an item's message
 * turns up later, the item is only announced again.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SplitReconciler
{

    /** Why an item was recovered, on its audit event. */
    static final String REASON = "MESSAGE_MISSING";

    /** Logs each run, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(SplitReconciler.class);

    /** Tells which items have been started. */
    private final ItemCheckpoints checkpoints;

    /** Splits the missing items. */
    private final ItemSplitter splitter;

    /** The {@code landed_items} table. */
    private final LandedItemsTable landedItems;

    /** The intake the stage serves. */
    private final String intake;

    /** How long an item may wait for its message. */
    private final Duration grace;

    /** Gives the time. */
    private final Clock clock;

    /** The previous run's cutoff; null before the first run. */
    private Instant previousCutoff;

    /**
     * Creates a reconciler.
     *
     * @param checkpoints tells which items have been started
     * @param splitter    splits the missing items
     * @param landedItems the {@code landed_items} table
     * @param intake      the intake the stage serves
     * @param grace       how long an item may wait for its message
     * @param clock       gives the time
     */
    public SplitReconciler(ItemCheckpoints checkpoints, ItemSplitter splitter,
            LandedItemsTable landedItems, String intake, Duration grace,
            Clock clock)
    {
        this.checkpoints = checkpoints;
        this.splitter = splitter;
        this.landedItems = landedItems;
        this.intake = intake;
        this.grace = grace;
        this.clock = clock;
    }

    /**
     * Splits every landed item with no checkpoint for longer than the
     * grace.
     *
     * @return how many were split
     * @throws Exception if the landing table or checkpoints can't be read,
     *                   or an item can't be split; the next run tries
     *                   again, from the same point
     */
    public int reconcile() throws Exception
    {
        Instant cutoff = clock.instant().minus(grace);
        Table table;
        try
        {
            table = landedItems.get();
        }
        catch (IOException notYet)
        {
            // Nothing has been landed yet.
            return 0;
        }
        table.refresh();
        if (table.currentSnapshot() == null)
        {
            return 0;
        }
        Instant from = previousCutoff == null ? null
                : previousCutoff.minus(grace);
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<Record> read = IcebergGenerics.read(table)
                .where(filter(from, cutoff))
                .select(ITEM_ID, ITEM_DIGEST, LANDED_AT)
                .build())
        {
            for (Record row : read)
            {
                rows.add(row);
            }
        }
        Set<UUID> known = checkpoints.known(rows.stream()
                .map(row -> (UUID) row.getField(ITEM_ID)).toList());
        List<Record> missing = new ArrayList<>(rows.stream()
                .filter(row -> !known.contains((UUID) row.getField(ITEM_ID)))
                .toList());
        missing.sort(Comparator.comparing(row ->
                (OffsetDateTime) row.getField(LANDED_AT)));
        int split = 0;
        for (Record row : missing)
        {
            UUID itemId = (UUID) row.getField(ITEM_ID);
            Telemetry.get().auditWarn("split.recovered",
                    Fields.of(Attr.ITEM_ID, itemId).with(Attr.INTAKE, intake)
                            .with(Attr.REASON, REASON));
            try
            {
                splitter.split(itemId, (String) row.getField(ITEM_DIGEST));
                split++;
            }
            catch (LeaseHeldException held)
            {
                // Its message arrived meanwhile; another pod is on it.
            }
        }
        previousCutoff = cutoff;
        LOG.info("split.reconciled checked={} recovered={} full={}",
                rows.size(), split, from == null);
        return split;
    }

    /**
     * Which rows to look at: the intake's summary rows landed before the
     * cutoff, and, after the first run, not before {@code from}.
     *
     * @param from   the earliest landing time; null for every item
     * @param cutoff the latest landing time, not included
     * @return the filter
     */
    private Expression filter(Instant from, Instant cutoff)
    {
        Expression filter = Expressions.and(
                Expressions.equal(CHUNK_INDEX, SUMMARY_ROW),
                Expressions.equal(INTAKE, intake),
                Expressions.lessThan(LANDED_AT, micros(cutoff)));
        if (from != null)
        {
            filter = Expressions.and(filter,
                    Expressions.greaterThanOrEqual(LANDED_AT, micros(from)));
        }
        return filter;
    }

    /**
     * An instant as Iceberg timestamp microseconds.
     *
     * @param instant the instant
     * @return microseconds since the epoch
     */
    private static long micros(Instant instant)
    {
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
    }
}
