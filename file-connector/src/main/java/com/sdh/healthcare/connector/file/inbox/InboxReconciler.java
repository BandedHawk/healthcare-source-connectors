/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.inbox;

import static com.sdh.healthcare.connector.core.landing.LandingTables.CHUNK_INDEX;
import static com.sdh.healthcare.connector.core.landing.LandingTables.INTAKE;
import static com.sdh.healthcare.connector.core.landing.LandingTables.ITEM_DIGEST;
import static com.sdh.healthcare.connector.core.landing.LandingTables.ITEM_ID;
import static com.sdh.healthcare.connector.core.landing.LandingTables.ITEM_SIZE;
import static com.sdh.healthcare.connector.core.landing.LandingTables.KEK_REF;
import static com.sdh.healthcare.connector.core.landing.LandingTables.KIND;
import static com.sdh.healthcare.connector.core.landing.LandingTables.LANDED_AT;
import static com.sdh.healthcare.connector.core.landing.LandingTables.REPLACES_ITEM_ID;
import static com.sdh.healthcare.connector.core.landing.LandingTables.SUMMARY_ROW;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemLanded;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.inbox.Inbox;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.Snapshot;
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
 * inbox, and adds them (docs/DEPLOYMENT.md, design §5.3.12). The landing
 * node sends the last item's message again after a restart, which covers a
 * crash between landing an item and announcing it. It can't cover a broker
 * that accepted a message and then lost it: a purged queue, a lost broker
 * node, a Kafka retention limit passed while the splitter was down. Such an
 * item is safe in {@code landed_items}, but nothing would split it.
 * <p>
 * So the splitter compares the two itself: every item of its intake fully
 * landed (with a summary row) more than {@code grace} ago and not in the
 * inbox is added, in the order it was landed, with an
 * {@code inbox.recovered} warning, so that the broker problem is seen. The
 * grace leaves time for a message on its way. The first run looks at every
 * landed item; later runs only at those landed since the previous run's
 * cutoff, less the grace again, in case the two nodes' clocks differ. A
 * recovered item is split after the items already in the inbox.
 * <p>
 * Adding an item replaces nothing, and an item already in the inbox is
 * never added again. If its message turns up later, it is a repeat.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class InboxReconciler
{

    /** Logs each run, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(InboxReconciler.class);

    /** Why an item was recovered, on its audit event. */
    static final String REASON = "MESSAGE_MISSING";

    /** Where items wait. */
    private final Inbox inbox;

    /** The {@code landed_items} table. */
    private final LandedItemsTable landedItems;

    /** The intake the splitter serves. */
    private final String intake;

    /** The landing catalog's name, for the recovered items' messages. */
    private final String catalogName;

    /** The landing namespace. */
    private final String namespace;

    /** How long an item may wait for its message. */
    private final Duration grace;

    /** Gives the time. */
    private final Clock clock;

    /** The previous run's cutoff; null before the first run. */
    private Instant previousCutoff;

    /**
     * Creates a reconciler.
     *
     * @param inbox       where items wait
     * @param landedItems the {@code landed_items} table
     * @param intake      the intake the splitter serves
     * @param catalogName the landing catalog's name
     * @param namespace   the landing namespace
     * @param grace       how long an item may wait for its message
     * @param clock       gives the time
     */
    public InboxReconciler(Inbox inbox, LandedItemsTable landedItems,
            String intake, String catalogName, String namespace,
            Duration grace, Clock clock)
    {
        this.inbox = inbox;
        this.landedItems = landedItems;
        this.intake = intake;
        this.catalogName = catalogName;
        this.namespace = namespace;
        this.grace = grace;
        this.clock = clock;
    }

    /**
     * Adds every landed item missing from the inbox for longer than the
     * grace.
     *
     * @return how many were added
     * @throws Exception if the landing table can't be read or the inbox
     *                   written; the next run tries again, from the same
     *                   point
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
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null)
        {
            return 0;
        }
        Instant from = previousCutoff == null ? null
                : previousCutoff.minus(grace);
        List<Record> missing = new ArrayList<>();
        int checked = 0;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(filter(from, cutoff))
                .select(ITEM_ID, INTAKE, KIND, ITEM_SIZE, ITEM_DIGEST,
                        LANDED_AT, KEK_REF, REPLACES_ITEM_ID)
                .build())
        {
            for (Record row : rows)
            {
                checked++;
                if (!inbox.contains((UUID) row.getField(ITEM_ID)))
                {
                    missing.add(row);
                }
            }
        }
        missing.sort(Comparator.comparing(row ->
                (OffsetDateTime) row.getField(LANDED_AT)));
        for (Record row : missing)
        {
            ItemLanded message = message(row, snapshot.snapshotId());
            Telemetry.get().auditWarn("inbox.recovered",
                    Fields.of(Attr.ITEM_ID, message.itemId())
                            .with(Attr.INTAKE, intake)
                            .with(Attr.REASON, REASON));
            inbox.add(message);
        }
        previousCutoff = cutoff;
        LOG.info("inbox.reconciled checked={} recovered={} full={}", checked,
                missing.size(), from == null);
        return missing.size();
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
     * The {@code item.landed} message a summary row stands for, as the
     * landing node would have sent it.
     *
     * @param row        the summary row
     * @param snapshotId the landing table's snapshot it was read in
     * @return the message
     */
    private ItemLanded message(Record row, long snapshotId)
    {
        Instant landedAt = ((OffsetDateTime) row.getField(LANDED_AT))
                .toInstant();
        return new ItemLanded((UUID) row.getField(ITEM_ID), intake,
                (String) row.getField(KIND), (Long) row.getField(ITEM_SIZE),
                (String) row.getField(ITEM_DIGEST), landedAt,
                new StorageRef(catalogName, namespace,
                        LandingTables.LANDED_ITEMS,
                        landedAt.atOffset(ZoneOffset.UTC).toLocalDate(),
                        snapshotId, (String) row.getField(KEK_REF)),
                (UUID) row.getField(REPLACES_ITEM_ID));
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
