/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes a record stage's registry rows for items finished longer ago
 * than the customer's retention (docs/DEPLOYMENT.md "Registry retention",
 * {@link StageProperties.Retention}): each such item's per-record rows,
 * then its own row, a batch of items to a transaction. An item is
 * finished once complete ({@code completed_at} set) with none of its
 * failures open; failures and their events are never deleted, nor is
 * anything stored in the stage's tables. Runs on the pod that holds the
 * duties lease, at most once each {@code interval}, as one of its duties;
 * with no retention set it does nothing. Each run that deletes anything is
 * audited, {@code stage.registry_pruned}, with the counts and the cutoff,
 * never an item's ID.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RegistryPruner
{

    /** Logs runs, with counts only. */
    private static final Logger LOG =
            LoggerFactory.getLogger(RegistryPruner.class);

    /** The database. */
    private final DataSource dataSource;

    /** The stage, as its registry rows name it. */
    private final String stage;

    /** The per-record table, with its schema. */
    private final String records;

    /** The per-item table, with its schema. */
    private final String items;

    /** The failures table, with its schema. */
    private final String failures;

    /** The retention. */
    private final StageProperties.Retention retention;

    /** The time. */
    private final Clock clock;

    /** The longest a statement may run, in seconds. */
    private final int queryTimeoutSeconds;

    /** When the last run began; null before the first. */
    private Instant lastRun;

    /**
     * Creates the pruner.
     *
     * @param dataSource   the database
     * @param schema       the registry's schema
     * @param stage        the stage, as its rows name it
     * @param retention    the retention
     * @param clock        the time
     * @param queryTimeout the longest a statement may run
     */
    public RegistryPruner(DataSource dataSource, String schema, String stage,
            StageProperties.Retention retention, Clock clock,
            Duration queryTimeout)
    {
        this.dataSource = dataSource;
        this.stage = stage;
        this.records = schema + ".stage_record";
        this.items = schema + ".stage_item";
        this.failures = FailureStore.table(schema);
        this.retention = retention;
        this.clock = clock;
        this.queryTimeoutSeconds =
                (int) Math.max(1, queryTimeout.toSeconds());
    }

    /**
     * Prunes, if retention is set and the interval has passed since the
     * last run.
     *
     * @throws SQLException if the database fails; the next run tries
     *                      again
     */
    public void prune() throws SQLException
    {
        if (retention.forever())
        {
            return;
        }
        Instant now = clock.instant();
        if (lastRun != null
                && now.isBefore(lastRun.plus(retention.interval())))
        {
            return;
        }
        lastRun = now;
        pruneBefore(now.minus(retention.after()));
    }

    /**
     * Deletes the rows of every item finished before a time.
     *
     * @param cutoff the time
     * @return the items whose rows were deleted
     * @throws SQLException if the database fails
     */
    long pruneBefore(Instant cutoff) throws SQLException
    {
        long itemCount = 0;
        long recordCount = 0;
        while (true)
        {
            long[] done = pruneBatch(cutoff);
            itemCount += done[0];
            recordCount += done[1];
            if (done[0] < retention.batch()
                    || Thread.currentThread().isInterrupted())
            {
                break;
            }
        }
        if (itemCount > 0)
        {
            Telemetry.get().audit("stage.registry_pruned",
                    Fields.of(Attr.STAGE, stage).with(Attr.COUNT, itemCount));
        }
        LOG.info("stage.registry_pruned stage={} items={} records={}"
                + " retention_h={}", stage, itemCount, recordCount,
                retention.after().toHours());
        return itemCount;
    }

    /**
     * Deletes one batch of finished items' rows, in one transaction.
     *
     * @param cutoff finished before this
     * @return the items, and the per-record rows, deleted
     * @throws SQLException if the database fails
     */
    private long[] pruneBatch(Instant cutoff) throws SQLException
    {
        String pick = """
                SELECT i.item_id FROM %s i
                WHERE i.stage = ? AND i.completed_at IS NOT NULL
                  AND i.completed_at < ?
                  AND NOT EXISTS (SELECT 1 FROM %s f
                                  WHERE f.stage = i.stage
                                    AND f.item_id = i.item_id
                                    AND f.state IN %s)
                ORDER BY i.completed_at
                LIMIT ?
                FOR UPDATE SKIP LOCKED""".formatted(items, failures,
                FailureStore.State.OPEN_SQL);
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                List<UUID> ids = new ArrayList<>();
                try (PreparedStatement select =
                        connection.prepareStatement(pick))
                {
                    select.setQueryTimeout(queryTimeoutSeconds);
                    select.setString(1, stage);
                    select.setTimestamp(2, Timestamp.from(cutoff));
                    select.setInt(3, retention.batch());
                    try (ResultSet rows = select.executeQuery())
                    {
                        while (rows.next())
                        {
                            ids.add(rows.getObject(1, UUID.class));
                        }
                    }
                }
                long deleted = delete(connection, records, ids);
                delete(connection, items, ids);
                connection.commit();
                return new long[] {ids.size(), deleted};
            }
            catch (SQLException | RuntimeException failure)
            {
                connection.rollback();
                throw failure;
            }
        }
    }

    /**
     * Deletes the stage's rows of some items from a table.
     *
     * @param connection the transaction
     * @param table      the table
     * @param ids        the items
     * @return the rows deleted
     * @throws SQLException if the database fails
     */
    private long delete(Connection connection, String table, List<UUID> ids)
            throws SQLException
    {
        if (ids.isEmpty())
        {
            return 0;
        }
        long deleted = 0;
        try (PreparedStatement delete = connection.prepareStatement(
                "DELETE FROM " + table + " WHERE stage = ? AND item_id = ?"))
        {
            delete.setQueryTimeout(queryTimeoutSeconds);
            for (UUID id : ids)
            {
                delete.setString(1, stage);
                delete.setObject(2, id);
                delete.addBatch();
            }
            for (int count : delete.executeBatch())
            {
                deleted += Math.max(0, count);
            }
        }
        return deleted;
    }
}
