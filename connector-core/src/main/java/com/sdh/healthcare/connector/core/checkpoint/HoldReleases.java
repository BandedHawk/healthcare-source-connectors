/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.failure.FailureReason;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * Operators' requests to end the hold on a split item held across versions
 * (STAGES.md §14.3, §15.6e), in {@code <schema>.split_release}. The
 * {@code release} command records a request; the split stage's pods apply
 * it under the item's lease, so no output is made outside a pod, and note
 * what it came to. One request is pending per item. The reason given for
 * closing an item is encrypted as a decline's is, bound to the item and the
 * request's number.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class HoldReleases
{

    /**
     * The record number a release's reason is bound to: none a record or an
     * item as a whole has, so a reason can't be moved to or from a failure.
     */
    public static final long REASON_SEQ = Long.MIN_VALUE;

    /** A plain SQL identifier. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[a-z_][a-z0-9_]*");

    /** What an operator asked for. */
    public enum Action
    {
        /** Resume the item under the version running now. */
        RESUME,
        /** End it, {@code CLOSED}, to be landed again. */
        CLOSE
    }

    /** What a request came to. */
    public enum State
    {
        /** Not yet applied. */
        PENDING,
        /** Applied. */
        DONE,
        /** Not applied: the item wasn't held when a pod came to it. */
        REFUSED
    }

    /** Connections to the database. */
    private final DataSource dataSource;

    /** The table, with its schema. */
    private final String table;

    /** How long a statement may run, in seconds. */
    private final int timeout;

    /**
     * Creates the store.
     *
     * @param dataSource   connections to the database
     * @param schema       the split stage's registry schema
     * @param queryTimeout how long a statement may run
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier
     */
    public HoldReleases(DataSource dataSource, String schema,
            Duration queryTimeout)
    {
        if (!IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException("Invalid schema name");
        }
        this.dataSource = dataSource;
        this.table = schema + ".split_release";
        this.timeout = (int) Math.max(1, queryTimeout.toSeconds());
    }

    /**
     * Creates the table if it isn't there.
     *
     * @throws IllegalStateException if it can't be created
     */
    public void createTableIfMissing()
    {
        String ddl = """
                CREATE TABLE IF NOT EXISTS %s (
                  item_id      uuid        NOT NULL,
                  request      integer     NOT NULL,
                  action       text        NOT NULL,
                  state        text        NOT NULL,
                  requested_at timestamptz NOT NULL,
                  requested_by text        NOT NULL,
                  reason_enc   bytea,
                  reason_key   bytea,
                  reason_kek   text,
                  done_at      timestamptz,
                  from_version text,
                  to_version   text,
                  refusal      text,
                  PRIMARY KEY (item_id, request)
                )""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(timeout);
            statement.execute("CREATE SCHEMA IF NOT EXISTS "
                    + table.substring(0, table.indexOf('.')));
            statement.execute(ddl);
            statement.execute("CREATE INDEX IF NOT EXISTS"
                    + " split_release_pending ON " + table
                    + " (state, requested_at)");
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the release table", failure);
        }
    }

    /**
     * A request, as recorded.
     *
     * @param itemId      the item
     * @param request     its number for the item, from 1
     * @param action      what was asked
     * @param requestedBy who asked
     * @param requestedAt when
     */
    public record Request(UUID itemId, int request, Action action,
            String requestedBy, Instant requestedAt)
    {
    }

    /**
     * Thrown when a request can't be recorded: one is already pending for
     * the item.
     */
    public static final class RefusedException extends RuntimeException
    {

        /** Serial version. */
        private static final long serialVersionUID = 1L;

        /**
         * Creates the exception.
         *
         * @param message why
         */
        public RefusedException(String message)
        {
            super(message);
        }
    }

    /**
     * Records a request. Audited {@code split.release_requested}, with no
     * reason.
     *
     * @param itemId   the item
     * @param action   what is asked
     * @param operator who asks
     * @param reason   the reason, for {@code CLOSE}; null for none
     * @param keys     wraps the reason's data key; null with no reason
     * @param keyRef   the stage's records KEK; null with no reason
     * @return the request's number
     * @throws RefusedException      if one is already pending for the item
     * @throws IllegalStateException if the database can't be reached
     */
    public int request(UUID itemId, Action action, String operator,
            String reason, KeyProvider keys, String keyRef)
    {
        String pending = """
                SELECT 1 FROM %s WHERE item_id = ? AND state = 'PENDING'
                   FOR UPDATE""".formatted(table);
        String next = """
                SELECT COALESCE(max(request), 0) + 1 FROM %s
                 WHERE item_id = ?""".formatted(table);
        String insert = """
                INSERT INTO %s (item_id, request, action, state,
                       requested_at, requested_by, reason_enc, reason_key,
                       reason_kek)
                VALUES (?, ?, ?, 'PENDING', clock_timestamp(), ?, ?, ?, ?)"""
                .formatted(table);
        int number;
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                // Two requests at once for one item: the key makes the
                // second fail, as a refusal would.
                try (PreparedStatement select =
                        connection.prepareStatement(pending))
                {
                    select.setQueryTimeout(timeout);
                    select.setObject(1, itemId);
                    try (ResultSet rows = select.executeQuery())
                    {
                        if (rows.next())
                        {
                            throw new RefusedException("A release of this"
                                    + " item is already pending");
                        }
                    }
                }
                try (PreparedStatement select =
                        connection.prepareStatement(next))
                {
                    select.setQueryTimeout(timeout);
                    select.setObject(1, itemId);
                    try (ResultSet rows = select.executeQuery())
                    {
                        rows.next();
                        number = rows.getInt(1);
                    }
                }
                FailureReason sealed = reason == null ? null
                        : FailureReason.seal(keys, keyRef, itemId,
                                REASON_SEQ, number, reason);
                try (PreparedStatement add =
                        connection.prepareStatement(insert))
                {
                    add.setQueryTimeout(timeout);
                    add.setObject(1, itemId);
                    add.setInt(2, number);
                    add.setString(3, action.name());
                    add.setString(4, operator);
                    add.setBytes(5, sealed == null ? null : sealed.sealed());
                    add.setBytes(6, sealed == null ? null : sealed.key());
                    add.setString(7, sealed == null ? null : sealed.keyRef());
                    add.executeUpdate();
                }
                connection.commit();
            }
            catch (SQLException | RuntimeException failure)
            {
                connection.rollback();
                throw failure;
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Release store unavailable",
                    failure);
        }
        Telemetry.get().audit("split.release_requested",
                Fields.of(Attr.ITEM_ID, itemId)
                        .with(Attr.ACTION, action.name())
                        .with(Attr.OPERATOR, operator));
        return number;
    }

    /**
     * The requests not yet applied, oldest first.
     *
     * @param limit the most returned
     * @return the requests
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Request> pending(int limit)
    {
        String sql = """
                SELECT item_id, request, action, requested_by, requested_at
                  FROM %s WHERE state = 'PENDING'
                 ORDER BY requested_at LIMIT ?""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setInt(1, limit);
            List<Request> found = new ArrayList<>();
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.add(new Request(rows.getObject(1, UUID.class),
                            rows.getInt(2), Action.valueOf(rows.getString(3)),
                            rows.getString(4),
                            rows.getTimestamp(5).toInstant()));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Release store unavailable",
                    failure);
        }
    }

    /**
     * Whether a request is still pending: read again once its item's lease
     * is held, as another pod may have applied it since it was listed.
     *
     * @param request the request, as listed
     * @return true if it is still {@code PENDING}
     * @throws IllegalStateException if the database can't be reached
     */
    public boolean stillPending(Request request)
    {
        String sql = """
                SELECT 1 FROM %s WHERE item_id = ? AND request = ?
                   AND state = 'PENDING'""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setObject(1, request.itemId());
            select.setInt(2, request.request());
            try (ResultSet rows = select.executeQuery())
            {
                return rows.next();
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Release store unavailable",
                    failure);
        }
    }

    /**
     * Notes what a request came to, once the item's checkpoint is written.
     *
     * @param request     the request
     * @param state       {@code DONE} or {@code REFUSED}
     * @param fromVersion the version the item was held under; null if
     *                    refused
     * @param toVersion   the version it resumes under; null unless resumed
     * @param refusal     why it was refused; null otherwise
     * @throws IllegalStateException if the database can't be reached
     */
    public void finish(Request request, State state, String fromVersion,
            String toVersion, String refusal)
    {
        String sql = """
                UPDATE %s SET state = ?, done_at = clock_timestamp(),
                       from_version = ?, to_version = ?, refusal = ?
                 WHERE item_id = ? AND request = ? AND state = 'PENDING'"""
                .formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(timeout);
            update.setString(1, state.name());
            update.setString(2, fromVersion);
            update.setString(3, toVersion);
            update.setString(4, refusal);
            update.setObject(5, request.itemId());
            update.setInt(6, request.request());
            update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Release store unavailable",
                    failure);
        }
    }

    /**
     * A request, with what it came to, without its reason.
     *
     * @param request     the request
     * @param state       what it came to
     * @param doneAt      when it was applied or refused; null if pending
     * @param fromVersion the version the item was held under
     * @param toVersion   the version it resumed under
     * @param refusal     why it was refused
     * @param reasoned    whether a reason was given
     */
    public record Listed(Request request, State state, Instant doneAt,
            String fromVersion, String toVersion, String refusal,
            boolean reasoned)
    {
    }

    /**
     * An item's requests, oldest first.
     *
     * @param itemId the item
     * @return its requests
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Listed> list(UUID itemId)
    {
        String sql = """
                SELECT request, action, requested_by, requested_at, state,
                       done_at, from_version, to_version, refusal,
                       reason_enc IS NOT NULL
                  FROM %s WHERE item_id = ? ORDER BY request"""
                .formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setObject(1, itemId);
            List<Listed> found = new ArrayList<>();
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    java.sql.Timestamp done = rows.getTimestamp(6);
                    found.add(new Listed(new Request(itemId, rows.getInt(1),
                            Action.valueOf(rows.getString(2)),
                            rows.getString(3),
                            rows.getTimestamp(4).toInstant()),
                            State.valueOf(rows.getString(5)),
                            done == null ? null : done.toInstant(),
                            rows.getString(7), rows.getString(8),
                            rows.getString(9), rows.getBoolean(10)));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Release store unavailable",
                    failure);
        }
    }
}
