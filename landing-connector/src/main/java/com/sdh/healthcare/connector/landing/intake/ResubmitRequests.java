/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * Operators' requests to land an item again as a new item linked to it
 * (STAGES.md §13.5), in the lease database, beside the lease table: the
 * admin command takes no lease, so it can't write landing storage, and
 * leaves a request here for the node holding the intake's lease to carry
 * out ({@link Resubmitter}).
 * <p>
 * A request names the item to land again. The node notes the new item's ID
 * on it before landing, so a request tried again after a failure finds an
 * item it already landed rather than landing a second; and marks it
 * finished, landed or refused. At most one request per item is open at a
 * time.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ResubmitRequests
{

    /** The table's name, in the lease table's schema. */
    public static final String TABLE = "sdh_landing_resubmit";

    /** A lease table name, optionally with its schema. */
    private static final Pattern LEASE_TABLE = Pattern.compile(
            "([a-z_][a-z0-9_]{0,62}\\.)?[a-z_][a-z0-9_]{0,62}");

    /** The SQL state of a unique constraint broken. */
    private static final String UNIQUE_VIOLATION = "23505";

    /** How a request ended. */
    public enum Outcome
    {
        /** The item was landed again, as {@link Request#newItemId}. */
        LANDED,

        /** It couldn't be: see {@link Request#reason}. */
        REFUSED
    }

    /**
     * A request.
     *
     * @param requestId   the request's ID
     * @param intake      the intake that landed the item
     * @param itemId      the item to land again
     * @param operator    who asked
     * @param requestedAt when
     * @param newItemId   the new item's ID, once the node has chosen it;
     *                    null before
     * @param finishedAt  when the node finished it; null while open
     * @param outcome     how it ended; null while open
     * @param reason      why it was refused; null otherwise
     */
    public record Request(UUID requestId, String intake, UUID itemId,
            String operator, Instant requestedAt, UUID newItemId,
            Instant finishedAt, Outcome outcome, String reason)
    {

        /**
         * Whether the node has finished it.
         *
         * @return true once landed or refused
         */
        public boolean finished()
        {
            return finishedAt != null;
        }

        /**
         * The request with the new item's ID noted.
         *
         * @param id the new item's ID
         * @return the request
         */
        Request withNewItemId(UUID id)
        {
            return new Request(requestId, intake, itemId, operator,
                    requestedAt, id, finishedAt, outcome, reason);
        }
    }

    /** Thrown when an item already has an open request. */
    public static final class AlreadyOpenException extends Exception
    {

        /** Serialization version. */
        private static final long serialVersionUID = 1L;

        /** Creates the exception. */
        public AlreadyOpenException()
        {
            super("The item already has a request the node hasn't finished");
        }
    }

    /**
     * Thrown when the lease database can't be reached. Not an
     * {@link IllegalStateException}, which the node takes for a broken
     * invariant: a request not carried out stays open, and is tried again.
     */
    public static final class UnavailableException extends RuntimeException
    {

        /** Serialization version. */
        private static final long serialVersionUID = 1L;

        /**
         * Creates the exception.
         *
         * @param cause the database's error
         */
        UnavailableException(SQLException cause)
        {
            super("Resubmit request store unavailable", cause);
        }
    }

    /** Connections to the lease database. */
    private final DataSource dataSource;

    /** The table, with its schema if the lease table has one. */
    private final String table;

    /** How long a statement may run, in seconds. */
    private final int queryTimeoutSeconds;

    /**
     * Creates the store.
     *
     * @param dataSource   the lease database
     * @param leaseTable   the lease table, optionally with its schema: the
     *                     requests go in the same schema
     * @param queryTimeout how long a statement may run; at least a second
     * @throws IllegalArgumentException if the lease table name isn't a
     *                                  plain identifier
     */
    public ResubmitRequests(DataSource dataSource, String leaseTable,
            Duration queryTimeout)
    {
        if (!LEASE_TABLE.matcher(leaseTable).matches())
        {
            throw new IllegalArgumentException("Invalid lease table name");
        }
        int dot = leaseTable.indexOf('.');
        this.dataSource = dataSource;
        this.table = (dot < 0 ? "" : leaseTable.substring(0, dot + 1))
                + TABLE;
        this.queryTimeoutSeconds =
                (int) Math.max(1, queryTimeout.toSeconds());
    }

    /**
     * Creates the table if it isn't there.
     *
     * @throws IllegalStateException if it can't be created
     */
    public void createTableIfMissing()
    {
        String name = table.substring(table.indexOf('.') + 1);
        String ddl = """
                CREATE TABLE IF NOT EXISTS %1$s (
                  request_id   uuid        PRIMARY KEY,
                  intake       text        NOT NULL,
                  item_id      uuid        NOT NULL,
                  operator     text        NOT NULL,
                  requested_at timestamptz NOT NULL,
                  new_item_id  uuid,
                  finished_at  timestamptz,
                  outcome      text,
                  reason       text
                )""".formatted(table);
        String open = """
                CREATE UNIQUE INDEX IF NOT EXISTS %2$s_open
                    ON %1$s (intake, item_id) WHERE finished_at IS NULL"""
                .formatted(table, name);
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(queryTimeoutSeconds);
            statement.execute(ddl);
            statement.execute(open);
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the resubmit request table", failure);
        }
    }

    /**
     * Adds a request.
     *
     * @param intake   the intake that landed the item
     * @param itemId   the item to land again
     * @param operator who asks
     * @param id       the request's ID
     * @return the request
     * @throws AlreadyOpenException if the item already has an open request
     * @throws UnavailableException if the database can't be reached
     */
    public Request add(String intake, UUID itemId, String operator, UUID id)
            throws AlreadyOpenException
    {
        String sql = "INSERT INTO " + table + " (request_id, intake, item_id,"
                + " operator, requested_at) VALUES (?, ?, ?, ?,"
                + " clock_timestamp())";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement insert = connection.prepareStatement(sql))
        {
            insert.setQueryTimeout(queryTimeoutSeconds);
            insert.setObject(1, id);
            insert.setString(2, intake);
            insert.setObject(3, itemId);
            insert.setString(4, operator);
            insert.executeUpdate();
        }
        catch (SQLException failure)
        {
            if (UNIQUE_VIOLATION.equals(failure.getSQLState()))
            {
                throw new AlreadyOpenException();
            }
            throw unavailable(failure);
        }
        return find(id).orElseThrow();
    }

    /**
     * Reads a request.
     *
     * @param requestId the request
     * @return it; empty if there is none
     * @throws UnavailableException if the database can't be reached
     */
    public Optional<Request> find(UUID requestId)
    {
        List<Request> found = select("request_id = ?", 1, requestId);
        return found.isEmpty() ? Optional.empty() : Optional.of(found.get(0));
    }

    /**
     * The requests for an item, oldest first.
     *
     * @param intake the intake
     * @param itemId the item
     * @return its requests
     * @throws UnavailableException if the database can't be reached
     */
    public List<Request> forItem(String intake, UUID itemId)
    {
        return select("intake = ? AND item_id = ?", Integer.MAX_VALUE,
                intake, itemId);
    }

    /**
     * An intake's open requests, oldest first.
     *
     * @param intake the intake
     * @param limit  the most to return
     * @return the requests
     * @throws UnavailableException if the database can't be reached
     */
    public List<Request> open(String intake, int limit)
    {
        return select("finished_at IS NULL AND intake = ?", limit, intake);
    }

    /**
     * Notes the new item's ID on a request, before it is landed.
     *
     * @param request   the request
     * @param newItemId the new item's ID
     * @return the request with it
     * @throws UnavailableException  if the database can't be reached
     * @throws IllegalStateException if the request is finished
     */
    public Request assign(Request request, UUID newItemId)
    {
        update("SET new_item_id = ? WHERE request_id = ?"
                + " AND finished_at IS NULL", newItemId, request.requestId());
        return request.withNewItemId(newItemId);
    }

    /**
     * Marks a request landed.
     *
     * @param request the request, with its new item's ID
     * @throws UnavailableException  if the database can't be reached
     * @throws IllegalStateException if the request is finished
     */
    public void landed(Request request)
    {
        update("SET finished_at = clock_timestamp(), outcome = 'LANDED'"
                + " WHERE request_id = ? AND finished_at IS NULL"
                + " AND new_item_id IS NOT NULL", null, request.requestId());
    }

    /**
     * Marks a request refused.
     *
     * @param request the request
     * @param reason  why, such as {@code NOT_LANDED}; no PHI
     * @throws UnavailableException  if the database can't be reached
     * @throws IllegalStateException if the request is finished
     */
    public void refused(Request request, String reason)
    {
        String sql = "UPDATE " + table + " SET finished_at = clock_timestamp(),"
                + " outcome = 'REFUSED', reason = ? WHERE request_id = ?"
                + " AND finished_at IS NULL";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setString(1, reason);
            update.setObject(2, request.requestId());
            if (update.executeUpdate() != 1)
            {
                throw new IllegalStateException("The request is finished");
            }
        }
        catch (SQLException failure)
        {
            throw unavailable(failure);
        }
    }

    /**
     * Runs an update of one open request.
     *
     * @param tail   the statement after the table: parameters are the
     *               value, if not null, then the request's ID
     * @param value  the first parameter; null for none
     * @param id     the request's ID
     */
    private void update(String tail, UUID value, UUID id)
    {
        String sql = "UPDATE " + table + " " + tail;
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            int next = 1;
            if (value != null)
            {
                update.setObject(next++, value);
            }
            update.setObject(next, id);
            if (update.executeUpdate() != 1)
            {
                throw new IllegalStateException("The request is finished");
            }
        }
        catch (SQLException failure)
        {
            throw unavailable(failure);
        }
    }

    /**
     * Reads requests, oldest first.
     *
     * @param where      the condition
     * @param limit      the most to return
     * @param parameters the condition's parameters
     * @return the requests
     */
    private List<Request> select(String where, int limit,
            Object... parameters)
    {
        String sql = "SELECT request_id, intake, item_id, operator,"
                + " requested_at, new_item_id, finished_at, outcome, reason"
                + " FROM " + table + " WHERE " + where
                + " ORDER BY requested_at, request_id LIMIT " + limit;
        List<Request> requests = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            for (int i = 0; i < parameters.length; i++)
            {
                select.setObject(i + 1, parameters[i]);
            }
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    String outcome = rows.getString(8);
                    requests.add(new Request(rows.getObject(1, UUID.class),
                            rows.getString(2), rows.getObject(3, UUID.class),
                            rows.getString(4), instant(rows.getTimestamp(5)),
                            rows.getObject(6, UUID.class),
                            instant(rows.getTimestamp(7)),
                            outcome == null ? null : Outcome.valueOf(outcome),
                            rows.getString(9)));
                }
            }
        }
        catch (SQLException failure)
        {
            throw unavailable(failure);
        }
        return requests;
    }

    /**
     * A timestamp as an instant.
     *
     * @param timestamp the timestamp, or null
     * @return the instant, or null
     */
    private static Instant instant(Timestamp timestamp)
    {
        return timestamp == null ? null : timestamp.toInstant();
    }

    /**
     * The error for a database that can't be reached.
     *
     * @param failure the cause
     * @return the error
     */
    private static UnavailableException unavailable(SQLException failure)
    {
        return new UnavailableException(failure);
    }
}
