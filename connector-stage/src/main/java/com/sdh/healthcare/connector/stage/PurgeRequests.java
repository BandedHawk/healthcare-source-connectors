/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * The item purges asked for (CONTROL-API-DESIGN.md §11, HIPAA.md P4): a
 * row of {@code sdh_control_purges}, beside the scope's control state, for
 * each operation that asked to purge an item, approved by a second person.
 * The stage's maintenance carries each out ({@link PurgePass}) and records
 * here how far it got; the row is kept once it ends, as the record that
 * the item was purged, with who asked, when, and the ticket they gave.
 *
 * <p>A row names an item by its ID, never by a value of its document.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PurgeRequests
{

    /** A plain SQL identifier, for the schema. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");

    /** Postgres's code for a table that isn't there. */
    private static final String UNDEFINED_TABLE = "42P01";

    /** How long a statement may run, in seconds. */
    private static final int TIMEOUT_SECONDS = 10;

    /** The most purges a maintenance run takes at once. */
    static final int AT_ONCE = 100;

    /** The columns read, in {@link #read}'s order. */
    private static final String COLUMNS = "operation_id, item_id, state,"
            + " requested_by, requested_at, ticket, files, rows_purged,"
            + " failure, updated_at";

    /** Where a purge stands. */
    public enum State
    {
        /** Asked for and approved; no maintenance run has taken it yet. */
        REQUESTED,

        /** Being carried out by the stage's maintenance. */
        RUNNING,

        /** Done: the stage's tables hold no row of the item. */
        DONE,

        /** Given up, for the reason recorded. */
        FAILED;

        /**
         * The state as stored and answered.
         *
         * @return it, in lower case
         */
        public String wire()
        {
            return name().toLowerCase(Locale.ROOT);
        }

        /**
         * Whether it is still to finish.
         *
         * @return true if requested or running
         */
        public boolean active()
        {
            return this == REQUESTED || this == RUNNING;
        }

        /**
         * Reads a stored state.
         *
         * @param wire the state as stored
         * @return it
         */
        static State of(String wire)
        {
            return valueOf(wire.toUpperCase(Locale.ROOT));
        }
    }

    /**
     * An item's purge.
     *
     * @param id          the operation that asked for it
     * @param scope       the scope
     * @param itemId      the item
     * @param state       where it stands
     * @param requestedBy who asked for it
     * @param requestedAt when
     * @param ticket      the ticket it was asked under
     * @param files       data files rewritten by the runs it was in
     * @param rows        the item's rows purged from the stage's tables
     * @param failure     why it failed, by the failure's class; null unless
     *                    it did
     * @param updatedAt   when it last moved
     */
    public record Purge(String id, String scope, UUID itemId, State state,
            String requestedBy, Instant requestedAt, String ticket,
            long files, long rows, String failure, Instant updatedAt)
    {
    }

    /** Connections to the pipeline's database. */
    private final DataSource dataSource;

    /** The table, with its schema. */
    private final String table;

    /** The schema; null for none. */
    private final String schema;

    /** The scope read and written. */
    private final String scope;

    /**
     * Describes a scope's purges, without touching the database.
     *
     * @param dataSource connections to the pipeline's database
     * @param schema     the schema, as the control state's; null for none
     * @param scope      the scope, as the control state's
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier, or the scope is blank
     */
    public PurgeRequests(DataSource dataSource, String schema, String scope)
    {
        if (schema != null && !IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException("Invalid schema name");
        }
        if (scope == null || scope.isBlank())
        {
            throw new IllegalArgumentException("A control scope is required");
        }
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.schema = schema;
        this.table = (schema == null ? "" : schema + ".")
                + "sdh_control_purges";
        this.scope = scope;
    }

    /**
     * The purges of a scope's control state.
     *
     * @param dataSource connections to the pipeline's database
     * @param schema     the schema
     * @param state      the scope's control state
     * @return them
     */
    public static PurgeRequests of(DataSource dataSource, String schema,
            ControlState state)
    {
        return new PurgeRequests(dataSource, schema, state.scope());
    }

    /**
     * The scope read and written.
     *
     * @return it
     */
    public String scope()
    {
        return scope;
    }

    /**
     * Creates the table if it isn't there.
     *
     * @throws IllegalStateException if it can't be created
     */
    public void createTableIfMissing()
    {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(TIMEOUT_SECONDS);
            if (schema != null)
            {
                statement.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
            }
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      operation_id text        PRIMARY KEY,
                      scope        text        NOT NULL,
                      item_id      uuid        NOT NULL,
                      state        text        NOT NULL,
                      requested_by text        NOT NULL,
                      requested_at timestamptz NOT NULL,
                      ticket       text        NOT NULL,
                      files        bigint      NOT NULL,
                      rows_purged  bigint      NOT NULL,
                      failure      text,
                      updated_at   timestamptz NOT NULL
                    )""".formatted(table));
            statement.execute("CREATE INDEX IF NOT EXISTS"
                    + " sdh_control_purges_active ON " + table
                    + " (scope, state, requested_at)");
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Could not create the purge"
                    + " table", failure);
        }
    }

    /**
     * Asks for an item's purge, for the stage's maintenance to carry out.
     * Asking again with the same operation keeps the one asked for, so a
     * run taken over by another control pod doesn't ask twice.
     *
     * @param id     the operation asking for it
     * @param by     who asked for it
     * @param item   the item
     * @param ticket the ticket it is asked under
     * @return the purge
     * @throws IllegalArgumentException if the operation already asked for
     *                                  another item or scope
     * @throws IllegalStateException    if the database can't be reached
     */
    public Purge request(String id, String by, UUID item, String ticket)
    {
        createTableIfMissing();
        String sql = """
                INSERT INTO %s (operation_id, scope, item_id, state,
                       requested_by, requested_at, ticket, files,
                       rows_purged, failure, updated_at)
                VALUES (?, ?, ?, 'requested', ?, clock_timestamp(), ?, 0, 0,
                        NULL, clock_timestamp())
                ON CONFLICT (operation_id) DO NOTHING""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement insert = connection.prepareStatement(sql))
        {
            insert.setQueryTimeout(TIMEOUT_SECONDS);
            insert.setString(1, id);
            insert.setString(2, scope);
            insert.setObject(3, item);
            insert.setString(4, by);
            insert.setString(5, ticket);
            insert.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Purge requests unavailable",
                    failure);
        }
        Purge now = get(id).orElseThrow(() -> new IllegalStateException(
                "Purge requests unavailable"));
        if (!now.scope().equals(scope) || !now.itemId().equals(item))
        {
            throw new IllegalArgumentException("The operation asked to"
                    + " purge another item");
        }
        return now;
    }

    /**
     * A purge, by the operation that asked for it.
     *
     * @param id the operation
     * @return it; empty if there is none, or the table isn't there
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<Purge> get(String id)
    {
        List<Purge> found = select("SELECT scope, " + COLUMNS + " FROM "
                + table + " WHERE operation_id = ?", id);
        return found.stream().findFirst();
    }

    /**
     * The scope's purges still to finish, oldest first, at most
     * {@link #AT_ONCE}.
     *
     * @return them; none if the table isn't there
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Purge> active()
    {
        return select("SELECT scope, " + COLUMNS + " FROM " + table
                + " WHERE scope = ? AND state IN ('requested', 'running')"
                + " ORDER BY requested_at, operation_id LIMIT " + AT_ONCE,
                scope);
    }

    /**
     * The scope's latest purges, newest first.
     *
     * @param limit the most
     * @return them; none if the table isn't there
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Purge> recent(int limit)
    {
        return select("SELECT scope, " + COLUMNS + " FROM " + table
                + " WHERE scope = ? ORDER BY requested_at DESC, operation_id"
                + " LIMIT " + Math.max(1, limit), scope);
    }

    /**
     * Records a purge's progress: it is running, with more files and rows
     * done.
     *
     * @param id    the operation that asked for it
     * @param files more data files rewritten
     * @param rows  more of the item's rows purged
     * @return true if it was recorded: it is still to finish
     * @throws IllegalStateException if the database can't be reached
     */
    public boolean advanced(String id, long files, long rows)
    {
        String sql = """
                UPDATE %s
                   SET state = 'running', files = files + ?,
                       rows_purged = rows_purged + ?,
                       updated_at = clock_timestamp()
                 WHERE operation_id = ? AND scope = ?
                   AND state IN ('requested', 'running')"""
                .formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(TIMEOUT_SECONDS);
            update.setLong(1, files);
            update.setLong(2, rows);
            update.setString(3, id);
            update.setString(4, scope);
            return update.executeUpdate() == 1;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Purge requests unavailable",
                    failure);
        }
    }

    /**
     * Ends a purge, done or failed.
     *
     * @param id      the operation that asked for it
     * @param state   {@link State#DONE} or {@link State#FAILED}
     * @param failure why it failed, by the failure's class; null if done
     * @return true if it was recorded: it was still to finish
     * @throws IllegalArgumentException if the state isn't an end
     * @throws IllegalStateException    if the database can't be reached
     */
    public boolean ended(String id, State state, String failure)
    {
        if (state.active())
        {
            throw new IllegalArgumentException("Not an end: " + state);
        }
        String sql = """
                UPDATE %s
                   SET state = ?, failure = ?,
                       updated_at = clock_timestamp()
                 WHERE operation_id = ? AND scope = ?
                   AND state IN ('requested', 'running')"""
                .formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(TIMEOUT_SECONDS);
            update.setString(1, state.wire());
            update.setString(2, failure);
            update.setString(3, id);
            update.setString(4, scope);
            return update.executeUpdate() == 1;
        }
        catch (SQLException unreached)
        {
            throw new IllegalStateException("Purge requests unavailable",
                    unreached);
        }
    }

    /**
     * Reads purges.
     *
     * @param sql       the query, selecting the scope then {@link #COLUMNS}
     * @param parameter its one text parameter
     * @return them; none if the table isn't there
     * @throws IllegalStateException if the database can't be reached
     */
    private List<Purge> select(String sql, String parameter)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(TIMEOUT_SECONDS);
            select.setString(1, parameter);
            List<Purge> found = new ArrayList<>();
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.add(read(rows));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            if (UNDEFINED_TABLE.equals(failure.getSQLState()))
            {
                return List.of();
            }
            throw new IllegalStateException("Purge requests unavailable",
                    failure);
        }
    }

    /**
     * Reads a purge's row.
     *
     * @param rows the rows, at one
     * @return the purge
     * @throws SQLException if it can't be read
     */
    private static Purge read(ResultSet rows) throws SQLException
    {
        return new Purge(rows.getString(2), rows.getString(1),
                rows.getObject(3, UUID.class),
                State.of(rows.getString(4)), rows.getString(5),
                instant(rows.getTimestamp(6)), rows.getString(7),
                rows.getLong(8), rows.getLong(9), rows.getString(10),
                instant(rows.getTimestamp(11)));
    }

    /**
     * A stored time.
     *
     * @param at the timestamp; null if none
     * @return the instant; null if none
     */
    private static Instant instant(Timestamp at)
    {
        return at == null ? null : at.toInstant();
    }
}
