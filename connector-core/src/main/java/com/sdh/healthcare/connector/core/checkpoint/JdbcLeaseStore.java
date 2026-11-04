/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * A PostgreSQL lease store, with one row per lease name. Every expiry check
 * uses the database clock ({@code clock_timestamp()}), so clock skew
 * between instances can't make two holders.
 * <p>
 * Renewals update one row and write nothing to Iceberg. The epoch only
 * ever rises: a release expires the row instead of deleting it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class JdbcLeaseStore implements LeaseStore
{

    /** A table name, optionally with its schema. */
    private static final Pattern IDENTIFIER = Pattern.compile(
            "[a-z_][a-z0-9_]{0,62}(\\.[a-z_][a-z0-9_]{0,62})?");

    /** The SQL state of a missing table. */
    private static final String UNDEFINED_TABLE = "42P01";

    /** Connections to the database. */
    private final DataSource dataSource;

    /** The lease table. */
    private final String table;

    /** How long a statement may run. */
    private final int queryTimeoutSeconds;

    /**
     * Creates a store.
     *
     * @param dataSource   connections to the database
     * @param table        the lease table, optionally with its schema
     * @param queryTimeout how long a statement may run; at least a second
     * @throws IllegalArgumentException if the table name isn't a plain
     *                                  identifier
     */
    public JdbcLeaseStore(DataSource dataSource, String table,
            Duration queryTimeout)
    {
        if (!IDENTIFIER.matcher(table).matches())
        {
            throw new IllegalArgumentException("Invalid lease table name");
        }
        this.dataSource = dataSource;
        this.table = table;
        this.queryTimeoutSeconds =
                (int) Math.max(1, queryTimeout.toSeconds());
    }

    /**
     * Creates the lease table if it isn't there.
     *
     * @throws IllegalStateException if it can't be created
     */
    public void createTableIfMissing()
    {
        String ddl = """
                CREATE TABLE IF NOT EXISTS %s (
                  name        text PRIMARY KEY,
                  owner       text        NOT NULL,
                  epoch       bigint      NOT NULL,
                  expires_at  timestamptz NOT NULL,
                  acquired_at timestamptz NOT NULL,
                  renewed_at  timestamptz NOT NULL
                )""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(queryTimeoutSeconds);
            statement.execute(ddl);
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Could not create lease table",
                    failure);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the database can't be reached
     */
    @Override
    public LeaseGrant acquire(String leaseName, String owner, Duration ttl)
            throws LeaseHeldException
    {
        // Insert a new lease, or take over an expired one with epoch + 1. A
        // live lease is left untouched and no row is returned. ON CONFLICT
        // checks the WHERE clause again under a row lock, so two callers at
        // once can't both win.
        String sql = """
                INSERT INTO %1$s AS l
                       (name, owner, epoch, expires_at, acquired_at,
                        renewed_at)
                VALUES (?, ?, 1,
                        clock_timestamp() + ? * interval '1 millisecond',
                        clock_timestamp(), clock_timestamp())
                ON CONFLICT (name) DO UPDATE
                   SET owner = EXCLUDED.owner,
                       epoch = l.epoch + 1,
                       expires_at = EXCLUDED.expires_at,
                       acquired_at = EXCLUDED.acquired_at,
                       renewed_at = EXCLUDED.renewed_at
                 WHERE l.expires_at <= clock_timestamp()
                RETURNING epoch, expires_at""".formatted(table);
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(true);
            try (PreparedStatement insert = connection.prepareStatement(sql))
            {
                insert.setQueryTimeout(queryTimeoutSeconds);
                insert.setString(1, leaseName);
                insert.setString(2, owner);
                insert.setLong(3, ttl.toMillis());
                try (ResultSet rows = insert.executeQuery())
                {
                    if (rows.next())
                    {
                        return new LeaseGrant(leaseName, owner,
                                rows.getLong(1), instant(rows, 2));
                    }
                }
            }
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT owner, expires_at FROM " + table
                            + " WHERE name = ?"))
            {
                select.setQueryTimeout(queryTimeoutSeconds);
                select.setString(1, leaseName);
                try (ResultSet rows = select.executeQuery())
                {
                    if (rows.next())
                    {
                        throw new LeaseHeldException(rows.getString(1),
                                instant(rows, 2));
                    }
                }
            }
            throw new LeaseHeldException("unknown", Instant.EPOCH);
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Lease store unavailable",
                    failure);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the database can't be reached
     */
    @Override
    public boolean renew(LeaseGrant grant, Duration ttl)
    {
        String sql = """
                UPDATE %s
                   SET expires_at = clock_timestamp()
                                    + ? * interval '1 millisecond',
                       renewed_at = clock_timestamp()
                 WHERE name = ? AND owner = ? AND epoch = ?"""
                .formatted(table);
        return update(sql, ttl.toMillis(), grant) == 1;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the database can't be reached
     */
    @Override
    public void release(LeaseGrant grant)
    {
        String sql = """
                UPDATE %s
                   SET expires_at = clock_timestamp()
                                    - ? * interval '1 millisecond'
                 WHERE name = ? AND owner = ? AND epoch = ?"""
                .formatted(table);
        update(sql, 1L, grant);
    }

    /**
     * Reads a lease without taking or changing it.
     *
     * @param leaseName the lease's name
     * @return its row; empty if it has never been held, or the table isn't
     *         there
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<LeaseStatus> inspect(String leaseName)
    {
        String sql = """
                SELECT owner, epoch, acquired_at, renewed_at, expires_at,
                       (extract(epoch FROM expires_at - clock_timestamp())
                        * 1000)::bigint
                  FROM %s WHERE name = ?""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, leaseName);
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    return Optional.empty();
                }
                return Optional.of(new LeaseStatus(leaseName,
                        rows.getString(1), rows.getLong(2), instant(rows, 3),
                        instant(rows, 4), instant(rows, 5),
                        Duration.ofMillis(rows.getLong(6))));
            }
        }
        catch (SQLException failure)
        {
            if (UNDEFINED_TABLE.equals(failure.getSQLState()))
            {
                return Optional.empty();
            }
            throw new IllegalStateException("Lease store unavailable",
                    failure);
        }
    }

    /**
     * Runs an update of one grant's row.
     *
     * @param sql    the update, with parameters for the milliseconds, then
     *               the name, owner and epoch
     * @param millis the milliseconds to add or take away
     * @param grant  the grant
     * @return how many rows changed
     * @throws IllegalStateException if the database can't be reached
     */
    private int update(String sql, long millis, LeaseGrant grant)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            connection.setAutoCommit(true);
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setLong(1, millis);
            update.setString(2, grant.leaseName());
            update.setString(3, grant.owner());
            update.setLong(4, grant.epoch());
            return update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Lease store unavailable",
                    failure);
        }
    }

    /**
     * Reads a timestamp column.
     *
     * @param rows   the result
     * @param column the column, from 1
     * @return the instant
     * @throws SQLException if it can't be read
     */
    private static Instant instant(ResultSet rows, int column)
            throws SQLException
    {
        return rows.getObject(column, OffsetDateTime.class).toInstant();
    }
}
