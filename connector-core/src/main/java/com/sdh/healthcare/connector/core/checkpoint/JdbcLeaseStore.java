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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * A PostgreSQL lease store, with one row per lease name. Every expiry check
 * uses the database clock ({@code clock_timestamp()}), so clock skew
 * between instances can't make two holders.
 * <p>
 * Renewals update one row and write nothing to Iceberg. The epoch only
 * ever rises: a release expires the row instead of deleting it.
 * <p>
 * A store made with a gathering limit over 1 takes, renews and releases
 * the leases asked for at the same time in one statement each ({@link
 * RegistryBatcher}; STAGES.md §18.3, part B), each lease still its own:
 * one held by another owner is refused to its caller alone.
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

    /** The most leases one gathered statement takes; 1 for none. */
    private final int gather;

    /** Gathers acquisitions, when gathering. */
    private final RegistryBatcher<Asked, Object> acquires;

    /** Gathers renewals, when gathering. */
    private final RegistryBatcher<Held, Boolean> renewals;

    /** Gathers releases, when gathering. */
    private final RegistryBatcher<Held, Boolean> releases;

    /**
     * A lease asked for.
     *
     * @param name   its name
     * @param owner  who asks
     * @param millis how long it is to last
     */
    private record Asked(String name, String owner, long millis)
    {
    }

    /**
     * A lease held, to renew or release.
     *
     * @param grant  the grant
     * @param millis how long a renewal is to last; ignored for a release
     */
    private record Held(LeaseGrant grant, long millis)
    {
    }

    /**
     * A lease another owner holds, as an acquisition found it.
     *
     * @param owner     its owner
     * @param expiresAt when it runs out
     */
    private record Refused(String owner, Instant expiresAt)
    {
    }

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
        this(dataSource, table, queryTimeout, 1);
    }

    /**
     * Creates a store that gathers leases asked for at once.
     *
     * @param dataSource   connections to the database
     * @param table        the lease table, optionally with its schema
     * @param queryTimeout how long a statement may run; at least a second
     * @param gather       the most leases one statement takes; 1 runs each
     *                     alone, as the store without it does
     * @throws IllegalArgumentException if the table name isn't a plain
     *                                  identifier, or the limit is under 1
     */
    public JdbcLeaseStore(DataSource dataSource, String table,
            Duration queryTimeout, int gather)
    {
        if (!IDENTIFIER.matcher(table).matches())
        {
            throw new IllegalArgumentException("Invalid lease table name");
        }
        this.dataSource = dataSource;
        this.table = table;
        this.queryTimeoutSeconds =
                (int) Math.max(1, queryTimeout.toSeconds());
        this.gather = gather;
        this.acquires = new RegistryBatcher<>(gather, Asked::name,
                this::acquireAll);
        Function<Held, Object> byName = held -> held.grant().leaseName();
        this.renewals = new RegistryBatcher<>(gather, byName,
                held -> updateAll(held, true));
        this.releases = new RegistryBatcher<>(gather, byName,
                held -> updateAll(held, false));
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
        if (gather > 1)
        {
            Object got = acquires.run(
                    new Asked(leaseName, owner, ttl.toMillis()));
            if (got instanceof Refused refused)
            {
                throw new LeaseHeldException(refused.owner(),
                        refused.expiresAt());
            }
            return (LeaseGrant) got;
        }
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
        if (gather > 1)
        {
            return renewals.run(new Held(grant, ttl.toMillis()));
        }
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
        if (gather > 1)
        {
            releases.run(new Held(grant, 1));
            return;
        }
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
    @Override
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
     * How many leases whose names start with a prefix are held now, live:
     * a stage's item leases, for its drain (CONTROL-API-DESIGN.md §9).
     *
     * @param prefix the names' start, such as {@code split/<connector>/}
     * @return how many; 0 if the table isn't there
     * @throws IllegalStateException if the database can't be reached
     */
    public long live(String prefix)
    {
        return live(prefix, null);
    }

    /**
     * How many leases whose names start with a prefix are held now, live,
     * less one lease that shares the prefix but isn't an item's: a stage's
     * duties lease, held all the time a pod runs.
     *
     * @param prefix the names' start, such as {@code split/<connector>/}
     * @param except the one lease not counted; null for none
     * @return how many; 0 if the table isn't there
     * @throws IllegalStateException if the database can't be reached
     */
    public long live(String prefix, String except)
    {
        String sql = """
                SELECT count(*) FROM %s
                 WHERE name LIKE ? ESCAPE '!' AND name <> ?
                   AND expires_at > clock_timestamp()""".formatted(table);
        String pattern = prefix.replace("!", "!!").replace("%", "!%")
                .replace("_", "!_") + "%";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, pattern);
            select.setString(2, except == null ? "" : except);
            try (ResultSet rows = select.executeQuery())
            {
                rows.next();
                return rows.getLong(1);
            }
        }
        catch (SQLException failure)
        {
            if (UNDEFINED_TABLE.equals(failure.getSQLState()))
            {
                return 0;
            }
            throw new IllegalStateException("Lease store unavailable",
                    failure);
        }
    }

    /**
     * Takes leases in one statement: each new one inserted, each expired one
     * taken over with its epoch raised, each live one left, and its owner
     * and expiry read for its caller. Locks are taken in name order, so two
     * pods' statements can't deadlock.
     *
     * @param asked the leases asked for, each name once
     * @return for each, its {@link LeaseGrant}, or a {@link Refused}
     * @throws IllegalStateException if the database can't be reached
     */
    private List<Object> acquireAll(List<Asked> asked)
    {
        String sql = """
                INSERT INTO %1$s AS l
                       (name, owner, epoch, expires_at, acquired_at,
                        renewed_at)
                SELECT v.name, v.owner, 1,
                       clock_timestamp() + v.millis * interval '1 millisecond',
                       clock_timestamp(), clock_timestamp()
                  FROM unnest(?::text[], ?::text[], ?::bigint[])
                       AS v(name, owner, millis)
                 ORDER BY v.name
                ON CONFLICT (name) DO UPDATE
                   SET owner = EXCLUDED.owner,
                       epoch = l.epoch + 1,
                       expires_at = EXCLUDED.expires_at,
                       acquired_at = EXCLUDED.acquired_at,
                       renewed_at = EXCLUDED.renewed_at
                 WHERE l.expires_at <= clock_timestamp()
                RETURNING name, owner, epoch, expires_at""".formatted(table);
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(true);
            Map<String, Object> got = new HashMap<>();
            try (PreparedStatement insert = connection.prepareStatement(sql))
            {
                insert.setQueryTimeout(queryTimeoutSeconds);
                insert.setArray(1, connection.createArrayOf("text",
                        asked.stream().map(Asked::name).toArray()));
                insert.setArray(2, connection.createArrayOf("text",
                        asked.stream().map(Asked::owner).toArray()));
                insert.setArray(3, connection.createArrayOf("bigint",
                        asked.stream().map(Asked::millis).toArray()));
                try (ResultSet rows = insert.executeQuery())
                {
                    while (rows.next())
                    {
                        got.put(rows.getString(1), new LeaseGrant(
                                rows.getString(1), rows.getString(2),
                                rows.getLong(3), instant(rows, 4)));
                    }
                }
            }
            List<String> refused = asked.stream().map(Asked::name)
                    .filter(name -> !got.containsKey(name)).toList();
            if (!refused.isEmpty())
            {
                try (PreparedStatement select = connection.prepareStatement(
                        "SELECT name, owner, expires_at FROM " + table
                                + " WHERE name = ANY (?)"))
                {
                    select.setQueryTimeout(queryTimeoutSeconds);
                    select.setArray(1, connection.createArrayOf("text",
                            refused.toArray()));
                    try (ResultSet rows = select.executeQuery())
                    {
                        while (rows.next())
                        {
                            got.put(rows.getString(1), new Refused(
                                    rows.getString(2), instant(rows, 3)));
                        }
                    }
                }
            }
            return asked.stream().map(one -> got.getOrDefault(one.name(),
                    new Refused("unknown", Instant.EPOCH))).toList();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Lease store unavailable",
                    failure);
        }
    }

    /**
     * Renews or releases leases in one statement, each only if its row is
     * still the grant's (name, owner and epoch), locking the rows in name
     * order first.
     *
     * @param held  the leases, each name once
     * @param renew true to renew them; false to release them
     * @return for each, whether its row was the grant's and changed
     * @throws IllegalStateException if the database can't be reached
     */
    private List<Boolean> updateAll(List<Held> held, boolean renew)
    {
        String expiry = renew
                ? "clock_timestamp() + v.millis * interval '1 millisecond'"
                : "clock_timestamp() - interval '1 millisecond'";
        String renewed = renew ? ", renewed_at = clock_timestamp()" : "";
        String sql = """
                WITH v AS (
                    SELECT * FROM unnest(?::text[], ?::text[], ?::bigint[],
                                         ?::bigint[])
                           AS v(name, owner, epoch, millis)),
                locked AS (
                    SELECT l.name FROM %1$s l
                      JOIN v ON l.name = v.name AND l.owner = v.owner
                            AND l.epoch = v.epoch
                     ORDER BY l.name
                       FOR UPDATE OF l)
                UPDATE %1$s AS l
                   SET expires_at = %2$s%3$s
                  FROM v
                 WHERE l.name = v.name AND l.owner = v.owner
                   AND l.epoch = v.epoch
                   AND l.name IN (SELECT name FROM locked)
                RETURNING l.name""".formatted(table, expiry, renewed);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            connection.setAutoCommit(true);
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setArray(1, connection.createArrayOf("text",
                    held.stream().map(one -> one.grant().leaseName())
                            .toArray()));
            update.setArray(2, connection.createArrayOf("text",
                    held.stream().map(one -> one.grant().owner())
                            .toArray()));
            update.setArray(3, connection.createArrayOf("bigint",
                    held.stream().map(one -> one.grant().epoch())
                            .toArray()));
            update.setArray(4, connection.createArrayOf("bigint",
                    held.stream().map(Held::millis).toArray()));
            Set<String> changed = new HashSet<>();
            try (ResultSet rows = update.executeQuery())
            {
                while (rows.next())
                {
                    changed.add(rows.getString(1));
                }
            }
            return held.stream().map(one -> changed.contains(
                    one.grant().leaseName())).toList();
        }
        catch (SQLException failure)
        {
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
