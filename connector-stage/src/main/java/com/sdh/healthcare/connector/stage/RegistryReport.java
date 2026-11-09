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
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * What a stage's registry (STAGES.md §5, §6) says about its work, for an
 * operator's status: read only, with no owner, and never creating tables.
 * It reads states, counts and times; never a record's ID, digest or
 * message.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RegistryReport
{

    /** A schema name: a plain identifier, as it is put in the SQL. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[a-z_][a-z0-9_]{0,62}");

    /** How long a query may run, in seconds. */
    private static final int QUERY_TIMEOUT_SECONDS = 10;

    /** The database. */
    private final DataSource dataSource;

    /** The stage, such as {@code convert}. */
    private final String stage;

    /** The records table, with its schema. */
    private final String records;

    /** The reconciler's watermark table, with its schema. */
    private final String reconcile;

    /** The items table, with its schema. */
    private final String items;

    /**
     * An item not yet complete, or one asked for.
     *
     * @param itemId      the item
     * @param expected    its record count, from the item message of the
     *                    stage before; -1 until that message is received
     * @param done        its records stored
     * @param failed      those of them stored as failed
     * @param completedAt when its item-complete message was sent; null if
     *                    it hasn't been
     * @param completion  how many times the stage has completed it
     *                    (STAGES.md §15.6e)
     */
    public record Item(UUID itemId, long expected, long done, long failed,
            Instant completedAt, int completion)
    {
    }

    /**
     * The records being worked on now.
     *
     * @param claimed claimed and not yet stored
     * @param expired of them, those whose claim has expired: their worker
     *                stopped, and the next delivery takes them over
     */
    public record Claims(long claimed, long expired)
    {
    }

    /**
     * Reads a stage's registry.
     *
     * @param dataSource the lease database
     * @param schema     the registry's schema
     * @param stage      the stage
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier
     */
    public RegistryReport(DataSource dataSource, String schema, String stage)
    {
        if (!IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException("Invalid registry schema name");
        }
        this.dataSource = dataSource;
        this.stage = stage;
        this.records = schema + ".stage_record";
        this.items = schema + ".stage_item";
        this.reconcile = schema + ".stage_reconcile";
    }

    /**
     * How far the stage's reconciler has checked (STAGES.md §9), and how
     * long ago that is, by the database's clock.
     *
     * @param checkedTo every record committed before it has been checked
     * @param behind    how long before now that is
     */
    public record Reconciled(Instant checkedTo, Duration behind)
    {
    }

    /**
     * How far the stage's reconciler has checked.
     *
     * @return it; empty if it has never checked a day, or the registry is
     *         from before the watermark and the stage hasn't started since
     */
    public Optional<Reconciled> reconciled()
    {
        // The stage is bound first, as in every query here.
        boolean exists = query("SELECT ?::text IS NOT NULL"
                + " AND to_regclass(?) IS NOT NULL",
                statement -> statement.setString(2, reconcile),
                result -> result.next() && result.getBoolean(1));
        if (!exists)
        {
            return Optional.empty();
        }
        return query("SELECT checked_to, extract(epoch FROM now()"
                + " - checked_to) FROM " + reconcile + " WHERE stage = ?",
                statement ->
                {
                }, result -> result.next()
                        ? Optional.of(new Reconciled(
                                result.getTimestamp(1).toInstant(),
                                Duration.ofMillis(Math.round(
                                        result.getDouble(2) * 1000))))
                        : Optional.empty());
    }

    /**
     * The items not yet complete, by item ID.
     *
     * @param limit the most to return
     * @return the items
     */
    public List<Item> incomplete(int limit)
    {
        return items("SELECT item_id, expected, done, failed, completed_at,"
                + " completion"
                + " FROM " + items + " WHERE stage = ? AND completed_at IS NULL"
                + " ORDER BY item_id LIMIT " + Math.max(0, limit));
    }

    /**
     * One item.
     *
     * @param itemId the item
     * @return it; empty if the stage has none of its records yet
     */
    public Optional<Item> item(UUID itemId)
    {
        return query("SELECT item_id, expected, done, failed, completed_at,"
                + " completion"
                + " FROM " + items + " WHERE stage = ? AND item_id = ?",
                statement -> statement.setObject(2, itemId),
                result -> result.next()
                        ? Optional.of(item(result)) : Optional.empty());
    }

    /**
     * The item's total message, the {@code item.split} or its counterpart
     * as received, still sealed: for a replay of a completed item
     * (STAGES.md §13.2).
     *
     * @param itemId the item
     * @return the sealed message; empty if none was received
     */
    public Optional<byte[]> totalMessage(UUID itemId)
    {
        return query("SELECT total_message FROM " + items
                + " WHERE stage = ? AND item_id = ?",
                statement -> statement.setObject(2, itemId),
                result -> result.next()
                        ? Optional.ofNullable(result.getBytes(1))
                        : Optional.empty());
    }

    /**
     * How many items are complete, and how many aren't.
     *
     * @return the counts, by {@code complete} and {@code incomplete}
     */
    public Map<String, Long> itemCounts()
    {
        return query("SELECT completed_at IS NOT NULL, count(*) FROM " + items
                + " WHERE stage = ? GROUP BY 1", statement ->
                {
                }, result ->
                {
                    Map<String, Long> counts = new LinkedHashMap<>();
                    counts.put("incomplete", 0L);
                    counts.put("complete", 0L);
                    while (result.next())
                    {
                        counts.put(result.getBoolean(1) ? "complete"
                                : "incomplete", result.getLong(2));
                    }
                    return counts;
                });
    }

    /**
     * The records claimed now, across the stage.
     *
     * @return the claims
     */
    public Claims claims()
    {
        return query("SELECT count(*), count(*) FILTER (WHERE claim_expires"
                + " < now()) FROM " + records
                + " WHERE stage = ? AND state = 'CLAIMED'", statement ->
                {
                }, result ->
                {
                    result.next();
                    return new Claims(result.getLong(1), result.getLong(2));
                });
    }

    /**
     * An item's records, counted by state, and the most attempts any of
     * them took.
     *
     * @param itemId the item
     * @return the counts, by state ({@code CLAIMED}, {@code STORED},
     *         {@code SENT}), and {@code attempts}
     */
    public Map<String, Long> recordStates(UUID itemId)
    {
        return query("SELECT state, count(*), max(attempts) FROM " + records
                + " WHERE stage = ? AND item_id = ? GROUP BY state"
                + " ORDER BY state", statement ->
                        statement.setObject(2, itemId), result ->
                {
                    Map<String, Long> counts = new LinkedHashMap<>();
                    long attempts = 0;
                    while (result.next())
                    {
                        counts.put(result.getString(1), result.getLong(2));
                        attempts = Math.max(attempts, result.getLong(3));
                    }
                    counts.put("attempts", attempts);
                    return counts;
                });
    }

    /**
     * Runs a query for items.
     *
     * @param sql the query, with the stage as its first parameter
     * @return the items
     */
    private List<Item> items(String sql)
    {
        return query(sql, statement ->
        {
        }, result ->
        {
            List<Item> found = new ArrayList<>();
            while (result.next())
            {
                found.add(item(result));
            }
            return found;
        });
    }

    /**
     * Reads an item's row.
     *
     * @param result positioned on the row
     * @return the item
     * @throws SQLException if it can't be read
     */
    private static Item item(ResultSet result) throws SQLException
    {
        long expected = result.getLong(2);
        if (result.wasNull())
        {
            expected = -1;
        }
        Timestamp completed = result.getTimestamp(5);
        return new Item(result.getObject(1, UUID.class), expected,
                result.getLong(3), result.getLong(4),
                completed == null ? null : completed.toInstant(),
                result.getInt(6));
    }

    /** Sets a query's parameters after the stage. */
    @FunctionalInterface
    private interface Parameters
    {

        /**
         * Sets them.
         *
         * @param statement the query
         * @throws SQLException if one can't be set
         */
        void set(PreparedStatement statement) throws SQLException;
    }

    /**
     * Reads a query's result.
     *
     * @param <T> what is read
     */
    @FunctionalInterface
    private interface Reader<T>
    {

        /**
         * Reads it.
         *
         * @param result the result
         * @return what was read
         * @throws SQLException if it can't be read
         */
        T read(ResultSet result) throws SQLException;
    }

    /**
     * Runs a read-only query with the stage as its first parameter.
     *
     * @param sql        the query
     * @param parameters sets the parameters after the stage
     * @param reader     reads the result
     * @param <T>        what is read
     * @return what was read
     * @throws IllegalStateException if the database can't be read
     */
    private <T> T query(String sql, Parameters parameters, Reader<T> reader)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql))
        {
            statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            statement.setString(1, stage);
            parameters.set(statement);
            try (ResultSet result = statement.executeQuery())
            {
                return reader.read(result);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Can't read the stage registry ("
                    + failure.getSQLState() + ")", failure);
        }
    }
}
