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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * What the split stage's item rows ({@link ItemCheckpoints}) say, for an
 * operator's status: read only, from the plain columns, so it needs no
 * keys. The checkpoints themselves, MAC'd, are read with
 * {@link ItemCheckpoints#load}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SplitItemReport
{

    /** A schema name: a plain identifier, as it is put in the SQL. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[a-z_][a-z0-9_]{0,62}");

    /** How long a query may run, in seconds. */
    private static final int QUERY_TIMEOUT_SECONDS = 10;

    /** The database. */
    private final DataSource dataSource;

    /** The table, with its schema. */
    private final String table;

    /**
     * An item not yet finished.
     *
     * @param itemId    the item
     * @param state     its checkpoint's state, such as {@code SPLITTING}
     * @param epoch     the epoch of the item lease it was last written under
     * @param updatedAt when it was last written
     */
    public record Unfinished(UUID itemId, String state, long epoch,
            Instant updatedAt)
    {
    }

    /**
     * Reads the split stage's item rows.
     *
     * @param dataSource the lease database
     * @param schema     the schema of the rows
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier
     */
    public SplitItemReport(DataSource dataSource, String schema)
    {
        if (!IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException("Invalid schema name");
        }
        this.dataSource = dataSource;
        this.table = schema + ".split_item";
    }

    /**
     * The items, counted by state.
     *
     * @return the counts, by state name, in name order
     * @throws IllegalStateException if the rows can't be read
     */
    public Map<String, Long> countsByState()
    {
        Map<String, Long> counts = new LinkedHashMap<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT state, count(*) FROM " + table
                                + " GROUP BY state ORDER BY state"))
        {
            statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            try (ResultSet result = statement.executeQuery())
            {
                while (result.next())
                {
                    counts.put(result.getString(1), result.getLong(2));
                }
            }
        }
        catch (SQLException failure)
        {
            throw unreadable(failure);
        }
        return counts;
    }

    /**
     * The items not yet finished (neither {@code COMPLETED} nor
     * {@code FAILED}), the longest untouched first.
     *
     * @param limit the most to return
     * @return the items
     * @throws IllegalStateException if the rows can't be read
     */
    public List<Unfinished> unfinished(int limit)
    {
        List<Unfinished> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT item_id, state, epoch, updated_at FROM "
                                + table + " WHERE state NOT IN"
                                + " ('COMPLETED', 'FAILED')"
                                + " ORDER BY updated_at LIMIT ?"))
        {
            statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            statement.setInt(1, Math.max(0, limit));
            try (ResultSet result = statement.executeQuery())
            {
                while (result.next())
                {
                    found.add(new Unfinished(
                            result.getObject(1, UUID.class),
                            result.getString(2), result.getLong(3),
                            result.getTimestamp(4).toInstant()));
                }
            }
        }
        catch (SQLException failure)
        {
            throw unreadable(failure);
        }
        return found;
    }

    /**
     * The failure for a query.
     *
     * @param failure what went wrong
     * @return the exception to throw
     */
    private static IllegalStateException unreadable(SQLException failure)
    {
        return new IllegalStateException("Can't read the split stage's items ("
                + failure.getSQLState() + ")", failure);
    }
}
