/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.crypto;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * A database table whose rows hold a data key of their own, wrapped with a
 * KEK, such as a failure's reasons or a correction's content
 * (CONTROL-API-DESIGN.md §11): what a re-key re-wraps there, and what the
 * {@code keys} query counts. The encrypted value doesn't change, only its
 * wrapped key and KEK reference.
 *
 * <p>The SQL is plain: a select of the key columns and an update by them,
 * so it is portable. Nothing read is logged.
 *
 * @param table   the table, with its schema
 * @param keys    the columns that pick out a row
 * @param wrapped the column holding the wrapped key
 * @param kek     the column naming its KEK
 * @param stage   the column naming the row's stage, for a table stages
 *                share; null if the table is the stage's alone
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record WrappedRows(String table, List<String> keys, String wrapped,
        String kek, String stage)
{

    /** A plain SQL name, possibly with its schema. */
    private static final Pattern NAME =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}"
                    + "(\\.[A-Za-z_][A-Za-z0-9_]{0,62})?");

    /** Postgres's code for a table that isn't there. */
    private static final String UNDEFINED_TABLE = "42P01";

    /** How many rows are re-wrapped in one transaction. */
    private static final int BATCH = 500;

    /** How long a statement may run, in seconds. */
    private static final int TIMEOUT_SECONDS = 60;

    /**
     * Checks the names.
     *
     * @param table   the table, with its schema
     * @param keys    the columns that pick out a row
     * @param wrapped the column holding the wrapped key
     * @param kek     the column naming its KEK
     * @param stage   the stage's column, or null
     * @throws IllegalArgumentException if a name isn't plain, or there is
     *                                  no key column
     */
    public WrappedRows
    {
        keys = List.copyOf(keys);
        if (keys.isEmpty())
        {
            throw new IllegalArgumentException("A row needs a key");
        }
        List<String> names = new ArrayList<>(keys);
        names.add(table);
        names.add(wrapped);
        names.add(kek);
        if (stage != null)
        {
            names.add(stage);
        }
        for (String name : names)
        {
            if (name == null || !NAME.matcher(name).matches())
            {
                throw new IllegalArgumentException("Invalid SQL name");
            }
        }
    }

    /**
     * How many rows each KEK wraps.
     *
     * @param dataSource the database
     * @param stageName  the stage, if the table is shared
     * @return rows by KEK; empty if the table isn't there
     * @throws IllegalStateException if the database can't be read
     */
    public Map<String, Long> byKek(DataSource dataSource, String stageName)
    {
        String sql = "SELECT " + kek + ", count(*) FROM " + table
                + " WHERE " + kek + " IS NOT NULL" + stageClause()
                + " GROUP BY " + kek;
        Map<String, Long> counts = new TreeMap<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(TIMEOUT_SECONDS);
            if (stage != null)
            {
                select.setString(1, stageName);
            }
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    counts.put(rows.getString(1), rows.getLong(2));
                }
            }
        }
        catch (SQLException failure)
        {
            if (UNDEFINED_TABLE.equals(failure.getSQLState()))
            {
                return counts;
            }
            throw new IllegalStateException("Wrapped rows unavailable",
                    failure);
        }
        return counts;
    }

    /**
     * Re-wraps every row's key from one KEK to another, a batch per
     * transaction, checking each new wrap gives the key back. A row
     * changed meanwhile is left for a later run, which takes it then.
     *
     * @param dataSource the database, writing
     * @param stageName  the stage, if the table is shared
     * @param provider   unwraps with the old KEK and wraps with the new
     * @param from       the old KEK
     * @param to         the new KEK
     * @return how many rows were re-wrapped
     * @throws IllegalStateException if the database can't be reached, or
     *                               a new wrap doesn't round-trip
     */
    public long rekey(DataSource dataSource, String stageName,
            KeyProvider provider, String from, String to)
    {
        Objects.requireNonNull(provider, "provider");
        String columns = String.join(", ", keys);
        String select = "SELECT " + columns + ", " + wrapped + " FROM "
                + table + " WHERE " + kek + " = ?" + stageClause()
                + " ORDER BY " + columns + " FETCH FIRST " + BATCH
                + " ROWS ONLY";
        String update = "UPDATE " + table + " SET " + kek + " = ?, "
                + wrapped + " = ? WHERE " + String.join(" AND ",
                        keys.stream().map(each -> each + " = ?").toList())
                + " AND " + kek + " = ? AND " + wrapped + " = ?";
        long total = 0;
        try (Connection connection = dataSource.getConnection())
        {
            boolean auto = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try
            {
                while (true)
                {
                    int changed = batch(connection, select, update,
                            stageName, provider, from, to);
                    connection.commit();
                    total += changed;
                    if (changed < BATCH)
                    {
                        return total;
                    }
                }
            }
            catch (SQLException | RuntimeException failure)
            {
                connection.rollback();
                throw failure;
            }
            finally
            {
                connection.setAutoCommit(auto);
            }
        }
        catch (SQLException failure)
        {
            if (UNDEFINED_TABLE.equals(failure.getSQLState()))
            {
                return total;
            }
            throw new IllegalStateException("Wrapped rows unavailable",
                    failure);
        }
    }

    /**
     * Re-wraps one batch of rows.
     *
     * @param connection the transaction
     * @param select     selects a batch
     * @param update     changes a row
     * @param stageName  the stage, if the table is shared
     * @param provider   unwraps and wraps
     * @param from       the old KEK
     * @param to         the new KEK
     * @return how many rows of the batch were selected
     * @throws SQLException if the database fails
     */
    private int batch(Connection connection, String select, String update,
            String stageName, KeyProvider provider, String from, String to)
            throws SQLException
    {
        List<Object[]> found = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                select))
        {
            statement.setQueryTimeout(TIMEOUT_SECONDS);
            statement.setString(1, from);
            if (stage != null)
            {
                statement.setString(2, stageName);
            }
            try (ResultSet rows = statement.executeQuery())
            {
                while (rows.next())
                {
                    Object[] row = new Object[keys.size() + 1];
                    for (int i = 0; i < keys.size(); i++)
                    {
                        row[i] = rows.getObject(i + 1);
                    }
                    row[keys.size()] = rows.getBytes(keys.size() + 1);
                    found.add(row);
                }
            }
        }
        if (found.isEmpty())
        {
            return 0;
        }
        try (PreparedStatement statement = connection.prepareStatement(
                update))
        {
            statement.setQueryTimeout(TIMEOUT_SECONDS);
            for (Object[] row : found)
            {
                byte[] old = (byte[]) row[keys.size()];
                WrappedKey fresh = rewrap(provider, from, to, old);
                int at = 1;
                statement.setString(at++, fresh.keyRef());
                statement.setBytes(at++, fresh.wrapped());
                for (int i = 0; i < keys.size(); i++)
                {
                    statement.setObject(at++, row[i]);
                }
                statement.setString(at++, from);
                statement.setBytes(at, old);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        return found.size();
    }

    /**
     * Unwraps a key with the old KEK and wraps it with the new, checking
     * the new wrap gives the same key back.
     *
     * @param provider unwraps and wraps
     * @param from     the old KEK
     * @param to       the new KEK
     * @param old      the key wrapped with the old KEK
     * @return the key wrapped with the new
     * @throws IllegalStateException if the new wrap doesn't round-trip
     */
    static WrappedKey rewrap(KeyProvider provider, String from, String to,
            byte[] old)
    {
        byte[] plain = provider.unwrap(new WrappedKey(from, old));
        byte[] back = null;
        try
        {
            WrappedKey fresh = provider.wrap(to, plain);
            back = provider.unwrap(fresh);
            if (!MessageDigest.isEqual(plain, back))
            {
                throw new IllegalStateException("A re-wrapped key did not"
                        + " round-trip");
            }
            return fresh;
        }
        finally
        {
            Arrays.fill(plain, (byte) 0);
            if (back != null)
            {
                Arrays.fill(back, (byte) 0);
            }
        }
    }

    /**
     * The clause picking the stage's rows, if the table is shared.
     *
     * @return it, or nothing
     */
    private String stageClause()
    {
        return stage == null ? "" : " AND " + stage + " = ?";
    }
}
