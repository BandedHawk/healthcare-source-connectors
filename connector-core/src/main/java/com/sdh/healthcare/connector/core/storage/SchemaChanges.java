/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Runs the statements a pod makes its tables with at startup, leaving out
 * those whose change is already made.
 * <p>
 * Postgres locks a table for some of them before it looks whether there is
 * anything to do: {@code CREATE INDEX IF NOT EXISTS} takes a SHARE lock,
 * which waits for every open write to the table, and {@code ALTER TABLE ...
 * ADD COLUMN IF NOT EXISTS} or {@code ALTER COLUMN} take an ACCESS
 * EXCLUSIVE lock, which waits for every open read as well. Each holds up
 * every statement on the table after it while it waits. A pod starting
 * while others work on the table would wait for them, past its query
 * timeout, and fail to start (AWS run K, 2026-11-24: new split pods could
 * not make their append queue's tables; docs/DESIGN.md revision (ez)). So
 * these are first looked for in the catalogs, which takes no lock on the
 * table, and run only if their change is missing. Two pods starting at
 * once on a new table may both run one; its {@code IF NOT EXISTS} settles
 * that, with nothing yet using the table.
 * <p>
 * Other statements ({@code CREATE TABLE IF NOT EXISTS}, which looks first,
 * {@code CREATE SCHEMA}, a {@code DO} block that looks in the catalogs
 * itself) are run as they are.
 */
public final class SchemaChanges
{

    /** A plain or schema-qualified name. */
    private static final String NAME =
            "([A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?)";

    /** A plain name. */
    private static final String PLAIN = "([A-Za-z_][\\w$]*)";

    /** {@code CREATE [UNIQUE] INDEX IF NOT EXISTS name ON table}. */
    private static final Pattern INDEX = Pattern.compile(
            "\\s*CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+IF\\s+NOT\\s+EXISTS\\s+"
                    + PLAIN + "\\s+ON\\s+(?:ONLY\\s+)?" + NAME + "\\b.*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** {@code ALTER TABLE table ADD COLUMN IF NOT EXISTS column ...}. */
    private static final Pattern ADD_COLUMN = Pattern.compile(
            "\\s*ALTER\\s+TABLE\\s+" + NAME
                    + "\\s+ADD\\s+COLUMN\\s+IF\\s+NOT\\s+EXISTS\\s+" + PLAIN
                    + "\\b.*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** {@code ALTER TABLE table ALTER COLUMN column DROP DEFAULT}. */
    private static final Pattern DROP_DEFAULT = Pattern.compile(
            "\\s*ALTER\\s+TABLE\\s+" + NAME + "\\s+ALTER\\s+COLUMN\\s+"
                    + PLAIN + "\\s+DROP\\s+DEFAULT\\s*",
            Pattern.CASE_INSENSITIVE);

    /** {@code ALTER TABLE table ALTER COLUMN column DROP NOT NULL}. */
    private static final Pattern DROP_NOT_NULL = Pattern.compile(
            "\\s*ALTER\\s+TABLE\\s+" + NAME + "\\s+ALTER\\s+COLUMN\\s+"
                    + PLAIN + "\\s+DROP\\s+NOT\\s+NULL\\s*",
            Pattern.CASE_INSENSITIVE);

    /** Whether the table has an index of the name. */
    private static final String HAS_INDEX = """
            SELECT 1 FROM pg_index x
              JOIN pg_class i ON i.oid = x.indexrelid
             WHERE x.indrelid = to_regclass(?) AND i.relname = ?""";

    /** Whether the table has the column. */
    private static final String HAS_COLUMN = """
            SELECT 1 FROM pg_attribute
             WHERE attrelid = to_regclass(?) AND attname = ?
               AND attnum > 0 AND NOT attisdropped""";

    /** Whether the table's column has a default. */
    private static final String HAS_DEFAULT = """
            SELECT 1 FROM pg_attribute a
              JOIN pg_attrdef d ON d.adrelid = a.attrelid
                               AND d.adnum = a.attnum
             WHERE a.attrelid = to_regclass(?) AND a.attname = ?
               AND NOT a.attisdropped""";

    /** Whether the table's column is NOT NULL. */
    private static final String IS_NOT_NULL = """
            SELECT 1 FROM pg_attribute
             WHERE attrelid = to_regclass(?) AND attname = ?
               AND attnotnull AND NOT attisdropped""";

    /** Not made: only static methods. */
    private SchemaChanges()
    {
    }

    /**
     * Runs a statement unless its change is already made.
     *
     * @param statement the statement to run it on, its query timeout set;
     *                  its connection is used for the catalog lookups too
     * @param ddl       the statement
     * @return whether it was run
     * @throws SQLException if it, or a lookup, fails
     */
    public static boolean apply(Statement statement, String ddl)
            throws SQLException
    {
        Matcher index = INDEX.matcher(ddl);
        if (index.matches())
        {
            return runUnless(statement, ddl, HAS_INDEX, index.group(2),
                    index.group(1), true);
        }
        Matcher column = ADD_COLUMN.matcher(ddl);
        if (column.matches())
        {
            return runUnless(statement, ddl, HAS_COLUMN, column.group(1),
                    column.group(2), true);
        }
        Matcher dropDefault = DROP_DEFAULT.matcher(ddl);
        if (dropDefault.matches())
        {
            return runUnless(statement, ddl, HAS_DEFAULT,
                    dropDefault.group(1), dropDefault.group(2), false);
        }
        Matcher dropNotNull = DROP_NOT_NULL.matcher(ddl);
        if (dropNotNull.matches())
        {
            return runUnless(statement, ddl, IS_NOT_NULL,
                    dropNotNull.group(1), dropNotNull.group(2), false);
        }
        statement.execute(ddl);
        return true;
    }

    /**
     * Runs a statement unless a lookup on the table's catalog rows says its
     * change is made.
     *
     * @param statement the statement to run it on
     * @param ddl       the statement
     * @param lookup    the query, taking the table and a name, that finds
     *                  a row
     * @param table     the table, as the statement names it
     * @param name      the index or column
     * @param madeIfRow true if a row means the change is made (the index
     *                  or column is there); false if a row means it is
     *                  still to make (the default or NOT NULL is there)
     * @return whether it was run
     * @throws SQLException if it, or the lookup, fails
     */
    private static boolean runUnless(Statement statement, String ddl,
            String lookup, String table, String name, boolean madeIfRow)
            throws SQLException
    {
        boolean row;
        try (PreparedStatement query = statement.getConnection()
                .prepareStatement(lookup))
        {
            query.setQueryTimeout(statement.getQueryTimeout());
            query.setString(1, table);
            // An unquoted name is folded to lower case, as Postgres does.
            query.setString(2, name.toLowerCase(Locale.ROOT));
            try (ResultSet rows = query.executeQuery())
            {
                row = rows.next();
            }
        }
        if (row == madeIfRow)
        {
            return false;
        }
        statement.execute(ddl);
        return true;
    }
}
