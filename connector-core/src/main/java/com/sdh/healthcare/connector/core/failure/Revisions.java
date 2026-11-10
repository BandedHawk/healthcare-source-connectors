/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.failure;

import com.sdh.healthcare.connector.contract.message.Lineage;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * Gives out a stage's revision numbers (STAGES.md §15.8.4): one counter per
 * place, shared by an operator's correction or retry when submitted and a
 * reprocess when queued, so the two never make the same line, whichever
 * comes first. A place here is the stage, item, record number, generation
 * and the line before this stage: a record the stage before revised is
 * numbered from 1 again here.
 * <p>
 * A number given is never below what the stage holds at the place already:
 * its stored rows, its failures and the revisions its corrections noted,
 * from before the counter was kept.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Revisions
{

    /** Not instantiated: this class only has static methods. */
    private Revisions()
    {
    }

    /**
     * The counters' table.
     *
     * @param schema the stage's registry schema
     * @return the table, with its schema
     */
    public static String table(String schema)
    {
        FailureStore.table(schema);
        return schema + ".stage_revision";
    }

    /**
     * The statement that makes the table.
     *
     * @param schema the stage's registry schema
     * @return the statement
     */
    public static String ddl(String schema)
    {
        return """
                CREATE TABLE IF NOT EXISTS %s (
                  stage      text    NOT NULL,
                  item_id    uuid    NOT NULL,
                  seq        bigint  NOT NULL,
                  generation integer NOT NULL,
                  before     text    NOT NULL,
                  last       integer NOT NULL,
                  PRIMARY KEY (stage, item_id, seq, generation, before)
                )""".formatted(table(schema));
    }

    /**
     * Gives the next revision at a place, in the caller's transaction, its
     * counter's row locked until it commits.
     *
     * @param connection the transaction's connection
     * @param schema     the stage's registry schema
     * @param stage      the stage
     * @param itemId     the item
     * @param seq        the record's number in its generation
     * @param from       a line of the place: its generation and the
     *                   revisions before this stage are kept
     * @param least      a revision of this stage known to be taken (that
     *                   of a failure corrected); 0 if none is known, so a
     *                   place the stage holds nothing at is given 1
     * @param timeout    how long a statement may run, in seconds
     * @return the line, this stage's revision the next given
     * @throws SQLException if a statement fails
     */
    public static Lineage next(Connection connection, String schema,
            String stage, UUID itemId, long seq, Lineage from, int least,
            int timeout) throws SQLException
    {
        int place = Lineage.stageOf(stage);
        int floor = Math.max(least,
                held(connection, schema, stage, itemId, seq, from, place,
                        timeout));
        String upsert = """
                INSERT INTO %s AS r (stage, item_id, seq, generation,
                       before, last)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (stage, item_id, seq, generation, before)
                DO UPDATE SET last = GREATEST(r.last + 1, EXCLUDED.last)
                RETURNING last""".formatted(table(schema));
        try (PreparedStatement add = connection.prepareStatement(upsert))
        {
            add.setQueryTimeout(timeout);
            add.setString(1, stage);
            add.setObject(2, itemId);
            add.setLong(3, seq);
            add.setInt(4, from.generation());
            add.setString(5, before(from, place));
            add.setInt(6, floor + 1);
            try (ResultSet rows = add.executeQuery())
            {
                rows.next();
                return from.withRevision(place, rows.getInt(1));
            }
        }
    }

    /**
     * Gives an item's next generation at the split stage (STAGES.md
     * §15.8.4), in the caller's transaction, its counter's row locked until
     * it commits: shared by a reprocess when queued and a correction or
     * retry of the item as a whole when submitted. The counter is the
     * item's whole-item place ({@code seq -1}) with no generation; a number
     * given is never below the generation the stage's checkpoint, its
     * failures and its corrections already hold.
     *
     * @param connection the transaction's connection
     * @param schema     the stage's registry schema
     * @param stage      the stage
     * @param itemId     the item
     * @param timeout    how long a statement may run, in seconds
     * @return the generation, from 2
     * @throws SQLException if a statement fails
     */
    public static int nextGeneration(Connection connection, String schema,
            String stage, UUID itemId, int timeout) throws SQLException
    {
        int floor = 1;
        if (exists(connection, schema + ".split_item", timeout))
        {
            floor = Math.max(floor, highest(connection, "SELECT max("
                    + "generation) FROM " + schema + ".split_item"
                    + " WHERE item_id = ?", null, itemId, timeout));
        }
        for (String table : List.of("stage_failure", "stage_correction"))
        {
            if (exists(connection, schema + "." + table, timeout))
            {
                floor = Math.max(floor, highest(connection, "SELECT max("
                        + "generation) FROM " + schema + "." + table
                        + " WHERE stage = ? AND item_id = ?", stage, itemId,
                        timeout));
            }
        }
        String upsert = """
                INSERT INTO %s AS r (stage, item_id, seq, generation,
                       before, last)
                VALUES (?, ?, -1, 0, 'g', ?)
                ON CONFLICT (stage, item_id, seq, generation, before)
                DO UPDATE SET last = GREATEST(r.last + 1, EXCLUDED.last)
                RETURNING last""".formatted(table(schema));
        try (PreparedStatement add = connection.prepareStatement(upsert))
        {
            add.setQueryTimeout(timeout);
            add.setString(1, stage);
            add.setObject(2, itemId);
            add.setInt(3, floor + 1);
            try (ResultSet rows = add.executeQuery())
            {
                rows.next();
                return rows.getInt(1);
            }
        }
    }

    /**
     * Reads one number.
     *
     * @param connection the connection
     * @param sql        the query: the stage, if given, then the item
     * @param stage      the stage; null if the query takes none
     * @param itemId     the item
     * @param timeout    how long a statement may run, in seconds
     * @return the number; 0 if none
     * @throws SQLException if the statement fails
     */
    private static int highest(Connection connection, String sql,
            String stage, UUID itemId, int timeout) throws SQLException
    {
        try (PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            int index = 1;
            if (stage != null)
            {
                select.setString(index++, stage);
            }
            select.setObject(index, itemId);
            try (ResultSet rows = select.executeQuery())
            {
                return rows.next() ? rows.getInt(1) : 0;
            }
        }
    }

    /**
     * The line before a stage, as text: the revisions of the stages
     * before it, such as {@code 1} for the convert stage's input as first
     * cut, or nothing at the split stage.
     *
     * @param lineage the lineage
     * @param place   the stage's place in a line
     * @return the revisions, joined by dots
     */
    static String before(Lineage lineage, int place)
    {
        StringJoiner joined = new StringJoiner(".");
        for (int stage = 0; stage < place; stage++)
        {
            joined.add(Integer.toString(lineage.revision(stage)));
        }
        return joined.toString();
    }

    /**
     * The highest revision of this stage the stage holds at a place, in
     * whichever of its tables are there: the registry's rows (a stage of
     * the stage library), the fail store, and the revisions noted by
     * corrections.
     *
     * @param connection the transaction's connection
     * @param schema     the stage's registry schema
     * @param stage      the stage
     * @param itemId     the item
     * @param seq        the record's number
     * @param from       a line of the place
     * @param place      the stage's place in a line
     * @param timeout    how long a statement may run, in seconds
     * @return the revision; 0 if it holds none
     * @throws SQLException if a statement fails
     */
    private static int held(Connection connection, String schema,
            String stage, UUID itemId, long seq, Lineage from, int place,
            int timeout) throws SQLException
    {
        // Postgres arrays count from 1; a line leaves out trailing 1s.
        StringBuilder match = new StringBuilder();
        for (int before = 0; before < place; before++)
        {
            match.append(" AND COALESCE(line[").append(before + 1)
                    .append("], 1) = ").append(from.revision(before));
        }
        String revision = "COALESCE(line[" + (place + 1) + "], 1)";
        List<String> tables = new ArrayList<>();
        for (String table : List.of("stage_record", "stage_failure",
                "stage_correction"))
        {
            if (exists(connection, schema + "." + table, timeout))
            {
                tables.add(table);
            }
        }
        int highest = 0;
        for (String table : tables)
        {
            String sql = "SELECT max(" + revision + ") FROM " + schema + "."
                    + table + " WHERE stage = ? AND item_id = ? AND seq = ?"
                    + " AND generation = ? AND line IS NOT NULL" + match;
            try (PreparedStatement select = connection.prepareStatement(sql))
            {
                select.setQueryTimeout(timeout);
                select.setString(1, stage);
                select.setObject(2, itemId);
                select.setLong(3, seq);
                select.setInt(4, from.generation());
                try (ResultSet rows = select.executeQuery())
                {
                    if (rows.next())
                    {
                        highest = Math.max(highest, rows.getInt(1));
                    }
                }
            }
        }
        return highest;
    }

    /**
     * Whether a table is there.
     *
     * @param connection the connection
     * @param table      the table, with its schema
     * @param timeout    how long a statement may run, in seconds
     * @return true if it is
     * @throws SQLException if the statement fails
     */
    private static boolean exists(Connection connection, String table,
            int timeout) throws SQLException
    {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT to_regclass(?) IS NOT NULL"))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, table);
            try (ResultSet rows = select.executeQuery())
            {
                rows.next();
                return rows.getBoolean(1);
            }
        }
    }
}
