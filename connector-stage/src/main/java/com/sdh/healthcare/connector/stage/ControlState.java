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
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * Whether a stage is paused (CONTROL-API-DESIGN.md §9): a row of
 * {@code sdh_control_state}, beside the stage's audit journal, for each
 * scope that has ever been paused, rotated or re-keyed. A scope is a
 * stage's deployment, named as its journal's chains are, such as
 * {@code convert/<connector ID>}; a landing node is one intake, so its
 * scope is that intake's.
 *
 * <p>The stage's pods read it when they start and every few seconds after
 * ({@link PauseWatch}); the operator's {@code pause} and {@code resume}
 * write it. Being in the database, a pause outlasts every pod.
 *
 * <p>A pause keeps who paused, when, and the reason they gave, which is
 * checked to hold only letters, digits, spaces and a few marks, as the
 * control API checks every action's reason. Resuming clears all three.
 *
 * <p>The row also holds the KEK the scope wraps new data with once rotated
 * ({@link #rotate}), which its pods read as they read the pause
 * ({@link KekWatch}), and its last re-key ({@link #requestRekey}), which
 * its maintenance carries out ({@link RekeyPass}) (CONTROL-API-DESIGN.md
 * §11). They hold KEK references, never key material.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ControlState
{

    /**
     * A reason: letters, digits, spaces and a few marks, so it can't hold a
     * document's text, as the control API's actions take.
     */
    public static final Pattern REASON =
            Pattern.compile("[A-Za-z0-9 .,:_#/-]{1,200}");

    /** A plain SQL identifier, for the schema. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");

    /** Postgres's code for a table that isn't there. */
    private static final String UNDEFINED_TABLE = "42P01";

    /** How long a statement may run, in seconds. */
    private static final int TIMEOUT_SECONDS = 10;

    /**
     * A scope's pause.
     *
     * @param scope    what is paused
     * @param pausedBy who paused it
     * @param pausedAt when
     * @param reason   the reason they gave
     */
    public record Paused(String scope, String pausedBy, Instant pausedAt,
            String reason)
    {
    }

    /**
     * The KEK a scope wraps new data with, once rotated
     * (CONTROL-API-DESIGN.md §11): until then, its setting's.
     *
     * @param scope the scope
     * @param kek   the KEK's reference
     * @param setBy who rotated to it
     * @param setAt when
     */
    public record Kek(String scope, String kek, String setBy, Instant setAt)
    {
    }

    /** Where a re-key stands. */
    public enum RekeyState
    {
        /** Asked for and approved; no maintenance run has taken it yet. */
        REQUESTED,

        /** Being carried out by the stage's maintenance. */
        RUNNING,

        /** Done: nothing the stage stores is wrapped with the old KEK. */
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
        static RekeyState of(String wire)
        {
            return valueOf(wire.toUpperCase(Locale.ROOT));
        }
    }

    /**
     * A scope's re-key: its stored data moved from one KEK to another
     * (CONTROL-API-DESIGN.md §11). The last one is kept once it ends.
     *
     * @param scope       the scope
     * @param id          the operation that asked for it
     * @param from        the old KEK
     * @param to          the new KEK
     * @param state       where it stands
     * @param requestedBy who asked for it
     * @param requestedAt when
     * @param files       data files rewritten so far
     * @param rows        rows re-wrapped so far, in tables and the database
     * @param failure     why it failed, by the failure's class; null unless
     *                    it did
     * @param updatedAt   when it last moved
     */
    public record Rekeying(String scope, String id, String from, String to,
            RekeyState state, String requestedBy, Instant requestedAt,
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
     * Describes a scope's state, without touching the database.
     *
     * @param dataSource connections to the pipeline's database
     * @param schema     the schema, as the audit journal's; null for none
     * @param scope      the scope, such as {@code convert/<connector ID>}
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier, or the scope is blank
     */
    public ControlState(DataSource dataSource, String schema, String scope)
    {
        if (schema != null && !IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException("Invalid schema name");
        }
        if (scope == null || scope.isBlank())
        {
            throw new IllegalArgumentException("A control scope is required");
        }
        this.dataSource = dataSource;
        this.schema = schema;
        this.table = (schema == null ? "" : schema + ".")
                + "sdh_control_state";
        this.scope = scope;
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
                      scope         text        PRIMARY KEY,
                      paused        boolean     NOT NULL,
                      paused_by     text,
                      paused_at     timestamptz,
                      reason        text,
                      kek           text,
                      kek_by        text,
                      kek_at        timestamptz,
                      rekey_id      text,
                      rekey_from    text,
                      rekey_to      text,
                      rekey_state   text,
                      rekey_by      text,
                      rekey_at      timestamptz,
                      rekey_files   bigint,
                      rekey_rows    bigint,
                      rekey_failure text,
                      rekey_updated timestamptz,
                      updated_at    timestamptz NOT NULL
                    )""".formatted(table));
            // Added 2026-11-21 (C7) to a table C6 made.
            for (String column : List.of("kek text", "kek_by text",
                    "kek_at timestamptz", "rekey_id text", "rekey_from text",
                    "rekey_to text", "rekey_state text", "rekey_by text",
                    "rekey_at timestamptz", "rekey_files bigint",
                    "rekey_rows bigint", "rekey_failure text",
                    "rekey_updated timestamptz"))
            {
                statement.execute("ALTER TABLE " + table
                        + " ADD COLUMN IF NOT EXISTS " + column);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Could not create the control"
                    + " state table", failure);
        }
    }

    /**
     * The scope's pause, if it is paused.
     *
     * @return the pause; empty if it isn't paused, or the table isn't there
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<Paused> paused()
    {
        String sql = "SELECT paused_by, paused_at, reason FROM " + table
                + " WHERE scope = ? AND paused";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(TIMEOUT_SECONDS);
            select.setString(1, scope);
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    return Optional.empty();
                }
                Timestamp at = rows.getTimestamp(2);
                return Optional.of(new Paused(scope, rows.getString(1),
                        at == null ? null : at.toInstant(),
                        rows.getString(3)));
            }
        }
        catch (SQLException failure)
        {
            if (UNDEFINED_TABLE.equals(failure.getSQLState()))
            {
                return Optional.empty();
            }
            throw new IllegalStateException("Control state unavailable",
                    failure);
        }
    }

    /**
     * Pauses the scope, creating the table if it isn't there. A scope
     * already paused keeps its first pause: who, when and why.
     *
     * @param by     who pauses it
     * @param reason why, checked
     * @return the pause in force: this one, or the one already there
     * @throws IllegalArgumentException if the reason isn't as
     *                                  {@link #REASON} allows
     * @throws IllegalStateException    if the database can't be reached
     */
    public Paused pause(String by, String reason)
    {
        if (reason == null || !REASON.matcher(reason).matches())
        {
            throw new IllegalArgumentException("A pause needs a reason: 1 to"
                    + " 200 letters, digits, spaces or . , : _ # / -");
        }
        createTableIfMissing();
        String sql = """
                INSERT INTO %1$s AS s (scope, paused, paused_by, paused_at,
                       reason, updated_at)
                VALUES (?, true, ?, clock_timestamp(), ?, clock_timestamp())
                ON CONFLICT (scope) DO UPDATE
                   SET paused = true, paused_by = EXCLUDED.paused_by,
                       paused_at = EXCLUDED.paused_at,
                       reason = EXCLUDED.reason,
                       updated_at = EXCLUDED.updated_at
                 WHERE NOT s.paused""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement upsert = connection.prepareStatement(sql))
        {
            upsert.setQueryTimeout(TIMEOUT_SECONDS);
            upsert.setString(1, scope);
            upsert.setString(2, by);
            upsert.setString(3, reason);
            upsert.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Control state unavailable",
                    failure);
        }
        return paused().orElseThrow(() -> new IllegalStateException(
                "Control state unavailable"));
    }

    /**
     * Resumes the scope: clears its pause.
     *
     * @return the pause cleared; empty if it wasn't paused
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<Paused> resume()
    {
        String sql = """
                UPDATE %1$s AS s
                   SET paused = false, paused_by = NULL, paused_at = NULL,
                       reason = NULL, updated_at = clock_timestamp()
                  FROM (SELECT scope, paused_by, paused_at, reason
                          FROM %1$s WHERE scope = ? AND paused
                           FOR UPDATE) AS was
                 WHERE s.scope = was.scope
                RETURNING was.paused_by, was.paused_at, was.reason"""
                .formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(TIMEOUT_SECONDS);
            update.setString(1, scope);
            try (ResultSet rows = update.executeQuery())
            {
                if (!rows.next())
                {
                    return Optional.empty();
                }
                Timestamp at = rows.getTimestamp(2);
                return Optional.of(new Paused(scope, rows.getString(1),
                        at == null ? null : at.toInstant(),
                        rows.getString(3)));
            }
        }
        catch (SQLException failure)
        {
            if (UNDEFINED_TABLE.equals(failure.getSQLState()))
            {
                return Optional.empty();
            }
            throw new IllegalStateException("Control state unavailable",
                    failure);
        }
    }

    /**
     * The KEK the scope was last rotated to.
     *
     * @return it; empty if never rotated, or the table isn't there
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<Kek> kek()
    {
        String sql = "SELECT kek, kek_by, kek_at FROM " + table
                + " WHERE scope = ? AND kek IS NOT NULL";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(TIMEOUT_SECONDS);
            select.setString(1, scope);
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    return Optional.empty();
                }
                return Optional.of(new Kek(scope, rows.getString(1),
                        rows.getString(2), instant(rows.getTimestamp(3))));
            }
        }
        catch (SQLException failure)
        {
            if (UNDEFINED_TABLE.equals(failure.getSQLState()))
            {
                return Optional.empty();
            }
            throw new IllegalStateException("Control state unavailable",
                    failure);
        }
    }

    /**
     * Rotates the scope to a KEK: its pods wrap each new batch's data key
     * with it, once they have read it and checked they can. The caller
     * checks the KEK can be used first.
     *
     * @param by  who rotates it
     * @param kek the KEK's reference
     * @return the rotation in force
     * @throws IllegalArgumentException if the reference is blank
     * @throws IllegalStateException    if the database can't be reached
     */
    public Kek rotate(String by, String kek)
    {
        if (kek == null || kek.isBlank())
        {
            throw new IllegalArgumentException("A KEK is required");
        }
        createTableIfMissing();
        String sql = """
                INSERT INTO %1$s AS s (scope, paused, kek, kek_by, kek_at,
                       updated_at)
                VALUES (?, false, ?, ?, clock_timestamp(), clock_timestamp())
                ON CONFLICT (scope) DO UPDATE
                   SET kek = EXCLUDED.kek, kek_by = EXCLUDED.kek_by,
                       kek_at = EXCLUDED.kek_at,
                       updated_at = EXCLUDED.updated_at""".formatted(table);
        update(sql, scope, kek, by);
        return kek().orElseThrow(() -> new IllegalStateException(
                "Control state unavailable"));
    }

    /**
     * The scope's re-key under way, or its last one.
     *
     * @return it; empty if there has been none, or the table isn't there
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<Rekeying> rekeying()
    {
        String sql = "SELECT rekey_id, rekey_from, rekey_to, rekey_state,"
                + " rekey_by, rekey_at, rekey_files, rekey_rows,"
                + " rekey_failure, rekey_updated FROM " + table
                + " WHERE scope = ? AND rekey_id IS NOT NULL";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(TIMEOUT_SECONDS);
            select.setString(1, scope);
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    return Optional.empty();
                }
                return Optional.of(new Rekeying(scope, rows.getString(1),
                        rows.getString(2), rows.getString(3),
                        RekeyState.of(rows.getString(4)), rows.getString(5),
                        instant(rows.getTimestamp(6)), rows.getLong(7),
                        rows.getLong(8), rows.getString(9),
                        instant(rows.getTimestamp(10))));
            }
        }
        catch (SQLException failure)
        {
            if (UNDEFINED_TABLE.equals(failure.getSQLState()))
            {
                return Optional.empty();
            }
            throw new IllegalStateException("Control state unavailable",
                    failure);
        }
    }

    /**
     * Asks for a re-key, for the stage's maintenance to carry out. Asking
     * again with the same operation keeps the one asked for, so a run
     * taken over by another control pod doesn't start it again.
     *
     * @param id   the operation asking for it
     * @param by   who asked for it
     * @param from the old KEK
     * @param to   the new KEK
     * @return the re-key in force, this one
     * @throws IllegalArgumentException if the KEKs are blank or the same
     * @throws IllegalStateException    if another re-key is under way, or
     *                                  the database can't be reached
     */
    public Rekeying requestRekey(String id, String by, String from,
            String to)
    {
        if (from == null || from.isBlank() || to == null || to.isBlank()
                || from.equals(to))
        {
            throw new IllegalArgumentException("A re-key needs two"
                    + " different KEKs");
        }
        createTableIfMissing();
        String sql = """
                INSERT INTO %1$s AS s (scope, paused, rekey_id, rekey_from,
                       rekey_to, rekey_state, rekey_by, rekey_at,
                       rekey_files, rekey_rows, rekey_failure,
                       rekey_updated, updated_at)
                VALUES (?, false, ?, ?, ?, 'requested', ?,
                        clock_timestamp(), 0, 0, NULL, clock_timestamp(),
                        clock_timestamp())
                ON CONFLICT (scope) DO UPDATE
                   SET rekey_id = EXCLUDED.rekey_id,
                       rekey_from = EXCLUDED.rekey_from,
                       rekey_to = EXCLUDED.rekey_to,
                       rekey_state = EXCLUDED.rekey_state,
                       rekey_by = EXCLUDED.rekey_by,
                       rekey_at = EXCLUDED.rekey_at,
                       rekey_files = 0, rekey_rows = 0,
                       rekey_failure = NULL,
                       rekey_updated = EXCLUDED.rekey_updated,
                       updated_at = EXCLUDED.updated_at
                 WHERE s.rekey_id IS DISTINCT FROM EXCLUDED.rekey_id
                   AND (s.rekey_state IS NULL
                        OR s.rekey_state IN ('done', 'failed'))"""
                .formatted(table);
        update(sql, scope, id, from, to, by);
        Rekeying now = rekeying().orElseThrow(() ->
                new IllegalStateException("Control state unavailable"));
        if (!now.id().equals(id))
        {
            throw new IllegalStateException("Another re-key is under way");
        }
        return now;
    }

    /**
     * Records a re-key's progress: it is running, with more files and
     * rows done.
     *
     * @param id    the operation that asked for it
     * @param files more data files rewritten
     * @param rows  more rows re-wrapped
     * @return true if it was recorded: the re-key is still that one, and
     *         still to finish
     * @throws IllegalStateException if the database can't be reached
     */
    public boolean rekeyAdvanced(String id, long files, long rows)
    {
        String sql = """
                UPDATE %1$s
                   SET rekey_state = 'running',
                       rekey_files = rekey_files + ?,
                       rekey_rows = rekey_rows + ?,
                       rekey_updated = clock_timestamp(),
                       updated_at = clock_timestamp()
                 WHERE scope = ? AND rekey_id = ?
                   AND rekey_state IN ('requested', 'running')"""
                .formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(TIMEOUT_SECONDS);
            update.setLong(1, files);
            update.setLong(2, rows);
            update.setString(3, scope);
            update.setString(4, id);
            return update.executeUpdate() == 1;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Control state unavailable",
                    failure);
        }
    }

    /**
     * Ends a re-key, done or failed.
     *
     * @param id      the operation that asked for it
     * @param state   {@link RekeyState#DONE} or {@link RekeyState#FAILED}
     * @param failure why it failed, by the failure's class; null if done
     * @return true if it was recorded: the re-key is still that one, and
     *         was still to finish
     * @throws IllegalArgumentException if the state isn't an end
     * @throws IllegalStateException    if the database can't be reached
     */
    public boolean rekeyEnded(String id, RekeyState state, String failure)
    {
        if (state.active())
        {
            throw new IllegalArgumentException("Not an end: " + state);
        }
        String sql = """
                UPDATE %1$s
                   SET rekey_state = ?, rekey_failure = ?,
                       rekey_updated = clock_timestamp(),
                       updated_at = clock_timestamp()
                 WHERE scope = ? AND rekey_id = ?
                   AND rekey_state IN ('requested', 'running')"""
                .formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(TIMEOUT_SECONDS);
            update.setString(1, state.wire());
            update.setString(2, failure);
            update.setString(3, scope);
            update.setString(4, id);
            return update.executeUpdate() == 1;
        }
        catch (SQLException unreached)
        {
            throw new IllegalStateException("Control state unavailable",
                    unreached);
        }
    }

    /**
     * Runs an update with text parameters.
     *
     * @param sql        the statement
     * @param parameters its parameters, in order
     * @throws IllegalStateException if the database can't be reached
     */
    private void update(String sql, String... parameters)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(TIMEOUT_SECONDS);
            for (int i = 0; i < parameters.length; i++)
            {
                update.setString(i + 1, parameters[i]);
            }
            update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Control state unavailable",
                    failure);
        }
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
