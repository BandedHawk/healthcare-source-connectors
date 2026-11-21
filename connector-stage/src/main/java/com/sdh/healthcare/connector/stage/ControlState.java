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
import java.util.Optional;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * Whether a stage is paused (CONTROL-API-DESIGN.md §9): a row of
 * {@code sdh_control_state}, beside the stage's audit journal, for each
 * scope that has ever been paused. A scope is a stage's deployment, named
 * as its journal's chains are, such as {@code convert/<connector ID>}; a
 * landing node is one intake, so its scope is that intake's.
 *
 * <p>The stage's pods read it when they start and every few seconds after
 * ({@link PauseWatch}); the operator's {@code pause} and {@code resume}
 * write it. Being in the database, a pause outlasts every pod.
 *
 * <p>A pause keeps who paused, when, and the reason they gave, which is
 * checked to hold only letters, digits, spaces and a few marks, as the
 * control API checks every action's reason. Resuming clears all three.
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
                      scope      text        PRIMARY KEY,
                      paused     boolean     NOT NULL,
                      paused_by  text,
                      paused_at  timestamptz,
                      reason     text,
                      updated_at timestamptz NOT NULL
                    )""".formatted(table));
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
}
