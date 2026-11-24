/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.audit;

import com.sdh.healthcare.connector.core.storage.SchemaChanges;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * The audit journal's tables (HIPAA.md §3.1, audit controls), in a stage's
 * schema of the lease database, or in the lease table's when the stage has
 * none of its own:
 * <ul>
 * <li>{@code sdh_audit_chain}: one row per chain, a process's events, with
 *     the number and hash of its last entry written, and when it closed;
 * <li>{@code sdh_audit_event}: the entries;
 * <li>{@code sdh_audit_seal}: the stage's signatures over chain heads.
 * </ul>
 * Nothing in them is PHI: event names, allow-listed attributes, hashes and
 * signatures.
 *
 * @param schema the schema; null for the connection's default
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record AuditTables(String schema)
{

    /** A plain SQL identifier. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[a-z_][a-z0-9_]*");

    /**
     * Checks the schema's name.
     *
     * @throws IllegalArgumentException if it isn't a plain identifier
     */
    public AuditTables
    {
        if (schema != null && !IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException("Invalid schema name");
        }
    }

    /**
     * The tables beside a lease table: in its schema, if its name has one.
     *
     * @param leaseTable the lease table's name, such as {@code sdh_lease}
     *                   or {@code landing.sdh_lease}
     * @return the tables
     */
    public static AuditTables besideLeaseTable(String leaseTable)
    {
        int dot = leaseTable.indexOf('.');
        return new AuditTables(dot < 0 ? null : leaseTable.substring(0, dot));
    }

    /**
     * The chains table, with its schema.
     *
     * @return its name
     */
    public String chains()
    {
        return qualified("sdh_audit_chain");
    }

    /**
     * The entries table, with its schema.
     *
     * @return its name
     */
    public String events()
    {
        return qualified("sdh_audit_event");
    }

    /**
     * The seals table, with its schema.
     *
     * @return its name
     */
    public String seals()
    {
        return qualified("sdh_audit_seal");
    }

    /**
     * Creates the tables if they aren't there.
     *
     * @param dataSource connections to the database
     * @param timeout    how long a statement may run, in seconds
     * @throws IllegalStateException if they can't be created
     */
    public void createIfMissing(DataSource dataSource, int timeout)
    {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(timeout);
            if (schema != null)
            {
                statement.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
            }
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      chain_id   uuid        NOT NULL PRIMARY KEY,
                      source     text        NOT NULL,
                      pod        text        NOT NULL,
                      started_at timestamptz NOT NULL,
                      last_seq   bigint      NOT NULL,
                      last_hash  bytea       NOT NULL,
                      updated_at timestamptz NOT NULL,
                      closed_at  timestamptz
                    )""".formatted(chains()));
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      chain_id   uuid        NOT NULL,
                      seq        bigint      NOT NULL,
                      at         timestamptz NOT NULL,
                      name       text        NOT NULL,
                      severity   text        NOT NULL,
                      attributes text        NOT NULL,
                      hash       bytea       NOT NULL,
                      PRIMARY KEY (chain_id, seq)
                    )""".formatted(events()));
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      chain_id  uuid        NOT NULL,
                      seq       bigint      NOT NULL,
                      sealed_at timestamptz NOT NULL,
                      seal      text        NOT NULL,
                      PRIMARY KEY (chain_id, seq)
                    )""".formatted(seals()));
            SchemaChanges.apply(statement,
                    "CREATE INDEX IF NOT EXISTS sdh_audit_event_at"
                    + " ON " + events() + " (at)");
            SchemaChanges.apply(statement, "CREATE INDEX IF NOT EXISTS"
                    + " sdh_audit_chain_updated ON " + chains()
                    + " (updated_at)");
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the audit tables", failure);
        }
    }

    /**
     * A table's name, with the schema.
     *
     * @param table the table
     * @return {@code schema.table}, or the table alone
     */
    private String qualified(String table)
    {
        return schema == null ? table : schema + "." + table;
    }
}
