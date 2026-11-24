/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.TestPostgres;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link SchemaChanges}: a change already made is left out, so it
 * waits on no one using the table; one missing is made.
 */
class SchemaChangesTest
{

    /** The database. */
    private DataSource database;

    /** A table of this test's own, with its schema. */
    private String table;

    /** Runs the changes while the table is in use. */
    private ExecutorService changers;

    /**
     * Makes a table with an index, a column with a default and a NOT NULL
     * column.
     *
     * @throws SQLException if it can't be made
     */
    @BeforeEach
    void setUp() throws SQLException
    {
        database = TestPostgres.dataSource();
        String schema = "sc_" + UUID.randomUUID().toString()
                .replace("-", "").substring(0, 12);
        table = schema + ".t";
        try (Connection connection = database.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.execute("CREATE SCHEMA " + schema);
            statement.execute("CREATE TABLE " + table + " (id bigint"
                    + " PRIMARY KEY, a text DEFAULT 'x', b text NOT NULL)");
            statement.execute("CREATE INDEX t_a ON " + table + " (a)");
        }
        changers = Executors.newVirtualThreadPerTaskExecutor();
    }

    /** Stops the changers. */
    @AfterEach
    void tearDown()
    {
        changers.shutdownNow();
    }

    /**
     * Each change already made is left out, while another connection
     * holds the strongest lock a reader or writer takes on the table.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aChangeMadeIsLeftOutWithoutWaitingOnTheTablesUsers()
            throws Exception
    {
        try (Connection user = database.getConnection();
                Statement statement = user.createStatement())
        {
            user.setAutoCommit(false);
            statement.execute("LOCK TABLE " + table
                    + " IN ROW EXCLUSIVE MODE");
            assertThat(apply("CREATE INDEX IF NOT EXISTS t_a ON " + table
                    + " (a)")).isFalse();
            assertThat(apply("CREATE UNIQUE INDEX IF NOT EXISTS T_A\n    ON "
                    + table + " (a) WHERE b IS NULL")).isFalse();
            assertThat(apply("ALTER TABLE " + table
                    + " ADD COLUMN IF NOT EXISTS a text")).isFalse();
            user.rollback();
        }
        try (Connection connection = database.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.execute("ALTER TABLE " + table
                    + " ALTER COLUMN a DROP DEFAULT,"
                    + " ALTER COLUMN b DROP NOT NULL");
        }
        try (Connection user = database.getConnection();
                Statement statement = user.createStatement())
        {
            user.setAutoCommit(false);
            statement.execute("LOCK TABLE " + table
                    + " IN ROW EXCLUSIVE MODE");
            assertThat(apply("ALTER TABLE " + table
                    + " ALTER COLUMN a DROP DEFAULT")).isFalse();
            assertThat(apply("ALTER TABLE " + table
                    + " ALTER COLUMN b DROP NOT NULL")).isFalse();
            user.rollback();
        }
    }

    /**
     * Each change missing is made.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aChangeMissingIsMade() throws Exception
    {
        assertThat(apply("CREATE INDEX IF NOT EXISTS t_b ON " + table
                + " (b)")).isTrue();
        assertThat(apply("ALTER TABLE " + table
                + " ADD COLUMN IF NOT EXISTS c integer NOT NULL DEFAULT 0"))
                .isTrue();
        assertThat(apply("ALTER TABLE " + table
                + " ALTER COLUMN a DROP DEFAULT")).isTrue();
        assertThat(apply("ALTER TABLE " + table
                + " ALTER COLUMN b DROP NOT NULL")).isTrue();
        try (Connection connection = database.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.execute("INSERT INTO " + table + " (id) VALUES (1)");
            try (ResultSet rows = statement.executeQuery("SELECT a, b, c"
                    + " FROM " + table + " WHERE id = 1"))
            {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isNull();
                assertThat(rows.getString(2)).isNull();
                assertThat(rows.getInt(3)).isZero();
            }
            try (ResultSet rows = statement.executeQuery("SELECT 1 FROM"
                    + " pg_indexes WHERE indexname = 't_b'"))
            {
                assertThat(rows.next()).isTrue();
            }
        }
        assertThat(apply("CREATE INDEX IF NOT EXISTS t_b ON " + table
                + " (b)")).isFalse();
    }

    /**
     * A statement of another kind is run as it is.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void anotherStatementIsRun() throws Exception
    {
        assertThat(apply("CREATE TABLE IF NOT EXISTS " + table
                + "_other (id bigint)")).isTrue();
        assertThat(apply("INSERT INTO " + table + "_other VALUES (1)"))
                .isTrue();
    }

    /**
     * Applies a change on a connection of its own, within a few seconds:
     * one that waited on the table's lock would not finish in time.
     *
     * @param ddl the change
     * @return whether it was run
     * @throws Exception if it fails or takes too long
     */
    private boolean apply(String ddl) throws Exception
    {
        Future<Boolean> applied = changers.submit(() ->
        {
            try (Connection connection = database.getConnection();
                    Statement statement = connection.createStatement())
            {
                statement.setQueryTimeout(10);
                return SchemaChanges.apply(statement, ddl);
            }
        });
        return applied.get(5, TimeUnit.SECONDS);
    }
}
