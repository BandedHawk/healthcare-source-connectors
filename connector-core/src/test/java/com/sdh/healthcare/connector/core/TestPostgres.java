/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.io.UncheckedIOException;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * One embedded PostgreSQL (real binaries, no Docker) shared by all tests in
 * the JVM.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class TestPostgres
{

    /** The database; null until first asked for. */
    private static EmbeddedPostgres postgres;

    /**
     * A database to test against in place of the embedded one, from
     * {@code -Dsdh.test.pooled-url}: a JDBC URL with its user and password,
     * such as PgBouncer's in front of Postgres, to check that what the
     * stages do works through a pool in transaction mode (DESIGN.md
     * revision (ea)); unset for the embedded one.
     */
    public static final String POOLED_URL =
            System.getProperty("sdh.test.pooled-url");

    /** Not instantiated. */
    private TestPostgres()
    {
    }

    /**
     * The system property that starts the database with
     * {@code pg_stat_statements} loaded, for a performance test that counts
     * its statements (docs/PERFORMANCE.md §13.3).
     */
    public static final String STATEMENTS = "sdh.test.pg.statements";

    /**
     * The database, started on first use and stopped when the JVM exits;
     * with {@code pg_stat_statements} loaded if {@value #STATEMENTS} is
     * {@code true}.
     *
     * @return the database
     * @throws UncheckedIOException if it can't be started
     */
    public static synchronized EmbeddedPostgres get()
    {
        if (postgres == null)
        {
            try
            {
                EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();
                if (Boolean.getBoolean(STATEMENTS))
                {
                    builder.setServerConfig("shared_preload_libraries",
                            "pg_stat_statements")
                            .setServerConfig("pg_stat_statements.track",
                                    "all");
                }
                postgres = builder.start();
            }
            catch (IOException failure)
            {
                throw new UncheckedIOException(failure);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(() ->
            {
                try
                {
                    postgres.close();
                }
                catch (IOException ignored)
                {
                    // best effort
                }
            }));
        }
        return postgres;
    }

    /**
     * Connections to the {@code postgres} database, or to
     * {@link #POOLED_URL}'s if set.
     *
     * @return the data source
     */
    public static DataSource dataSource()
    {
        if (POOLED_URL != null)
        {
            PGSimpleDataSource pooled = new PGSimpleDataSource();
            pooled.setURL(POOLED_URL);
            return pooled;
        }
        return get().getPostgresDatabase();
    }

    /**
     * The JDBC URL of the {@code postgres} database, or
     * {@link #POOLED_URL} if set.
     *
     * @return the URL
     */
    public static String jdbcUrl()
    {
        return POOLED_URL != null ? POOLED_URL
                : get().getJdbcUrl("postgres", "postgres");
    }
}
