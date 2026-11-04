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

    /** Not instantiable. */
    private TestPostgres()
    {
    }

    /**
     * The database, started on first use and stopped when the JVM exits.
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
                postgres = EmbeddedPostgres.builder().start();
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
     * Connections to the {@code postgres} database.
     *
     * @return the data source
     */
    public static DataSource dataSource()
    {
        return get().getPostgresDatabase();
    }

    /**
     * The JDBC URL of the {@code postgres} database.
     *
     * @return the URL
     */
    public static String jdbcUrl()
    {
        return get().getJdbcUrl("postgres", "postgres");
    }
}
