/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;

/**
 * The lease database, as the admin commands open it: one read-only
 * connection, opened on first use. It holds the leases, the stages'
 * registries and the split stage's item rows (STAGES.md §5).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AdminDatabase implements AutoCloseable
{

    /** The database's settings. */
    private final CommonSettings.Lease settings;

    /** The pool, once opened. */
    private HikariDataSource pool;

    /**
     * Describes the database, without opening it.
     *
     * @param settings the stage's lease settings
     */
    public AdminDatabase(CommonSettings.Lease settings)
    {
        this.settings = settings;
    }

    /**
     * The read-only pool, opened on first use.
     *
     * @return the pool
     * @throws IllegalArgumentException if no JDBC URL is set
     */
    public HikariDataSource dataSource()
    {
        if (pool == null)
        {
            CommonSettings.Jdbc jdbc = settings.jdbc();
            if (jdbc.url() == null || jdbc.url().isBlank())
            {
                throw new IllegalArgumentException(
                        "sdh.lease.jdbc.url is required");
            }
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(jdbc.url());
            config.setUsername(jdbc.username());
            config.setPassword(jdbc.password());
            config.setMaximumPoolSize(1);
            config.setConnectionTimeout(10_000);
            config.setReadOnly(true);
            config.setPoolName("sdh-admin");
            pool = new HikariDataSource(config);
        }
        return pool;
    }

    /**
     * The leases, to inspect: never taken.
     *
     * @return the lease store
     */
    public JdbcLeaseStore leases()
    {
        return new JdbcLeaseStore(dataSource(), settings.jdbc().table(),
                Duration.ofSeconds(10));
    }

    @Override
    public void close()
    {
        if (pool != null)
        {
            pool.close();
        }
    }
}
