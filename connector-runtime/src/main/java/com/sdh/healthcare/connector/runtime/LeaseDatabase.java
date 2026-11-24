/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime;

import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.metrics.IMetricsTracker;

/**
 * The connection pool for the lease database (design §9.3). The lease is
 * the only thing kept there, so the pool is small.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LeaseDatabase
{

    /** How long to wait for a connection, in milliseconds. */
    private static final long CONNECTION_TIMEOUT_MS = 10_000;

    /** Not instantiated: this class only has static methods. */
    private LeaseDatabase()
    {
    }

    /** The step a wait for one of a stage's connections is timed as. */
    public static final String POOL_WAIT = "pool_wait";

    /**
     * Opens a pool for the lease database, of the size its settings give.
     *
     * @param jdbc the database's settings
     * @return the pool; the caller closes it
     * @throws IllegalArgumentException if no JDBC URL is set
     */
    public static HikariDataSource dataSource(CommonSettings.Jdbc jdbc)
    {
        return dataSource(jdbc, "sdh-lease", jdbc.poolSize());
    }

    /**
     * Opens a stage's pool for the lease database, of the size its settings
     * give, timing each wait for a connection as the stage's
     * {@value #POOL_WAIT} step (docs/PERFORMANCE.md §13.3).
     *
     * @param jdbc  the database's settings
     * @param stage the stage the waits are timed for, such as
     *              {@code split}
     * @return the pool; the caller closes it
     * @throws IllegalArgumentException if no JDBC URL is set
     */
    public static HikariDataSource dataSource(CommonSettings.Jdbc jdbc,
            String stage)
    {
        HikariConfig config = config(jdbc, "sdh-lease", jdbc.poolSize());
        config.setMetricsTrackerFactory((pool, stats) -> new WaitTimer(stage));
        return new HikariDataSource(config);
    }

    /**
     * Opens a pool of its own on the lease database, such as the one a
     * split pod waits for its turn to commit on (docs/PERFORMANCE.md §3.7).
     *
     * @param jdbc     the database's settings
     * @param poolName the pool's name, for its metrics and logs
     * @param size     the most connections it holds
     * @return the pool; the caller closes it
     * @throws IllegalArgumentException if no JDBC URL is set
     */
    public static HikariDataSource dataSource(CommonSettings.Jdbc jdbc,
            String poolName, int size)
    {
        return new HikariDataSource(config(jdbc, poolName, size));
    }

    /**
     * A pool's settings.
     *
     * @param jdbc     the database's settings
     * @param poolName the pool's name, for its metrics and logs
     * @param size     the most connections it holds
     * @return them
     * @throws IllegalArgumentException if no JDBC URL is set
     */
    private static HikariConfig config(CommonSettings.Jdbc jdbc,
            String poolName, int size)
    {
        if (jdbc.url() == null || jdbc.url().isBlank())
        {
            throw new IllegalArgumentException(
                    "sdh.lease.jdbc.url is required");
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbc.url());
        config.setUsername(jdbc.username());
        config.setPassword(jdbc.password());
        config.setMaximumPoolSize(size);
        config.setConnectionTimeout(CONNECTION_TIMEOUT_MS);
        config.setPoolName(poolName);
        return config;
    }

    /**
     * Times each wait for a connection as a stage's step.
     *
     * @param stage the stage
     */
    private record WaitTimer(String stage) implements IMetricsTracker
    {

        /**
         * Records one wait.
         *
         * @param nanos how long it took, in nanoseconds
         */
        @Override
        public void recordConnectionAcquiredNanos(long nanos)
        {
            Telemetry.get().phaseDuration(stage, POOL_WAIT, nanos);
        }
    }
}
