/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime;

import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

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

    /** Not used: this class only has static methods. */
    private LeaseDatabase()
    {
    }

    /**
     * Opens a pool for the lease database.
     *
     * @param jdbc the database's settings
     * @return the pool; the caller closes it
     * @throws IllegalArgumentException if no JDBC URL is set
     */
    public static HikariDataSource dataSource(CommonSettings.Jdbc jdbc)
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
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(CONNECTION_TIMEOUT_MS);
        config.setPoolName("sdh-lease");
        return new HikariDataSource(config);
    }
}
