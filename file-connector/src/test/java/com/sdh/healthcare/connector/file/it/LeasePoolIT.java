/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.requireDocker;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.LeaseDatabase;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.zaxxer.hikari.HikariDataSource;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * A stage's pool for its registry, leases and checkpoints
 * (docs/PERFORMANCE.md §13.3): as large as {@code sdh.lease.jdbc.pool-size}
 * says, and each wait for one of its connections timed as the stage's
 * {@code pool_wait} step.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class LeasePoolIT
{

    /** The database. */
    static PostgreSQLContainer postgres;

    /** Creates the tests; JUnit runs each test on a new instance. */
    LeasePoolIT()
    {
    }

    /** Starts Postgres. */
    @BeforeAll
    static void start()
    {
        requireDocker();
        postgres = new PostgreSQLContainer("postgres:18.4");
        postgres.start();
    }

    /** Stops Postgres. */
    @AfterAll
    static void stop()
    {
        if (postgres != null)
        {
            postgres.stop();
        }
    }

    /**
     * The pool holds as many connections as the setting gives, 2 unless
     * set.
     */
    @Test
    void thePoolIsAsLargeAsItsSetting()
    {
        try (HikariDataSource set = LeaseDatabase.dataSource(jdbc(6),
                "split");
                HikariDataSource unset = LeaseDatabase.dataSource(jdbc(2)))
        {
            assertThat(set.getMaximumPoolSize()).isEqualTo(6);
            assertThat(unset.getMaximumPoolSize()).isEqualTo(2);
        }
        assertThatThrownBy(() -> jdbc(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pool-size");
    }

    /**
     * Each connection taken from a stage's pool is timed as that stage's
     * {@code pool_wait} step.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void eachWaitForAConnectionIsTimedForItsStage() throws Exception
    {
        InMemoryMetricReader metrics = InMemoryMetricReader.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setMeterProvider(SdkMeterProvider.builder()
                        .registerMetricReader(metrics).build())
                .build())
        {
            Telemetry.install(sdk);
            try (HikariDataSource pool = LeaseDatabase.dataSource(jdbc(2),
                    "split"))
            {
                for (int i = 0; i < 3; i++)
                {
                    try (Connection connection = pool.getConnection())
                    {
                        assertThat(connection.isValid(5)).isTrue();
                    }
                }
            }
            Map<String, Long> waits = waits(metrics);
            assertThat(waits).containsEntry("split", 3L)
                    .doesNotContainKey("convert");
        }
        finally
        {
            Telemetry.install(OpenTelemetry.noop());
        }
    }

    /**
     * How many waits were timed, by stage.
     *
     * @param metrics what was recorded
     * @return the count of each stage's {@code pool_wait} step
     */
    private static Map<String, Long> waits(InMemoryMetricReader metrics)
    {
        Map<String, Long> waits = new HashMap<>();
        for (MetricData metric : metrics.collectAllMetrics())
        {
            if (!"sdh.stage.phase_duration".equals(metric.getName()))
            {
                continue;
            }
            for (HistogramPointData point
                    : metric.getHistogramData().getPoints())
            {
                if (LeaseDatabase.POOL_WAIT.equals(point.getAttributes()
                        .get(AttributeKey.stringKey("sdh.phase"))))
                {
                    waits.merge(point.getAttributes().get(
                            AttributeKey.stringKey("sdh.stage")),
                            point.getCount(), Long::sum);
                }
            }
        }
        return waits;
    }

    /**
     * The test database's settings.
     *
     * @param poolSize the pool size
     * @return them
     */
    static CommonSettings.Jdbc jdbc(int poolSize)
    {
        return new CommonSettings.Jdbc(postgres.getJdbcUrl(),
                postgres.getUsername(), postgres.getPassword(), "sdh_lease",
                poolSize);
    }
}
