/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.runtime;

import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.intake.RemoteSource;
import com.sdh.healthcare.connector.landing.route.IntakeRoute;
import com.sdh.healthcare.connector.landing.route.RemoteIntakeRoute;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import com.sdh.healthcare.connector.runtime.telemetry.TelemetryConfiguration;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * The landing node's Spring wiring: the runtime, the intake route, its
 * gauges, and the startup sequence of recovery, then listing.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@Configuration
@EnableConfigurationProperties(LandingProperties.class)
@Import(TelemetryConfiguration.class)
public class LandingConfiguration
{

    /** Logs when the route starts. */
    private static final Logger LOG =
            LoggerFactory.getLogger(LandingConfiguration.class);

    /**
     * Exits the process on a fatal error; tests put their own in its place.
     *
     * @return the exit hook
     */
    @Bean
    @ConditionalOnMissingBean
    FatalExit fatalExit()
    {
        return FatalExit.exitProcess();
    }

    /**
     * Starts the runtime. Takes {@link Telemetry} so the SDK is installed
     * before the lease and the lander record anything.
     *
     * @param properties the node's settings
     * @param camel      the Camel context
     * @param fatal      the exit hook
     * @param telemetry  the installed telemetry
     * @return the runtime
     * @throws Exception if it can't start
     */
    @Bean(destroyMethod = "close")
    LandingRuntime landingRuntime(LandingProperties properties,
            CamelContext camel, FatalExit fatal, Telemetry telemetry)
            throws Exception
    {
        LandingRuntime runtime =
                LandingRuntime.start(properties, camel, fatal);
        if (runtime.service() == null)
        {
            return runtime;
        }
        telemetry.gauge("sdh.landing.source_reachable", "1",
                "1 if the intake's source could be listed last time",
                () -> runtime.service().reachable() ? 1 : 0);
        telemetry.gauge("sdh.landing.items_waiting", "{item}",
                "Items listed and waiting to settle",
                () -> runtime.service().waiting());
        return runtime;
    }

    /**
     * Runs table maintenance on the landing tables between items (design
     * §5.3.13). Retention purges whole days of {@code landed_items}, never
     * part of an item; the ledger is kept forever.
     *
     * @param runtime    the runtime
     * @param properties the node's settings
     * @param fatal      the exit hook
     * @return the scheduler
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    MaintenanceScheduler maintenanceScheduler(LandingRuntime runtime,
            LandingProperties properties, FatalExit fatal)
    {
        LandingProperties.Maintenance settings = properties.maintenance();
        Optional<Duration> retention = MaintenanceScheduler.retention(
                "sdh.maintenance.retention.landed-items",
                settings.retention().landedItems(),
                LandingProperties.Maintenance.MIN_RETENTION);
        LandingRuntime.Tables tables = runtime.tables();
        UnaryOperator<Instant> wholeItems = cutoff ->
        {
            try
            {
                return LandedItems.keepFrom(tables.landedItems(), cutoff,
                        tables.lander().checkpoint());
            }
            catch (IOException failure)
            {
                throw new UncheckedIOException(failure);
            }
        };
        return new MaintenanceScheduler(tables.committer(),
                settings.toSettings(), settings.toSchedule(),
                () -> List.of(
                        new MaintenanceScheduler.Target(tables.landedItems(),
                                LandingTables.LANDED_AT, retention,
                                wholeItems),
                        MaintenanceScheduler.Target.of(tables.ledger(),
                                LandingTables.LANDED_AT, Optional.empty())),
                runtime.intake().writerLock(), fatal, Clock.systemUTC());
    }

    /**
     * The route that lists the source: Camel's file component for a
     * directory, the node's own listing for a remote source. An HTTP intake
     * has no route: its endpoint is {@code PushController}.
     *
     * @param runtime the runtime
     * @return the route; one with nothing in it for HTTP
     */
    @Bean
    RouteBuilder intakeRoute(LandingRuntime runtime)
    {
        if (runtime.pushService() != null)
        {
            return new RouteBuilder()
            {
                @Override
                public void configure()
                {
                }
            };
        }
        if (runtime.intakeSource() instanceof RemoteSource remote)
        {
            return new RemoteIntakeRoute(runtime.source(), remote,
                    runtime.service(), System::nanoTime);
        }
        return new IntakeRoute(runtime.source(), runtime.service());
    }

    /**
     * Recovers from any crash, then starts listing (design §5.3.5). An
     * HTTP intake takes pushes as soon as its server is up; each push
     * recovers first if this hasn't yet.
     *
     * @param runtime the runtime
     * @param camel   the Camel context
     * @return the startup step
     */
    @Bean
    ApplicationRunner startIntake(LandingRuntime runtime, CamelContext camel)
    {
        return arguments ->
        {
            runtime.intake().recoverAtStartup();
            if (runtime.service() != null)
            {
                camel.getRouteController().startRoute(IntakeRoute.ROUTE_ID);
            }
            LOG.info("intake.started");
        };
    }
}
