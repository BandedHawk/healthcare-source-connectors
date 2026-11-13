/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.runtime;

import com.sdh.healthcare.connector.broker.receive.ArtemisReceiveRoute;
import com.sdh.healthcare.connector.broker.receive.BrokerKind;
import com.sdh.healthcare.connector.broker.receive.CamelReceiveRoute;
import com.sdh.healthcare.connector.broker.receive.ReceiveSettings;
import com.sdh.healthcare.connector.core.landing.GivenUpItems;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.intake.IntakeHealth;
import com.sdh.healthcare.connector.landing.intake.IntakeService;
import com.sdh.healthcare.connector.landing.intake.RemoteSource;
import com.sdh.healthcare.connector.landing.intake.ResubmitPoller;
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
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
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
        registerGauges(telemetry, runtime, properties);
        return runtime;
    }

    /**
     * Adds the landing and intake health gauges (design §5.3.4, §5.3.7),
     * each by intake. The health gauges are for intakes that read a
     * source: a listing, or a queue; an HTTP intake reads nothing.
     *
     * @param telemetry  the installed telemetry
     * @param runtime    the runtime
     * @param properties the node's settings
     */
    private static void registerGauges(Telemetry telemetry,
            LandingRuntime runtime, LandingProperties properties)
    {
        Fields intake = Fields.of(Attr.INTAKE, properties.landing().intake());
        telemetry.gauge("sdh.landing.ledger_items", "{item}",
                "Entries in the intake's ledger", intake,
                () -> runtime.intake().ledgerItems());
        IntakeHealth health = runtime.health();
        if (health != null)
        {
            telemetry.gauge("sdh.intake.up", "1",
                    "1 while the source answers, 0 during an outage", intake,
                    () -> health.reachable() ? 1 : 0);
            telemetry.gauge("sdh.intake.last_poll_age", "s",
                    "Seconds since the source last answered a listing or"
                            + " poll",
                    intake, health::secondsSinceAnswered);
            telemetry.gauge("sdh.intake.outage_seconds", "s",
                    "Seconds since the current outage began; 0 when there"
                            + " is none",
                    intake, health::outageSeconds);
        }
        IntakeService service = runtime.service();
        if (service != null)
        {
            telemetry.gauge("sdh.intake.listed_items", "{item}",
                    "Items the last listing found", intake,
                    service::listedItems);
            telemetry.gauge("sdh.landing.backlog", "{item}",
                    "Items listed and not yet landed", intake,
                    service::waiting);
            telemetry.gauge("sdh.landing.oldest_waiting_age", "s",
                    "Seconds the longest-waiting listed item has waited",
                    intake, service::oldestWaitingSeconds);
        }
        if (runtime.pushService() != null)
        {
            telemetry.gauge("sdh.landing.backlog", "{item}",
                    "Pushes waiting for the intake", intake,
                    runtime.pushService()::waiting);
        }
    }

    /**
     * Runs table maintenance on the landing tables between items (design
     * §5.3.13). Retention purges whole days of {@code landed_items}, never
     * part of an item, and the chunks of items given up are removed after
     * {@code retention.given-up}; the ledger is kept forever.
     *
     * @param runtime    the runtime
     * @param properties the node's settings
     * @param fatal      the exit hook
     * @return the scheduler
     */
    /**
     * Carries out operators' requests to land an item again (STAGES.md
     * §13.5).
     *
     * @param runtime    the node's runtime
     * @param properties the node's settings
     * @param fatal      stops the process for a fatal error
     * @return the poller
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    ResubmitPoller resubmitPoller(LandingRuntime runtime,
            LandingProperties properties, FatalExit fatal)
    {
        return new ResubmitPoller(properties.landing().intake(),
                runtime.intake(), runtime.resubmitter(), fatal,
                ResubmitPoller.INTERVAL);
    }

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
        GivenUpItems givenUp = new GivenUpItems(tables.committer(),
                tables.ids(), properties.landing().intake(),
                settings.retention().givenUp());
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
                                wholeItems, givenUp),
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
     * @param properties the settings, for the retry backoff
     * @return the route; one with nothing in it for HTTP
     */
    @Bean
    RouteBuilder intakeRoute(LandingRuntime runtime,
            LandingProperties properties)
    {
        if (runtime.batchService() != null)
        {
            Duration backoff = properties.retry().initialBackoff();
            Duration longest = properties.retry().maxBackoff();
            ReceiveSettings receive = runtime.broker().receive();
            return receive.kind() == BrokerKind.ARTEMIS
                    ? new ArtemisReceiveRoute(IntakeRoute.ROUTE_ID,
                            Fields.of(Attr.INTAKE,
                                    properties.landing().intake()),
                            receive, runtime.batchService(),
                            runtime.health(), backoff, longest)
                    : new CamelReceiveRoute(IntakeRoute.ROUTE_ID, receive,
                            runtime.batchService(), backoff, longest);
        }
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
     * Stops a remote listing at the next item as soon as the node starts to
     * stop, before Camel stops its routes. A listing is one exchange, so
     * otherwise Camel waits for all of it, the pod is killed first, and the
     * intake's lease runs out instead of being released: a standby then
     * waits a whole TTL to take over.
     *
     * @param intakeRoute the route that lists the source
     * @return the listener
     */
    @Bean
    ApplicationListener<ContextClosedEvent> stopListing(
            RouteBuilder intakeRoute)
    {
        return event ->
        {
            if (intakeRoute instanceof RemoteIntakeRoute remote)
            {
                remote.stopListing();
            }
        };
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
            if (runtime.service() != null || runtime.batchService() != null)
            {
                camel.getRouteController().startRoute(IntakeRoute.ROUTE_ID);
            }
            if (runtime.brokerWatch() != null)
            {
                runtime.brokerWatch().start();
            }
            LOG.info("intake.started");
        };
    }
}
