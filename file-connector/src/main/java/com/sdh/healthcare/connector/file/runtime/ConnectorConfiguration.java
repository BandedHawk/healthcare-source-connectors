/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.runtime;

import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.inbox.InboxTables;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.inbox.InboxReconciler;
import com.sdh.healthcare.connector.file.inbox.InboxRoute;
import com.sdh.healthcare.connector.file.inbox.InboxWorker;
import com.sdh.healthcare.connector.file.inbox.ResultReporter;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import com.sdh.healthcare.connector.runtime.telemetry.TelemetryConfiguration;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.camel.CamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * The splitter's Spring wiring: the runtime, the inbox route and worker,
 * maintenance, and the startup sequence.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@Configuration
@EnableConfigurationProperties(ConnectorProperties.class)
@Import(TelemetryConfiguration.class)
public class ConnectorConfiguration
{

    /** Logs when the route starts. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ConnectorConfiguration.class);

    /** Created by Spring. */
    public ConnectorConfiguration()
    {
    }

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
     * before the lease and the pipeline record anything.
     *
     * @param properties the splitter's settings
     * @param camel      the Camel context
     * @param fatal      the exit hook
     * @param telemetry  the installed telemetry
     * @return the runtime
     * @throws Exception if it can't start
     */
    @Bean(destroyMethod = "close")
    ConnectorRuntime connectorRuntime(ConnectorProperties properties,
            CamelContext camel, FatalExit fatal, Telemetry telemetry)
            throws Exception
    {
        ConnectorRuntime runtime =
                ConnectorRuntime.start(properties, camel, fatal);
        telemetry.gauge("sdh.inbox.backlog", "{item}",
                "Landed items waiting to be split",
                () -> runtime.inbox().backlog());
        return runtime;
    }

    /**
     * Held while an item is processed; maintenance runs only when it can
     * take it without waiting.
     *
     * @return the lock
     */
    @Bean
    ReentrantLock writerLock()
    {
        return new ReentrantLock();
    }

    /**
     * Splits the inbox's items. Closed before the runtime, so no item is
     * being processed when the lease is released.
     *
     * @param runtime    the runtime
     * @param properties the splitter's settings
     * @param fatal      the exit hook
     * @param writerLock held while an item is processed
     * @return the worker, not yet started
     */
    @Bean(destroyMethod = "close")
    InboxWorker inboxWorker(ConnectorRuntime runtime,
            ConnectorProperties properties, FatalExit fatal,
            ReentrantLock writerLock)
    {
        ResultReporter results = runtime.results()
                .map(channel -> new ResultReporter(channel,
                        runtime.processor(), runtime.tables().landedItems(),
                        runtime.keys().wrap(), properties.landing().intake(),
                        Clock.systemUTC()))
                .orElse(null);
        InboxWorker worker = new InboxWorker(runtime.processor(),
                runtime.inbox(), runtime.tables().landedItems(),
                runtime.keys().wrap(), properties.landing().idleWait(),
                properties.retry().initialBackoff(), fatal, writerLock,
                results);
        ConnectorProperties.Reconcile reconcile =
                properties.landing().reconcile();
        if (reconcile.enabled())
        {
            worker.reconcileWith(new InboxReconciler(runtime.inbox(),
                    runtime.tables().landedItems(),
                    properties.landing().intake(),
                    properties.landing().storage().catalogName(),
                    properties.landing().storage().namespace(),
                    reconcile.grace(), Clock.systemUTC()),
                    reconcile.interval());
        }
        return worker;
    }

    /**
     * Runs table maintenance between items.
     *
     * @param runtime    the runtime
     * @param properties the splitter's settings
     * @param writerLock held while an item is processed
     * @param fatal      the exit hook
     * @return the scheduler
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    MaintenanceScheduler maintenanceScheduler(ConnectorRuntime runtime,
            ConnectorProperties properties, ReentrantLock writerLock,
            FatalExit fatal)
    {
        ConnectorProperties.Maintenance settings = properties.maintenance();
        // Only records has retention: the inbox keeps every row, to
        // recognise items delivered again.
        Optional<Duration> records = MaintenanceScheduler.retention(
                "sdh.maintenance.retention.records",
                settings.retention().records(), Duration.ofDays(1));
        return new MaintenanceScheduler(runtime.committer(),
                settings.toSettings(), settings.toSchedule(),
                () -> List.of(
                        MaintenanceScheduler.Target.of(
                                runtime.tables().records(),
                                RecordsTable.INGESTED_AT, records),
                        MaintenanceScheduler.Target.of(
                                runtime.tables().inbox(),
                                InboxTables.RECEIVED_AT, Optional.empty())),
                writerLock, fatal, Clock.systemUTC());
    }

    /**
     * The route that takes {@code item.landed} messages into the inbox.
     *
     * @param runtime    the runtime
     * @param properties the splitter's settings
     * @param fatal      the exit hook
     * @return the route
     */
    @Bean
    InboxRoute inboxRoute(ConnectorRuntime runtime,
            ConnectorProperties properties, FatalExit fatal)
    {
        return new InboxRoute(properties.landing().queueUri(),
                runtime.receiver(), runtime.retry(), fatal);
    }

    /**
     * Starts the worker, which first finishes or abandons any item left
     * in progress, then the route (design §5.3.6).
     *
     * @param worker the worker
     * @param camel  the Camel context
     * @return the startup step
     */
    @Bean
    ApplicationRunner startInbox(InboxWorker worker, CamelContext camel)
    {
        return arguments ->
        {
            worker.start();
            camel.getRouteController().startRoute(InboxRoute.ROUTE_ID);
            LOG.info("inbox.started");
        };
    }
}
