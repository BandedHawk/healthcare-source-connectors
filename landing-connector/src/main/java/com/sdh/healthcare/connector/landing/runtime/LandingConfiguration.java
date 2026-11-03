/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.runtime;

import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.route.IntakeRoute;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.telemetry.TelemetryConfiguration;
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
        telemetry.gauge("sdh.landing.source_reachable", "1",
                "1 if the intake's source could be listed last time",
                () -> runtime.service().reachable() ? 1 : 0);
        telemetry.gauge("sdh.landing.items_waiting", "{item}",
                "Items listed and waiting to settle",
                () -> runtime.service().waiting());
        return runtime;
    }

    /**
     * The route that lists the source.
     *
     * @param runtime the runtime
     * @return the route
     */
    @Bean
    IntakeRoute intakeRoute(LandingRuntime runtime)
    {
        return new IntakeRoute(runtime.source(), runtime.service());
    }

    /**
     * Recovers from any crash, then starts listing (design §5.3.5).
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
            runtime.service().recoverAtStartup();
            camel.getRouteController().startRoute(IntakeRoute.ROUTE_ID);
            LOG.info("intake.started");
        };
    }
}
