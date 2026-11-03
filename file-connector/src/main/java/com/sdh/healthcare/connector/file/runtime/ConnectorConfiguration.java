/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.file.runtime;

import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.telemetry.InboundGauge;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.telemetry.TelemetryConfiguration;
import com.sdh.healthcare.connector.file.ingest.InboundService;
import com.sdh.healthcare.connector.file.route.InboundRoute;
import java.time.Clock;
import java.util.concurrent.locks.ReentrantLock;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
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
 * Spring wiring: runtime, inbound service and route, and the startup sequence.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@Configuration
@EnableConfigurationProperties(ConnectorProperties.class)
@Import(TelemetryConfiguration.class)
public class ConnectorConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectorConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    FatalExit fatalExit() {
        return FatalExit.exitProcess();
    }

    /** Takes {@link Telemetry} so the SDK is installed before the lease and pipeline emit anything. */
    @Bean(destroyMethod = "close")
    ConnectorRuntime connectorRuntime(ConnectorProperties props, CamelContext camel, FatalExit fatal,
            Telemetry telemetry) throws Exception {
        InboundGauge.register(telemetry, props.file().inboundDir());
        return ConnectorRuntime.start(props, camel, fatal);
    }

    /** Held while a file is handled; maintenance runs only when it can take it without waiting. */
    @Bean
    ReentrantLock writerLock() {
        return new ReentrantLock();
    }

    @Bean
    InboundService inboundService(ConnectorRuntime runtime, ConnectorProperties props, FatalExit fatal,
            ReentrantLock writerLock) {
        return new InboundService(runtime.processor(), runtime.claim(), props.file(),
                props.retry().initialBackoff(), fatal, writerLock);
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    MaintenanceScheduler maintenanceScheduler(ConnectorRuntime runtime, ConnectorProperties props,
            ReentrantLock writerLock, FatalExit fatal) {
        return new MaintenanceScheduler(runtime, props.maintenance(), writerLock, fatal, Clock.systemUTC());
    }

    @Bean
    InboundRoute inboundRoute(ConnectorProperties props, InboundService service) {
        return new InboundRoute(props.file(), service);
    }

    /** Finish any in-progress file before the inbound route is allowed to pick up new ones (design §5). */
    @Bean
    ApplicationRunner startInbound(InboundService service, CamelContext camel) {
        return args -> {
            service.recoverAtStartup();
            camel.getRouteController().startRoute(ConnectorRuntime.InboundRouteIds.INBOUND);
            LOG.info("inbound.started");
        };
    }
}
