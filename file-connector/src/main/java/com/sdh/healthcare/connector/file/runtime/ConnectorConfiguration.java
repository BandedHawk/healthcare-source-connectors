package com.sdh.healthcare.connector.file.runtime;

import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.ingest.InboundService;
import com.sdh.healthcare.connector.file.route.InboundRoute;
import org.apache.camel.CamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Spring wiring: runtime, inbound service and route, and the startup sequence. */
@Configuration
@EnableConfigurationProperties(ConnectorProperties.class)
public class ConnectorConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectorConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    FatalExit fatalExit() {
        return reason -> {
            LOG.error("connector.fatal reason={} status={}", reason, FatalExit.STATUS);
            // Exit from another thread so Spring's shutdown hooks can run (and stop this route) without deadlock.
            Thread.ofPlatform().name("sdh-fatal-exit").start(() -> System.exit(FatalExit.STATUS));
        };
    }

    @Bean(destroyMethod = "close")
    ConnectorRuntime connectorRuntime(ConnectorProperties props, CamelContext camel, FatalExit fatal)
            throws Exception {
        return ConnectorRuntime.start(props, camel, fatal);
    }

    @Bean
    InboundService inboundService(ConnectorRuntime runtime, ConnectorProperties props, FatalExit fatal) {
        return new InboundService(runtime.processor(), props.file(), props.retry().initialBackoff(), fatal);
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
