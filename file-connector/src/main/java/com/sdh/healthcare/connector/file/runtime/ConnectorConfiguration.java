/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.runtime;

import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.telemetry.TelemetryConfiguration;
import org.apache.camel.CamelContext;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * The split stage's Spring wiring: the runtime, and starting its duties
 * and input once the application is up.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@Configuration
@EnableConfigurationProperties(ConnectorProperties.class)
@Import(TelemetryConfiguration.class)
public class ConnectorConfiguration
{

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
     * Sets up the runtime. Takes {@link Telemetry} so the SDK is installed
     * before the pipeline records anything.
     *
     * @param properties the stage's settings
     * @param camel      the Camel context
     * @param fatal      the exit hook
     * @param telemetry  the installed telemetry
     * @return the runtime, its input and duties not yet started
     * @throws Exception if it can't be set up
     */
    @Bean(destroyMethod = "close")
    ConnectorRuntime connectorRuntime(ConnectorProperties properties,
            CamelContext camel, FatalExit fatal, Telemetry telemetry)
            throws Exception
    {
        return ConnectorRuntime.create(properties, camel, fatal);
    }

    /**
     * Starts the duties and the input once the application is up, unless
     * {@code sdh.split.autostart} is false: for tests that hand messages
     * to the split handler themselves.
     *
     * @param runtime the runtime
     * @return the startup step
     */
    @Bean
    @ConditionalOnProperty(name = "sdh.split.autostart", havingValue = "true",
            matchIfMissing = true)
    ApplicationRunner startSplit(ConnectorRuntime runtime)
    {
        return arguments -> runtime.start();
    }
}
