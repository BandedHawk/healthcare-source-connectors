/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import java.util.Map;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Keeps a stage's workers off the network now that every stage carries a
 * web server for its control API (CONTROL-API-DESIGN.md §1). A worker runs
 * no web server and listens on no port unless its own settings ask for one,
 * as the landing node's HTTP intake does. The control process
 * ({@code control} on a stage's command line, {@link ControlServer}) runs a
 * servlet server whatever the stage's settings say.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ControlWebSwitch implements EnvironmentPostProcessor
{

    /** The setting that picks the web application type. */
    static final String TYPE = "spring.main.web-application-type";

    /** The property source added for a worker, behind all others. */
    static final String WORKER_SOURCE = "sdh-control-web-switch-worker";

    /** The property source added for the control process, ahead of all. */
    static final String CONTROL_SOURCE = "sdh-control-web-switch-control";

    /** Created by Spring. */
    public ControlWebSwitch()
    {
    }

    /**
     * Sets the web application type: a servlet server in control mode,
     * otherwise none unless the stage's settings say.
     *
     * @param environment the environment, with the stage's settings loaded
     * @param application the application
     */
    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment,
            SpringApplication application)
    {
        if (controlMode(environment))
        {
            environment.getPropertySources().addFirst(new MapPropertySource(
                    CONTROL_SOURCE, Map.of(TYPE, "servlet")));
        }
        else
        {
            environment.getPropertySources().addLast(new MapPropertySource(
                    WORKER_SOURCE, Map.of(TYPE, "none")));
        }
    }

    /**
     * Whether the process serves a control API.
     *
     * @param environment the environment
     * @return true in control mode
     */
    public static boolean controlMode(ConfigurableEnvironment environment)
    {
        return Boolean.parseBoolean(environment.getProperty(
                ControlSettings.MODE, "false").trim());
    }
}
