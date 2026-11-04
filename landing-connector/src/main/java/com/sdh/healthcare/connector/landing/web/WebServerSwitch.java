/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.web;

import java.util.Locale;
import java.util.Map;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Runs the web server only on an HTTP push intake (design §5.3.15). Every
 * other intake has no web server and listens on no port, whatever
 * {@code spring.main.web-application-type} says, so a directory or SFTP
 * node can't be reached over the network.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class WebServerSwitch implements EnvironmentPostProcessor
{

    /** The setting that picks the intake's type. */
    static final String TYPE = "sdh.landing.source.type";

    /** The property source this adds, ahead of all others. */
    static final String SOURCE_NAME = "sdh-web-server-switch";

    /**
     * Sets the web application type from the intake's type.
     *
     * @param environment the environment, with the node's settings loaded
     * @param application the application
     */
    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment,
            SpringApplication application)
    {
        String type = environment.getProperty(TYPE, "directory");
        boolean http = "http".equals(type.trim().toLowerCase(Locale.ROOT));
        environment.getPropertySources().addFirst(new MapPropertySource(
                SOURCE_NAME, Map.of("spring.main.web-application-type",
                        http ? "servlet" : "none")));
    }
}
