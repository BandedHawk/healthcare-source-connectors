/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.core.env.MapPropertySource;
import org.springframework.mock.env.MockEnvironment;

/**
 * Tests for {@link WebServerSwitch}: a web server only on an HTTP intake,
 * and never for an application set to run none, as an operator command is.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class WebServerSwitchTest
{

    /**
     * The web application type the switch gives.
     *
     * @param intakeType the intake's type, or null for none set
     * @param type       the application's type before
     * @return what the switch set
     */
    static String switched(String intakeType, WebApplicationType type)
    {
        MockEnvironment environment = new MockEnvironment();
        if (intakeType != null)
        {
            environment.getPropertySources().addLast(new MapPropertySource(
                    "test", Map.of(WebServerSwitch.TYPE, intakeType)));
        }
        SpringApplication application = new SpringApplication();
        application.setWebApplicationType(type);
        new WebServerSwitch().postProcessEnvironment(environment,
                application);
        return environment.getProperty("spring.main.web-application-type");
    }

    /** Only an HTTP intake's node runs a web server. */
    @Test
    void runsAWebServerOnlyOnAnHttpIntake()
    {
        assertThat(switched("http", WebApplicationType.SERVLET))
                .isEqualTo("servlet");
        assertThat(switched(" HTTP ", WebApplicationType.SERVLET))
                .isEqualTo("servlet");
        assertThat(switched("sftp", WebApplicationType.SERVLET))
                .isEqualTo("none");
        assertThat(switched(null, WebApplicationType.SERVLET))
                .isEqualTo("none");
    }

    /**
     * An operator command on an HTTP intake's node starts no web server:
     * before, it tried to, and couldn't start.
     */
    @Test
    void keepsAnOperatorCommandOffTheWeb()
    {
        assertThat(switched("http", WebApplicationType.NONE))
                .isEqualTo("none");
    }
}
