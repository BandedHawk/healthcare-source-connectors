/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

/**
 * A worker runs no web server unless its settings ask for one; the
 * control process always runs one (CONTROL-API-DESIGN.md §1).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ControlWebSwitchTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    ControlWebSwitchTest()
    {
    }

    /**
     * A worker's web type is none by default, and what its settings say
     * when they say.
     */
    @Test
    void aWorkerRunsNoWebServerUnlessItsSettingsSay()
    {
        StandardEnvironment worker = new StandardEnvironment();
        new ControlWebSwitch().postProcessEnvironment(worker,
                new SpringApplication());
        assertThat(worker.getProperty(ControlWebSwitch.TYPE))
                .isEqualTo("none");

        StandardEnvironment asked = new StandardEnvironment();
        asked.getPropertySources().addFirst(new MapPropertySource("yml",
                Map.of(ControlWebSwitch.TYPE, "servlet")));
        new ControlWebSwitch().postProcessEnvironment(asked,
                new SpringApplication());
        assertThat(asked.getProperty(ControlWebSwitch.TYPE))
                .isEqualTo("servlet");
    }

    /** The control process runs a servlet server whatever is set. */
    @Test
    void theControlProcessRunsAServletServer()
    {
        StandardEnvironment control = new StandardEnvironment();
        control.getPropertySources().addFirst(new MapPropertySource("yml",
                Map.of(ControlWebSwitch.TYPE, "none",
                        ControlSettings.MODE, "true")));
        new ControlWebSwitch().postProcessEnvironment(control,
                new SpringApplication());
        assertThat(control.getProperty(ControlWebSwitch.TYPE))
                .isEqualTo("servlet");
        assertThat(ControlWebSwitch.controlMode(control)).isTrue();
    }
}
