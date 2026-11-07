/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.admin;

import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import java.util.List;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * The landing node's operator commands (design §11.4): {@code replay}, on
 * the stages' shared {@link AdminCli}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LandingAdmin
{

    /** Not instantiated. */
    private LandingAdmin()
    {
    }

    /**
     * The landing node's CLI.
     *
     * @return the CLI
     */
    public static AdminCli<LandingProperties, ?> cli()
    {
        return new AdminCli<LandingProperties, LandingAdminContext>(
                new AdminCli.Stage<>("landing-connector.jar",
                        "landing-connector", Configuration.class,
                        LandingProperties.class, LandingProperties::telemetry,
                        LandingAdminContext::new),
                List.of(new ReplayCommand()));
    }

    /**
     * Binds {@code sdh.*} and nothing else: not a component, so the node's
     * own scan never picks it up.
     */
    @EnableConfigurationProperties(LandingProperties.class)
    static class Configuration
    {

        /** Created by Spring. */
        Configuration()
        {
        }
    }
}
