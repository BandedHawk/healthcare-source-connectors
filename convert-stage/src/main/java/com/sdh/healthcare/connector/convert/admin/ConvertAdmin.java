/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.admin;

import com.sdh.healthcare.connector.convert.config.ConvertProperties;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.StageStatusCommand;
import java.util.List;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * The convert stage's operator commands (design §11.4): {@code status}, on
 * the shared {@link AdminCli}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ConvertAdmin
{

    /** Not instantiated. */
    private ConvertAdmin()
    {
    }

    /**
     * The convert stage's CLI.
     *
     * @return the CLI
     */
    public static AdminCli<ConvertProperties, ?> cli()
    {
        return new AdminCli<ConvertProperties, ConvertAdminContext>(
                new AdminCli.Stage<>("convert-stage.jar", "convert-stage",
                        Configuration.class, ConvertProperties.class,
                        ConvertProperties::telemetry,
                        ConvertAdminContext::new),
                List.of(new StageStatusCommand<>(), new ReplayCommand()));
    }

    /**
     * Binds {@code sdh.*} and nothing else: not a component, so the
     * stage's own scan never picks it up.
     */
    @EnableConfigurationProperties(ConvertProperties.class)
    static class Configuration
    {

        /** Created by Spring. */
        Configuration()
        {
        }
    }
}
