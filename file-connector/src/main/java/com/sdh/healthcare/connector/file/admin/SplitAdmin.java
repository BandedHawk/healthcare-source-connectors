/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import java.util.List;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * The split stage's operator commands (design §11.4): {@code restore} and
 * {@code status}, on the shared {@link AdminCli}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SplitAdmin
{

    /** Not instantiated. */
    private SplitAdmin()
    {
    }

    /**
     * The split stage's CLI.
     *
     * @return the CLI
     */
    public static AdminCli<ConnectorProperties, ?> cli()
    {
        return new AdminCli<ConnectorProperties, SplitAdminContext>(
                new AdminCli.Stage<>("file-connector.jar", "file-connector",
                        Configuration.class, ConnectorProperties.class,
                        ConnectorProperties::telemetry,
                        SplitAdminContext::new),
                List.of(new RestoreCommand(), new SplitStatusCommand()));
    }

    /**
     * Binds {@code sdh.*} and nothing else: not a component, so the
     * stage's own scan never picks it up.
     */
    @EnableConfigurationProperties(ConnectorProperties.class)
    static class Configuration
    {

        /** Created by Spring. */
        Configuration()
        {
        }
    }
}
