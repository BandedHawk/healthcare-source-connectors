/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.core.audit.AuditTables;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AuditTarget;
import com.sdh.healthcare.connector.stage.admin.DataCommands;
import com.sdh.healthcare.connector.stage.admin.DrainCheckCommand;
import com.sdh.healthcare.connector.stage.admin.DrainCommand;
import com.sdh.healthcare.connector.stage.admin.FailureCommands;
import com.sdh.healthcare.connector.stage.admin.KeyCommands;
import com.sdh.healthcare.connector.stage.admin.ReprocessCommand;
import com.sdh.healthcare.connector.stage.admin.VersionCommand;
import java.util.ArrayList;
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
                        SplitAdminContext::new,
                        properties -> new AuditTarget(properties.lease(),
                                properties.crypto(),
                                new AuditTables(properties.stage()
                                        .registrySchema()),
                                ItemProcessor.STAGE + "/"
                                        + properties.connectorId())),
                commands());
    }

    /**
     * The stage's commands, with those for its fail store.
     *
     * @return the commands
     */
    static List<AdminCommand<SplitAdminContext>> commands()
    {
        List<AdminCommand<SplitAdminContext>> commands =
                new ArrayList<>(List.of(
                        new RestoreCommand(), new SplitStatusCommand(),
                        new ReleaseCommand(),
                        new ReprocessCommand<SplitAdminContext>(),
                        new DrainCheckCommand<SplitAdminContext>(),
                        new DrainCommand<SplitAdminContext>(),
                        new VersionCommand<SplitAdminContext>()));
        commands.addAll(FailureCommands.<SplitAdminContext>all());
        commands.addAll(KeyCommands.<SplitAdminContext>all());
        commands.addAll(DataCommands.<SplitAdminContext>all());
        return List.copyOf(commands);
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
