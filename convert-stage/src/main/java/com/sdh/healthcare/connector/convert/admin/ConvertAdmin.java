/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.admin;

import com.sdh.healthcare.connector.convert.config.ConvertProperties;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.DrainCheckCommand;
import com.sdh.healthcare.connector.stage.admin.FailureCommands;
import com.sdh.healthcare.connector.stage.admin.ReprocessCommand;
import com.sdh.healthcare.connector.stage.admin.StageStatusCommand;
import com.sdh.healthcare.connector.stage.admin.VersionCommand;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * The convert stage's operator commands (design §11.4): {@code status},
 * {@code reprocess} and those for its fail store, on the shared
 * {@link AdminCli}. None sends to a broker (STAGES.md §15.8.10 step 5).
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
                commands());
    }

    /**
     * The stage's commands, with those for its fail store.
     *
     * @return the commands
     */
    static List<AdminCommand<ConvertAdminContext>> commands()
    {
        List<AdminCommand<ConvertAdminContext>> commands =
                new ArrayList<>(List.of(new StageStatusCommand<>(),
                        new ReprocessCommand<>(), new DrainCheckCommand<>(),
                        new VersionCommand<>()));
        commands.addAll(FailureCommands.<ConvertAdminContext>all());
        return List.copyOf(commands);
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
