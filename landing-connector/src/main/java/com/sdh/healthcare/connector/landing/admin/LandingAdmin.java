/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.admin;

import com.sdh.healthcare.connector.core.audit.AuditTables;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AuditTarget;
import com.sdh.healthcare.connector.stage.admin.DataCommands;
import com.sdh.healthcare.connector.stage.admin.KeyCommands;
import com.sdh.healthcare.connector.stage.admin.RestoreCommands;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * The landing node's operator commands (design §11.4): {@code resubmit},
 * and the key commands (CONTROL-API-DESIGN.md §11), on the stages' shared
 * {@link AdminCli}. Its {@code replay} went with
 * STAGES.md §15.8.10 step 5: an item is split again by a reprocess at the
 * split stage.
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
                        LandingAdminContext::new,
                        properties -> new AuditTarget(properties.lease(),
                                properties.crypto(),
                                AuditTables.besideLeaseTable(
                                        properties.lease().jdbc().table()),
                                "landing/" + properties.connectorId())),
                commands());
    }

    /**
     * The node's commands: {@code resubmit}, and the key commands.
     *
     * @return them
     */
    static List<AdminCommand<LandingAdminContext>> commands()
    {
        List<AdminCommand<LandingAdminContext>> commands =
                new ArrayList<>(List.of(new ResubmitCommand()));
        commands.addAll(KeyCommands.<LandingAdminContext>all());
        commands.addAll(DataCommands.<LandingAdminContext>all());
        commands.addAll(RestoreCommands.<LandingAdminContext>all());
        return List.copyOf(commands);
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
