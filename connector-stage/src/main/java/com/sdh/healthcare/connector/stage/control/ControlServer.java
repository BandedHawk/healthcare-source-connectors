/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import com.sdh.healthcare.connector.stage.admin.AdminCli;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Runs a stage's control API: {@code java -jar <stage>.jar control
 * [--a.b=c ...]} (CONTROL-API-DESIGN.md §1), as the stage's control
 * Deployment, from the stage's own image and settings. It starts none of
 * the stage: no Camel, no lease, no consumer. Each request opens what its
 * operation reads, as an operator command does, and closes it after.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ControlServer
{

    /** The first argument that starts the control API. */
    public static final String COMMAND = "control";

    /** Not instantiated. */
    private ControlServer()
    {
    }

    /**
     * The stage's operator commands, for the control API's beans.
     *
     * @param cli the commands
     */
    public record Commands(AdminCli<Object, ?> cli)
    {
    }

    /**
     * Runs the control API until the process is stopped.
     *
     * @param cli  the stage's operator commands
     * @param args the command line, after {@code control}: only
     *             {@code --a.b=c} settings
     */
    public static void serve(AdminCli<?, ?> cli, List<String> args)
    {
        start(cli, args);
    }

    /**
     * Starts the control API.
     *
     * @param cli  the stage's operator commands
     * @param args the command line, after {@code control}: only
     *             {@code --a.b=c} settings
     * @return the running API, to close to stop it
     * @throws IllegalArgumentException if an argument isn't a setting
     */
    @SuppressWarnings("unchecked")
    public static ConfigurableApplicationContext start(AdminCli<?, ?> cli,
            List<String> args)
    {
        for (String arg : args)
        {
            if (!arg.startsWith("--") || !arg.contains("="))
            {
                throw new IllegalArgumentException("control takes only"
                        + " --a.b=c settings");
            }
        }
        AdminCli<Object, ?> stage = (AdminCli<Object, ?>) cli;
        SpringApplication application = new SpringApplication(
                ControlConfiguration.class, stage.stage().configuration());
        application.setWebApplicationType(WebApplicationType.SERVLET);
        application.setBannerMode(Banner.Mode.OFF);
        application.setDefaultProperties(Map.of(
                "logging.level.org.apache.hadoop", "ERROR",
                // A failure is answered by its class alone; never let a
                // default error page show its message.
                "server.error.include-message", "never",
                "server.error.include-stacktrace", "never",
                "server.error.whitelabel.enabled", "false",
                "spring.mvc.problemdetails.enabled", "false"));
        application.addInitializers(context -> context.getBeanFactory()
                .registerSingleton("sdhControlCommands", new Commands(stage)));
        List<String> all = new ArrayList<>(args);
        all.add("--" + ControlSettings.MODE + "=true");
        return application.run(all.toArray(String[]::new));
    }
}
