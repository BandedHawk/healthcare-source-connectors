/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.telemetry.AuditJournals;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * A stage's operator commands: {@code java -jar <stage>.jar admin <command>
 * …} (design §11.4).
 *
 * <p>Uses the stage's own configuration (application.yml, environment,
 * {@code SPRING_APPLICATION_JSON}, {@code --a.b=c} overrides) but starts
 * none of the stage: no Camel, no lease, no writes. Every command emits an
 * {@code sdh.audit.admin.<command>} event naming the operator, which is
 * required: {@code --operator <name>} or {@code $SDH_OPERATOR}. The option
 * is for {@code kubectl exec}, which can't set an environment variable,
 * into an image with no shell or {@code env} (design §14.3).
 *
 * <p>Each run is journaled in the stage's durable audit journal (HIPAA.md
 * §3.1), as a chain of its own, unsealed, since a command holds no stage
 * key: a command whose run can't be journaled doesn't run. Every stage
 * also has the journal's own commands, {@code audit-verify},
 * {@code audit-export} and {@code audit-prune} ({@link AuditCommands}).
 *
 * <p>Exit status: 0 done, 1 failed, 2 usage error.
 *
 * @param <P> the stage's settings
 * @param <C> what its commands read from
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AdminCli<P, C extends AutoCloseable>
{

    /**
     * The name an operator command's reads of stored records are audited
     * under (HIPAA.md §4.3); the run's own event, in the same chain, names
     * the operator.
     */
    public static final String ADMIN_READER = "sdh-admin";

    /** The environment variable naming the operator. */
    public static final String OPERATOR_ENV = "SDH_OPERATOR";

    /**
     * The same, as an option of every command; it wins over the
     * environment variable.
     */
    public static final String OPERATOR_OPTION = "operator";

    /**
     * An account or ticket-style name: enough to identify who ran it,
     * nothing free-form.
     */
    private static final Pattern OPERATOR =
            Pattern.compile("[A-Za-z0-9._@+-]{1,128}");

    /** What the stage is, for usage and telemetry. */
    private final Stage<P, C> stage;

    /** The commands, by name. */
    private final Map<String, AdminCommand<C>> commands =
            new LinkedHashMap<>();

    /** The audit journal's commands, by name. */
    private final Map<String, AdminCommand<AuditTarget>> auditCommands =
            new LinkedHashMap<>();

    /**
     * What a stage gives its CLI.
     *
     * @param jar           the stage's jar, for the usage text, such as
     *                      {@code file-connector.jar}
     * @param service       the service name audit events are sent under
     * @param configuration a class that binds the stage's settings
     *                      ({@code @EnableConfigurationProperties}) and
     *                      nothing else
     * @param settings      the settings' class
     * @param telemetry     the telemetry settings within them
     * @param context       opens what the commands read from
     * @param audit         where the stage's audit journal is
     * @param <P>           the stage's settings
     * @param <C>           what its commands read from
     */
    public record Stage<P, C>(String jar, String service,
            Class<?> configuration, Class<P> settings,
            Function<P, CommonSettings.Telemetry> telemetry,
            Function<P, C> context, Function<P, AuditTarget> audit)
    {
    }

    /**
     * Creates the CLI.
     *
     * @param stage    what the stage is
     * @param commands its commands
     */
    public AdminCli(Stage<P, C> stage, List<AdminCommand<C>> commands)
    {
        this.stage = stage;
        for (AdminCommand<C> command : commands)
        {
            this.commands.put(command.name(), command);
        }
        for (AdminCommand<AuditTarget> command : AuditCommands.all())
        {
            this.auditCommands.put(command.name(), command);
        }
    }

    /**
     * What the stage is.
     *
     * @return it
     */
    public Stage<P, C> stage()
    {
        return stage;
    }

    /**
     * One of the stage's commands, as an operation the control API can
     * serve (CONTROL-API-DESIGN.md §3).
     *
     * @param name the command's name, such as {@code stuck}
     * @return it; empty if the stage has no such command, or it isn't an
     *         operation yet
     */
    public Optional<CliOperation<?, ?, ?>> operation(String name)
    {
        AdminCommand<?> command = commands.containsKey(name)
                ? commands.get(name) : auditCommands.get(name);
        return command instanceof CliOperation<?, ?, ?> operation
                ? Optional.of(operation) : Optional.empty();
    }

    /**
     * Opens what a command reads from: the stage's context, or for the
     * audit journal's commands, the journal. Nothing is connected until
     * the command reads.
     *
     * @param name     the command's name
     * @param settings the stage's settings
     * @return what it reads from, to close after the command
     */
    public AutoCloseable open(String name, P settings)
    {
        if (auditCommands.containsKey(name))
        {
            return stage.audit().apply(settings);
        }
        return stage.context().apply(settings);
    }

    /**
     * Runs a command.
     *
     * @param args the command and its options, after {@code admin}
     * @param out  where results go
     * @param err  where errors and usage go
     * @param env  the environment
     * @return the exit status: 0 done, 1 failed, 2 usage error
     */
    public int run(List<String> args, PrintStream out, PrintStream err,
            Map<String, String> env)
    {
        AdminArgs parsed;
        AdminCommand<?> command;
        String operator;
        try
        {
            if (args.isEmpty() || args.getFirst().equals("help")
                    || args.getFirst().equals("--help"))
            {
                out.println(usage());
                return args.isEmpty() ? 2 : 0;
            }
            command = commands.containsKey(args.getFirst())
                    ? commands.get(args.getFirst())
                    : auditCommands.get(args.getFirst());
            if (command == null)
            {
                throw new AdminArgs.UsageException(
                        "Unknown command: " + args.getFirst());
            }
            parsed = AdminArgs.parse(args, command.flags());
            Set<String> allowed = new HashSet<>(command.options());
            allowed.addAll(command.flags());
            allowed.add(OPERATOR_OPTION);
            parsed.allowOnly(allowed);
            operator = parsed.option(OPERATOR_OPTION)
                    .orElse(env.get(OPERATOR_ENV));
            if (operator == null || !OPERATOR.matcher(operator).matches())
            {
                throw new AdminArgs.UsageException("--" + OPERATOR_OPTION
                        + " or $" + OPERATOR_ENV
                        + " must name who is running this (letters, digits,"
                        + " . _ @ + -), for the audit trail");
            }
        }
        catch (AdminArgs.UsageException failure)
        {
            err.println("error: " + failure.getMessage());
            err.println();
            err.println(usage());
            return 2;
        }

        AuditRecord audit = new AuditRecord(operator);
        String event = "admin." + command.name().replace('-', '_');
        try (ConfigurableApplicationContext spring =
                        configuration(parsed.configArgs());
                AdminTelemetry telemetry = AdminTelemetry.install(
                        stage.telemetry().apply(settings(spring)),
                        stage.service());
                AuditTarget target = stage.audit().apply(settings(spring));
                AuditJournals.Opened journal = journal(command, target,
                        stage.telemetry().apply(settings(spring)), err);
                C context = stage.context().apply(settings(spring)))
        {
            if (journal == null)
            {
                return 1;
            }
            try
            {
                run(command, parsed, context, target, audit, out);
                Telemetry.get().audit(event,
                        audit.fields().with(Attr.RESULT, "ok"));
                return 0;
            }
            catch (AdminArgs.UsageException failure)
            {
                err.println("error: " + failure.getMessage());
                return 2;
            }
            catch (Exception failure)
            {
                Telemetry.get().auditWarn(event, audit.fields()
                        .with(Attr.RESULT, "failed")
                        .with(Attr.REASON,
                                failure.getClass().getSimpleName()));
                if (!(failure instanceof StatusParts.Unavailable))
                {
                    err.println("error: " + failure.getClass().getSimpleName()
                            + ": " + failure.getMessage());
                }
                return 1;
            }
        }
        catch (Exception failure)
        {
            err.println("error: could not load the configuration: "
                    + failure.getClass().getSimpleName() + ": "
                    + failure.getMessage());
            return 1;
        }
    }

    /**
     * Opens the audit journal for a command's run. If it can't be opened,
     * as when the database is down, a command that only reads runs all
     * the same, audited in the console log and export only, with a
     * warning; any other is refused.
     *
     * @param command   the command
     * @param target    where the stage's journal is
     * @param telemetry the stage's telemetry settings
     * @param err       where the warning or error goes
     * @return the journal, or one that keeps nothing; null to refuse
     */
    private static AuditJournals.Opened journal(AdminCommand<?> command,
            AuditTarget target, CommonSettings.Telemetry telemetry,
            PrintStream err)
    {
        try
        {
            return AuditJournals.start(target.lease(), telemetry,
                    target.tables(), target.source() + "/admin", null);
        }
        catch (RuntimeException failure)
        {
            if (command.readsOnly())
            {
                err.println("warning: the audit journal can't be written ("
                        + failure.getClass().getSimpleName() + "); this run"
                        + " is audited in the console log and export only");
                return AuditJournals.none();
            }
            err.println("error: the audit journal can't be written ("
                    + failure.getClass().getSimpleName() + "), so this"
                    + " command, which acts or reads PHI, doesn't run");
            return null;
        }
    }

    /**
     * Runs a command with what it reads from: the stage's context, or for
     * the audit journal's commands, the journal.
     *
     * @param command   the command
     * @param arguments its options
     * @param context   the stage's context
     * @param target    the stage's audit journal
     * @param audit     the audit event
     * @param out       where results go
     * @throws Exception if the command fails
     */
    @SuppressWarnings("unchecked")
    private void run(AdminCommand<?> command, AdminArgs arguments, C context,
            AuditTarget target, AuditRecord audit, PrintStream out)
            throws Exception
    {
        if (auditCommands.containsValue(command))
        {
            ((AdminCommand<AuditTarget>) command).run(arguments, target,
                    audit, out);
        }
        else
        {
            ((AdminCommand<C>) command).run(arguments, context, audit, out);
        }
    }

    /**
     * The stage's settings, from the configuration.
     *
     * @param spring the configuration
     * @return the settings
     */
    private P settings(ConfigurableApplicationContext spring)
    {
        return spring.getBean(stage.settings());
    }

    /**
     * The stage's configuration, bound exactly as the stage binds it, in a
     * context that contains nothing else. Logging is quiet apart from audit
     * events, so command output stays readable.
     *
     * @param configArgs the {@code --a.b=c} overrides
     * @return the running context
     */
    private ConfigurableApplicationContext configuration(
            List<String> configArgs)
    {
        SpringApplication application =
                new SpringApplication(stage.configuration());
        application.setWebApplicationType(WebApplicationType.NONE);
        application.setBannerMode(Banner.Mode.OFF);
        application.setLogStartupInfo(false);
        application.setRegisterShutdownHook(false);
        application.setDefaultProperties(Map.of(
                "logging.level.root", "WARN",
                "logging.level.sdh.audit", "INFO",
                // Hadoop warns that its native library is missing, which is
                // expected and harmless here.
                "logging.level.org.apache.hadoop", "ERROR"));
        return application.run(configArgs.toArray(String[]::new));
    }

    /**
     * The usage text of every command.
     *
     * @return the text
     */
    public String usage()
    {
        List<String> parts = new ArrayList<>();
        parts.add("usage: java -jar " + stage.jar() + " admin <command>"
                + " [options] [--sdh.some.setting=value ...]\n"
                + "Requires --" + OPERATOR_OPTION + " <name> or $"
                + OPERATOR_ENV
                + " (who is running it; recorded in the audit event).\n"
                + "Uses the stage's configuration; --a.b=c arguments"
                + " override it.");
        commands.values().forEach(command -> parts.add(command.usage()));
        auditCommands.values()
                .forEach(command -> parts.add(command.usage()));
        return String.join("\n\n", parts);
    }
}
