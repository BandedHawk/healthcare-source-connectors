package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Operator commands: {@code java -jar file-connector.jar admin <command> …} (design §11.4).
 *
 * <p>Uses the connector's own configuration (application.yml, environment, {@code SPRING_APPLICATION_JSON},
 * {@code --a.b=c} overrides) but starts none of the connector: no Camel, no lease, no writes. Every command emits an
 * {@code sdh.audit.admin.<command>} event naming the operator from {@code $SDH_OPERATOR}, which is required.
 *
 * <p>Exit status: 0 done, 1 failed, 2 usage error.
 */
public final class AdminCli {
    public static final String OPERATOR_ENV = "SDH_OPERATOR";
    /** An account or ticket-style name: enough to identify who ran it, nothing free-form. */
    private static final Pattern OPERATOR = Pattern.compile("[A-Za-z0-9._@+-]{1,128}");

    private AdminCli() {}

    public static void main(String[] args) {
        System.exit(run(Arrays.asList(args), System.out, System.err, System.getenv()));
    }

    public static int run(List<String> args, PrintStream out, PrintStream err, Map<String, String> env) {
        Map<String, AdminCommand> commands = new LinkedHashMap<>();
        for (AdminCommand c : List.of(new RestoreCommand(), new DecryptFallbackCommand(env), new StatusCommand())) {
            commands.put(c.name(), c);
        }
        AdminArgs parsed;
        AdminCommand command;
        String operator;
        try {
            if (args.isEmpty() || args.getFirst().equals("help") || args.getFirst().equals("--help")) {
                out.println(usage());
                return args.isEmpty() ? 2 : 0;
            }
            command = commands.get(args.getFirst());
            if (command == null) {
                throw new AdminArgs.UsageException("Unknown command: " + args.getFirst());
            }
            parsed = AdminArgs.parse(args, command.flags());
            parsed.allowOnly(command.options());
            operator = env.get(OPERATOR_ENV);
            if (operator == null || !OPERATOR.matcher(operator).matches()) {
                throw new AdminArgs.UsageException("$" + OPERATOR_ENV
                        + " must name who is running this (letters, digits, . _ @ + -), for the audit trail");
            }
        } catch (AdminArgs.UsageException e) {
            err.println("error: " + e.getMessage());
            err.println();
            err.println(usage());
            return 2;
        }

        AuditRecord audit = new AuditRecord(operator);
        String event = "admin." + command.name().replace('-', '_');
        try (ConfigurableApplicationContext spring = configuration(parsed.configArgs());
                AdminTelemetry telemetry = AdminTelemetry.install(spring.getBean(ConnectorProperties.class));
                AdminContext ctx = new AdminContext(spring.getBean(ConnectorProperties.class))) {
            try {
                command.run(parsed, ctx, audit, out);
                Telemetry.get().audit(event, audit.fields().with(Attr.RESULT, "ok"));
                return 0;
            } catch (AdminArgs.UsageException e) {
                err.println("error: " + e.getMessage());
                return 2;
            } catch (Exception e) {
                Telemetry.get().auditWarn(event, audit.fields().with(Attr.RESULT, "failed")
                        .with(Attr.REASON, e.getClass().getSimpleName()));
                if (!(e instanceof StatusCommand.PartialStatusException)) {
                    err.println("error: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                }
                return 1;
            }
        } catch (Exception e) {
            err.println("error: could not load the configuration: " + e.getClass().getSimpleName() + ": "
                    + e.getMessage());
            return 1;
        }
    }

    /**
     * The connector's configuration, bound exactly as the connector binds it, in a context that contains nothing
     * else. Logging is quiet apart from audit events, so command output stays readable.
     */
    static ConfigurableApplicationContext configuration(List<String> configArgs) {
        SpringApplication app = new SpringApplication(AdminConfiguration.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        app.setBannerMode(Banner.Mode.OFF);
        app.setLogStartupInfo(false);
        app.setRegisterShutdownHook(false);
        app.setDefaultProperties(Map.of(
                "logging.level.root", "WARN",
                "logging.level.sdh.audit", "INFO",
                // Hadoop warns that its native library is missing, which is expected and harmless here.
                "logging.level.org.apache.hadoop", "ERROR"));
        return app.run(configArgs.toArray(String[]::new));
    }

    static String usage() {
        return String.join("\n\n",
                "usage: java -jar file-connector.jar admin <command> [options] [--sdh.some.setting=value ...]\n"
                        + "Requires $" + OPERATOR_ENV + " (who is running it; recorded in the audit event).\n"
                        + "Uses the connector's configuration; --a.b=c arguments override it.",
                RestoreCommand.USAGE, DecryptFallbackCommand.USAGE, StatusCommand.USAGE);
    }

    /** Binds {@code sdh.*} and nothing else: not a component, so the connector's own scan never picks it up. */
    @EnableConfigurationProperties(ConnectorProperties.class)
    static class AdminConfiguration {}
}
