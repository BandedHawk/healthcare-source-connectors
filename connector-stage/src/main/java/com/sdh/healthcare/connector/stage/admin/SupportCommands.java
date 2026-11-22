/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.runtime.compliance.ComplianceConfiguration;
import com.sdh.healthcare.connector.runtime.compliance.ComplianceRules;
import com.sdh.healthcare.connector.runtime.config.SettingsView;
import com.sdh.healthcare.connector.stage.ControlState;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.Table;

/**
 * Every stage's support commands (CONTROL-API-DESIGN.md §5, HIPAA.md P5,
 * §8.1 item 11): {@code health}, whether each dependency answers;
 * {@code settings}, the settings in force with their secrets removed;
 * {@code compliance}, what the profile makes of them; and
 * {@code support-bundle}, all of those with the stage's build, control
 * state, status and recent operations, as one JSON document to send to
 * support. None of them reads PHI, so support never needs it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SupportCommands
{

    /** How long one dependency's check may take. */
    static final Duration CHECK_TIMEOUT = Duration.ofSeconds(10);

    /** How long a connection to a broker or collector may take. */
    static final int CONNECT_MILLIS = 5_000;

    /** Who reads the stage's status for the bundle, in its audit. */
    static final String READER = "support-bundle";

    /** How many recent operations the bundle holds. */
    public static final int OPERATIONS = 50;

    /** A Kafka endpoint's brokers, in an endpoint URI's options. */
    private static final Pattern BROKERS =
            Pattern.compile("(?:^|[?&])brokers=([^&]+)");

    /** The collector's endpoint setting. */
    static final String OTLP_ENDPOINT = "sdh.telemetry.otlp.endpoint";

    /** Not created: holds the commands. */
    private SupportCommands()
    {
    }

    /**
     * The commands.
     *
     * @return them
     */
    public static List<AdminCommand<SupportTarget>> all()
    {
        return List.of(new Health(), new Settings(), new Compliance(),
                new Bundle());
    }

    /**
     * What one dependency's check found.
     *
     * @param name    the dependency: {@code database}, {@code store},
     *                {@code kms}, {@code broker} or {@code collector}
     * @param target  what was reached: a host and port, a table's name or
     *                a KEK's reference; never a value
     * @param reached whether it answered
     * @param millis  how long the check took
     * @param problem why it didn't, by the failure's class; null if it did
     */
    public record Check(String name, String target, boolean reached,
            long millis, String problem)
    {
    }

    /**
     * A check still to run.
     *
     * @param name   the dependency
     * @param target what it reaches
     * @param probe  reaches it; throws if it can't
     */
    record Probe(String name, String target, Callable<?> probe)
    {
    }

    /**
     * The checks a stage's dependencies need: its database always; its
     * store and KMS if its context holds keys and tables; each broker and
     * the collector its settings name.
     *
     * @param target what the commands read
     * @return the checks
     */
    static List<Probe> probes(SupportTarget target)
    {
        List<Probe> probes = new ArrayList<>();
        probes.add(new Probe("database", "lease database", () ->
        {
            target.audit().ping();
            return null;
        }));
        if (target.context() instanceof KeyAdmin keys)
        {
            probes.add(new Probe("store", "the stage's tables", () ->
            {
                for (Table table : keys.keyTables())
                {
                    table.refresh();
                }
                return null;
            }));
            probes.add(new Probe("kms", "the KEK in use", () ->
            {
                KeyCommands.probe(keys, KeyCommands.active(
                        keys.controlState(false), keys));
                return null;
            }));
        }
        for (String broker : brokers(target.settings()))
        {
            probes.add(new Probe("broker", broker, () -> connect(broker)));
        }
        String collector = target.settings().get(OTLP_ENDPOINT);
        if (collector != null && !collector.isBlank())
        {
            String address = address(collector);
            probes.add(new Probe("collector", address, () ->
                    connect(address)));
        }
        return probes;
    }

    /**
     * The brokers the settings name, as host and port: Kafka's, in each
     * endpoint URI's {@code brokers} option or the component's setting,
     * and RabbitMQ's.
     *
     * @param settings the settings in force
     * @return them, in the order found
     */
    static Set<String> brokers(Map<String, String> settings)
    {
        Set<String> brokers = new LinkedHashSet<>();
        settings.forEach((name, value) ->
        {
            List<String> lists = new ArrayList<>();
            Matcher option = BROKERS.matcher(value);
            while (option.find())
            {
                lists.add(option.group(1));
            }
            if (name.endsWith(".brokers"))
            {
                lists.add(value);
            }
            for (String list : lists)
            {
                for (String each : list.split(","))
                {
                    if (!each.isBlank())
                    {
                        brokers.add(address(each.strip()));
                    }
                }
            }
        });
        String addresses = settings.get("spring.rabbitmq.addresses");
        if (addresses != null)
        {
            for (String each : addresses.split(","))
            {
                if (!each.isBlank())
                {
                    brokers.add(address(each.strip()));
                }
            }
        }
        else if (settings.containsKey("spring.rabbitmq.host"))
        {
            boolean ssl = "true".equalsIgnoreCase(settings.get(
                    "spring.rabbitmq.ssl.enabled"));
            brokers.add(settings.get("spring.rabbitmq.host") + ":"
                    + settings.getOrDefault("spring.rabbitmq.port",
                            ssl ? "5671" : "5672"));
        }
        return brokers;
    }

    /**
     * An endpoint's host and port: a URI's, its user information left
     * out, its port its scheme's if not given; or as given.
     *
     * @param endpoint the endpoint
     * @return {@code host:port}
     */
    static String address(String endpoint)
    {
        if (!endpoint.contains("://"))
        {
            return endpoint;
        }
        URI uri = URI.create(endpoint);
        int port = uri.getPort();
        if (port < 0)
        {
            port = switch (uri.getScheme().toLowerCase())
            {
                case "https" -> 443;
                case "amqps" -> 5671;
                case "amqp" -> 5672;
                default -> 80;
            };
        }
        return uri.getHost() + ":" + port;
    }

    /**
     * Opens a TCP connection, and closes it.
     *
     * @param address {@code host:port}
     * @return nothing
     * @throws Exception if it can't be opened
     */
    static Void connect(String address) throws Exception
    {
        int colon = address.lastIndexOf(':');
        if (colon < 1)
        {
            throw new IllegalArgumentException("No port");
        }
        try (Socket socket = new Socket())
        {
            socket.connect(new InetSocketAddress(address.substring(0, colon),
                    Integer.parseInt(address.substring(colon + 1))),
                    CONNECT_MILLIS);
        }
        return null;
    }

    /**
     * Runs the checks at once, each for at most {@link #CHECK_TIMEOUT}.
     *
     * @param probes the checks
     * @return what each found, in order
     * @throws InterruptedException if interrupted meanwhile
     */
    static List<Check> check(List<Probe> probes) throws InterruptedException
    {
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1,
                probes.size()), runnable ->
                {
                    Thread thread = new Thread(runnable, "sdh-health");
                    thread.setDaemon(true);
                    return thread;
                });
        try
        {
            long started = System.nanoTime();
            List<Future<?>> running = new ArrayList<>();
            for (Probe probe : probes)
            {
                running.add(pool.submit(probe.probe()));
            }
            List<Check> checks = new ArrayList<>();
            long until = started + CHECK_TIMEOUT.toNanos();
            for (int i = 0; i < probes.size(); i++)
            {
                Probe probe = probes.get(i);
                String problem = null;
                try
                {
                    running.get(i).get(Math.max(0, until - System.nanoTime()),
                            TimeUnit.NANOSECONDS);
                }
                catch (ExecutionException failed)
                {
                    problem = failed.getCause().getClass().getSimpleName();
                }
                catch (TimeoutException slow)
                {
                    running.get(i).cancel(true);
                    problem = "Timeout";
                }
                checks.add(new Check(probe.name(), probe.target(),
                        problem == null, Duration.ofNanos(System.nanoTime()
                                - started).toMillis(), problem));
            }
            return checks;
        }
        finally
        {
            pool.shutdownNow();
        }
    }

    /**
     * The checks as answered.
     *
     * @param checks what they found
     * @return the answer
     */
    static Map<String, Object> healthAnswer(List<Check> checks)
    {
        return Answers.object("up", checks.stream().allMatch(Check::reached),
                "checks", Answers.list(checks, check -> Answers.object(
                        "name", check.name(), "target", check.target(),
                        "reached", check.reached(),
                        "millis", check.millis(),
                        "problem", check.problem())));
    }

    /**
     * The settings as answered, secrets removed.
     *
     * @param target what the commands read
     * @return the answer
     */
    static Map<String, Object> settingsAnswer(SupportTarget target)
    {
        Map<String, String> view = SettingsView.redacted(target.settings());
        long removed = view.values().stream()
                .filter(SettingsView.REDACTED::equals).count();
        return Answers.object("settings", new LinkedHashMap<>(view),
                "redacted", removed);
    }

    /**
     * The profile's outcome as answered.
     *
     * @param target what the commands read
     * @return the answer
     */
    static Map<String, Object> complianceAnswer(SupportTarget target)
    {
        ComplianceRules.Outcome outcome = ComplianceRules.evaluate(
                target.settings(), ComplianceConfiguration::jvmOption);
        List<Object> refused = new ArrayList<>();
        for (ComplianceRules.Finding finding : outcome.refused())
        {
            refused.add(Answers.object("check", finding.check().id(),
                    "setting", finding.setting(),
                    "meaning", finding.check().meaning()));
        }
        List<Object> waived = new ArrayList<>();
        outcome.waived().forEach((finding, reason) -> waived.add(
                Answers.object("check", finding.check().id(),
                        "setting", finding.setting(),
                        "meaning", finding.check().meaning(),
                        "reason", reason)));
        return Answers.object("profile", outcome.profile().id(),
                "passed", outcome.passed(), "refused", refused,
                "waived", waived);
    }

    /**
     * The scope's control state as answered.
     *
     * @param state the scope's state
     * @return the answer
     */
    static Map<String, Object> controlAnswer(ControlState state)
    {
        Map<String, Object> answer = Answers.object("scope", state.scope());
        state.paused().ifPresentOrElse(paused -> answer.put("paused",
                Answers.object("by", paused.pausedBy(),
                        "at", paused.pausedAt(), "reason", paused.reason())),
                () -> answer.put("paused", null));
        answer.put("kek", state.kek().map(ControlState.Kek::kek)
                .orElse(null));
        answer.put("rekey", KeyCommands.rekeyAnswer(state.rekeying()));
        answer.put("retention", state.retention()
                .map(ControlState.Retention::wire).orElse(null));
        answer.put("erased", state.erased().map(erased -> Answers.object(
                "id", erased.id(), "by", erased.erasedBy(),
                "at", erased.erasedAt())).orElse(null));
        return answer;
    }

    /**
     * A part of the bundle, or why it couldn't be read.
     *
     * @param part reads it
     * @return it; or {@code unavailable} with the failure's class
     */
    static Object part(Callable<?> part)
    {
        try
        {
            return part.call();
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
            return Answers.object("unavailable", "InterruptedException");
        }
        catch (Exception failure)
        {
            return Answers.object("unavailable",
                    failure.getClass().getSimpleName());
        }
    }

    /** A command that takes no options. */
    record Request()
    {
    }

    /**
     * The commands' common parts: no options, {@code pipeline.view}, a
     * query.
     *
     * @param <R> the result
     */
    abstract static class Query<R> implements
            CliOperation<SupportTarget, Request, R>, Answering<R>
    {

        /** Creates the command. */
        Query()
        {
        }

        @Override
        public Set<String> options()
        {
            return Set.of();
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.PIPELINE_VIEW;
        }

        @Override
        public Kind kind()
        {
            return Kind.QUERY;
        }

        @Override
        public Request bind(AdminArgs arguments, SupportTarget context)
        {
            return new Request();
        }
    }

    /** {@code health}: whether each dependency answers. */
    static final class Health extends Query<List<Check>>
    {

        /** Creates the command. */
        Health()
        {
        }

        @Override
        public String name()
        {
            return "health";
        }

        @Override
        public String usage()
        {
            return """
                    health
                        Whether each dependency answers, as this process
                        reaches it: the database, the store (the stage's
                        tables), the KMS (the KEK in use, wrapped and
                        unwrapped), each broker and the collector the
                        settings name. Each is given 10 seconds. Reads no
                        data and changes nothing.""";
        }

        @Override
        public List<Check> run(Request request, SupportTarget context,
                Invocation invocation) throws Exception
        {
            return check(probes(context));
        }

        @Override
        public Map<String, Object> answer(List<Check> result)
        {
            return healthAnswer(result);
        }

        @Override
        public void print(List<Check> result, PrintStream out)
        {
            for (Check check : result)
            {
                out.println(check.name() + " " + check.target() + ": "
                        + (check.reached() ? "reached" : "NOT reached ("
                                + check.problem() + ")") + " in "
                        + check.millis() + " ms");
            }
        }
    }

    /** {@code settings}: the settings in force, secrets removed. */
    static final class Settings extends Query<Map<String, Object>>
    {

        /** Creates the command. */
        Settings()
        {
        }

        @Override
        public String name()
        {
            return "settings";
        }

        @Override
        public String usage()
        {
            return """
                    settings
                        The settings in force (sdh.*, camel.component.*,
                        spring.rabbitmq.*), however they were set, each
                        secret shown as <redacted>: passwords, tokens,
                        secrets, private keys, JAAS configurations, and a
                        URI's user information and secret options.""";
        }

        @Override
        public Map<String, Object> run(Request request,
                SupportTarget context, Invocation invocation)
        {
            return settingsAnswer(context);
        }

        @Override
        public Map<String, Object> answer(Map<String, Object> result)
        {
            return result;
        }

        @Override
        public void print(Map<String, Object> result, PrintStream out)
        {
            ((Map<?, ?>) result.get("settings")).forEach((name, value) ->
                    out.println(name + "=" + value));
        }
    }

    /** {@code compliance}: what the profile makes of the settings. */
    static final class Compliance extends Query<Map<String, Object>>
    {

        /** Creates the command. */
        Compliance()
        {
        }

        @Override
        public String name()
        {
            return "compliance";
        }

        @Override
        public String usage()
        {
            return """
                    compliance
                        The compliance profile in force, and what it makes
                        of the settings: each setting it refuses, and each
                        it found but an operator waived, with the reason.
                        Names settings, never their values.""";
        }

        @Override
        public Map<String, Object> run(Request request,
                SupportTarget context, Invocation invocation)
        {
            return complianceAnswer(context);
        }

        @Override
        public Map<String, Object> answer(Map<String, Object> result)
        {
            return result;
        }

        @Override
        public void print(Map<String, Object> result, PrintStream out)
        {
            out.println("Profile " + result.get("profile") + ": "
                    + (Boolean.TRUE.equals(result.get("passed")) ? "passed"
                            : "REFUSED"));
            for (Object each : (List<?>) result.get("refused"))
            {
                Map<?, ?> finding = (Map<?, ?>) each;
                out.println("refused " + finding.get("check") + ": "
                        + finding.get("setting") + " ("
                        + finding.get("meaning") + ")");
            }
            for (Object each : (List<?>) result.get("waived"))
            {
                Map<?, ?> finding = (Map<?, ?>) each;
                out.println("waived " + finding.get("check") + ": "
                        + finding.get("setting") + ", because "
                        + finding.get("reason"));
            }
        }
    }

    /**
     * {@code support-bundle}: everything support needs, as one JSON
     * document, no PHI.
     */
    static final class Bundle extends Query<Map<String, Object>>
    {

        /** Creates the command. */
        Bundle()
        {
        }

        @Override
        public String name()
        {
            return "support-bundle";
        }

        @Override
        public String usage()
        {
            return """
                    support-bundle
                        One JSON document for support: the stage's build,
                        health, settings (secrets removed), compliance
                        outcome, control state, status and, through the
                        control API, its recent operations (no parameters
                        or reasons). A part that can't be read says so.
                        Holds no PHI: IDs, counts, codes and settings.""";
        }

        @Override
        public Map<String, Object> run(Request request,
                SupportTarget context, Invocation invocation)
        {
            Map<String, Object> bundle = Answers.object("service",
                    context.service(), "build", context.build(),
                    "generatedAt", Instant.now(),
                    "generatedBy", invocation.principal(),
                    "phi", "none: settings with secrets removed, IDs,"
                            + " counts and codes only");
            bundle.put("health", part(() -> healthAnswer(check(probes(
                    context)))));
            bundle.put("settings", part(() -> settingsAnswer(context)));
            bundle.put("compliance", part(() -> complianceAnswer(context)));
            bundle.put("control", part(() -> controlAnswer(context.audit()
                    .controlState(false))));
            bundle.put("status", context.status() == null ? null
                    : part(context.status()));
            bundle.put("operations", context.operations() == null ? null
                    : part(context.operations()));
            return bundle;
        }

        @Override
        public Map<String, Object> answer(Map<String, Object> result)
        {
            return result;
        }

        @Override
        public void print(Map<String, Object> result, PrintStream out)
        {
            out.println(JSONObjectUtils.toJSONString(result));
        }
    }
}
