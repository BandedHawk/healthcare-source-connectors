/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * What a scenario works with: the target, a client of each stage's
 * control API, the test principals, and the checks every scenario ends
 * with (CONTROL-API-DESIGN.md §14): each answer against the contract, each
 * call audited by its stage with its principal (the denied ones as
 * denied), and the audit journal unchanged.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Harness
{

    /** Sees the pipeline, nothing more. */
    public static final Operator VIEWER = Operator.of("vic-viewer",
            "pipeline.view");

    /** Operates the pipeline: pause, resume, retry, reprocess. */
    public static final Operator OPERATOR = Operator.of("olga-operator",
            "pipeline.view", "pipeline.operate");

    /** Exports and corrects failures, sealed to its own key. */
    public static final Operator EXPORTER = Operator.of("erin-exporter",
            "pipeline.view", "pipeline.operate", "phi.export");

    /** Asks for a break-glass restore. */
    public static final Operator BREAK_GLASS = Operator.of("bea-breakglass",
            "pipeline.view", "phi.break-glass");

    /** Approves a break-glass restore. */
    public static final Operator BREAK_GLASS_2 = Operator.of("bo-breakglass",
            "pipeline.view", "phi.break-glass");

    /** Reads and checks the audit journal. */
    public static final Operator AUDITOR = Operator.of("ada-auditor",
            "pipeline.view", "audit.read");

    /** Registers sealing keys, rotates and re-keys. */
    public static final Operator KEYS = Operator.of("kai-keys",
            "pipeline.view", "keys.admin");

    /** Approves a re-key. */
    public static final Operator KEYS_2 = Operator.of("kim-keys",
            "pipeline.view", "keys.admin");

    /** Sets retention, asks for purges and erasure. */
    public static final Operator DATA = Operator.of("dana-data",
            "pipeline.view", "data.admin");

    /** Approves a purge or an erasure. */
    public static final Operator DATA_2 = Operator.of("dirk-data",
            "pipeline.view", "data.admin");

    /** Authenticated, with no role at all. */
    public static final Operator NOBODY = Operator.of("nemo-nobody");

    /** Reads the audit lines. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The pipeline. */
    private final Target target;

    /** The scenarios' documents. */
    private final Samples samples;

    /** A client of each stage. */
    private final Map<String, ControlClient> clients = new LinkedHashMap<>();

    /** Each stage's contract, as it serves it. */
    private final Map<String, Contract> contracts = new HashMap<>();

    /** Every answer of the scenario running. */
    private final List<Reply> calls = new CopyOnWriteArrayList<>();

    /** The sealing keys registered, as stage and principal. */
    private final Set<String> registered = new HashSet<>();

    /** What scenarios leave for later ones, such as item IDs. */
    private final Map<String, Object> notes = new HashMap<>();

    /**
     * Connects to each stage's control API and reads its contract.
     *
     * @param target  the pipeline
     * @param samples the scenarios' documents
     * @throws Exception if a stage can't be reached
     */
    public Harness(Target target, Samples samples) throws Exception
    {
        this.target = target;
        this.samples = samples;
        for (String stage : target.stages())
        {
            String yaml = new ControlClient(stage, target.control(stage),
                    target.trust(), target.issuer(), null, reply ->
                    {
                    }).openapi(VIEWER);
            Contract contract = new Contract(yaml);
            contracts.put(stage, contract);
            clients.put(stage, new ControlClient(stage,
                    target.control(stage), target.trust(), target.issuer(),
                    contract, calls::add));
        }
    }

    /**
     * The pipeline.
     *
     * @return the target
     */
    public Target target()
    {
        return target;
    }

    /**
     * The scenarios' documents.
     *
     * @return them
     */
    public Samples samples()
    {
        return samples;
    }

    /**
     * A stage's client.
     *
     * @param stage the stage
     * @return its client
     */
    public ControlClient stage(String stage)
    {
        ControlClient client = clients.get(stage);
        if (client == null)
        {
            throw new IllegalArgumentException("No stage " + stage);
        }
        return client;
    }

    /**
     * The stages, in pipeline order.
     *
     * @return their names
     */
    public List<String> stages()
    {
        return target.stages();
    }

    /**
     * Something a scenario left for a later one.
     *
     * @param name its name
     * @return it; null if none was left
     */
    public Object note(String name)
    {
        return notes.get(name);
    }

    /**
     * Leaves something for a later scenario.
     *
     * @param name  its name
     * @param value it
     */
    public void note(String name, Object value)
    {
        notes.put(name, value);
    }

    /**
     * Has {@link #KEYS} register an operator's sealing key on a stage,
     * once.
     *
     * @param stage    the stage
     * @param operator the operator
     * @throws Exception if it is refused
     */
    public void sealingKey(String stage, Operator operator) throws Exception
    {
        if (!registered.add(stage + "/" + operator.name()))
        {
            return;
        }
        Reply reply = stage(stage).action(KEYS, "POST", "/operator-keys",
                Map.of("reason", "harness sealing key",
                        "principal", operator.name(),
                        "publicKey", operator.publicPem(),
                        "expiresAt", Instant.now().plus(30, ChronoUnit.DAYS)
                                .truncatedTo(ChronoUnit.SECONDS)
                                .toString()));
        HarnessFailure.check(reply.status() == 200 || reply.status() == 201
                || "refused".equals(reply.problem()),
                "Registering a sealing key: " + reply);
    }

    /**
     * Waits until a condition holds.
     *
     * @param what      what is awaited, for the failure
     * @param timeout   how long to wait
     * @param condition the condition, which may throw while it can't yet
     *                  be read
     * @throws Exception if it doesn't hold in time
     */
    public static void await(String what, Duration timeout,
            Callable<Boolean> condition) throws Exception
    {
        long until = System.nanoTime() + timeout.toNanos();
        Exception last = null;
        while (System.nanoTime() < until)
        {
            try
            {
                if (Boolean.TRUE.equals(condition.call()))
                {
                    return;
                }
                last = null;
            }
            catch (HarnessFailure | java.io.IOException failure)
            {
                last = failure;
            }
            Thread.sleep(1000);
        }
        throw new HarnessFailure("Not within " + timeout.toSeconds()
                + " s: " + what + (last == null ? ""
                        : " (last: " + last.getMessage() + ")"), last);
    }

    /**
     * Says what the harness is doing.
     *
     * @param line what, with no document content
     */
    public static void say(String line)
    {
        System.out.println("[harness] " + line);
    }

    /**
     * Runs a scenario, then its checks: the contract, then the audit.
     *
     * @param scenario the scenario
     * @return what became of it
     */
    public Result run(Scenario scenario)
    {
        calls.clear();
        Map<String, Integer> violationsBefore = new HashMap<>();
        contracts.forEach((stage, contract) -> violationsBefore.put(stage,
                contract.violations().size()));
        Instant start = Instant.now();
        say("Scenario " + scenario.name() + ": " + scenario.summary());
        try
        {
            scenario.run(this);
            List<String> violations = new ArrayList<>();
            contracts.forEach((stage, contract) -> violations.addAll(
                    contract.violations().subList(violationsBefore.get(stage),
                            contract.violations().size())));
            HarnessFailure.check(violations.isEmpty(),
                    "Answers outside the contract: " + violations);
            audited(start);
            return new Result(scenario.name(), true,
                    Duration.between(start, Instant.now()), "passed");
        }
        catch (Exception | AssertionError failure)
        {
            say("Scenario " + scenario.name() + " FAILED: "
                    + failure.getMessage());
            failure.printStackTrace(System.out);
            resumeAll();
            return new Result(scenario.name(), false,
                    Duration.between(start, Instant.now()),
                    failure.getClass().getSimpleName() + ": "
                            + failure.getMessage());
        }
    }

    /**
     * Resumes every stage after a scenario that failed, which may have left
     * one paused, so the next scenario starts from a running pipeline. A
     * stage that can't be resumed is said and left.
     */
    private void resumeAll()
    {
        for (String stage : stages())
        {
            try
            {
                Reply resumed = stage(stage).action(OPERATOR, "POST",
                        "/resume", Map.of("reason", "harness: after a"
                                + " failed scenario"));
                if (resumed.status() != 200)
                {
                    say(stage + " not resumed: " + resumed.status());
                }
            }
            catch (Exception failure)
            {
                say(stage + " not resumed: "
                        + failure.getClass().getSimpleName());
            }
        }
    }

    /**
     * Checks every call of the scenario is in its stage's audit journal,
     * with its principal, the denied ones as denied; and that each
     * journal checks out unchanged.
     *
     * @param from when the scenario started
     * @throws Exception if an event is missing or a chain changed
     */
    void audited(Instant from) throws Exception
    {
        List<Reply> made = List.copyOf(calls);
        Map<String, List<Reply>> byStage = new LinkedHashMap<>();
        for (Reply call : made)
        {
            if (call.principal() != null && call.requestId() != null
                    && call.status() != 401)
            {
                byStage.computeIfAbsent(call.stage(), stage ->
                        new ArrayList<>()).add(call);
            }
        }
        for (Map.Entry<String, List<Reply>> entry : byStage.entrySet())
        {
            ControlClient client = stage(entry.getKey());
            await("every call audited on " + entry.getKey(),
                    Duration.ofSeconds(60), () ->
                    {
                        Map<String, JsonNode> events = events(client, from);
                        for (Reply call : entry.getValue())
                        {
                            JsonNode event = events.get(call.requestId());
                            HarnessFailure.check(event != null,
                                    "No audit event for " + call);
                            JsonNode attributes = event.path("attributes");
                            HarnessFailure.check(call.principal().equals(
                                    attributes.path("sdh.operator").asText()),
                                    "Audited as another principal: " + call);
                            if ("permission-required".equals(call.problem()))
                            {
                                HarnessFailure.check("denied".equals(
                                        attributes.path("sdh.decision")
                                                .asText()),
                                        "A refusal not audited as denied: "
                                                + call);
                            }
                        }
                        return true;
                    });
            Reply verify = client.get(AUDITOR, "/audit/verify").expect(200);
            HarnessFailure.check(((Number) verify.json().getOrDefault(
                    "changed", 0)).intValue() == 0,
                    "The audit journal changed on " + entry.getKey() + ": "
                            + verify.body());
        }
    }

    /** What the journal names a control API call's event. */
    static final String EVENTS = "sdh.audit.control.";

    /**
     * A stage's audit events since a time, by request ID.
     *
     * @param client the stage's client
     * @param from   the time
     * @return the events naming a request
     * @throws Exception if the journal can't be read
     */
    private static Map<String, JsonNode> events(ControlClient client,
            Instant from) throws Exception
    {
        Reply export = client.get(AUDITOR, "/audit/events?from="
                + from.minusSeconds(60).truncatedTo(ChronoUnit.SECONDS))
                .expect(200);
        Map<String, JsonNode> events = new HashMap<>();
        for (String line : export.body().split("\n"))
        {
            if (line.isBlank())
            {
                continue;
            }
            JsonNode event = JSON.readTree(line);
            String id = event.path("attributes").path("sdh.request_id")
                    .asText(null);
            if (id != null && event.path("event").asText().startsWith(
                    EVENTS))
            {
                events.putIfAbsent(id, event);
            }
        }
        return events;
    }

    /**
     * What became of a scenario.
     *
     * @param scenario its name
     * @param passed   whether every check held
     * @param took     how long it took
     * @param outcome  {@code passed}, or what failed
     */
    public record Result(String scenario, boolean passed, Duration took,
            String outcome)
    {
    }
}
