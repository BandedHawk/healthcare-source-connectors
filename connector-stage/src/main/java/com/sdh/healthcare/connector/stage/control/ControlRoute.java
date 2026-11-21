/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A control API path and the operation it serves (CONTROL-API-DESIGN.md
 * §5). A request is bound into the operation's own request exactly as the
 * CLI binds its options, so the API and the CLI check the same things; a
 * route names which of the command's options it takes, so a path that
 * only reads can't be given an option that acts.
 *
 * <p>A query takes its options from the path and the query string. An
 * action takes them from the path and its JSON body, which always holds a
 * {@code reason}; what the CLI reads from standard input, the reason or a
 * corrected record, it reads from the body too ({@link Input}).
 *
 * @param method    the HTTP method
 * @param path      the path, with {@code {name}} for a path variable
 * @param operation the operation's name, as the CLI names it
 * @param options   each of the command's options, by the path variable,
 *                  query parameter or body field that gives it
 * @param flags     each of the command's flags, by the query parameter or
 *                  body field that sets it ({@code true})
 * @param fixed     the flags always set, such as {@code status} for a
 *                  run's status
 * @param streamed  whether the result is rows streamed as JSON lines,
 *                  rather than one answer
 * @param input     what of the body the command reads in place of
 *                  standard input
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record ControlRoute(String method, String path, String operation,
        Map<String, String> options, Map<String, String> flags,
        Set<String> fixed, boolean streamed, Input input)
{

    /** The API's version prefix. */
    public static final String PREFIX = "/api/v1";

    /** The body field every action needs. */
    public static final String REASON = "reason";

    /** The body field a corrected record is given in, base64. */
    public static final String RECORD = "record";

    /** What of an action's body the command reads as standard input. */
    public enum Input
    {

        /** Nothing. */
        NONE,

        /** The reason, which the command keeps encrypted. */
        REASON,

        /** The corrected record, {@value ControlRoute#RECORD}, base64. */
        RECORD
    }

    /**
     * Whether the route acts, rather than reads: it needs an idempotency
     * key and a reason, and is answered once per key.
     *
     * @return true for {@code POST} and {@code DELETE}
     */
    public boolean acts()
    {
        return !"GET".equals(method);
    }

    /**
     * Every route: the queries (build step C2) and the actions (C3).
     *
     * @return them
     */
    public static List<ControlRoute> all()
    {
        List<ControlRoute> all = new ArrayList<>(queries());
        all.addAll(actions());
        return all;
    }

    /**
     * The query routes (build step C2).
     *
     * @return them
     */
    public static List<ControlRoute> queries()
    {
        return List.of(
                query("/status", "status", Map.of(), Map.of()),
                query("/items/{itemId}/status", "status",
                        Map.of("itemId", "item"), Map.of()),
                query("/stuck", "stuck", Map.of("idle", "idle"), Map.of()),
                query("/drain", "drain-check", Map.of(), Map.of()),
                query("/version", "version", Map.of(), Map.of()),
                query("/failures", "failures",
                        Map.of("itemId", "item-id"), Map.of("all", "all")),
                new ControlRoute("GET", PREFIX + "/reprocess-runs/{runId}",
                        "reprocess", Map.of("runId", "run"), Map.of(),
                        Set.of("status"), false, Input.NONE),
                query("/audit/verify", "audit-verify",
                        Map.of("from", "from", "to", "to", "stale", "stale"),
                        Map.of()),
                new ControlRoute("GET", PREFIX + "/audit/events",
                        "audit-export", Map.of("from", "from", "to", "to"),
                        Map.of(), Set.of(), true, Input.NONE));
    }

    /**
     * The action routes (build step C3).
     *
     * @return them
     */
    public static List<ControlRoute> actions()
    {
        Map<String, String> failure = Map.of("itemId", "item-id", "seq",
                "seq");
        return List.of(
                action("POST", "/reprocess-runs", "reprocess", Map.of(
                        "itemIds", "item-id", "seq", "seq", "madeBy",
                        "made-by", "landedFrom", "landed-from", "landedTo",
                        "landed-to", "intake", "intake"),
                        Map.of("dryRun", "dry-run"), Set.of(), Input.REASON),
                action("DELETE", "/reprocess-runs/{runId}", "reprocess",
                        Map.of("runId", "run"), Map.of(), Set.of("cancel"),
                        Input.NONE),
                action("POST", "/failures/{itemId}/{seq}/retry", "retry",
                        failure, Map.of(), Set.of(), Input.NONE),
                action("POST", "/failures/{itemId}/{seq}/decline",
                        "decline", failure, Map.of(), Set.of(),
                        Input.REASON),
                action("POST", "/failures/{itemId}/{seq}/reopen", "reopen",
                        failure, Map.of(), Set.of(), Input.REASON),
                action("POST", "/failures/{itemId}/{seq}/correct",
                        "correct", failure, Map.of(), Set.of(),
                        Input.RECORD),
                action("POST", "/version/accept", "version", Map.of(),
                        Map.of(), Set.of("accept"), Input.REASON),
                action("POST", "/items/{itemId}/release", "release",
                        Map.of("itemId", "item-id"),
                        Map.of("resume", "resume", "close", "close"),
                        Set.of(), Input.REASON),
                action("POST", "/items/{itemId}/resubmit", "resubmit",
                        Map.of("itemId", "item-id"),
                        Map.of("again", "again"), Set.of(), Input.NONE));
    }

    /**
     * A query route, answered with one result.
     *
     * @param path      the path, after the prefix
     * @param operation the operation's name
     * @param options   its options, by parameter
     * @param flags     its flags, by parameter
     * @return the route
     */
    private static ControlRoute query(String path, String operation,
            Map<String, String> options, Map<String, String> flags)
    {
        return new ControlRoute("GET", PREFIX + path, operation, options,
                flags, Set.of(), false, Input.NONE);
    }

    /**
     * An action route.
     *
     * @param method    the HTTP method
     * @param path      the path, after the prefix
     * @param operation the operation's name
     * @param options   its options, by path variable or body field
     * @param flags     its flags, by body field
     * @param fixed     the flags always set
     * @param input     what of the body is read as standard input
     * @return the route
     */
    private static ControlRoute action(String method, String path,
            String operation, Map<String, String> options,
            Map<String, String> flags, Set<String> fixed, Input input)
    {
        return new ControlRoute(method, PREFIX + path, operation, options,
                flags, fixed, false, input);
    }
}
