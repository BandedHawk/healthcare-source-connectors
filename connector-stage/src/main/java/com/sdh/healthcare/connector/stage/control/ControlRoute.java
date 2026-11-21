/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

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
 * @param path      the path, with {@code {name}} for a path variable
 * @param operation the operation's name, as the CLI names it
 * @param options   each of the command's options, by the path variable or
 *                  query parameter that gives it
 * @param flags     each of the command's flags, by the query parameter
 *                  that sets it ({@code ?all=true})
 * @param fixed     the flags always set, such as {@code status} for a
 *                  run's status
 * @param streamed  whether the result is rows streamed as JSON lines,
 *                  rather than one answer
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record ControlRoute(String path, String operation,
        Map<String, String> options, Map<String, String> flags,
        Set<String> fixed, boolean streamed)
{

    /** The API's version prefix. */
    public static final String PREFIX = "/api/v1";

    /**
     * The query routes (build step C2).
     *
     * @return them
     */
    public static List<ControlRoute> queries()
    {
        return List.of(
                answer("/status", "status", Map.of(), Map.of()),
                answer("/items/{itemId}/status", "status",
                        Map.of("itemId", "item"), Map.of()),
                answer("/stuck", "stuck", Map.of("idle", "idle"), Map.of()),
                answer("/drain", "drain-check", Map.of(), Map.of()),
                answer("/version", "version", Map.of(), Map.of()),
                answer("/failures", "failures",
                        Map.of("itemId", "item-id"), Map.of("all", "all")),
                new ControlRoute(PREFIX + "/reprocess-runs/{runId}",
                        "reprocess", Map.of("runId", "run"), Map.of(),
                        Set.of("status"), false),
                answer("/audit/verify", "audit-verify",
                        Map.of("from", "from", "to", "to", "stale", "stale"),
                        Map.of()),
                new ControlRoute(PREFIX + "/audit/events", "audit-export",
                        Map.of("from", "from", "to", "to"), Map.of(),
                        Set.of(), true));
    }

    /**
     * A route answered with one result.
     *
     * @param path      the path, after the prefix
     * @param operation the operation's name
     * @param options   its options, by parameter
     * @param flags     its flags, by parameter
     * @return the route
     */
    private static ControlRoute answer(String path, String operation,
            Map<String, String> options, Map<String, String> flags)
    {
        return new ControlRoute(PREFIX + path, operation, options, flags,
                Set.of(), false);
    }
}
