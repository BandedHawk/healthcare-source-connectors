/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * The control API's contract, its OpenAPI file, as a stage serves it, and
 * the check of every answer against it (CONTROL-API-DESIGN.md §12): the
 * route is in the contract, with its method; the status is one it lists,
 * or it has a default; a success has the content type it declares; a
 * refusal is a problem (RFC 9457) with its type, title and status; and
 * every answer names its request. What doesn't hold is kept, and a
 * scenario fails on it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Contract
{

    /** The routes' prefix in the contract's paths. */
    static final String PREFIX = "/api/v1";

    /** The contract's paths, each with its pattern. */
    private final List<Route> routes = new ArrayList<>();

    /** The contract's named responses. */
    private final Map<String, Object> responses;

    /** What didn't hold, in the order found. */
    private final List<String> violations = new CopyOnWriteArrayList<>();

    /**
     * A path of the contract.
     *
     * @param template the path as written, such as
     *                 {@code /items/{itemId}/status}
     * @param pattern  the path's pattern, each variable one segment
     * @param methods  its operations, by lower-case method
     */
    record Route(String template, Pattern pattern,
            Map<String, Object> methods)
    {
    }

    /**
     * Reads a contract.
     *
     * @param yaml the OpenAPI file
     * @throws HarnessFailure if it isn't one
     */
    @SuppressWarnings("unchecked")
    public Contract(String yaml)
    {
        Object loaded = new Yaml(new SafeConstructor(new LoaderOptions()))
                .load(yaml);
        if (!(loaded instanceof Map<?, ?> document)
                || !(document.get("paths") instanceof Map<?, ?> paths))
        {
            throw new HarnessFailure("The contract has no paths");
        }
        Map<String, Object> components = document.get("components")
                instanceof Map<?, ?> found ? (Map<String, Object>) found
                : Map.of();
        responses = components.get("responses") instanceof Map<?, ?> named
                ? (Map<String, Object>) named : Map.of();
        paths.forEach((template, operations) -> routes.add(new Route(
                template.toString(), pattern(template.toString()),
                (Map<String, Object>) operations)));
    }

    /**
     * A path template's pattern: each {@code {name}} one segment.
     *
     * @param template the template
     * @return the pattern
     */
    static Pattern pattern(String template)
    {
        StringBuilder regex = new StringBuilder();
        for (String part : template.split("(?=\\{)|(?<=\\})"))
        {
            regex.append(part.startsWith("{") ? "[^/]+"
                    : Pattern.quote(part));
        }
        return Pattern.compile(regex.toString());
    }

    /**
     * Checks an answer against the contract, keeping what doesn't hold.
     *
     * @param reply the answer
     * @return what didn't hold in it; empty if it all did
     */
    public List<String> check(Reply reply)
    {
        List<String> found = new ArrayList<>();
        String call = reply.method() + " " + reply.path() + " ("
                + reply.stage() + ", " + reply.status() + ")";
        if (reply.requestId() == null)
        {
            found.add(call + ": no X-Request-Id");
        }
        Route route = route(reply.path());
        if (route == null)
        {
            if (reply.status() != 401)
            {
                found.add(call + ": not in the contract");
            }
            violations.addAll(found);
            return found;
        }
        Object operation = route.methods().get(reply.method().toLowerCase(
                Locale.ROOT));
        if (!(operation instanceof Map<?, ?> declared))
        {
            if (reply.status() != 401 && reply.status() != 405)
            {
                found.add(call + ": the contract has no "
                        + reply.method() + " for " + route.template());
            }
            violations.addAll(found);
            return found;
        }
        Map<?, ?> listed = declared.get("responses") instanceof Map<?, ?> map
                ? map : Map.of();
        Object response = listed.get(Integer.toString(reply.status()));
        if (response == null)
        {
            response = listed.get("default");
        }
        if (response == null && reply.status() != 401)
        {
            found.add(call + ": a status the contract doesn't list");
        }
        String type = reply.header("Content-Type").orElse("");
        if (reply.status() >= 400)
        {
            problem(reply, call, type, found);
        }
        else if (response != null)
        {
            String expected = contentType(response);
            if (expected != null && !type.startsWith(expected))
            {
                found.add(call + ": " + type + ", not " + expected);
            }
        }
        violations.addAll(found);
        return found;
    }

    /**
     * Checks a refusal: a problem with its type, title and status.
     *
     * @param reply the answer
     * @param call  the call, for the message
     * @param type  its content type
     * @param found receives what doesn't hold
     */
    private static void problem(Reply reply, String call, String type,
            List<String> found)
    {
        if (!type.startsWith("application/problem+json"))
        {
            found.add(call + ": a refusal as " + type
                    + ", not application/problem+json");
            return;
        }
        try
        {
            Map<String, Object> problem = reply.json();
            for (String field : List.of("type", "title", "status"))
            {
                if (problem.get(field) == null)
                {
                    found.add(call + ": a problem without " + field);
                }
            }
            if (problem.get("status") instanceof Number status
                    && status.intValue() != reply.status())
            {
                found.add(call + ": a problem whose status is " + status);
            }
        }
        catch (RuntimeException wrong)
        {
            found.add(call + ": a problem that isn't JSON");
        }
    }

    /**
     * A response's content type, its {@code $ref} followed.
     *
     * @param response the response, as the contract gives it
     * @return its first content type; null if it gives none
     */
    private String contentType(Object response)
    {
        Object resolved = response;
        if (resolved instanceof Map<?, ?> map
                && map.get("$ref") instanceof String ref)
        {
            resolved = responses.get(ref.substring(ref.lastIndexOf('/')
                    + 1));
        }
        if (resolved instanceof Map<?, ?> map
                && map.get("content") instanceof Map<?, ?> content
                && !content.isEmpty())
        {
            return content.keySet().iterator().next().toString();
        }
        return null;
    }

    /**
     * The contract's path for a call's path.
     *
     * @param path the call's path, under {@code /api/v1}, without its query
     * @return the route; null if the contract has none
     */
    Route route(String path)
    {
        String under = path.startsWith(PREFIX) ? path.substring(
                PREFIX.length()) : path;
        Route best = null;
        for (Route route : routes)
        {
            if (route.pattern().matcher(under).matches()
                    && (best == null || literal(route) > literal(best)))
            {
                best = route;
            }
        }
        return best;
    }

    /**
     * How much of a route's path is literal, so that
     * {@code /operations/{id}/approve} wins over {@code /operations/{id}}
     * and a literal segment over a variable.
     *
     * @param route the route
     * @return the count of literal characters
     */
    private static int literal(Route route)
    {
        return route.template().replaceAll("\\{[^}]*\\}", "").length();
    }

    /**
     * The paths the contract lists.
     *
     * @return them, as written
     */
    public List<String> paths()
    {
        return routes.stream().map(Route::template).toList();
    }

    /**
     * What didn't hold, in every answer checked so far.
     *
     * @return the violations
     */
    public List<String> violations()
    {
        return List.copyOf(violations);
    }
}
