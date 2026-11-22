/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * A control API's answer to one call: its status, headers and body, as the
 * client got them.
 *
 * @param stage     the stage that answered
 * @param method    the call's method
 * @param path      the call's path, under {@code /api/v1}, without its query
 * @param principal who made the call; null for a call with no credential
 * @param status    the HTTP status
 * @param headers   the headers, by lower-case name
 * @param body      the body, as text
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record Reply(String stage, String method, String path,
        String principal, int status, Map<String, List<String>> headers,
        String body)
{

    /** Reads answers. */
    static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Copies the headers.
     *
     * @param stage     the stage
     * @param method    the method
     * @param path      the path
     * @param principal the principal
     * @param status    the status
     * @param headers   the headers
     * @param body      the body
     */
    public Reply
    {
        headers = Map.copyOf(headers);
    }

    /**
     * A header's first value.
     *
     * @param name its name, in any case
     * @return its value, if sent
     */
    public Optional<String> header(String name)
    {
        List<String> values = headers.get(name.toLowerCase(Locale.ROOT));
        return values == null || values.isEmpty() ? Optional.empty()
                : Optional.of(values.get(0));
    }

    /**
     * The request's ID, which its audit event names.
     *
     * @return the ID; null if the answer has none
     */
    public String requestId()
    {
        return header("X-Request-Id").orElse(null);
    }

    /**
     * The body as a JSON object.
     *
     * @return its fields
     * @throws UncheckedIOException if the body isn't a JSON object
     */
    public Map<String, Object> json()
    {
        try
        {
            return JSON.readValue(body, new TypeReference<>()
            {
            });
        }
        catch (IOException wrong)
        {
            throw new UncheckedIOException("Not a JSON object: " + this,
                    wrong);
        }
    }

    /**
     * A field of the JSON body, as text.
     *
     * @param name the field
     * @return its value as text; null if absent or null
     */
    public String field(String name)
    {
        Object value = json().get(name);
        return value == null ? null : value.toString();
    }

    /**
     * A refusal's title (RFC 9457), such as {@code permission-required}.
     *
     * @return the title; null if the answer isn't a problem
     */
    public String problem()
    {
        return status >= 400 && body.startsWith("{") ? field("title") : null;
    }

    /**
     * Fails unless the status is the one expected.
     *
     * @param expected the status
     * @return this
     * @throws HarnessFailure if it isn't
     */
    public Reply expect(int expected)
    {
        if (status != expected)
        {
            throw new HarnessFailure("Expected " + expected + " from "
                    + this);
        }
        return this;
    }

    /**
     * The call and its answer, for a failure's message: never the body of
     * a sealed or exported result, which may hold PHI.
     *
     * @return the summary
     */
    @Override
    public String toString()
    {
        String shown = body.startsWith("{\"operation\"")
                || body.length() > 600 ? "(" + body.length() + " chars)"
                : body;
        return method + " " + stage + path + " as " + principal + ": "
                + status + " " + shown;
    }
}
