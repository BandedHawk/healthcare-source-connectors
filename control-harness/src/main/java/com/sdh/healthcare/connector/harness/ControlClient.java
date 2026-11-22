/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;

/**
 * A typed client of one stage's control API (CONTROL-API-DESIGN.md §14),
 * on {@code java.net.http}: each call made as an {@link Operator}, with a
 * token from the test issuer, and each answer checked against the
 * contract and handed to whoever records the calls, for the audit check.
 * It is also a reference for the console and the customer's tools.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ControlClient
{

    /** How long a call may take: an export streams. */
    static final Duration TIMEOUT = Duration.ofMinutes(2);

    /** Writes request bodies. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The stage. */
    private final String stage;

    /** Its API's base, ending {@code /api/v1}. */
    private final URI base;

    /** The HTTP client, trusting the stage's certificate. */
    private final HttpClient http;

    /** Signs each call's token. */
    private final TestIssuer issuer;

    /** The contract every answer is checked against; null for none. */
    private final Contract contract;

    /** Receives every answer. */
    private final Consumer<Reply> calls;

    /**
     * Creates a client.
     *
     * @param stage    the stage, for messages
     * @param base     its API's base, ending {@code /api/v1}
     * @param trust    trusts its server certificate
     * @param issuer   signs the callers' tokens
     * @param contract checks each answer; null to skip
     * @param calls    receives every answer
     */
    public ControlClient(String stage, URI base, SSLContext trust,
            TestIssuer issuer, Contract contract, Consumer<Reply> calls)
    {
        this.stage = stage;
        this.base = base;
        this.http = HttpClient.newBuilder().sslContext(trust)
                .connectTimeout(Duration.ofSeconds(10)).build();
        this.issuer = issuer;
        this.contract = contract;
        this.calls = calls;
    }

    /**
     * The stage's contract, as it serves it.
     *
     * @param caller who reads it
     * @return the OpenAPI file
     * @throws IOException          if it can't be read
     * @throws InterruptedException if interrupted
     */
    public String openapi(Operator caller)
            throws IOException, InterruptedException
    {
        return send("GET", "/openapi.yaml", caller, null, null, false)
                .expect(200).body();
    }

    /**
     * A query.
     *
     * @param caller who asks; null for no credential
     * @param path   the path under {@code /api/v1}, with any query
     * @return the answer
     * @throws IOException          if the stage can't be reached
     * @throws InterruptedException if interrupted
     */
    public Reply get(Operator caller, String path)
            throws IOException, InterruptedException
    {
        return send("GET", path, caller, null, null, true);
    }

    /**
     * An action, with a fresh {@code Idempotency-Key}.
     *
     * @param caller who asks
     * @param method {@code POST}, {@code PUT} or {@code DELETE}
     * @param path   the path
     * @param body   the body's fields, a {@code reason} among them
     * @return the answer
     * @throws IOException          if the stage can't be reached
     * @throws InterruptedException if interrupted
     */
    public Reply action(Operator caller, String method, String path,
            Map<String, Object> body) throws IOException, InterruptedException
    {
        return action(caller, method, path, body, "harness-"
                + UUID.randomUUID());
    }

    /**
     * An action, with the {@code Idempotency-Key} given: to send it again.
     *
     * @param caller who asks
     * @param method the method
     * @param path   the path
     * @param body   the body's fields
     * @param key    the key
     * @return the answer
     * @throws IOException          if the stage can't be reached
     * @throws InterruptedException if interrupted
     */
    public Reply action(Operator caller, String method, String path,
            Map<String, Object> body, String key)
            throws IOException, InterruptedException
    {
        return send(method, path, caller, body, key, true);
    }

    /**
     * A {@code POST} without an {@code Idempotency-Key}: a decision on an
     * operation, or a sealed read.
     *
     * @param caller who asks
     * @param path   the path
     * @param body   the body's fields
     * @return the answer
     * @throws IOException          if the stage can't be reached
     * @throws InterruptedException if interrupted
     */
    public Reply post(Operator caller, String path, Map<String, Object> body)
            throws IOException, InterruptedException
    {
        return send("POST", path, caller, body, null, true);
    }

    /**
     * Follows a long operation until it ends.
     *
     * @param caller  who follows it
     * @param id      the operation's ID
     * @param timeout how long it may take
     * @return its last record
     * @throws Exception if it doesn't end in time
     */
    public Reply follow(Operator caller, String id, Duration timeout)
            throws Exception
    {
        long until = System.nanoTime() + timeout.toNanos();
        while (true)
        {
            Reply record = get(caller, "/operations/" + id).expect(200);
            String state = record.field("state");
            if (List.of("done", "failed", "cancelled", "rejected", "expired")
                    .contains(state))
            {
                return record;
            }
            if (System.nanoTime() > until)
            {
                throw new HarnessFailure("Operation " + id + " still "
                        + state + " after " + timeout.toSeconds() + " s");
            }
            Thread.sleep(1000);
        }
    }

    /**
     * Sends a call.
     *
     * @param method    the method
     * @param path      the path, with any query
     * @param caller    who asks; null for no credential
     * @param body      the body's fields; null for none
     * @param key       the {@code Idempotency-Key}; null for none
     * @param recorded  whether to check and record the answer
     * @return the answer
     * @throws IOException          if the stage can't be reached
     * @throws InterruptedException if interrupted
     */
    private Reply send(String method, String path, Operator caller,
            Map<String, Object> body, String key, boolean recorded)
            throws IOException, InterruptedException
    {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                URI.create(base + path)).timeout(TIMEOUT);
        if (caller != null)
        {
            request.header("Authorization", "Bearer "
                    + issuer.token(caller.name(), caller.roles()));
        }
        if (key != null)
        {
            request.header("Idempotency-Key", key);
        }
        if (body == null)
        {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        }
        else
        {
            request.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(
                            json(body)));
        }
        HttpResponse<String> response = http.send(request.build(),
                HttpResponse.BodyHandlers.ofString());
        int query = path.indexOf('?');
        Reply reply = new Reply(stage, method, Contract.PREFIX
                + (query < 0 ? path : path.substring(0, query)),
                caller == null ? null : caller.name(),
                response.statusCode(), response.headers().map(),
                response.body());
        if (recorded)
        {
            if (contract != null)
            {
                contract.check(reply);
            }
            calls.accept(reply);
        }
        return reply;
    }

    /**
     * A body as JSON.
     *
     * @param body its fields
     * @return the JSON
     * @throws IOException if a field can't be written
     */
    private static String json(Map<String, Object> body) throws IOException
    {
        try
        {
            return JSON.writeValueAsString(body);
        }
        catch (JsonProcessingException wrong)
        {
            throw new IOException("A body that can't be JSON", wrong);
        }
    }
}
