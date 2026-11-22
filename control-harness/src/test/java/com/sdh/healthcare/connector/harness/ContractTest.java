/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link Contract}: answers checked against a small contract.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ContractTest
{

    /** A contract of two routes. */
    static final String YAML = """
            openapi: 3.1.0
            paths:
              /items/{itemId}/status:
                get:
                  responses:
                    "200": {$ref: "#/components/responses/Answer"}
                    default: {$ref: "#/components/responses/Problem"}
              /items/{itemId}/restore:
                post:
                  responses:
                    "202": {$ref: "#/components/responses/Answer"}
            components:
              responses:
                Answer:
                  content:
                    application/json: {}
                Problem:
                  content:
                    application/problem+json: {}
            """;

    /** Creates the test. */
    ContractTest()
    {
    }

    /**
     * An answer.
     *
     * @param method the method
     * @param path   the path, under the API's prefix
     * @param status the status
     * @param type   its content type
     * @param body   its body
     * @return the answer, with a request ID
     */
    static Reply reply(String method, String path, int status, String type,
            String body)
    {
        return new Reply("convert", method, Contract.PREFIX + path, "vic",
                status, Map.of("x-request-id", List.of("r-1"),
                        "content-type", List.of(type)), body);
    }

    /** An answer the contract lists holds. */
    @Test
    void listedAnswerHolds()
    {
        Contract contract = new Contract(YAML);

        assertThat(contract.check(reply("GET", "/items/abc/status", 200,
                "application/json", "{}"))).isEmpty();
        assertThat(contract.violations()).isEmpty();
    }

    /** A refusal must be a problem with its type, title and status. */
    @Test
    void refusalMustBeAProblem()
    {
        Contract contract = new Contract(YAML);

        assertThat(contract.check(reply("GET", "/items/abc/status", 404,
                "application/problem+json", "{\"type\":\"t\",\"title\":"
                        + "\"not-found\",\"status\":404}"))).isEmpty();
        assertThat(contract.check(reply("GET", "/items/abc/status", 404,
                "application/json", "{}"))).isNotEmpty();
    }

    /** A route, method or status the contract lacks is a violation. */
    @Test
    void unlistedCallsAreViolations()
    {
        Contract contract = new Contract(YAML);

        assertThat(contract.check(reply("GET", "/nowhere", 200,
                "application/json", "{}"))).hasSize(1);
        assertThat(contract.check(reply("DELETE", "/items/abc/status", 200,
                "application/json", "{}"))).hasSize(1);
        assertThat(contract.check(reply("POST", "/items/abc/restore", 200,
                "application/json", "{}"))).hasSize(1);
        assertThat(contract.violations()).hasSize(3);
    }

    /** An answer without a request ID is a violation. */
    @Test
    void requestIdIsRequired()
    {
        Contract contract = new Contract(YAML);
        Reply bare = new Reply("convert", "GET", Contract.PREFIX
                + "/items/abc/status", "vic", 200, Map.of("content-type",
                        List.of("application/json")), "{}");

        assertThat(contract.check(bare)).singleElement().asString()
                .contains("X-Request-Id");
    }

    /** An unauthenticated call needn't be a listed route. */
    @Test
    void unauthenticatedNeedNotBeListed()
    {
        Contract contract = new Contract(YAML);

        assertThat(contract.check(reply("GET", "/nowhere", 401,
                "application/problem+json", "{\"type\":\"t\",\"title\":"
                        + "\"unauthenticated\",\"status\":401}"))).isEmpty();
    }
}
