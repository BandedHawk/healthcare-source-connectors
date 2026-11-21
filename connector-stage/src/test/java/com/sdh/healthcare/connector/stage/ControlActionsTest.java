/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.CliOperation;
import com.sdh.healthcare.connector.stage.admin.FailureCommands;
import com.sdh.healthcare.connector.stage.admin.Followed;
import com.sdh.healthcare.connector.stage.admin.Invocation;
import com.sdh.healthcare.connector.stage.admin.Permission;
import com.sdh.healthcare.connector.stage.control.ControlServer;
import com.sun.net.httpserver.HttpServer;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The control API's actions (CONTROL-API-DESIGN.md §5, §8, build step C3),
 * served by a test stage over TLS: an action needs an idempotency key and
 * a checked reason, which is audited; a retry with its key is answered
 * with the first answer and isn't carried out again; the key with another
 * request is refused. A long operation is answered {@code 202} with its
 * record, which is brought up to date from the stage as it is read.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ControlActionsTest
{

    /** A reason, synthetic, within the reason's characters. */
    static final String REASON = "Ticket OPS-42: not correctable";

    /** Where each test resubmission stands, by its request's ID. */
    static final Map<UUID, Followed.State> RESUBMISSIONS =
            new ConcurrentHashMap<>();

    /** Captures each test's audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** Serves the issuer's key set. */
    static HttpServer keys;

    /** The test CA and the server's certificate. */
    static ControlServerTest.Pki pki;

    /** The registry's schema. */
    static String schema;

    /** The control API. */
    static ConfigurableApplicationContext server;

    /** The API's port. */
    static int port;

    /** Creates the tests; JUnit runs each test on a new instance. */
    ControlActionsTest()
    {
    }

    /**
     * Starts the control API of a stage with the fail store's commands and
     * a test resubmission.
     *
     * @param directory where the certificates go
     * @throws Exception if it can't be started
     */
    @BeforeAll
    static void start(@TempDir Path directory) throws Exception
    {
        keys = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        byte[] jwks = new JWKSet(ControlServerTest.ISSUER_KEY.toPublicJWK())
                .toString().getBytes(UTF_8);
        keys.createContext("/jwks", exchange ->
        {
            exchange.getResponseHeaders().add("Content-Type",
                    "application/json");
            exchange.sendResponseHeaders(200, jwks.length);
            exchange.getResponseBody().write(jwks);
            exchange.close();
        });
        keys.start();
        pki = new ControlServerTest.Pki(directory);
        schema = StageTestSupport.freshSchema();
        StageTestSupport.registry(schema, "pod-a");
        port = ControlServerTest.freePort();
        List<String> args = new ArrayList<>();
        args.add("--sdh.lease.jdbc.url="
                + StageTestSupport.POSTGRES.getJdbcUrl("postgres", "postgres"));
        args.add("--sdh.lease.jdbc.username=postgres");
        args.add("--sdh.lease.jdbc.password=postgres");
        args.add("--sdh.lease.jdbc.table=" + schema + "_lease");
        args.add("--sdh.registry-schema=" + schema);
        args.add("--sdh.control.port=" + port);
        args.add("--sdh.control.probe-port=" + ControlServerTest.freePort());
        args.add("--sdh.control.tls.certificate=" + pki.serverCertificate);
        args.add("--sdh.control.tls.key=" + pki.serverKey);
        args.add("--sdh.control.permissions[pipeline.view]=viewers,operators");
        args.add("--sdh.control.permissions[pipeline.operate]=operators");
        args.add("--sdh.control.permissions[phi.export]=exporters");
        args.add("--sdh.control.auth.jwt.jwk-set-uri=http://127.0.0.1:"
                + keys.getAddress().getPort() + "/jwks");
        args.add("--sdh.control.auth.jwt.issuer-uri="
                + ControlServerTest.ISSUER);
        args.add("--sdh.control.auth.jwt.audience="
                + ControlServerTest.AUDIENCE);
        server = ControlServer.start(cli(), args);
    }

    /** Stops the control API and the key server. */
    @AfterAll
    static void stop()
    {
        if (server != null)
        {
            server.close();
        }
        keys.stop(0);
    }

    /**
     * The test stage's commands: the fail store's, and a resubmission
     * the test follows.
     *
     * @return them
     */
    static AdminCli<FailureCommandsTest.TestSettings,
            FailureCommandsTest.Context> cli()
    {
        List<AdminCommand<FailureCommandsTest.Context>> commands =
                new ArrayList<>(FailureCommands.all());
        commands.add(new Resubmit());
        return new AdminCli<>(new AdminCli.Stage<>("test-stage.jar",
                "test-stage", FailureCommandsTest.Configuration.class,
                FailureCommandsTest.TestSettings.class,
                FailureCommandsTest.TestSettings::telemetry,
                FailureCommandsTest.Context::new, StageAdminTest::audit),
                commands);
    }

    /**
     * An action without a key, without a reason, with a reason outside
     * its characters, with a field it doesn't take, or without its
     * permission is refused, and nothing is done.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesAnActionWithoutAKeyOrAReason() throws Exception
    {
        UUID item = failed(schema);
        String path = "/api/v1/failures/" + item + "/2/decline";
        String operator = ControlServerTest.token("olga", "operators");
        HttpResponse<String> noKey = send("POST", path, null, operator,
                body("reason", REASON));
        assertThat(noKey.statusCode()).isEqualTo(400);
        assertThat(ControlServerTest.field(noKey.body(), "detail"))
                .contains("Idempotency-Key");

        HttpResponse<String> noReason = send("POST", path, key(), operator,
                "{}");
        assertThat(noReason.statusCode()).isEqualTo(400);
        assertThat(ControlServerTest.field(noReason.body(), "detail"))
                .contains("reason");

        HttpResponse<String> shaped = send("POST", path, key(), operator,
                body("reason", "born <1970-01-01>"));
        assertThat(shaped.statusCode()).isEqualTo(400);
        assertThat(shaped.body()).doesNotContain("1970");

        HttpResponse<String> unknown = send("POST", path, key(), operator,
                body("reason", REASON, "itemId", item.toString()));
        assertThat(unknown.statusCode()).isEqualTo(400);
        assertThat(ControlServerTest.field(unknown.body(), "detail"))
                .isEqualTo("Unknown field: itemId");

        HttpResponse<String> viewer = send("POST", path, key(),
                ControlServerTest.token("vera", "viewers"),
                body("reason", REASON));
        assertThat(viewer.statusCode()).isEqualTo(403);
        assertThat(audit.all("control.decline"))
                .extracting(event -> event.get("sdh.decision"))
                .containsExactly("bad-request", "bad-request", "bad-request",
                        "bad-request", "denied");

        HttpResponse<String> absent = send("DELETE",
                "/api/v1/reprocess-runs/r-1", key(), operator,
                body("reason", REASON));
        assertThat(absent.statusCode()).isEqualTo(404);
        assertThat(store().counts(item))
                .isEqualTo(new FailureStore.Counts(1, 0, 0));
    }

    /**
     * An action with its key and reason is carried out and audited with
     * the reason; a retry with the key gets the first answer back, audited
     * as a replay, and isn't carried out again; the key with another body
     * is refused; a new key for what can't be done again is refused by
     * the stage, by its class only.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void carriesOutAnActionOncePerKey() throws Exception
    {
        UUID item = failed(schema);
        String path = "/api/v1/failures/" + item + "/2/decline";
        String operator = ControlServerTest.token("olga", "operators");
        String key = key();
        String body = body("reason", REASON);
        HttpResponse<String> declined = send("POST", path, key, operator,
                body);
        assertThat(declined.statusCode()).as(declined.body()).isEqualTo(200);
        Map<String, Object> answer = JSONObjectUtils.parse(declined.body());
        assertThat(answer).containsEntry("itemId", item.toString())
                .containsEntry("seq", 2L);
        assertThat(declined.body()).doesNotContain(REASON);
        assertThat(store().counts(item))
                .isEqualTo(new FailureStore.Counts(0, 1, 0));
        assertThat(audit.attributes("control.decline"))
                .containsEntry("sdh.operator", "olga")
                .containsEntry("sdh.permission", "pipeline.operate")
                .containsEntry("sdh.decision", "allowed")
                .containsEntry("sdh.action_reason", REASON)
                .containsEntry("sdh.result", "ok");

        HttpResponse<String> retried = send("POST", path, key, operator,
                body);
        assertThat(retried.statusCode()).isEqualTo(200);
        assertThat(retried.body()).isEqualTo(declined.body());
        assertThat(audit.all("control.decline")).hasSize(2).last()
                .satisfies(event -> assertThat(event)
                        .containsEntry("sdh.idempotent_replay", "true")
                        .containsEntry("sdh.result", "replayed"));
        assertThat(audit.names()).filteredOn("failure.declined"::equals)
                .hasSize(1);
        assertThat(store().counts(item))
                .isEqualTo(new FailureStore.Counts(0, 1, 0));

        HttpResponse<String> reused = send("POST", path, key, operator,
                body("reason", "another reason"));
        assertThat(reused.statusCode()).isEqualTo(409);
        assertThat(ControlServerTest.field(reused.body(), "title"))
                .isEqualTo("idempotency-key-reused");

        HttpResponse<String> again = send("POST", path, key(), operator,
                body);
        assertThat(again.statusCode()).isEqualTo(409);
        assertThat(ControlServerTest.field(again.body(), "title"))
                .isEqualTo("refused");
        assertThat(ControlServerTest.field(again.body(), "detail"))
                .isEqualTo("IllegalStateException");
    }

    /**
     * A correction is given in the body, base64, and needs PHI export's
     * permission as well as an operator's.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void takesACorrectedRecordFromTheBody() throws Exception
    {
        UUID item = failed(schema);
        String path = "/api/v1/failures/" + item + "/2/correct";
        String record = Base64.getEncoder().encodeToString(
                "<synthetic fixed/>".getBytes(UTF_8));
        String body = body("reason", REASON, "record", record);
        HttpResponse<String> operatorOnly = send("POST", path, key(),
                ControlServerTest.token("olga", "operators"), body);
        assertThat(operatorOnly.statusCode()).isEqualTo(403);
        assertThat(ControlServerTest.field(operatorOnly.body(), "detail"))
                .contains("phi.export");

        HttpResponse<String> notBase64 = send("POST", path, key(),
                ControlServerTest.token("ed", "operators", "exporters"),
                body("reason", REASON, "record", "%%%"));
        assertThat(notBase64.statusCode()).isEqualTo(400);

        HttpResponse<String> corrected = send("POST", path, key(),
                ControlServerTest.token("ed", "operators", "exporters"),
                body);
        assertThat(corrected.statusCode()).as(corrected.body())
                .isEqualTo(200);
        assertThat(JSONObjectUtils.parse(corrected.body()))
                .containsEntry("correction", 1L)
                .containsEntry("bytes", 18L);
        assertThat(corrected.body()).doesNotContain("synthetic");
    }

    /**
     * A long operation is answered {@code 202}, with its record's place;
     * reading the record brings it up to date from the stage, and a
     * record the stage can't be read for says so, by the failure's class.
     * A retry with the key is answered with the same record.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void followsALongOperationThroughItsRecord() throws Exception
    {
        UUID item = UUID.randomUUID();
        String operator = ControlServerTest.token("olga", "operators");
        String key = key();
        String body = body("reason", REASON);
        HttpResponse<String> started = send("POST", "/api/v1/items/" + item
                + "/resubmit", key, operator, body);
        assertThat(started.statusCode()).as(started.body()).isEqualTo(202);
        String location = started.headers().firstValue("Location")
                .orElseThrow();
        Map<String, Object> record = JSONObjectUtils.parse(started.body());
        assertThat(location).isEqualTo("/api/v1/operations/"
                + record.get("id"));
        assertThat(record).containsEntry("kind", "resubmit")
                .containsEntry("scope", "stage:test-stage")
                .containsEntry("state", "running")
                .containsEntry("requestedBy", "olga")
                .containsEntry("reason", REASON);
        assertThat(audit.attributes("control.resubmit"))
                .containsEntry("sdh.operation_id", record.get("id"));
        UUID request = UUID.fromString(String.valueOf(
                JSONObjectUtils.getJSONObject(record, "parameters")
                        .get("requestId")));

        HttpResponse<String> retried = send("POST", "/api/v1/items/" + item
                + "/resubmit", key, operator, body);
        assertThat(retried.statusCode()).isEqualTo(202);
        assertThat(retried.headers().firstValue("Location"))
                .hasValue(location);
        assertThat(RESUBMISSIONS).hasSize(1);

        String viewer = ControlServerTest.token("vera", "viewers");
        Map<String, Object> running = JSONObjectUtils.parse(
                get(location, viewer).body());
        assertThat(running).containsEntry("state", "running");
        assertThat(JSONObjectUtils.getJSONObject(running, "progress"))
                .containsEntry("looks", 1L);

        RESUBMISSIONS.put(request, Followed.State.DONE);
        Map<String, Object> done = JSONObjectUtils.parse(
                get(location, viewer).body());
        assertThat(done).containsEntry("state", "done");

        RESUBMISSIONS.remove(request);
        Map<String, Object> still = JSONObjectUtils.parse(
                get(location, viewer).body());
        assertThat(still).containsEntry("state", "done")
                .doesNotContainKey("progressUnavailable");

        HttpResponse<String> lost = send("POST", "/api/v1/items/"
                + UUID.randomUUID() + "/resubmit", key(), operator, body);
        RESUBMISSIONS.clear();
        Map<String, Object> unread = JSONObjectUtils.parse(get(
                lost.headers().firstValue("Location").orElseThrow(), viewer)
                .body());
        assertThat(unread).containsEntry("state", "running")
                .containsEntry("progressUnavailable",
                        "IllegalStateException");

        assertThat(get("/api/v1/operations/" + UUID.randomUUID(), viewer)
                .statusCode()).isEqualTo(404);
        assertThat(get("/api/v1/operations/not-an-id", viewer).statusCode())
                .isEqualTo(400);
        assertThat(get(location, ControlServerTest.token("mallory"))
                .statusCode()).isEqualTo(403);
    }

    /**
     * The stage's records are listed newest first, a page at a time,
     * each page naming the next.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void listsTheRecordsAPageAtATime() throws Exception
    {
        String operator = ControlServerTest.token("olga", "operators");
        List<Object> ids = new ArrayList<>();
        for (int i = 0; i < 3; i++)
        {
            HttpResponse<String> started = send("POST", "/api/v1/items/"
                    + UUID.randomUUID() + "/resubmit", key(), operator,
                    body("reason", REASON));
            ids.addFirst(JSONObjectUtils.parse(started.body()).get("id"));
        }
        String viewer = ControlServerTest.token("vera", "viewers");
        Map<String, Object> first = JSONObjectUtils.parse(get(
                "/api/v1/operations?limit=2", viewer).body());
        assertThat(JSONObjectUtils.getJSONObjectArray(first, "operations"))
                .extracting(each -> each.get("id"))
                .containsExactly(ids.get(0), ids.get(1));
        String next = (String) first.get("next");
        assertThat(next).isNotNull();
        Map<String, Object> second = JSONObjectUtils.parse(get(
                "/api/v1/operations?limit=2&cursor=" + next, viewer).body());
        assertThat(JSONObjectUtils.getJSONObjectArray(second, "operations"))
                .extracting(each -> each.get("id"))
                .startsWith(ids.get(2));

        assertThat(get("/api/v1/operations?limit=0", viewer).statusCode())
                .isEqualTo(400);
        assertThat(get("/api/v1/operations?cursor=nope", viewer)
                .statusCode()).isEqualTo(400);
    }

    /**
     * An item with one record failed, seq 2.
     *
     * @param schema the registry's schema
     * @return the item
     */
    static UUID failed(String schema)
    {
        UUID item = UUID.randomUUID();
        StageRegistry registry = StageTestSupport.registry(schema, "pod-a");
        registry.expect(item, 2);
        registry.claim("r1", item, Duration.ofMinutes(5));
        registry.claim("r2", item, Duration.ofMinutes(5));
        registry.store(List.of(StageAdminTest.entry("r1", item, false),
                new StageRegistry.Entry("r2", item, "d-r2", "v1", true, null,
                        new FailureStore.Failure(item, 2, "r2",
                                "TEST_FAILED", 10, "v1"))));
        return item;
    }

    /**
     * The fail store, read directly.
     *
     * @return it
     */
    static FailureStore store()
    {
        return StageTestSupport.registry(schema, "pod-a").failureStore();
    }

    /**
     * A new idempotency key.
     *
     * @return it
     */
    static String key()
    {
        return "test-" + UUID.randomUUID();
    }

    /**
     * A JSON body.
     *
     * @param fields its fields, name then value
     * @return it
     */
    static String body(String... fields)
    {
        Map<String, Object> body = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2)
        {
            body.put(fields[i], fields[i + 1]);
        }
        return JSONObjectUtils.toJSONString(body);
    }

    /**
     * Sends a {@code GET} to the API.
     *
     * @param path  the path
     * @param token the bearer token
     * @return the response
     * @throws Exception if it can't be sent
     */
    static HttpResponse<String> get(String path, String token)
            throws Exception
    {
        return ControlServerTest.client(pki.clientContext(null)).send(
                HttpRequest.newBuilder(URI.create("https://localhost:"
                        + port + path))
                        .header("Authorization", "Bearer " + token).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Sends an action to the API.
     *
     * @param method the method
     * @param path   the path
     * @param key    the idempotency key; null for none
     * @param token  the bearer token
     * @param body   the JSON body
     * @return the response
     * @throws Exception if it can't be sent
     */
    static HttpResponse<String> send(String method, String path, String key,
            String token, String body) throws Exception
    {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(
                "https://localhost:" + port + path))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(body));
        if (key != null)
        {
            request.header("Idempotency-Key", key);
        }
        return ControlServerTest.client(pki.clientContext(null))
                .send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    /**
     * A resubmission the test follows: it is asked, and stands as
     * {@link #RESUBMISSIONS} says; one not there can't be read.
     */
    static final class Resubmit implements
            CliOperation<FailureCommandsTest.Context, UUID, UUID>,
            Followed<FailureCommandsTest.Context, UUID, UUID>
    {

        /** Created by the test. */
        Resubmit()
        {
        }

        @Override
        public String name()
        {
            return "resubmit";
        }

        @Override
        public String usage()
        {
            return "resubmit --item-id <id> [--again]";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("item-id");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of("again");
        }

        @Override
        public Permission permission()
        {
            return Permission.PIPELINE_OPERATE;
        }

        @Override
        public Kind kind()
        {
            return Kind.LONG;
        }

        @Override
        public UUID bind(AdminArgs arguments,
                FailureCommandsTest.Context context)
        {
            return UUID.fromString(arguments.required("item-id"));
        }

        @Override
        public UUID run(UUID itemId, FailureCommandsTest.Context context,
                Invocation invocation)
        {
            UUID request = UUID.randomUUID();
            RESUBMISSIONS.put(request, Followed.State.RUNNING);
            return request;
        }

        @Override
        public void print(UUID result, PrintStream out)
        {
            out.println(result);
        }

        @Override
        public Optional<Map<String, Object>> started(UUID result)
        {
            return Optional.of(Map.of("requestId", result.toString()));
        }

        @Override
        public Progress progress(Map<String, Object> parameters,
                FailureCommandsTest.Context context)
        {
            Followed.State state = RESUBMISSIONS.get(UUID.fromString(
                    String.valueOf(parameters.get("requestId"))));
            if (state == null)
            {
                throw new IllegalStateException("No such request");
            }
            return new Progress(state, Map.of("looks", 1));
        }
    }
}
