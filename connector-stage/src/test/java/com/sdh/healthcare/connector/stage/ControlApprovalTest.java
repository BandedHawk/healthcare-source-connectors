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
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.CliOperation;
import com.sdh.healthcare.connector.stage.admin.FailureCommands;
import com.sdh.healthcare.connector.stage.admin.Invocation;
import com.sdh.healthcare.connector.stage.admin.Permission;
import com.sdh.healthcare.connector.stage.control.ControlServer;
import com.sun.net.httpserver.HttpServer;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Four eyes and break-glass through the control API (CONTROL-API-DESIGN.md
 * §6, build step C5), served by a test stage over TLS: an operation that
 * needs a second person waits for one; another principal with the same
 * permissions approves or rejects it, never its requester; the requester
 * may cancel it; one waiting too long expires; and once approved, a
 * control pod runs it, audited with both. A break-glass result is held
 * only with a stage KEK, and fetched only by its requester.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ControlApprovalTest
{

    /** A reason, synthetic, within the reason's characters. */
    static final String REASON = "Ticket OPS-91: retention";

    /** Captures each test's audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** Serves the issuer's key set. */
    static HttpServer keys;

    /** The registry's schema. */
    static String schema;

    /** The control API. */
    static ConfigurableApplicationContext server;

    /** Creates the tests; JUnit runs each test on a new instance. */
    ControlApprovalTest()
    {
    }

    /**
     * Starts the control API of a stage with the fail store's commands
     * and a test restore.
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
        ControlSealedTest.pki = new ControlServerTest.Pki(directory);
        schema = StageTestSupport.freshSchema();
        StageTestSupport.registry(schema, "pod-a");
        ControlSealedTest.port = ControlServerTest.freePort();
        List<String> args = new ArrayList<>();
        args.add("--sdh.lease.jdbc.url="
                + StageTestSupport.POSTGRES.getJdbcUrl("postgres", "postgres"));
        args.add("--sdh.lease.jdbc.username=postgres");
        args.add("--sdh.lease.jdbc.password=postgres");
        args.add("--sdh.lease.jdbc.table=" + schema + "_lease");
        args.add("--sdh.registry-schema=" + schema);
        args.add("--sdh.control.port=" + ControlSealedTest.port);
        args.add("--sdh.control.probe-port=" + ControlServerTest.freePort());
        args.add("--sdh.control.tls.certificate="
                + ControlSealedTest.pki.serverCertificate);
        args.add("--sdh.control.tls.key=" + ControlSealedTest.pki.serverKey);
        args.add("--sdh.control.permissions[pipeline.view]=viewers,"
                + "auditadmins,breakglass");
        args.add("--sdh.control.permissions[audit.admin]=auditadmins");
        args.add("--sdh.control.permissions[phi.break-glass]=breakglass");
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
     * The test stage's commands: the fail store's, and a restore whose
     * plaintext is synthetic.
     *
     * @return them
     */
    static AdminCli<FailureCommandsTest.TestSettings,
            FailureCommandsTest.Context> cli()
    {
        List<AdminCommand<FailureCommandsTest.Context>> commands =
                new ArrayList<>(FailureCommands.all());
        commands.add(new Restore());
        return new AdminCli<>(new AdminCli.Stage<>("test-stage.jar",
                "test-stage", FailureCommandsTest.Configuration.class,
                FailureCommandsTest.TestSettings.class,
                FailureCommandsTest.TestSettings::telemetry,
                FailureCommandsTest.Context::new, StageAdminTest::audit),
                commands);
    }

    /**
     * An audit prune waits for a second person: its requester can't
     * approve it, nor can a caller without {@code audit.admin}; another
     * audit admin does, once, and a control pod runs it, audited with
     * both. A retry with its key gets the first answer.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void prunesOnceASecondPersonApproves() throws Exception
    {
        String alice = ControlServerTest.token("alice", "auditadmins");
        String bob = ControlServerTest.token("bob", "auditadmins");
        String key = ControlSealedTest.key();
        String request = prune();
        HttpResponse<String> asked = ControlSealedTest.send("POST",
                "/api/v1/audit/prune", key, alice, request);
        assertThat(asked.statusCode()).as(asked.body()).isEqualTo(202);
        Map<String, Object> waiting = JSONObjectUtils.parse(asked.body());
        String id = (String) waiting.get("id");
        assertThat(waiting).containsEntry("state", "awaiting-approval")
                .containsEntry("kind", "audit-prune")
                .containsEntry("requestedBy", "alice");
        assertThat(asked.headers().firstValue("Location"))
                .hasValue("/api/v1/operations/" + id);
        assertThat(JSONObjectUtils.getJSONObject(waiting, "parameters"))
                .containsEntry("permissions", List.of("audit.admin"))
                .containsKey("approveBy");
        assertThat(audit.attributes("control.audit_prune"))
                .containsEntry("sdh.outcome", "awaiting-approval")
                .containsEntry("sdh.operation_id", id)
                .containsEntry("sdh.result", "ok");
        HttpResponse<String> retried = ControlSealedTest.send("POST",
                "/api/v1/audit/prune", key, alice, request);
        assertThat(retried.statusCode()).isEqualTo(202);
        assertThat(retried.body()).isEqualTo(asked.body());

        String path = "/api/v1/operations/" + id;
        HttpResponse<String> own = decide(path, "approve", alice);
        assertThat(own.statusCode()).isEqualTo(403);
        assertThat(ControlServerTest.field(own.body(), "title"))
                .isEqualTo("own-request");
        HttpResponse<String> viewer = decide(path, "approve",
                ControlServerTest.token("vera", "viewers"));
        assertThat(viewer.statusCode()).isEqualTo(403);
        assertThat(ControlServerTest.field(viewer.body(), "title"))
                .isEqualTo("permission-required");
        assertThat(ControlSealedTest.send("POST", path + "/approve", null,
                bob, "{}").statusCode()).isEqualTo(400);

        HttpResponse<String> approved = decide(path, "approve", bob);
        assertThat(approved.statusCode()).as(approved.body()).isEqualTo(200);
        assertThat(JSONObjectUtils.parse(approved.body()))
                .containsEntry("decidedBy", "bob")
                .containsEntry("decision", REASON);
        assertThat(audit.all("control.operation_approve")).anySatisfy(
                event -> assertThat(event).containsEntry("sdh.operator",
                        "bob").containsEntry("sdh.requested_by", "alice")
                        .containsEntry("sdh.result", "ok"));
        HttpResponse<String> twice = decide(path, "approve",
                ControlServerTest.token("carl", "auditadmins"));
        assertThat(twice.statusCode()).isEqualTo(409);
        assertThat(ControlServerTest.field(twice.body(), "title"))
                .isEqualTo("wrong-state");

        Map<String, Object> done = finished(path, alice);
        assertThat(done).containsEntry("state", "done");
        assertThat(JSONObjectUtils.getJSONObject(done, "result"))
                .containsKeys("chains", "entries");
        assertThat(audit.all("control.audit_prune")).anySatisfy(event ->
                assertThat(event).containsEntry("sdh.operator", "alice")
                        .containsEntry("sdh.approved_by", "bob")
                        .containsEntry("sdh.operation_id", id)
                        .containsEntry("sdh.result", "ok"));
    }

    /**
     * A rejected request never runs; only its requester cancels one; one
     * waiting past its time expires, and can't then be approved.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void rejectsCancelsAndExpires() throws Exception
    {
        String alice = ControlServerTest.token("alice", "auditadmins");
        String bob = ControlServerTest.token("bob", "auditadmins");

        String rejected = requested(alice);
        assertThat(decide(rejected, "reject", bob).statusCode())
                .isEqualTo(200);
        assertThat(state(rejected, alice)).isEqualTo("rejected");
        assertThat(decide(rejected, "approve", bob).statusCode())
                .isEqualTo(409);

        String cancelled = requested(alice);
        HttpResponse<String> notTheirs = decide(cancelled, "cancel", bob);
        assertThat(notTheirs.statusCode()).isEqualTo(403);
        assertThat(ControlServerTest.field(notTheirs.body(), "title"))
                .isEqualTo("not-your-request");
        assertThat(decide(cancelled, "cancel", alice).statusCode())
                .isEqualTo(200);
        assertThat(state(cancelled, alice)).isEqualTo("cancelled");

        String late = requested(alice);
        try (Connection connection = StageTestSupport.database()
                .getConnection();
                PreparedStatement update = connection.prepareStatement(
                        "UPDATE " + schema + ".sdh_control_operations SET"
                                + " parameters = regexp_replace(parameters,"
                                + " '\"approveBy\":\"[^\"]*\"',"
                                + " '\"approveBy\":\"2000-01-01T00:00:00Z\"')"
                                + " WHERE id = ?"))
        {
            update.setObject(1, UUID.fromString(late.substring(
                    late.lastIndexOf('/') + 1)));
            assertThat(update.executeUpdate()).isOne();
        }
        assertThat(decide(late, "approve", bob).statusCode())
                .isEqualTo(409);
        assertThat(state(late, alice)).isEqualTo("expired");
    }

    /**
     * A plaintext restore needs a ticket and four eyes; once approved, its
     * result is held only under a stage KEK, so on a stage without one it
     * fails, holding nothing; only its requester fetches a result.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void holdsABreakGlassResultOnlyWithAStageKey() throws Exception
    {
        String ana = ControlServerTest.token("ana", "breakglass");
        String path = "/api/v1/items/" + UUID.randomUUID()
                + "/restore/plaintext";
        HttpResponse<String> noTicket = ControlSealedTest.send("POST", path,
                ControlSealedTest.key(), ana, ControlSealedTest.body(
                        "reason", REASON));
        assertThat(noTicket.statusCode()).isEqualTo(400);
        assertThat(ControlServerTest.field(noTicket.body(), "detail"))
                .contains("ticket");
        HttpResponse<String> noPermission = ControlSealedTest.send("POST",
                path, ControlSealedTest.key(), ControlServerTest.token(
                        "vera", "viewers"), ControlSealedTest.body("reason",
                                REASON, "ticket", "INC-7"));
        assertThat(noPermission.statusCode()).isEqualTo(403);

        HttpResponse<String> asked = ControlSealedTest.send("POST", path,
                ControlSealedTest.key(), ana, ControlSealedTest.body(
                        "reason", REASON, "ticket", "INC-7"));
        assertThat(asked.statusCode()).as(asked.body()).isEqualTo(202);
        Map<String, Object> waiting = JSONObjectUtils.parse(asked.body());
        assertThat(waiting).containsEntry("ticket", "INC-7");
        String operation = "/api/v1/operations/" + waiting.get("id");
        assertThat(decide(operation, "approve", ControlServerTest.token(
                "ben", "breakglass")).statusCode()).isEqualTo(200);

        Map<String, Object> failed = finished(operation, ana);
        assertThat(failed).containsEntry("state", "failed");
        assertThat(JSONObjectUtils.getJSONObject(failed, "result"))
                .containsEntry("failure", "IllegalStateException");
        assertThat(ControlSealedTest.get(operation + "/result", ana)
                .statusCode()).isEqualTo(410);
        HttpResponse<String> other = ControlSealedTest.get(operation
                + "/result", ControlServerTest.token("ben", "breakglass"));
        assertThat(other.statusCode()).isEqualTo(403);
        assertThat(ControlServerTest.field(other.body(), "title"))
                .isEqualTo("not-your-request");
        assertThat(ControlSealedTest.get(operation + "/result",
                ControlServerTest.token("ana", "viewers")).statusCode())
                .isEqualTo(403);
    }

    /**
     * An audit prune's body: chains last written over a year ago.
     *
     * @return it
     */
    static String prune()
    {
        return ControlSealedTest.body("reason", REASON, "before",
                Instant.now().minus(Duration.ofDays(400)).toString());
    }

    /**
     * Asks for an audit prune.
     *
     * @param token the requester's token
     * @return its record's path
     * @throws Exception if it can't be asked
     */
    static String requested(String token) throws Exception
    {
        HttpResponse<String> asked = ControlSealedTest.send("POST",
                "/api/v1/audit/prune", ControlSealedTest.key(), token,
                prune());
        assertThat(asked.statusCode()).as(asked.body()).isEqualTo(202);
        return asked.headers().firstValue("Location").orElseThrow();
    }

    /**
     * Decides a request.
     *
     * @param path     its record's path
     * @param decision approve, reject or cancel
     * @param token    the caller's token
     * @return the answer
     * @throws Exception if it can't be sent
     */
    static HttpResponse<String> decide(String path, String decision,
            String token) throws Exception
    {
        return ControlSealedTest.send("POST", path + "/" + decision, null,
                token, ControlSealedTest.body("reason", REASON));
    }

    /**
     * An operation's state.
     *
     * @param path  its record's path
     * @param token the caller's token
     * @return the state
     * @throws Exception if it can't be read
     */
    static String state(String path, String token) throws Exception
    {
        return ControlServerTest.field(ControlSealedTest.get(path, token)
                .body(), "state");
    }

    /**
     * An operation's record once it has finished, waiting up to 30
     * seconds for its run.
     *
     * @param path  its record's path
     * @param token the caller's token
     * @return the record
     * @throws Exception if it can't be read, or doesn't finish
     */
    static Map<String, Object> finished(String path, String token)
            throws Exception
    {
        Instant until = Instant.now().plusSeconds(30);
        while (true)
        {
            Map<String, Object> record = JSONObjectUtils.parse(
                    ControlSealedTest.get(path, token).body());
            Object state = record.get("state");
            if ("done".equals(state) || "failed".equals(state)
                    || Instant.now().isAfter(until))
            {
                return record;
            }
            Thread.sleep(100);
        }
    }

    /**
     * A restore whose plaintext is synthetic: it needs four eyes and
     * break-glass when plaintext, as the split stage's does.
     */
    static final class Restore implements CliOperation<
            FailureCommandsTest.Context, Boolean, Long>
    {

        /** Created by the test. */
        Restore()
        {
        }

        @Override
        public String name()
        {
            return "restore";
        }

        @Override
        public String usage()
        {
            return "restore --item-id <id> --plaintext --ticket <ref>";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("item-id", "from-offset", "ticket");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of("plaintext", "unprocessed");
        }

        @Override
        public Permission permission()
        {
            return Permission.PHI_EXPORT;
        }

        @Override
        public Set<Permission> permissions(Boolean plaintext)
        {
            return EnumSet.of(plaintext ? Permission.PHI_BREAK_GLASS
                    : Permission.PHI_EXPORT);
        }

        @Override
        public Approval approval(Boolean plaintext)
        {
            return plaintext ? Approval.FOUR_EYES : Approval.NONE;
        }

        @Override
        public Kind kind()
        {
            return Kind.SEALED;
        }

        @Override
        public Boolean bind(AdminArgs arguments,
                FailureCommandsTest.Context context)
        {
            UUID.fromString(arguments.required("item-id"));
            return arguments.flag("plaintext");
        }

        @Override
        public Long run(Boolean plaintext,
                FailureCommandsTest.Context context, Invocation invocation)
        {
            byte[] part = "synthetic plaintext".getBytes(UTF_8);
            invocation.sink().plaintext(part);
            return (long) part.length;
        }

        @Override
        public void print(Long result, PrintStream out)
        {
            out.println(result);
        }
    }
}
