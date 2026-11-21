/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.crypto.ECDHDecrypter;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.FailureCommands;
import com.sdh.healthcare.connector.stage.control.ControlServer;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.interfaces.ECPrivateKey;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The control API's sealing keys and sealed results (CONTROL-API-DESIGN.md
 * §7, build step C4), served by a test stage over TLS: a key is registered
 * for a principal by someone else with {@code keys.admin}, never by the
 * principal itself; a PHI result is sealed only to one of the caller's own
 * keys, unexpired and not revoked, and streamed as a header, sealed parts
 * and a trailer that proves it whole.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ControlSealedTest
{

    /** A reason, synthetic, within the reason's characters. */
    static final String REASON = "Ticket OPS-77: incident review";

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
    ControlSealedTest()
    {
    }

    /**
     * Starts the control API of a stage with the fail store's commands.
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
        args.add("--sdh.control.permissions[pipeline.view]=viewers");
        args.add("--sdh.control.permissions[phi.export]=exporters");
        args.add("--sdh.control.permissions[keys.admin]=keyadmins");
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
     * The test stage's commands: the fail store's.
     *
     * @return them
     */
    static AdminCli<FailureCommandsTest.TestSettings,
            FailureCommandsTest.Context> cli()
    {
        return new AdminCli<>(new AdminCli.Stage<>("test-stage.jar",
                "test-stage", FailureCommandsTest.Configuration.class,
                FailureCommandsTest.TestSettings.class,
                FailureCommandsTest.TestSettings::telemetry,
                FailureCommandsTest.Context::new, StageAdminTest::audit),
                FailureCommands.all());
    }

    /**
     * A key admin registers a key for another principal, audited; nobody
     * registers one for themselves; a key too weak, expiring too late or
     * already, or registered already is refused. The keys are listed
     * without the keys, and a revoked key stays listed, revoked.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void registersAKeyForSomeoneElseOnly() throws Exception
    {
        String admin = ControlServerTest.token("alice", "keyadmins");
        String principal = "ed-" + UUID.randomUUID();
        KeyPair pair = ec();
        String pem = pem(pair);
        Instant expires = Instant.now().plus(Duration.ofDays(30));
        HttpResponse<String> registered = register(admin, principal, pem,
                expires);
        assertThat(registered.statusCode()).as(registered.body())
                .isEqualTo(200);
        Map<String, Object> key = JSONObjectUtils.parse(registered.body());
        String fingerprint = HexFormat.of().formatHex(MessageDigest
                .getInstance("SHA-256").digest(pair.getPublic()
                        .getEncoded()));
        assertThat(key).containsEntry("fingerprint", fingerprint)
                .containsEntry("principal", principal)
                .containsEntry("registeredBy", "alice")
                .doesNotContainKey("publicKey");
        assertThat(audit.attributes("control.operator_key_register"))
                .containsEntry("sdh.sealing_key", fingerprint)
                .containsEntry("sdh.key_principal", principal)
                .containsEntry("sdh.permission", "keys.admin")
                .containsEntry("sdh.result", "ok");

        HttpResponse<String> self = register(ControlServerTest.token(
                principal, "keyadmins"), principal, pem(ec()), expires);
        assertThat(self.statusCode()).isEqualTo(403);
        assertThat(ControlServerTest.field(self.body(), "title"))
                .isEqualTo("not-for-yourself");

        KeyPairGenerator rsa = KeyPairGenerator.getInstance("RSA");
        rsa.initialize(2048);
        assertThat(register(admin, principal, pem(rsa.generateKeyPair()),
                expires).statusCode()).isEqualTo(400);
        assertThat(register(admin, principal, pem(ec()),
                Instant.now().plus(Duration.ofDays(400))).statusCode())
                .isEqualTo(400);
        assertThat(register(admin, principal, pem(ec()),
                Instant.now().minusSeconds(60)).statusCode()).isEqualTo(400);
        HttpResponse<String> again = register(admin, principal, pem,
                expires);
        assertThat(again.statusCode()).isEqualTo(409);

        Map<String, Object> listed = JSONObjectUtils.parse(get(
                "/api/v1/operator-keys?principal=" + principal, admin)
                .body());
        assertThat(JSONObjectUtils.getJSONObjectArray(listed, "keys"))
                .singleElement().satisfies(each -> assertThat(each)
                        .containsEntry("fingerprint", fingerprint)
                        .containsEntry("revokedAt", null));
        assertThat(get("/api/v1/operator-keys", ControlServerTest.token(
                "vera", "viewers")).statusCode()).isEqualTo(403);

        HttpResponse<String> revoked = send("DELETE",
                "/api/v1/operator-keys/" + fingerprint, key(), admin,
                body("reason", REASON));
        assertThat(revoked.statusCode()).as(revoked.body()).isEqualTo(200);
        assertThat(JSONObjectUtils.parse(revoked.body()))
                .containsEntry("revokedBy", "alice");
        assertThat(JSONObjectUtils.parse(revoked.body()).get("revokedAt"))
                .isNotNull();
        HttpResponse<String> unknown = send("DELETE",
                "/api/v1/operator-keys/" + "0".repeat(64), key(), admin,
                body("reason", REASON));
        assertThat(unknown.statusCode()).isEqualTo(404);
        assertThat(ControlServerTest.field(unknown.body(), "title"))
                .isEqualTo("key-not-found");
    }

    /**
     * A failure's content is sealed to the caller's own key: a header, one
     * part that only its private key opens, and a trailer whose digest
     * matches. Another's key, a revoked key, or none is refused, before
     * anything is read; so is a caller without {@code phi.export}.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void sealsAFailureToTheCallersOwnKey() throws Exception
    {
        UUID item = ControlActionsTest.failed(schema);
        String admin = ControlServerTest.token("alice", "keyadmins");
        String principal = "ed-" + UUID.randomUUID();
        KeyPair pair = ec();
        String fingerprint = String.valueOf(JSONObjectUtils.parse(register(
                admin, principal, pem(pair), Instant.now().plus(
                        Duration.ofDays(30))).body()).get("fingerprint"));
        String path = "/api/v1/failures/" + item + "/2/export";
        String exporter = ControlServerTest.token(principal, "exporters");

        HttpResponse<String> sealed = send("POST", path, null, exporter,
                body("reason", REASON, "sealTo", fingerprint));
        assertThat(sealed.statusCode()).as(sealed.body()).isEqualTo(200);
        assertThat(sealed.headers().firstValue("Content-Type"))
                .hasValueSatisfying(type -> assertThat(type)
                        .startsWith("application/x-ndjson"));
        assertThat(sealed.body()).doesNotContain("synthetic");
        List<Map<String, Object>> lines = new ArrayList<>();
        for (String line : sealed.body().split("\n"))
        {
            lines.add(JSONObjectUtils.parse(line));
        }
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).containsEntry("operation", "export")
                .containsEntry("sealedTo", fingerprint)
                .containsEntry("requestId", sealed.headers()
                        .firstValue("X-Request-Id").orElseThrow());
        String jwe = (String) lines.get(1).get("jwe");
        assertThat(lines.get(1)).containsEntry("part", 1L);
        JWEObject opened = JWEObject.parse(jwe);
        opened.decrypt(new ECDHDecrypter((ECPrivateKey) pair.getPrivate()));
        assertThat(opened.getPayload().toString())
                .isEqualTo(FailureCommandsTest.FAILED_CONTENT);
        assertThat(lines.get(2)).containsEntry("end", true)
                .containsEntry("parts", 1L)
                .containsEntry("sha256", HexFormat.of().formatHex(
                        MessageDigest.getInstance("SHA-256").digest(
                                jwe.getBytes(StandardCharsets.US_ASCII))));
        assertThat(audit.attributes("control.export"))
                .containsEntry("sdh.operator", principal)
                .containsEntry("sdh.sealing_key", fingerprint)
                .containsEntry("sdh.recipient_key", fingerprint)
                .containsEntry("sdh.action_reason", REASON)
                .containsEntry("sdh.parts", 1L)
                .containsEntry("sdh.result", "ok");

        HttpResponse<String> notTheirs = send("POST", path, null,
                ControlServerTest.token("bob", "exporters"),
                body("reason", REASON, "sealTo", fingerprint));
        assertThat(notTheirs.statusCode()).isEqualTo(403);
        assertThat(ControlServerTest.field(notTheirs.body(), "title"))
                .isEqualTo("sealing-key-not-yours");

        HttpResponse<String> noExport = send("POST", path, null,
                ControlServerTest.token(principal, "viewers"),
                body("reason", REASON, "sealTo", fingerprint));
        assertThat(noExport.statusCode()).isEqualTo(403);
        assertThat(ControlServerTest.field(noExport.body(), "title"))
                .isEqualTo("permission-required");

        assertThat(send("POST", path, null, exporter, body("reason",
                REASON)).statusCode()).isEqualTo(400);
        HttpResponse<String> absent = send("POST", "/api/v1/failures/"
                + UUID.randomUUID() + "/2/export", null, exporter,
                body("reason", REASON, "sealTo", fingerprint));
        assertThat(absent.statusCode()).isEqualTo(409);
        assertThat(absent.headers().firstValue("Content-Type"))
                .hasValueSatisfying(type -> assertThat(type)
                        .startsWith("application/problem+json"));

        send("DELETE", "/api/v1/operator-keys/" + fingerprint, key(), admin,
                body("reason", REASON));
        HttpResponse<String> revoked = send("POST", path, null, exporter,
                body("reason", REASON, "sealTo", fingerprint));
        assertThat(revoked.statusCode()).isEqualTo(403);
        assertThat(ControlServerTest.field(revoked.body(), "title"))
                .isEqualTo("sealing-key-not-yours");
    }

    /**
     * Registers a key.
     *
     * @param token     the caller's token
     * @param principal whose key it is
     * @param pem       the key
     * @param expires   when it expires
     * @return the answer
     * @throws Exception if it can't be sent
     */
    static HttpResponse<String> register(String token, String principal,
            String pem, Instant expires) throws Exception
    {
        return send("POST", "/api/v1/operator-keys", key(), token,
                body("reason", REASON, "principal", principal, "publicKey",
                        pem, "expiresAt", expires.toString()));
    }

    /**
     * A new P-256 key pair.
     *
     * @return it
     * @throws Exception if it can't be made
     */
    static KeyPair ec() throws Exception
    {
        return ControlServerTest.Pki.ec();
    }

    /**
     * A key pair's public key, PEM.
     *
     * @param pair the keys
     * @return the PEM
     */
    static String pem(KeyPair pair)
    {
        return ControlServerTest.Pki.pem("PUBLIC KEY",
                pair.getPublic().getEncoded());
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
     * Sends a request with a body to the API.
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
}
