/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.StageStatusCommand;
import com.sdh.healthcare.connector.stage.admin.StuckCommand;
import com.sdh.healthcare.connector.stage.control.ControlServer;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The control API (CONTROL-API-DESIGN.md, build step C2), served by a test
 * stage over TLS: a request without a valid token is refused and audited;
 * one without its operation's permission is refused and audited; one with
 * it is answered with the operation's result and audited with the token's
 * principal, the permission and the request's ID. The probe port serves
 * the probes alone, and the API's own events reach the stage's journal.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ControlServerTest
{

    /** The audience tokens must name. */
    static final String AUDIENCE = "sdh-control";

    /** The issuer, as in the tokens. */
    static final String ISSUER = "https://issuer.test/realm";

    /** The issuer's signing key. */
    static final ECKey ISSUER_KEY;

    /** Another key, not the issuer's. */
    static final ECKey OTHER_KEY;

    static
    {
        try
        {
            ISSUER_KEY = new ECKeyGenerator(Curve.P_256).keyID("issuer-1")
                    .generate();
            OTHER_KEY = new ECKeyGenerator(Curve.P_256).keyID("issuer-1")
                    .generate();
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
    }

    /** Captures each test's audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** Serves the issuer's key set. */
    static HttpServer keys;

    /** The test CA and the server's certificate. */
    static Pki pki;

    /** The registry's schema. */
    static String schema;

    /** An item that waits for records. */
    static final UUID WAITING = UUID.randomUUID();

    /** The control API, token mode. */
    static ConfigurableApplicationContext server;

    /** The API's port. */
    static int port;

    /** The probe port. */
    static int probePort;

    /** Creates the tests; JUnit runs each test on a new instance. */
    ControlServerTest()
    {
    }

    /**
     * Starts the key server and the control API, on a registry with one
     * item waiting.
     *
     * @param directory where the certificates go
     * @throws Exception if they can't be started
     */
    @BeforeAll
    static void start(@TempDir Path directory) throws Exception
    {
        keys = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        byte[] jwks = new JWKSet(ISSUER_KEY.toPublicJWK()).toString()
                .getBytes(UTF_8);
        keys.createContext("/jwks", exchange ->
        {
            exchange.getResponseHeaders().add("Content-Type",
                    "application/json");
            exchange.sendResponseHeaders(200, jwks.length);
            exchange.getResponseBody().write(jwks);
            exchange.close();
        });
        keys.start();
        pki = new Pki(directory);
        schema = StageTestSupport.freshSchema();
        StageRegistry registry = StageTestSupport.registry(schema, "pod-a");
        registry.expect(WAITING, 3);
        port = freePort();
        probePort = freePort();
        List<String> args = settings(port, probePort);
        args.add("--sdh.control.auth.jwt.jwk-set-uri=http://127.0.0.1:"
                + keys.getAddress().getPort() + "/jwks");
        args.add("--sdh.control.auth.jwt.issuer-uri=" + ISSUER);
        args.add("--sdh.control.auth.jwt.audience=" + AUDIENCE);
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
     * The settings every server here is started with: the test's database
     * and registry, its certificates, and its permissions.
     *
     * @param port      the API's port
     * @param probePort the probe port
     * @return the arguments
     */
    static List<String> settings(int port, int probePort)
    {
        List<String> args = new ArrayList<>();
        args.add("--sdh.lease.jdbc.url="
                + StageTestSupport.POSTGRES.getJdbcUrl("postgres", "postgres"));
        args.add("--sdh.lease.jdbc.username=postgres");
        args.add("--sdh.lease.jdbc.password=postgres");
        args.add("--sdh.lease.jdbc.table=" + schema + "_lease");
        args.add("--sdh.registry-schema=" + schema);
        args.add("--sdh.control.port=" + port);
        args.add("--sdh.control.probe-port=" + probePort);
        args.add("--sdh.control.tls.certificate=" + pki.serverCertificate);
        args.add("--sdh.control.tls.key=" + pki.serverKey);
        args.add("--sdh.control.permissions[pipeline.view]=viewers,auditors");
        args.add("--sdh.control.permissions[audit.read]=auditors");
        return args;
    }

    /**
     * The test stage's commands: status and stuck, with the audit
     * journal's.
     *
     * @return them
     */
    static AdminCli<StageAdminTest.TestSettings, StageAdminTest.Context>
            cli()
    {
        return new AdminCli<>(new AdminCli.Stage<>("test-stage.jar",
                "test-stage", StageAdminTest.Configuration.class,
                StageAdminTest.TestSettings.class,
                StageAdminTest.TestSettings::telemetry,
                StageAdminTest.Context::new, StageAdminTest::audit),
                List.of(new StageStatusCommand<StageAdminTest.Context>(),
                        new StuckCommand<StageAdminTest.Context>()));
    }

    /**
     * A request with no token, a token for another audience, an expired
     * one, one issued for longer than an hour, or one signed by another
     * key is refused as unauthenticated, names no detail, and is audited
     * by the failure's class.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesARequestWithoutAValidToken() throws Exception
    {
        Instant now = Instant.now();
        List<String> bad = List.of(
                token(ISSUER_KEY, AUDIENCE, "alice", List.of("viewers"),
                        now.minusSeconds(600), now.minusSeconds(300)),
                token(ISSUER_KEY, "someone-else", "alice",
                        List.of("viewers"), now, now.plusSeconds(300)),
                token(ISSUER_KEY, AUDIENCE, "alice", List.of("viewers"),
                        now, now.plus(Duration.ofHours(2))),
                token(OTHER_KEY, AUDIENCE, "alice", List.of("viewers"), now,
                        now.plusSeconds(300)));
        HttpResponse<String> none = get("/api/v1/stuck", null);
        assertThat(none.statusCode()).isEqualTo(401);
        assertThat(none.headers().firstValue("WWW-Authenticate"))
                .hasValue("Bearer");
        assertThat(field(none.body(), "title")).isEqualTo("unauthenticated");
        for (String token : bad)
        {
            HttpResponse<String> refused = get("/api/v1/stuck", token);
            assertThat(refused.statusCode()).as(refused.body())
                    .isEqualTo(401);
            assertThat(refused.body()).doesNotContain("alice")
                    .doesNotContain("audience").doesNotContain("expired");
        }
        assertThat(audit.names()).hasSize(5)
                .containsOnly("control.unauthenticated");
        assertThat(audit.attributes("control.unauthenticated"))
                .containsEntry("sdh.auth", "jwt")
                .containsEntry("sdh.decision", "unauthenticated")
                .containsEntry("sdh.result", "refused")
                .containsKey("sdh.reason");
    }

    /**
     * A query with its permission is answered with the operation's own
     * answer, and audited with the token's principal, its permission, the
     * decision and the request's ID, which the answer carries.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void answersAQueryWithItsPermissionAndAuditsIt() throws Exception
    {
        HttpResponse<String> stuck = get("/api/v1/stuck?idle=0",
                token("alice", "viewers"));

        assertThat(stuck.statusCode()).as(stuck.body()).isEqualTo(200);
        assertThat(stuck.headers().firstValue("Content-Type"))
                .hasValueSatisfying(type ->
                        assertThat(type).startsWith("application/json"));
        assertThat(stuck.headers().firstValue("Cache-Control"))
                .hasValue("no-store");
        Map<String, Object> answer = JSONObjectUtils.parse(stuck.body());
        assertThat(answer).containsEntry("idle", "PT0S");
        assertThat(answer.get("items").toString())
                .contains(WAITING.toString());
        String requestId = stuck.headers().firstValue("X-Request-Id")
                .orElseThrow();
        assertThat(audit.attributes("control.stuck"))
                .containsEntry("sdh.operator", "alice")
                .containsEntry("sdh.auth", "jwt")
                .containsEntry("sdh.permission", "pipeline.view")
                .containsEntry("sdh.decision", "allowed")
                .containsEntry("sdh.request_id", requestId)
                .containsEntry("sdh.result", "ok");

        HttpResponse<String> status = get("/api/v1/items/" + WAITING
                + "/status", token("alice", "viewers"));
        assertThat(status.statusCode()).as(status.body()).isEqualTo(200);
        assertThat(status.body()).contains(WAITING.toString());
    }

    /**
     * Health, settings, compliance and the support bundle answer with
     * {@code pipeline.view}; the bundle holds the API's recent operations
     * and no secret.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void answersTheSupportQueries() throws Exception
    {
        for (String path : List.of("/health", "/settings", "/compliance"))
        {
            HttpResponse<String> answer = get("/api/v1" + path,
                    token("alice", "viewers"));
            assertThat(answer.statusCode()).as(path + answer.body())
                    .isEqualTo(200);
            assertThat(answer.body()).doesNotContain("password\":\"postgres");
        }
        HttpResponse<String> bundle = get("/api/v1/support-bundle",
                token("alice", "viewers"));
        assertThat(bundle.statusCode()).as(bundle.body()).isEqualTo(200);
        Map<String, Object> answer = JSONObjectUtils.parse(bundle.body());
        assertThat(answer).containsEntry("generatedBy", "alice")
                .containsKeys("health", "settings", "compliance", "control",
                        "status");
        assertThat(answer.get("operations")).isInstanceOf(List.class);
        assertThat(bundle.body()).doesNotContain("password\":\"postgres");
        assertThat(audit.attributes("control.support_bundle"))
                .containsEntry("sdh.permission", "pipeline.view")
                .containsEntry("sdh.result", "ok");
        assertThat(get("/api/v1/support-bundle", token("bob", "nobody"))
                .statusCode()).isEqualTo(403);
    }

    /**
     * A query without its permission is refused and audited as denied;
     * so is a caller with no roles. The audit journal's checks need
     * {@code audit.read}, and with it find the API's own chain.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesAQueryWithoutItsPermission() throws Exception
    {
        HttpResponse<String> denied = get("/api/v1/audit/verify",
                token("alice", "viewers"));
        assertThat(denied.statusCode()).isEqualTo(403);
        assertThat(field(denied.body(), "title"))
                .isEqualTo("permission-required");
        assertThat(field(denied.body(), "detail")).contains("audit.read");
        assertThat(audit.attributes("control.audit_verify"))
                .containsEntry("sdh.operator", "alice")
                .containsEntry("sdh.permission", "audit.read")
                .containsEntry("sdh.decision", "denied")
                .containsEntry("sdh.result", "refused");

        HttpResponse<String> nobody = get("/api/v1/stuck",
                token("mallory"));
        assertThat(nobody.statusCode()).isEqualTo(403);

        HttpResponse<String> verified = get("/api/v1/audit/verify",
                token("carol", "auditors"));
        assertThat(verified.statusCode()).as(verified.body()).isEqualTo(200);
        assertThat(verified.body()).contains("test/test-stage/control");
    }

    /**
     * A parameter the route doesn't take, a malformed value, an operation
     * the stage doesn't have, or a method a query's path doesn't take is
     * refused, and nothing runs.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesWhatTheRouteDoesNotTake() throws Exception
    {
        String token = token("alice", "viewers");
        HttpResponse<String> unknown = get("/api/v1/stuck?accept=true",
                token);
        assertThat(unknown.statusCode()).isEqualTo(400);
        assertThat(field(unknown.body(), "title")).isEqualTo("bad-request");

        HttpResponse<String> malformed = get("/api/v1/items/not-a-uuid"
                + "/status", token);
        assertThat(malformed.statusCode()).isEqualTo(400);

        HttpResponse<String> absent = get("/api/v1/version", token);
        assertThat(absent.statusCode()).isEqualTo(404);
        assertThat(field(absent.body(), "title"))
                .isEqualTo("not-on-this-stage");

        HttpResponse<String> post = client().send(HttpRequest.newBuilder(
                URI.create("https://localhost:" + port + "/api/v1/stuck"))
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(post.statusCode()).isEqualTo(404);
    }

    /**
     * The probe port serves liveness and readiness without a token, and
     * nothing else; the API's port doesn't serve the probes. The OpenAPI
     * description is served to any authenticated caller.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void servesTheProbesOnTheProbePortOnly() throws Exception
    {
        HttpClient plain = HttpClient.newHttpClient();
        for (String probe : List.of("/livez", "/readyz"))
        {
            assertThat(plain.send(HttpRequest.newBuilder(URI.create(
                    "http://127.0.0.1:" + probePort + probe)).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode())
                    .isEqualTo(200);
            assertThat(get(probe, null).statusCode()).isEqualTo(401);
        }
        assertThat(plain.send(HttpRequest.newBuilder(URI.create(
                "http://127.0.0.1:" + probePort + "/api/v1/stuck"))
                .header("Authorization", "Bearer " + token("alice",
                        "viewers")).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode())
                .isIn(401, 403);

        HttpResponse<String> openapi = get("/api/v1/openapi.yaml",
                token("mallory"));
        assertThat(openapi.statusCode()).isEqualTo(200);
        assertThat(openapi.body()).startsWith("openapi: 3.1.0");
    }

    /**
     * With client certificates, a listed subject is let in with its
     * roles; an unlisted one, even from the trusted CA, is refused.
     *
     * @param directory where the client's certificates go
     * @throws Exception if the test fails to run
     */
    @Test
    void letsInAListedClientCertificate(@TempDir Path directory)
            throws Exception
    {
        int mtlsPort = freePort();
        List<String> args = settings(mtlsPort, freePort());
        args.add("--sdh.control.auth.mode=mtls");
        args.add("--sdh.control.auth.mtls.trusted-ca=" + pki.caCertificate);
        // A subject's name holds commas and equals signs, so its roles are
        // given in YAML, its name in brackets, as a deployment gives them.
        Path roles = Files.writeString(directory.resolve("roles.yml"), """
                sdh.control.auth.mtls.roles:
                  "[CN=ops-tool,O=SDH Test]": [viewers]
                """);
        args.add("--spring.config.additional-location=file:" + roles);
        try (ConfigurableApplicationContext mtls =
                ControlServer.start(cli(), args))
        {
            HttpClient listed = client(pki.clientContext(
                    pki.issue("CN=ops-tool,O=SDH Test", false)));
            HttpResponse<String> answered = listed.send(HttpRequest
                    .newBuilder(URI.create("https://localhost:" + mtlsPort
                            + "/api/v1/stuck")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(answered.statusCode()).as(answered.body())
                    .isEqualTo(200);
            assertThat(audit.attributes("control.stuck"))
                    .containsEntry("sdh.operator", "cn=ops-tool,o=sdh test")
                    .containsEntry("sdh.auth", "mtls");

            HttpClient unlisted = client(pki.clientContext(
                    pki.issue("CN=intruder,O=SDH Test", false)));
            assertThat(unlisted.send(HttpRequest.newBuilder(URI.create(
                    "https://localhost:" + mtlsPort + "/api/v1/stuck"))
                    .build(), HttpResponse.BodyHandlers.ofString())
                    .statusCode()).isEqualTo(401);
        }
    }

    /**
     * The API doesn't start without the settings to authenticate its
     * callers, or with a permission it doesn't know.
     */
    @Test
    void doesNotStartWithoutAWayToAuthenticate()
    {
        List<String> noAudience = settings(freePort(), freePort());
        noAudience.add("--sdh.control.auth.jwt.issuer-uri=" + ISSUER);
        assertThatThrownBy(() -> ControlServer.start(cli(), noAudience)
                .close())
                .rootCause().hasMessageContaining(
                        "sdh.control.auth.jwt.audience is required");

        List<String> unknown = settings(freePort(), freePort());
        unknown.add("--sdh.control.auth.jwt.issuer-uri=" + ISSUER);
        unknown.add("--sdh.control.auth.jwt.audience=" + AUDIENCE);
        unknown.add("--sdh.control.permissions[pipeline.everything]=all");
        assertThatThrownBy(() -> ControlServer.start(cli(), unknown)
                .close())
                .rootCause().hasMessageContaining("pipeline.everything");
    }

    /**
     * Sends a {@code GET} to the API.
     *
     * @param path  the path
     * @param token the bearer token; null for none
     * @return the response
     * @throws Exception if it can't be sent
     */
    static HttpResponse<String> get(String path, String token)
            throws Exception
    {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                URI.create("https://localhost:" + port + path));
        if (token != null)
        {
            request.header("Authorization", "Bearer " + token);
        }
        return client().send(request.build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /**
     * A good token for five minutes.
     *
     * @param subject the caller
     * @param roles   its roles
     * @return the token
     * @throws Exception if it can't be signed
     */
    static String token(String subject, String... roles) throws Exception
    {
        Instant now = Instant.now();
        return token(ISSUER_KEY, AUDIENCE, subject, List.of(roles), now,
                now.plusSeconds(300));
    }

    /**
     * A token.
     *
     * @param key      the key it is signed with
     * @param audience its audience
     * @param subject  its subject
     * @param roles    its groups
     * @param issued   when it was issued
     * @param expires  when it expires
     * @return the token
     * @throws Exception if it can't be signed
     */
    static String token(ECKey key, String audience, String subject,
            List<String> roles, Instant issued, Instant expires)
            throws Exception
    {
        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256)
                .keyID(key.getKeyID()).build(),
                new JWTClaimsSet.Builder().issuer(ISSUER).subject(subject)
                        .audience(audience).claim("groups", roles)
                        .issueTime(Date.from(issued))
                        .expirationTime(Date.from(expires)).build());
        jwt.sign(new ECDSASigner(key));
        return jwt.serialize();
    }

    /**
     * A field of a JSON answer, as text.
     *
     * @param json  the answer
     * @param field the field
     * @return its value
     * @throws Exception if the answer isn't JSON
     */
    static String field(String json, String field) throws Exception
    {
        return String.valueOf(JSONObjectUtils.parse(json).get(field));
    }

    /**
     * A client trusting the test CA.
     *
     * @return the client
     * @throws Exception if its context can't be made
     */
    static HttpClient client() throws Exception
    {
        return client(pki.clientContext(null));
    }

    /**
     * A client with a TLS context.
     *
     * @param context the context
     * @return the client
     */
    static HttpClient client(SSLContext context)
    {
        return HttpClient.newBuilder().sslContext(context)
                .connectTimeout(Duration.ofSeconds(10)).build();
    }

    /**
     * A port nothing listens on.
     *
     * @return the port
     */
    static int freePort()
    {
        try (ServerSocket socket = new ServerSocket(0))
        {
            return socket.getLocalPort();
        }
        catch (IOException failure)
        {
            throw new IllegalStateException(failure);
        }
    }

    /**
     * A CA, the server's certificate for {@code localhost}, and client
     * certificates, written as PEM files where the server needs them.
     */
    static final class Pki
    {

        /** The CA's name. */
        private final X500Name caName;

        /** The CA's certificate. */
        private final X509Certificate ca;

        /** The CA's key. */
        private final PrivateKey caKey;

        /** The server's certificate chain, a PEM file. */
        final Path serverCertificate;

        /** The server's key, a PEM file. */
        final Path serverKey;

        /** The CA's certificate, a PEM file. */
        final Path caCertificate;

        /**
         * Makes the CA and the server's certificate, and writes them.
         *
         * @param directory where the files go
         * @throws Exception if they can't be made or written
         */
        Pki(Path directory) throws Exception
        {
            caName = name("CN=Control Test CA,O=SDH Test");
            KeyPair caKeys = ec();
            caKey = caKeys.getPrivate();
            JcaX509v3CertificateBuilder builder = builder(caName, caKeys);
            builder.addExtension(Extension.basicConstraints, true,
                    new BasicConstraints(true));
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(
                    KeyUsage.keyCertSign | KeyUsage.cRLSign));
            ca = sign(builder);
            Issued server = issue("CN=localhost,O=SDH Test", true);
            serverCertificate = Files.writeString(
                    directory.resolve("server.crt"),
                    pem("CERTIFICATE", server.certificate().getEncoded())
                            + pem("CERTIFICATE", ca.getEncoded()));
            serverKey = Files.writeString(directory.resolve("server.key"),
                    pem("PRIVATE KEY", server.key().getEncoded()));
            caCertificate = Files.writeString(directory.resolve("ca.crt"),
                    pem("CERTIFICATE", ca.getEncoded()));
        }

        /**
         * A certificate and its key.
         *
         * @param certificate the certificate
         * @param key         its private key
         */
        record Issued(X509Certificate certificate, PrivateKey key)
        {
        }

        /**
         * Issues a certificate from the CA.
         *
         * @param subject its subject
         * @param server  whether it is the server's, for localhost
         * @return the certificate and its key
         * @throws Exception if it can't be made
         */
        Issued issue(String subject, boolean server) throws Exception
        {
            KeyPair keys = ec();
            JcaX509v3CertificateBuilder builder = builder(name(subject),
                    keys);
            if (server)
            {
                builder.addExtension(Extension.subjectAlternativeName, false,
                        new GeneralNames(new GeneralName[] {
                            new GeneralName(GeneralName.dNSName,
                                    "localhost"),
                            new GeneralName(GeneralName.iPAddress,
                                    "127.0.0.1")}));
            }
            return new Issued(sign(builder), keys.getPrivate());
        }

        /**
         * A client's TLS context that trusts the CA's servers and, if
         * given, presents a client certificate.
         *
         * @param client the client certificate; null for none
         * @return the context
         * @throws Exception if it can't be made
         */
        SSLContext clientContext(Issued client) throws Exception
        {
            KeyStore trust = KeyStore.getInstance("PKCS12");
            trust.load(null, null);
            trust.setCertificateEntry("ca", ca);
            TrustManagerFactory trusted = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            trusted.init(trust);
            KeyManagerFactory keys = null;
            if (client != null)
            {
                KeyStore store = KeyStore.getInstance("PKCS12");
                store.load(null, null);
                store.setKeyEntry("client", client.key(), new char[0],
                        new Certificate[] {client.certificate(), ca});
                keys = KeyManagerFactory.getInstance(
                        KeyManagerFactory.getDefaultAlgorithm());
                keys.init(store, new char[0]);
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys == null ? null : keys.getKeyManagers(),
                    trusted.getTrustManagers(), new SecureRandom());
            return context;
        }

        /**
         * A certificate builder from the CA, valid from a minute ago for a
         * day.
         *
         * @param subject its subject
         * @param keys    its keys
         * @return the builder
         */
        private JcaX509v3CertificateBuilder builder(X500Name subject,
                KeyPair keys)
        {
            Instant now = Instant.now();
            return new JcaX509v3CertificateBuilder(caName,
                    new BigInteger(64, new SecureRandom()),
                    Date.from(now.minusSeconds(60)),
                    Date.from(now.plus(Duration.ofDays(1))), subject,
                    keys.getPublic());
        }

        /**
         * Signs a certificate with the CA's key.
         *
         * @param builder the certificate
         * @return it, signed
         * @throws Exception if it can't be signed
         */
        private X509Certificate sign(JcaX509v3CertificateBuilder builder)
                throws Exception
        {
            return new JcaX509CertificateConverter().getCertificate(
                    builder.build(new JcaContentSignerBuilder(
                            "SHA256withECDSA").build(caKey)));
        }

        /**
         * A name as RFC 2253 reads it, as the JDK does.
         *
         * @param name the name
         * @return the name
         */
        static X500Name name(String name)
        {
            return X500Name.getInstance(
                    new X500Principal(name).getEncoded());
        }

        /**
         * A new P-256 key pair.
         *
         * @return the keys
         * @throws Exception if they can't be made
         */
        static KeyPair ec() throws Exception
        {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
            generator.initialize(new ECGenParameterSpec("secp256r1"));
            return generator.generateKeyPair();
        }

        /**
         * PEM text.
         *
         * @param type the PEM type
         * @param der  the encoded object
         * @return the text
         */
        static String pem(String type, byte[] der)
        {
            return "-----BEGIN " + type + "-----\n"
                    + Base64.getMimeEncoder(64, "\n".getBytes(UTF_8))
                            .encodeToString(der)
                    + "\n-----END " + type + "-----\n";
        }
    }
}
