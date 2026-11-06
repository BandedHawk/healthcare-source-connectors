/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.landing.runtime.LandingRuntime;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The whole landing node as an HTTP push intake with OAuth2: a JDBC
 * catalog, embedded Postgres, a SEDA queue, test certificates, and an
 * issuer whose keys the test serves over HTTPS.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"camel.main.run-controller=false",
            "sdh.landing.source.type=http"})
@ExtendWith(OutputCaptureExtension.class)
class PushOAuth2ApplicationTest extends PushTestSupport
{

    /** The intake's name. */
    static final String INTAKE = "push-oauth2";

    /** The audience tokens must name. */
    static final String AUDIENCE = "sdh-landing";

    /** The scope tokens must carry. */
    static final String SCOPE = "sdh.push";

    /** The issuer's signing key. */
    static final ECKey ISSUER_KEY;

    /** Another key, not the issuer's. */
    static final ECKey OTHER_KEY;

    /** Serves the issuer's key set. */
    static final HttpsServer KEYS_SERVER;

    /** The issuer, as in the tokens. */
    static final String ISSUER;

    /** The JVM's TLS defaults, put back after the tests. */
    static final SSLContext DEFAULT_CONTEXT;

    /** The JVM's HTTPS socket factory, put back after the tests. */
    static final SSLSocketFactory DEFAULT_FACTORY;

    static
    {
        try
        {
            ISSUER_KEY = new ECKeyGenerator(Curve.P_256).keyID("issuer-1")
                    .generate();
            OTHER_KEY = new ECKeyGenerator(Curve.P_256).keyID("issuer-1")
                    .generate();
            KEYS_SERVER = HttpsServer.create(
                    new InetSocketAddress("127.0.0.1", 0), 0);
            SSLContext serverContext = SSLContext.getInstance("TLS");
            java.security.KeyStore store =
                    java.security.KeyStore.getInstance("PKCS12");
            store.load(null, null);
            store.setKeyEntry("server", PKI.server.key(), new char[0],
                    new java.security.cert.Certificate[] {
                        PKI.server.certificate(), PKI.ca.certificate()});
            javax.net.ssl.KeyManagerFactory keys =
                    javax.net.ssl.KeyManagerFactory.getInstance(
                            javax.net.ssl.KeyManagerFactory
                                    .getDefaultAlgorithm());
            keys.init(store, new char[0]);
            serverContext.init(keys.getKeyManagers(), null, null);
            KEYS_SERVER.setHttpsConfigurator(
                    new HttpsConfigurator(serverContext));
            byte[] jwks = new JWKSet(ISSUER_KEY.toPublicJWK()).toString()
                    .getBytes(UTF_8);
            KEYS_SERVER.createContext("/jwks", exchange ->
            {
                exchange.getResponseHeaders().add("Content-Type",
                        "application/json");
                exchange.sendResponseHeaders(200, jwks.length);
                exchange.getResponseBody().write(jwks);
                exchange.close();
            });
            KEYS_SERVER.start();
            ISSUER = "https://127.0.0.1:" + KEYS_SERVER.getAddress().getPort()
                    + "/issuer";
            // The node fetches the key set with the JVM's defaults.
            DEFAULT_CONTEXT = SSLContext.getDefault();
            DEFAULT_FACTORY = HttpsURLConnection.getDefaultSSLSocketFactory();
            SSLContext trusting = PKI.clientContext(null);
            SSLContext.setDefault(trusting);
            HttpsURLConnection.setDefaultSSLSocketFactory(
                    trusting.getSocketFactory());
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
    }

    /** Stops the key server and puts the JVM's TLS defaults back. */
    @AfterAll
    static void stopKeyServer()
    {
        KEYS_SERVER.stop(0);
        SSLContext.setDefault(DEFAULT_CONTEXT);
        HttpsURLConnection.setDefaultSSLSocketFactory(DEFAULT_FACTORY);
    }

    /**
     * Points the node at the test's database, keys and issuer.
     *
     * @param registry the property registry
     * @throws Exception if the recipient's fingerprint can't be made
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception
    {
        common(registry, INTAKE);
        registry.add("sdh.landing.http.auth", () -> "oauth2");
        registry.add("sdh.landing.http.oauth2.issuer", () -> ISSUER);
        registry.add("sdh.landing.http.oauth2.jwk-set-uri",
                () -> "https://127.0.0.1:"
                        + KEYS_SERVER.getAddress().getPort() + "/jwks");
        registry.add("sdh.landing.http.oauth2.audience", () -> AUDIENCE);
        registry.add("sdh.landing.http.oauth2.scope", () -> SCOPE);
    }

    /** The server's port. */
    @LocalServerPort
    int port;

    /** The node's services. */
    @Autowired
    LandingRuntime runtime;

    /**
     * A token.
     *
     * @param key      the key it is signed with
     * @param issuer   its issuer
     * @param subject  its subject
     * @param audience its audience
     * @param scope    its scope
     * @param expires  when it expires
     * @return the token
     * @throws Exception if it can't be signed
     */
    static String token(ECKey key, String issuer, String subject,
            String audience, String scope, Instant expires) throws Exception
    {
        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256)
                .keyID(key.getKeyID()).build(),
                new JWTClaimsSet.Builder().issuer(issuer).subject(subject)
                        .audience(audience).claim("scope", scope)
                        .issueTime(Date.from(Instant.now().minusSeconds(5)))
                        .expirationTime(Date.from(expires)).build());
        jwt.sign(new ECDSASigner(key));
        return jwt.serialize();
    }

    /**
     * A good token for a sender.
     *
     * @param subject the sender
     * @return the token
     * @throws Exception if it can't be signed
     */
    static String token(String subject) throws Exception
    {
        return token(ISSUER_KEY, ISSUER, subject, AUDIENCE, SCOPE,
                Instant.now().plusSeconds(300));
    }

    /**
     * An HTTP client trusting the test CA.
     *
     * @return the client
     * @throws Exception if its context can't be made
     */
    static HttpClient client() throws Exception
    {
        return client(PKI.clientContext(null));
    }

    /**
     * A push streamed without a length is landed as it arrives, and the
     * receipt's SHA-256 and size match what was sent; its item.landed
     * message follows. Nothing of the body or the sender is logged.
     *
     * @param output the captured log
     * @throws Exception if the test fails
     */
    @Test
    void landsAStreamedPushAndGivesAReceipt(CapturedOutput output)
            throws Exception
    {
        byte[] bytes = body(1_500_000, "PATIENT-SMITH-0042");
        HttpResponse<String> response = client().send(push(port, INTAKE)
                .header("Authorization", "Bearer " + token("producer-a"))
                .POST(chunked(bytes)).build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(response.headers().firstValue("Content-Type"))
                .hasValueSatisfying(type ->
                        assertThat(type).startsWith("application/json"));
        assertThat(response.headers().firstValue("Authorization")).isEmpty();
        String receipt = response.body();
        assertThat(field(receipt, "sha256")).isEqualTo(sha256(bytes));
        assertThat(field(receipt, "size")).isEqualTo("1500000");
        assertThat(field(receipt, "intake")).isEqualTo(INTAKE);
        List<ConnectorMessage.ItemLanded> landed =
                receive(INTAKE, 1, Duration.ofSeconds(30));
        assertThat(landed).singleElement().satisfies(message ->
        {
            assertThat(message.itemId().toString())
                    .isEqualTo(field(receipt, "itemId"));
            assertThat(message.kind()).isEqualTo("push");
            assertThat(message.size()).isEqualTo(1_500_000L);
        });
        assertThat(output.getAll()).doesNotContain("PATIENT-SMITH")
                .doesNotContain("producer-a");
        assertThat(FATAL).isEmpty();
    }

    /**
     * A push sent again with the same idempotency key gets the first
     * receipt and lands nothing; the same key from another sender is a
     * different push.
     *
     * @param output the captured log
     * @throws Exception if the test fails
     */
    @Test
    void repeatsTheReceiptForAKeyFromTheSameSender(CapturedOutput output)
            throws Exception
    {
        byte[] bytes = body(10_000, "first");
        HttpRequest first = push(port, INTAKE)
                .header("Authorization", "Bearer " + token("producer-a"))
                .header("Idempotency-Key", "order-7781")
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes)).build();
        String receipt = client().send(first,
                HttpResponse.BodyHandlers.ofString()).body();
        assertThat(receive(INTAKE, 1, Duration.ofSeconds(30))).hasSize(1);

        HttpResponse<String> again = client().send(push(port, INTAKE)
                .header("Authorization", "Bearer " + token("producer-a"))
                .header("Idempotency-Key", "order-7781")
                .POST(HttpRequest.BodyPublishers.ofByteArray(
                        body(20, "changed"))).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(again.statusCode()).isEqualTo(202);
        assertThat(again.body()).isEqualTo(receipt);

        HttpResponse<String> other = client().send(push(port, INTAKE)
                .header("Authorization", "Bearer " + token("producer-b"))
                .header("Idempotency-Key", "order-7781")
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(other.statusCode()).isEqualTo(202);
        assertThat(field(other.body(), "itemId"))
                .isNotEqualTo(field(receipt, "itemId"));
        assertThat(receive(INTAKE, 1, Duration.ofSeconds(30))).hasSize(1);
        assertThat(output.getAll()).doesNotContain("order-7781");
    }

    /**
     * Only a signed, current token from the issuer, for the audience,
     * with the scope, may push, and only to the intake's endpoint.
     *
     * @throws Exception if the test fails
     */
    @Test
    void refusesWhatIsntAnAuthorisedPush() throws Exception
    {
        Instant later = Instant.now().plusSeconds(300);
        assertThat(status(null)).isEqualTo(401);
        assertThat(status(token(OTHER_KEY, ISSUER, "a", AUDIENCE, SCOPE,
                later))).isEqualTo(401);
        assertThat(status(token(ISSUER_KEY, "https://elsewhere/", "a",
                AUDIENCE, SCOPE, later))).isEqualTo(401);
        assertThat(status(token(ISSUER_KEY, ISSUER, "a", "someone-else",
                SCOPE, later))).isEqualTo(401);
        assertThat(status(token(ISSUER_KEY, ISSUER, "a", AUDIENCE, SCOPE,
                Instant.now().minusSeconds(120)))).isEqualTo(401);
        assertThat(status(token(ISSUER_KEY, ISSUER, "a", AUDIENCE,
                "sdh.read", later))).isEqualTo(403);

        String good = "Bearer " + token("producer-a");
        HttpResponse<String> get = client().send(push(port, INTAKE)
                .header("Authorization", good).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(get.statusCode()).isEqualTo(403);
        HttpResponse<String> elsewhere = client().send(push(port, "other")
                .header("Authorization", good)
                .POST(HttpRequest.BodyPublishers.ofString("x")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(elsewhere.statusCode()).isEqualTo(403);
        assertThat(receive(INTAKE, 1, Duration.ofSeconds(1))).isEmpty();
    }

    /**
     * Bad bodies are refused and leave nothing behind: one over the size
     * limit, one that breaks off, a form, and a malformed key. The next
     * push lands as usual.
     *
     * @throws Exception if the test fails
     */
    @Test
    void refusesBadBodiesAndCarriesOn() throws Exception
    {
        String auth = "Bearer " + token("producer-a");
        assertThat(client().send(push(port, INTAKE)
                .header("Authorization", auth)
                .POST(chunked(body(3_000_000, "big"))).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(413);
        sendCutOff(PKI.clientContext(null), port,
                "/intakes/" + INTAKE + "/items",
                "Authorization: " + auth + "\r\n");
        assertThat(client().send(push(port, INTAKE)
                .header("Authorization", auth)
                .setHeader("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("a=b")).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(415);
        assertThat(client().send(push(port, INTAKE)
                .header("Authorization", auth)
                .header("Idempotency-Key", "has space")
                .POST(HttpRequest.BodyPublishers.ofString("a")).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(400);
        assertThat(receive(INTAKE, 1, Duration.ofSeconds(2))).isEmpty();

        byte[] bytes = body(5_000, "after");
        HttpResponse<String> response = client().send(push(port, INTAKE)
                .header("Authorization", auth)
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(field(response.body(), "sha256")).isEqualTo(sha256(bytes));
        assertThat(receive(INTAKE, 1, Duration.ofSeconds(30))).hasSize(1);
        assertThat(FATAL).isEmpty();
    }

    /**
     * While another push holds the intake past the wait, a push is turned
     * away with 503 and Retry-After; once it is free, the push lands.
     *
     * @throws Exception if the test fails
     */
    @Test
    void turnsAPushAwayWhileTheIntakeIsBusy() throws Exception
    {
        String auth = "Bearer " + token("producer-a");
        ReentrantLock writer = runtime.intake().writerLock();
        writer.lock();
        HttpResponse<String> busy;
        try
        {
            busy = client().send(push(port, INTAKE)
                    .header("Authorization", auth)
                    .POST(HttpRequest.BodyPublishers.ofString("x")).build(),
                    HttpResponse.BodyHandlers.ofString());
        }
        finally
        {
            writer.unlock();
        }
        assertThat(busy.statusCode()).isEqualTo(503);
        assertThat(busy.headers().firstValue("Retry-After")).contains("7");
        assertThat(client().send(push(port, INTAKE)
                .header("Authorization", auth)
                .POST(HttpRequest.BodyPublishers.ofString("x")).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(202);
        assertThat(receive(INTAKE, 1, Duration.ofSeconds(30))).hasSize(1);
    }

    /**
     * The status a push gets with a token.
     *
     * @param token the token; null for none
     * @return the HTTP status
     * @throws Exception if the request fails
     */
    int status(String token) throws Exception
    {
        HttpRequest.Builder request = push(port, INTAKE)
                .POST(HttpRequest.BodyPublishers.ofString("x"));
        if (token != null)
        {
            request.header("Authorization", "Bearer " + token);
        }
        return client().send(request.build(),
                HttpResponse.BodyHandlers.ofString()).statusCode();
    }
}
