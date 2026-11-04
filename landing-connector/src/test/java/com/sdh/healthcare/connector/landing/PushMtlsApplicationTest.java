/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The whole landing node as an HTTP push intake with mTLS: only a client
 * certificate from the trusted CA, with a listed subject, may push.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"camel.main.run-controller=false",
            "sdh.landing.source.type=http"})
class PushMtlsApplicationTest extends PushTestSupport
{

    /** The intake's name. */
    static final String INTAKE = "push-mtls";

    /** The listed subject, written as an operator might. */
    static final String LISTED = "CN=claims-producer, O=Example";

    /** A CA the node doesn't trust. */
    static final PushTestPki STRANGER_CA;

    static
    {
        try
        {
            STRANGER_CA = new PushTestPki(ROOT.resolve("stranger"),
                    "stranger-ca");
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
    }

    /**
     * Points the node at the test's database, keys and client CA.
     *
     * @param registry the property registry
     * @throws Exception if the recipient's fingerprint can't be made
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception
    {
        common(registry, INTAKE);
        registry.add("sdh.landing.http.auth", () -> "mtls");
        registry.add("sdh.landing.http.mtls.trusted-cas",
                PKI.caCertificate::toString);
        registry.add("sdh.landing.http.mtls.subjects[0]", () -> LISTED);
    }

    /** The server's port. */
    @LocalServerPort
    int port;

    /**
     * A listed client lands a push, whatever case or spacing its subject
     * is written in; its idempotency key is its own.
     *
     * @throws Exception if the test fails
     */
    @Test
    void landsAPushFromAListedClient() throws Exception
    {
        PushTestPki.Issued client =
                PKI.issue("CN=Claims-Producer,O=example", false);
        byte[] bytes = body(70_000, "listed");
        HttpRequest request = push(port, INTAKE)
                .header("Idempotency-Key", "batch-1")
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes)).build();
        HttpResponse<String> response = client(PKI.clientContext(client))
                .send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(field(response.body(), "sha256")).isEqualTo(sha256(bytes));
        assertThat(client(PKI.clientContext(client)).send(request,
                HttpResponse.BodyHandlers.ofString()).body())
                .isEqualTo(response.body());
        assertThat(receive(INTAKE, 2, Duration.ofSeconds(5))).hasSize(1);
        assertThat(FATAL).isEmpty();
    }

    /**
     * A certificate from the trusted CA with an unlisted subject is
     * refused; one from another CA, or none, fails the handshake.
     *
     * @throws Exception if the test fails
     */
    @Test
    void refusesOtherClients() throws Exception
    {
        HttpRequest request = push(port, INTAKE)
                .POST(HttpRequest.BodyPublishers.ofString("x")).build();
        PushTestPki.Issued unlisted = PKI.issue("CN=stranger,O=Example",
                false);
        assertThat(client(PKI.clientContext(unlisted)).send(request,
                HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(403);

        PushTestPki.Issued foreign = STRANGER_CA.issue(LISTED, false);
        assertThatThrownBy(() -> client(clientTrustingServer(foreign))
                .send(request, HttpResponse.BodyHandlers.ofString()))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> client(PKI.clientContext(null))
                .send(request, HttpResponse.BodyHandlers.ofString()))
                .isInstanceOf(IOException.class);
        assertThat(receive(INTAKE, 1, Duration.ofSeconds(1))).isEmpty();
    }

    /**
     * A context that trusts the node's server but presents a certificate
     * from another CA.
     *
     * @param client the certificate
     * @return the context
     * @throws Exception if it can't be made
     */
    static javax.net.ssl.SSLContext clientTrustingServer(
            PushTestPki.Issued client) throws Exception
    {
        java.security.KeyStore store =
                java.security.KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setKeyEntry("client", client.key(), new char[0],
                new java.security.cert.Certificate[] {client.certificate()});
        javax.net.ssl.KeyManagerFactory keys =
                javax.net.ssl.KeyManagerFactory.getInstance(
                        javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, new char[0]);
        java.security.KeyStore trust =
                java.security.KeyStore.getInstance("PKCS12");
        trust.load(null, null);
        trust.setCertificateEntry("ca", PKI.ca.certificate());
        javax.net.ssl.TrustManagerFactory trusted =
                javax.net.ssl.TrustManagerFactory.getInstance(
                        javax.net.ssl.TrustManagerFactory
                                .getDefaultAlgorithm());
        trusted.init(trust);
        javax.net.ssl.SSLContext context =
                javax.net.ssl.SSLContext.getInstance("TLS");
        context.init(keys.getKeyManagers(), trusted.getTrustManagers(), null);
        return context;
    }
}
