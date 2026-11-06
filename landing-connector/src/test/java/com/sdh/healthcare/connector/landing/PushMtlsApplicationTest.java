/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.interfaces.ECPublicKey;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
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

    /** The producer's key pair, for its results. */
    static final KeyPair PRODUCER;

    /** The producer's public key file. */
    static final Path PRODUCER_PEM;

    static
    {
        try
        {
            STRANGER_CA = new PushTestPki(ROOT.resolve("stranger"),
                    "stranger-ca");
            PRODUCER = LandingTestKeys.ec();
            PRODUCER_PEM = Files.writeString(ROOT.resolve("producer.pem"),
                    LandingTestKeys.pem("PUBLIC KEY",
                            PRODUCER.getPublic().getEncoded()));
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
        registry.add("sdh.results.enabled", () -> "true");
        registry.add("sdh.results.queue.type", () -> "camel");
        registry.add("sdh.results.queue.uri",
                () -> "seda:" + INTAKE + "-results?size=100");
        registry.add("sdh.results.recipient.kid", () -> "producer-2026");
        registry.add("sdh.results.recipient.pem", PRODUCER_PEM::toString);
        String pin = KeyMaterial.fingerprint(PRODUCER.getPublic());
        registry.add("sdh.results.recipient.sha256", () -> pin);
    }

    /**
     * Waits for an item's receipt on the producer's results queue, passing
     * over other tests' receipts.
     *
     * @param itemId the item
     * @return the receipt, opened with the producer's key
     * @throws Exception if none comes, or one can't be opened
     */
    ConnectorMessage.ItemReceived receipt(String itemId) throws Exception
    {
        MessageOpener opener = new MessageOpener(
                Map.of("producer-2026", PRODUCER.getPrivate()),
                Map.of("landing-2026",
                        (ECPublicKey) KEYS.signing.getPublic()));
        while (true)
        {
            String body = camel.createConsumerTemplate().receiveBody(
                    "seda:" + INTAKE + "-results", 15_000, String.class);
            assertThat(body).as("a receipt").isNotNull();
            ConnectorMessage.ItemReceived receipt =
                    (ConnectorMessage.ItemReceived) opener.open(
                            body.getBytes(StandardCharsets.UTF_8));
            if (receipt.itemId().toString().equals(itemId))
            {
                return receipt;
            }
        }
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
     * With results on, the producer gets each push's receipt on its own
     * queue, sealed to its key, with its correlation ID; a push may name
     * an earlier item it replaces, which is linked, never replaced, and
     * one naming an item this intake didn't land is refused.
     *
     * @throws Exception if the test fails
     */
    @Test
    void sendsReceiptsToTheProducer() throws Exception
    {
        PushTestPki.Issued client = PKI.issue(LISTED, false);
        HttpResponse<String> first = client(PKI.clientContext(client)).send(
                push(port, INTAKE).header("X-Correlation-Id", "order-1")
                        .POST(HttpRequest.BodyPublishers.ofString("one"))
                        .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(first.statusCode()).isEqualTo(202);
        ConnectorMessage.ItemReceived receipt =
                receipt(field(first.body(), "itemId"));
        assertThat(receipt.producerRef()).isEqualTo("order-1");
        assertThat(receipt.intake()).isEqualTo(INTAKE);
        assertThat(receipt.sha256()).isEqualTo(field(first.body(), "sha256"));

        HttpResponse<String> unknown = client(PKI.clientContext(client))
                .send(push(port, INTAKE)
                        .header("Replaces", UUID.randomUUID().toString())
                        .POST(HttpRequest.BodyPublishers.ofString("two"))
                        .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(unknown.statusCode()).isEqualTo(400);
        assertThat(unknown.body()).contains("unknown-replaces");
        assertThat(client(PKI.clientContext(client)).send(push(port, INTAKE)
                .header("Replaces", "not-an-id")
                .POST(HttpRequest.BodyPublishers.ofString("two")).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(400);

        HttpResponse<String> second = client(PKI.clientContext(client)).send(
                push(port, INTAKE)
                        .header("Replaces", receipt.itemId().toString())
                        .POST(HttpRequest.BodyPublishers.ofString("two"))
                        .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(second.statusCode()).isEqualTo(202);
        assertThat(receipt(field(second.body(), "itemId")).replaces())
                .isEqualTo(receipt.itemId());
        assertThat(receive(INTAKE, 3, Duration.ofSeconds(5)))
                .extracting(ConnectorMessage.ItemLanded::replaces)
                .endsWith(null, receipt.itemId());
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
