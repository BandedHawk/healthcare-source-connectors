/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.interfaces.ECPublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;

/**
 * What the HTTP push application tests share: embedded Postgres, a JDBC
 * catalog, key files, test certificates, a SEDA queue, and helpers to push
 * and to read the {@code item.landed} messages.
 * <p>
 * Each test class's node is closed when the class ends, while Postgres is
 * still running. Left to the end of the run, it would race Postgres's own
 * shutdown hook, and wait out the pool's timeout to release its lease.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
abstract class PushTestSupport
{

    /** The test files. */
    static final Path ROOT;

    /** The key files. */
    static final LandingTestKeys KEYS;

    /** The lease database and catalog. */
    static final EmbeddedPostgres POSTGRES;

    /** The CA the server's and accepted clients' certificates are from. */
    static final PushTestPki PKI;

    static
    {
        try
        {
            ROOT = Files.createTempDirectory("sdh-landing-push");
            KEYS = new LandingTestKeys(
                    Files.createDirectories(ROOT.resolve("keys")));
            POSTGRES = EmbeddedPostgres.start();
            PKI = new PushTestPki(ROOT.resolve("pki"), "push-test-ca");
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
    }

    /** The reasons given to the exit hook. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /** Puts a recording exit hook in place of the real one. */
    @TestConfiguration
    static class Overrides
    {

        /**
         * The recording hook.
         *
         * @return the hook
         */
        @Bean
        @Primary
        FatalExit recordingFatalExit()
        {
            return FATAL::add;
        }
    }

    /** The application's Camel context. */
    @Autowired
    CamelContext camel;

    /**
     * Points the node at the test's database, keys and certificates, as an
     * HTTP intake.
     *
     * @param registry the property registry
     * @param intake   the intake's name, unique to the test class
     * @throws Exception if the recipient's fingerprint can't be made
     */
    static void common(DynamicPropertyRegistry registry, String intake)
            throws Exception
    {
        String jdbc = POSTGRES.getJdbcUrl("postgres", "postgres");
        registry.add("sdh.landing.intake", () -> intake);
        registry.add("sdh.landing.http.port", () -> "0");
        registry.add("sdh.landing.http.tls.certificate",
                PKI.serverCertificate::toString);
        registry.add("sdh.landing.http.tls.private-key",
                PKI.serverKey::toString);
        registry.add("sdh.landing.http.max-item-bytes", () -> "2MB");
        registry.add("sdh.landing.http.max-wait", () -> "1s");
        registry.add("sdh.landing.http.retry-after", () -> "7s");
        registry.add("sdh.storage.catalog.type", () -> "jdbc");
        registry.add("sdh.storage.catalog.uri", () -> jdbc);
        registry.add("sdh.storage.catalog.warehouse",
                () -> "memory://wh-" + UUID.randomUUID());
        registry.add("sdh.storage.catalog.io-impl",
                () -> "org.apache.iceberg.inmemory.InMemoryFileIO");
        registry.add("sdh.storage.catalog[jdbc.user]", () -> "postgres");
        registry.add("sdh.storage.catalog[jdbc.password]", () -> "postgres");
        registry.add("sdh.storage.catalog[jdbc.schema-version]", () -> "V1");
        registry.add("sdh.lease.jdbc.url", () -> jdbc);
        registry.add("sdh.lease.jdbc.username", () -> "postgres");
        registry.add("sdh.lease.jdbc.password", () -> "postgres");
        registry.add("sdh.crypto.keystore.path", KEYS.keystore::toString);
        registry.add("sdh.crypto.keystore.password",
                () -> LandingTestKeys.KEYSTORE_PASSWORD);
        registry.add("sdh.crypto.keys.landed-items-kek",
                () -> LandingTestKeys.LANDED_ITEMS);
        registry.add("sdh.crypto.keys.signing.kid", () -> "landing-2026");
        registry.add("sdh.crypto.keys.signing.pem",
                KEYS.signingPem::toString);
        registry.add("sdh.crypto.keys.signing.passphrase",
                () -> LandingTestKeys.SIGNING_PASSPHRASE);
        registry.add("sdh.crypto.keys.recipient.kid",
                () -> "downstream-2026");
        registry.add("sdh.crypto.keys.recipient.pem",
                KEYS.recipientPem::toString);
        String pin = KeyMaterial.fingerprint(KEYS.recipient.getPublic());
        registry.add("sdh.crypto.keys.recipient.sha256", () -> pin);
        registry.add("sdh.queue.type", () -> "camel");
        registry.add("sdh.queue.uri",
                () -> "seda:" + intake + "-out?size=1000");
        registry.add("sdh.retry.max-attempts", () -> "2");
        registry.add("sdh.retry.initial-backoff", () -> "10ms");
    }

    /**
     * An HTTP client with a TLS context.
     *
     * @param context the context
     * @return the client
     */
    static HttpClient client(SSLContext context)
    {
        return HttpClient.newBuilder().sslContext(context)
                .connectTimeout(Duration.ofSeconds(5)).build();
    }

    /**
     * A push request to the intake.
     *
     * @param port   the server's port
     * @param intake the intake's name
     * @return the request, to finish
     */
    static HttpRequest.Builder push(int port, String intake)
    {
        return HttpRequest.newBuilder(URI.create("https://127.0.0.1:" + port
                + "/intakes/" + intake + "/items"))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/octet-stream");
    }

    /**
     * A body sent without a length, in chunks, so it can only reach the
     * node as a stream.
     *
     * @param bytes the body
     * @return the publisher
     */
    static HttpRequest.BodyPublisher chunked(byte[] bytes)
    {
        return HttpRequest.BodyPublishers.ofInputStream(
                () -> new java.io.ByteArrayInputStream(bytes));
    }

    /**
     * Sends a body that stops short of its stated length, then closes the
     * connection.
     *
     * @param context the client's TLS context
     * @param port    the server's port
     * @param path    the request path
     * @param headers further header lines, each ending in CRLF
     * @throws Exception if the connection fails
     */
    static void sendCutOff(SSLContext context, int port, String path,
            String headers) throws Exception
    {
        try (SSLSocket socket = (SSLSocket) context.getSocketFactory()
                .createSocket("127.0.0.1", port))
        {
            socket.startHandshake();
            OutputStream output = socket.getOutputStream();
            output.write(("POST " + path + " HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                    + headers + "Content-Type: application/octet-stream\r\n"
                    + "Content-Length: 100000\r\n\r\n").getBytes(UTF_8));
            output.write(new byte[1000]);
            output.flush();
        }
    }

    /**
     * Test bytes.
     *
     * @param length how many
     * @param marker text at the start, to look for in logs
     * @return the bytes
     */
    static byte[] body(int length, String marker)
    {
        byte[] bytes = new byte[length];
        byte[] start = marker.getBytes(UTF_8);
        System.arraycopy(start, 0, bytes, 0, start.length);
        for (int i = start.length; i < length; i++)
        {
            bytes[i] = (byte) (i * 31);
        }
        return bytes;
    }

    /**
     * The SHA-256 of bytes, in hex.
     *
     * @param bytes the bytes
     * @return the hash
     * @throws Exception if SHA-256 is missing
     */
    static String sha256(byte[] bytes) throws Exception
    {
        return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    /**
     * A field of a receipt.
     *
     * @param json  the receipt
     * @param field the field's name
     * @return its value, unquoted
     */
    static String field(String json, String field)
    {
        Matcher matcher = Pattern.compile("\"" + field
                + "\":\"?([^\",}]*)\"?").matcher(json);
        assertThat(matcher.find()).as(field + " in " + json).isTrue();
        return matcher.group(1);
    }

    /**
     * Waits for item.landed messages.
     *
     * @param intake the intake's name
     * @param count  how many
     * @param wait   how long to wait for them
     * @return the messages, opened
     * @throws Exception if one can't be opened
     */
    List<ConnectorMessage.ItemLanded> receive(String intake, int count,
            Duration wait) throws Exception
    {
        MessageOpener opener = new MessageOpener(
                Map.of("downstream-2026", KEYS.recipient.getPrivate()),
                Map.of("landing-2026",
                        (ECPublicKey) KEYS.signing.getPublic()));
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        List<ConnectorMessage.ItemLanded> landed = new ArrayList<>();
        long deadline = System.nanoTime() + wait.toNanos();
        while (landed.size() < count && System.nanoTime() < deadline)
        {
            String body = consumer.receiveBody("seda:" + intake + "-out",
                    200, String.class);
            if (body != null)
            {
                landed.add((ConnectorMessage.ItemLanded)
                        opener.open(body.getBytes(UTF_8)));
            }
        }
        return landed;
    }

    /**
     * Waits up to 15 seconds for a condition.
     *
     * @param condition the condition
     * @throws Exception if interrupted
     */
    static void awaitTrue(BooleanSupplier condition) throws Exception
    {
        long deadline = System.nanoTime() + 15_000_000_000L;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline)
        {
            Thread.sleep(50);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    /**
     * Reads a stream whole, for small test answers.
     *
     * @param input the stream
     * @return its text
     * @throws Exception if it can't be read
     */
    static String text(InputStream input) throws Exception
    {
        try (input)
        {
            return new String(input.readAllBytes(), UTF_8);
        }
    }
}
