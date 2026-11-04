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
import com.sdh.healthcare.connector.landing.runtime.LandingRuntime;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.KeyPair;
import java.security.interfaces.ECPublicKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.sshd.common.config.keys.PublicKeyEntry;
import org.apache.sshd.common.config.keys.writer.openssh.OpenSSHKeyPairResourceWriter;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The whole landing node on an SFTP intake, without Docker: an SFTP server
 * in the test, embedded Postgres, a JDBC catalog, key files and a SEDA
 * queue. The server is stopped and started again part-way.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
class SftpLandingApplicationTest
{

    /** The account on the test server. */
    static final String USER = "sdh";

    /** The server's files; {@code /} on the server. */
    static final Path SERVED;

    /** The key files. */
    static final LandingTestKeys KEYS;

    /** The lease database and catalog. */
    static final EmbeddedPostgres POSTGRES;

    /** The server's host key. */
    static final KeyPair HOST_KEY;

    /** The account's key. */
    static final KeyPair USER_KEY;

    /** The account's key file. */
    static final Path USER_KEY_FILE;

    /** The known_hosts file. */
    static final Path KNOWN_HOSTS;

    /** The port the server listens on. */
    static final int PORT;

    /** The test server; null while stopped. */
    static SshServer server;

    static
    {
        try
        {
            Path root = Files.createTempDirectory("sdh-landing-sftp");
            SERVED = Files.createDirectories(root.resolve("served"));
            Files.createDirectories(SERVED.resolve("drop"));
            KEYS = new LandingTestKeys(
                    Files.createDirectories(root.resolve("keys")));
            POSTGRES = EmbeddedPostgres.start();
            HOST_KEY = LandingTestKeys.ec();
            USER_KEY = LandingTestKeys.ec();
            USER_KEY_FILE = root.resolve("id_ecdsa");
            try (OutputStream output = Files.newOutputStream(USER_KEY_FILE))
            {
                OpenSSHKeyPairResourceWriter.INSTANCE.writePrivateKey(
                        USER_KEY, "sdh-test", null, output);
            }
            startServer(0);
            PORT = server.getPort();
            KNOWN_HOSTS = Files.writeString(root.resolve("known_hosts"),
                    "[127.0.0.1]:" + PORT + " "
                            + PublicKeyEntry.toString(HOST_KEY.getPublic())
                            + "\n");
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
    }

    /** The reasons given to the exit hook. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /**
     * Starts the test server.
     *
     * @param port the port; 0 for any free one
     * @throws Exception if it can't start
     */
    static void startServer(int port) throws Exception
    {
        server = SshServer.setUpDefaultServer();
        server.setHost("127.0.0.1");
        server.setPort(port);
        server.setKeyPairProvider(KeyPairProvider.wrap(HOST_KEY));
        server.setPublickeyAuthenticator((user, key, session) ->
                USER.equals(user) && key.equals(USER_KEY.getPublic()));
        server.setSubsystemFactories(List.of(new SftpSubsystemFactory()));
        server.setFileSystemFactory(new VirtualFileSystemFactory(SERVED));
        server.start();
    }

    /** Stops the server after the tests. */
    @AfterAll
    static void stopServer() throws Exception
    {
        if (server != null)
        {
            server.stop(true);
        }
    }

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

    /**
     * Points the node at the test's database, keys and SFTP server.
     *
     * @param registry the property registry
     * @throws Exception if the recipient's fingerprint can't be made
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception
    {
        String jdbc = POSTGRES.getJdbcUrl("postgres", "postgres");
        registry.add("sdh.landing.intake", () -> "claims");
        registry.add("sdh.landing.source.type", () -> "sftp");
        registry.add("sdh.landing.source.path", () -> "/drop");
        registry.add("sdh.landing.source.access", () -> "read-write");
        registry.add("sdh.landing.source.settle", () -> "300ms");
        registry.add("sdh.landing.source.poll-interval", () -> "100ms");
        registry.add("sdh.landing.source.sftp.host", () -> "127.0.0.1");
        registry.add("sdh.landing.source.sftp.port", () -> PORT);
        registry.add("sdh.landing.source.sftp.user", () -> USER);
        registry.add("sdh.landing.source.sftp.private-key",
                USER_KEY_FILE::toString);
        registry.add("sdh.landing.source.sftp.known-hosts",
                KNOWN_HOSTS::toString);
        registry.add("sdh.landing.source.sftp.connect-timeout", () -> "2s");
        registry.add("sdh.landing.source.sftp.read-timeout", () -> "2s");
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
        registry.add("sdh.queue.uri", () -> "seda:sdh-out?size=1000");
        registry.add("sdh.retry.max-attempts", () -> "2");
        registry.add("sdh.retry.initial-backoff", () -> "10ms");
    }

    /** The application's Camel context. */
    @Autowired
    CamelContext camel;

    /** The node's services. */
    @Autowired
    LandingRuntime runtime;

    /**
     * Files on the server are landed, announced and deleted; while the
     * server is down the intake is reported unreachable, and once it is
     * back the node reconnects and carries on.
     */
    @Test
    void landsFromSftpAndRidesOutAnOutage() throws Exception
    {
        drop("one.csv", "1,a\n2,b\n");
        drop("two.csv", "3,c\n");
        List<ConnectorMessage.ItemLanded> landed = receive(2);
        assertThat(landed).extracting(ConnectorMessage.ItemLanded::size)
                .containsExactlyInAnyOrder(8L, 4L);
        awaitTrue(() -> !Files.exists(SERVED.resolve("drop/one.csv"))
                && !Files.exists(SERVED.resolve("drop/two.csv")));

        server.stop(true);
        server = null;
        awaitTrue(() -> !runtime.service().reachable());
        startServer(PORT);
        drop("three.csv", "4,d\n5,e\n6,f\n");
        assertThat(receive(1)).singleElement().extracting(
                ConnectorMessage.ItemLanded::size).isEqualTo(12L);
        assertThat(runtime.service().reachable()).isTrue();
        awaitTrue(() -> !Files.exists(SERVED.resolve("drop/three.csv")));
        assertThat(FATAL).isEmpty();
    }

    /**
     * Writes a file on the server, a minute old.
     *
     * @param name    its name
     * @param content its content
     * @throws Exception if it can't be written
     */
    static void drop(String name, String content) throws Exception
    {
        Path file = Files.writeString(SERVED.resolve("drop").resolve(name),
                content);
        Files.setLastModifiedTime(file,
                FileTime.from(Instant.now().minusSeconds(60)));
    }

    /**
     * Waits for item.landed messages.
     *
     * @param count how many
     * @return the messages, opened
     * @throws Exception if one can't be opened
     */
    List<ConnectorMessage.ItemLanded> receive(int count) throws Exception
    {
        MessageOpener opener = new MessageOpener(
                Map.of("downstream-2026", KEYS.recipient.getPrivate()),
                Map.of("landing-2026",
                        (ECPublicKey) KEYS.signing.getPublic()));
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        List<ConnectorMessage.ItemLanded> landed = new ArrayList<>();
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (landed.size() < count && System.nanoTime() < deadline)
        {
            String body = consumer.receiveBody("seda:sdh-out", 500,
                    String.class);
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
}
