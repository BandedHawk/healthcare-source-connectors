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
import com.sdh.healthcare.connector.runtime.InboundDirectoryClaim;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.ECPublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The whole landing node on real components without Docker: embedded
 * Postgres for the lease and a JDBC Iceberg catalog, in-memory object
 * storage, key files on disk, the Camel route on a read-write directory,
 * and a SEDA queue standing in for the broker.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
class LandingConnectorApplicationTest
{

    /** The intake's source directory. */
    static final Path SOURCE;

    /** The key files. */
    static final LandingTestKeys KEYS;

    /** The lease database and catalog. */
    static final EmbeddedPostgres POSTGRES;

    static
    {
        try
        {
            Path root = Files.createTempDirectory("sdh-landing");
            SOURCE = Files.createDirectories(root.resolve("drop"));
            KEYS = new LandingTestKeys(
                    Files.createDirectories(root.resolve("keys")));
            POSTGRES = EmbeddedPostgres.start();
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

    /**
     * Points the node at the test's database, keys and directory.
     *
     * @param registry the property registry
     * @throws Exception if the recipient's fingerprint can't be made
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception
    {
        String jdbc = POSTGRES.getJdbcUrl("postgres", "postgres");
        registry.add("sdh.landing.intake", () -> "claims");
        registry.add("sdh.landing.source.path", SOURCE::toString);
        registry.add("sdh.landing.source.access", () -> "read-write");
        registry.add("sdh.landing.source.settle", () -> "300ms");
        registry.add("sdh.landing.source.poll-interval", () -> "100ms");
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

    /**
     * Files dropped in the source are landed, announced with
     * {@code item.landed}, and deleted.
     */
    @Test
    void landsAnnouncesAndDeletesDroppedFiles() throws Exception
    {
        assertThat(SOURCE.resolve(InboundDirectoryClaim.FILE)).exists();
        Files.writeString(SOURCE.resolve("one.csv"), "1,a\n2,b\n");
        Files.writeString(SOURCE.resolve("two.csv"), "3,c\n");

        MessageOpener opener = new MessageOpener(
                Map.of("downstream-2026", KEYS.recipient.getPrivate()),
                Map.of("landing-2026",
                        (ECPublicKey) KEYS.signing.getPublic()));
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        List<ConnectorMessage.ItemLanded> landed = new ArrayList<>();
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (landed.size() < 2 && System.nanoTime() < deadline)
        {
            String body = consumer.receiveBody("seda:sdh-out", 500,
                    String.class);
            if (body != null)
            {
                landed.add((ConnectorMessage.ItemLanded)
                        opener.open(body.getBytes(UTF_8)));
            }
        }
        assertThat(landed).extracting(ConnectorMessage.ItemLanded::size)
                .containsExactlyInAnyOrder(8L, 4L);
        assertThat(landed).allSatisfy(message ->
        {
            assertThat(message.intake()).isEqualTo("claims");
            assertThat(message.storage().namespace())
                    .isEqualTo("sdh_landing");
        });
        long deleteBy = System.nanoTime() + 5_000_000_000L;
        while ((Files.exists(SOURCE.resolve("one.csv"))
                || Files.exists(SOURCE.resolve("two.csv")))
                && System.nanoTime() < deleteBy)
        {
            Thread.sleep(50);
        }
        assertThat(SOURCE.resolve("one.csv")).doesNotExist();
        assertThat(SOURCE.resolve("two.csv")).doesNotExist();
        assertThat(FATAL).isEmpty();
    }
}
