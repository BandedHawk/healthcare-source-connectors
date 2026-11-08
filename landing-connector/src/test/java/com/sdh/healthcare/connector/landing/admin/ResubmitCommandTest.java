/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.admin;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemLanded;
import com.sdh.healthcare.connector.landing.LandingTestKeys;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.ECPublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * {@code resubmit} asks the running node to land a landed item again, as a
 * new item that replaces it (STAGES.md §13.5), and the node does: its
 * {@code item.landed} names the item it replaces, with the same content.
 * Asked again, it is refused unless {@code --again}; an item never landed
 * is refused before anything is asked. Runs against a node that lands a
 * dropped file, so the items are real.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
// Closed after the class, while its database is still up: kept to the end
// of the run, its lease renewal held up the JVM's exit.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ResubmitCommandTest
{

    /** The directory the node lands from. */
    static final Path SOURCE;

    /** The node's keys, and the split stage's. */
    static final LandingTestKeys KEYS;

    /** The lease database, and the catalog's. */
    static final EmbeddedPostgres POSTGRES;

    static
    {
        try
        {
            Path root = Files.createTempDirectory("sdh-resubmit");
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

    /** Creates the tests; JUnit runs each on a new instance. */
    ResubmitCommandTest()
    {
    }

    /** Puts a recording exit hook in place of the real one. */
    @TestConfiguration
    static class Overrides
    {

        /** Created by Spring. */
        Overrides()
        {
        }

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

    /** The node's Camel context, which its own messages go through. */
    @Autowired
    CamelContext camel;

    /** The node's settings, as the admin command reads them. */
    @Autowired
    LandingProperties properties;

    /**
     * Runs the command.
     *
     * @param arguments the command line, after {@code resubmit}
     * @param output    where its output goes
     * @throws Exception if the command fails
     */
    void resubmit(List<String> arguments, ByteArrayOutputStream output)
            throws Exception
    {
        List<String> line = new ArrayList<>();
        line.add("resubmit");
        line.addAll(arguments);
        try (LandingAdminContext context = new LandingAdminContext(properties))
        {
            new ResubmitCommand().run(AdminArgs.parse(line, Set.of("again")),
                    context, new AuditRecord("alice"),
                    new PrintStream(output, true, UTF_8));
        }
    }

    /**
     * A dropped file is landed; resubmitted, the node lands it again under
     * a new ID, and announces it naming the first, with the same content.
     * Resubmitting it again is refused, unless {@code --again}, which lands
     * a third. An item never landed is refused at once.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void landsTheItemAgainAsANewItemThatReplacesIt() throws Exception
    {
        Files.writeString(SOURCE.resolve("one.csv"), "1,a\n2,b\n");
        MessageOpener opener = new MessageOpener(
                Map.of("downstream-2026", KEYS.recipient.getPrivate()),
                Map.of("landing-2026",
                        (ECPublicKey) KEYS.signing.getPublic()));
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        String body = consumer.receiveBody("seda:sdh-out", 30_000,
                String.class);
        assertThat(body).as("the node's own item.landed").isNotNull();
        ItemLanded first = (ItemLanded) opener.open(body.getBytes(UTF_8));
        String id = first.itemId().toString();

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        resubmit(List.of("--item-id", id), output);
        assertThat(output.toString(UTF_8)).contains("landed again as");

        String again = consumer.receiveBody("seda:sdh-out", 30_000,
                String.class);
        assertThat(again).as("the new item's item.landed").isNotNull();
        ItemLanded second = (ItemLanded) opener.open(again.getBytes(UTF_8));
        assertThat(second.itemId()).isNotEqualTo(first.itemId());
        assertThat(second.replaces()).isEqualTo(first.itemId());
        assertThat(second.intake()).isEqualTo(first.intake());
        assertThat(second.kind()).isEqualTo(first.kind());
        assertThat(second.size()).isEqualTo(first.size());
        assertThat(second.itemDigest()).isEqualTo(first.itemDigest());
        assertThat(output.toString(UTF_8)).contains(second.itemId()
                .toString());

        assertThatThrownBy(() -> resubmit(List.of("--item-id", id),
                new ByteArrayOutputStream()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("landed again before")
                .hasMessageContaining(second.itemId().toString());

        resubmit(List.of("--item-id", id, "--again"),
                new ByteArrayOutputStream());
        ItemLanded third = (ItemLanded) opener.open(consumer.receiveBody(
                "seda:sdh-out", 30_000, String.class).getBytes(UTF_8));
        assertThat(third.replaces()).isEqualTo(first.itemId());
        assertThat(third.itemId()).isNotIn(first.itemId(), second.itemId());

        assertThatThrownBy(() -> resubmit(List.of("--item-id",
                UUID.randomUUID().toString()), new ByteArrayOutputStream()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No fully landed item");
        assertThat(FATAL).isEmpty();
    }

    /**
     * The landing node's CLI knows the command, and gives its help.
     */
    @Test
    void theCliOffersResubmit()
    {
        ByteArrayOutputStream help = new ByteArrayOutputStream();
        int status = LandingAdmin.cli().run(List.of("help"),
                new PrintStream(help, true, UTF_8),
                new PrintStream(new ByteArrayOutputStream()),
                Map.of("SDH_OPERATOR", "alice"));
        assertThat(status).isZero();
        assertThat(help.toString(UTF_8))
                .contains("resubmit --item-id <id> [--again]");
    }
}
