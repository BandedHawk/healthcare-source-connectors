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
import com.sdh.healthcare.connector.landing.config.BrokerSettings;
import com.sdh.healthcare.connector.landing.runtime.LandingRuntime;
import com.sdh.healthcare.connector.runtime.FatalExit;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;

/**
 * The whole landing node on a RabbitMQ queue intake (design §5.3.16):
 * a real broker, embedded Postgres, a JDBC catalog, key files and a SEDA
 * queue for {@code item.landed}. Messages are landed in batches, announced,
 * and acknowledged, leaving the queue empty.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
class RabbitLandingApplicationIT
{

    /** How many messages are sent. */
    static final int MESSAGES = 25;

    /** The key files. */
    static final LandingTestKeys KEYS;

    /** The lease database and catalog. */
    static final EmbeddedPostgres POSTGRES;

    /** The broker. */
    static final RabbitMQContainer RABBIT;

    static
    {
        try
        {
            Path root = Files.createTempDirectory("sdh-landing-rabbit");
            KEYS = new LandingTestKeys(
                    Files.createDirectories(root.resolve("keys")));
            POSTGRES = EmbeddedPostgres.start();
            RABBIT = new RabbitMQContainer(
                    "rabbitmq:4.2.9-management-alpine");
            RABBIT.start();
            CachingConnectionFactory factory = factory();
            new RabbitAdmin(factory).declareQueue(new Queue("adt", true));
            factory.destroy();
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
    }

    /** The reasons given to the exit hook. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /**
     * A connection factory for the test's own use.
     *
     * @return the factory
     */
    static CachingConnectionFactory factory()
    {
        CachingConnectionFactory factory = new CachingConnectionFactory();
        factory.setAddresses(RABBIT.getHost() + ":" + RABBIT.getAmqpPort());
        factory.setUsername(RABBIT.getAdminUsername());
        factory.setPassword(RABBIT.getAdminPassword());
        return factory;
    }

    /** Stops the broker after the tests. */
    @AfterAll
    static void stopBroker()
    {
        RABBIT.stop();
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
     * Points the node at the test's database, keys and broker.
     *
     * @param registry the property registry
     * @throws Exception if the recipient's fingerprint can't be made
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception
    {
        String jdbc = POSTGRES.getJdbcUrl("postgres", "postgres");
        registry.add("sdh.landing.intake", () -> "adt");
        registry.add("sdh.landing.source.type", () -> "queue");
        registry.add("sdh.landing.broker.type", () -> "rabbitmq");
        registry.add("sdh.landing.broker.uri",
                () -> "spring-rabbitmq:default?queues=adt");
        registry.add("sdh.landing.broker.batch.max-messages", () -> "10");
        registry.add("sdh.landing.broker.batch.max-wait", () -> "300ms");
        registry.add("sdh.landing.broker.rabbitmq.addresses",
                () -> RABBIT.getHost() + ":" + RABBIT.getAmqpPort());
        registry.add("sdh.landing.broker.rabbitmq.username",
                RABBIT::getAdminUsername);
        registry.add("sdh.landing.broker.rabbitmq.password",
                RABBIT::getAdminPassword);
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
     * Messages sent to the queue are landed in batches as message-batch
     * items, announced, and acknowledged.
     *
     * @throws Exception if the test fails
     */
    @Test
    void landsAnnouncesAndAcknowledgesMessages() throws Exception
    {
        assertThat(runtime.broker().kind())
                .isEqualTo(BrokerSettings.Kind.RABBITMQ);
        CachingConnectionFactory factory = factory();
        try
        {
            RabbitAdmin admin = new RabbitAdmin(factory);
            RabbitTemplate template = new RabbitTemplate(factory);
            long bytes = 0;
            for (int i = 0; i < MESSAGES; i++)
            {
                byte[] body = ("MSH|" + i + "\r").getBytes(UTF_8);
                bytes += body.length;
                MessageProperties properties = new MessageProperties();
                properties.setMessageId("adt-" + i);
                template.send("", "adt", new Message(body, properties));
            }

            List<ConnectorMessage.ItemLanded> landed = receive(bytes);
            assertThat(landed).isNotEmpty().allSatisfy(message ->
            {
                assertThat(message.intake()).isEqualTo("adt");
                assertThat(message.kind()).isEqualTo("message-batch");
            });
            assertThat(landed.stream()
                    .mapToLong(ConnectorMessage.ItemLanded::size).sum())
                    .isEqualTo(bytes);
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (admin.getQueueInfo("adt").getMessageCount() > 0
                    && System.nanoTime() < deadline)
            {
                Thread.sleep(100);
            }
            assertThat(admin.getQueueInfo("adt").getMessageCount()).isZero();
            assertThat(runtime.batchService().recentKeys())
                    .isEqualTo(MESSAGES);
            assertThat(FATAL).isEmpty();
        }
        finally
        {
            factory.destroy();
        }
    }

    /**
     * Waits for item.landed messages covering a number of bytes.
     *
     * @param bytes how many bytes were sent
     * @return the messages, opened
     * @throws Exception if one can't be opened
     */
    List<ConnectorMessage.ItemLanded> receive(long bytes) throws Exception
    {
        MessageOpener opener = new MessageOpener(
                Map.of("downstream-2026", KEYS.recipient.getPrivate()),
                Map.of("landing-2026",
                        (ECPublicKey) KEYS.signing.getPublic()));
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        List<ConnectorMessage.ItemLanded> landed = new ArrayList<>();
        long total = 0;
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (total < bytes && System.nanoTime() < deadline)
        {
            String body = consumer.receiveBody("seda:sdh-out", 500,
                    String.class);
            if (body != null)
            {
                ConnectorMessage.ItemLanded message =
                        (ConnectorMessage.ItemLanded) opener.open(
                                body.getBytes(UTF_8));
                landed.add(message);
                total += message.size();
            }
        }
        return landed;
    }
}
