/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.broker.send.ClosablePublisher;
import com.sdh.healthcare.connector.broker.send.Publishers;
import com.sdh.healthcare.connector.broker.send.QueueType;
import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

/**
 * Kafka as production runs it (HIPAA.md §8.1 items 7 and 9): TLS, SCRAM
 * logins and ACLs, with each stage's identity given only the rights
 * DEPLOYMENT.md ("Broker rights") lists. Two pipelines, {@code alpha} and
 * {@code beta}, share the broker. Through the stages' own code (the
 * publisher, the receive route and the broker check), a stage sends and
 * receives with only its own rights; it is refused another pipeline's
 * topics and consumer group, and a topic it only sends to; and a broker
 * whose certificate it doesn't trust, or a wrong password, is refused.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class KafkaSecurityIT
{

    /** The receiving route's ID. */
    static final String ROUTE = "receive";

    /** The property the URIs read the JAAS configuration from. */
    static final String JAAS = "kafka.jaas";

    /** How many messages each test sends. */
    static final int COUNT = 5;

    /** The longest to wait for a message or a refusal. */
    static final Duration WAIT = Duration.ofMinutes(2);

    /** The test CA. */
    static TestPki pki;

    /** The broker. */
    static SecureKafka kafka;

    /** The broker's admin client, as {@code admin}. */
    static Admin admin;

    /** Creates the tests; JUnit runs each test on a new instance. */
    KafkaSecurityIT()
    {
    }

    /**
     * Starts the broker, makes each pipeline's topics, and gives each
     * identity its rights.
     *
     * @throws Exception if the broker can't start
     */
    @BeforeAll
    static void start() throws Exception
    {
        pki = new TestPki("sdh-kafka-ca");
        kafka = new SecureKafka(pki, List.of("alpha-landing", "alpha-split",
                "beta-landing", "beta-split"));
        admin = Admin.create(kafka.client(kafka.jaas("admin"),
                kafka.trustedCa()));
        List<NewTopic> topics = new ArrayList<>();
        List<AclBinding> rights = new ArrayList<>();
        for (String pipeline : List.of("alpha", "beta"))
        {
            String landed = pipeline + ".landed";
            String split = pipeline + ".split";
            topics.add(new NewTopic(landed, 1, (short) 1));
            topics.add(new NewTopic(split, 1, (short) 1));
            // The landing node sends to its output.
            rights.addAll(sends(pipeline + "-landing", landed));
            // The split stage reads its input in its own group, and sends
            // onward and to its own input (reprocess).
            String stage = pipeline + "-split";
            rights.addAll(reads(stage, landed, stage));
            rights.addAll(sends(stage, split));
            rights.addAll(sends(stage, landed));
        }
        admin.createTopics(topics).all().get();
        admin.createAcls(rights).all().get();
    }

    /** Stops the broker. */
    @AfterAll
    static void stop()
    {
        if (admin != null)
        {
            admin.close();
        }
        if (kafka != null)
        {
            kafka.close();
        }
    }

    /**
     * The landing node sends to its output; the split stage receives it in
     * its own group, through the receive route, and sends it onward. The
     * broker check passes, and the group's commit is at the end.
     *
     * @throws Exception if the test fails
     */
    @Test
    void aStageSendsAndReceivesWithOnlyItsOwnRights() throws Exception
    {
        List<String> received = Collections.synchronizedList(
                new ArrayList<>());
        try (DefaultCamelContext landing = camel("alpha-landing");
                ClosablePublisher output = publisher(landing,
                        "alpha.landed"))
        {
            for (int seq = 0; seq < COUNT; seq++)
            {
                output.publish(message(seq)).get(WAIT.toSeconds(),
                        TimeUnit.SECONDS);
            }
        }
        try (DefaultCamelContext split = camel("alpha-split");
                ClosablePublisher onward = publisher(split, "alpha.split"))
        {
            split.addRoutes(new CamelReceiveRoute(ROUTE,
                    settings(kafka.uri("alpha.landed", JAAS,
                            kafka.trustedCa()) + "&groupId=alpha-split"),
                    messages ->
                    {
                        for (BrokerMessage each : messages)
                        {
                            String body = new String(each.body(), UTF_8);
                            onward.publish(new OutboundMessage(
                                    UUID.randomUUID().toString(),
                                    MessageType.RECORD_STORED,
                                    UUID.randomUUID(), "k",
                                    body.getBytes(UTF_8)))
                                    .orTimeout(WAIT.toSeconds(),
                                            TimeUnit.SECONDS)
                                    .join();
                            received.add(body);
                        }
                        return BatchHandler.Outcome.DONE;
                    }, Duration.ofMillis(50), Duration.ofMillis(200)));
            split.start();
            split.getRouteController().startRoute(ROUTE);
            await(() -> received.size() >= COUNT, "every message received");
            try (KafkaCheck check = new KafkaCheck((KafkaEndpoint) split
                    .getRoute(ROUTE).getEndpoint(), Duration.ofSeconds(30)))
            {
                assertThat(check.check()).as("the broker check").isNull();
            }
            await(() -> committed("alpha-split", "alpha.landed") == COUNT,
                    "the group committed to the end");
        }
        assertThat(received).hasSize(COUNT);
        assertThat(endOffset("alpha.split")).isEqualTo(COUNT);
    }

    /**
     * The split stage is refused another pipeline's input, topics to send
     * to and consumer group, and reading a topic it only sends to.
     *
     * @throws Exception if the test fails
     */
    @Test
    void aStageIsRefusedWhatItsRightsDontAllow() throws Exception
    {
        try (DefaultCamelContext split = camel("alpha-split"))
        {
            split.start();
            KafkaEndpoint other = split.getEndpoint(kafka.uri("beta.landed",
                    JAAS, kafka.trustedCa()) + "&groupId=alpha-split",
                    KafkaEndpoint.class);
            try (KafkaCheck check = new KafkaCheck(other,
                    Duration.ofSeconds(30)))
            {
                assertThat(check.check()).as("another pipeline's input")
                        .isEqualTo(BrokerProblem.PERMISSION);
            }
            try (ClosablePublisher publisher = publisher(split, "beta.split"))
            {
                assertThatThrownBy(() -> publisher.publish(message(0))
                        .get(WAIT.toSeconds(), TimeUnit.SECONDS))
                        .as("sending to another pipeline's topic")
                        .hasRootCauseInstanceOf(
                                TopicAuthorizationException.class);
            }
        }
        Properties alphaSplit = kafka.client(kafka.jaas("alpha-split"),
                kafka.trustedCa());
        assertThatThrownBy(() -> poll(alphaSplit, "beta.landed",
                "alpha-split")).as("reading another pipeline's input")
                .isInstanceOf(TopicAuthorizationException.class);
        assertThatThrownBy(() -> poll(alphaSplit, "alpha.split",
                "alpha-split")).as("reading a topic it only sends to")
                .isInstanceOf(TopicAuthorizationException.class);
        assertThatThrownBy(() -> poll(alphaSplit, "alpha.landed",
                "beta-split")).as("another pipeline's consumer group")
                .isInstanceOf(GroupAuthorizationException.class);
    }

    /**
     * A broker whose certificate isn't from a trusted CA is refused, and so
     * is a wrong password, each named as such by the broker check.
     *
     * @throws Exception if the test fails
     */
    @Test
    void anUntrustedBrokerAndAWrongPasswordAreRefused() throws Exception
    {
        Path otherCa = Files.writeString(Files.createTempFile("other-ca",
                ".pem"), new TestPki("sdh-other-ca").caPem());
        try (DefaultCamelContext split = camel("alpha-split"))
        {
            split.start();
            try (KafkaCheck check = new KafkaCheck(split.getEndpoint(
                    kafka.uri("alpha.landed", JAAS, otherCa)
                            + "&groupId=alpha-split", KafkaEndpoint.class),
                    Duration.ofSeconds(30)))
            {
                assertThat(check.check()).as("an untrusted certificate")
                        .isEqualTo(BrokerProblem.AUTH);
            }
        }
        try (DefaultCamelContext wrong = new DefaultCamelContext())
        {
            wrong.getPropertiesComponent().addInitialProperty(JAAS,
                    SecureKafka.jaas("alpha-split", "not-the-password"));
            wrong.start();
            try (KafkaCheck check = new KafkaCheck(wrong.getEndpoint(
                    kafka.uri("alpha.landed", JAAS, kafka.trustedCa())
                            + "&groupId=alpha-split", KafkaEndpoint.class),
                    Duration.ofSeconds(30)))
            {
                assertThat(check.check()).as("a wrong password")
                        .isEqualTo(BrokerProblem.AUTH);
            }
        }
    }

    /**
     * The rights to send to a topic.
     *
     * @param user  the identity
     * @param topic the topic
     * @return the ACLs
     */
    static List<AclBinding> sends(String user, String topic)
    {
        return List.of(allow(user, ResourceType.TOPIC, topic,
                AclOperation.WRITE), allow(user, ResourceType.TOPIC, topic,
                AclOperation.DESCRIBE));
    }

    /**
     * The rights to read a topic in a consumer group.
     *
     * @param user  the identity
     * @param topic the topic
     * @param group the group
     * @return the ACLs
     */
    static List<AclBinding> reads(String user, String topic, String group)
    {
        return List.of(allow(user, ResourceType.TOPIC, topic,
                AclOperation.READ), allow(user, ResourceType.TOPIC, topic,
                AclOperation.DESCRIBE), allow(user, ResourceType.GROUP, group,
                AclOperation.READ));
    }

    /**
     * One ACL allowing an operation.
     *
     * @param user      the identity
     * @param type      the resource's type
     * @param name      the resource
     * @param operation the operation
     * @return the ACL
     */
    static AclBinding allow(String user, ResourceType type, String name,
            AclOperation operation)
    {
        return new AclBinding(new ResourcePattern(type, name,
                PatternType.LITERAL), new AccessControlEntry("User:" + user,
                "*", operation, AclPermissionType.ALLOW));
    }

    /**
     * A Camel context whose URIs log in as an identity.
     *
     * @param user the identity
     * @return the context, not started
     */
    static DefaultCamelContext camel(String user)
    {
        DefaultCamelContext camel = new DefaultCamelContext();
        camel.getPropertiesComponent().addInitialProperty(JAAS,
                kafka.jaas(user));
        return camel;
    }

    /**
     * The stages' publisher on a topic.
     *
     * @param camel the context, which it starts if it isn't yet
     * @param topic the topic
     * @return the publisher
     */
    static ClosablePublisher publisher(DefaultCamelContext camel,
            String topic)
    {
        if (!camel.isStarted())
        {
            camel.start();
        }
        return Publishers.create(camel, QueueType.KAFKA,
                QueueType.KAFKA.effectiveUri(
                        kafka.uri(topic, JAAS, kafka.trustedCa())));
    }

    /**
     * A test message.
     *
     * @param seq its number
     * @return the message
     */
    static OutboundMessage message(int seq)
    {
        return new OutboundMessage(UUID.randomUUID().toString(),
                MessageType.ITEM_LANDED, UUID.randomUUID(), "k",
                ("m-" + seq).getBytes(UTF_8));
    }

    /**
     * The receive route's settings for a URI.
     *
     * @param uri the consumer URI
     * @return the settings
     */
    static ReceiveSettings settings(String uri)
    {
        return ReceiveSettings.of(
                new ReceiveProperties("kafka",
                        uri + "&autoOffsetReset=earliest",
                        new ReceiveProperties.Batch(10,
                                DataSize.ofKilobytes(64),
                                Duration.ofMillis(200)),
                        Duration.ofSeconds(30),
                        new ReceiveProperties.Rabbitmq(null, "/",
                                null, null, false),
                        new ReceiveProperties.Artemis(null, null,
                                null, null),
                        new ReceiveProperties.Eventhubs(null, null,
                                null)),
                "sdh.test.broker", Set.of());
    }

    /**
     * Polls a topic in a group until the broker answers or refuses.
     *
     * @param client the client's settings
     * @param topic  the topic
     * @param group  the group
     * @throws Exception the broker's refusal
     */
    static void poll(Properties client, String topic, String group)
            throws Exception
    {
        Properties consuming = new Properties();
        consuming.putAll(client);
        consuming.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consuming.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(
                consuming, new ByteArrayDeserializer(),
                new ByteArrayDeserializer()))
        {
            consumer.subscribe(List.of(topic));
            long until = System.nanoTime() + WAIT.toNanos();
            while (System.nanoTime() < until)
            {
                consumer.poll(Duration.ofMillis(500));
            }
        }
    }

    /**
     * A group's committed offset on a topic's only partition.
     *
     * @param group the group
     * @param topic the topic
     * @return the next offset; -1 if none is committed yet
     */
    static long committed(String group, String topic)
    {
        try
        {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin
                    .listConsumerGroupOffsets(group)
                    .partitionsToOffsetAndMetadata().get();
            OffsetAndMetadata offset = offsets.get(
                    new TopicPartition(topic, 0));
            return offset == null ? -1 : offset.offset();
        }
        catch (Exception notYet)
        {
            return -1;
        }
    }

    /**
     * A topic's end offset on its only partition.
     *
     * @param topic the topic
     * @return the end offset
     * @throws Exception if it can't be read
     */
    static long endOffset(String topic) throws Exception
    {
        TopicPartition partition = new TopicPartition(topic, 0);
        return admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                .all().get().get(partition).offset();
    }

    /**
     * Waits for a condition.
     *
     * @param condition the condition
     * @param what      what is waited for, for the failure
     * @throws InterruptedException if interrupted
     */
    static void await(BooleanSupplier condition, String what)
            throws InterruptedException
    {
        long until = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean())
        {
            assertThat(System.nanoTime()).as(what).isLessThan(until);
            Thread.sleep(100);
        }
    }
}
