/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.util.unit.DataSize;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Tests the Kafka route on a real broker (STAGES.md §10, "found
 * 2026-11-07"): a poll that isn't handled comes again, rather than being
 * passed by the next poll's commit, a handled poll commits every one of
 * its partitions, not only its last record's, and a batch tried again for
 * longer than the poll interval doesn't take the consumer out of its group.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class KafkaRewindIT
{

    /** The route's ID. */
    static final String ROUTE = "receive";

    /** The topic. */
    static final String TOPIC = "records";

    /** The consumer group. */
    static final String GROUP = "rewind";

    /** The records sent to each partition. */
    static final int EACH = 3;

    /** The consumer's poll interval, when a test sets it. */
    static final Duration POLL_INTERVAL = Duration.ofSeconds(5);

    /** The longest to wait for records or commits. */
    static final Duration WAIT = Duration.ofMinutes(2);

    /** The bodies handled, in order, over all batches. */
    final List<String> handled = Collections.synchronizedList(
            new ArrayList<>());

    /** The bodies of the batch given up. */
    final List<String> givenUp = Collections.synchronizedList(
            new ArrayList<>());

    /** Creates the tests; JUnit runs each test on a new instance. */
    KafkaRewindIT()
    {
    }

    /**
     * The first batch is given up, as fatal; its records come again to the
     * same consumer, and once all are handled both partitions are committed
     * to their ends.
     *
     * @throws Exception if the test fails
     */
    @Test
    void aBatchGivenUpComesAgainAndEveryPartitionIsCommitted()
            throws Exception
    {
        try (KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.2"))
        {
            kafka.start();
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                    kafka.getBootstrapServers());
            try (Admin admin = Admin.create(config))
            {
                admin.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1)))
                        .all().get();
                send(config);
                AtomicInteger batches = new AtomicInteger();
                ReceiveSettings settings = settings(kafka, "");
                try (DefaultCamelContext camel = new DefaultCamelContext())
                {
                    camel.addRoutes(new CamelReceiveRoute(ROUTE, settings,
                            messages ->
                            {
                                List<String> bodies = new ArrayList<>();
                                for (BrokerMessage message : messages)
                                {
                                    bodies.add(new String(message.body(),
                                            UTF_8));
                                }
                                if (batches.getAndIncrement() == 0)
                                {
                                    givenUp.addAll(bodies);
                                    return BatchHandler.Outcome.FATAL;
                                }
                                handled.addAll(bodies);
                                return BatchHandler.Outcome.DONE;
                            }, Duration.ofMillis(50), Duration.ofMillis(200)));
                    camel.start();
                    camel.getRouteController().startRoute(ROUTE);
                    await(() -> handled.size() >= 2 * EACH,
                            "every record is handled");
                    assertThat(givenUp).isNotEmpty();
                    assertThat(handled).containsAll(givenUp);
                    await(() -> committed(admin).equals(Map.of(0, (long) EACH,
                            1, (long) EACH)), "both partitions committed");
                }
            }
        }
    }

    /**
     * A batch tried again for longer than the poll interval is left for the
     * next poll each time, so the consumer stays in its group: every record
     * is handled once, and none comes again after its commit. Retried in
     * place, the consumer left the group, its commit failed and the records
     * were handled twice.
     *
     * @param protocol the consumer group protocol
     * @throws Exception if the test fails
     */
    @ParameterizedTest
    @ValueSource(strings = {"classic", "consumer"})
    void aBatchTriedAgainPastThePollIntervalKeepsTheGroup(String protocol)
            throws Exception
    {
        try (KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.2"))
        {
            kafka.start();
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                    kafka.getBootstrapServers());
            try (Admin admin = Admin.create(config))
            {
                admin.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1)))
                        .all().get();
                send(config);
                AtomicInteger attempts = new AtomicInteger();
                long[] firstAttempt = {0};
                ReceiveSettings settings = settings(kafka,
                        "&groupProtocol=" + protocol + "&maxPollIntervalMs="
                                + POLL_INTERVAL.toMillis());
                try (DefaultCamelContext camel = new DefaultCamelContext())
                {
                    camel.addRoutes(new CamelReceiveRoute(ROUTE, settings,
                            messages ->
                            {
                                attempts.incrementAndGet();
                                if (firstAttempt[0] == 0)
                                {
                                    firstAttempt[0] = System.nanoTime();
                                }
                                if (System.nanoTime() - firstAttempt[0]
                                        < 3 * POLL_INTERVAL.toNanos())
                                {
                                    return BatchHandler.Outcome.RETRY;
                                }
                                for (BrokerMessage message : messages)
                                {
                                    handled.add(new String(message.body(),
                                            UTF_8));
                                }
                                return BatchHandler.Outcome.DONE;
                            }, Duration.ofMillis(50), Duration.ofMillis(200)));
                    camel.start();
                    camel.getRouteController().startRoute(ROUTE);
                    await(() -> committed(admin).equals(Map.of(0, (long) EACH,
                            1, (long) EACH)), "both partitions committed");
                    Thread.sleep(2 * POLL_INTERVAL.toMillis());
                    assertThat(attempts.get()).isGreaterThan(2);
                    assertThat(handled).doesNotHaveDuplicates()
                            .hasSize(2 * EACH);
                }
            }
        }
    }

    /**
     * The route's settings on a test broker.
     *
     * @param kafka the broker
     * @param more  more URI options, each from {@code &}
     * @return the settings
     */
    static ReceiveSettings settings(KafkaContainer kafka, String more)
    {
        return ReceiveSettings.of(
                new ReceiveProperties("kafka", "kafka:" + TOPIC
                        + "?brokers=" + kafka.getBootstrapServers()
                        + "&groupId=" + GROUP
                        + "&autoOffsetReset=earliest" + more,
                        new ReceiveProperties.Batch(10,
                                DataSize.ofKilobytes(64),
                                Duration.ofMillis(500)),
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
     * Sends {@link #EACH} records to each partition.
     *
     * @param config the broker's address
     * @throws Exception if one isn't taken
     */
    static void send(Properties config) throws Exception
    {
        Properties producing = new Properties();
        producing.putAll(config);
        producing.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
                producing, new StringSerializer(), new ByteArraySerializer()))
        {
            for (int seq = 0; seq < EACH; seq++)
            {
                for (int partition = 0; partition < 2; partition++)
                {
                    producer.send(new ProducerRecord<>(TOPIC, partition,
                            "k" + partition, ("r-" + partition + "-" + seq)
                                    .getBytes(UTF_8))).get();
                }
            }
        }
    }

    /**
     * The group's committed offsets, by partition.
     *
     * @param admin the broker's admin client
     * @return each committed partition's next offset
     */
    static Map<Integer, Long> committed(Admin admin)
    {
        try
        {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin
                    .listConsumerGroupOffsets(GROUP)
                    .partitionsToOffsetAndMetadata().get();
            Map<Integer, Long> found = new java.util.TreeMap<>();
            offsets.forEach((partition, offset) ->
            {
                if (offset != null)
                {
                    found.put(partition.partition(), offset.offset());
                }
            });
            return found;
        }
        catch (Exception notYet)
        {
            return Map.of();
        }
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
