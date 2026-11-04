/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.assertDelivered;
import static com.sdh.healthcare.connector.file.it.ItSupport.interleaved;
import static com.sdh.healthcare.connector.file.it.ItSupport.publishAll;
import static com.sdh.healthcare.connector.file.it.ItSupport.requireDocker;

import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.it.ItSupport.Received;
import com.sdh.healthcare.connector.runtime.publish.ClosablePublisher;
import com.sdh.healthcare.connector.runtime.publish.Publishers;
import com.sdh.healthcare.connector.runtime.publish.QueueType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Design §10.3 kafka: record key = fileId, acks=all + idempotent producer,
 * metadata as record headers.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class KafkaIT
{

    /** The broker. */
    static KafkaContainer kafka;

    /** Starts the broker. */
    @BeforeAll
    static void start()
    {
        requireDocker();
        kafka = new KafkaContainer("apache/kafka:4.2.2");
        kafka.start();
    }

    /** Stops the broker. */
    @AfterAll
    static void stop()
    {
        if (kafka != null)
        {
            kafka.stop();
        }
    }

    /**
     * A record header's value.
     *
     * @param record the record
     * @param name the header name
     * @return its value as UTF-8, or null if absent
     */
    static String header(ConsumerRecord<String, String> record, String name)
    {
        Header header = record.headers().lastHeader(name);
        return header == null ? null
                : new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * Messages are keyed by file, acknowledged, and arrive in order within
     * their partition.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void keyedOrderedAcknowledgedDelivery() throws Exception
    {
        String bootstrap = kafka.getBootstrapServers();
        try (Admin admin =
                Admin.create(Map.of("bootstrap.servers", bootstrap)))
        {
            admin.createTopics(List.of(new NewTopic("sdh-sections", 3,
                    (short) 1))).all().get();
        }
        List<OutboundMessage> sent =
                interleaved(UUID.randomUUID(), UUID.randomUUID(), 100);
        try (DefaultCamelContext camel = new DefaultCamelContext())
        {
            camel.start();
            String uri = QueueType.KAFKA.effectiveUri(
                    "kafka:sdh-sections?brokers=" + bootstrap);
            try (ClosablePublisher publisher =
                    Publishers.create(camel, QueueType.KAFKA, uri))
            {
                publishAll(publisher, sent);
            }
        }

        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                Map.of("bootstrap.servers", bootstrap, "group.id", "it",
                        "auto.offset.reset", "earliest"),
                new StringDeserializer(), new StringDeserializer()))
        {
            consumer.subscribe(List.of("sdh-sections"));
            long deadline = System.currentTimeMillis() + 60_000;
            while (records.size() < sent.size()
                    && System.currentTimeMillis() < deadline)
            {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
        }
        records.sort(Comparator
                .<ConsumerRecord<String, String>>comparingInt(
                        ConsumerRecord::partition)
                .thenComparingLong(ConsumerRecord::offset));
        List<Received> got = records.stream()
                .map(record -> new Received(header(record, "messageId"),
                        header(record, "type"), header(record, "fileId"),
                        header(record, "contentType"), record.key(),
                        record.value()))
                .toList();
        assertDelivered(sent, got, true);
    }
}
