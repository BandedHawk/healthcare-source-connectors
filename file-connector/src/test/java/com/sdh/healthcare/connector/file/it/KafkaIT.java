package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.*;

import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.publish.ClosablePublisher;
import com.sdh.healthcare.connector.file.publish.Publishers;
import com.sdh.healthcare.connector.file.publish.QueueType;
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

/** Design §10.3 kafka: record key = fileId, acks=all + idempotent producer, metadata as record headers. */
class KafkaIT {
    static KafkaContainer kafka;

    @BeforeAll
    static void start() {
        requireDocker();
        kafka = new KafkaContainer("apache/kafka:4.2.2");
        kafka.start();
    }

    @AfterAll
    static void stop() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    static String header(ConsumerRecord<String, String> r, String name) {
        Header h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    @Test
    void keyedOrderedAcknowledgedDelivery() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(List.of(new NewTopic("sdh-sections", 3, (short) 1))).all().get();
        }
        List<OutboundMessage> sent = interleaved(UUID.randomUUID(), UUID.randomUUID(), 100);
        try (DefaultCamelContext camel = new DefaultCamelContext()) {
            camel.start();
            String uri = QueueType.KAFKA.effectiveUri("kafka:sdh-sections?brokers=" + bootstrap);
            try (ClosablePublisher pub = Publishers.create(camel, QueueType.KAFKA, uri)) {
                publishAll(pub, sent);
            }
        }

        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(Map.of("bootstrap.servers", bootstrap,
                "group.id", "it", "auto.offset.reset", "earliest"), new StringDeserializer(),
                new StringDeserializer())) {
            c.subscribe(List.of("sdh-sections"));
            long deadline = System.currentTimeMillis() + 60_000;
            while (records.size() < sent.size() && System.currentTimeMillis() < deadline) {
                c.poll(Duration.ofMillis(500)).forEach(records::add);
            }
        }
        records.sort(Comparator.<ConsumerRecord<String, String>>comparingInt(ConsumerRecord::partition)
                .thenComparingLong(ConsumerRecord::offset));
        List<Received> got = records.stream().map(r -> new Received(header(r, "messageId"), header(r, "type"),
                header(r, "fileId"), header(r, "contentType"), r.key(), r.value())).toList();
        assertDelivered(sent, got, true);
    }
}
