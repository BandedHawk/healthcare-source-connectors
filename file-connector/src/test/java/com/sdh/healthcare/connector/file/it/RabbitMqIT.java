package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.*;

import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.publish.ClosablePublisher;
import com.sdh.healthcare.connector.file.publish.Publishers;
import com.sdh.healthcare.connector.file.publish.QueueType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.component.springrabbit.SpringRabbitMQComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.testcontainers.containers.RabbitMQContainer;

/** Design §10.3 rabbitmq: message_id = messageId, publisher confirms, metadata as AMQP headers. */
class RabbitMqIT {
    static RabbitMQContainer rabbit;

    @BeforeAll
    static void start() {
        requireDocker();
        rabbit = new RabbitMQContainer("rabbitmq:4.2.9-management-alpine");
        rabbit.start();
    }

    @AfterAll
    static void stop() {
        if (rabbit != null) {
            rabbit.stop();
        }
    }

    @Test
    void confirmedOrderedDeliveryWithMessageIds() throws Exception {
        CachingConnectionFactory cf = new CachingConnectionFactory(rabbit.getHost(), rabbit.getAmqpPort());
        cf.setUsername(rabbit.getAdminUsername());
        cf.setPassword(rabbit.getAdminPassword());
        cf.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        new RabbitAdmin(cf).declareQueue(new Queue("sdh", true));

        List<OutboundMessage> sent = interleaved(UUID.randomUUID(), UUID.randomUUID(), 50);
        try (DefaultCamelContext camel = new DefaultCamelContext()) {
            SpringRabbitMQComponent component = new SpringRabbitMQComponent();
            component.setConnectionFactory(cf);
            camel.addComponent("spring-rabbitmq", component);
            camel.start();
            String uri = QueueType.RABBITMQ.effectiveUri("spring-rabbitmq:default?routingKey=sdh");
            try (ClosablePublisher pub = Publishers.create(camel, QueueType.RABBITMQ, uri)) {
                publishAll(pub, sent);
            }
        }

        RabbitTemplate template = new RabbitTemplate(cf);
        List<Received> got = new ArrayList<>();
        Message m;
        while ((m = template.receive("sdh", 2000)) != null) {
            Map<String, Object> h = m.getMessageProperties().getHeaders();
            got.add(new Received(m.getMessageProperties().getMessageId(), str(h.get("type")), str(h.get("fileId")),
                    m.getMessageProperties().getContentType(), str(h.get("fileId")),
                    new String(m.getBody(), StandardCharsets.US_ASCII)));
        }
        cf.destroy();
        assertDelivered(sent, got, true);
    }

    static String str(Object o) {
        return o == null ? null : o.toString();
    }
}
