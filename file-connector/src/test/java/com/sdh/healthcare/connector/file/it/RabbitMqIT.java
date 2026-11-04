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

/**
 * Design §10.3 rabbitmq: message_id = messageId, publisher confirms,
 * metadata as AMQP headers.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RabbitMqIT
{

    /** The broker. */
    static RabbitMQContainer rabbit;

    /** Starts the broker. */
    @BeforeAll
    static void start()
    {
        requireDocker();
        rabbit = new RabbitMQContainer("rabbitmq:4.2.9-management-alpine");
        rabbit.start();
    }

    /** Stops the broker. */
    @AfterAll
    static void stop()
    {
        if (rabbit != null)
        {
            rabbit.stop();
        }
    }

    /**
     * Messages are confirmed and arrive in order with their IDs and
     * metadata.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void confirmedOrderedDeliveryWithMessageIds() throws Exception
    {
        CachingConnectionFactory factory = new CachingConnectionFactory(
                rabbit.getHost(), rabbit.getAmqpPort());
        factory.setUsername(rabbit.getAdminUsername());
        factory.setPassword(rabbit.getAdminPassword());
        factory.setPublisherConfirmType(
                CachingConnectionFactory.ConfirmType.CORRELATED);
        new RabbitAdmin(factory).declareQueue(new Queue("sdh", true));

        List<OutboundMessage> sent =
                interleaved(UUID.randomUUID(), UUID.randomUUID(), 50);
        try (DefaultCamelContext camel = new DefaultCamelContext())
        {
            SpringRabbitMQComponent component = new SpringRabbitMQComponent();
            component.setConnectionFactory(factory);
            camel.addComponent("spring-rabbitmq", component);
            camel.start();
            String uri = QueueType.RABBITMQ.effectiveUri(
                    "spring-rabbitmq:default?routingKey=sdh");
            try (ClosablePublisher publisher =
                    Publishers.create(camel, QueueType.RABBITMQ, uri))
            {
                publishAll(publisher, sent);
            }
        }

        RabbitTemplate template = new RabbitTemplate(factory);
        List<Received> got = new ArrayList<>();
        Message message;
        while ((message = template.receive("sdh", 2000)) != null)
        {
            Map<String, Object> headers =
                    message.getMessageProperties().getHeaders();
            got.add(new Received(
                    message.getMessageProperties().getMessageId(),
                    text(headers.get("type")), text(headers.get("fileId")),
                    message.getMessageProperties().getContentType(),
                    text(headers.get("fileId")),
                    new String(message.getBody(), StandardCharsets.US_ASCII)));
        }
        factory.destroy();
        assertDelivered(sent, got, true);
    }

    /**
     * A header value as text.
     *
     * @param value the value, or null
     * @return its text, or null
     */
    static String text(Object value)
    {
        return value == null ? null : value.toString();
    }
}
