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
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.it.ItSupport.Received;
import com.sdh.healthcare.connector.runtime.publish.ClosablePublisher;
import com.sdh.healthcare.connector.runtime.publish.Publishers;
import com.sdh.healthcare.connector.runtime.publish.QueueType;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.activemq.ArtemisContainer;

/**
 * Design §10.3 artemis: JMSXGroupID = fileId, duplicate detection via
 * _AMQ_DUPL_ID = messageId.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ArtemisIT
{

    /** The broker. */
    static ArtemisContainer artemis;

    /** Starts the broker. */
    @BeforeAll
    static void start()
    {
        requireDocker();
        artemis = new ArtemisContainer(
                "apache/activemq-artemis:2.44.0-alpine");
        artemis.start();
    }

    /** Stops the broker. */
    @AfterAll
    static void stop()
    {
        if (artemis != null)
        {
            artemis.stop();
        }
    }

    /**
     * Messages arrive grouped by file and in order, and a republished
     * message is dropped as a duplicate.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void groupedOrderedDeliveryAndDuplicatesDropped() throws Exception
    {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                artemis.getBrokerUrl(), artemis.getUser(),
                artemis.getPassword());
        List<OutboundMessage> sent =
                interleaved(UUID.randomUUID(), UUID.randomUUID(), 50);
        try (DefaultCamelContext camel = new DefaultCamelContext())
        {
            JmsComponent jms = new JmsComponent();
            jms.setConnectionFactory(factory);
            camel.addComponent("jms", jms);
            camel.start();
            String uri = QueueType.ARTEMIS.effectiveUri("jms:queue:sdh");
            try (ClosablePublisher publisher =
                    Publishers.create(camel, QueueType.ARTEMIS, uri))
            {
                publishAll(publisher, sent);
                // Republished after a crash: the same message IDs.
                publishAll(publisher, sent.subList(0, 10));
            }
        }

        List<Received> got = new ArrayList<>();
        try (Connection connection = factory.createConnection())
        {
            connection.start();
            Session session =
                    connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer =
                    session.createConsumer(session.createQueue("sdh"));
            Message message;
            while ((message = consumer.receive(2000)) != null)
            {
                assertThat(message.getStringProperty("_AMQ_DUPL_ID"))
                        .isEqualTo(message.getStringProperty("messageId"));
                got.add(new Received(message.getStringProperty("messageId"),
                        message.getStringProperty("type"),
                        message.getStringProperty("fileId"),
                        message.getStringProperty("contentType"),
                        message.getStringProperty("JMSXGroupID"),
                        ((TextMessage) message).getText()));
            }
        }
        assertDelivered(sent, got, true);
    }
}
