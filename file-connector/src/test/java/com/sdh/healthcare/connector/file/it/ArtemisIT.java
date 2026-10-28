package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.publish.ClosablePublisher;
import com.sdh.healthcare.connector.file.publish.Publishers;
import com.sdh.healthcare.connector.file.publish.QueueType;
import jakarta.jms.Connection;
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

/** Design §10.3 artemis: JMSXGroupID = fileId, duplicate detection via _AMQ_DUPL_ID = messageId. */
class ArtemisIT {
    static ArtemisContainer artemis;

    @BeforeAll
    static void start() {
        requireDocker();
        artemis = new ArtemisContainer("apache/activemq-artemis:2.44.0-alpine");
        artemis.start();
    }

    @AfterAll
    static void stop() {
        if (artemis != null) {
            artemis.stop();
        }
    }

    @Test
    void groupedOrderedDeliveryAndDuplicatesDropped() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(artemis.getBrokerUrl(), artemis.getUser(),
                artemis.getPassword());
        List<OutboundMessage> sent = interleaved(UUID.randomUUID(), UUID.randomUUID(), 50);
        try (DefaultCamelContext camel = new DefaultCamelContext()) {
            JmsComponent jms = new JmsComponent();
            jms.setConnectionFactory(cf);
            camel.addComponent("jms", jms);
            camel.start();
            String uri = QueueType.ARTEMIS.effectiveUri("jms:queue:sdh");
            try (ClosablePublisher pub = Publishers.create(camel, QueueType.ARTEMIS, uri)) {
                publishAll(pub, sent);
                publishAll(pub, sent.subList(0, 10)); // republished after a crash: same message IDs
            }
        }

        List<Received> got = new ArrayList<>();
        try (Connection c = cf.createConnection()) {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(s.createQueue("sdh"));
            jakarta.jms.Message m;
            while ((m = consumer.receive(2000)) != null) {
                assertThat(m.getStringProperty("_AMQ_DUPL_ID")).isEqualTo(m.getStringProperty("messageId"));
                got.add(new Received(m.getStringProperty("messageId"), m.getStringProperty("type"),
                        m.getStringProperty("fileId"), m.getStringProperty("contentType"),
                        m.getStringProperty("JMSXGroupID"), ((TextMessage) m).getText()));
            }
        }
        assertDelivered(sent, got, true);
    }
}
