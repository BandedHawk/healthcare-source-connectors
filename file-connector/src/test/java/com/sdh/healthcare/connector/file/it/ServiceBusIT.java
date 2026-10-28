package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.*;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.publish.ClosablePublisher;
import com.sdh.healthcare.connector.file.publish.Publishers;
import com.sdh.healthcare.connector.file.publish.QueueType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.azure.ServiceBusEmulatorContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.mssqlserver.MSSQLServerContainer;

/**
 * Design §10.3 servicebus: SessionId = fileId, MessageId = messageId with duplicate detection, metadata as
 * application properties. Microsoft's emulator, backed by SQL Server; config is copied in (no host mounts). The
 * emulator caps the duplicate-detection window at 5 minutes.
 */
class ServiceBusIT {
    static final String CONFIG = """
            {"UserConfig":{"Namespaces":[{"Name":"sbemulatorns","Queues":[{"Name":"sdh","Properties":{
              "DeadLetteringOnMessageExpiration":false,"DefaultMessageTimeToLive":"PT1H",
              "DuplicateDetectionHistoryTimeWindow":"PT5M","ForwardDeadLetteredMessagesTo":"","ForwardTo":"",
              "LockDuration":"PT1M","MaxDeliveryCount":3,"RequiresDuplicateDetection":true,"RequiresSession":true}}],
              "Topics":[]}],"Logging":{"Type":"File"}}}""";

    static Network network;
    static MSSQLServerContainer sql;
    static ServiceBusEmulatorContainer servicebus;

    @BeforeAll
    static void start() {
        requireDocker();
        network = Network.newNetwork();
        sql = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04")
                .acceptLicense().withNetwork(network);
        servicebus = new ServiceBusEmulatorContainer("mcr.microsoft.com/azure-messaging/servicebus-emulator:2.0.0")
                .acceptLicense()
                .withConfig(Transferable.of(CONFIG))
                .withMsSqlServerContainer(sql)
                .withNetwork(network);
        servicebus.start();
    }

    @AfterAll
    static void stop() {
        if (servicebus != null) {
            servicebus.stop();
        }
        if (sql != null) {
            sql.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @Test
    void sessionsPerFileAndDuplicatesDropped() throws Exception {
        String cs = servicebus.getConnectionString();
        List<OutboundMessage> sent = interleaved(UUID.randomUUID(), UUID.randomUUID(), 20);
        try (DefaultCamelContext camel = new DefaultCamelContext()) {
            camel.start();
            String uri = QueueType.SERVICEBUS.effectiveUri("azure-servicebus:sdh?serviceBusType=queue"
                    + "&producerOperation=sendMessages&connectionString=RAW(" + cs + ")");
            try (ClosablePublisher pub = Publishers.create(camel, QueueType.SERVICEBUS, uri)) {
                publishAll(pub, sent);
                publishAll(pub, sent.subList(0, 4)); // duplicate detection on MessageId drops these
            }
        }

        List<Received> got = new ArrayList<>();
        try (ServiceBusSessionReceiverClient sessions = new ServiceBusClientBuilder().connectionString(cs)
                .sessionReceiver().queueName("sdh").receiveMode(ServiceBusReceiveMode.RECEIVE_AND_DELETE)
                .buildClient()) {
            for (int s = 0; s < 2; s++) {
                var receiver = sessions.acceptNextSession();
                for (ServiceBusReceivedMessage m : receiver.receiveMessages(100, Duration.ofSeconds(5))) {
                    Map<String, Object> p = m.getApplicationProperties();
                    got.add(new Received(m.getMessageId(), str(p.get("type")), str(p.get("fileId")),
                            m.getContentType(), m.getSessionId(), m.getBody().toString()));
                }
                receiver.close();
            }
        }
        assertDelivered(sent, got, true);
    }

    static String str(Object o) {
        return o == null ? null : o.toString();
    }
}
