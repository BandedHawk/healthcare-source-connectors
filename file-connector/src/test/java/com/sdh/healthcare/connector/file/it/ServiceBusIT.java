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

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.it.ItSupport.Received;
import com.sdh.healthcare.connector.broker.send.ClosablePublisher;
import com.sdh.healthcare.connector.broker.send.Publishers;
import com.sdh.healthcare.connector.broker.send.QueueType;
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
 * Design §10.3 servicebus: SessionId = itemId, MessageId = messageId with
 * duplicate detection, metadata as application properties. Microsoft's
 * emulator, backed by SQL Server; config is copied in (no host mounts).
 * The emulator caps the duplicate-detection window at 5 minutes.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ServiceBusIT
{

    /**
     * The emulator's configuration: one session queue, duplicate detection
     * on.
     */
    static final String CONFIG = """
            {"UserConfig":{"Namespaces":[{"Name":"sbemulatorns","Queues":\
            [{"Name":"sdh","Properties":{
              "DeadLetteringOnMessageExpiration":false,\
            "DefaultMessageTimeToLive":"PT1H",
              "DuplicateDetectionHistoryTimeWindow":"PT5M",\
            "ForwardDeadLetteredMessagesTo":"","ForwardTo":"",
              "LockDuration":"PT1M","MaxDeliveryCount":3,\
            "RequiresDuplicateDetection":true,"RequiresSession":true}}],
              "Topics":[]}],"Logging":{"Type":"File"}}}""";

    /** The network the emulator and its database share. */
    static Network network;

    /** The emulator's database. */
    static MSSQLServerContainer sql;

    /** The Service Bus emulator. */
    static ServiceBusEmulatorContainer servicebus;

    /** Starts the database and the emulator. */
    @BeforeAll
    static void start()
    {
        requireDocker();
        network = Network.newNetwork();
        sql = new MSSQLServerContainer(
                "mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04")
                .acceptLicense().withNetwork(network);
        servicebus = new ServiceBusEmulatorContainer(
                "mcr.microsoft.com/azure-messaging/servicebus-emulator:2.0.0")
                .acceptLicense()
                .withConfig(Transferable.of(CONFIG))
                .withMsSqlServerContainer(sql)
                .withNetwork(network);
        servicebus.start();
    }

    /** Stops the emulator and the database. */
    @AfterAll
    static void stop()
    {
        if (servicebus != null)
        {
            servicebus.stop();
        }
        if (sql != null)
        {
            sql.stop();
        }
        if (network != null)
        {
            network.close();
        }
    }

    /**
     * Each file's messages arrive in their own session, and republished
     * ones are dropped.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void sessionsPerFileAndDuplicatesDropped() throws Exception
    {
        String connectionString = servicebus.getConnectionString();
        List<OutboundMessage> sent =
                interleaved(UUID.randomUUID(), UUID.randomUUID(), 20);
        try (DefaultCamelContext camel = new DefaultCamelContext())
        {
            camel.start();
            String uri = QueueType.SERVICEBUS.effectiveUri(
                    "azure-servicebus:sdh?serviceBusType=queue"
                    + "&producerOperation=sendMessages&connectionString=RAW("
                    + connectionString + ")");
            try (ClosablePublisher publisher =
                    Publishers.create(camel, QueueType.SERVICEBUS, uri))
            {
                publishAll(publisher, sent);
                // Duplicate detection on MessageId drops these.
                publishAll(publisher, sent.subList(0, 4));
            }
        }

        List<Received> got = new ArrayList<>();
        try (ServiceBusSessionReceiverClient sessions =
                new ServiceBusClientBuilder()
                        .connectionString(connectionString)
                        .sessionReceiver().queueName("sdh")
                        .receiveMode(ServiceBusReceiveMode.RECEIVE_AND_DELETE)
                        .buildClient())
        {
            for (int session = 0; session < 2; session++)
            {
                ServiceBusReceiverClient receiver =
                        sessions.acceptNextSession();
                for (ServiceBusReceivedMessage message :
                        receiver.receiveMessages(100, Duration.ofSeconds(5)))
                {
                    Map<String, Object> properties =
                            message.getApplicationProperties();
                    got.add(new Received(message.getMessageId(),
                            text(properties.get("type")),
                            text(properties.get("itemId")),
                            message.getContentType(), message.getSessionId(),
                            message.getBody().toString()));
                }
                receiver.close();
            }
        }
        assertDelivered(sent, got, true);
    }

    /**
     * A property value as text.
     *
     * @param value the value, or null
     * @return its text, or null
     */
    static String text(Object value)
    {
        return value == null ? null : value.toString();
    }
}
