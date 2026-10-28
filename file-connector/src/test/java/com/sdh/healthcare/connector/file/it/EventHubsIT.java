package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.*;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.publish.ClosablePublisher;
import com.sdh.healthcare.connector.file.publish.Publishers;
import com.sdh.healthcare.connector.file.publish.QueueType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.azure.EventHubsEmulatorContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;

/**
 * Design §10.3 eventhubs: partitionKey = fileId keeps each file in one partition, in order. The adapter carries no
 * metadata here (messageId is inside the payload), so only key, order and body are checked.
 */
class EventHubsIT {
    static final String CONFIG = """
            {"UserConfig":{"NamespaceConfig":[{"Type":"EventHub","Name":"emulatorns1","Entities":[
              {"Name":"sdh","PartitionCount":"4","ConsumerGroups":[{"Name":"it"}]}]}],
              "LoggingConfig":{"Type":"File"}}}""";

    static Network network;
    static AzuriteContainer azurite;
    static EventHubsEmulatorContainer eventhubs;

    @BeforeAll
    static void start() {
        requireDocker();
        network = Network.newNetwork();
        azurite = new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.35.0").withNetwork(network);
        eventhubs = new EventHubsEmulatorContainer("mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.2.0")
                .acceptLicense()
                .withConfig(Transferable.of(CONFIG))
                .withAzuriteContainer(azurite)
                .withNetwork(network);
        eventhubs.start();
    }

    @AfterAll
    static void stop() {
        if (eventhubs != null) {
            eventhubs.stop();
        }
        if (azurite != null) {
            azurite.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @Test
    void eachFileStaysInOnePartitionInOrder() throws Exception {
        String base = eventhubs.getConnectionString();
        String cs = base + (base.endsWith(";") ? "" : ";") + "EntityPath=sdh";
        List<OutboundMessage> sent = interleaved(UUID.randomUUID(), UUID.randomUUID(), 30);
        try (DefaultCamelContext camel = new DefaultCamelContext()) {
            camel.start();
            String uri = QueueType.EVENTHUBS.effectiveUri("azure-eventhubs:?connectionString=RAW(" + cs + ")");
            try (ClosablePublisher pub = Publishers.create(camel, QueueType.EVENTHUBS, uri)) {
                publishAll(pub, sent);
            }
        }

        List<Received> got = new ArrayList<>();
        try (EventHubConsumerClient consumer = new EventHubClientBuilder().connectionString(cs)
                .consumerGroup("it").buildConsumerClient()) {
            for (String partition : consumer.getPartitionIds()) {
                for (PartitionEvent e : consumer.receiveFromPartition(partition, 200, EventPosition.earliest(),
                        Duration.ofSeconds(5))) {
                    String body = e.getData().getBodyAsString();
                    got.add(new Received(body.substring(4), null, null, null, e.getData().getPartitionKey(), body));
                }
            }
        }
        assertDelivered(sent, got, false);
    }
}
