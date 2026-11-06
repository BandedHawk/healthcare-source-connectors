/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.broker.receive.ArtemisReceiveRoute;
import com.sdh.healthcare.connector.broker.receive.BrokerKind;
import com.sdh.healthcare.connector.broker.receive.BrokerWatch;
import com.sdh.healthcare.connector.broker.receive.CamelReceiveRoute;
import com.sdh.healthcare.connector.broker.receive.EventHubsSupport;
import com.sdh.healthcare.connector.broker.receive.ReceiveProperties;
import com.sdh.healthcare.connector.broker.receive.ReceiveSettings;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandingLedger;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.landing.config.BrokerSettings;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.intake.Announcer;
import com.sdh.healthcare.connector.landing.intake.BatchService;
import com.sdh.healthcare.connector.landing.intake.IntakeHealth;
import com.sdh.healthcare.connector.landing.intake.IntakeService;
import com.sdh.healthcare.connector.landing.route.IntakeRoute;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.net.URI;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import javax.crypto.KeyGenerator;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.google.pubsub.GooglePubsubComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.util.unit.DataSize;
import org.testcontainers.activemq.ArtemisContainer;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.azure.EventHubsEmulatorContainer;
import org.testcontainers.azure.ServiceBusEmulatorContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.gcloud.PubSubEmulatorContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * The queue intake against real brokers (design §5.3.16): Kafka, SQS (on
 * ElasticMQ), RabbitMQ, Artemis, and the Pub/Sub, Service Bus and Event
 * Hubs emulators. For each, a node lands a batch but
 * crashes before it can announce it, so nothing is acknowledged; a new
 * node then lands every message exactly once: the crashed batch's
 * messages, delivered again, are recognised by their IDs, and the rest are
 * landed.
 * <p>
 * The node is the intake's route and {@link BatchService} on an in-memory
 * catalog, as {@code LandingRuntime} wires them, without the Spring
 * application around them.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class QueueIntakeIT
{

    /** The intake under test. */
    static final String INTAKE = "queue";

    /** The KEK that wraps the data keys. */
    static final String KEK = "landed-items-kek/1";

    /** How many messages each test sends. */
    static final int MESSAGES = 30;

    /** A quick retry policy with one attempt. */
    static final RetryPolicy RETRY =
            new RetryPolicy(1, Duration.ofMillis(1), Duration.ofMillis(1));

    /** Small batches, closed quickly. */
    static final ReceiveProperties.Batch BATCH = new ReceiveProperties.Batch(
            10, DataSize.ofKilobytes(64), Duration.ofMillis(500));

    /** No RabbitMQ connection. */
    static final ReceiveProperties.Rabbitmq NO_RABBIT =
            new ReceiveProperties.Rabbitmq(null, "/", null, null, false);

    /** How often the broker is checked. */
    static final Duration CHECK = Duration.ofSeconds(30);

    /** No Artemis connection. */
    static final ReceiveProperties.Artemis NO_ARTEMIS =
            new ReceiveProperties.Artemis(null, null, null, null);

    /** No Event Hubs checkpoint store. */
    static final ReceiveProperties.Eventhubs NO_EVENTHUBS =
            new ReceiveProperties.Eventhubs(null, null, null);

    /** Thrown by the publisher of a node that is to crash. */
    static final class Crash extends Error
    {

        /** For serialization. */
        private static final long serialVersionUID = 1L;
    }

    /** The landed items table. */
    Table items;

    /** The ledger table. */
    Table ledgerTable;

    /** Wraps the data keys. */
    AesKeyWrapProvider keys;

    /** Makes the hashes and MACs. */
    final RecordIds ids = new RecordIds(new byte[32]);

    /** The last lease epoch handed out. */
    long epoch;

    /** The signing key pair. */
    KeyPair signing;

    /** The recipient's key pair. */
    KeyPair recipient;

    /** What the publisher was sent. */
    final List<OutboundMessage> sent = new CopyOnWriteArrayList<>();

    /** The reasons given to the exit hook. */
    final List<String> fatal = new CopyOnWriteArrayList<>();

    /** An item the ledger has, which every message names as replaced. */
    final UUID earlier = UUID.randomUUID();

    /**
     * Creates the catalog, tables and keys.
     *
     * @throws Exception if a key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        items = LandingTables.landedItems(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        ledgerTable = LandingTables.ledger(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(KEK, generator.generateKey()));
        KeyPairGenerator ec = KeyPairGenerator.getInstance("EC");
        ec.initialize(new ECGenParameterSpec("secp256r1"));
        signing = ec.generateKeyPair();
        recipient = ec.generateKeyPair();
        new LandingLedger(ledgerTable, committer(), RETRY).record(List.of(
                new LandingLedger.Entry(INTAKE, null, earlier, "a".repeat(52),
                        Instant.now(), 0, null)));
    }

    /**
     * The link a message carries to the one it replaces.
     *
     * @param index its number
     * @return the link
     */
    String link(int index)
    {
        return earlier + "/prior-" + index;
    }

    /**
     * Kafka: offsets are committed only after the poll is landed.
     *
     * @throws Exception if the test fails
     */
    @Test
    void kafkaLandsEveryMessageOnceAcrossACrash() throws Exception
    {
        try (KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.2"))
        {
            kafka.start();
            Properties config = new Properties();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    kafka.getBootstrapServers());
            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(
                    config, new StringSerializer(), new ByteArraySerializer()))
            {
                for (int i = 0; i < MESSAGES; i++)
                {
                    producer.send(new ProducerRecord<String, byte[]>("hl7",
                            null, null, body(i), List.of(new RecordHeader(
                                    BatchService.REPLACES_HEADER,
                                    link(i).getBytes(UTF_8))))).get();
                }
            }
            BrokerSettings settings = BrokerSettings.of(
                    new LandingProperties.Broker("kafka", "kafka:hl7?brokers="
                            + kafka.getBootstrapServers() + "&groupId=landing",
                            BATCH, Duration.ofHours(1), CHECK, NO_RABBIT,
                            NO_ARTEMIS, NO_EVENTHUBS));
            crashThenRecover(settings, camel ->
            {
            });
        }
    }

    /**
     * SQS: a message is deleted only once its batch is landed; one that
     * isn't comes back after its visibility timeout. The keys are property
     * placeholders, as the intake requires.
     *
     * @throws Exception if the test fails
     */
    @Test
    void sqsLandsEveryMessageOnceAcrossACrash() throws Exception
    {
        try (GenericContainer<?> elasticmq = new GenericContainer<>(
                "softwaremill/elasticmq-native:1.7.1").withExposedPorts(9324))
        {
            elasticmq.start();
            String endpoint = "http://" + elasticmq.getHost() + ":"
                    + elasticmq.getMappedPort(9324);
            try (SqsClient sqs = SqsClient.builder()
                    .endpointOverride(URI.create(endpoint))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("x", "x")))
                    .build())
            {
                String queue = sqs.createQueue(request -> request
                        .queueName("hl7").attributes(Map.of(
                                QueueAttributeName.VISIBILITY_TIMEOUT, "2")))
                        .queueUrl();
                for (int i = 0; i < MESSAGES; i++)
                {
                    String text = new String(body(i), UTF_8);
                    MessageAttributeValue value = MessageAttributeValue
                            .builder().dataType("String")
                            .stringValue(link(i)).build();
                    sqs.sendMessage(request -> request.queueUrl(queue)
                            .messageBody(text).messageAttributes(Map.of(
                                    BatchService.REPLACES_HEADER, value)));
                }
            }
            BrokerSettings settings = BrokerSettings.of(
                    new LandingProperties.Broker("sqs", "aws2-sqs:hl7"
                            + "?overrideEndpoint=true&uriEndpointOverride="
                            + endpoint + "&region=us-east-1"
                            + "&accessKey={{sqs.key}}&secretKey={{sqs.key}}"
                            + "&autoCreateQueue=false&visibilityTimeout=2"
                            + "&waitTimeSeconds=1&delay=100", BATCH,
                            Duration.ofHours(1), CHECK, NO_RABBIT, NO_ARTEMIS,
                            NO_EVENTHUBS));
            crashThenRecover(settings, camel ->
            {
                Properties placeholders = new Properties();
                placeholders.put("sqs.key", "x");
                camel.getPropertiesComponent()
                        .setInitialProperties(placeholders);
            });
        }
    }

    /**
     * RabbitMQ: messages are acknowledged on their channel once landed, and
     * returned to the queue if not.
     *
     * @throws Exception if the test fails
     */
    @Test
    void rabbitmqLandsEveryMessageOnceAcrossACrash() throws Exception
    {
        try (RabbitMQContainer rabbit = new RabbitMQContainer(
                "rabbitmq:4.2.9-management-alpine"))
        {
            rabbit.start();
            ReceiveProperties.Rabbitmq connection =
                    new ReceiveProperties.Rabbitmq(rabbit.getHost() + ":"
                            + rabbit.getAmqpPort(), "/",
                            rabbit.getAdminUsername(),
                            rabbit.getAdminPassword(), false);
            CachingConnectionFactory factory = new CachingConnectionFactory();
            factory.setAddresses(connection.addresses());
            factory.setUsername(connection.username());
            factory.setPassword(connection.password());
            try
            {
                new RabbitAdmin(factory).declareQueue(new Queue("hl7", true));
                RabbitTemplate template = new RabbitTemplate(factory);
                for (int i = 0; i < MESSAGES; i++)
                {
                    MessageProperties properties = new MessageProperties();
                    properties.setMessageId("m-" + i);
                    properties.setHeader(BatchService.REPLACES_HEADER,
                            link(i));
                    template.send("", "hl7", new Message(body(i),
                            properties));
                }
                BrokerSettings settings = BrokerSettings.of(
                        new LandingProperties.Broker("rabbitmq",
                                "spring-rabbitmq:default?queues=hl7", BATCH,
                                Duration.ofHours(1), CHECK, connection,
                                NO_ARTEMIS, NO_EVENTHUBS));
                crashThenRecover(settings, camel -> camel.getRegistry().bind(
                        ReceiveSettings.RABBITMQ_BEAN, factory));
            }
            finally
            {
                factory.destroy();
            }
        }
    }

    /**
     * Artemis, by the JMS client: the batch's last message is acknowledged
     * once it is landed; a dropped connection returns the rest.
     *
     * @throws Exception if the test fails
     */
    @Test
    void artemisLandsEveryMessageOnceAcrossACrash() throws Exception
    {
        try (ArtemisContainer artemis = new ArtemisContainer(
                "apache/activemq-artemis:2.44.0-alpine"))
        {
            artemis.start();
            ReceiveProperties.Artemis connection =
                    new ReceiveProperties.Artemis(artemis.getBrokerUrl(),
                            artemis.getUser(), artemis.getPassword(), "hl7");
            try (ActiveMQConnectionFactory factory =
                    new ActiveMQConnectionFactory(connection.brokerUrl(),
                            connection.username(), connection.password());
                    Connection jms = factory.createConnection())
            {
                Session session = jms.createSession(false,
                        Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer =
                        session.createProducer(session.createQueue("hl7"));
                for (int i = 0; i < MESSAGES; i++)
                {
                    TextMessage message = session.createTextMessage(
                            new String(body(i), UTF_8));
                    message.setStringProperty(BatchService.REPLACES_HEADER,
                            link(i));
                    producer.send(message);
                }
            }
            BrokerSettings settings = BrokerSettings.of(
                    new LandingProperties.Broker("artemis", null, BATCH,
                            Duration.ofHours(1), CHECK, NO_RABBIT, connection,
                            NO_EVENTHUBS));
            crashThenRecover(settings, camel ->
            {
            });
        }
    }

    /**
     * Pub/Sub: each message is acknowledged once its batch is landed, by
     * the acknowledgement its exchange carries.
     *
     * @throws Exception if the test fails
     */
    @Test
    void pubsubLandsEveryMessageOnceAcrossACrash() throws Exception
    {
        try (PubSubEmulatorContainer emulator = new PubSubEmulatorContainer(
                "gcr.io/google.com/cloudsdktool/google-cloud-cli:"
                        + "590.0.0-emulators"))
        {
            emulator.start();
            String endpoint = emulator.getEmulatorEndpoint();
            ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint)
                    .usePlaintext().build();
            try
            {
                TransportChannelProvider transport =
                        FixedTransportChannelProvider.create(
                                GrpcTransportChannel.create(channel));
                CredentialsProvider none = NoCredentialsProvider.create();
                TopicName topic = TopicName.of("sdh-it", "hl7");
                SubscriptionName subscription =
                        SubscriptionName.of("sdh-it", "hl7-landing");
                try (TopicAdminClient topics = TopicAdminClient.create(
                        TopicAdminSettings.newBuilder()
                                .setTransportChannelProvider(transport)
                                .setCredentialsProvider(none).build());
                        SubscriptionAdminClient subscriptions =
                                SubscriptionAdminClient.create(
                                        SubscriptionAdminSettings.newBuilder()
                                                .setTransportChannelProvider(
                                                        transport)
                                                .setCredentialsProvider(none)
                                                .build()))
                {
                    topics.createTopic(topic);
                    subscriptions.createSubscription(Subscription.newBuilder()
                            .setName(subscription.toString())
                            .setTopic(topic.toString())
                            .setAckDeadlineSeconds(10).build());
                }
                Publisher publisher = Publisher.newBuilder(topic)
                        .setChannelProvider(transport)
                        .setCredentialsProvider(none).build();
                for (int i = 0; i < MESSAGES; i++)
                {
                    publisher.publish(PubsubMessage.newBuilder()
                            .setData(ByteString.copyFrom(body(i)))
                            .putAttributes(BatchService.REPLACES_HEADER,
                                    link(i)).build())
                            .get();
                }
                publisher.shutdown();
            }
            finally
            {
                channel.shutdownNow();
            }
            BrokerSettings settings = BrokerSettings.of(
                    new LandingProperties.Broker("pubsub",
                            "google-pubsub:sdh-it:hl7-landing", BATCH,
                            Duration.ofHours(1), CHECK, NO_RABBIT, NO_ARTEMIS,
                            NO_EVENTHUBS));
            // The subscriber takes its endpoint from the component only.
            crashThenRecover(settings, camel ->
            {
                GooglePubsubComponent component = new GooglePubsubComponent();
                component.setEndpoint(endpoint);
                component.setAuthenticate(false);
                camel.addComponent("google-pubsub", component);
            });
        }
    }

    /**
     * Service Bus: messages are locked while held, and completed once their
     * batch is landed; the sequence number is the ID.
     *
     * @throws Exception if the test fails
     */
    @Test
    void servicebusLandsEveryMessageOnceAcrossACrash() throws Exception
    {
        String config = """
                {"UserConfig": {"Namespaces": [{"Name": "sbemulatorns",
                  "Queues": [{"Name": "hl7", "Properties": {
                    "DeadLetteringOnMessageExpiration": false,
                    "DefaultMessageTimeToLive": "PT1H",
                    "DuplicateDetectionHistoryTimeWindow": "PT20S",
                    "ForwardDeadLetteredMessagesTo": "", "ForwardTo": "",
                    "LockDuration": "PT1M", "MaxDeliveryCount": 10,
                    "RequiresDuplicateDetection": false,
                    "RequiresSession": false}}],
                  "Topics": []}], "Logging": {"Type": "File"}}}""";
        try (Network network = Network.newNetwork();
                MSSQLServerContainer sql = new MSSQLServerContainer(
                        "mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04")
                        .acceptLicense().withNetwork(network);
                ServiceBusEmulatorContainer servicebus =
                        new ServiceBusEmulatorContainer("mcr.microsoft.com/"
                                + "azure-messaging/servicebus-emulator:2.0.0")
                                .acceptLicense()
                                .withConfig(Transferable.of(config))
                                .withMsSqlServerContainer(sql)
                                .withNetwork(network))
        {
            servicebus.start();
            String connection = servicebus.getConnectionString();
            try (ServiceBusSenderClient sender = new ServiceBusClientBuilder()
                    .connectionString(connection).sender().queueName("hl7")
                    .buildClient())
            {
                for (int i = 0; i < MESSAGES; i++)
                {
                    ServiceBusMessage message = new ServiceBusMessage(body(i));
                    message.getApplicationProperties().put(
                            BatchService.REPLACES_HEADER, link(i));
                    sender.sendMessage(message);
                }
            }
            BrokerSettings settings = BrokerSettings.of(
                    new LandingProperties.Broker("servicebus",
                            "azure-servicebus:hl7?serviceBusType=queue"
                                    + "&connectionString=RAW({{sb.cs}})",
                            BATCH, Duration.ofHours(1), CHECK, NO_RABBIT,
                            NO_ARTEMIS, NO_EVENTHUBS));
            crashThenRecover(settings, camel -> placeholder(camel, "sb.cs",
                    connection));
        }
    }

    /**
     * Event Hubs: the checkpoint moves only past landed batches, in the
     * node's blob checkpoint store (on Azurite); a new consumer group
     * starts from the earliest event.
     *
     * @throws Exception if the test fails
     */
    @Test
    void eventhubsLandsEveryMessageOnceAcrossACrash() throws Exception
    {
        String config = """
                {"UserConfig": {"NamespaceConfig": [{"Type": "EventHub",
                  "Name": "emulatorns1", "Entities": [{"Name": "hl7",
                    "PartitionCount": "2",
                    "ConsumerGroups": [{"Name": "landing"}]}]}],
                  "LoggingConfig": {"Type": "File"}}}""";
        try (Network network = Network.newNetwork();
                AzuriteContainer azurite = new AzuriteContainer(
                        "mcr.microsoft.com/azure-storage/azurite:3.35.0")
                        .withNetwork(network);
                EventHubsEmulatorContainer eventhubs =
                        new EventHubsEmulatorContainer("mcr.microsoft.com/"
                                + "azure-messaging/eventhubs-emulator:2.2.0")
                                .acceptLicense()
                                .withConfig(Transferable.of(config))
                                .withAzuriteContainer(azurite)
                                .withNetwork(network))
        {
            eventhubs.start();
            String base = eventhubs.getConnectionString();
            String connection = base + (base.endsWith(";") ? "" : ";")
                    + "EntityPath=hl7";
            try (EventHubProducerClient producer = new EventHubClientBuilder()
                    .connectionString(connection).buildProducerClient())
            {
                EventDataBatch batch = producer.createBatch();
                for (int i = 0; i < MESSAGES; i++)
                {
                    EventData event = new EventData(body(i));
                    event.getProperties().put(BatchService.REPLACES_HEADER,
                            link(i));
                    assertThat(batch.tryAdd(event)).isTrue();
                }
                producer.send(batch);
            }
            String storage = azurite.getConnectionString();
            new BlobContainerClientBuilder().connectionString(storage)
                    .containerName("checkpoints").buildClient().create();
            ReceiveProperties.Eventhubs checkpoints =
                    new ReceiveProperties.Eventhubs(storage, null,
                            "checkpoints");
            BrokerSettings settings = BrokerSettings.of(
                    new LandingProperties.Broker("eventhubs",
                            "azure-eventhubs:?consumerGroupName=landing"
                                    + "&connectionString=RAW({{eh.cs}})",
                            BATCH, Duration.ofHours(1), CHECK, NO_RABBIT,
                            NO_ARTEMIS, checkpoints));
            crashThenRecover(settings, camel ->
            {
                placeholder(camel, "eh.cs", connection);
                EventHubsSupport.bind(camel, checkpoints);
            });
        }
    }

    /**
     * Gives a Camel context a property for a placeholder.
     *
     * @param camel the context
     * @param name  the property
     * @param value its value
     */
    static void placeholder(CamelContext camel, String name, String value)
    {
        Properties properties = new Properties();
        properties.put(name, value);
        camel.getPropertiesComponent().setInitialProperties(properties);
    }

    /**
     * A message's body.
     *
     * @param index its number
     * @return the body
     */
    static byte[] body(int index)
    {
        return ("m-" + index + "\n").getBytes(UTF_8);
    }

    /**
     * Runs a node that crashes after landing its first batch, then a node
     * that lands the rest, and checks every message was landed once.
     *
     * @param settings the broker
     * @param setup    prepares each node's Camel context
     * @throws Exception if the test fails
     */
    void crashThenRecover(BrokerSettings settings,
            Consumer<CamelContext> setup) throws Exception
    {
        try (Node crashing = new Node(settings, setup, true))
        {
            awaitTrue(() -> !fatal.isEmpty() && landedKeys().size() > 0);
        }
        int landedBeforeTheCrash = landedKeys().size();
        assertThat(landedBeforeTheCrash).isBetween(1, BATCH.maxMessages());
        assertThat(sent).isEmpty();

        fatal.clear();
        try (Node recovered = new Node(settings, setup, false))
        {
            awaitTrue(() -> landedKeys().size() >= MESSAGES);
            // Let any message delivered again come and be skipped.
            Thread.sleep(3000);
            List<String> landed = landedKeys();
            assertThat(landed).hasSize(MESSAGES);
            assertThat(new HashSet<>(landed)).hasSize(MESSAGES);
            assertThat(recovered.service.recentKeys()).isEqualTo(MESSAGES);
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < MESSAGES; i++)
            {
                expected.add(link(i));
            }
            assertThat(landedLinks()).containsExactlyInAnyOrderElementsOf(
                    expected);
        }
        assertThat(fatal).isEmpty();
        assertThat(sent).isNotEmpty();
    }

    /**
     * The keys of every message landed so far.
     *
     * @return the keys, one per landed message
     */
    List<String> landedKeys()
    {
        try
        {
            items.refresh();
            return LandedItems.messageKeys(items, INTAKE, Instant.EPOCH);
        }
        catch (Exception failure)
        {
            throw new IllegalStateException(failure);
        }
    }

    /**
     * The links to replaced messages kept for every message landed so
     * far.
     *
     * @return the links, one per landed message, nulls included
     * @throws Exception if the table can't be read
     */
    List<String> landedLinks() throws Exception
    {
        items.refresh();
        List<String> links = new ArrayList<>();
        try (CloseableIterable<Record> rows =
                IcebergGenerics.read(items).build())
        {
            for (Record row : rows)
            {
                if (LandedItems.messageIndex(row).isPresent())
                {
                    links.addAll(LandedItems.messageReplaces(row, keys)
                            .orElseThrow());
                }
            }
        }
        return links;
    }

    /**
     * Waits up to three minutes for a condition.
     *
     * @param condition the condition
     * @throws Exception if interrupted
     */
    static void awaitTrue(BooleanSupplier condition)
            throws Exception
    {
        long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline)
        {
            Thread.sleep(100);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    /**
     * A committer under a new lease epoch, as after a restart.
     *
     * @return the committer
     */
    FencedCommitter committer()
    {
        long ours = ++epoch;
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return ours;
            }

            @Override
            public void checkHeld()
            {
            }
        });
        committer.establish(items);
        committer.establish(ledgerTable);
        return committer;
    }

    /** A landing node: the intake's route and service. */
    final class Node implements AutoCloseable
    {

        /** The node's Camel context. */
        final DefaultCamelContext camel = new DefaultCamelContext();

        /** The node's service. */
        final BatchService service;

        /**
         * Starts a node, as {@code LandingRuntime} would: recovery first,
         * then the route.
         *
         * @param settings the broker
         * @param setup    prepares the Camel context
         * @param crash    true for a node whose announcements all crash
         * @throws Exception if it can't start
         */
        Node(BrokerSettings settings, Consumer<CamelContext> setup,
                boolean crash) throws Exception
        {
            FencedCommitter committer = committer();
            ItemLander lander = new ItemLander(items, committer, keys, ids,
                    ItemLander.Settings.defaults(INTAKE, KEK)
                            .withRetry(RETRY), Clock.systemUTC());
            LandingLedger ledger = new LandingLedger(ledgerTable, committer,
                    RETRY);
            Announcer announcer = new Announcer(new MessageSealer(
                    new ECDSASigner((ECPrivateKey) signing.getPrivate()),
                    "connector", recipient.getPublic(), "downstream"),
                    message ->
                    {
                        if (crash)
                        {
                            throw new Crash();
                        }
                        sent.add(message);
                        return CompletableFuture.completedFuture(null);
                    }, "sdh", LandingTables.DEFAULT_NAMESPACE, RETRY,
                    Duration.ofSeconds(5));
            service = new BatchService(INTAKE, new IntakeService.Storage(
                    lander, ledger, ledger.load(INTAKE), ids), items,
                    announcer, fatal::add, Clock.systemUTC(),
                    settings.dedupWindow());
            setup.accept(camel);
            ReceiveSettings receive = settings.receive();
            if (receive.kind() == BrokerKind.SQS)
            {
                new BrokerWatch(IntakeRoute.ROUTE_ID, receive,
                        new IntakeHealth(INTAKE, System::nanoTime), camel)
                        .bind(null);
            }
            RouteBuilder route = receive.kind() == BrokerKind.ARTEMIS
                    ? new ArtemisReceiveRoute(IntakeRoute.ROUTE_ID,
                            Fields.of(Attr.INTAKE, INTAKE), receive, service,
                            new IntakeHealth(INTAKE, System::nanoTime),
                            Duration.ofMillis(50), Duration.ofMillis(200))
                    : new CamelReceiveRoute(IntakeRoute.ROUTE_ID, receive,
                            service, Duration.ofMillis(50),
                            Duration.ofMillis(200));
            camel.addRoutes(route);
            camel.start();
            service.recoverAtStartup();
            camel.getRouteController().startRoute(IntakeRoute.ROUTE_ID);
        }

        /** Stops the node, as a crash or shutdown would. */
        @Override
        public void close()
        {
            camel.stop();
        }
    }
}
