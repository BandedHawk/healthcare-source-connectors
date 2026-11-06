/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.US_ASCII;
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
import com.sdh.healthcare.connector.broker.receive.ArtemisReceiveRoute;
import com.sdh.healthcare.connector.broker.receive.BrokerHealth;
import com.sdh.healthcare.connector.broker.receive.BrokerKind;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.broker.receive.BrokerProblem;
import com.sdh.healthcare.connector.broker.receive.BrokerWatch;
import com.sdh.healthcare.connector.broker.receive.CamelReceiveRoute;
import com.sdh.healthcare.connector.broker.receive.EventHubsSupport;
import com.sdh.healthcare.connector.broker.receive.ReceiveProperties;
import com.sdh.healthcare.connector.broker.receive.ReceiveSettings;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.google.pubsub.GooglePubsubComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
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
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * The stage worker on every broker (STAGES.md §11, step 3), with a test
 * processor. An item's records and total are put on the stage's input.
 * One worker claims a batch and stops part-way, its commit failing, as a
 * pod would that is killed; two more then run at once, wait out its claims
 * and take them over, and between them store every record exactly once and
 * complete the item. The output is kept in memory: sending on each broker
 * is tested by {@code file-connector}'s broker tests.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StageWorkerIT
{

    /** How many records the item has. */
    static final int RECORDS = 30;

    /** The receiving route's ID. */
    static final String ROUTE = "stage-input";

    /** Small batches, closed quickly. */
    static final ReceiveProperties.Batch BATCH = new ReceiveProperties.Batch(
            10, DataSize.ofKilobytes(256), Duration.ofMillis(500));

    /** How often a broker is checked. */
    static final Duration CHECK = Duration.ofSeconds(30);

    /** No RabbitMQ connection. */
    static final ReceiveProperties.Rabbitmq NO_RABBIT =
            new ReceiveProperties.Rabbitmq(null, "/", null, null, false);

    /** No Artemis connection. */
    static final ReceiveProperties.Artemis NO_ARTEMIS =
            new ReceiveProperties.Artemis(null, null, null, null);

    /** No Event Hubs checkpoint store. */
    static final ReceiveProperties.Eventhubs NO_EVENTHUBS =
            new ReceiveProperties.Eventhubs(null, null, null);

    /** Claims short enough that the stopped worker's soon expire. */
    static final StageWorker.Settings SETTINGS = new StageWorker.Settings(4,
            Duration.ofSeconds(3), Duration.ofSeconds(1),
            Duration.ofMillis(200), Duration.ofSeconds(10));

    /** The longest to wait for the workers. */
    static final Duration WAIT = Duration.ofMinutes(4);

    /** A broker's health, which these tests don't look at. */
    static final BrokerHealth HEALTH = new BrokerHealth()
    {
        @Override
        public void answered()
        {
        }

        @Override
        public void unreachable(BrokerProblem reason)
        {
        }
    };

    /** Every record committed, over all workers. */
    final List<String> committed =
            Collections.synchronizedList(new ArrayList<>());

    /** The stage's output, shared by all workers. */
    final StageTestSupport.RecordingPublisher output =
            new StageTestSupport.RecordingPublisher();

    /** The item the records are from. */
    final UUID item = UUID.randomUUID();

    /** The registry's schema for this test. */
    String schema;

    /** Puts the item's messages on a broker's input, directly. */
    interface Sender
    {

        /**
         * Sends.
         *
         * @param bodies the sealed messages
         * @throws Exception if it can't
         */
        void send(List<byte[]> bodies) throws Exception;
    }

    /** Creates the tests; JUnit runs each test on a new instance. */
    StageWorkerIT()
    {
    }

    /** Gives each test its own registry. */
    @BeforeEach
    void tables()
    {
        schema = StageTestSupport.freshSchema();
    }

    /**
     * The item's messages: its records, and its total.
     *
     * @return the sealed bodies
     */
    List<byte[]> bodies()
    {
        List<byte[]> bodies = new ArrayList<>();
        for (int seq = 0; seq < RECORDS; seq++)
        {
            bodies.add(StageTestSupport.record(item, seq).body());
        }
        BrokerMessage total = StageTestSupport.total(item, RECORDS);
        bodies.add(total.body());
        return bodies;
    }

    /**
     * Checked input settings.
     *
     * @param type      the broker
     * @param uri       the consumer URI
     * @param rabbitmq  the RabbitMQ connection, or none
     * @param artemis   the Artemis connection, or none
     * @param eventhubs the checkpoint store, or none
     * @return the settings
     */
    static ReceiveSettings input(String type, String uri,
            ReceiveProperties.Rabbitmq rabbitmq,
            ReceiveProperties.Artemis artemis,
            ReceiveProperties.Eventhubs eventhubs)
    {
        return ReceiveSettings.of(new ReceiveProperties(type, uri, BATCH,
                CHECK, rabbitmq, artemis, eventhubs), "sdh.stage.input",
                java.util.Set.of());
    }

    /**
     * Kafka, on two partitions, the two workers in one group.
     *
     * @throws Exception if the test fails
     */
    @Test
    void kafka() throws Exception
    {
        try (KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.0"))
        {
            kafka.start();
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                    kafka.getBootstrapServers());
            try (Admin admin = Admin.create(config))
            {
                admin.createTopics(List.of(new NewTopic("records", 2,
                        (short) 1))).all().get();
            }
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    kafka.getBootstrapServers());
            run(input("kafka", "kafka:records?brokers="
                    + kafka.getBootstrapServers() + "&groupId=stage",
                    NO_RABBIT, NO_ARTEMIS, NO_EVENTHUBS), camel ->
            {
            }, bodies ->
            {
                try (KafkaProducer<String, byte[]> producer =
                        new KafkaProducer<>(config, new StringSerializer(),
                                new ByteArraySerializer()))
                {
                    int key = 0;
                    for (byte[] body : bodies)
                    {
                        producer.send(new ProducerRecord<>("records",
                                "k" + key++, body)).get();
                    }
                }
            });
        }
    }

    /**
     * SQS, on ElasticMQ: a message not deleted comes back after its
     * visibility timeout.
     *
     * @throws Exception if the test fails
     */
    @Test
    void sqs() throws Exception
    {
        try (GenericContainer<?> elasticmq = new GenericContainer<>(
                "softwaremill/elasticmq-native:1.7.1").withExposedPorts(9324))
        {
            elasticmq.start();
            String endpoint = "http://" + elasticmq.getHost() + ":"
                    + elasticmq.getMappedPort(9324);
            SqsClient sqs = SqsClient.builder()
                    .endpointOverride(URI.create(endpoint))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("x", "x")))
                    .build();
            try (sqs)
            {
                String queue = sqs.createQueue(request -> request
                        .queueName("records").attributes(Map.of(
                                QueueAttributeName.VISIBILITY_TIMEOUT, "2")))
                        .queueUrl();
                run(input("sqs", "aws2-sqs:records?overrideEndpoint=true"
                        + "&uriEndpointOverride=" + endpoint
                        + "&region=us-east-1"
                        + "&accessKey={{sqs.key}}&secretKey={{sqs.key}}"
                        + "&autoCreateQueue=false&visibilityTimeout=2"
                        + "&waitTimeSeconds=1&delay=100", NO_RABBIT,
                        NO_ARTEMIS, NO_EVENTHUBS),
                        camel -> placeholder(camel, "sqs.key", "x"),
                        bodies ->
                        {
                            for (byte[] body : bodies)
                            {
                                sqs.sendMessage(request -> request
                                        .queueUrl(queue).messageBody(
                                                new String(body, US_ASCII)));
                            }
                        });
            }
        }
    }

    /**
     * RabbitMQ: a message not acknowledged goes back to the queue when its
     * channel closes.
     *
     * @throws Exception if the test fails
     */
    @Test
    void rabbitmq() throws Exception
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
            CachingConnectionFactory admin = factory(connection);
            try
            {
                new RabbitAdmin(admin).declareQueue(
                        new Queue("records", true));
                RabbitTemplate template = new RabbitTemplate(admin);
                run(input("rabbitmq", "spring-rabbitmq:default?queues=records",
                        connection, NO_ARTEMIS, NO_EVENTHUBS),
                        camel -> camel.getRegistry().bind(
                                ReceiveSettings.RABBITMQ_BEAN,
                                factory(connection)),
                        bodies ->
                        {
                            for (byte[] body : bodies)
                            {
                                template.send("", "records",
                                        new Message(body));
                            }
                        });
            }
            finally
            {
                admin.destroy();
            }
        }
    }

    /**
     * Artemis, by the JMS client: a dropped connection returns what it
     * hadn't acknowledged.
     *
     * @throws Exception if the test fails
     */
    @Test
    void artemis() throws Exception
    {
        try (ArtemisContainer artemis = new ArtemisContainer(
                "apache/activemq-artemis:2.44.0-alpine"))
        {
            artemis.start();
            ReceiveProperties.Artemis connection =
                    new ReceiveProperties.Artemis(artemis.getBrokerUrl(),
                            artemis.getUser(), artemis.getPassword(),
                            "records");
            run(input("artemis", null, NO_RABBIT, connection, NO_EVENTHUBS),
                    camel ->
                    {
                    }, bodies ->
                    {
                        try (ActiveMQConnectionFactory factory =
                                new ActiveMQConnectionFactory(
                                        connection.brokerUrl(),
                                        connection.username(),
                                        connection.password());
                                Connection jms = factory.createConnection())
                        {
                            Session session = jms.createSession(false,
                                    Session.AUTO_ACKNOWLEDGE);
                            MessageProducer producer = session.createProducer(
                                    session.createQueue("records"));
                            for (byte[] body : bodies)
                            {
                                producer.send(session.createTextMessage(
                                        new String(body, US_ASCII)));
                            }
                        }
                    });
        }
    }

    /**
     * Pub/Sub, on its emulator: a message held back is handed back when its
     * route stops.
     *
     * @throws Exception if the test fails
     */
    @Test
    void pubsub() throws Exception
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
                TopicName topic = TopicName.of("sdh-it", "records");
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
                            .setName(SubscriptionName.of("sdh-it",
                                    "records-stage").toString())
                            .setTopic(topic.toString())
                            .setAckDeadlineSeconds(10).build());
                }
                run(input("pubsub", "google-pubsub:sdh-it:records-stage",
                        NO_RABBIT, NO_ARTEMIS, NO_EVENTHUBS), camel ->
                {
                    // The subscriber takes its endpoint from the component.
                    GooglePubsubComponent component =
                            new GooglePubsubComponent();
                    component.setEndpoint(endpoint);
                    component.setAuthenticate(false);
                    camel.addComponent("google-pubsub", component);
                }, bodies ->
                {
                    Publisher publisher = Publisher.newBuilder(topic)
                            .setChannelProvider(transport)
                            .setCredentialsProvider(none).build();
                    for (byte[] body : bodies)
                    {
                        publisher.publish(PubsubMessage.newBuilder()
                                .setData(ByteString.copyFrom(body)).build())
                                .get();
                    }
                    publisher.shutdown();
                });
            }
            finally
            {
                channel.shutdownNow();
            }
        }
    }

    /**
     * Service Bus, on its emulator: a message locked by a stopped worker
     * comes again once its lock expires.
     *
     * @throws Exception if the test fails
     */
    @Test
    void servicebus() throws Exception
    {
        String config = """
                {"UserConfig": {"Namespaces": [{"Name": "sbemulatorns",
                  "Queues": [{"Name": "records", "Properties": {
                    "DeadLetteringOnMessageExpiration": false,
                    "DefaultMessageTimeToLive": "PT1H",
                    "DuplicateDetectionHistoryTimeWindow": "PT20S",
                    "ForwardDeadLetteredMessagesTo": "", "ForwardTo": "",
                    "LockDuration": "PT30S", "MaxDeliveryCount": 10,
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
            run(input("servicebus", "azure-servicebus:records"
                    + "?serviceBusType=queue&connectionString=RAW({{sb.cs}})",
                    NO_RABBIT, NO_ARTEMIS, NO_EVENTHUBS),
                    camel -> placeholder(camel, "sb.cs", connection),
                    bodies ->
                    {
                        try (ServiceBusSenderClient sender =
                                new ServiceBusClientBuilder()
                                        .connectionString(connection)
                                        .sender().queueName("records")
                                        .buildClient())
                        {
                            for (byte[] body : bodies)
                            {
                                sender.sendMessage(
                                        new ServiceBusMessage(body));
                            }
                        }
                    });
        }
    }

    /**
     * Event Hubs, on its emulator, with two partitions shared between the
     * two workers through the blob checkpoint store (on Azurite).
     *
     * @throws Exception if the test fails
     */
    @Test
    void eventhubs() throws Exception
    {
        String config = """
                {"UserConfig": {"NamespaceConfig": [{"Type": "EventHub",
                  "Name": "emulatorns1", "Entities": [{"Name": "records",
                    "PartitionCount": "2",
                    "ConsumerGroups": [{"Name": "stage"}]}]}],
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
                    + "EntityPath=records";
            String storage = azurite.getConnectionString();
            new BlobContainerClientBuilder().connectionString(storage)
                    .containerName("checkpoints").buildClient().create();
            ReceiveProperties.Eventhubs checkpoints =
                    new ReceiveProperties.Eventhubs(storage, null,
                            "checkpoints");
            run(input("eventhubs", "azure-eventhubs:?consumerGroupName=stage"
                    + "&connectionString=RAW({{eh.cs}})", NO_RABBIT,
                    NO_ARTEMIS, checkpoints), camel ->
            {
                placeholder(camel, "eh.cs", connection);
                EventHubsSupport.bind(camel, checkpoints);
            }, bodies ->
            {
                try (EventHubProducerClient producer =
                        new EventHubClientBuilder()
                                .connectionString(connection)
                                .buildProducerClient())
                {
                    EventDataBatch batch = producer.createBatch();
                    for (byte[] body : bodies)
                    {
                        assertThat(batch.tryAdd(new EventData(body)))
                                .isTrue();
                    }
                    producer.send(batch);
                }
            });
        }
    }

    /**
     * Runs the scenario on a broker: one worker stops part-way, then two
     * run at once and finish the item.
     *
     * @param settings the input
     * @param setup    prepares each worker's Camel context
     * @param sender   puts the item's messages on the input
     * @throws Exception if the test fails
     */
    void run(ReceiveSettings settings, Consumer<CamelContext> setup,
            Sender sender) throws Exception
    {
        sender.send(bodies());

        StageTestSupport.TestProcessor stopping = processor();
        stopping.commitFails.set(true);
        try (Node first = new Node("pod-a", settings, setup, stopping))
        {
            await(() -> stopping.commits.get() > 0,
                    "the first worker claims a batch");
        }

        StageTestSupport.TestProcessor second = processor();
        StageTestSupport.TestProcessor third = processor();
        try (Node other = new Node("pod-b", settings, setup, second);
                Node another = new Node("pod-c", settings, setup, third))
        {
            await(() -> !completions().isEmpty(), "the item completes");
        }
        assertThat(committed).hasSize(RECORDS).doesNotHaveDuplicates();
        assertThat(completions()).allSatisfy(done ->
        {
            assertThat(done.recordCount()).isEqualTo(RECORDS);
            assertThat(done.failedRecordCount()).isEqualTo(RECORDS / 5);
        });
        // The first worker's claims were taken over, as second attempts.
        List<Integer> attempts = new ArrayList<>();
        attempts.addAll(second.attempts.values());
        attempts.addAll(third.attempts.values());
        assertThat(attempts).contains(2);
    }

    /**
     * The item-complete messages sent.
     *
     * @return the messages
     */
    List<FileCompleted> completions()
    {
        return output.opened().stream()
                .filter(FileCompleted.class::isInstance)
                .map(FileCompleted.class::cast).toList();
    }

    /**
     * A processor at version 1, committing to this test's list.
     *
     * @return the processor
     */
    StageTestSupport.TestProcessor processor()
    {
        return new StageTestSupport.TestProcessor(1, committed);
    }

    /** A worker in a pod of its own: its Camel context and route. */
    final class Node implements AutoCloseable
    {

        /** The pod's Camel context. */
        private final DefaultCamelContext camel = new DefaultCamelContext();

        /** The pod's worker. */
        private final StageWorker<String> worker;

        /**
         * Starts a pod.
         *
         * @param owner     its name in the claims
         * @param settings  the input
         * @param setup     prepares its Camel context
         * @param processor its processor
         * @throws Exception if it can't start
         */
        Node(String owner, ReceiveSettings settings,
                Consumer<CamelContext> setup,
                StageTestSupport.TestProcessor processor) throws Exception
        {
            worker = new StageWorker<>(processor,
                    StageTestSupport.registry(schema, owner),
                    StageTestSupport.stageOpener(),
                    StageTestSupport.stageSealer(), output,
                    new StageTestSupport.RecordingPublisher(), SETTINGS,
                    Clock.systemUTC());
            worker.start();
            setup.accept(camel);
            if (settings.kind() == BrokerKind.SQS)
            {
                new BrokerWatch(ROUTE, settings, HEALTH, camel).bind(null);
            }
            RouteBuilder route = settings.kind() == BrokerKind.ARTEMIS
                    ? new ArtemisReceiveRoute(ROUTE,
                            Fields.of(Attr.STAGE, "test"), settings, worker,
                            HEALTH, Duration.ofMillis(50),
                            Duration.ofMillis(200))
                    : new CamelReceiveRoute(ROUTE, settings, worker,
                            Duration.ofMillis(50), Duration.ofMillis(200));
            camel.addRoutes(route);
            camel.start();
            camel.getRouteController().startRoute(ROUTE);
        }

        /**
         * Stops the pod: its route, then its worker, whose claims lapse.
         *
         * @throws java.io.IOException if the Camel context can't close
         */
        @Override
        public void close() throws java.io.IOException
        {
            camel.close();
            worker.close();
        }
    }

    /**
     * A RabbitMQ connection factory.
     *
     * @param connection the connection
     * @return the factory
     */
    static CachingConnectionFactory factory(
            ReceiveProperties.Rabbitmq connection)
    {
        CachingConnectionFactory factory = new CachingConnectionFactory();
        factory.setAddresses(connection.addresses());
        factory.setUsername(connection.username());
        factory.setPassword(connection.password());
        return factory;
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
     * Waits for a condition.
     *
     * @param condition the condition
     * @param what      what it means, for the failure
     * @throws InterruptedException if interrupted
     */
    static void await(BooleanSupplier condition, String what)
            throws InterruptedException
    {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline)
        {
            Thread.sleep(200);
        }
        assertThat(condition.getAsBoolean()).as(what).isTrue();
    }
}
