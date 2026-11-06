/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.azure.core.util.BinaryData;
import com.azure.messaging.eventhubs.EventData;
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
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.google.pubsub.GooglePubsubComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.ComponentCustomizer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.util.unit.DataSize;
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
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Broker health against real brokers (design §5.3.16): each Camel
 * consumer's broker is cut off through Toxiproxy, and its health must say
 * so, with the reason, then say it is back once it is, and take messages
 * again. Each broker is also broken its own way: a refused RabbitMQ login,
 * a deleted RabbitMQ or SQS queue, and a deleted Pub/Sub subscription,
 * after which Camel's consumer gives up for good until the route is
 * restarted.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class BrokerHealthIT
{

    /** The Toxiproxy image. */
    static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0";

    /** The port the proxy listens on. */
    static final int PROXY_PORT = 8666;

    /** The receiving route's ID. */
    static final String ROUTE = "receive";

    /** How often the brokers are checked. */
    static final Duration CHECK = Duration.ofSeconds(2);

    /** The longest to wait for the health or a message. */
    static final Duration WAIT = Duration.ofMinutes(3);

    /** Small batches, closed quickly. */
    static final ReceiveProperties.Batch BATCH = new ReceiveProperties.Batch(
            10, DataSize.ofKilobytes(64), Duration.ofMillis(200));

    /** No RabbitMQ connection. */
    static final ReceiveProperties.Rabbitmq NO_RABBIT =
            new ReceiveProperties.Rabbitmq(null, "/", null, null, false);

    /** No Artemis connection. */
    static final ReceiveProperties.Artemis NO_ARTEMIS =
            new ReceiveProperties.Artemis(null, null, null, null);

    /** No Event Hubs checkpoint store. */
    static final ReceiveProperties.Eventhubs NO_EVENTHUBS =
            new ReceiveProperties.Eventhubs(null, null, null);

    /** Sends one message to the broker, directly. */
    interface Sender
    {

        /**
         * Sends.
         *
         * @param body the body
         * @throws Exception if it can't
         */
        void send(String body) throws Exception;
    }

    /** Something done to the broker. */
    interface Action
    {

        /**
         * Does it.
         *
         * @throws Exception if it can't
         */
        void run() throws Exception;
    }

    /**
     * A broker-specific breakage.
     *
     * @param brk      breaks the broker
     * @param expected the reason the health must give
     * @param heal     heals it
     */
    record Breakage(Action brk, BrokerProblem expected, Action heal)
    {
    }

    /** Bodies received. */
    final Set<String> received = ConcurrentHashMap.newKeySet();

    /**
     * Kafka, by its admin client, through an extra listener on the proxy.
     *
     * @throws Exception if the test fails
     */
    @Test
    void kafka() throws Exception
    {
        try (Network network = Network.newNetwork();
                ToxiproxyContainer toxiproxy = new ToxiproxyContainer(
                        TOXIPROXY_IMAGE).withNetwork(network))
        {
            toxiproxy.start();
            String proxied = proxied(toxiproxy);
            try (KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.2")
                    .withNetwork(network).withNetworkAliases("kafka")
                    .withListener("kafka:19092", () -> proxied))
            {
                kafka.start();
                Proxy proxy = proxy(toxiproxy, "kafka:19092");
                Properties config = new Properties();
                config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        kafka.getBootstrapServers());
                try (KafkaProducer<String, byte[]> producer =
                        new KafkaProducer<>(config, new StringSerializer(),
                                new ByteArraySerializer()))
                {
                    producer.send(new ProducerRecord<>("hl7",
                            "m-0".getBytes(UTF_8))).get();
                    run(settings("kafka", "kafka:hl7?brokers=" + proxied
                            + "&groupId=landing", NO_RABBIT, NO_EVENTHUBS),
                            camel ->
                            {
                            }, null, proxy,
                            body -> producer.send(new ProducerRecord<>("hl7",
                                    body.getBytes(UTF_8))).get(), List.of());
                }
            }
        }
    }

    /**
     * SQS on ElasticMQ, by its poll strategy.
     *
     * @throws Exception if the test fails
     */
    @Test
    void sqs() throws Exception
    {
        try (Network network = Network.newNetwork();
                ToxiproxyContainer toxiproxy = new ToxiproxyContainer(
                        TOXIPROXY_IMAGE).withNetwork(network);
                GenericContainer<?> elasticmq = new GenericContainer<>(
                        "softwaremill/elasticmq-native:1.7.1")
                        .withExposedPorts(9324).withNetwork(network)
                        .withNetworkAliases("elasticmq"))
        {
            toxiproxy.start();
            elasticmq.start();
            Proxy proxy = proxy(toxiproxy, "elasticmq:9324");
            try (SqsClient sqs = SqsClient.builder()
                    .endpointOverride(URI.create("http://"
                            + elasticmq.getHost() + ":"
                            + elasticmq.getMappedPort(9324)))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("x", "x")))
                    .build())
            {
                String[] queue = {sqs.createQueue(r -> r.queueName("hl7"))
                        .queueUrl()};
                run(settings("sqs", "aws2-sqs:hl7?overrideEndpoint=true"
                        + "&uriEndpointOverride=http://" + proxied(toxiproxy)
                        + "&region=us-east-1&accessKey={{sqs.key}}"
                        + "&secretKey={{sqs.key}}&autoCreateQueue=false"
                        + "&visibilityTimeout=10&waitTimeSeconds=1"
                        + "&delay=200", NO_RABBIT, NO_EVENTHUBS),
                        camel -> placeholder(camel, "sqs.key", "x"), null,
                        proxy, body -> sqs.sendMessage(r -> r
                                .queueUrl(queue[0]).messageBody(body)),
                        List.of(new Breakage(
                                () -> sqs.deleteQueue(r -> r
                                        .queueUrl(queue[0])),
                                BrokerProblem.NOT_FOUND,
                                () -> queue[0] = sqs.createQueue(r -> r
                                        .queueName("hl7")).queueUrl())));
            }
        }
    }

    /**
     * RabbitMQ, by its connection listener and queue check.
     *
     * @throws Exception if the test fails
     */
    @Test
    void rabbitmq() throws Exception
    {
        try (Network network = Network.newNetwork();
                ToxiproxyContainer toxiproxy = new ToxiproxyContainer(
                        TOXIPROXY_IMAGE).withNetwork(network);
                RabbitMQContainer rabbit = new RabbitMQContainer(
                        "rabbitmq:4.2.9-management-alpine")
                        .withNetwork(network).withNetworkAliases("rabbit"))
        {
            toxiproxy.start();
            rabbit.start();
            Proxy proxy = proxy(toxiproxy, "rabbit:5672");
            String user = rabbit.getAdminUsername();
            String password = rabbit.getAdminPassword();
            CachingConnectionFactory direct = new CachingConnectionFactory();
            direct.setAddresses(rabbit.getHost() + ":" + rabbit.getAmqpPort());
            direct.setUsername(user);
            direct.setPassword(password);
            CachingConnectionFactory consumer = new CachingConnectionFactory();
            consumer.setAddresses(proxied(toxiproxy));
            consumer.setUsername(user);
            consumer.setPassword(password);
            try
            {
                RabbitAdmin admin = new RabbitAdmin(direct);
                admin.declareQueue(new Queue("hl7", true));
                RabbitTemplate template = new RabbitTemplate(direct);
                run(settings("rabbitmq", "spring-rabbitmq:default?queues=hl7",
                        new ReceiveProperties.Rabbitmq(proxied(toxiproxy), "/",
                                user, password, false), NO_EVENTHUBS),
                        camel -> camel.getRegistry().bind(
                                ReceiveSettings.RABBITMQ_BEAN, consumer),
                        consumer, proxy, body -> template.send("", "hl7",
                                new Message(body.getBytes(UTF_8))),
                        List.of(new Breakage(() ->
                        {
                            rabbit.execInContainer("rabbitmqctl",
                                    "change_password", user, "wrong");
                            proxy.disable();
                            Thread.sleep(2000);
                            proxy.enable();
                        }, BrokerProblem.AUTH, () -> rabbit.execInContainer(
                                "rabbitmqctl", "change_password", user,
                                password)),
                                new Breakage(() -> admin.deleteQueue("hl7"),
                                        BrokerProblem.NOT_FOUND,
                                        () -> admin.declareQueue(
                                                new Queue("hl7", true)))));
            }
            finally
            {
                consumer.destroy();
                direct.destroy();
            }
        }
    }

    /**
     * Pub/Sub, on the emulator, by its subscription check; a deleted
     * subscription makes the consumer give up, and the route is restarted
     * once it is back.
     *
     * @throws Exception if the test fails
     */
    @Test
    void pubsub() throws Exception
    {
        try (Network network = Network.newNetwork();
                ToxiproxyContainer toxiproxy = new ToxiproxyContainer(
                        TOXIPROXY_IMAGE).withNetwork(network);
                PubSubEmulatorContainer emulator = new PubSubEmulatorContainer(
                        "gcr.io/google.com/cloudsdktool/google-cloud-cli:"
                                + "590.0.0-emulators")
                        .withNetwork(network).withNetworkAliases("pubsub"))
        {
            toxiproxy.start();
            emulator.start();
            Proxy proxy = proxy(toxiproxy, "pubsub:8085");
            ManagedChannel channel = ManagedChannelBuilder.forTarget(
                    emulator.getEmulatorEndpoint()).usePlaintext().build();
            TransportChannelProvider transport =
                    FixedTransportChannelProvider.create(
                            GrpcTransportChannel.create(channel));
            CredentialsProvider none = NoCredentialsProvider.create();
            TopicName topic = TopicName.of("sdh-it", "hl7");
            SubscriptionName name =
                    SubscriptionName.of("sdh-it", "hl7-landing");
            Subscription subscription = Subscription.newBuilder()
                    .setName(name.toString()).setTopic(topic.toString())
                    .setAckDeadlineSeconds(10).build();
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
                subscriptions.createSubscription(subscription);
                Publisher publisher = Publisher.newBuilder(topic)
                        .setChannelProvider(transport)
                        .setCredentialsProvider(none).build();
                String endpoint = proxied(toxiproxy);
                try
                {
                    run(settings("pubsub", "google-pubsub:sdh-it:hl7-landing",
                            NO_RABBIT, NO_EVENTHUBS),
                            camel -> camel.getRegistry().bind("pubsubSettings",
                                    ComponentCustomizer.forType(
                                            GooglePubsubComponent.class,
                                            component ->
                                            {
                                                component.setEndpoint(
                                                        endpoint);
                                                component.setAuthenticate(
                                                        false);
                                            })),
                            null, proxy, body -> publisher.publish(
                                    PubsubMessage.newBuilder().setData(
                                            ByteString.copyFromUtf8(body))
                                            .build()).get(),
                            List.of(new Breakage(
                                    () -> subscriptions.deleteSubscription(
                                            name),
                                    BrokerProblem.NOT_FOUND,
                                    () -> subscriptions.createSubscription(
                                            subscription))));
                }
                finally
                {
                    publisher.shutdown();
                }
            }
            finally
            {
                channel.shutdownNow();
            }
        }
    }

    /**
     * Service Bus, on the emulator, by its peek check.
     *
     * @throws Exception if the test fails
     */
    @Test
    void servicebus() throws Exception
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
                ToxiproxyContainer toxiproxy = new ToxiproxyContainer(
                        TOXIPROXY_IMAGE).withNetwork(network);
                MSSQLServerContainer sql = new MSSQLServerContainer(
                        "mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04")
                        .acceptLicense().withNetwork(network);
                ServiceBusEmulatorContainer servicebus =
                        new ServiceBusEmulatorContainer("mcr.microsoft.com/"
                                + "azure-messaging/servicebus-emulator:2.0.0")
                                .acceptLicense()
                                .withConfig(Transferable.of(config))
                                .withMsSqlServerContainer(sql)
                                .withNetwork(network)
                                .withNetworkAliases("servicebus"))
        {
            toxiproxy.start();
            servicebus.start();
            Proxy proxy = proxy(toxiproxy, "servicebus:5672");
            String direct = servicebus.getConnectionString();
            String proxied = viaProxy(direct, toxiproxy);
            try (ServiceBusSenderClient sender = new ServiceBusClientBuilder()
                    .connectionString(direct).sender().queueName("hl7")
                    .buildClient())
            {
                run(settings("servicebus",
                        "azure-servicebus:hl7?serviceBusType=queue"
                                + "&connectionString=RAW({{sb.cs}})",
                        NO_RABBIT, NO_EVENTHUBS),
                        camel -> placeholder(camel, "sb.cs", proxied), null,
                        proxy, body -> sender.sendMessage(
                                new ServiceBusMessage(body)),
                        List.of());
            }
        }
    }

    /**
     * Event Hubs, on the emulator, by its properties and checkpoint
     * container check.
     *
     * @throws Exception if the test fails
     */
    @Test
    void eventhubs() throws Exception
    {
        String config = """
                {"UserConfig": {"NamespaceConfig": [{"Type": "EventHub",
                  "Name": "emulatorns1", "Entities": [{"Name": "hl7",
                    "PartitionCount": "2",
                    "ConsumerGroups": [{"Name": "landing"}]}]}],
                  "LoggingConfig": {"Type": "File"}}}""";
        try (Network network = Network.newNetwork();
                ToxiproxyContainer toxiproxy = new ToxiproxyContainer(
                        TOXIPROXY_IMAGE).withNetwork(network);
                AzuriteContainer azurite = new AzuriteContainer(
                        "mcr.microsoft.com/azure-storage/azurite:3.35.0")
                        .withNetwork(network);
                EventHubsEmulatorContainer eventhubs =
                        new EventHubsEmulatorContainer("mcr.microsoft.com/"
                                + "azure-messaging/eventhubs-emulator:2.2.0")
                                .acceptLicense()
                                .withConfig(Transferable.of(config))
                                .withAzuriteContainer(azurite)
                                .withNetwork(network)
                                .withNetworkAliases("eventhubs"))
        {
            toxiproxy.start();
            eventhubs.start();
            Proxy proxy = proxy(toxiproxy, "eventhubs:5672");
            String base = eventhubs.getConnectionString();
            String direct = base + (base.endsWith(";") ? "" : ";")
                    + "EntityPath=hl7";
            String storage = azurite.getConnectionString();
            new BlobContainerClientBuilder().connectionString(storage)
                    .containerName("checkpoints").buildClient().create();
            ReceiveProperties.Eventhubs checkpoints =
                    new ReceiveProperties.Eventhubs(storage, null,
                            "checkpoints");
            try (EventHubProducerClient producer = new EventHubClientBuilder()
                    .connectionString(direct).buildProducerClient())
            {
                String proxied = viaProxy(direct, toxiproxy);
                run(settings("eventhubs",
                        "azure-eventhubs:?consumerGroupName=landing"
                                + "&connectionString=RAW({{eh.cs}})",
                        NO_RABBIT, checkpoints),
                        camel ->
                        {
                            placeholder(camel, "eh.cs", proxied);
                            EventHubsSupport.bind(camel, checkpoints);
                        }, null, proxy,
                        body -> producer.send(List.of(new EventData(
                                BinaryData.fromString(body)))),
                        List.of());
            }
        }
    }

    /**
     * A broker's settings, checked every {@link #CHECK}.
     *
     * @param type      the broker
     * @param uri       its URI
     * @param rabbitmq  its RabbitMQ connection, or none
     * @param eventhubs its checkpoint store, or none
     * @return the settings
     */
    static ReceiveSettings settings(String type, String uri,
            ReceiveProperties.Rabbitmq rabbitmq,
            ReceiveProperties.Eventhubs eventhubs)
    {
        return ReceiveSettings.of(new ReceiveProperties(type, uri, BATCH,
                CHECK, rabbitmq, NO_ARTEMIS, eventhubs), "sdh.test.broker",
                Set.of());
    }

    /**
     * Runs a node's route and broker watch, cuts the broker off and heals
     * it, then breaks it each other way and heals it, checking the
     * broker's health and that messages arrive after each.
     *
     * @param settings  the broker
     * @param setup     prepares the Camel context
     * @param factory   the RabbitMQ connection factory, or null
     * @param proxy     the proxy in front of the broker
     * @param sender    sends a message, directly
     * @param breakages the broker's own breakages
     * @throws Exception if the test fails
     */
    void run(ReceiveSettings settings, Consumer<CamelContext> setup,
            CachingConnectionFactory factory, Proxy proxy, Sender sender,
            List<Breakage> breakages) throws Exception
    {
        RecordingHealth health = new RecordingHealth();
        try (DefaultCamelContext camel = new DefaultCamelContext();
                BrokerWatch watch = new BrokerWatch(ROUTE, settings, health,
                        camel))
        {
            setup.accept(camel);
            watch.bind(factory);
            camel.addRoutes(route(settings));
            camel.start();
            camel.getRouteController().startRoute(ROUTE);
            watch.start();
            receives(sender, "m-1");
            assertThat(health.reachable()).isTrue();

            proxy.disable();
            await(() -> health.problem() == BrokerProblem.NETWORK,
                    "the outage is reported");
            proxy.enable();
            await(health::reachable, "the broker is back");
            receives(sender, "m-2");

            int sent = 2;
            for (Breakage breakage : breakages)
            {
                breakage.brk().run();
                await(() -> health.problem() == breakage.expected(),
                        breakage.expected() + " is reported");
                breakage.heal().run();
                await(health::reachable, "the broker is back after "
                        + breakage.expected());
                receives(sender, "m-" + ++sent);
            }
        }
    }

    /**
     * Sends a message and waits for the route to take it.
     *
     * @param sender sends it
     * @param body   its body
     * @throws Exception if it doesn't arrive
     */
    void receives(Sender sender, String body) throws Exception
    {
        sender.send(body);
        await(() -> received.contains(body), body + " arrives");
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

    /**
     * The receiving route, handling each batch by noting its bodies.
     *
     * @param settings the broker
     * @return the route
     */
    RouteBuilder route(ReceiveSettings settings)
    {
        return new CamelReceiveRoute(ROUTE, settings, messages ->
        {
            for (BrokerMessage message : messages)
            {
                received.add(new String(message.body(), UTF_8));
            }
            return BatchHandler.Outcome.DONE;
        }, Duration.ofMillis(50), Duration.ofMillis(200));
    }

    /**
     * The proxy's address, from the host.
     *
     * @param toxiproxy the proxy
     * @return host and port
     */
    static String proxied(ToxiproxyContainer toxiproxy)
    {
        return toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(PROXY_PORT);
    }

    /**
     * A connection string pointed at the proxy.
     *
     * @param connection the connection string
     * @param toxiproxy  the proxy
     * @return the connection string
     */
    static String viaProxy(String connection, ToxiproxyContainer toxiproxy)
    {
        return connection.replaceFirst("Endpoint=sb://[^;]+",
                "Endpoint=sb://" + proxied(toxiproxy));
    }

    /**
     * Makes the proxy.
     *
     * @param toxiproxy the container
     * @param upstream  the broker's address on the network
     * @return the proxy
     * @throws IOException if it can't be made
     */
    static Proxy proxy(ToxiproxyContainer toxiproxy, String upstream)
            throws IOException
    {
        return new ToxiproxyClient(toxiproxy.getHost(),
                toxiproxy.getControlPort()).createProxy("broker",
                        "0.0.0.0:" + PROXY_PORT, upstream);
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
}
