/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

/**
 * Tests for {@link ReceiveSettings}: the options each broker's consumer
 * needs, and the settings refused.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ReceiveSettingsTest
{

    /** Where the settings are configured, as errors name them. */
    static final String PREFIX = "sdh.stage.input";

    /** The attribute the tests ask SQS for. */
    static final String ATTRIBUTE = "sdh_replaces";

    /** Small batch limits. */
    static final ReceiveProperties.Batch BATCH = new ReceiveProperties.Batch(
            50, DataSize.ofMegabytes(1), Duration.ofSeconds(2));

    /** A RabbitMQ connection. */
    static final ReceiveProperties.Rabbitmq RABBIT =
            new ReceiveProperties.Rabbitmq("rabbit:5672", "/", "u", "p",
                    false);

    /** How often the broker is checked. */
    static final Duration CHECK = Duration.ofSeconds(30);

    /** No Artemis connection. */
    static final ReceiveProperties.Artemis NO_ARTEMIS =
            new ReceiveProperties.Artemis(null, null, null, null);

    /** No Event Hubs checkpoint store. */
    static final ReceiveProperties.Eventhubs NO_EVENTHUBS =
            new ReceiveProperties.Eventhubs(null, null, null);

    /**
     * Broker settings as configured.
     *
     * @param type the broker
     * @param uri  the consumer URI
     * @return the settings
     */
    static ReceiveProperties broker(String type, String uri)
    {
        return new ReceiveProperties(type, uri, BATCH, CHECK, RABBIT,
                NO_ARTEMIS, NO_EVENTHUBS);
    }

    /**
     * Checks settings, asking for {@link #ATTRIBUTE}.
     *
     * @param broker the settings as configured
     * @return the checked settings
     */
    static ReceiveSettings of(ReceiveProperties broker)
    {
        return ReceiveSettings.of(broker, PREFIX, Set.of(ATTRIBUTE));
    }

    /**
     * Kafka gets batching, manual commits and the batch limits, and keeps
     * the options it was given.
     */
    @Test
    void addsTheOptionsKafkaNeeds()
    {
        ReceiveSettings settings = of(broker("kafka",
                "kafka:hl7?brokers=k:9092&groupId=landing"));
        assertThat(settings.kind()).isEqualTo(BrokerKind.KAFKA);
        assertThat(settings.uri()).startsWith("kafka:hl7?")
                .contains("brokers=k:9092", "groupId=landing",
                        "batching=true", "allowManualCommit=true",
                        "autoCommitEnable=false", "maxPollRecords=50",
                        "batchingIntervalMs=2000",
                        "autoOffsetReset=earliest", "ByteArrayDeserializer");
        assertThat(settings.maxBytes()).isEqualTo(1024 * 1024);
    }

    /**
     * A Kafka default is added unless the URI sets the option, and isn't
     * added for another broker.
     */
    @Test
    void addsAKafkaDefaultUnlessSet()
    {
        assertThat(of(broker("kafka", "kafka:hl7?groupId=g"))
                .withKafkaDefault("maxPollIntervalMs", "1800000").uri())
                .contains("&maxPollIntervalMs=1800000");
        assertThat(of(broker("kafka",
                "kafka:hl7?groupId=g&maxPollIntervalMs=600000"))
                .withKafkaDefault("maxPollIntervalMs", "1800000").uri())
                .contains("maxPollIntervalMs=600000")
                .doesNotContain("1800000");
        assertThat(of(broker("sqs", "aws2-sqs:q"))
                .withKafkaDefault("maxPollIntervalMs", "1800000").uri())
                .doesNotContain("maxPollIntervalMs");
    }

    /** Kafka without a consumer group is refused. */
    @Test
    void refusesKafkaWithoutAGroup()
    {
        assertThatThrownBy(() -> of(broker("kafka",
                "kafka:hl7?brokers=k:9092")))
                .hasMessageContaining("groupId");
    }

    /** An option that would acknowledge too early is refused. */
    @Test
    void refusesOptionsThatAcknowledgeEarly()
    {
        assertThatThrownBy(() -> of(broker("kafka",
                "kafka:hl7?groupId=g&autoCommitEnable=true")))
                .hasMessageContaining("autoCommitEnable");
        assertThatThrownBy(() -> of(broker("sqs",
                "aws2-sqs:q?deleteAfterRead=false")))
                .hasMessageContaining("deleteAfterRead");
        assertThatThrownBy(() -> of(broker("rabbitmq",
                "spring-rabbitmq:ex?queues=q&acknowledgeMode=AUTO")))
                .hasMessageContaining("acknowledgeMode");
    }

    /**
     * SQS and RabbitMQ get their options, SQS the link attribute, and
     * RabbitMQ the node's factory.
     */
    @Test
    void addsTheOptionsSqsAndRabbitmqNeed()
    {
        assertThat(of(broker("sqs",
                "aws2-sqs:q?region=eu-west-1")).uri())
                .contains("region=eu-west-1", "deleteAfterRead=true",
                        "extendMessageVisibility=true",
                        "visibilityTimeout=60", "waitTimeSeconds=20",
                        "pollStrategy=#" + ReceiveSettings.POLL_STRATEGY_BEAN,
                        "messageAttributeNames=sdh_replaces");
        assertThat(of(broker("sqs",
                "aws2-sqs:q?messageAttributeNames=trace")).uri())
                .contains("messageAttributeNames=trace,sdh_replaces");
        assertThat(of(broker("sqs",
                "aws2-sqs:q?messageAttributeNames=All")).uri())
                .contains("messageAttributeNames=All")
                .doesNotContain("sdh_replaces");
        assertThat(of(broker("RabbitMQ",
                "spring-rabbitmq:ex?queues=q")).uri())
                .contains("queues=q", "acknowledgeMode=MANUAL",
                        "asyncConsumer=true",
                        "connectionFactory=#" + ReceiveSettings.RABBITMQ_BEAN,
                        "autoDeclare=false", "prefetchCount=50");
    }

    /** A secret written into the URI is refused; a placeholder is kept. */
    @Test
    void refusesSecretsInTheUri()
    {
        assertThatThrownBy(() -> of(broker("sqs",
                "aws2-sqs:q?accessKey=AKIA&secretKey=s3cret")))
                .hasMessageContaining("placeholder")
                .hasMessageNotContaining("s3cret")
                .hasMessageNotContaining("AKIA");
        assertThat(of(broker("sqs",
                "aws2-sqs:q?accessKey={{env:AK}}&secretKey=RAW({{env:SK}})"))
                .uri()).contains("{{env:AK}}", "RAW({{env:SK}})");
    }

    /**
     * A URI for another component, an unknown broker, or a missing
     * connection is refused.
     */
    @Test
    void refusesWrongOrMissingSettings()
    {
        assertThatThrownBy(() -> of(broker("kafka",
                "aws2-sqs:q"))).hasMessageContaining("kafka:");
        assertThatThrownBy(() -> of(broker("nats", "x:y")))
                .hasMessageContaining("must be");
        assertThatThrownBy(() -> of(broker(null, null)))
                .hasMessageContaining("required");
        assertThatThrownBy(() -> of(
                new ReceiveProperties("rabbitmq",
                        "spring-rabbitmq:ex?queues=q", BATCH,
                        CHECK,
                        new ReceiveProperties.Rabbitmq(null, "/", null, null,
                                false), NO_ARTEMIS, NO_EVENTHUBS)))
                .hasMessageContaining("addresses");
        assertThatThrownBy(() -> of(
                new ReceiveProperties("kafka", "kafka:t?groupId=g",
                        BATCH, Duration.ZERO, RABBIT,
                        NO_ARTEMIS, NO_EVENTHUBS)))
                .hasMessageContaining("check-interval");
    }

    /**
     * Pub/Sub acknowledges by hand, by streaming pull, and with no
     * delivery limit of Camel's own.
     */
    @Test
    void addsTheOptionsPubsubNeeds()
    {
        assertThat(of(broker("pubsub",
                "google-pubsub:proj:sub")).uri())
                .contains("ackMode=NONE", "synchronousPull=false",
                        "maxDeliveryAttempts=0",
                        "maxAckExtensionPeriod=3600");
        assertThatThrownBy(() -> of(broker("pubsub",
                "google-pubsub:proj:sub?ackMode=AUTO")))
                .hasMessageContaining("ackMode");
        assertThatThrownBy(() -> of(broker("pubsub",
                "google-pubsub:proj:sub?synchronousPull=true")))
                .hasMessageContaining("synchronousPull");
    }

    /**
     * Service Bus locks messages, without dead-lettering or sessions, and
     * keeps renewing their locks; the connection string is a placeholder.
     */
    @Test
    void addsTheOptionsServicebusNeeds()
    {
        assertThat(of(broker("servicebus",
                "azure-servicebus:hl7?connectionString=RAW({{env:SB}})"))
                .uri()).contains("connectionString=RAW({{env:SB}})",
                        "serviceBusReceiveMode=PEEK_LOCK",
                        "enableDeadLettering=false", "sessionEnabled=false",
                        "maxAutoLockRenewDuration=300000");
        assertThatThrownBy(() -> of(broker("servicebus",
                "azure-servicebus:hl7?connectionString=Endpoint=sb://x")))
                .hasMessageContaining("placeholder");
        assertThatThrownBy(() -> of(broker("servicebus",
                "azure-servicebus:hl7?enableDeadLettering=true")))
                .hasMessageContaining("enableDeadLettering");
        assertThatThrownBy(() -> of(broker("servicebus",
                "azure-servicebus:hl7?maxAutoLockRenewDuration=0")))
                .hasMessageContaining("maxAutoLockRenewDuration");
        assertThatThrownBy(() -> of(broker("servicebus",
                "azure-servicebus:hl7?processorClient=#mine")))
                .hasMessageContaining("processorClient");
        assertThat(of(broker("servicebus",
                "azure-servicebus:hl7?credentialType=AZURE_IDENTITY"
                        + "&fullyQualifiedNamespace=ns.servicebus.windows.net"))
                .uri()).contains("credentialType=AZURE_IDENTITY");
    }

    /**
     * Event Hubs uses the node's checkpoint store and start positions, and
     * needs a container and one way to reach it.
     */
    @Test
    void addsTheOptionsEventhubsNeeds()
    {
        ReceiveProperties.Eventhubs checkpoints =
                new ReceiveProperties.Eventhubs(null,
                        "https://acct.blob.core.windows.net", "cp");
        ReceiveSettings settings = of(
                new ReceiveProperties("eventhubs",
                        "azure-eventhubs:ns/hl7", BATCH, CHECK, RABBIT,
                        NO_ARTEMIS, checkpoints));
        assertThat(settings.uri()).contains(
                "checkpointStore=#" + ReceiveSettings.CHECKPOINTS_BEAN,
                "eventPosition=#" + ReceiveSettings.POSITIONS_BEAN);
        assertThatThrownBy(() -> of(
                new ReceiveProperties("eventhubs",
                        "azure-eventhubs:ns/hl7?blobAccountName=a", BATCH,
                        CHECK, RABBIT, NO_ARTEMIS,
                        checkpoints)))
                .hasMessageContaining("blobAccountName");
        assertThatThrownBy(() -> of(broker("eventhubs",
                "azure-eventhubs:ns/hl7")))
                .hasMessageContaining("checkpoint-container");
        assertThatThrownBy(() -> of(
                new ReceiveProperties("eventhubs",
                        "azure-eventhubs:ns/hl7", BATCH, CHECK, RABBIT,
                        NO_ARTEMIS,
                        new ReceiveProperties.Eventhubs("cs", "https://x",
                                "cp"))))
                .hasMessageContaining("one of");
        assertThat(new ReceiveProperties.Eventhubs("AccountKey=s3cret",
                null, "cp").toString()).doesNotContain("s3cret");
    }

    /** Artemis takes its connection, not a URI. */
    @Test
    void takesArtemisFromItsOwnSettings()
    {
        ReceiveProperties.Artemis artemis = new ReceiveProperties.Artemis(
                "tcp://a:61616", "u", "p", "hl7");
        ReceiveSettings settings = of(
                new ReceiveProperties("artemis", null, BATCH,
                        CHECK, RABBIT, artemis,
                        NO_EVENTHUBS));
        assertThat(settings.uri()).isNull();
        assertThat(settings.artemis().queue()).isEqualTo("hl7");
        assertThat(settings.artemis().toString()).doesNotContain("p,", "=p");
        assertThatThrownBy(() -> of(
                new ReceiveProperties("artemis", "jms:q", BATCH,
                        CHECK, RABBIT, artemis,
                        NO_EVENTHUBS)))
                .hasMessageContaining("isn't used");
        assertThatThrownBy(() -> of(broker("artemis", null)))
                .hasMessageContaining("broker-url");
    }

    /** Errors name the settings where they are configured. */
    @Test
    void namesTheSettingsInErrors()
    {
        assertThatThrownBy(() -> of(broker("kafka", "kafka:t")))
                .hasMessageStartingWith(PREFIX + ".uri");
        assertThatThrownBy(() -> ReceiveSettings.of(broker(null, null),
                "sdh.landing.broker", Set.of()))
                .hasMessageStartingWith("sdh.landing.broker.type");
    }
}
