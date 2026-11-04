/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

/**
 * Tests for {@link BrokerSettings}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class BrokerSettingsTest
{

    /** Small batch limits. */
    static final LandingProperties.Batch BATCH = new LandingProperties.Batch(
            50, DataSize.ofMegabytes(1), Duration.ofSeconds(2));

    /** A RabbitMQ connection. */
    static final LandingProperties.Rabbitmq RABBIT =
            new LandingProperties.Rabbitmq("rabbit:5672", "/", "u", "p",
                    false);

    /** No Artemis connection. */
    static final LandingProperties.Artemis NO_ARTEMIS =
            new LandingProperties.Artemis(null, null, null, null);

    /** No Event Hubs checkpoint store. */
    static final LandingProperties.Eventhubs NO_EVENTHUBS =
            new LandingProperties.Eventhubs(null, null, null);

    /**
     * Broker settings as configured.
     *
     * @param type the broker
     * @param uri  the consumer URI
     * @return the settings
     */
    static LandingProperties.Broker broker(String type, String uri)
    {
        return new LandingProperties.Broker(type, uri, BATCH,
                Duration.ofHours(1), RABBIT, NO_ARTEMIS, NO_EVENTHUBS);
    }

    /**
     * Kafka gets batching, manual commits and the batch limits, and keeps
     * the options it was given.
     */
    @Test
    void addsTheOptionsKafkaNeeds()
    {
        BrokerSettings settings = BrokerSettings.of(broker("kafka",
                "kafka:hl7?brokers=k:9092&groupId=landing"));
        assertThat(settings.kind()).isEqualTo(BrokerSettings.Kind.KAFKA);
        assertThat(settings.uri()).startsWith("kafka:hl7?")
                .contains("brokers=k:9092", "groupId=landing",
                        "batching=true", "allowManualCommit=true",
                        "autoCommitEnable=false", "maxPollRecords=50",
                        "batchingIntervalMs=2000",
                        "autoOffsetReset=earliest", "ByteArrayDeserializer");
        assertThat(settings.maxBytes()).isEqualTo(1024 * 1024);
    }

    /** Kafka without a consumer group is refused. */
    @Test
    void refusesKafkaWithoutAGroup()
    {
        assertThatThrownBy(() -> BrokerSettings.of(broker("kafka",
                "kafka:hl7?brokers=k:9092")))
                .hasMessageContaining("groupId");
    }

    /** An option that would acknowledge too early is refused. */
    @Test
    void refusesOptionsThatAcknowledgeEarly()
    {
        assertThatThrownBy(() -> BrokerSettings.of(broker("kafka",
                "kafka:hl7?groupId=g&autoCommitEnable=true")))
                .hasMessageContaining("autoCommitEnable");
        assertThatThrownBy(() -> BrokerSettings.of(broker("sqs",
                "aws2-sqs:q?deleteAfterRead=false")))
                .hasMessageContaining("deleteAfterRead");
        assertThatThrownBy(() -> BrokerSettings.of(broker("rabbitmq",
                "spring-rabbitmq:ex?queues=q&acknowledgeMode=AUTO")))
                .hasMessageContaining("acknowledgeMode");
    }

    /** SQS and RabbitMQ get their options; RabbitMQ the node's factory. */
    @Test
    void addsTheOptionsSqsAndRabbitmqNeed()
    {
        assertThat(BrokerSettings.of(broker("sqs",
                "aws2-sqs:q?region=eu-west-1")).uri())
                .contains("region=eu-west-1", "deleteAfterRead=true",
                        "extendMessageVisibility=true",
                        "visibilityTimeout=60", "waitTimeSeconds=20");
        assertThat(BrokerSettings.of(broker("RabbitMQ",
                "spring-rabbitmq:ex?queues=q")).uri())
                .contains("queues=q", "acknowledgeMode=MANUAL",
                        "asyncConsumer=true",
                        "connectionFactory=#" + BrokerSettings.RABBITMQ_BEAN,
                        "autoDeclare=false", "prefetchCount=50");
    }

    /** A secret written into the URI is refused; a placeholder is kept. */
    @Test
    void refusesSecretsInTheUri()
    {
        assertThatThrownBy(() -> BrokerSettings.of(broker("sqs",
                "aws2-sqs:q?accessKey=AKIA&secretKey=s3cret")))
                .hasMessageContaining("placeholder")
                .hasMessageNotContaining("s3cret")
                .hasMessageNotContaining("AKIA");
        assertThat(BrokerSettings.of(broker("sqs",
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
        assertThatThrownBy(() -> BrokerSettings.of(broker("kafka",
                "aws2-sqs:q"))).hasMessageContaining("kafka:");
        assertThatThrownBy(() -> BrokerSettings.of(broker("nats", "x:y")))
                .hasMessageContaining("must be");
        assertThatThrownBy(() -> BrokerSettings.of(broker(null, null)))
                .hasMessageContaining("required");
        assertThatThrownBy(() -> BrokerSettings.of(
                new LandingProperties.Broker("rabbitmq",
                        "spring-rabbitmq:ex?queues=q", BATCH,
                        Duration.ofHours(1),
                        new LandingProperties.Rabbitmq(null, "/", null, null,
                                false), NO_ARTEMIS, NO_EVENTHUBS)))
                .hasMessageContaining("addresses");
    }

    /**
     * Pub/Sub acknowledges by hand, by streaming pull, and with no
     * delivery limit of Camel's own.
     */
    @Test
    void addsTheOptionsPubsubNeeds()
    {
        assertThat(BrokerSettings.of(broker("pubsub",
                "google-pubsub:proj:sub")).uri())
                .contains("ackMode=NONE", "synchronousPull=false",
                        "maxDeliveryAttempts=0",
                        "maxAckExtensionPeriod=3600");
        assertThatThrownBy(() -> BrokerSettings.of(broker("pubsub",
                "google-pubsub:proj:sub?ackMode=AUTO")))
                .hasMessageContaining("ackMode");
        assertThatThrownBy(() -> BrokerSettings.of(broker("pubsub",
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
        assertThat(BrokerSettings.of(broker("servicebus",
                "azure-servicebus:hl7?connectionString=RAW({{env:SB}})"))
                .uri()).contains("connectionString=RAW({{env:SB}})",
                        "serviceBusReceiveMode=PEEK_LOCK",
                        "enableDeadLettering=false", "sessionEnabled=false",
                        "maxAutoLockRenewDuration=300000");
        assertThatThrownBy(() -> BrokerSettings.of(broker("servicebus",
                "azure-servicebus:hl7?connectionString=Endpoint=sb://x")))
                .hasMessageContaining("placeholder");
        assertThatThrownBy(() -> BrokerSettings.of(broker("servicebus",
                "azure-servicebus:hl7?enableDeadLettering=true")))
                .hasMessageContaining("enableDeadLettering");
        assertThatThrownBy(() -> BrokerSettings.of(broker("servicebus",
                "azure-servicebus:hl7?maxAutoLockRenewDuration=0")))
                .hasMessageContaining("maxAutoLockRenewDuration");
        assertThatThrownBy(() -> BrokerSettings.of(broker("servicebus",
                "azure-servicebus:hl7?processorClient=#mine")))
                .hasMessageContaining("processorClient");
        assertThat(BrokerSettings.of(broker("servicebus",
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
        LandingProperties.Eventhubs checkpoints =
                new LandingProperties.Eventhubs(null,
                        "https://acct.blob.core.windows.net", "cp");
        BrokerSettings settings = BrokerSettings.of(
                new LandingProperties.Broker("eventhubs",
                        "azure-eventhubs:ns/hl7", BATCH, Duration.ofHours(1),
                        RABBIT, NO_ARTEMIS, checkpoints));
        assertThat(settings.uri()).contains(
                "checkpointStore=#" + BrokerSettings.CHECKPOINTS_BEAN,
                "eventPosition=#" + BrokerSettings.POSITIONS_BEAN);
        assertThatThrownBy(() -> BrokerSettings.of(
                new LandingProperties.Broker("eventhubs",
                        "azure-eventhubs:ns/hl7?blobAccountName=a", BATCH,
                        Duration.ofHours(1), RABBIT, NO_ARTEMIS,
                        checkpoints)))
                .hasMessageContaining("blobAccountName");
        assertThatThrownBy(() -> BrokerSettings.of(broker("eventhubs",
                "azure-eventhubs:ns/hl7")))
                .hasMessageContaining("checkpoint-container");
        assertThatThrownBy(() -> BrokerSettings.of(
                new LandingProperties.Broker("eventhubs",
                        "azure-eventhubs:ns/hl7", BATCH, Duration.ofHours(1),
                        RABBIT, NO_ARTEMIS, new LandingProperties.Eventhubs(
                                "cs", "https://x", "cp"))))
                .hasMessageContaining("one of");
        assertThat(new LandingProperties.Eventhubs("AccountKey=s3cret",
                null, "cp").toString()).doesNotContain("s3cret");
    }

    /** Artemis takes its connection, not a URI. */
    @Test
    void takesArtemisFromItsOwnSettings()
    {
        LandingProperties.Artemis artemis = new LandingProperties.Artemis(
                "tcp://a:61616", "u", "p", "hl7");
        BrokerSettings settings = BrokerSettings.of(
                new LandingProperties.Broker("artemis", null, BATCH,
                        Duration.ofHours(1), RABBIT, artemis, NO_EVENTHUBS));
        assertThat(settings.uri()).isNull();
        assertThat(settings.artemis().queue()).isEqualTo("hl7");
        assertThat(settings.artemis().toString()).doesNotContain("p,", "=p");
        assertThatThrownBy(() -> BrokerSettings.of(
                new LandingProperties.Broker("artemis", "jms:q", BATCH,
                        Duration.ofHours(1), RABBIT, artemis, NO_EVENTHUBS)))
                .hasMessageContaining("isn't used");
        assertThatThrownBy(() -> BrokerSettings.of(broker("artemis", null)))
                .hasMessageContaining("broker-url");
    }
}
