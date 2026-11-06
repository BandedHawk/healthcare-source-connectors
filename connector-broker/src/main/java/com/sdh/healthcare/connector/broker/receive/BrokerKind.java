/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

/**
 * The brokers messages can be received from (design §5.3.16).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum BrokerKind
{

    /** Kafka, by Camel's {@code kafka} component in batching mode. */
    KAFKA("kafka"),

    /** Amazon SQS, by Camel's {@code aws2-sqs} component. */
    SQS("aws2-sqs"),

    /** RabbitMQ, by Camel's {@code spring-rabbitmq} component. */
    RABBITMQ("spring-rabbitmq"),

    /**
     * Artemis, by the JMS client directly: Camel's JMS components
     * acknowledge a message as soon as it is handed over (§5.3.16).
     */
    ARTEMIS(null),

    /**
     * Google Pub/Sub, by Camel's {@code google-pubsub} component,
     * acknowledging by hand.
     */
    PUBSUB("google-pubsub"),

    /** Azure Service Bus, by Camel's {@code azure-servicebus}. */
    SERVICEBUS("azure-servicebus"),

    /** Azure Event Hubs, by Camel's {@code azure-eventhubs}. */
    EVENTHUBS("azure-eventhubs");

    /** The Camel component's scheme; null if Camel isn't used. */
    private final String scheme;

    /**
     * Creates a kind.
     *
     * @param scheme the Camel scheme, or null
     */
    BrokerKind(String scheme)
    {
        this.scheme = scheme;
    }

    /**
     * The Camel component's scheme.
     *
     * @return the scheme; null for {@link #ARTEMIS}
     */
    public String scheme()
    {
        return scheme;
    }
}
