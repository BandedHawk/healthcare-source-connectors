/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.send;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.component.jms.JmsComponent;

/**
 * Picks the publisher for a queue type: the Camel producer, except where a
 * component can't carry what's needed.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Publishers
{

    /** Not instantiated: this class only has static methods. */
    private Publishers()
    {
    }

    /**
     * Creates the publisher for a queue type. Service Bus uses the Azure
     * SDK (design §10.3); brokers whose client reorders sends in flight
     * together send one at a time.
     *
     * @param camel        the Camel context
     * @param type         the queue type
     * @param effectiveUri the endpoint URI, with the type's options added
     * @return the publisher
     */
    public static ClosablePublisher create(CamelContext camel, QueueType type,
            String effectiveUri)
    {
        ClosablePublisher publisher = switch (type)
        {
            case SERVICEBUS ->
                    ServiceBusPublisher.fromEndpoint(camel, effectiveUri);
            default -> new CamelMessagePublisher(camel, type, effectiveUri);
        };
        return type.keepsOrderWhenPipelined()
                ? publisher : new SequentialPublisher(publisher);
    }

    /**
     * Artemis needs a JMS connection factory on the {@code jms} component;
     * other brokers configure themselves.
     *
     * @param camel     the Camel context
     * @param type      the queue type
     * @param brokerUrl Artemis's URL; null if not set
     * @param username  the Artemis user
     * @param password  the user's password, from the environment
     */
    public static void configureBroker(CamelContext camel, QueueType type,
            String brokerUrl, String username, String password)
    {
        if (type == QueueType.ARTEMIS && brokerUrl != null)
        {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                    brokerUrl, username, password);
            camel.getComponent("jms", JmsComponent.class)
                    .setConnectionFactory(factory);
        }
    }
}
