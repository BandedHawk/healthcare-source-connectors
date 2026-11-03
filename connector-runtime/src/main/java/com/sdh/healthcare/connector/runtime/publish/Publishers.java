/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.runtime.publish;

import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import org.apache.camel.CamelContext;

/**
 * Picks the publisher for a queue type: the Camel producer, except where a component can't carry what's needed.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Publishers {
    private Publishers() {}

    public static ClosablePublisher create(CamelContext camel, QueueType type, String effectiveUri) {
        ClosablePublisher p = switch (type) {
            case SERVICEBUS -> ServiceBusPublisher.fromEndpoint(camel, effectiveUri);
            default -> new CamelMessagePublisher(camel, type, effectiveUri);
        };
        return type.keepsOrderWhenPipelined() ? p : new SequentialPublisher(p);
    }

    /**
     * Artemis needs a JMS connection factory on the jms component; other brokers configure themselves.
     *
     * @param camel the Camel context
     * @param type  the queue type
     * @param q     the queue settings
     */
    public static void configureBroker(CamelContext camel, QueueType type, CommonSettings.Queue q) {
        if (type == QueueType.ARTEMIS && q.artemis().brokerUrl() != null) {
            var cf = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(q.artemis().brokerUrl(),
                    q.artemis().username(), q.artemis().password());
            camel.getComponent("jms", org.apache.camel.component.jms.JmsComponent.class).setConnectionFactory(cf);
        }
    }
}
