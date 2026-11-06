/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import java.util.Map;
import java.util.Objects;

/**
 * A message received from a broker. It may hold PHI: never log it.
 *
 * @param body       its body, as bytes
 * @param messageId  the broker's ID for it, which stays the same when it is
 *                   delivered again: Kafka's and Event Hubs' partition and
 *                   offset or sequence number, Service Bus's sequence
 *                   number, or the broker's message ID; null if the broker
 *                   gave none (RabbitMQ without a producer's
 *                   {@code message_id})
 * @param attributes the message's attributes or headers that were asked for
 *                   ({@link ReceiveSettings#attributes()}), as text; one not
 *                   set is absent
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record BrokerMessage(byte[] body, String messageId,
        Map<String, String> attributes)
{

    /**
     * Creates a message.
     *
     * @param body       its body
     * @param messageId  the broker's ID; null if none
     * @param attributes the attributes asked for
     */
    public BrokerMessage
    {
        Objects.requireNonNull(body, "body");
        attributes = Map.copyOf(attributes);
    }

    /**
     * One attribute.
     *
     * @param name the attribute's name
     * @return its value; null if not set, or not asked for
     */
    public String attribute(String name)
    {
        return attributes.get(name);
    }
}
