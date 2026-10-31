/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.file.publish;

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
}
