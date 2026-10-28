package com.sdh.healthcare.connector.file.publish;

import org.apache.camel.CamelContext;

/** Picks the publisher for a queue type: the Camel producer, except where a component can't carry what's needed. */
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
