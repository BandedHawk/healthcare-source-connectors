/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import java.time.Duration;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * The broker to receive from, as configured (design §5.3.16): the landing
 * node's {@code sdh.landing.broker}, or a stage's input. Messages are
 * handled in batches, and acknowledged only once their batch is handled.
 * Secrets come from the environment, never the file.
 * {@link ReceiveSettings#of} checks them.
 *
 * @param type          {@code kafka}, {@code sqs}, {@code rabbitmq},
 *                      {@code pubsub}, {@code servicebus} or
 *                      {@code eventhubs} (Camel consumers), or
 *                      {@code artemis} (the JMS client)
 * @param uri           the Camel consumer URI, for every type but
 *                      {@code artemis}; the options receiving needs are
 *                      added, and any that would break it refused
 * @param batch         when a batch is closed and handled
 * @param checkInterval how often the broker is checked, for a broker whose
 *                      consumer doesn't report an outage itself
 * @param rabbitmq      the RabbitMQ connection, for {@code rabbitmq}
 * @param artemis       the Artemis connection and queue, for
 *                      {@code artemis}
 * @param eventhubs     the checkpoint store, for {@code eventhubs}
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record ReceiveProperties(String type, String uri,
        @DefaultValue Batch batch,
        @DefaultValue("30s") Duration checkInterval,
        @DefaultValue Rabbitmq rabbitmq,
        @DefaultValue Artemis artemis,
        @DefaultValue Eventhubs eventhubs)
{

    /**
     * Where an Event Hubs consumer keeps its checkpoints: a blob container,
     * which must exist. Reached by a connection string, or by its endpoint
     * with the default Azure credential (managed or workload identity).
     *
     * @param checkpointConnectionString the storage account's connection
     *                                   string; from the environment
     * @param checkpointEndpoint         the blob endpoint, such as
     *                                   {@code https://a.blob.core.windows.net}
     * @param checkpointContainer        the container's name
     */
    public record Eventhubs(String checkpointConnectionString,
            String checkpointEndpoint, String checkpointContainer)
    {

        /**
         * The settings without the connection string.
         *
         * @return the settings as text
         */
        @Override
        public String toString()
        {
            return "Eventhubs[checkpointEndpoint=" + checkpointEndpoint
                    + ", checkpointContainer=" + checkpointContainer + "]";
        }
    }

    /**
     * When a batch of messages is closed: whichever limit comes first.
     *
     * @param maxMessages the most messages in a batch
     * @param maxBytes    the most bytes in a batch; a single larger message
     *                    is a batch of its own
     * @param maxWait     the longest a batch stays open after its first
     *                    message
     */
    public record Batch(@DefaultValue("1000") int maxMessages,
            @DefaultValue("64MB") DataSize maxBytes,
            @DefaultValue("5s") Duration maxWait)
    {
    }

    /**
     * A RabbitMQ connection.
     *
     * @param addresses   {@code host:port}, comma-separated
     * @param virtualHost the virtual host
     * @param username    the user
     * @param password    the password; from the environment
     * @param ssl         whether to connect over TLS
     */
    public record Rabbitmq(String addresses,
            @DefaultValue("/") String virtualHost, String username,
            String password, @DefaultValue("false") boolean ssl)
    {

        /**
         * The connection without its password.
         *
         * @return the settings as text
         */
        @Override
        public String toString()
        {
            return "Rabbitmq[addresses=" + addresses + ", virtualHost="
                    + virtualHost + ", username=" + username + ", ssl=" + ssl
                    + "]";
        }
    }

    /**
     * An Artemis connection and the queue to consume.
     *
     * @param brokerUrl the broker URL, such as {@code tcp://host:61616}
     * @param username  the user
     * @param password  the password; from the environment
     * @param queue     the queue's name
     */
    public record Artemis(String brokerUrl, String username, String password,
            String queue)
    {

        /**
         * The connection without its password.
         *
         * @return the settings as text
         */
        @Override
        public String toString()
        {
            return "Artemis[brokerUrl=" + brokerUrl + ", username=" + username
                    + ", queue=" + queue + "]";
        }
    }
}
