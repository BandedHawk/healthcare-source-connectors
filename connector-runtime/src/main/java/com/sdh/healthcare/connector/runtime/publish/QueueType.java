/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.publish;

import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.camel.Exchange;

/**
 * Broker adapters (design §10.3). Each sets its broker's ordering key
 * ({@code fileId}) and dedup ID ({@code messageId}), and adds the endpoint
 * options needed for durable acknowledgement when the configured URI
 * doesn't set them.
 * <p>
 * Unencrypted metadata is limited to {@code messageId}, {@code type},
 * {@code fileId} and the content type (design §10.1); the body is the JWE
 * compact serialisation, as ASCII text.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum QueueType
{

    /** Record key = fileId; idempotent producer, acks=all. */
    KAFKA("kafka", Map.of("requestRequiredAcks", "all",
            "enableIdempotence", "true"))
    {
        @Override
        void decorate(Exchange exchange, OutboundMessage message)
        {
            exchange.getIn().setHeader("CamelKafkaKey",
                    message.fileId().toString());
        }
    },

    /**
     * FIFO queues: MessageGroupId = fileId, MessageDeduplicationId =
     * messageId.
     */
    SQS("aws2-sqs", Map.of())
    {
        @Override
        void decorate(Exchange exchange, OutboundMessage message)
        {
            exchange.setProperty("CamelAwsMessageGroupId",
                    message.fileId().toString());
            exchange.setExchangeId(message.messageId());
        }

        @Override
        Map<String, String> requiredOptions(String uri)
        {
            return path(uri).endsWith(".fifo")
                    ? Map.of("messageGroupIdStrategy", "usePropertyValue",
                            "messageDeduplicationIdStrategy", "useExchangeId")
                    : Map.of();
        }
    },

    /**
     * orderingKey = fileId (needs a regional {@code pubsubEndpoint});
     * metadata as attributes.
     */
    PUBSUB("google-pubsub", Map.of("messageOrderingEnabled", "true"))
    {
        @Override
        void decorate(Exchange exchange, OutboundMessage message)
        {
            exchange.getIn().setHeader("CamelGooglePubsubOrderingKey",
                    message.fileId().toString());
            exchange.getIn().setHeader("CamelGooglePubsubAttributes",
                    metadata(message));
        }
    },

    /**
     * SessionId = fileId; MessageId = messageId (enable duplicate detection
     * on the entity). Sent by {@link ServiceBusPublisher}: the Camel
     * producer can't set these per message.
     */
    SERVICEBUS("azure-servicebus", Map.of())
    {
        @Override
        void decorate(Exchange exchange, OutboundMessage message)
        {
        }

        /**
         * {@inheritDoc} Not for Service Bus: it uses the same Azure AMQP
         * client as Event Hubs, which reorders the same way (found by
         * ServiceBusIT).
         *
         * @return false
         */
        @Override
        public boolean keepsOrderWhenPipelined()
        {
            return false;
        }
    },

    /**
     * partitionKey = fileId; no broker-side dedup (the messageId is inside
     * the payload). Sends one at a time: the Azure AMQP client reorders
     * sends that are in flight together (found by EventHubsIT).
     */
    EVENTHUBS("azure-eventhubs", Map.of())
    {
        @Override
        void decorate(Exchange exchange, OutboundMessage message)
        {
            exchange.getIn().setHeader("CamelAzureEventHubsPartitionKey",
                    message.fileId().toString());
        }

        @Override
        public boolean keepsOrderWhenPipelined()
        {
            return false;
        }
    },

    /**
     * JMSXGroupID = fileId; Artemis duplicate detection through
     * {@code _AMQ_DUPL_ID} = messageId.
     */
    ARTEMIS("jms", Map.of())
    {
        @Override
        void decorate(Exchange exchange, OutboundMessage message)
        {
            exchange.getIn().setHeader("JMSXGroupID",
                    message.fileId().toString());
            exchange.getIn().setHeader("_AMQ_DUPL_ID", message.messageId());
        }
    },

    /** message_id = messageId; publisher confirms. */
    RABBITMQ("spring-rabbitmq", Map.of("confirm", "enabled"))
    {
        @Override
        void decorate(Exchange exchange, OutboundMessage message)
        {
            exchange.getIn().setHeader("CamelSpringRabbitmqMessageId",
                    message.messageId());
            exchange.getIn().setHeader("CamelSpringRabbitmqContentType",
                    OutboundMessage.CONTENT_TYPE);
        }
    },

    /**
     * Any Camel endpoint, with the metadata headers only. For tests and for
     * components without an adapter of their own.
     */
    CAMEL(null, Map.of())
    {
        @Override
        void decorate(Exchange exchange, OutboundMessage message)
        {
        }
    };

    /** The Camel component a URI must use; null for any. */
    private final String scheme;

    /** Options added to the URI when it doesn't set them. */
    private final Map<String, String> defaults;

    /**
     * Creates a type.
     *
     * @param scheme   the Camel component a URI must use; null for any
     * @param defaults options added to the URI when it doesn't set them
     */
    QueueType(String scheme, Map<String, String> defaults)
    {
        this.scheme = scheme;
        this.defaults = defaults;
    }

    /**
     * The type a setting names, in any case.
     *
     * @param name the setting's value, such as {@code kafka}
     * @return the type
     * @throws IllegalArgumentException if it is missing or unknown
     */
    public static QueueType of(String name)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("sdh.queue.type is required");
        }
        try
        {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException unknown)
        {
            throw new IllegalArgumentException("Unknown sdh.queue.type '"
                    + name + "'; one of " + List.of(values()).stream()
                            .map(type -> type.name().toLowerCase(Locale.ROOT))
                            .toList());
        }
    }

    /**
     * Sets the broker's own headers on an exchange.
     *
     * @param exchange the exchange to send
     * @param message  the message it carries
     */
    abstract void decorate(Exchange exchange, OutboundMessage message);

    /**
     * Whether messages sent back to back, without waiting for each
     * acknowledgement, still arrive in order. Checked against each real
     * broker by the integration tests. If false, the publisher waits for
     * each acknowledgement before the next send.
     *
     * @return true if pipelined sends keep their order
     */
    public boolean keepsOrderWhenPipelined()
    {
        return true;
    }

    /**
     * The options a URI gets when it doesn't set them.
     *
     * @param uri the configured URI
     * @return the options
     */
    Map<String, String> requiredOptions(String uri)
    {
        return defaults;
    }

    /**
     * Sets the metadata headers, then the broker's own.
     *
     * @param exchange the exchange to send
     * @param message  the message it carries
     */
    public void apply(Exchange exchange, OutboundMessage message)
    {
        metadata(message).forEach((name, value) ->
                exchange.getIn().setHeader(name, value));
        decorate(exchange, message);
    }

    /**
     * The configured URI plus any required options it doesn't set itself;
     * an explicit setting always wins.
     *
     * @param uri the configured URI
     * @return the URI to use
     * @throws IllegalArgumentException if it is missing or uses another
     *                                  component
     */
    public String effectiveUri(String uri)
    {
        if (uri == null || uri.isBlank())
        {
            throw new IllegalArgumentException("sdh.queue.uri is required");
        }
        if (scheme != null && !uri.startsWith(scheme + ":"))
        {
            throw new IllegalArgumentException("sdh.queue.uri must start with '"
                    + scheme + ":' for type "
                    + name().toLowerCase(Locale.ROOT));
        }
        Set<String> present = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        int query = uri.indexOf('?');
        if (query >= 0)
        {
            for (String option : uri.substring(query + 1).split("&"))
            {
                int equals = option.indexOf('=');
                present.add(equals < 0 ? option : option.substring(0, equals));
            }
        }
        StringBuilder out = new StringBuilder(uri);
        char separator = query >= 0 ? '&' : '?';
        for (Map.Entry<String, String> option
                : new TreeMap<>(requiredOptions(uri)).entrySet())
        {
            if (!present.contains(option.getKey()))
            {
                out.append(separator).append(option.getKey()).append('=')
                        .append(option.getValue());
                separator = '&';
            }
        }
        return out.toString();
    }

    /**
     * The unencrypted metadata every message carries (design §10.1).
     *
     * @param message the message
     * @return {@code messageId}, {@code type}, {@code fileId} and
     *         {@code contentType}, in that order
     */
    public static Map<String, String> metadata(OutboundMessage message)
    {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("messageId", message.messageId());
        headers.put("type", message.type().wireName());
        headers.put("fileId", message.fileId().toString());
        headers.put("contentType", OutboundMessage.CONTENT_TYPE);
        return headers;
    }

    /**
     * A URI without its query.
     *
     * @param uri the URI
     * @return everything before the {@code ?}
     */
    private static String path(String uri)
    {
        int query = uri.indexOf('?');
        return query < 0 ? uri : uri.substring(0, query);
    }
}
