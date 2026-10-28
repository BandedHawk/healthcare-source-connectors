package com.sdh.healthcare.connector.file.publish;

import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.camel.Exchange;

/**
 * Broker adapters (design §10.3). Each sets its broker's ordering key ({@code fileId}) and dedup ID ({@code messageId}),
 * and adds the endpoint options needed for durable acknowledgement when the configured URI doesn't set them.
 *
 * <p>Unencrypted metadata is limited to {@code messageId}, {@code type}, {@code fileId} and the content type
 * (design §10.1); the body is the JWE compact serialisation (ASCII text).
 */
public enum QueueType {
    /** Record key = fileId; idempotent producer, acks=all. */
    KAFKA("kafka", Map.of("requestRequiredAcks", "all", "enableIdempotence", "true")) {
        @Override
        void decorate(Exchange e, OutboundMessage m) {
            e.getIn().setHeader("CamelKafkaKey", m.fileId().toString());
        }
    },
    /** FIFO queues: MessageGroupId = fileId, MessageDeduplicationId = messageId. */
    SQS("aws2-sqs", Map.of()) {
        @Override
        void decorate(Exchange e, OutboundMessage m) {
            e.setProperty("CamelAwsMessageGroupId", m.fileId().toString());
            e.setExchangeId(m.messageId());
        }

        @Override
        Map<String, String> requiredOptions(String uri) {
            return path(uri).endsWith(".fifo")
                    ? Map.of("messageGroupIdStrategy", "usePropertyValue",
                            "messageDeduplicationIdStrategy", "useExchangeId")
                    : Map.of();
        }
    },
    /** orderingKey = fileId (needs a regional pubsubEndpoint); metadata as attributes. */
    PUBSUB("google-pubsub", Map.of("messageOrderingEnabled", "true")) {
        @Override
        void decorate(Exchange e, OutboundMessage m) {
            e.getIn().setHeader("CamelGooglePubsubOrderingKey", m.fileId().toString());
            e.getIn().setHeader("CamelGooglePubsubAttributes", metadata(m));
        }
    },
    /** SessionId = fileId; MessageId = messageId (enable duplicate detection on the entity). */
    SERVICEBUS("azure-servicebus", Map.of()) {
        @Override
        void decorate(Exchange e, OutboundMessage m) {
            e.getIn().setHeader("CamelAzureServiceBusSessionId", m.fileId().toString());
            e.getIn().setHeader("CamelAzureServiceBusMessageId", m.messageId());
            e.getIn().setHeader("CamelAzureServiceBusContentType", OutboundMessage.CONTENT_TYPE);
            e.getIn().setHeader("CamelAzureServiceBusApplicationProperties", new LinkedHashMap<>(metadata(m)));
        }
    },
    /** partitionKey = fileId; no broker-side dedup (messageId is inside the payload). */
    EVENTHUBS("azure-eventhubs", Map.of()) {
        @Override
        void decorate(Exchange e, OutboundMessage m) {
            e.getIn().setHeader("CamelAzureEventHubsPartitionKey", m.fileId().toString());
        }
    },
    /** JMSXGroupID = fileId; Artemis duplicate detection via _AMQ_DUPL_ID = messageId. */
    ARTEMIS("jms", Map.of()) {
        @Override
        void decorate(Exchange e, OutboundMessage m) {
            e.getIn().setHeader("JMSXGroupID", m.fileId().toString());
            e.getIn().setHeader("_AMQ_DUPL_ID", m.messageId());
        }
    },
    /** message_id = messageId; publisher confirms. */
    RABBITMQ("spring-rabbitmq", Map.of("confirm", "enabled")) {
        @Override
        void decorate(Exchange e, OutboundMessage m) {
            e.getIn().setHeader("CamelSpringRabbitmqMessageId", m.messageId());
            e.getIn().setHeader("CamelSpringRabbitmqContentType", OutboundMessage.CONTENT_TYPE);
        }
    },
    /** Any Camel endpoint, metadata headers only. For tests and components without a dedicated adapter. */
    CAMEL(null, Map.of()) {
        @Override
        void decorate(Exchange e, OutboundMessage m) {}
    };

    private final String scheme;
    private final Map<String, String> defaults;

    QueueType(String scheme, Map<String, String> defaults) {
        this.scheme = scheme;
        this.defaults = defaults;
    }

    public static QueueType of(String name) {
        if (name == null) {
            throw new IllegalArgumentException("sdh.queue.type is required");
        }
        try {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown sdh.queue.type '" + name + "'; one of "
                    + List.of(values()).stream().map(v -> v.name().toLowerCase(Locale.ROOT)).toList());
        }
    }

    abstract void decorate(Exchange e, OutboundMessage m);

    Map<String, String> requiredOptions(String uri) {
        return defaults;
    }

    /** Sets the metadata headers, then the broker-specific ones. */
    public void apply(Exchange e, OutboundMessage m) {
        metadata(m).forEach((k, v) -> e.getIn().setHeader(k, v));
        decorate(e, m);
    }

    /** The configured URI plus any required options it doesn't set itself (an explicit setting always wins). */
    public String effectiveUri(String uri) {
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException("sdh.queue.uri is required");
        }
        if (scheme != null && !uri.startsWith(scheme + ":")) {
            throw new IllegalArgumentException("sdh.queue.uri must start with '" + scheme + ":' for type "
                    + name().toLowerCase(Locale.ROOT));
        }
        Set<String> present = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        int q = uri.indexOf('?');
        if (q >= 0) {
            for (String kv : uri.substring(q + 1).split("&")) {
                int eq = kv.indexOf('=');
                present.add(eq < 0 ? kv : kv.substring(0, eq));
            }
        }
        StringBuilder out = new StringBuilder(uri);
        char sep = q >= 0 ? '&' : '?';
        for (Map.Entry<String, String> opt : new java.util.TreeMap<>(requiredOptions(uri)).entrySet()) {
            if (!present.contains(opt.getKey())) {
                out.append(sep).append(opt.getKey()).append('=').append(opt.getValue());
                sep = '&';
            }
        }
        return out.toString();
    }

    static Map<String, String> metadata(OutboundMessage m) {
        Map<String, String> h = new LinkedHashMap<>();
        h.put("messageId", m.messageId());
        h.put("type", m.type().wireName());
        h.put("fileId", m.fileId().toString());
        h.put("contentType", OutboundMessage.CONTENT_TYPE);
        return h;
    }

    private static String path(String uri) {
        int q = uri.indexOf('?');
        return q < 0 ? uri : uri.substring(0, q);
    }
}
