/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import java.net.URISyntaxException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import org.apache.camel.util.URISupport;

/**
 * The checked settings of a queue intake (design §5.3.16), and the Camel
 * consumer URI it runs with.
 * <p>
 * Each broker's consumer is set up so that a message is acknowledged only
 * once the batch holding it is committed. The options that does needs are
 * added to the URI; one set to anything else is refused, since it would
 * acknowledge messages that could then be lost. So is a password, key,
 * secret or connection string written into the URI: it must be a property
 * placeholder ({@code {{...}}}), resolved from the environment, or a bean
 * reference ({@code #name}).
 *
 * @param kind        which broker
 * @param uri         the Camel consumer URI, with the intake's options; null
 *                    for {@link Kind#ARTEMIS}
 * @param maxMessages the most messages in a batch
 * @param maxBytes    the most bytes in a batch
 * @param maxWait     the longest a batch stays open
 * @param dedupWindow how far back a message delivered again is recognised
 * @param rabbitmq    the RabbitMQ connection, for {@link Kind#RABBITMQ}
 * @param artemis     the Artemis connection, for {@link Kind#ARTEMIS}
 * @param eventhubs   the checkpoint store, for {@link Kind#EVENTHUBS}
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record BrokerSettings(Kind kind, String uri, int maxMessages,
        long maxBytes, Duration maxWait, Duration dedupWindow,
        LandingProperties.Rabbitmq rabbitmq,
        LandingProperties.Artemis artemis,
        LandingProperties.Eventhubs eventhubs)
{

    /** The bean name of the intake's RabbitMQ connection factory. */
    public static final String RABBITMQ_BEAN = "sdhIntakeRabbit";

    /** The bean name of the intake's Event Hubs checkpoint store. */
    public static final String CHECKPOINTS_BEAN = "sdhIntakeCheckpoints";

    /**
     * The bean name of the intake's Event Hubs start positions: the earliest
     * event, for a partition with no checkpoint.
     */
    public static final String POSITIONS_BEAN = "sdhIntakeEventPositions";

    /** The brokers' names, as {@code sdh.landing.broker.type} takes them. */
    private static final String TYPES = "kafka, sqs, rabbitmq, artemis,"
            + " pubsub, servicebus or eventhubs";

    /** Option names that hold secrets, in lower case. */
    private static final Set<String> SECRET_PARTS = Set.of("password",
            "secret", "jaas", "accesskey", "sessiontoken", "token",
            "connectionstring");

    /**
     * A property placeholder, resolved when the route starts, or a bean
     * reference.
     */
    private static final Pattern PLACEHOLDER = Pattern.compile(
            "(RAW\\()?\\{\\{[^{}]+\\}\\}\\)?|#[A-Za-z0-9_.-]+");

    /** The brokers a queue intake can consume from. */
    public enum Kind
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
        Kind(String scheme)
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

    /**
     * Checks the settings and works out the consumer URI.
     *
     * @param broker the settings as configured
     * @return the checked settings
     * @throws IllegalArgumentException if a setting is missing or would let
     *                                  a message be acknowledged before it
     *                                  is landed
     */
    public static BrokerSettings of(LandingProperties.Broker broker)
    {
        if (broker.type() == null || broker.type().isBlank())
        {
            throw new IllegalArgumentException("sdh.landing.broker.type is"
                    + " required: " + TYPES);
        }
        Kind kind;
        try
        {
            kind = Kind.valueOf(broker.type().trim().toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException unknown)
        {
            throw new IllegalArgumentException("sdh.landing.broker.type must"
                    + " be " + TYPES);
        }
        LandingProperties.Batch batch = broker.batch();
        if (batch.maxMessages() < 1 || batch.maxBytes().toBytes() < 1
                || batch.maxWait().isNegative() || batch.maxWait().isZero())
        {
            throw new IllegalArgumentException("sdh.landing.broker.batch"
                    + " limits must be positive");
        }
        if (broker.dedupWindow().isNegative())
        {
            throw new IllegalArgumentException(
                    "sdh.landing.broker.dedup-window must not be negative");
        }
        String uri = null;
        if (kind == Kind.ARTEMIS)
        {
            LandingProperties.Artemis artemis = broker.artemis();
            if (broker.uri() != null && !broker.uri().isBlank())
            {
                throw new IllegalArgumentException("sdh.landing.broker.uri"
                        + " isn't used for artemis; set"
                        + " sdh.landing.broker.artemis.*");
            }
            if (blank(artemis.brokerUrl()) || blank(artemis.queue()))
            {
                throw new IllegalArgumentException("sdh.landing.broker"
                        + ".artemis.broker-url and queue are required");
            }
        }
        else
        {
            if (kind == Kind.RABBITMQ && blank(broker.rabbitmq().addresses()))
            {
                throw new IllegalArgumentException("sdh.landing.broker"
                        + ".rabbitmq.addresses is required");
            }
            if (kind == Kind.EVENTHUBS)
            {
                LandingProperties.Eventhubs eventhubs = broker.eventhubs();
                if (blank(eventhubs.checkpointContainer())
                        || blank(eventhubs.checkpointConnectionString())
                                == blank(eventhubs.checkpointEndpoint()))
                {
                    throw new IllegalArgumentException("sdh.landing.broker"
                            + ".eventhubs needs checkpoint-container, and"
                            + " one of checkpoint-connection-string and"
                            + " checkpoint-endpoint");
                }
            }
            uri = consumerUri(kind, broker.uri(), batch);
        }
        return new BrokerSettings(kind, uri, batch.maxMessages(),
                batch.maxBytes().toBytes(), batch.maxWait(),
                broker.dedupWindow(), broker.rabbitmq(), broker.artemis(),
                broker.eventhubs());
    }

    /**
     * The consumer URI with the intake's options.
     *
     * @param kind  the broker
     * @param uri   the URI as configured
     * @param batch the batch limits
     * @return the URI to consume from
     */
    static String consumerUri(Kind kind, String uri,
            LandingProperties.Batch batch)
    {
        if (blank(uri))
        {
            throw new IllegalArgumentException(
                    "sdh.landing.broker.uri is required for " + kind.scheme());
        }
        String trimmed = uri.trim();
        if (!trimmed.startsWith(kind.scheme() + ":"))
        {
            throw new IllegalArgumentException("sdh.landing.broker.uri must"
                    + " start with " + kind.scheme() + ":");
        }
        int question = trimmed.indexOf('?');
        String base = question < 0 ? trimmed : trimmed.substring(0, question);
        Map<String, Object> options;
        try
        {
            options = new LinkedHashMap<>(URISupport.parseQuery(
                    question < 0 ? "" : trimmed.substring(question + 1),
                    true));
        }
        catch (URISyntaxException bad)
        {
            throw new IllegalArgumentException(
                    "sdh.landing.broker.uri has a malformed query");
        }
        for (Map.Entry<String, Object> option : options.entrySet())
        {
            String name = option.getKey().toLowerCase(Locale.ROOT);
            if (SECRET_PARTS.stream().anyMatch(name::contains)
                    && !PLACEHOLDER.matcher(
                            String.valueOf(option.getValue())).matches())
            {
                throw new IllegalArgumentException("sdh.landing.broker.uri"
                        + " option " + option.getKey() + " must be a"
                        + " property placeholder ({{...}}), not a value");
            }
        }
        switch (kind)
        {
            case KAFKA ->
            {
                if (!options.containsKey("groupId"))
                {
                    throw new IllegalArgumentException(
                            "sdh.landing.broker.uri needs a groupId");
                }
                require(options, "batching", "true");
                require(options, "allowManualCommit", "true");
                require(options, "autoCommitEnable", "false");
                require(options, "maxPollRecords",
                        Integer.toString(batch.maxMessages()));
                require(options, "batchingIntervalMs",
                        Long.toString(batch.maxWait().toMillis()));
                require(options, "valueDeserializer",
                        "org.apache.kafka.common.serialization"
                                + ".ByteArrayDeserializer");
                options.putIfAbsent("autoOffsetReset", "earliest");
            }
            case SQS ->
            {
                require(options, "deleteAfterRead", "true");
                require(options, "extendMessageVisibility", "true");
                options.putIfAbsent("visibilityTimeout", "60");
                options.putIfAbsent("maxMessagesPerPoll", "10");
                options.putIfAbsent("waitTimeSeconds", "20");
            }
            case RABBITMQ ->
            {
                if (!options.containsKey("queues"))
                {
                    throw new IllegalArgumentException(
                            "sdh.landing.broker.uri needs queues");
                }
                require(options, "acknowledgeMode", "MANUAL");
                require(options, "asyncConsumer", "true");
                require(options, "connectionFactory", "#" + RABBITMQ_BEAN);
                options.putIfAbsent("autoDeclare", "false");
                options.putIfAbsent("prefetchCount",
                        Integer.toString(batch.maxMessages()));
            }
            case PUBSUB ->
            {
                require(options, "ackMode", "NONE");
                require(options, "synchronousPull", "false");
                // Above 0, Camel nacks at the limit, logging the message ID.
                require(options, "maxDeliveryAttempts", "0");
                options.putIfAbsent("maxAckExtensionPeriod", "3600");
            }
            case SERVICEBUS ->
            {
                require(options, "serviceBusReceiveMode", "PEEK_LOCK");
                require(options, "enableDeadLettering", "false");
                require(options, "sessionEnabled", "false");
                refuse(options, "processorClient");
                options.putIfAbsent("maxAutoLockRenewDuration", "300000");
                if (Long.parseLong(String.valueOf(options.get(
                        "maxAutoLockRenewDuration"))) <= 0)
                {
                    throw new IllegalArgumentException("sdh.landing.broker"
                            + ".uri option maxAutoLockRenewDuration must be"
                            + " positive, so held messages keep their lock");
                }
            }
            case EVENTHUBS ->
            {
                require(options, "checkpointStore", "#" + CHECKPOINTS_BEAN);
                require(options, "eventPosition", "#" + POSITIONS_BEAN);
                for (String blob : List.of("blobAccountName", "blobAccessKey",
                        "blobContainerName", "blobStorageSharedKeyCredential"))
                {
                    refuse(options, blob);
                }
            }
            default -> throw new IllegalStateException(kind.name());
        }
        // Joined as written: encoding would hide {{...}} placeholders from
        // Camel, which resolves them before it decodes the URI.
        StringJoiner query = new StringJoiner("&", base + "?", "");
        options.forEach((name, value) -> query.add(name + "=" + value));
        return query.toString();
    }

    /**
     * Sets an option the intake needs, refusing a different value.
     *
     * @param options the options
     * @param name    the option
     * @param value   the value it must have
     */
    private static void require(Map<String, Object> options, String name,
            String value)
    {
        Object set = options.putIfAbsent(name, value);
        if (set != null && !String.valueOf(set).equalsIgnoreCase(value))
        {
            throw new IllegalArgumentException("sdh.landing.broker.uri option "
                    + name + " must be " + value + ": anything else could"
                    + " acknowledge messages before they are landed");
        }
    }

    /**
     * Refuses an option the node sets up itself.
     *
     * @param options the options
     * @param name    the option
     */
    private static void refuse(Map<String, Object> options, String name)
    {
        if (options.containsKey(name))
        {
            throw new IllegalArgumentException("sdh.landing.broker.uri option "
                    + name + " isn't allowed: the node sets it up");
        }
    }

    /**
     * Whether a value is missing.
     *
     * @param value the value
     * @return true if it is null or blank
     */
    private static boolean blank(String value)
    {
        return value == null || value.isBlank();
    }
}
