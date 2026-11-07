/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import java.net.URISyntaxException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.camel.util.URISupport;

/**
 * The checked settings of a broker to receive from (design §5.3.16), and
 * the Camel consumer URI it is received with.
 * <p>
 * Each broker's consumer is set up so that a message is acknowledged only
 * once the batch holding it is handled. The options that needs are
 * added to the URI; one set to anything else is refused, since it would
 * acknowledge messages that could then be lost. So is a password, key,
 * secret or connection string written into the URI: it must be a property
 * placeholder ({@code {{...}}}), resolved from the environment, or a bean
 * reference ({@code #name}).
 *
 * @param kind          which broker
 * @param uri           the Camel consumer URI, with the options receiving
 *                      needs; null for {@link BrokerKind#ARTEMIS}
 * @param maxMessages   the most messages in a batch
 * @param maxBytes      the most bytes in a batch
 * @param maxWait       the longest a batch stays open
 * @param checkInterval how often a broker whose consumer doesn't report an
 *                      outage is checked
 * @param attributes    the message attributes or headers to read into each
 *                      {@link BrokerMessage}, such as the landing node's
 *                      {@code sdh_replaces}
 * @param rabbitmq      the RabbitMQ connection, for
 *                      {@link BrokerKind#RABBITMQ}
 * @param artemis       the Artemis connection, for {@link BrokerKind#ARTEMIS}
 * @param eventhubs     the checkpoint store, for
 *                      {@link BrokerKind#EVENTHUBS}
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record ReceiveSettings(BrokerKind kind, String uri, int maxMessages,
        long maxBytes, Duration maxWait, Duration checkInterval,
        Set<String> attributes, ReceiveProperties.Rabbitmq rabbitmq,
        ReceiveProperties.Artemis artemis,
        ReceiveProperties.Eventhubs eventhubs)
{

    /** The bean name of the RabbitMQ connection factory. */
    public static final String RABBITMQ_BEAN = "sdhIntakeRabbit";

    /**
     * The bean name of the SQS poll strategy, which tells the broker's
     * health of each poll.
     */
    public static final String POLL_STRATEGY_BEAN = "sdhIntakePolls";

    /** The bean name of the Event Hubs checkpoint store. */
    public static final String CHECKPOINTS_BEAN = "sdhIntakeCheckpoints";

    /**
     * The bean name of the Event Hubs start positions: the earliest event,
     * for a partition with no checkpoint.
     */
    public static final String POSITIONS_BEAN = "sdhIntakeEventPositions";

    /** The brokers' names, as the {@code type} setting takes them. */
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

    /**
     * Creates the settings, copying the attribute names.
     *
     * @param kind          which broker
     * @param uri           the consumer URI; null for Artemis
     * @param maxMessages   the most messages in a batch
     * @param maxBytes      the most bytes in a batch
     * @param maxWait       the longest a batch stays open
     * @param checkInterval how often the broker is checked
     * @param attributes    the attributes to read
     * @param rabbitmq      the RabbitMQ connection
     * @param artemis       the Artemis connection
     * @param eventhubs     the checkpoint store
     */
    public ReceiveSettings
    {
        Objects.requireNonNull(kind, "kind");
        attributes = Set.copyOf(attributes);
    }

    /**
     * Checks the settings and works out the consumer URI.
     *
     * @param broker     the settings as configured
     * @param prefix     where they are configured, such as
     *                   {@code sdh.landing.broker}, to name them in errors
     * @param attributes the message attributes or headers to read
     * @return the checked settings
     * @throws IllegalArgumentException if a setting is missing or would let
     *                                  a message be acknowledged before its
     *                                  batch is handled
     */
    public static ReceiveSettings of(ReceiveProperties broker, String prefix,
            Set<String> attributes)
    {
        if (broker.type() == null || broker.type().isBlank())
        {
            throw new IllegalArgumentException(prefix + ".type is"
                    + " required: " + TYPES);
        }
        BrokerKind kind;
        try
        {
            kind = BrokerKind.valueOf(
                    broker.type().trim().toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException unknown)
        {
            throw new IllegalArgumentException(prefix + ".type must"
                    + " be " + TYPES);
        }
        ReceiveProperties.Batch batch = broker.batch();
        if (batch.maxMessages() < 1 || batch.maxBytes().toBytes() < 1
                || batch.maxWait().isNegative() || batch.maxWait().isZero())
        {
            throw new IllegalArgumentException(prefix + ".batch"
                    + " limits must be positive");
        }
        if (broker.checkInterval() == null
                || broker.checkInterval().isNegative()
                || broker.checkInterval().isZero())
        {
            throw new IllegalArgumentException(
                    prefix + ".check-interval must be positive");
        }
        String uri = null;
        if (kind == BrokerKind.ARTEMIS)
        {
            ReceiveProperties.Artemis artemis = broker.artemis();
            if (broker.uri() != null && !broker.uri().isBlank())
            {
                throw new IllegalArgumentException(prefix + ".uri"
                        + " isn't used for artemis; set " + prefix
                        + ".artemis.*");
            }
            if (blank(artemis.brokerUrl()) || blank(artemis.queue()))
            {
                throw new IllegalArgumentException(prefix
                        + ".artemis.broker-url and queue are required");
            }
        }
        else
        {
            if (kind == BrokerKind.RABBITMQ
                    && blank(broker.rabbitmq().addresses()))
            {
                throw new IllegalArgumentException(prefix
                        + ".rabbitmq.addresses is required");
            }
            if (kind == BrokerKind.EVENTHUBS)
            {
                ReceiveProperties.Eventhubs eventhubs = broker.eventhubs();
                if (blank(eventhubs.checkpointContainer())
                        || blank(eventhubs.checkpointConnectionString())
                                == blank(eventhubs.checkpointEndpoint()))
                {
                    throw new IllegalArgumentException(prefix
                            + ".eventhubs needs checkpoint-container, and"
                            + " one of checkpoint-connection-string and"
                            + " checkpoint-endpoint");
                }
            }
            uri = consumerUri(kind, broker.uri(), batch, prefix, attributes);
        }
        return new ReceiveSettings(kind, uri, batch.maxMessages(),
                batch.maxBytes().toBytes(), batch.maxWait(),
                broker.checkInterval(), attributes, broker.rabbitmq(),
                broker.artemis(), broker.eventhubs());
    }

    /**
     * These settings with a Kafka consumer option set, unless the URI sets
     * it already. For any other broker, these settings as they are.
     *
     * @param option the Camel Kafka option, such as
     *               {@code maxPollIntervalMs}
     * @param value  its value
     * @return the settings
     */
    public ReceiveSettings withKafkaDefault(String option, String value)
    {
        if (kind != BrokerKind.KAFKA
                || Pattern.compile("[?&]" + Pattern.quote(option) + "=")
                        .matcher(uri).find())
        {
            return this;
        }
        return new ReceiveSettings(kind,
                uri + (uri.indexOf('?') < 0 ? "?" : "&") + option + "="
                        + value,
                maxMessages, maxBytes, maxWait, checkInterval, attributes,
                rabbitmq, artemis, eventhubs);
    }

    /**
     * The consumer URI with the options receiving needs.
     *
     * @param kind       the broker
     * @param uri        the URI as configured
     * @param batch      the batch limits
     * @param prefix     where the settings are configured, for errors
     * @param attributes the message attributes to ask for
     * @return the URI to consume from
     */
    static String consumerUri(BrokerKind kind, String uri,
            ReceiveProperties.Batch batch, String prefix,
            Set<String> attributes)
    {
        if (blank(uri))
        {
            throw new IllegalArgumentException(
                    prefix + ".uri is required for " + kind.scheme());
        }
        String trimmed = uri.trim();
        if (!trimmed.startsWith(kind.scheme() + ":"))
        {
            throw new IllegalArgumentException(prefix + ".uri must"
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
                    prefix + ".uri has a malformed query");
        }
        for (Map.Entry<String, Object> option : options.entrySet())
        {
            String name = option.getKey().toLowerCase(Locale.ROOT);
            if (SECRET_PARTS.stream().anyMatch(name::contains)
                    && !PLACEHOLDER.matcher(
                            String.valueOf(option.getValue())).matches())
            {
                throw new IllegalArgumentException(prefix + ".uri"
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
                            prefix + ".uri needs a groupId");
                }
                require(options, "batching", "true", prefix);
                require(options, "allowManualCommit", "true", prefix);
                require(options, "autoCommitEnable", "false", prefix);
                require(options, "maxPollRecords",
                        Integer.toString(batch.maxMessages()), prefix);
                require(options, "batchingIntervalMs",
                        Long.toString(batch.maxWait().toMillis()), prefix);
                require(options, "valueDeserializer",
                        "org.apache.kafka.common.serialization"
                                + ".ByteArrayDeserializer", prefix);
                options.putIfAbsent("autoOffsetReset", "earliest");
            }
            case SQS ->
            {
                require(options, "deleteAfterRead", "true", prefix);
                require(options, "pollStrategy", "#" + POLL_STRATEGY_BEAN,
                        prefix);
                require(options, "extendMessageVisibility", "true", prefix);
                options.putIfAbsent("visibilityTimeout", "60");
                options.putIfAbsent("maxMessagesPerPoll", "10");
                options.putIfAbsent("waitTimeSeconds", "20");
                for (String attribute : new TreeSet<>(attributes))
                {
                    withAttribute(options, attribute);
                }
            }
            case RABBITMQ ->
            {
                if (!options.containsKey("queues"))
                {
                    throw new IllegalArgumentException(
                            prefix + ".uri needs queues");
                }
                require(options, "acknowledgeMode", "MANUAL", prefix);
                require(options, "asyncConsumer", "true", prefix);
                require(options, "connectionFactory", "#" + RABBITMQ_BEAN,
                        prefix);
                options.putIfAbsent("autoDeclare", "false");
                options.putIfAbsent("prefetchCount",
                        Integer.toString(batch.maxMessages()));
            }
            case PUBSUB ->
            {
                require(options, "ackMode", "NONE", prefix);
                require(options, "synchronousPull", "false", prefix);
                // Above 0, Camel nacks at the limit, logging the message ID.
                require(options, "maxDeliveryAttempts", "0", prefix);
                options.putIfAbsent("maxAckExtensionPeriod", "3600");
            }
            case SERVICEBUS ->
            {
                require(options, "serviceBusReceiveMode", "PEEK_LOCK",
                        prefix);
                require(options, "enableDeadLettering", "false", prefix);
                require(options, "sessionEnabled", "false", prefix);
                refuse(options, "processorClient", prefix);
                options.putIfAbsent("maxAutoLockRenewDuration", "300000");
                if (Long.parseLong(String.valueOf(options.get(
                        "maxAutoLockRenewDuration"))) <= 0)
                {
                    throw new IllegalArgumentException(prefix
                            + ".uri option maxAutoLockRenewDuration must be"
                            + " positive, so held messages keep their lock");
                }
            }
            case EVENTHUBS ->
            {
                require(options, "checkpointStore", "#" + CHECKPOINTS_BEAN,
                        prefix);
                require(options, "eventPosition", "#" + POSITIONS_BEAN,
                        prefix);
                for (String blob : List.of("blobAccountName", "blobAccessKey",
                        "blobContainerName", "blobStorageSharedKeyCredential"))
                {
                    refuse(options, blob, prefix);
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
     * Sets an option receiving needs, refusing a different value.
     *
     * @param options the options
     * @param name    the option
     * @param value   the value it must have
     * @param prefix  where the settings are configured, for errors
     */
    private static void require(Map<String, Object> options, String name,
            String value, String prefix)
    {
        Object set = options.putIfAbsent(name, value);
        if (set != null && !String.valueOf(set).equalsIgnoreCase(value))
        {
            throw new IllegalArgumentException(prefix + ".uri option " + name
                    + " must be " + value + ": anything else could"
                    + " acknowledge messages before they are handled");
        }
    }

    /**
     * Asks SQS for a message attribute as well as any configured, unless
     * all are asked for.
     *
     * @param options the options
     * @param name    the attribute
     */
    private static void withAttribute(Map<String, Object> options,
            String name)
    {
        Object set = options.get("messageAttributeNames");
        String names = set == null ? "" : String.valueOf(set).strip();
        if (names.isEmpty())
        {
            options.put("messageAttributeNames", name);
        }
        else if (!List.of(names.split("\\s*,\\s*")).contains(name)
                && !names.equalsIgnoreCase("All")
                && !names.equals(".*"))
        {
            options.put("messageAttributeNames", names + "," + name);
        }
    }

    /**
     * Refuses an option the node sets up itself.
     *
     * @param options the options
     * @param name    the option
     * @param prefix  where the settings are configured, for errors
     */
    private static void refuse(Map<String, Object> options, String name,
            String prefix)
    {
        if (options.containsKey(name))
        {
            throw new IllegalArgumentException(prefix + ".uri option " + name
                    + " isn't allowed: the node sets it up");
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
