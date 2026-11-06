/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import com.azure.core.util.BinaryData;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.azure.eventhubs.EventHubsConstants;
import org.apache.camel.component.azure.servicebus.ServiceBusConstants;
import org.apache.camel.component.google.pubsub.GooglePubsubConstants;
import org.apache.camel.component.google.pubsub.consumer.GooglePubsubAcknowledge;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.spi.ShutdownPrepared;
import org.apache.camel.support.service.ServiceSupport;

/**
 * Consumes a broker through Camel and hands its messages to a
 * {@link BatchHandler} in batches (design §5.3.16). No message is
 * acknowledged before its batch is handled:
 * <ul>
 *   <li><b>Kafka:</b> the consumer hands over each poll as one exchange
 *       ({@code batching}); it is handled, in batches of at most
 *       {@code max-messages} and {@code max-bytes}, then its offsets are
 *       committed by hand.</li>
 *   <li><b>SQS, Service Bus, Event Hubs and RabbitMQ:</b> exchanges are
 *       held open until their batch is handled ({@link HeldBatches}). Then
 *       SQS deletes each message, Service Bus completes it (renewing its
 *       lock while it is held), Event Hubs moves its checkpoint, and
 *       RabbitMQ messages are acknowledged on their channel.</li>
 *   <li><b>Pub/Sub:</b> with {@code ackMode=NONE}, each exchange carries a
 *       way to acknowledge its message later, and completes at once; the
 *       message is acknowledged once its batch is handled. The client
 *       extends its deadline meanwhile.</li>
 * </ul>
 * Each message's attributes or headers named in
 * {@link ReceiveSettings#attributes()} are read with it, as text. A batch
 * that can't be handled yet is tried again in place, with a growing
 * pause, rather than handed back: that would raise its messages' delivery
 * counts, and dead-letter them during a long outage on a broker with a
 * delivery limit. Messages still held when the route stops are handed back.
 * The route starts after startup recovery. Messages are never logged.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class CamelReceiveRoute extends RouteBuilder
{

    /** The SQS message ID header. */
    static final String SQS_MESSAGE_ID = "CamelAwsSqsMessageId";

    /** The RabbitMQ message ID header, from the AMQP properties. */
    static final String RABBITMQ_MESSAGE_ID = "CamelSpringRabbitmqMessageId";

    /** The RabbitMQ delivery tag header. */
    static final String RABBITMQ_DELIVERY_TAG =
            "CamelSpringRabbitmqDeliveryTag";

    /** The exchange property holding the RabbitMQ channel. */
    static final String RABBITMQ_CHANNEL = "CamelSpringRabbitmqChannel";

    /** The route's ID, by which it is started and stopped. */
    private final String routeId;

    /** The broker's settings. */
    private final ReceiveSettings settings;

    /** Handles batches. */
    private final BatchHandler handler;

    /** The first pause after a failure. */
    private final Duration initialBackoff;

    /** The longest pause after failures. */
    private final Duration maxBackoff;

    /** Whether the Kafka route is stopping, so a retry should give up. */
    private volatile boolean stopping;

    /**
     * Creates the route. It doesn't start with the Camel context: the caller
     * starts it by its ID when ready.
     *
     * @param routeId        the route's ID
     * @param settings       the broker's settings; not Artemis
     * @param handler        handles batches
     * @param initialBackoff the first pause after a failure
     * @param maxBackoff     the longest pause after failures
     */
    public CamelReceiveRoute(String routeId, ReceiveSettings settings,
            BatchHandler handler, Duration initialBackoff, Duration maxBackoff)
    {
        this.routeId = routeId;
        this.settings = settings;
        this.handler = handler;
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
    }

    @Override
    public void configure()
    {
        errorHandler(noErrorHandler());
        RouteDefinition route = from(settings.uri()).routeId(routeId)
                .autoStartup(false);
        switch (settings.kind())
        {
            case KAFKA -> route.process(new KafkaPolls());
            case SQS -> route.process(held(new Sqs(settings.attributes())));
            case RABBITMQ -> route.process(
                    held(new Rabbitmq(settings.attributes())));
            case PUBSUB -> route.process(
                    held(new Pubsub(settings.attributes())));
            case SERVICEBUS -> route.process(
                    held(new Servicebus(settings.attributes())));
            case EVENTHUBS -> route.process(
                    held(new Eventhubs(settings.attributes())));
            default -> throw new IllegalStateException(
                    "Not a Camel broker: " + settings.kind());
        }
    }

    /**
     * The batcher for a broker that acknowledges as exchanges complete.
     *
     * @param broker reads and settles the broker's messages
     * @return the processor
     */
    private HeldBatches held(HeldBatches.Broker broker)
    {
        return new HeldBatches(handler, broker, settings.maxMessages(),
                settings.maxBytes(), settings.maxWait(), initialBackoff,
                maxBackoff, System::nanoTime);
    }

    /**
     * The Kafka route's processor. It tells a batch being retried to give
     * up as soon as Camel prepares the route to stop: the poll's offsets
     * aren't committed, so its records come again, and the route stops at
     * once rather than when its shutdown times out.
     */
    private final class KafkaPolls extends ServiceSupport
            implements Processor, ShutdownPrepared
    {

        /** Creates the processor. */
        KafkaPolls()
        {
        }

        @Override
        public void process(Exchange exchange) throws Exception
        {
            kafka(exchange);
        }

        @Override
        public void prepareShutdown(boolean suspendOnly, boolean forced)
        {
            stopping = true;
        }

        @Override
        protected void doStart()
        {
            stopping = false;
        }

        @Override
        protected void doStop()
        {
            stopping = true;
        }
    }

    /**
     * Handles a Kafka poll, then commits its offsets.
     *
     * @param exchange the poll, its records as a list of exchanges
     * @throws Exception if the node is stopping, so nothing is committed
     */
    private void kafka(Exchange exchange) throws Exception
    {
        List<?> records = exchange.getMessage().getBody(List.class);
        if (records == null || records.isEmpty())
        {
            return;
        }
        List<BrokerMessage> batch = new ArrayList<>();
        long bytes = 0;
        for (Object polled : records)
        {
            BrokerMessage message = kafkaMessage((Exchange) polled,
                    settings.attributes());
            long size = message.body().length;
            if (!batch.isEmpty() && (batch.size() >= settings.maxMessages()
                    || bytes + size > settings.maxBytes()))
            {
                handleUntilDone(batch);
                batch = new ArrayList<>();
                bytes = 0;
            }
            batch.add(message);
            bytes += size;
        }
        handleUntilDone(batch);
        KafkaManualCommit commit = exchange.getMessage().getHeader(
                KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        if (commit != null)
        {
            commit.commit();
        }
    }

    /**
     * A Kafka record as a message: its value, and its topic, partition and
     * offset as its ID.
     *
     * @param polled     the record's exchange
     * @param attributes the headers to read
     * @return the message
     */
    static BrokerMessage kafkaMessage(Exchange polled,
            Set<String> attributes)
    {
        org.apache.camel.Message in = polled.getMessage();
        byte[] body = in.getBody(byte[].class);
        String id = in.getHeader(KafkaConstants.TOPIC, String.class) + "/"
                + in.getHeader(KafkaConstants.PARTITION, Integer.class) + "/"
                + in.getHeader(KafkaConstants.OFFSET, Long.class);
        return new BrokerMessage(body == null ? new byte[0] : body, id,
                headers(in, attributes));
    }

    /**
     * A header's value as text: bytes, as Kafka gives them, as UTF-8.
     *
     * @param value the value; null if not set
     * @return the text; null if not set
     */
    static String text(Object value)
    {
        if (value instanceof byte[] bytes)
        {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value == null ? null : value.toString();
    }

    /**
     * The asked-for headers of a message, as text.
     *
     * @param in         the message
     * @param attributes the headers to read
     * @return the ones set
     */
    static Map<String, String> headers(org.apache.camel.Message in,
            Set<String> attributes)
    {
        Map<String, String> found = new HashMap<>();
        for (String name : attributes)
        {
            String value = text(in.getHeader(name));
            if (value != null)
            {
                found.put(name, value);
            }
        }
        return found;
    }

    /**
     * The asked-for entries of a map header, such as Pub/Sub's attributes,
     * as text.
     *
     * @param in         the message
     * @param header     the map header
     * @param attributes the entries to read
     * @return the ones set
     */
    static Map<String, String> entries(org.apache.camel.Message in,
            String header, Set<String> attributes)
    {
        Map<?, ?> entries = in.getHeader(header, Map.class);
        Map<String, String> found = new HashMap<>();
        if (entries != null)
        {
            for (String name : attributes)
            {
                String value = text(entries.get(name));
                if (value != null)
                {
                    found.put(name, value);
                }
            }
        }
        return found;
    }

    /**
     * Handles a batch, trying again in place until it is handled.
     *
     * @param batch the messages
     * @throws Exception if the node is stopping, or the thread interrupted
     */
    private void handleUntilDone(List<BrokerMessage> batch) throws Exception
    {
        int failures = 0;
        while (true)
        {
            switch (handler.handle(batch))
            {
                case DONE ->
                {
                    return;
                }
                case FATAL -> throw new HeldBatches.NotHandledException();
                case RETRY ->
                {
                    failures++;
                    long until = System.nanoTime() + 1_000_000L * Math.min(
                            maxBackoff.toMillis(), initialBackoff.toMillis()
                                    << Math.min(failures - 1, 20));
                    while (!stopping && System.nanoTime() < until)
                    {
                        Thread.sleep(Math.min(50, Math.max(1,
                                (until - System.nanoTime()) / 1_000_000)));
                    }
                    if (stopping)
                    {
                        throw new HeldBatches.NotHandledException();
                    }
                }
            }
        }
    }

    /**
     * A message's body as bytes, whatever form the component gives it in.
     *
     * @param in the message
     * @return the body; empty if none
     */
    static byte[] body(org.apache.camel.Message in)
    {
        Object body = in.getBody();
        if (body instanceof BinaryData binary)
        {
            return binary.toBytes();
        }
        byte[] bytes = in.getBody(byte[].class);
        return bytes == null ? new byte[0] : bytes;
    }

    /**
     * A broker whose messages carry attributes: remembers which to read.
     */
    abstract static class WithAttributes implements HeldBatches.Broker
    {

        /** The attributes or headers to read. */
        final Set<String> attributes;

        /**
         * Creates the broker's adapter.
         *
         * @param attributes the attributes or headers to read
         */
        WithAttributes(Set<String> attributes)
        {
            this.attributes = attributes;
        }
    }

    /**
     * Pub/Sub, acknowledging by hand: the broker's message ID, and an
     * acknowledgement used once the batch is handled.
     */
    static final class Pubsub extends WithAttributes
    {

        /**
         * Creates the adapter.
         *
         * @param attributes the message attributes to read
         */
        Pubsub(Set<String> attributes)
        {
            super(attributes);
        }

        @Override
        public BrokerMessage message(Exchange exchange)
        {
            org.apache.camel.Message in = exchange.getMessage();
            return new BrokerMessage(body(in), in.getHeader(
                    GooglePubsubConstants.MESSAGE_ID, String.class),
                    entries(in, GooglePubsubConstants.ATTRIBUTES, attributes));
        }

        @Override
        public void settle(Exchange exchange, boolean handled)
        {
            GooglePubsubAcknowledge acknowledge = exchange.getMessage()
                    .getHeader(GooglePubsubConstants.GOOGLE_PUBSUB_ACKNOWLEDGE,
                            GooglePubsubAcknowledge.class);
            if (acknowledge == null)
            {
                throw new IllegalStateException(
                        "Pub/Sub gave no acknowledgement");
            }
            if (handled)
            {
                acknowledge.ack(exchange);
            }
            else
            {
                acknowledge.nack(exchange);
            }
        }

        @Override
        public boolean holdsExchange()
        {
            return false;
        }
    }

    /**
     * Service Bus: the broker's sequence number is the ID, since it stays
     * the same when a message is delivered again and a producer's
     * {@code messageId} is optional.
     */
    static final class Servicebus extends WithAttributes
    {

        /**
         * Creates the adapter.
         *
         * @param attributes the application properties to read
         */
        Servicebus(Set<String> attributes)
        {
            super(attributes);
        }

        @Override
        public BrokerMessage message(Exchange exchange)
        {
            org.apache.camel.Message in = exchange.getMessage();
            Long sequence = in.getHeader(ServiceBusConstants.SEQUENCE_NUMBER,
                    Long.class);
            return new BrokerMessage(body(in),
                    sequence == null ? null : sequence.toString(),
                    entries(in, ServiceBusConstants.APPLICATION_PROPERTIES,
                            attributes));
        }
    }

    /** Event Hubs: the partition and sequence number are the ID. */
    static final class Eventhubs extends WithAttributes
    {

        /**
         * Creates the adapter.
         *
         * @param attributes the event properties to read
         */
        Eventhubs(Set<String> attributes)
        {
            super(attributes);
        }

        @Override
        public BrokerMessage message(Exchange exchange)
        {
            org.apache.camel.Message in = exchange.getMessage();
            String partition = in.getHeader(EventHubsConstants.PARTITION_ID,
                    String.class);
            Long sequence = in.getHeader(EventHubsConstants.SEQUENCE_NUMBER,
                    Long.class);
            return new BrokerMessage(body(in),
                    partition == null || sequence == null ? null
                            : partition + "/" + sequence,
                    entries(in, EventHubsConstants.METADATA, attributes));
        }
    }

    /** SQS: the body is text; the broker gives each message an ID. */
    static final class Sqs extends WithAttributes
    {

        /**
         * Creates the adapter.
         *
         * @param attributes the message attributes to read
         */
        Sqs(Set<String> attributes)
        {
            super(attributes);
        }

        @Override
        public BrokerMessage message(Exchange exchange)
        {
            org.apache.camel.Message in = exchange.getMessage();
            byte[] body = in.getBody(byte[].class);
            return new BrokerMessage(body == null ? new byte[0] : body,
                    in.getHeader(SQS_MESSAGE_ID, String.class),
                    headers(in, attributes));
        }
    }

    /**
     * RabbitMQ in manual mode: each message is acknowledged, or returned to
     * the queue, on its channel. Its ID is the producer's
     * {@code message_id}, if it set one.
     */
    static final class Rabbitmq extends WithAttributes
    {

        /**
         * Creates the adapter.
         *
         * @param attributes the message headers to read
         */
        Rabbitmq(Set<String> attributes)
        {
            super(attributes);
        }

        @Override
        public BrokerMessage message(Exchange exchange)
        {
            org.apache.camel.Message in = exchange.getMessage();
            byte[] body = in.getBody(byte[].class);
            return new BrokerMessage(body == null ? new byte[0] : body,
                    in.getHeader(RABBITMQ_MESSAGE_ID, String.class),
                    headers(in, attributes));
        }

        @Override
        public void settle(Exchange exchange, boolean handled)
        {
            Channel channel = exchange.getProperty(RABBITMQ_CHANNEL,
                    Channel.class);
            Long tag = exchange.getMessage().getHeader(RABBITMQ_DELIVERY_TAG,
                    Long.class);
            if (channel == null || tag == null)
            {
                throw new IllegalStateException(
                        "RabbitMQ gave no channel or delivery tag");
            }
            try
            {
                if (handled)
                {
                    channel.basicAck(tag, false);
                }
                else
                {
                    channel.basicNack(tag, false, true);
                }
            }
            catch (IOException failure)
            {
                throw new UncheckedIOException(failure);
            }
        }
    }
}
