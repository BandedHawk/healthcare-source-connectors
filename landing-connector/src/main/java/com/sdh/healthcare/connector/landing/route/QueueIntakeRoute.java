/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.route;

import com.rabbitmq.client.Channel;
import com.sdh.healthcare.connector.landing.config.BrokerSettings;
import com.sdh.healthcare.connector.landing.intake.BatchService;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;

/**
 * Consumes a queue intake's broker through Camel and lands its messages in
 * batches (design §5.3.16). No message is acknowledged before its batch is
 * committed:
 * <ul>
 *   <li><b>Kafka:</b> the consumer hands over each poll as one exchange
 *       ({@code batching}); it is landed, in batches of at most
 *       {@code max-messages} and {@code max-bytes}, then its offsets are
 *       committed by hand. A batch that can't be landed is tried again in
 *       place, with a growing pause, since Kafka has no way to deliver one
 *       message again.</li>
 *   <li><b>SQS and RabbitMQ:</b> exchanges are held open until their batch
 *       is landed ({@link HeldBatches}). SQS deletes a message when its
 *       exchange completes, and redelivers it after its visibility timeout
 *       if the exchange failed; RabbitMQ messages are acknowledged, or
 *       returned to the queue, on their channel.</li>
 * </ul>
 * The route starts after startup recovery. Messages are never logged.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class QueueIntakeRoute extends RouteBuilder
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

    /** The broker's settings. */
    private final BrokerSettings settings;

    /** Lands batches. */
    private final BatchService service;

    /** The first pause after a failure. */
    private final Duration initialBackoff;

    /** The longest pause after failures. */
    private final Duration maxBackoff;

    /**
     * Creates the route.
     *
     * @param settings       the broker's settings
     * @param service        lands batches
     * @param initialBackoff the first pause after a failure
     * @param maxBackoff     the longest pause after failures
     */
    public QueueIntakeRoute(BrokerSettings settings, BatchService service,
            Duration initialBackoff, Duration maxBackoff)
    {
        this.settings = settings;
        this.service = service;
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
    }

    @Override
    public void configure()
    {
        errorHandler(noErrorHandler());
        var route = from(settings.uri()).routeId(IntakeRoute.ROUTE_ID)
                .autoStartup(false);
        switch (settings.kind())
        {
            case KAFKA -> route.process(this::kafka);
            case SQS -> route.process(held(new Sqs()));
            case RABBITMQ -> route.process(held(new Rabbitmq()));
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
        return new HeldBatches(service::land, broker, settings.maxMessages(),
                settings.maxBytes(), settings.maxWait(), initialBackoff,
                maxBackoff, System::nanoTime);
    }

    /**
     * Lands a Kafka poll, then commits its offsets.
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
        List<BatchService.Message> batch = new ArrayList<>();
        long bytes = 0;
        for (Object record : records)
        {
            BatchService.Message message = kafkaMessage((Exchange) record);
            long size = message.body().length;
            if (!batch.isEmpty() && (batch.size() >= settings.maxMessages()
                    || bytes + size > settings.maxBytes()))
            {
                landUntilDone(batch);
                batch = new ArrayList<>();
                bytes = 0;
            }
            batch.add(message);
            bytes += size;
        }
        landUntilDone(batch);
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
     * @param record the record's exchange
     * @return the message
     */
    static BatchService.Message kafkaMessage(Exchange record)
    {
        var in = record.getMessage();
        byte[] body = in.getBody(byte[].class);
        String id = in.getHeader(KafkaConstants.TOPIC, String.class) + "/"
                + in.getHeader(KafkaConstants.PARTITION, Integer.class) + "/"
                + in.getHeader(KafkaConstants.OFFSET, Long.class);
        return new BatchService.Message(body == null ? new byte[0] : body,
                id);
    }

    /**
     * Lands a batch, trying again in place until it is landed.
     *
     * @param batch the messages
     * @throws Exception if the node is stopping, or the thread interrupted
     */
    private void landUntilDone(List<BatchService.Message> batch)
            throws Exception
    {
        int failures = 0;
        while (true)
        {
            BatchService.Result result = service.land(batch);
            switch (result.status())
            {
                case LANDED, SKIPPED ->
                {
                    return;
                }
                case FATAL -> throw new HeldBatches.NotLandedException();
                default ->
                {
                    failures++;
                    Thread.sleep(Math.min(maxBackoff.toMillis(),
                            initialBackoff.toMillis()
                                    << Math.min(failures - 1, 20)));
                }
            }
        }
    }

    /** SQS: the body is text; the broker gives each message an ID. */
    static final class Sqs implements HeldBatches.Broker
    {

        @Override
        public BatchService.Message message(Exchange exchange)
        {
            var in = exchange.getMessage();
            byte[] body = in.getBody(byte[].class);
            return new BatchService.Message(body == null ? new byte[0] : body,
                    in.getHeader(SQS_MESSAGE_ID, String.class));
        }
    }

    /**
     * RabbitMQ in manual mode: each message is acknowledged, or returned to
     * the queue, on its channel. Its ID is the producer's
     * {@code message_id}, if it set one.
     */
    static final class Rabbitmq implements HeldBatches.Broker
    {

        @Override
        public BatchService.Message message(Exchange exchange)
        {
            var in = exchange.getMessage();
            byte[] body = in.getBody(byte[].class);
            return new BatchService.Message(body == null ? new byte[0] : body,
                    in.getHeader(RABBITMQ_MESSAGE_ID, String.class));
        }

        @Override
        public void settle(Exchange exchange, boolean landed)
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
                if (landed)
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
