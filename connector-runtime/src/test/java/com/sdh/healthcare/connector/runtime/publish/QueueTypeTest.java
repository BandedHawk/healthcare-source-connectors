/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.messaging.servicebus.ServiceBusMessage;
import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link QueueType}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class QueueTypeTest
{

    /** The file the message is about. */
    static final UUID FILE =
            UUID.fromString("01a18433-40a8-79fb-8f3a-86092423a553");

    /** The message. */
    static final OutboundMessage MESSAGE = new OutboundMessage("sec-abc",
            MessageType.SECTION_STORED, FILE, new byte[0]);

    /**
     * Applies a type's headers to a new exchange.
     *
     * @param type the type
     * @return the exchange
     */
    static Exchange apply(QueueType type)
    {
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        type.apply(exchange, MESSAGE);
        return exchange;
    }

    /** Durability options are added unless the URI sets them. */
    @Test
    void addsDurabilityOptionsUnlessSetExplicitly()
    {
        assertThat(QueueType.KAFKA.effectiveUri(
                "kafka:sdh.sections?brokers=k:9093")).isEqualTo(
                        "kafka:sdh.sections?brokers=k:9093"
                                + "&enableIdempotence=true"
                                + "&requestRequiredAcks=all");
        assertThat(QueueType.KAFKA.effectiveUri(
                "kafka:t?requestRequiredAcks=1"))
                .isEqualTo("kafka:t?requestRequiredAcks=1"
                        + "&enableIdempotence=true");
        assertThat(QueueType.RABBITMQ.effectiveUri("spring-rabbitmq:sdh"))
                .isEqualTo("spring-rabbitmq:sdh?confirm=enabled");
        assertThat(QueueType.SQS.effectiveUri("aws2-sqs:sdh.fifo")).isEqualTo(
                "aws2-sqs:sdh.fifo"
                        + "?messageDeduplicationIdStrategy=useExchangeId"
                        + "&messageGroupIdStrategy=usePropertyValue");
        assertThat(QueueType.SQS.effectiveUri("aws2-sqs:standard"))
                .isEqualTo("aws2-sqs:standard");
        assertThat(QueueType.PUBSUB.effectiveUri("google-pubsub:proj:topic"))
                .isEqualTo("google-pubsub:proj:topic"
                        + "?messageOrderingEnabled=true");
        assertThatThrownBy(() -> QueueType.KAFKA.effectiveUri("jms:queue:x"))
                .hasMessageContaining("must start with 'kafka:'");
        assertThatThrownBy(() -> QueueType.of("mqtt"))
                .hasMessageContaining("Unknown");
    }

    /**
     * Each type sets its ordering key and dedup ID, and only the allowed
     * metadata.
     */
    @Test
    void setsOrderingKeyDedupIdAndOnlyAllowedMetadata()
    {
        Exchange kafka = apply(QueueType.KAFKA);
        assertThat(kafka.getIn().getHeaders()).containsOnlyKeys("messageId",
                "type", "fileId", "contentType", "CamelKafkaKey");
        assertThat(kafka.getIn().getHeader("CamelKafkaKey"))
                .isEqualTo(FILE.toString());
        assertThat(kafka.getIn().getHeader("type"))
                .isEqualTo("section.stored");

        Exchange sqs = apply(QueueType.SQS);
        assertThat(sqs.getProperty("CamelAwsMessageGroupId"))
                .isEqualTo(FILE.toString());
        assertThat(sqs.getExchangeId()).isEqualTo("sec-abc");

        Exchange artemis = apply(QueueType.ARTEMIS);
        assertThat(artemis.getIn().getHeader("JMSXGroupID"))
                .isEqualTo(FILE.toString());
        assertThat(artemis.getIn().getHeader("_AMQ_DUPL_ID"))
                .isEqualTo("sec-abc");

        ServiceBusMessage serviceBus = ServiceBusPublisher.toMessage(MESSAGE);
        assertThat(serviceBus.getSessionId()).isEqualTo(FILE.toString());
        assertThat(serviceBus.getMessageId()).isEqualTo("sec-abc");
        assertThat(serviceBus.getContentType())
                .isEqualTo(OutboundMessage.CONTENT_TYPE);
        assertThat(serviceBus.getApplicationProperties())
                .containsEntry("type", "section.stored");

        Exchange pubsub = apply(QueueType.PUBSUB);
        assertThat(pubsub.getIn().getHeader("CamelGooglePubsubOrderingKey"))
                .isEqualTo(FILE.toString());
        assertThat(pubsub.getIn().getHeader("CamelGooglePubsubAttributes",
                Map.class)).containsEntry("messageId", "sec-abc");

        assertThat(apply(QueueType.RABBITMQ).getIn()
                .getHeader("CamelSpringRabbitmqMessageId"))
                .isEqualTo("sec-abc");
        assertThat(apply(QueueType.EVENTHUBS).getIn()
                .getHeader("CamelAzureEventHubsPartitionKey"))
                .isEqualTo(FILE.toString());
    }
}
