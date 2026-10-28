package com.sdh.healthcare.connector.file.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

class QueueTypeTest {
    static final UUID FILE = UUID.fromString("01a18433-40a8-79fb-8f3a-86092423a553");
    static final OutboundMessage MSG = new OutboundMessage("sec-abc", MessageType.SECTION_STORED, FILE, new byte[0]);

    static Exchange apply(QueueType t) {
        Exchange e = new DefaultExchange(new DefaultCamelContext());
        t.apply(e, MSG);
        return e;
    }

    @Test
    void addsDurabilityOptionsUnlessSetExplicitly() {
        assertThat(QueueType.KAFKA.effectiveUri("kafka:sdh.sections?brokers=k:9093"))
                .isEqualTo("kafka:sdh.sections?brokers=k:9093&enableIdempotence=true&requestRequiredAcks=all");
        assertThat(QueueType.KAFKA.effectiveUri("kafka:t?requestRequiredAcks=1"))
                .isEqualTo("kafka:t?requestRequiredAcks=1&enableIdempotence=true");
        assertThat(QueueType.RABBITMQ.effectiveUri("spring-rabbitmq:sdh")).isEqualTo("spring-rabbitmq:sdh?confirm=enabled");
        assertThat(QueueType.SQS.effectiveUri("aws2-sqs:sdh.fifo")).isEqualTo(
                "aws2-sqs:sdh.fifo?messageDeduplicationIdStrategy=useExchangeId&messageGroupIdStrategy=usePropertyValue");
        assertThat(QueueType.SQS.effectiveUri("aws2-sqs:standard")).isEqualTo("aws2-sqs:standard");
        assertThat(QueueType.PUBSUB.effectiveUri("google-pubsub:proj:topic"))
                .isEqualTo("google-pubsub:proj:topic?messageOrderingEnabled=true");
        assertThatThrownBy(() -> QueueType.KAFKA.effectiveUri("jms:queue:x"))
                .hasMessageContaining("must start with 'kafka:'");
        assertThatThrownBy(() -> QueueType.of("mqtt")).hasMessageContaining("Unknown");
    }

    @Test
    void setsOrderingKeyDedupIdAndOnlyAllowedMetadata() {
        Exchange k = apply(QueueType.KAFKA);
        assertThat(k.getIn().getHeaders()).containsOnlyKeys("messageId", "type", "fileId", "contentType",
                "CamelKafkaKey");
        assertThat(k.getIn().getHeader("CamelKafkaKey")).isEqualTo(FILE.toString());
        assertThat(k.getIn().getHeader("type")).isEqualTo("section.stored");

        Exchange s = apply(QueueType.SQS);
        assertThat(s.getProperty("CamelAwsMessageGroupId")).isEqualTo(FILE.toString());
        assertThat(s.getExchangeId()).isEqualTo("sec-abc");

        Exchange a = apply(QueueType.ARTEMIS);
        assertThat(a.getIn().getHeader("JMSXGroupID")).isEqualTo(FILE.toString());
        assertThat(a.getIn().getHeader("_AMQ_DUPL_ID")).isEqualTo("sec-abc");

        var sb = ServiceBusPublisher.toMessage(MSG);
        assertThat(sb.getSessionId()).isEqualTo(FILE.toString());
        assertThat(sb.getMessageId()).isEqualTo("sec-abc");
        assertThat(sb.getContentType()).isEqualTo(OutboundMessage.CONTENT_TYPE);
        assertThat(sb.getApplicationProperties()).containsEntry("type", "section.stored");

        Exchange ps = apply(QueueType.PUBSUB);
        assertThat(ps.getIn().getHeader("CamelGooglePubsubOrderingKey")).isEqualTo(FILE.toString());
        assertThat(ps.getIn().getHeader("CamelGooglePubsubAttributes", Map.class)).containsEntry("messageId", "sec-abc");

        assertThat(apply(QueueType.RABBITMQ).getIn().getHeader("CamelSpringRabbitmqMessageId")).isEqualTo("sec-abc");
        assertThat(apply(QueueType.EVENTHUBS).getIn().getHeader("CamelAzureEventHubsPartitionKey"))
                .isEqualTo(FILE.toString());
    }
}
