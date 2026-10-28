package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.*;

import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.publish.ClosablePublisher;
import com.sdh.healthcare.connector.file.publish.Publishers;
import com.sdh.healthcare.connector.file.publish.QueueType;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * Design §10.3 sqs on a FIFO queue: MessageGroupId = fileId, MessageDeduplicationId = messageId. Runs against
 * ElasticMQ, an SQS-compatible server (no cloud account or LocalStack token needed).
 */
class SqsIT {
    static GenericContainer<?> elasticmq;

    @BeforeAll
    static void start() {
        requireDocker();
        elasticmq = new GenericContainer<>("softwaremill/elasticmq-native:1.7.1")
                .withExposedPorts(9324)
                .waitingFor(Wait.forListeningPort());
        elasticmq.start();
    }

    @AfterAll
    static void stop() {
        if (elasticmq != null) {
            elasticmq.stop();
        }
    }

    @Test
    void fifoGroupsAndDeduplicates() throws Exception {
        String endpoint = "http://" + elasticmq.getHost() + ":" + elasticmq.getMappedPort(9324);
        SqsClient sqs = SqsClient.builder().endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))).build();
        String queueUrl = sqs.createQueue(b -> b.queueName("sdh.fifo").attributes(Map.of(
                QueueAttributeName.FIFO_QUEUE, "true",
                QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "false"))).queueUrl();

        List<OutboundMessage> sent = interleaved(UUID.randomUUID(), UUID.randomUUID(), 25);
        try (DefaultCamelContext camel = new DefaultCamelContext()) {
            camel.start();
            String uri = QueueType.SQS.effectiveUri("aws2-sqs:sdh.fifo?overrideEndpoint=true&uriEndpointOverride="
                    + endpoint + "&region=us-east-1&accessKey=x&secretKey=x&autoCreateQueue=false");
            try (ClosablePublisher pub = Publishers.create(camel, QueueType.SQS, uri)) {
                publishAll(pub, sent);
                publishAll(pub, sent.subList(0, 6)); // same message IDs again: dropped by FIFO dedup
            }
        }

        List<Received> got = new ArrayList<>();
        while (true) {
            List<Message> batch = sqs.receiveMessage(b -> b.queueUrl(queueUrl).maxNumberOfMessages(10)
                    .waitTimeSeconds(1).messageAttributeNames("All")
                    .messageSystemAttributeNames(MessageSystemAttributeName.MESSAGE_GROUP_ID)).messages();
            if (batch.isEmpty()) {
                break;
            }
            for (Message m : batch) {
                Map<String, MessageAttributeValue> a = m.messageAttributes();
                got.add(new Received(val(a, "messageId"), val(a, "type"), val(a, "fileId"), val(a, "contentType"),
                        m.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID), m.body()));
                sqs.deleteMessage(b -> b.queueUrl(queueUrl).receiptHandle(m.receiptHandle()));
            }
        }
        sqs.close();
        assertDelivered(sent, got, true);
    }

    static String val(Map<String, MessageAttributeValue> a, String k) {
        MessageAttributeValue v = a.get(k);
        return v == null ? null : v.stringValue();
    }
}
