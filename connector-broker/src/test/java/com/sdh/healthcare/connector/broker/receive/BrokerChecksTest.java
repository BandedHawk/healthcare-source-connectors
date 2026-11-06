/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.core.amqp.exception.AmqpErrorCondition;
import com.azure.core.amqp.exception.AmqpException;
import com.azure.core.http.HttpResponse;
import com.azure.messaging.servicebus.ServiceBusErrorSource;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.rabbitmq.client.AuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;
import io.grpc.Status;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.SqsException;

/**
 * Tests for how each broker's failures are read: which are refused
 * credentials, a missing permission, a missing queue or topic, and which
 * are the network.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class BrokerChecksTest
{

    /** The broker's health. */
    final RecordingHealth health = new RecordingHealth();

    /**
     * An SQS service error.
     *
     * @param code its error code
     * @return the error
     */
    static Exception sqs(String code)
    {
        return SqsException.builder().awsErrorDetails(
                AwsErrorDetails.builder().errorCode(code).build()).build();
    }

    /** SQS: each poll is reported, and each failure read. */
    @Test
    void readsSqsPolls()
    {
        SqsPollStrategy polls = new SqsPollStrategy(health);
        assertThat(polls.begin(null, null)).isTrue();
        assertThat(polls.rollback(null, null, 0,
                QueueDoesNotExistException.builder().build())).isFalse();
        assertThat(health.problem()).isEqualTo(BrokerProblem.NOT_FOUND);
        polls.commit(null, null, 0);
        assertThat(health.reachable()).isTrue();

        assertThat(SqsPollStrategy.classify(sqs("InvalidClientTokenId")))
                .isEqualTo(BrokerProblem.AUTH);
        assertThat(SqsPollStrategy.classify(new IllegalStateException(
                sqs("AccessDenied")))).isEqualTo(BrokerProblem.PERMISSION);
        assertThat(SqsPollStrategy.classify(sqs("ServiceUnavailable")))
                .isEqualTo(BrokerProblem.NETWORK);
        assertThat(SqsPollStrategy.classify(SdkClientException.create("no")))
                .isEqualTo(BrokerProblem.NETWORK);
    }

    /**
     * RabbitMQ: a refused login is told apart from the network; a
     * connection the node closes itself isn't an outage.
     */
    @Test
    void readsRabbitmqConnections()
    {
        ConnectionListener listener = RabbitmqCheck.listener(health);
        listener.onFailed(new AmqpConnectException(
                new AuthenticationFailureException("ACCESS_REFUSED")));
        assertThat(health.problem()).isEqualTo(BrokerProblem.AUTH);
        listener.onCreate(null);
        assertThat(health.reachable()).isTrue();
        listener.onShutDown(new ShutdownSignalException(true, true, null,
                null));
        assertThat(health.reachable()).isTrue();
        listener.onShutDown(new ShutdownSignalException(true, false, null,
                null));
        assertThat(health.problem()).isEqualTo(BrokerProblem.NETWORK);
        assertThat(RabbitmqCheck.classify(new AmqpConnectException(
                new IOException("refused")))).isEqualTo(BrokerProblem.NETWORK);
    }

    /**
     * Kafka: the admin client takes the endpoint's brokers, and only the
     * consumer's settings it knows, with the check's timeouts; its
     * failures are read.
     */
    @Test
    void readsKafka()
    {
        Properties consumer = new Properties();
        consumer.put("security.protocol", "SSL");
        consumer.put("group.id", "landing");
        consumer.put("key.deserializer", "x");
        Properties admin = KafkaCheck.adminProperties(consumer, "k:9093",
                Duration.ofSeconds(10));
        assertThat(admin).containsEntry("bootstrap.servers", "k:9093")
                .containsEntry("security.protocol", "SSL")
                .containsEntry("default.api.timeout.ms", "10000")
                .containsEntry("request.timeout.ms", "10000")
                .doesNotContainKeys("group.id", "key.deserializer");

        assertThat(KafkaCheck.classify(new SaslAuthenticationException("x")))
                .isEqualTo(BrokerProblem.AUTH);
        assertThat(KafkaCheck.classify(new TopicAuthorizationException("x")))
                .isEqualTo(BrokerProblem.PERMISSION);
        assertThat(KafkaCheck.classify(
                new UnknownTopicOrPartitionException("x")))
                .isEqualTo(BrokerProblem.NOT_FOUND);
        assertThat(KafkaCheck.classify(new TimeoutException("x")))
                .isEqualTo(BrokerProblem.NETWORK);
    }

    /**
     * A Pub/Sub error.
     *
     * @param code      its status
     * @param retryable whether the client retries it
     * @return the error
     */
    static ApiException pubsub(Status.Code code, boolean retryable)
    {
        return ApiExceptionFactory.createException(new IOException("x"),
                GrpcStatusCode.of(code), retryable);
    }

    /**
     * Pub/Sub: statuses are read, and a subscriber gives up, as Camel's
     * consumer decides, on any error it can't retry.
     */
    @Test
    void readsPubsub()
    {
        assertThat(PubsubCheck.classify(StatusCode.Code.NOT_FOUND))
                .isEqualTo(BrokerProblem.NOT_FOUND);
        assertThat(PubsubCheck.classify(StatusCode.Code.PERMISSION_DENIED))
                .isEqualTo(BrokerProblem.PERMISSION);
        assertThat(PubsubCheck.classify(StatusCode.Code.UNAUTHENTICATED))
                .isEqualTo(BrokerProblem.AUTH);
        assertThat(PubsubCheck.classify(StatusCode.Code.UNAVAILABLE))
                .isEqualTo(BrokerProblem.NETWORK);

        assertThat(WatchedPubsubComponent.gaveUpOn(
                pubsub(Status.Code.UNAVAILABLE, true))).isNull();
        assertThat(WatchedPubsubComponent.gaveUpOn(
                pubsub(Status.Code.NOT_FOUND, false)))
                .isEqualTo(BrokerProblem.NOT_FOUND);
        assertThat(WatchedPubsubComponent.gaveUpOn(new IllegalStateException(
                pubsub(Status.Code.PERMISSION_DENIED, false))))
                .isEqualTo(BrokerProblem.PERMISSION);
        assertThat(WatchedPubsubComponent.gaveUpOn(
                new IllegalStateException("x")))
                .isEqualTo(BrokerProblem.NETWORK);
    }

    /**
     * An AMQP refusal.
     *
     * @param condition why
     * @return the refusal
     */
    static AmqpException amqp(AmqpErrorCondition condition)
    {
        return new AmqpException(false, condition, "x", null);
    }

    /** Service Bus and Event Hubs: AMQP refusals are read. */
    @Test
    void readsAzureRefusals()
    {
        assertThat(ServicebusCheck.classify(new ServiceBusException(
                amqp(AmqpErrorCondition.UNAUTHORIZED_ACCESS),
                ServiceBusErrorSource.RECEIVE)))
                .isEqualTo(BrokerProblem.AUTH);
        assertThat(ServicebusCheck.classify(new ServiceBusException(
                amqp(AmqpErrorCondition.NOT_FOUND),
                ServiceBusErrorSource.RECEIVE)))
                .isEqualTo(BrokerProblem.NOT_FOUND);
        assertThat(ServicebusCheck.classify(new IllegalStateException(
                "Timeout on blocking read")))
                .isEqualTo(BrokerProblem.NETWORK);

        assertThat(EventhubsCheck.classifyAmqp(
                amqp(AmqpErrorCondition.UNAUTHORIZED_ACCESS)))
                .isEqualTo(BrokerProblem.AUTH);
        assertThat(EventhubsCheck.classifyAmqp(
                amqp(AmqpErrorCondition.SERVER_BUSY_ERROR))).isNull();
        assertThat(EventhubsCheck.classifyStorage(storage(403)))
                .isEqualTo(BrokerProblem.PERMISSION);
        assertThat(EventhubsCheck.classifyStorage(storage(404)))
                .isEqualTo(BrokerProblem.NOT_FOUND);
        assertThat(EventhubsCheck.classifyStorage(storage(503)))
                .isEqualTo(BrokerProblem.NETWORK);
    }

    /**
     * A blob storage error.
     *
     * @param status its HTTP status
     * @return the error
     */
    static BlobStorageException storage(int status)
    {
        HttpResponse response = mock(HttpResponse.class);
        when(response.getStatusCode()).thenReturn(status);
        return new BlobStorageException("x", response, null);
    }
}
