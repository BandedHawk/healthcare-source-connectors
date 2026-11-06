/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.send;

import com.azure.core.util.BinaryData;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder.ServiceBusSenderClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderAsyncClient;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.camel.CamelContext;
import org.apache.camel.component.azure.servicebus.ServiceBusConfiguration;
import org.apache.camel.component.azure.servicebus.ServiceBusEndpoint;
import org.apache.camel.component.azure.servicebus.ServiceBusType;

/**
 * A Service Bus publisher on the Azure SDK's sender (design §10.3).
 * <p>
 * Camel 4.22's Service Bus producer ignores per-message SessionId,
 * MessageId and ContentType headers: it only takes a fixed session ID from
 * the endpoint. Per-file sessions (for order) and duplicate detection by
 * MessageId therefore need the SDK directly; ServiceBusIT found this. The
 * connection settings still come from the configured Camel URI
 * ({@code connectionString}, or {@code fullyQualifiedNamespace} with a token
 * credential or the default Azure credential chain), so the configuration
 * doesn't change.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ServiceBusPublisher implements ClosablePublisher
{

    /** Sends to the queue or topic. */
    private final ServiceBusSenderAsyncClient sender;

    /**
     * Creates a publisher on a sender.
     *
     * @param sender sends to the queue or topic
     */
    ServiceBusPublisher(ServiceBusSenderAsyncClient sender)
    {
        this.sender = sender;
    }

    /**
     * Creates a publisher from a Camel Service Bus endpoint's settings.
     *
     * @param camel resolves the endpoint
     * @param uri   the Camel endpoint URI
     * @return the publisher
     */
    public static ServiceBusPublisher fromEndpoint(CamelContext camel,
            String uri)
    {
        ServiceBusConfiguration config = camel
                .getEndpoint(uri, ServiceBusEndpoint.class).getConfiguration();
        ServiceBusClientBuilder client = new ServiceBusClientBuilder();
        if (config.getConnectionString() != null)
        {
            client.connectionString(config.getConnectionString());
        }
        else
        {
            client.fullyQualifiedNamespace(config.getFullyQualifiedNamespace())
                    .credential(config.getTokenCredential() != null
                            ? config.getTokenCredential()
                            : new DefaultAzureCredentialBuilder().build());
        }
        if (config.getAmqpTransportType() != null)
        {
            client.transportType(config.getAmqpTransportType());
        }
        if (config.getProxyOptions() != null)
        {
            client.proxyOptions(config.getProxyOptions());
        }
        if (config.getAmqpRetryOptions() != null)
        {
            client.retryOptions(config.getAmqpRetryOptions());
        }
        if (config.getClientOptions() != null)
        {
            client.clientOptions(config.getClientOptions());
        }
        ServiceBusSenderClientBuilder sender = client.sender();
        if (config.getServiceBusType() == ServiceBusType.topic)
        {
            sender.topicName(config.getTopicOrQueueName());
        }
        else
        {
            sender.queueName(config.getTopicOrQueueName());
        }
        return new ServiceBusPublisher(sender.buildAsyncClient());
    }

    /**
     * Builds the Service Bus message: SessionId is the file ID, MessageId
     * the message ID, and the metadata goes in application properties.
     *
     * @param message the message
     * @return the Service Bus message
     */
    static ServiceBusMessage toMessage(OutboundMessage message)
    {
        ServiceBusMessage busMessage = new ServiceBusMessage(
                BinaryData.fromString(new String(message.body(),
                        StandardCharsets.US_ASCII)));
        busMessage.setMessageId(message.messageId());
        busMessage.setSessionId(message.itemId().toString());
        busMessage.setContentType(OutboundMessage.CONTENT_TYPE);
        QueueType.metadata(message).forEach((key, value) ->
                busMessage.getApplicationProperties().put(key, value));
        return busMessage;
    }

    /**
     * Sends a message.
     *
     * @param message the message
     * @return completes when Service Bus acknowledges it
     */
    @Override
    public CompletableFuture<Void> publish(OutboundMessage message)
    {
        try
        {
            return sender.sendMessage(toMessage(message)).toFuture();
        }
        catch (RuntimeException failure)
        {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** Closes the sender. */
    @Override
    public void close()
    {
        sender.close();
    }
}
