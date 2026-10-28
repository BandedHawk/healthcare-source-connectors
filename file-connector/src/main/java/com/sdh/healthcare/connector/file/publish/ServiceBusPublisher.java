package com.sdh.healthcare.connector.file.publish;

import com.azure.core.util.BinaryData;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
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
 * Service Bus publisher on the Azure SDK sender (design §10.3).
 *
 * <p>Camel 4.22's Service Bus producer ignores per-message SessionId, MessageId and ContentType headers: it only takes
 * a fixed session ID from the endpoint. Per-file sessions (ordering) and MessageId duplicate detection therefore need
 * the SDK directly. ServiceBusIT found this. Connection settings still come from the configured Camel URI
 * ({@code connectionString}, or {@code fullyQualifiedNamespace} with a token credential or the default Azure
 * credential chain), so configuration doesn't change.
 */
public final class ServiceBusPublisher implements ClosablePublisher {
    private final ServiceBusSenderAsyncClient sender;

    ServiceBusPublisher(ServiceBusSenderAsyncClient sender) {
        this.sender = sender;
    }

    public static ServiceBusPublisher fromEndpoint(CamelContext camel, String uri) {
        ServiceBusConfiguration c = camel.getEndpoint(uri, ServiceBusEndpoint.class).getConfiguration();
        ServiceBusClientBuilder b = new ServiceBusClientBuilder();
        if (c.getConnectionString() != null) {
            b.connectionString(c.getConnectionString());
        } else {
            b.fullyQualifiedNamespace(c.getFullyQualifiedNamespace())
                    .credential(c.getTokenCredential() != null ? c.getTokenCredential()
                            : new DefaultAzureCredentialBuilder().build());
        }
        if (c.getAmqpTransportType() != null) {
            b.transportType(c.getAmqpTransportType());
        }
        if (c.getProxyOptions() != null) {
            b.proxyOptions(c.getProxyOptions());
        }
        if (c.getAmqpRetryOptions() != null) {
            b.retryOptions(c.getAmqpRetryOptions());
        }
        if (c.getClientOptions() != null) {
            b.clientOptions(c.getClientOptions());
        }
        var s = b.sender();
        if (c.getServiceBusType() == ServiceBusType.topic) {
            s.topicName(c.getTopicOrQueueName());
        } else {
            s.queueName(c.getTopicOrQueueName());
        }
        return new ServiceBusPublisher(s.buildAsyncClient());
    }

    /** SessionId = fileId, MessageId = messageId, metadata as application properties. */
    static ServiceBusMessage toMessage(OutboundMessage m) {
        ServiceBusMessage msg = new ServiceBusMessage(
                BinaryData.fromString(new String(m.body(), StandardCharsets.US_ASCII)));
        msg.setMessageId(m.messageId());
        msg.setSessionId(m.fileId().toString());
        msg.setContentType(OutboundMessage.CONTENT_TYPE);
        QueueType.metadata(m).forEach((k, v) -> msg.getApplicationProperties().put(k, v));
        return msg;
    }

    @Override
    public CompletableFuture<Void> publish(OutboundMessage m) {
        try {
            return sender.sendMessage(toMessage(m)).toFuture();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void close() {
        sender.close();
    }
}
