/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.assertDelivered;
import static com.sdh.healthcare.connector.file.it.ItSupport.interleaved;
import static com.sdh.healthcare.connector.file.it.ItSupport.publishAll;
import static com.sdh.healthcare.connector.file.it.ItSupport.requireDocker;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.file.it.ItSupport.Received;
import com.sdh.healthcare.connector.broker.send.ClosablePublisher;
import com.sdh.healthcare.connector.broker.send.Publishers;
import com.sdh.healthcare.connector.broker.send.QueueType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.component.google.pubsub.GooglePubsubComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.gcloud.PubSubEmulatorContainer;

/**
 * Design §10.3 pubsub: orderingKey = itemId with message ordering enabled,
 * metadata as attributes.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class PubSubIT
{

    /** The Pub/Sub emulator. */
    static PubSubEmulatorContainer emulator;

    /** Starts the emulator. */
    @BeforeAll
    static void start()
    {
        requireDocker();
        emulator = new PubSubEmulatorContainer("gcr.io/google.com/"
                + "cloudsdktool/google-cloud-cli:590.0.0-emulators");
        emulator.start();
    }

    /** Stops the emulator. */
    @AfterAll
    static void stop()
    {
        if (emulator != null)
        {
            emulator.stop();
        }
    }

    /**
     * Messages arrive in order per file, with their attributes.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void orderedDeliveryWithAttributes() throws Exception
    {
        String project = "sdh-it";
        ManagedChannel channel = ManagedChannelBuilder
                .forTarget(emulator.getEmulatorEndpoint()).usePlaintext()
                .build();
        FixedTransportChannelProvider transport =
                FixedTransportChannelProvider.create(
                        GrpcTransportChannel.create(channel));
        CredentialsProvider credentials = NoCredentialsProvider.create();
        TopicName topic = TopicName.of(project, "sdh-records");
        SubscriptionName subscription =
                SubscriptionName.of(project, "sdh-reader");
        try (TopicAdminClient topics = TopicAdminClient.create(
                        TopicAdminSettings.newBuilder()
                                .setTransportChannelProvider(transport)
                                .setCredentialsProvider(credentials).build());
                SubscriptionAdminClient subscriptions =
                        SubscriptionAdminClient.create(
                                SubscriptionAdminSettings.newBuilder()
                                        .setTransportChannelProvider(transport)
                                        .setCredentialsProvider(credentials)
                                        .build()))
        {
            topics.createTopic(topic);
            subscriptions.createSubscription(Subscription.newBuilder()
                    .setName(subscription.toString())
                    .setTopic(topic.toString())
                    .setEnableMessageOrdering(true)
                    .setAckDeadlineSeconds(60).build());

            List<OutboundMessage> sent =
                    interleaved(UUID.randomUUID(), UUID.randomUUID(), 50);
            try (DefaultCamelContext camel = new DefaultCamelContext())
            {
                GooglePubsubComponent component = new GooglePubsubComponent();
                component.setEndpoint(emulator.getEmulatorEndpoint());
                component.setAuthenticate(false);
                camel.addComponent("google-pubsub", component);
                camel.start();
                String uri = QueueType.PUBSUB.effectiveUri("google-pubsub:"
                        + project + ":sdh-records" + "?pubsubEndpoint="
                        + emulator.getEmulatorEndpoint());
                try (ClosablePublisher publisher =
                        Publishers.create(camel, QueueType.PUBSUB, uri))
                {
                    publishAll(publisher, sent);
                }
            }

            List<Received> got = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 60_000;
            while (got.size() < sent.size()
                    && System.currentTimeMillis() < deadline)
            {
                List<ReceivedMessage> batch = subscriptions.pullCallable()
                        .call(PullRequest.newBuilder()
                                .setSubscription(subscription.toString())
                                .setMaxMessages(100).build())
                        .getReceivedMessagesList();
                List<String> acks = new ArrayList<>();
                for (ReceivedMessage received : batch)
                {
                    PubsubMessage message = received.getMessage();
                    Map<String, String> attributes =
                            message.getAttributesMap();
                    got.add(new Received(attributes.get("messageId"),
                            attributes.get("type"), attributes.get("itemId"),
                            attributes.get("contentType"),
                            message.getOrderingKey(),
                            message.getData().toStringUtf8()));
                    acks.add(received.getAckId());
                }
                if (!acks.isEmpty())
                {
                    subscriptions.acknowledgeCallable().call(
                            AcknowledgeRequest.newBuilder()
                                    .setSubscription(subscription.toString())
                                    .addAllAckIds(acks).build());
                }
            }
            assertDelivered(sent, got, true);
        }
        finally
        {
            channel.shutdownNow();
        }
    }
}
