package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.ItSupport.*;

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
import com.sdh.healthcare.connector.file.publish.ClosablePublisher;
import com.sdh.healthcare.connector.file.publish.Publishers;
import com.sdh.healthcare.connector.file.publish.QueueType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.camel.component.google.pubsub.GooglePubsubComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.gcloud.PubSubEmulatorContainer;

/** Design §10.3 pubsub: orderingKey = fileId with message ordering enabled, metadata as attributes. */
class PubSubIT {
    static PubSubEmulatorContainer emulator;

    @BeforeAll
    static void start() {
        requireDocker();
        emulator = new PubSubEmulatorContainer("gcr.io/google.com/cloudsdktool/google-cloud-cli:590.0.0-emulators");
        emulator.start();
    }

    @AfterAll
    static void stop() {
        if (emulator != null) {
            emulator.stop();
        }
    }

    @Test
    void orderedDeliveryWithAttributes() throws Exception {
        String project = "sdh-it";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(emulator.getEmulatorEndpoint()).usePlaintext().build();
        var transport = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        var creds = NoCredentialsProvider.create();
        TopicName topic = TopicName.of(project, "sdh-sections");
        SubscriptionName sub = SubscriptionName.of(project, "sdh-reader");
        try (TopicAdminClient topics = TopicAdminClient.create(TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(transport).setCredentialsProvider(creds).build());
                SubscriptionAdminClient subs = SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder()
                        .setTransportChannelProvider(transport).setCredentialsProvider(creds).build())) {
            topics.createTopic(topic);
            subs.createSubscription(Subscription.newBuilder().setName(sub.toString()).setTopic(topic.toString())
                    .setEnableMessageOrdering(true).setAckDeadlineSeconds(60).build());

            List<OutboundMessage> sent = interleaved(UUID.randomUUID(), UUID.randomUUID(), 50);
            try (DefaultCamelContext camel = new DefaultCamelContext()) {
                GooglePubsubComponent component = new GooglePubsubComponent();
                component.setEndpoint(emulator.getEmulatorEndpoint());
                component.setAuthenticate(false);
                camel.addComponent("google-pubsub", component);
                camel.start();
                String uri = QueueType.PUBSUB.effectiveUri("google-pubsub:" + project + ":sdh-sections"
                        + "?pubsubEndpoint=" + emulator.getEmulatorEndpoint());
                try (ClosablePublisher pub = Publishers.create(camel, QueueType.PUBSUB, uri)) {
                    publishAll(pub, sent);
                }
            }

            List<Received> got = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 60_000;
            while (got.size() < sent.size() && System.currentTimeMillis() < deadline) {
                List<ReceivedMessage> batch = subs.pullCallable().call(PullRequest.newBuilder()
                        .setSubscription(sub.toString()).setMaxMessages(100).build()).getReceivedMessagesList();
                List<String> acks = new ArrayList<>();
                for (ReceivedMessage rm : batch) {
                    PubsubMessage m = rm.getMessage();
                    var a = m.getAttributesMap();
                    got.add(new Received(a.get("messageId"), a.get("type"), a.get("fileId"), a.get("contentType"),
                            m.getOrderingKey(), m.getData().toStringUtf8()));
                    acks.add(rm.getAckId());
                }
                if (!acks.isEmpty()) {
                    subs.acknowledgeCallable().call(AcknowledgeRequest.newBuilder().setSubscription(sub.toString())
                            .addAllAckIds(acks).build());
                }
            }
            assertDelivered(sent, got, true);
        } finally {
            channel.shutdownNow();
        }
    }
}
