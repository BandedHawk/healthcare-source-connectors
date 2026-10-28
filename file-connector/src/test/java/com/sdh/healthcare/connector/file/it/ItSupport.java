package com.sdh.healthcare.connector.file.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.DockerClientFactory;

/** Shared checks for the broker and storage integration tests. */
final class ItSupport {
    private ItSupport() {}

    /** Fails (not skips) with a pointer to the runner script when no Docker API is reachable. */
    static void requireDocker() {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            Assertions.fail("No Docker API reachable. With minikube, run scripts/it-minikube.sh, or set DOCKER_HOST, "
                    + "DOCKER_TLS_VERIFY and DOCKER_CERT_PATH from `minikube -p sdh-it docker-env`.");
        }
    }

    /** Two files' messages interleaved, as two files would never be, to make per-file ordering visible. */
    static List<OutboundMessage> interleaved(UUID a, UUID b, int perFile) {
        List<OutboundMessage> out = new ArrayList<>();
        for (int i = 0; i < perFile; i++) {
            out.add(message(a, i));
            out.add(message(b, i));
        }
        return out;
    }

    static OutboundMessage message(UUID fileId, int seq) {
        String id = fileId + "-" + seq;
        return new OutboundMessage(id, MessageType.SECTION_STORED, fileId,
                ("jwe." + id).getBytes(StandardCharsets.US_ASCII));
    }

    static void publishAll(MessagePublisher pub, List<OutboundMessage> msgs) throws Exception {
        List<CompletableFuture<Void>> acks = new ArrayList<>();
        for (OutboundMessage m : msgs) {
            acks.add(pub.publish(m));
        }
        CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).get(2, TimeUnit.MINUTES);
    }

    /** One message as a consumer saw it; null where the broker doesn't carry the value. */
    record Received(String messageId, String type, String fileId, String contentType, String orderingKey,
            String body) {}

    /** Every message arrived exactly once, each file's in order, with the metadata and body intact. */
    static void assertDelivered(List<OutboundMessage> sent, List<Received> got, boolean metadata) {
        assertThat(got).extracting(Received::messageId)
                .containsExactlyInAnyOrderElementsOf(sent.stream().map(OutboundMessage::messageId).toList());
        Map<String, List<Integer>> seqByFile = new LinkedHashMap<>();
        for (Received r : got) {
            String fileId = r.messageId().substring(0, 36);
            assertThat(r.orderingKey()).as("ordering key").isEqualTo(fileId);
            assertThat(r.body()).isEqualTo("jwe." + r.messageId());
            if (metadata) {
                assertThat(r.fileId()).isEqualTo(fileId);
                assertThat(r.type()).isEqualTo("section.stored");
                assertThat(r.contentType()).isEqualTo(OutboundMessage.CONTENT_TYPE);
            }
            seqByFile.computeIfAbsent(fileId, k -> new ArrayList<>())
                    .add(Integer.parseInt(r.messageId().substring(37)));
        }
        seqByFile.values().forEach(seqs -> assertThat(seqs).as("per-file order").isSorted());
    }
}
