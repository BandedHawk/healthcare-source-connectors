package com.sdh.healthcare.connector.file.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

class CamelMessagePublisherTest {

    @Test
    void sendsInOrderAndReportsFailures() throws Exception {
        try (DefaultCamelContext camel = new DefaultCamelContext()) {
            camel.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from("direct:broken").throwException(new IllegalStateException("broker said no"));
                }
            });
            camel.start();
            UUID file = UUID.randomUUID();
            List<CompletableFuture<Void>> acks = new ArrayList<>();
            try (CamelMessagePublisher pub = new CamelMessagePublisher(camel, QueueType.CAMEL, "mock:out")) {
                for (int i = 0; i < 200; i++) {
                    acks.add(pub.publish(new OutboundMessage("m" + i, MessageType.SECTION_STORED, file,
                            ("body" + i).getBytes(StandardCharsets.US_ASCII))));
                }
                CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).get();
            }
            MockEndpoint mock = camel.getEndpoint("mock:out", MockEndpoint.class);
            assertThat(mock.getExchanges()).hasSize(200);
            for (int i = 0; i < 200; i++) {
                assertThat(mock.getExchanges().get(i).getIn().getBody(String.class)).isEqualTo("body" + i);
                assertThat(mock.getExchanges().get(i).getIn().getHeader("messageId")).isEqualTo("m" + i);
            }
            try (CamelMessagePublisher broken = new CamelMessagePublisher(camel, QueueType.CAMEL, "direct:broken")) {
                CompletableFuture<Void> f = broken.publish(new OutboundMessage("x", MessageType.FILE_STARTED, file,
                        new byte[0]));
                assertThatThrownBy(f::get).isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(IllegalStateException.class);
            }
        }
    }
}
