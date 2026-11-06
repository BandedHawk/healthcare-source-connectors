/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.send;

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

/**
 * Tests for {@link CamelMessagePublisher}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class CamelMessagePublisherTest
{

    /** How many messages are sent. */
    static final int COUNT = 200;

    /**
     * Messages arrive in the order sent, and a failed send fails its
     * future.
     *
     * @throws Exception if Camel can't start
     */
    @Test
    void sendsInOrderAndReportsFailures() throws Exception
    {
        try (DefaultCamelContext camel = new DefaultCamelContext())
        {
            camel.addRoutes(new RouteBuilder()
            {
                @Override
                public void configure()
                {
                    from("direct:broken").throwException(
                            new IllegalStateException("broker said no"));
                }
            });
            camel.start();
            UUID file = UUID.randomUUID();
            List<CompletableFuture<Void>> acks = new ArrayList<>();
            try (CamelMessagePublisher publisher = new CamelMessagePublisher(
                    camel, QueueType.CAMEL, "mock:out"))
            {
                for (int i = 0; i < COUNT; i++)
                {
                    acks.add(publisher.publish(new OutboundMessage("m" + i,
                            MessageType.RECORD_STORED, file, "m" + i,
                            ("body" + i).getBytes(StandardCharsets.US_ASCII))));
                }
                CompletableFuture.allOf(
                        acks.toArray(CompletableFuture[]::new)).get();
            }
            MockEndpoint mock = camel.getEndpoint("mock:out",
                    MockEndpoint.class);
            assertThat(mock.getExchanges()).hasSize(COUNT);
            for (int i = 0; i < COUNT; i++)
            {
                assertThat(mock.getExchanges().get(i).getIn()
                        .getBody(String.class)).isEqualTo("body" + i);
                assertThat(mock.getExchanges().get(i).getIn()
                        .getHeader("messageId")).isEqualTo("m" + i);
            }
            try (CamelMessagePublisher broken = new CamelMessagePublisher(
                    camel, QueueType.CAMEL, "direct:broken"))
            {
                CompletableFuture<Void> sent = broken.publish(
                        new OutboundMessage("x", MessageType.FILE_STARTED,
                                file, file.toString(), new byte[0]));
                assertThatThrownBy(sent::get)
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(IllegalStateException.class);
            }
        }
    }
}
