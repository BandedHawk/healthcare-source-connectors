/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.broker.receive.ReceiveProperties;
import com.sdh.healthcare.connector.broker.receive.ReceiveSettings;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

/**
 * Tests for {@link ConnectorRuntime#consumers}: the split stage's Kafka
 * consumers (docs/PERFORMANCE.md §3.4).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SplitConsumersTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    SplitConsumersTest()
    {
    }

    /**
     * An input's settings.
     *
     * @param type the broker
     * @param uri  its URI
     * @return the settings
     */
    static ReceiveSettings input(String type, String uri)
    {
        return ReceiveSettings.of(
                new ReceiveProperties(type, uri,
                        new ReceiveProperties.Batch(4,
                                DataSize.ofMegabytes(1),
                                Duration.ofMillis(500)),
                        Duration.ofSeconds(30),
                        new ReceiveProperties.Rabbitmq(null, "/", null, null,
                                false),
                        new ReceiveProperties.Artemis(null, null, null, null),
                        new ReceiveProperties.Eventhubs(null, null, null)),
                "sdh.stage.input", Set.of());
    }

    /** One consumer leaves the input as it is. */
    @Test
    void oneConsumerChangesNothing()
    {
        ReceiveSettings kafka = input("kafka",
                "kafka:landed?brokers=k:9092&groupId=split");
        assertThat(ConnectorRuntime.consumers(kafka, 1)).isSameAs(kafka);
        ReceiveSettings sqs = input("sqs", "aws2-sqs:landed");
        assertThat(ConnectorRuntime.consumers(sqs, 1)).isSameAs(sqs);
    }

    /** More than one is a Kafka consumer count. */
    @Test
    void moreAreKafkaConsumers()
    {
        assertThat(ConnectorRuntime.consumers(input("kafka",
                "kafka:landed?brokers=k:9092&groupId=split"), 8).uri())
                .contains("&consumersCount=8");
    }

    /**
     * More than one on another broker is refused, as is a Kafka URI that
     * sets the count itself.
     */
    @Test
    void refusesOtherBrokersAndACountInTheUri()
    {
        assertThatThrownBy(() -> ConnectorRuntime.consumers(
                input("sqs", "aws2-sqs:landed"), 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sdh.split.consumers");
        assertThatThrownBy(() -> ConnectorRuntime.consumers(input("kafka",
                "kafka:landed?brokers=k:9092&groupId=split"
                        + "&consumersCount=3"), 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("consumersCount");
    }
}
