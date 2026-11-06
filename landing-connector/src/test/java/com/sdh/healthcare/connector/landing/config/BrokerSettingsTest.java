/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.broker.receive.BrokerKind;
import com.sdh.healthcare.connector.broker.receive.ReceiveProperties;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

/**
 * Tests for {@link BrokerSettings}: what the landing node adds to receiving.
 * How receiving's own settings are checked is tested in
 * {@code connector-broker}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class BrokerSettingsTest
{

    /** Small batch limits. */
    static final ReceiveProperties.Batch BATCH = new ReceiveProperties.Batch(
            50, DataSize.ofMegabytes(1), Duration.ofSeconds(2));

    /** A RabbitMQ connection. */
    static final ReceiveProperties.Rabbitmq RABBIT =
            new ReceiveProperties.Rabbitmq("rabbit:5672", "/", "u", "p",
                    false);

    /** No Artemis connection. */
    static final ReceiveProperties.Artemis NO_ARTEMIS =
            new ReceiveProperties.Artemis(null, null, null, null);

    /** No Event Hubs checkpoint store. */
    static final ReceiveProperties.Eventhubs NO_EVENTHUBS =
            new ReceiveProperties.Eventhubs(null, null, null);

    /**
     * Broker settings as configured.
     *
     * @param type        the broker
     * @param uri         the consumer URI
     * @param dedupWindow how far back a message delivered again is
     *                    recognised
     * @return the settings
     */
    static LandingProperties.Broker broker(String type, String uri,
            Duration dedupWindow)
    {
        return new LandingProperties.Broker(type, uri, BATCH, dedupWindow,
                Duration.ofSeconds(30), RABBIT, NO_ARTEMIS, NO_EVENTHUBS);
    }

    /**
     * The receiving settings are checked, the landing node's prefix names
     * them, and the attribute naming a replaced message is read.
     */
    @Test
    void receivesWithTheReplacesAttribute()
    {
        BrokerSettings settings = BrokerSettings.of(broker("sqs",
                "aws2-sqs:q?region=eu-west-1", Duration.ofHours(1)));
        assertThat(settings.receive().kind()).isEqualTo(BrokerKind.SQS);
        assertThat(settings.receive().attributes())
                .containsExactly(BrokerSettings.REPLACES_ATTRIBUTE);
        assertThat(settings.receive().uri())
                .contains("messageAttributeNames=sdh_replaces");
        assertThat(settings.dedupWindow()).isEqualTo(Duration.ofHours(1));
        assertThatThrownBy(() -> BrokerSettings.of(broker("kafka",
                "kafka:hl7?brokers=k:9092", Duration.ofHours(1))))
                .hasMessageContaining(BrokerSettings.PREFIX)
                .hasMessageContaining("groupId");
    }

    /** A negative dedup window is refused; none at all is allowed. */
    @Test
    void refusesANegativeDedupWindow()
    {
        assertThatThrownBy(() -> BrokerSettings.of(broker("sqs", "aws2-sqs:q",
                Duration.ofSeconds(-1))))
                .hasMessageContaining(BrokerSettings.PREFIX + ".dedup-window");
        assertThat(BrokerSettings.of(broker("sqs", "aws2-sqs:q",
                Duration.ZERO)).dedupWindow()).isZero();
    }
}
