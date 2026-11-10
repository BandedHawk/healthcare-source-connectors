/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.broker.receive.BrokerKind;
import com.sdh.healthcare.connector.broker.receive.ReceiveProperties;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

/**
 * Tests for {@link StageProperties}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StagePropertiesTest
{

    /** A Kafka input. */
    static final ReceiveProperties INPUT = new ReceiveProperties("kafka",
            "kafka:records?brokers=k:9092&groupId=convert",
            new ReceiveProperties.Batch(100, DataSize.ofMegabytes(8),
                    Duration.ofSeconds(1)),
            Duration.ofSeconds(30),
            new ReceiveProperties.Rabbitmq(null, "/", null, null, false),
            new ReceiveProperties.Artemis(null, null, null, null),
            new ReceiveProperties.Eventhubs(null, null, null));

    /** A queue. */
    static final CommonSettings.Queue QUEUE = new CommonSettings.Queue(
            "kafka", "kafka:out?brokers=k:9092",
            new CommonSettings.Artemis(null, null, null));

    /** The default claims. */
    static final StageProperties.Claim CLAIM = new StageProperties.Claim(
            Duration.ofSeconds(60), Duration.ofSeconds(15),
            Duration.ofSeconds(1));

    /** Creates the tests; JUnit runs each test on a new instance. */
    StagePropertiesTest()
    {
    }

    /**
     * Settings.
     *
     * @param input  the input
     * @param output the output
     * @param dlq    the dead-letter queue
     * @param claim  the claims
     * @return the settings
     */
    static StageProperties properties(ReceiveProperties input,
            CommonSettings.Queue output, CommonSettings.Queue dlq,
            StageProperties.Claim claim)
    {
        return new StageProperties(input, output, dlq, 4, claim, "sdh_stage",
                Duration.ofSeconds(30), null);
    }

    /** The input is checked as any receiver's, named by the stage. */
    @Test
    void checksTheInputUnderItsOwnName()
    {
        assertThat(properties(INPUT, QUEUE, QUEUE, CLAIM).receive(Set.of())
                .kind()).isEqualTo(BrokerKind.KAFKA);
        ReceiveProperties noGroup = new ReceiveProperties("kafka",
                "kafka:records?brokers=k:9092", INPUT.batch(),
                INPUT.checkInterval(), INPUT.rabbitmq(), INPUT.artemis(),
                INPUT.eventhubs());
        assertThatThrownBy(() -> properties(noGroup, QUEUE, QUEUE, CLAIM)
                .receive(Set.of())).hasMessageContaining("sdh.stage.input");
        assertThatThrownBy(() -> properties(null, QUEUE, QUEUE, CLAIM)
                .receive(Set.of()))
                .hasMessageContaining("sdh.stage.input is required");
    }

    /**
     * The output and dead-letter queues are required, and claims must be
     * renewed well within their life.
     */
    @Test
    void requiresBothQueuesAndATimelyRenewal()
    {
        assertThat(properties(INPUT, QUEUE, QUEUE, CLAIM).worker()
                .concurrency()).isEqualTo(4);
        assertThatThrownBy(() -> properties(INPUT, QUEUE, null, CLAIM)
                .worker()).hasMessageContaining("sdh.stage.dlq.type");
        assertThatThrownBy(() -> properties(INPUT,
                new CommonSettings.Queue("kafka", " ", null), QUEUE, CLAIM)
                .worker()).hasMessageContaining("sdh.stage.output.uri");
        assertThatThrownBy(() -> properties(INPUT, QUEUE, QUEUE,
                new StageProperties.Claim(Duration.ofSeconds(60),
                        Duration.ofSeconds(40), Duration.ofSeconds(1)))
                .worker()).hasMessageContaining("sdh.stage")
                .hasMessageContaining("half the claim's life");
    }
}
