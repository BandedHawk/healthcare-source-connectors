/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.send;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.pipeline.Outage;
import com.sdh.healthcare.connector.core.pipeline.PublishFailedException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * Kafka's errors as the split stage sees them when publishing fails:
 * the brokers gone or not enough of them is an outage, so the attempt
 * isn't held against the item; a message too large is not.
 * {@link Outage} knows Kafka's errors by name, so this checks it against
 * the real classes.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class KafkaOutageTest
{

    /** Creates the tests; JUnit runs each on a new instance. */
    KafkaOutageTest()
    {
    }

    /** Brokers gone, or too few, wrapped as publishing wraps them. */
    @Test
    void brokersGoneIsAnOutage()
    {
        assertThat(Outage.caused(new PublishFailedException(
                new ExecutionException(new TimeoutException(
                        "Expiring 1 record(s)"))))).isTrue();
        assertThat(Outage.caused(new PublishFailedException(
                new NotEnoughReplicasException("2 of 3")))).isTrue();
    }

    /** A message too large is the message's own problem. */
    @Test
    void aMessageTooLargeIsNot()
    {
        assertThat(Outage.caused(new PublishFailedException(
                new RecordTooLargeException("too large")))).isFalse();
    }
}
