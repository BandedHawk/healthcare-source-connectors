/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.CommitManager;
import org.apache.camel.component.kafka.consumer.DefaultKafkaManualSyncCommit;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.component.kafka.consumer.KafkaManualCommitFactory;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link KafkaOffsets}: a handled poll commits every partition, and
 * one not handled is sought back, so no later commit passes it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class KafkaOffsetsTest
{

    /** The topic. */
    static final String TOPIC = "records";

    /** Its first partition. */
    static final TopicPartition P0 = new TopicPartition(TOPIC, 0);

    /** Its second partition. */
    static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

    /** The exchanges' context. */
    final CamelContext camel = new DefaultCamelContext();

    /** The consumer the records were polled by. */
    final MockConsumer<Object, Object> consumer =
            new MockConsumer<>("earliest");

    /** The offsets committed, in order: partition, then offset. */
    final List<String> committed = new ArrayList<>();

    /** Creates the tests; JUnit runs each test on a new instance. */
    KafkaOffsetsTest()
    {
    }

    /**
     * Closes the context.
     *
     * @throws Exception if it can't be closed
     */
    @AfterEach
    void close() throws Exception
    {
        camel.close();
    }

    /**
     * A record's exchange as Camel's batching consumer makes it: its
     * partition and offset, and its own manual commit.
     *
     * @param partition the partition
     * @param offset    its offset
     * @return the exchange
     */
    Exchange record(TopicPartition partition, long offset)
    {
        Exchange exchange = new DefaultExchange(camel);
        exchange.getMessage().setHeader(KafkaConstants.TOPIC,
                partition.topic());
        exchange.getMessage().setHeader(KafkaConstants.PARTITION,
                partition.partition());
        exchange.getMessage().setHeader(KafkaConstants.OFFSET, offset);
        KafkaManualCommit commit = new DefaultKafkaManualSyncCommit(
                new KafkaManualCommitFactory.CamelExchangePayload(exchange,
                        consumer, "test", null),
                new KafkaManualCommitFactory.KafkaRecordPayload(partition,
                        offset, 1000),
                new RecordingCommits());
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);
        return exchange;
    }

    /**
     * A poll over both partitions, interleaved.
     *
     * @return its records
     */
    List<Exchange> poll()
    {
        return List.of(record(P0, 5), record(P1, 7), record(P0, 6),
                record(P1, 8));
    }

    /**
     * Assigns partitions to the consumer, positioned past the poll.
     *
     * @param partitions the partitions
     */
    void assignedPastThePoll(TopicPartition... partitions)
    {
        consumer.assign(List.of(partitions));
        for (TopicPartition partition : partitions)
        {
            consumer.updateBeginningOffsets(Map.of(partition, 0L));
            consumer.seek(partition, 9);
        }
    }

    /** Each partition's last record is committed, not only the poll's last. */
    @Test
    void commitsEveryPartitionOfAPoll()
    {
        KafkaOffsets.commit(poll());
        assertThat(committed).containsExactly(TOPIC + "-0@6", TOPIC + "-1@8");
    }

    /** A poll not handled is sought back to its first record on each. */
    @Test
    void rewindsEachPartitionToThePollsStart()
    {
        assignedPastThePoll(P0, P1);
        assertThat(KafkaOffsets.rewind(poll())).isEqualTo(2);
        assertThat(consumer.position(P0)).isEqualTo(5);
        assertThat(consumer.position(P1)).isEqualTo(7);
        assertThat(committed).isEmpty();
    }

    /**
     * A partition revoked meanwhile is left to its new owner; the others
     * are still rewound.
     */
    @Test
    void leavesAPartitionNoLongerAssigned()
    {
        assignedPastThePoll(P1);
        assertThat(KafkaOffsets.rewind(poll())).isEqualTo(1);
        assertThat(consumer.position(P1)).isEqualTo(7);
    }

    /** Records the commits Camel's per-record commits force. */
    final class RecordingCommits implements CommitManager
    {

        /** Creates the recorder. */
        RecordingCommits()
        {
        }

        @Override
        public KafkaManualCommit getManualCommit(Exchange exchange,
                TopicPartition partition,
                ConsumerRecord<Object, Object> record)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commit()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commit(TopicPartition partition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forceCommit(TopicPartition partition, long offset)
        {
            committed.add(partition + "@" + offset);
        }

        @Override
        public void recordOffset(TopicPartition partition, long offset)
        {
            throw new UnsupportedOperationException();
        }
    }
}
