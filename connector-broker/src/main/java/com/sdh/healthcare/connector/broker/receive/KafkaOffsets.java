/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.DefaultKafkaManualCommit;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits a Kafka poll's offsets once it is handled, or rewinds the
 * consumer to its start if it isn't (STAGES.md §10, "found 2026-11-07").
 * <p>
 * In batching mode Camel gives the poll's exchange the manual commit of
 * its last record only, which commits only that record's partition: the
 * poll's other partitions were left uncommitted until a later poll ended
 * on them. Each record's own exchange carries its commit, so the last
 * record of every partition is committed here.
 * <p>
 * A poll that isn't handled, its batch given up as the route stops or
 * fatal, must not be passed by: Camel only logs the failure and polls on,
 * from past it, and the next poll's commit would then move each partition
 * past records never handled. So each partition is sought back to the
 * poll's first record, and they come again, to this consumer or, after a
 * rebalance, to the next. This runs on the consumer's polling thread,
 * the only one that may use it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class KafkaOffsets
{

    /** Logs a rewind that couldn't be made, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaOffsets.class);

    /** Not instantiated. */
    private KafkaOffsets()
    {
    }

    /**
     * Commits, for each partition of a handled poll, the offset after its
     * last record.
     *
     * @param polled the poll's records, as exchanges, in the order polled
     */
    static void commit(List<Exchange> polled)
    {
        Map<TopicPartition, KafkaManualCommit> last = new LinkedHashMap<>();
        for (Exchange record : polled)
        {
            KafkaManualCommit commit = manualCommit(record);
            if (commit != null)
            {
                last.put(partition(record), commit);
            }
        }
        for (KafkaManualCommit commit : last.values())
        {
            commit.commit();
        }
    }

    /**
     * Seeks each partition of a poll that wasn't handled back to the poll's
     * first record on it, so that no later commit passes them. A partition
     * no longer assigned is left: its committed offset is before them, so
     * they come again to its new owner.
     *
     * @param polled the poll's records, as exchanges, in the order polled
     * @return how many partitions were sought back
     */
    static int rewind(List<Exchange> polled)
    {
        Map<TopicPartition, Long> first = new LinkedHashMap<>();
        Consumer<?, ?> consumer = null;
        for (Exchange record : polled)
        {
            first.putIfAbsent(partition(record), record.getMessage()
                    .getHeader(KafkaConstants.OFFSET, Long.class));
            if (consumer == null
                    && manualCommit(record) instanceof DefaultKafkaManualCommit
                            known)
            {
                consumer = known.getConsumer();
            }
        }
        if (consumer == null)
        {
            LOG.warn("broker.kafka_not_rewound partitions={} reason=no_consumer",
                    first.size());
            return 0;
        }
        int sought = 0;
        for (Map.Entry<TopicPartition, Long> start : first.entrySet())
        {
            try
            {
                consumer.seek(start.getKey(), start.getValue());
                sought++;
            }
            catch (IllegalStateException notAssigned)
            {
                // Revoked meanwhile; its new owner starts from before it.
                LOG.info("broker.kafka_not_rewound partition={}"
                        + " reason=not_assigned", start.getKey());
            }
        }
        return sought;
    }

    /**
     * A record's partition, from its headers.
     *
     * @param record the record's exchange
     * @return its topic and partition
     */
    private static TopicPartition partition(Exchange record)
    {
        org.apache.camel.Message in = record.getMessage();
        return new TopicPartition(
                in.getHeader(KafkaConstants.TOPIC, String.class),
                in.getHeader(KafkaConstants.PARTITION, Integer.class));
    }

    /**
     * A record's manual commit.
     *
     * @param record the record's exchange
     * @return its commit; null if it has none
     */
    private static KafkaManualCommit manualCommit(Exchange record)
    {
        return record.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT,
                KafkaManualCommit.class);
    }
}
