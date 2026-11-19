/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import java.util.ArrayList;
import java.util.HashMap;
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
 * fatal, or left to be tried again at the next poll, must not be passed
 * by: Camel only logs the failure and polls on,
 * from past it, and the next poll's commit would then move each partition
 * past records never handled. So each partition is sought back to the
 * poll's first record, and they come again, to this consumer or, after a
 * rebalance, to the next. This runs on the consumer's polling thread,
 * the only one that may use it.
 * <p>
 * Camel's batching consumer fills its batches across polls, so when a
 * batch fails part way through a poll, the rest of that poll is already in
 * Camel's next batch: records after those sought back. Handled and
 * committed, they would move the partition's commit past the records still
 * to come again, and a rebalance before those came would lose them: their
 * new owner starts from the commit (found 2026-11-19, after run 49). So a
 * partition sought back is noted ({@link Rewound}), and until the record
 * it was sought back to comes again, the later records still in Camel's
 * batches are left, neither handled nor committed ({@link #current}): the
 * consumer, sought back, fetches them again after it.
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
     * One polling thread's partitions sought back, each with the offset it
     * was sought back to, until that record comes again; and the group
     * generation they were noted in. Used only on its polling thread.
     */
    static final class Rewound
    {

        /** Each partition sought back, and its offset. */
        private final Map<TopicPartition, Long> from = new HashMap<>();

        /** The consumer group's generation when last looked at. */
        private int generation = Integer.MIN_VALUE;

        /** Creates an empty record. */
        Rewound()
        {
        }

        /**
         * The partitions sought back and not yet come again.
         *
         * @return a copy of them, with their offsets
         */
        Map<TopicPartition, Long> pending()
        {
            return Map.copyOf(from);
        }
    }

    /**
     * A batch's records, less those Camel took in before their partition
     * was sought back: records after the one it was sought back to, which
     * the consumer will fetch again. A record at or before that offset is
     * kept, and the one at it ends the wait. After a rebalance, a partition
     * no longer assigned, or whose position is past the offset (another
     * consumer committed past it), is no longer waited on.
     *
     * @param polled  the batch's records, as exchanges, in the order polled
     * @param rewound the polling thread's partitions sought back
     * @return the records to handle, in the same order
     */
    static List<Exchange> current(List<Exchange> polled, Rewound rewound)
    {
        if (rewound.from.isEmpty())
        {
            return polled;
        }
        Consumer<?, ?> consumer = consumer(polled);
        if (consumer != null)
        {
            int generation = generation(consumer);
            if (generation != rewound.generation)
            {
                rewound.generation = generation;
                rewound.from.entrySet().removeIf(sought ->
                        !consumer.assignment().contains(sought.getKey())
                                || consumer.position(sought.getKey())
                                        > sought.getValue());
            }
        }
        List<Exchange> kept = new ArrayList<>(polled.size());
        int left = 0;
        for (Exchange record : polled)
        {
            TopicPartition partition = partition(record);
            Long from = rewound.from.get(partition);
            long offset = record.getMessage().getHeader(KafkaConstants.OFFSET,
                    Long.class);
            if (from == null || offset < from)
            {
                kept.add(record);
            }
            else if (offset == from)
            {
                kept.add(record);
                rewound.from.remove(partition);
            }
            else
            {
                left++;
            }
        }
        if (left > 0)
        {
            LOG.info("broker.kafka_stale_left records={} reason=sought_back",
                    left);
        }
        return kept;
    }

    /**
     * The consumer group's generation.
     *
     * @param consumer the consumer
     * @return its generation; {@link Integer#MIN_VALUE} if it can't be
     *         read
     */
    private static int generation(Consumer<?, ?> consumer)
    {
        try
        {
            return consumer.groupMetadata().generationId();
        }
        catch (RuntimeException unknown)
        {
            return Integer.MIN_VALUE;
        }
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
     * first record on it, so that no later commit passes them, and notes
     * it, so that the later records Camel has taken in already are left
     * until that record comes again. A partition no longer assigned is
     * left: its committed offset is before them, so they come again to its
     * new owner.
     *
     * @param polled  the poll's records, as exchanges, in the order polled
     * @param rewound the polling thread's partitions sought back
     * @return how many partitions were sought back
     */
    static int rewind(List<Exchange> polled, Rewound rewound)
    {
        Map<TopicPartition, Long> first = new LinkedHashMap<>();
        for (Exchange record : polled)
        {
            first.putIfAbsent(partition(record), record.getMessage()
                    .getHeader(KafkaConstants.OFFSET, Long.class));
        }
        Consumer<?, ?> consumer = consumer(polled);
        if (consumer == null)
        {
            LOG.warn("broker.kafka_not_rewound partitions={} reason=no_consumer",
                    first.size());
            return 0;
        }
        if (rewound.from.isEmpty())
        {
            rewound.generation = generation(consumer);
        }
        int sought = 0;
        for (Map.Entry<TopicPartition, Long> start : first.entrySet())
        {
            try
            {
                consumer.seek(start.getKey(), start.getValue());
                rewound.from.merge(start.getKey(), start.getValue(),
                        Math::min);
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
     * The consumer a poll came from, which may seek on its polling thread.
     *
     * @param polled the poll's records, as exchanges
     * @return the consumer; null if no record's commit names it
     */
    static Consumer<?, ?> consumer(List<Exchange> polled)
    {
        for (Exchange record : polled)
        {
            if (manualCommit(record) instanceof DefaultKafkaManualCommit known
                    && known.getConsumer() != null)
            {
                return known.getConsumer();
            }
        }
        return null;
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
