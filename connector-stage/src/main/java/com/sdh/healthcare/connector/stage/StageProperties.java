/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.broker.receive.ReceiveProperties;
import com.sdh.healthcare.connector.broker.receive.ReceiveSettings;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * A stage's settings, {@code sdh.stage} (STAGES.md §7). The input's batch
 * is also the commit batch: each batch received is committed in one
 * append. Secrets come from the environment, never the file.
 *
 * @param input          the broker the stage receives from
 * @param output         where its messages go
 * @param dlq            where conflicts go: the stage's dead-letter queue
 * @param concurrency    how many records a pod processes at once
 * @param consumers      for a record stage, how many input batches a pod
 *                       handles at once (Kafka only), their records sharing
 *                       the {@code concurrency} threads: one batch's
 *                       records are processed while another commits
 *                       (docs/PERFORMANCE.md §3.4); the split stage takes
 *                       {@code sdh.split.consumers} instead
 * @param claim          how records are claimed
 * @param registrySchema the Postgres schema of the stage's registry, in
 *                       the lease database
 * @param nextRegistrySchema the schema of the next stage's registry, in the
 *                       same database, where its reconciler marks items
 *                       whose end from this stage never came, for this
 *                       stage to send it again (STAGES.md §13.2); unset
 *                       for the last stage, or to leave them to an
 *                       operator
 * @param sendTimeout    the longest to wait for the broker to take a
 *                       message
 * @param reprocess      the stage's own input, to send reprocess messages
 *                       to (STAGES.md §15.8.4); unset if the stage doesn't
 *                       reprocess
 * @param readyPort      the plain TCP port opened once the pod's release
 *                       is let in, for its {@code readinessProbe}
 *                       (STAGES.md §14.4a); below 0 for none
 * @param versionCheckInterval how often a pod held back by the drain check
 *                       looks again
 * @param source         for a record stage, the table it reads records
 *                       from (STAGES.md §17.7); unset for its default
 * @param sink           for a record stage, the table it stores what it
 *                       makes in; unset for its default
 * @param autostart      false to leave the input and duties stopped, for
 *                       tests that hand messages to the worker themselves
 * @param commit         for a record stage, how its pods commit to the
 *                       sink
 * @param duties         which of the stage's pods do its duties (STAGES.md
 *                       §8): a pod for the duties alone, the others taking
 *                       them up only if it is gone
 * @param retention      for a record stage, how long its registry keeps
 *                       the rows of finished items (docs/DEPLOYMENT.md
 *                       "Registry retention"); by default for ever
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record StageProperties(ReceiveProperties input,
        CommonSettings.Queue output, CommonSettings.Queue dlq,
        @DefaultValue("4") int concurrency,
        @DefaultValue("1") int consumers, @DefaultValue Claim claim,
        @DefaultValue("sdh_stage") String registrySchema,
        String nextRegistrySchema,
        @DefaultValue("30s") Duration sendTimeout,
        CommonSettings.Queue reprocess,
        @DefaultValue("8081") int readyPort,
        @DefaultValue("1m") Duration versionCheckInterval,
        @DefaultValue TableSettings source, @DefaultValue TableSettings sink,
        @DefaultValue("true") boolean autostart,
        @DefaultValue Commit commit, @DefaultValue Duties duties,
        @DefaultValue Retention retention)
{

    /**
     * No commit settings is the default's: each pod commits its own; no
     * duties settings, any pod does the duties; no retention, the registry
     * is kept for ever.
     */
    public StageProperties
    {
        commit = commit == null ? new Commit(false, Duration.ZERO) : commit;
        duties = duties == null ? Duties.ANY : duties;
        retention = retention == null ? Retention.FOREVER : retention;
    }

    /**
     * How long a record stage's registry keeps the rows of its finished
     * items (docs/DEPLOYMENT.md "Registry retention"): the per-record rows
     * ({@code stage_record}) and the item's own ({@code stage_item}) of an
     * item complete for longer than {@code after}, with none of its
     * failures open. Failures, their events, and everything stored in the
     * tables are kept. Unset, the registry is kept for ever.
     * <p>
     * A record's row is what stops a message delivered again from being
     * processed again, so {@code after} must be longer than the queue
     * keeps a message ({@code queueRetention}, which the stage can't read
     * from every broker, so the deployment states it), and longer than the
     * reconciler looks back, each by a day at least.
     *
     * @param after          how long after an item is complete its rows
     *                       are deleted; null to keep them for ever
     * @param queueRetention the longest the stage's input queue keeps a
     *                       message, as the broker is configured
     * @param interval       how often the duties pod prunes
     * @param batch          the most items deleted in one transaction
     */
    public record Retention(Duration after,
            @DefaultValue("7d") Duration queueRetention,
            @DefaultValue("1h") Duration interval,
            @DefaultValue("100") int batch)
    {

        /** The least margin over the queue's retention and the lookback. */
        public static final Duration MARGIN = Duration.ofDays(1);

        /** Kept for ever: the default. */
        public static final Retention FOREVER = new Retention(null,
                Duration.ofDays(7), Duration.ofHours(1), 100);

        /**
         * Checks the settings.
         *
         * @param after          how long finished items' rows are kept
         * @param queueRetention how long the queue keeps a message
         * @param interval       how often the duties pod prunes
         * @param batch          the most items a transaction deletes
         * @throws IllegalArgumentException if {@code after} is shorter
         *                                  than the queue's retention and
         *                                  the margin, or a setting is out
         *                                  of range
         */
        public Retention
        {
            queueRetention = queueRetention == null ? Duration.ofDays(7)
                    : queueRetention;
            interval = interval == null ? Duration.ofHours(1) : interval;
            if (queueRetention.isNegative() || queueRetention.isZero())
            {
                throw new IllegalArgumentException(PREFIX
                        + ".retention.queue-retention must be positive");
            }
            if (interval.compareTo(Duration.ofMinutes(1)) < 0)
            {
                throw new IllegalArgumentException(PREFIX
                        + ".retention.interval must be at least a minute");
            }
            if (batch < 1 || batch > 10_000)
            {
                throw new IllegalArgumentException(PREFIX
                        + ".retention.batch must be 1 to 10000");
            }
            if (after != null
                    && after.compareTo(queueRetention.plus(MARGIN)) < 0)
            {
                throw new IllegalArgumentException(PREFIX
                        + ".retention.after must be at least the queue's"
                        + " retention (" + PREFIX
                        + ".retention.queue-retention) and a day: a"
                        + " message delivered again after its record's"
                        + " row is gone would be processed again");
            }
        }

        /**
         * Whether the registry is kept for ever.
         *
         * @return true if nothing is pruned
         */
        public boolean forever()
        {
            return after == null;
        }

        /**
         * Checks that rows outlive the reconciler's look back.
         *
         * @param lookback how far back the reconciler looks
         * @throws IllegalArgumentException if {@code after} is shorter
         *                                  than the look back and the
         *                                  margin
         */
        public void checkLookback(Duration lookback)
        {
            if (after != null && lookback != null
                    && after.compareTo(lookback.plus(MARGIN)) < 0)
            {
                throw new IllegalArgumentException(PREFIX
                        + ".retention.after must be at least the"
                        + " reconciler's lookback and a day: an item whose"
                        + " rows were gone would look unfinished to it");
            }
        }
    }

    /** Where the settings are configured, to name them in errors. */
    public static final String PREFIX = "sdh.stage";

    /**
     * How records are claimed (STAGES.md §6).
     *
     * @param ttl           how long a claim lasts unless renewed
     * @param renewInterval how often a pod renews its claims
     * @param busyPoll      how long to wait before looking again at a
     *                      record another pod holds
     */
    public record Claim(@DefaultValue("60s") Duration ttl,
            @DefaultValue("15s") Duration renewInterval,
            @DefaultValue("1s") Duration busyPoll)
    {
    }

    /**
     * How a record stage's pods commit their batches to the sink
     * (docs/PERFORMANCE.md §3.7).
     *
     * @param shared   whether every pod's batches are committed together,
     *                 a pod at a time taking all that wait, through the
     *                 stage database; if not, each pod commits its own,
     *                 and the pods' commits conflict
     * @param interval the least time between two of the sink's commits,
     *                 so that fewer snapshots are kept; zero for none
     */
    public record Commit(@DefaultValue("false") boolean shared,
            @DefaultValue("0s") Duration interval)
    {

        /**
         * Checks the interval.
         *
         * @param shared   whether the pods commit together
         * @param interval the least time between commits
         * @throws IllegalArgumentException if the interval is negative
         */
        public Commit
        {
            interval = interval == null ? Duration.ZERO : interval;
            if (interval.isNegative())
            {
                throw new IllegalArgumentException(
                        PREFIX + ".commit.interval can't be negative");
            }
        }
    }

    /**
     * Which pods do the stage's duties (STAGES.md §8): its reconciler, its
     * claim sweep and its table maintenance, one pod for all under the
     * duties lease. By default any pod takes the lease when it is free. A
     * deployment can give the duties a pod of their own, on a node that
     * isn't reclaimed as a Spot node is ({@code only}), and have the
     * stage's other pods take them up only as a fallback, once that pod
     * has been gone for a while ({@code fallbackAfter}).
     *
     * @param only          whether this pod does the duties alone: it takes
     *                      no input, so the work it does is what the
     *                      duties hand in again
     * @param fallbackAfter how long the lease must have been free before
     *                      this pod takes it; zero to take it at once.
     *                      Longer than the duties pod takes to start again,
     *                      so a restart doesn't move the duties
     * @param handBackAfter for a fallback holder, how long it keeps the
     *                      lease before it gives it up, for the duties pod
     *                      to take back if it has come back; the fallback
     *                      takes it again after {@code fallbackAfter} if
     *                      it hasn't
     */
    public record Duties(@DefaultValue("false") boolean only,
            @DefaultValue("0s") Duration fallbackAfter,
            @DefaultValue("30m") Duration handBackAfter)
    {

        /** Any pod does the duties, taking the lease at once. */
        public static final Duties ANY = new Duties(false, Duration.ZERO,
                Duration.ofMinutes(30));

        /**
         * Checks the timings.
         *
         * @param only          whether the pod does the duties alone
         * @param fallbackAfter how long the lease must be free first
         * @param handBackAfter how long a fallback holder keeps it
         * @throws IllegalArgumentException if a timing is negative, a
         *                                  fallback hands back at once, or
         *                                  the duties pod is a fallback
         */
        public Duties
        {
            fallbackAfter = fallbackAfter == null ? Duration.ZERO
                    : fallbackAfter;
            handBackAfter = handBackAfter == null ? Duration.ofMinutes(30)
                    : handBackAfter;
            if (fallbackAfter.isNegative() || !handBackAfter.isPositive())
            {
                throw new IllegalArgumentException(PREFIX + ".duties:"
                        + " fallback-after can't be negative, and"
                        + " hand-back-after must be more than zero");
            }
            if (only && fallbackAfter.isPositive())
            {
                throw new IllegalArgumentException(PREFIX + ".duties: the"
                        + " duties pod (only) can't be a fallback");
            }
        }

        /**
         * Whether this pod takes the duties only as a fallback.
         *
         * @return true if it waits before taking a free lease
         */
        public boolean fallback()
        {
            return fallbackAfter.isPositive();
        }
    }

    /**
     * A table of records a record stage reads from or stores in (STAGES.md
     * §17.7), with the columns of {@code RecordsTable.SCHEMA}. A setting
     * left unset takes the stage's default.
     *
     * @param catalog     Iceberg catalog properties, passed through as they
     *                    are; for the sink, unset for the source's
     * @param catalogName the catalog's name, also put in messages
     * @param namespace   the table's namespace
     * @param table       the table's name
     * @param kek         for the sink, the KEK its data keys are wrapped
     *                    with; for the source, unused: each row names its
     *                    own
     */
    public record TableSettings(Map<String, String> catalog,
            String catalogName, String namespace, String table, String kek)
    {

        /**
         * No catalog properties is none, not null.
         */
        public TableSettings
        {
            catalog = catalog == null ? Map.of() : Map.copyOf(catalog);
        }
    }

    /**
     * The checked input settings.
     *
     * @param attributes the message attributes or headers to read
     * @return the settings
     * @throws IllegalArgumentException if a setting is missing or would let
     *                                  a message be acknowledged before it
     *                                  is stored
     */
    public ReceiveSettings receive(Set<String> attributes)
    {
        if (input == null)
        {
            throw new IllegalArgumentException(PREFIX + ".input is required");
        }
        return ReceiveSettings.of(input, PREFIX + ".input", attributes);
    }

    /**
     * The checked input settings of a record stage, with its
     * {@link #consumers} ({@link ReceiveSettings#withConsumers}).
     *
     * @param attributes the message attributes or headers to read
     * @return the settings
     * @throws IllegalArgumentException as {@link #receive}, or if the
     *                                  consumers can't be had
     */
    public ReceiveSettings receiveRecords(Set<String> attributes)
    {
        return receive(attributes).withConsumers(consumers,
                PREFIX + ".consumers");
    }

    /**
     * The checked worker settings.
     *
     * @return the settings
     * @throws IllegalArgumentException if the output or dead-letter queue
     *                                  is missing, or a value is out of
     *                                  range
     */
    public StageWorker.Settings worker()
    {
        requireQueue(output, "output");
        requireQueue(dlq, "dlq");
        try
        {
            return new StageWorker.Settings(concurrency, claim.ttl(),
                    claim.renewInterval(), claim.busyPoll(), sendTimeout);
        }
        catch (IllegalArgumentException invalid)
        {
            throw new IllegalArgumentException(PREFIX + ": "
                    + invalid.getMessage(), invalid);
        }
    }

    /**
     * The checked reprocess queue.
     *
     * @return the queue; null if none is configured
     * @throws IllegalArgumentException if it is configured in part
     */
    public CommonSettings.Queue reprocessQueue()
    {
        if (reprocess == null || (blank(reprocess.type())
                && blank(reprocess.uri())))
        {
            return null;
        }
        requireQueue(reprocess, "reprocess");
        return reprocess;
    }

    /**
     * Whether a setting is unset.
     *
     * @param value the setting
     * @return true if it is null or blank
     */
    private static boolean blank(String value)
    {
        return value == null || value.isBlank();
    }

    /**
     * Checks that a queue is configured.
     *
     * @param queue the queue
     * @param name  its setting's name
     * @throws IllegalArgumentException if its type or URI is missing
     */
    private static void requireQueue(CommonSettings.Queue queue, String name)
    {
        if (queue == null || queue.type() == null || queue.type().isBlank()
                || queue.uri() == null || queue.uri().isBlank())
        {
            throw new IllegalArgumentException(PREFIX + "." + name
                    + ".type and " + PREFIX + "." + name + ".uri are required");
        }
    }
}
