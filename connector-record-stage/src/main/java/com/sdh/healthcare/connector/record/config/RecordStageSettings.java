/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record.config;

import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Compaction;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Orphans;
import com.sdh.healthcare.connector.runtime.config.ConnectorSettings;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import com.sdh.healthcare.connector.stage.StageProperties;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The {@code sdh.*} settings every record stage has (STAGES.md §17.7).
 * A stage's own settings record implements this, with its own section
 * besides, such as the convert stage's {@code sdh.convert}; the library
 * reads only these. Its queues, source and sink are in {@code sdh.stage}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface RecordStageSettings extends ConnectorSettings
{

    /**
     * The stage's queues, registry, source and sink.
     *
     * @return {@code sdh.stage}
     */
    StageProperties stage();

    /**
     * The producer's results, if this stage sends them.
     *
     * @return {@code sdh.results}
     */
    Results results();

    /**
     * How records whose messages were lost are found.
     *
     * @return {@code sdh.reconcile}
     */
    Reconcile reconcile();

    /**
     * Table maintenance of the sink.
     *
     * @return {@code sdh.maintenance}
     */
    Maintenance maintenance();

    /**
     * The heap the transforms running at once may use, from the stage's
     * own settings, such as {@code sdh.convert.memory}.
     *
     * @return the bytes; 0 or less for a share of the heap
     */
    default long memoryBytes()
    {
        return 0;
    }

    /**
     * Where record messages go: the stage's output.
     *
     * @return the output queue; null if it isn't set
     */
    @Override
    default CommonSettings.Queue queue()
    {
        return stage().output();
    }

    /**
     * The producer's results (design §5.3.8, STAGES.md §17.5): on at one
     * stage of a pipeline, its last, whose result covers the failures at
     * every stage before it too.
     *
     * @param enabled   whether this stage sends results
     * @param queue     where they go
     * @param recipient the producer's public key, pinned by fingerprint
     * @param upstream  the tables before the source whose failures the
     *                  result reports, earliest first, each in the source's
     *                  catalog; the source's own are always reported
     */
    record Results(@DefaultValue("false") boolean enabled,
            @DefaultValue CommonSettings.Queue queue,
            @DefaultValue CommonSettings.Recipient recipient,
            List<Upstream> upstream)
    {

        /**
         * No earlier tables is none, not null.
         */
        public Results
        {
            upstream = upstream == null ? List.of() : List.copyOf(upstream);
        }

        /**
         * The settings the results channel takes.
         *
         * @return them
         */
        public CommonSettings.Results channel()
        {
            return new CommonSettings.Results(enabled, queue, recipient);
        }
    }

    /**
     * A table before the source, as {@code sdh.results.upstream} names it.
     *
     * @param stage     the stage whose table it is, to put its failures
     *                  down to, such as {@code split}
     * @param namespace its namespace; unset for the source's
     * @param table     its name
     */
    record Upstream(String stage, String namespace, String table)
    {
    }

    /**
     * Finding records whose messages were lost (STAGES.md §9).
     *
     * @param enabled      whether to look; on unless turned off
     * @param grace        how long a record may wait for its message
     *                     before it is processed without it
     * @param lookback     how far back to look
     * @param interval     how often to look, after the first time at
     *                     startup
     * @param backlogQuiet how long since a record older than the grace
     *                     last came off the queue, on any pod, before a run
     *                     rebuilds anything; zero never to wait
     * @param maxDeferral  the longest a run waits for the backlog before it
     *                     goes ahead anyway
     */
    record Reconcile(@DefaultValue("true") boolean enabled,
            @DefaultValue("10m") Duration grace,
            @DefaultValue("2d") Duration lookback,
            @DefaultValue("15m") Duration interval,
            @DefaultValue("15m") Duration backlogQuiet,
            @DefaultValue("6h") Duration maxDeferral)
    {
    }

    /**
     * Table maintenance of the sink (design §8.4), run by the pod that
     * holds the duties lease. The source is the stage before's to keep.
     *
     * @param enabled           whether it runs
     * @param interval          how often it runs
     * @param snapshotRetention how long old snapshots are kept
     * @param minSnapshots      the fewest snapshots kept
     * @param compaction        compaction of small files
     * @param orphans           removal of unreferenced files
     * @param retention         how long rows are kept, in whole days;
     *                          unset keeps them forever
     */
    record Maintenance(@DefaultValue("true") boolean enabled,
            @DefaultValue("1h") Duration interval,
            @DefaultValue("7d") Duration snapshotRetention,
            @DefaultValue("10") int minSnapshots,
            @DefaultValue Compaction compaction,
            @DefaultValue Orphans orphans,
            Duration retention)
    {

        /**
         * What each run does.
         *
         * @return the settings
         */
        public MaintenanceSettings toSettings()
        {
            return MaintenanceScheduler.settings(snapshotRetention,
                    minSnapshots, compaction, orphans);
        }

        /**
         * When it runs.
         *
         * @return the schedule
         */
        public MaintenanceScheduler.Schedule toSchedule()
        {
            return new MaintenanceScheduler.Schedule(enabled, interval,
                    orphans.enabled(), orphans.interval());
        }
    }
}
