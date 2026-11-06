/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.config;

import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Compaction;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Crypto;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Lease;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Orphans;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Queue;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Results;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Retry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Telemetry;
import com.sdh.healthcare.connector.runtime.config.ConnectorSettings;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import com.sdh.healthcare.connector.stage.StageProperties;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * Every {@code sdh.*} setting of the convert stage (STAGES.md §9). The
 * settings every application shares are in
 * {@link com.sdh.healthcare.connector.runtime.config.CommonSettings}.
 * Secrets normally come from environment variables or a Spring
 * {@code configtree:} mount.
 *
 * @param connectorId the stage's ID, naming its duties lease
 * @param convert     how records are converted
 * @param storage     the catalog, the namespace of {@code records}, and
 *                    where {@code split_records} is read from
 * @param lease       the lease database and timings, for the duties lease;
 *                    the stage's registry is in the same database
 * @param crypto      the keys: the records KEK, signing, recipient and
 *                    inbox keys; no landing KEK
 * @param stage       the split stage's output it receives from, where its
 *                    messages go, its dead-letter queue and its registry
 * @param results     the producer's results queue
 * @param reconcile   how records whose messages were lost are found
 * @param retry       how transient failures are retried
 * @param maintenance table maintenance of {@code records}
 * @param telemetry   the telemetry export
 * @author Jonas Barnett
 * @since 0.1.0
 */
@ConfigurationProperties("sdh")
public record ConvertProperties(
        @DefaultValue("convert-stage") String connectorId,
        @DefaultValue Convert convert,
        @DefaultValue Storage storage,
        @DefaultValue Lease lease,
        @DefaultValue Crypto crypto,
        @DefaultValue StageProperties stage,
        @DefaultValue Results results,
        @DefaultValue Reconcile reconcile,
        @DefaultValue Retry retry,
        @DefaultValue Maintenance maintenance,
        @DefaultValue Telemetry telemetry)
        implements ConnectorSettings
{

    /**
     * Where record messages go: the stage's output.
     *
     * @return the output queue; null if it isn't set
     */
    @Override
    public Queue queue()
    {
        return stage.output();
    }

    /**
     * How records are converted (design §6.4).
     *
     * @param options   format options as in design §6.2, applied to every
     *                  format, such as {@code namespace}
     * @param maxBytes  records larger than this are stored as split,
     *                  {@code CONVERT_TOO_LARGE}
     * @param memory    the heap the conversions running at once may use;
     *                  unset for a share of the heap
     * @param autostart false to leave the input and duties stopped, for
     *                  tests that hand messages to the worker themselves
     */
    public record Convert(Map<String, String> options,
            @DefaultValue("64MB") DataSize maxBytes, DataSize memory,
            @DefaultValue("true") boolean autostart)
    {
    }

    /**
     * The stage's catalog.
     *
     * @param catalog         Iceberg catalog properties, passed through as
     *                        they are
     * @param catalogName     the catalog's name, also put in messages; a
     *                        {@code record.split} naming another is refused
     * @param namespace       the namespace of {@code records}
     * @param splitNamespace  the namespace of {@code split_records}; unset
     *                        for the same one
     */
    public record Storage(Map<String, String> catalog,
            @DefaultValue("sdh") String catalogName,
            @DefaultValue("sdh_ingest") String namespace,
            String splitNamespace)
    {

        /**
         * The namespace {@code split_records} is read from.
         *
         * @return the namespace
         */
        public String effectiveSplitNamespace()
        {
            return splitNamespace == null || splitNamespace.isBlank()
                    ? namespace : splitNamespace;
        }
    }

    /**
     * Finding split records whose {@code record.split} messages were lost
     * (STAGES.md §9).
     *
     * @param enabled  whether to look; on unless turned off
     * @param grace    how long a record may wait for its message before it
     *                 is converted without it
     * @param lookback how far back to look
     * @param interval how often to look, after the first time at startup
     */
    public record Reconcile(@DefaultValue("true") boolean enabled,
            @DefaultValue("10m") Duration grace,
            @DefaultValue("2d") Duration lookback,
            @DefaultValue("15m") Duration interval)
    {
    }

    /**
     * Table maintenance of {@code records} (design §8.4), run by the pod
     * that holds the duties lease.
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
    public record Maintenance(@DefaultValue("true") boolean enabled,
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
