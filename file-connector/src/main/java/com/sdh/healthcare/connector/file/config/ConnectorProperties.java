/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.config;

import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Compaction;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Crypto;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Lease;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Queue;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Retry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Telemetry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Orphans;
import com.sdh.healthcare.connector.runtime.config.ConnectorSettings;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import com.sdh.healthcare.connector.stage.StageProperties;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * Every {@code sdh.*} setting of the split stage (design §14, STAGES.md
 * §8). The settings
 * both applications share are in
 * {@link com.sdh.healthcare.connector.runtime.config.CommonSettings}.
 * Secrets (passwords, passphrases) normally come from environment
 * variables, such as {@code SDH_CRYPTO_KEYSTORE_PASSWORD}, or a Spring
 * {@code configtree:} mount.
 *
 * @param connectorId    the stage's ID, recorded in messages and naming
 *                       its leases
 * @param landing        the intake it splits, and where its items are
 * @param split          how items are split
 * @param storage        the stage's catalog and namespace
 * @param lease          the lease database and timings, for the item and
 *                       duties leases
 * @param crypto         the keys and who holds them
 * @param stage          the broker {@code item.landed} messages come from,
 *                       where record messages go, and the Postgres schema
 *                       of the items' checkpoints
 * @param retry          how transient failures are retried
 * @param maintenance    table maintenance
 * @param telemetry      the telemetry export
 * @param encryptWorkers hash, encrypt and seal threads; 0 for one per CPU
 *                       core
 * @author Jonas Barnett
 * @since 0.1.0
 */
@ConfigurationProperties("sdh")
public record ConnectorProperties(
        @DefaultValue("file-connector") String connectorId,
        @DefaultValue Landing landing,
        @DefaultValue Split split,
        @DefaultValue Storage storage,
        @DefaultValue Lease lease,
        @DefaultValue Crypto crypto,
        @DefaultValue StageProperties stage,
        @DefaultValue Retry retry,
        @DefaultValue Maintenance maintenance,
        @DefaultValue Telemetry telemetry,
        @DefaultValue("0") int encryptWorkers)
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
     * The intake this stage serves (design §5.3.6).
     *
     * @param intake    its name; {@code item.landed} messages for any other
     *                  intake are refused
     * @param storage   where the landed items are
     * @param reconcile how the stage finds landed items whose messages were
     *                  lost
     */
    public record Landing(String intake,
            @DefaultValue LandingStorage storage,
            @DefaultValue Reconcile reconcile)
    {
    }

    /**
     * Finding landed items whose {@code item.landed} messages were lost
     * (docs/DEPLOYMENT.md).
     *
     * @param enabled  whether to look; on unless turned off
     * @param grace    how long an item may wait for its message before it
     *                 is added without it
     * @param interval how often to look, after the first time at startup
     */
    public record Reconcile(@DefaultValue("true") boolean enabled,
            @DefaultValue("10m") Duration grace,
            @DefaultValue("15m") Duration interval)
    {
    }

    /**
     * The landing catalog, read-only to the stage.
     *
     * @param catalog     Iceberg catalog properties; unset to use
     *                    {@code sdh.storage.catalog}
     * @param catalogName the catalog's name; {@code item.landed} messages
     *                    naming another are refused
     * @param namespace   the landing namespace
     */
    public record LandingStorage(Map<String, String> catalog,
            @DefaultValue("sdh") String catalogName,
            @DefaultValue(LandingTables.DEFAULT_NAMESPACE) String namespace)
    {
    }

    /**
     * How items are split. Records are converted by the convert stage.
     *
     * @param format   the format, such as {@code csv}
     * @param boundary a boundary override, where the format has one
     * @param options  format options as in design §6.2, such as
     *                 {@code csv.header} or {@code namespace}
     * @param limits   size limits; null values keep the format's
     */
    public record Split(String format, String boundary,
            Map<String, String> options, @DefaultValue Limits limits)
    {
    }

    /**
     * Size limits (design §6.3). Null values keep the format's defaults.
     *
     * @param maxItemBytes      the largest item
     * @param maxRecordBytes    the largest record
     * @param maxRecordsPerItem the most records in one item
     */
    public record Limits(DataSize maxItemBytes, DataSize maxRecordBytes,
            Long maxRecordsPerItem)
    {
    }

    /**
     * The stage's catalog.
     *
     * @param catalog     Iceberg catalog properties, passed through as they
     *                    are: {@code type}, {@code uri}, {@code warehouse},
     *                    {@code io-impl}, and so on
     * @param catalogName the catalog's name, also put in messages
     * @param namespace   the stage's namespace, holding
     *                    {@code split_records}
     * @param commit      when a batch of records is committed
     */
    public record Storage(Map<String, String> catalog,
            @DefaultValue("sdh") String catalogName,
            @DefaultValue("sdh_ingest") String namespace,
            @DefaultValue Commit commit)
    {
    }

    /**
     * When a batch of records is committed: at whichever limit comes
     * first.
     *
     * @param maxRecords  the most records in a batch
     * @param maxBytes    the most bytes in a batch
     * @param maxInterval the longest a batch stays open
     */
    public record Commit(@DefaultValue("10000") int maxRecords,
            @DefaultValue("512MB") DataSize maxBytes,
            @DefaultValue("30s") Duration maxInterval)
    {
    }

    /**
     * Table maintenance (design §8.4), run by the pod that holds the
     * duties lease.
     *
     * @param enabled           whether it runs
     * @param interval          how often it runs
     * @param snapshotRetention how long old snapshots are kept
     * @param minSnapshots      the fewest snapshots kept
     * @param compaction        compaction of small files
     * @param orphans           removal of unreferenced files
     * @param retention         how long rows are kept
     */
    public record Maintenance(@DefaultValue("true") boolean enabled,
            @DefaultValue("1h") Duration interval,
            @DefaultValue("7d") Duration snapshotRetention,
            @DefaultValue("10") int minSnapshots,
            @DefaultValue Compaction compaction,
            @DefaultValue Orphans orphans,
            @DefaultValue Retention retention)
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

    /**
     * How long to keep each table's rows, in whole days; unset keeps them
     * forever (the default).
     *
     * @param records the {@code split_records} table
     */
    public record Retention(Duration records)
    {
    }
}
