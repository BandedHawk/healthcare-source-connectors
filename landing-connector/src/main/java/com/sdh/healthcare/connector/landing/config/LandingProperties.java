/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

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
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * Every {@code sdh.*} setting of the landing node (design §5.3, §14). One
 * node serves one intake. The settings both applications share are in
 * {@link com.sdh.healthcare.connector.runtime.config.CommonSettings}; of
 * those, the node ignores {@code sdh.lease.name} and names its lease after
 * the intake (see {@link #leaseName()}), so two nodes for the same intake
 * always share one lease.
 *
 * @param connectorId the node's ID, recorded in audit events
 * @param landing     the intake and its source
 * @param storage     the landing catalog and namespace
 * @param lease       the lease database and timings
 * @param crypto      the keys and who holds them
 * @param queue       where {@code item.landed} messages go
 * @param retry       how transient failures are retried
 * @param telemetry   the telemetry export
 * @param maintenance table maintenance and retention
 * @author Jonas Barnett
 * @since 0.1.0
 */
@ConfigurationProperties("sdh")
public record LandingProperties(
        @DefaultValue("landing-connector") String connectorId,
        @DefaultValue Landing landing,
        @DefaultValue Storage storage,
        @DefaultValue Lease lease,
        @DefaultValue Crypto crypto,
        @DefaultValue Queue queue,
        @DefaultValue Retry retry,
        @DefaultValue Telemetry telemetry,
        @DefaultValue Maintenance maintenance)
        implements ConnectorSettings
{

    /** What the lease's name starts with; the intake's name follows. */
    public static final String LEASE_PREFIX = "landing/";

    /**
     * The lease this node takes: {@code landing/<intake>}.
     *
     * @return the lease's name
     */
    public String leaseName()
    {
        return LEASE_PREFIX + landing.intake();
    }

    /**
     * The intake.
     *
     * @param intake  its name, unique within the landing namespace; it
     *                names the lease, the checkpoint and the ledger entries
     * @param standby wait for the lease for as long as it takes, as a
     *                standby for the active node, instead of exiting if
     *                it is held
     * @param source  where items are picked up
     */
    public record Landing(String intake,
            @DefaultValue("false") boolean standby,
            @DefaultValue Source source)
    {
    }

    /**
     * A directory the node picks items up from.
     *
     * @param path         the directory
     * @param access       {@code read-only}, so nothing in it is changed,
     *                     or {@code read-write}, so each item is removed
     *                     from it after landing
     * @param after        what to do with an item after landing, on a
     *                     read-write source: {@code delete} (the default),
     *                     {@code move} or {@code rename}; {@code none} on a
     *                     read-only source
     * @param moveTo       where {@code move} puts items, keeping their
     *                     relative paths
     * @param renameSuffix what {@code rename} appends to an item's name;
     *                     names ending in it are never picked up
     * @param ready        when an item is ready: {@code settled}, once its
     *                     size and modification time have stayed the same
     *                     for {@code settle}, or {@code marker}, once a
     *                     {@code <name>.done} file sits beside it
     * @param settle       how long an item must stay unchanged, for
     *                     {@code settled}
     * @param include      an Ant-style pattern of the paths to pick up
     * @param recursive    whether to look in subdirectories
     * @param pollInterval how often the directory is listed
     */
    public record Source(Path path,
            @DefaultValue("read-only") String access,
            String after,
            Path moveTo,
            @DefaultValue(".landed") String renameSuffix,
            @DefaultValue("settled") String ready,
            @DefaultValue("2m") Duration settle,
            @DefaultValue("*") String include,
            @DefaultValue("false") boolean recursive,
            @DefaultValue("10s") Duration pollInterval)
    {
    }

    /**
     * The landing catalog (design §5.3.3). It may be a different catalog,
     * on different storage, from the one the splitter writes sections to.
     *
     * @param catalog     Iceberg catalog properties, passed through as they
     *                    are: {@code type}, {@code uri}, {@code warehouse},
     *                    {@code io-impl}, and so on
     * @param catalogName the catalog's name, also put in messages
     * @param namespace   the landing namespace
     * @param commitBytes commit a large item's chunks after at least this
     *                    many bytes, so a restart resumes from there
     */
    public record Storage(Map<String, String> catalog,
            @DefaultValue("sdh") String catalogName,
            @DefaultValue(LandingTables.DEFAULT_NAMESPACE) String namespace,
            @DefaultValue("512MB") DataSize commitBytes)
    {
    }

    /**
     * Table maintenance of the landing tables (design §5.3.13, §8.4), run
     * between items. The ledger is compacted and its snapshots expired, but
     * never purged: it is how a read-only source's files are recognised.
     *
     * @param enabled           whether it runs
     * @param interval          how often to try; a run is skipped while an
     *                          item is being landed
     * @param snapshotRetention how long old snapshots are kept
     * @param minSnapshots      the fewest snapshots kept
     * @param compaction        compaction of small files
     * @param orphans           removal of unreferenced files
     * @param retention         how long landed items are kept
     */
    public record Maintenance(@DefaultValue("true") boolean enabled,
            @DefaultValue("1h") Duration interval,
            @DefaultValue("7d") Duration snapshotRetention,
            @DefaultValue("10") int minSnapshots,
            @DefaultValue Compaction compaction,
            @DefaultValue Orphans orphans,
            @DefaultValue Retention retention)
    {

        /** The shortest retention allowed, so the splitter has time. */
        public static final Duration MIN_RETENTION = Duration.ofDays(7);

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
     * How long landed items are kept, in whole days; unset keeps them
     * forever (the default). At least {@link Maintenance#MIN_RETENTION}.
     *
     * @param landedItems the {@value LandingTables#LANDED_ITEMS} table
     */
    public record Retention(Duration landedItems)
    {
    }
}
