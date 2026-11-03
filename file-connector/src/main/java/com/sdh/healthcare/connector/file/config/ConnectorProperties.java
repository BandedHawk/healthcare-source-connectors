/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.config;

import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Crypto;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Lease;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Queue;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Retry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Telemetry;
import com.sdh.healthcare.connector.runtime.config.ConnectorSettings;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * Every {@code sdh.*} setting of the splitter (design §14). The settings
 * both applications share are in
 * {@link com.sdh.healthcare.connector.runtime.config.CommonSettings}.
 * Secrets (passwords, passphrases) normally come from environment
 * variables, such as {@code SDH_CRYPTO_KEYSTORE_PASSWORD}, or a Spring
 * {@code configtree:} mount.
 *
 * @param connectorId    the splitter's ID, recorded in messages and naming
 *                       its checkpoint
 * @param landing        the intake it splits, and where its items are
 * @param split          how items are split and converted
 * @param storage        the splitter's catalog and namespace
 * @param lease          the lease database and timings
 * @param crypto         the keys and who holds them
 * @param queue          where section messages go
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
        @DefaultValue Queue queue,
        @DefaultValue Retry retry,
        @DefaultValue Maintenance maintenance,
        @DefaultValue Telemetry telemetry,
        @DefaultValue("0") int encryptWorkers)
        implements ConnectorSettings
{

    /**
     * The intake this splitter serves (design §5.3.6).
     *
     * @param intake   its name; {@code item.landed} messages for any other
     *                 intake are refused
     * @param queueUri the Camel endpoint {@code item.landed} messages are
     *                 consumed from, such as
     *                 {@code kafka:sdh.landing.claims?brokers=...&groupId=...}
     * @param storage  where the landed items are
     * @param idleWait how long the splitter waits for an item before
     *                 looking again
     */
    public record Landing(String intake, String queueUri,
            @DefaultValue LandingStorage storage,
            @DefaultValue("5s") Duration idleWait)
    {
    }

    /**
     * The landing catalog, read-only to the splitter.
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
     * How items are split and converted.
     *
     * @param format          the format, such as {@code csv}
     * @param boundary        a boundary override, where the format has one
     * @param options         format options as in design §6.2, such as
     *                        {@code csv.header} or {@code namespace}
     * @param limits          size limits; null values keep the format's
     * @param convert         convert sections to JSON where the format has
     *                        a converter (design §6.4)
     * @param convertMaxBytes larger sections aren't converted, and are
     *                        stored in their original form as failed
     * @param convertMemory   heap that conversions running at once may use
     *                        between them; unset for 40% of the heap
     */
    public record Split(String format, String boundary,
            Map<String, String> options, @DefaultValue Limits limits,
            @DefaultValue("true") boolean convert,
            @DefaultValue("64MB") DataSize convertMaxBytes,
            DataSize convertMemory)
    {
    }

    /**
     * Size limits (design §6.3). Null values keep the format's defaults.
     *
     * @param maxFileBytes       the largest item
     * @param maxSectionBytes    the largest section
     * @param maxSectionsPerFile the most sections in one item
     */
    public record Limits(DataSize maxFileBytes, DataSize maxSectionBytes,
            Long maxSectionsPerFile)
    {
    }

    /**
     * The splitter's catalog.
     *
     * @param catalog     Iceberg catalog properties, passed through as they
     *                    are: {@code type}, {@code uri}, {@code warehouse},
     *                    {@code io-impl}, and so on
     * @param catalogName the catalog's name, also put in messages
     * @param namespace   the splitter's namespace
     * @param commit      when a batch of sections is committed
     */
    public record Storage(Map<String, String> catalog,
            @DefaultValue("sdh") String catalogName,
            @DefaultValue("sdh_ingest") String namespace,
            @DefaultValue Commit commit)
    {
    }

    /**
     * When a batch of sections is committed: at whichever limit comes
     * first.
     *
     * @param maxSections the most sections in a batch
     * @param maxBytes    the most bytes in a batch
     * @param maxInterval the longest a batch stays open
     */
    public record Commit(@DefaultValue("10000") int maxSections,
            @DefaultValue("512MB") DataSize maxBytes,
            @DefaultValue("30s") Duration maxInterval)
    {
    }

    /**
     * Table maintenance (design §8.4), run between items on the single
     * writer.
     *
     * @param enabled           whether it runs
     * @param interval          how often to try; a run is skipped while an
     *                          item is being processed
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
    }

    /**
     * Compaction of small files.
     *
     * @param enabled           whether it runs
     * @param smallFileFraction files smaller than this fraction of the
     *                          target size are compacted
     * @param minInputFiles     the fewest files worth compacting together
     * @param maxBytesPerRun    the most bytes rewritten in one run
     */
    public record Compaction(@DefaultValue("true") boolean enabled,
            @DefaultValue("0.75") double smallFileFraction,
            @DefaultValue("5") int minInputFiles,
            @DefaultValue("8GB") DataSize maxBytesPerRun)
    {
    }

    /**
     * Removal of files no snapshot refers to.
     *
     * @param enabled  whether it runs
     * @param interval how often
     * @param minAge   never delete an unreferenced file younger than this
     *                 (at least 1 hour)
     */
    public record Orphans(@DefaultValue("true") boolean enabled,
            @DefaultValue("24h") Duration interval,
            @DefaultValue("3d") Duration minAge)
    {
    }

    /**
     * How long to keep each table's rows, in whole days; unset keeps them
     * forever (the default).
     *
     * @param sections the {@code sections} table
     */
    public record Retention(Duration sections)
    {
    }
}
