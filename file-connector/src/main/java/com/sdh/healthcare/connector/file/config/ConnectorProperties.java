/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.file.config;

import com.sdh.healthcare.connector.runtime.config.CommonSettings.Crypto;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Lease;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Queue;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Retry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Telemetry;
import com.sdh.healthcare.connector.runtime.config.ConnectorSettings;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * Every {@code sdh.*} setting of the file connector (design §14). The settings both applications
 * share are in {@link com.sdh.healthcare.connector.runtime.config.CommonSettings}. Secrets (passwords, passphrases) are normally supplied through
 * environment variables, e.g. {@code SDH_CRYPTO_KEYSTORE_PASSWORD}, or a Spring {@code configtree:} mount.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@ConfigurationProperties("sdh")
public record ConnectorProperties(
        @DefaultValue("file-connector") String connectorId,
        @DefaultValue File file,
        @DefaultValue Split split,
        @DefaultValue Storage storage,
        @DefaultValue Lease lease,
        @DefaultValue Crypto crypto,
        @DefaultValue Queue queue,
        @DefaultValue Retry retry,
        @DefaultValue Maintenance maintenance,
        @DefaultValue Telemetry telemetry,
        /** Hash/encrypt/seal threads; 0 = one per CPU core. */
        @DefaultValue("0") int encryptWorkers)
        implements ConnectorSettings {

    /**
     * @param readLock {@code changed} (size and mtime unchanged for {@code minAge}) or {@code marker}
     *                 (wait for {@code <name>.done})
     * @param include  Ant-style pattern of files to pick up
     */
    public record File(Path inboundDir, @DefaultValue("changed") String readLock, @DefaultValue("30s") Duration minAge,
            @DefaultValue("5s") Duration pollInterval, @DefaultValue("*") String include) {}

    /** @param options format options as in design §6.2, e.g. {@code csv.header}, {@code namespace} */
    /**
     * @param convert          convert sections to JSON where the format has a converter (design §6.4): csv, ccda, x12,
     *                         edifact. FHIR, generic XML and delimited sections are always stored as split.
     * @param convertMaxBytes  larger sections aren't converted and are stored in their original form as failed
     * @param convertMemory    heap that conversions running at once may use between them; a large section waits
     *                         until there is room for it. Unset: 40% of the maximum heap
     */
    public record Split(String format, String boundary, Map<String, String> options, @DefaultValue Limits limits,
            @DefaultValue("true") boolean convert, @DefaultValue("64MB") DataSize convertMaxBytes,
            DataSize convertMemory) {}

    /** Null values keep the format's defaults (design §6.3). */
    public record Limits(DataSize maxFileBytes, DataSize maxSectionBytes, Long maxSectionsPerFile) {}

    /**
     * @param catalog     Iceberg catalog properties, passed through as-is: {@code type}, {@code uri},
     *                    {@code warehouse}, {@code io-impl}, {@code s3.endpoint}, …
     * @param catalogName name of the catalog, also put in messages for readers to check
     */
    public record Storage(Map<String, String> catalog, @DefaultValue("sdh") String catalogName,
            @DefaultValue("sdh_ingest") String namespace, @DefaultValue Commit commit,
            @DefaultValue Archive archive) {}

    public record Commit(@DefaultValue("10000") int maxSections, @DefaultValue("512MB") DataSize maxBytes,
            @DefaultValue("30s") Duration maxInterval) {}

    public record Archive(@DefaultValue("512MB") DataSize commitBytes) {}

    /**
     * Table maintenance (design §8.4), run between files on the single writer.
     *
     * @param interval how often to try; a run is skipped while a file is being processed
     */
    public record Maintenance(@DefaultValue("true") boolean enabled, @DefaultValue("1h") Duration interval,
            @DefaultValue("7d") Duration snapshotRetention, @DefaultValue("10") int minSnapshots,
            @DefaultValue Compaction compaction, @DefaultValue Orphans orphans, @DefaultValue Retention retention) {}

    public record Compaction(@DefaultValue("true") boolean enabled, @DefaultValue("0.75") double smallFileFraction,
            @DefaultValue("5") int minInputFiles, @DefaultValue("8GB") DataSize maxBytesPerRun) {}

    /** @param minAge never delete an unreferenced file younger than this (at least 1 hour) */
    public record Orphans(@DefaultValue("true") boolean enabled, @DefaultValue("24h") Duration interval,
            @DefaultValue("3d") Duration minAge) {}

    /** How long to keep each table's rows, in whole days; unset = forever (the default). */
    public record Retention(Duration sections, Duration sourceFiles, Duration failedFiles) {}
}
