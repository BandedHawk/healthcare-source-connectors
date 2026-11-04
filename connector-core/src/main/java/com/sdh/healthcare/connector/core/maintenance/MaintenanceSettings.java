/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.maintenance;

import java.time.Duration;
import java.util.Objects;

/**
 * Table maintenance settings (design §8.4).
 *
 * @param snapshotRetention     snapshots older than this are expired
 *                              (default 7 days)
 * @param minSnapshots          never keep fewer snapshots than this
 *                              (default 10)
 * @param compaction            bin-pack small files
 * @param smallFileFraction     a data file is "small" below this fraction
 *                              of the table's target file size
 * @param minInputFiles         compact a partition only if it has at least
 *                              this many small files
 * @param maxRewriteBytesPerRun stop compacting after rewriting this much in
 *                              one run, so intake isn't held up
 * @param orphanMinAge          unreferenced files younger than this are
 *                              never deleted (default 3 days)
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record MaintenanceSettings(
        Duration snapshotRetention,
        int minSnapshots,
        boolean compaction,
        double smallFileFraction,
        int minInputFiles,
        long maxRewriteBytesPerRun,
        Duration orphanMinAge)
{

    /**
     * Seven days' snapshots, at least ten; compaction of files under 75% of
     * the target size, five or more at a time, up to 8 GiB a run; orphans
     * older than three days.
     */
    public static final MaintenanceSettings DEFAULT = new MaintenanceSettings(
            Duration.ofDays(7), 10, true, 0.75, 5,
            8L << 30, Duration.ofDays(3));

    /**
     * Checks the settings.
     *
     * @param snapshotRetention     how long snapshots are kept
     * @param minSnapshots          the fewest snapshots kept
     * @param compaction            whether small files are bin-packed
     * @param smallFileFraction     the "small" fraction of the target size
     * @param minInputFiles         the fewest small files worth compacting
     * @param maxRewriteBytesPerRun the most rewritten in one run
     * @param orphanMinAge          the youngest orphan deleted
     * @throws NullPointerException     if a duration is null
     * @throws IllegalArgumentException if a setting is out of range, or the
     *                                  orphan age is under an hour but not
     *                                  zero
     */
    public MaintenanceSettings
    {
        Objects.requireNonNull(snapshotRetention);
        Objects.requireNonNull(orphanMinAge);
        if (minSnapshots < 1 || minInputFiles < 2 || smallFileFraction <= 0
                || smallFileFraction > 1 || maxRewriteBytesPerRun < 1)
        {
            throw new IllegalArgumentException("Invalid maintenance settings");
        }
        if (orphanMinAge.compareTo(Duration.ofHours(1)) < 0
                && !orphanMinAge.isZero())
        {
            throw new IllegalArgumentException(
                    "Orphan minimum age must be at least 1 hour");
        }
    }
}
