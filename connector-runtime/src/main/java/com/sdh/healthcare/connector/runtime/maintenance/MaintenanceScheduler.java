/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.maintenance;

import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs table maintenance (design §8.4) on a connector's tables every
 * interval, between items: it only proceeds if it can take the writer lock
 * without waiting, so it never overlaps with the work that writes them.
 * Lease loss, an unknown commit outcome and a tampered checkpoint stop the
 * process; any other failure is logged and tried again next interval.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class MaintenanceScheduler implements AutoCloseable
{

    /** Logs runs and failures, with no PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(MaintenanceScheduler.class);

    /**
     * When maintenance runs.
     *
     * @param enabled        whether it runs at all
     * @param interval       how often
     * @param orphans        whether orphan-file cleanup runs
     * @param orphanInterval how often orphan-file cleanup runs, at most
     */
    public record Schedule(boolean enabled, Duration interval,
            boolean orphans, Duration orphanInterval)
    {

        /**
         * Checks the schedule.
         *
         * @param enabled        whether it runs at all
         * @param interval       how often
         * @param orphans        whether orphan-file cleanup runs
         * @param orphanInterval how often orphan-file cleanup runs, at most
         */
        public Schedule
        {
            Objects.requireNonNull(interval, "interval");
            Objects.requireNonNull(orphanInterval, "orphanInterval");
            if (interval.isNegative() || interval.isZero())
            {
                throw new IllegalArgumentException(
                        "The maintenance interval must be positive");
            }
        }
    }

    /**
     * One table to maintain.
     *
     * @param table      the table
     * @param timeColumn its partition source column, used for retention
     * @param retention  how long rows are kept; empty keeps them forever
     * @param keepFrom   moves the retention cutoff back where rows must be
     *                   kept (see {@link TableMaintenance#run(Table, String,
     *                   Optional, boolean, UnaryOperator)})
     */
    public record Target(Table table, String timeColumn,
            Optional<Duration> retention, UnaryOperator<Instant> keepFrom)
    {

        /**
         * A table whose rows can be purged by day as they are.
         *
         * @param table      the table
         * @param timeColumn its partition source column
         * @param retention  how long rows are kept; empty keeps them forever
         * @return the target
         */
        public static Target of(Table table, String timeColumn,
                Optional<Duration> retention)
        {
            return new Target(table, timeColumn, retention,
                    UnaryOperator.identity());
        }
    }

    /** Runs maintenance on one table. */
    private final TableMaintenance maintenance;

    /** When it runs. */
    private final Schedule schedule;

    /** The tables, asked for at each run. */
    private final Supplier<List<Target>> targets;

    /** Held by whatever writes the tables; taken only if free. */
    private final ReentrantLock writer;

    /** Stops the process on a fatal error. */
    private final FatalExit fatal;

    /** The time. */
    private final Clock clock;

    /** Runs the ticks; null until started, or if disabled. */
    private ScheduledExecutorService executor;

    /** When orphan cleanup last ran. */
    private Instant lastOrphanRun = Instant.EPOCH;

    /**
     * Creates a scheduler; {@link #start} starts it.
     *
     * @param committer fences the commits with the lease
     * @param settings  what each run does
     * @param schedule  when it runs
     * @param targets   the tables, asked for at each run
     * @param writer    held by whatever writes the tables
     * @param fatal     stops the process on a fatal error
     * @param clock     the time
     * @throws IllegalArgumentException if orphan cleanup could delete files
     *                                  less than an hour old
     */
    public MaintenanceScheduler(FencedCommitter committer,
            MaintenanceSettings settings, Schedule schedule,
            Supplier<List<Target>> targets, ReentrantLock writer,
            FatalExit fatal, Clock clock)
    {
        if (settings.orphanMinAge().compareTo(Duration.ofHours(1)) < 0)
        {
            throw new IllegalArgumentException(
                    "sdh.maintenance.orphans.min-age must be at least 1 hour");
        }
        this.maintenance = new TableMaintenance(committer, settings, clock);
        this.schedule = schedule;
        this.targets = targets;
        this.writer = writer;
        this.fatal = fatal;
        this.clock = clock;
    }

    /**
     * What each run does, from the settings both applications share.
     *
     * @param snapshotRetention how long old snapshots are kept
     * @param minSnapshots      the fewest snapshots kept
     * @param compaction        compaction of small files
     * @param orphans           removal of unreferenced files
     * @return the settings
     */
    public static MaintenanceSettings settings(Duration snapshotRetention,
            int minSnapshots, CommonSettings.Compaction compaction,
            CommonSettings.Orphans orphans)
    {
        return new MaintenanceSettings(snapshotRetention, minSnapshots,
                compaction.enabled(), compaction.smallFileFraction(),
                compaction.minInputFiles(),
                compaction.maxBytesPerRun().toBytes(), orphans.minAge());
    }

    /**
     * Checks a retention setting.
     *
     * @param name      the setting's name, for the message
     * @param retention the setting; null keeps rows forever
     * @param minimum   the shortest allowed
     * @return the setting
     * @throws IllegalArgumentException if it is shorter than the minimum
     */
    public static Optional<Duration> retention(String name,
            Duration retention, Duration minimum)
    {
        if (retention != null && retention.compareTo(minimum) < 0)
        {
            throw new IllegalArgumentException(name + " must be at least "
                    + minimum.toDays() + " day(s)");
        }
        return Optional.ofNullable(retention);
    }

    /** Starts the ticks, unless maintenance is disabled. */
    public void start()
    {
        if (!schedule.enabled())
        {
            LOG.info("maintenance.disabled");
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("sdh-maintenance").daemon(true)
                        .factory());
        long every = schedule.interval().toMillis();
        executor.scheduleWithFixedDelay(this::tick, every, every,
                TimeUnit.MILLISECONDS);
    }

    /** One tick: a run, if the writer lock is free. */
    private void tick()
    {
        if (!writer.tryLock())
        {
            LOG.info("maintenance.skipped reason=item_in_progress");
            return;
        }
        try
        {
            runOnce();
        }
        catch (LeaseLostException | CommitStateUnknownException
                | CheckpointTamperedException failure)
        {
            String reason = failure instanceof LeaseLostException lost
                    ? "LEASE_LOST:" + lost.reason()
                    : failure.getClass().getSimpleName();
            LOG.error("maintenance.fatal reason={}", reason);
            fatal.exit(reason);
        }
        catch (RuntimeException failure)
        {
            LOG.warn("maintenance.failed error={}; retrying next interval",
                    failure.getClass().getSimpleName());
        }
        finally
        {
            writer.unlock();
        }
    }

    /**
     * One maintenance pass over every table. The caller must hold the writer
     * lock, or know nothing is writing.
     *
     * @return what was done to each table, in order
     */
    public List<TableMaintenance.Report> runOnce()
    {
        Instant now = clock.instant();
        boolean orphans = schedule.orphans()
                && !now.isBefore(lastOrphanRun.plus(schedule.orphanInterval()));
        List<TableMaintenance.Report> reports = new ArrayList<>();
        for (Target target : targets.get())
        {
            reports.add(maintenance.run(target.table(), target.timeColumn(),
                    target.retention(), orphans, target.keepFrom()));
        }
        if (orphans)
        {
            lastOrphanRun = now;
        }
        return reports;
    }

    /** Stops the ticks, waiting up to a minute for a run to finish. */
    @Override
    public void close()
    {
        if (executor == null)
        {
            return;
        }
        executor.shutdown();
        try
        {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS))
            {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
    }
}
