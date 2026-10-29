package com.sdh.healthcare.connector.file.runtime;

import com.sdh.healthcare.connector.contract.table.SectionsTable;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.storage.ArchiveTables;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs table maintenance (design §8.4) on all three ingest tables every {@code interval}, between files: it only
 * proceeds if it can take the writer lock without waiting, so it never overlaps with ingest.
 */
public final class MaintenanceScheduler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceScheduler.class);

    private record Target(Table table, String timeColumn, Optional<Duration> retention) {}

    private final ConnectorRuntime runtime;
    private final ConnectorProperties.Maintenance props;
    private final ReentrantLock writer;
    private final FatalExit fatal;
    private final Clock clock;
    private final TableMaintenance maintenance;
    private ScheduledExecutorService executor;
    private Instant lastOrphanRun = Instant.EPOCH;

    public MaintenanceScheduler(ConnectorRuntime runtime, ConnectorProperties.Maintenance props, ReentrantLock writer,
            FatalExit fatal, Clock clock) {
        this.runtime = runtime;
        this.props = props;
        this.writer = writer;
        this.fatal = fatal;
        this.clock = clock;
        this.maintenance = new TableMaintenance(runtime.committer(), settings(props), clock);
        for (Duration d : new Duration[] {props.retention().sections(), props.retention().sourceFiles(),
                props.retention().failedFiles()}) {
            if (d != null && d.compareTo(Duration.ofDays(1)) < 0) {
                throw new IllegalArgumentException("sdh.maintenance.retention values must be at least 1 day");
            }
        }
        if (props.orphans().minAge().compareTo(Duration.ofHours(1)) < 0) {
            throw new IllegalArgumentException("sdh.maintenance.orphans.min-age must be at least 1 hour");
        }
    }

    static MaintenanceSettings settings(ConnectorProperties.Maintenance p) {
        return new MaintenanceSettings(p.snapshotRetention(), p.minSnapshots(), p.compaction().enabled(),
                p.compaction().smallFileFraction(), p.compaction().minInputFiles(),
                p.compaction().maxBytesPerRun().toBytes(), p.orphans().minAge());
    }

    public void start() {
        if (!props.enabled()) {
            LOG.info("maintenance.disabled");
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("sdh-maintenance").daemon(true).factory());
        long every = props.interval().toMillis();
        executor.scheduleWithFixedDelay(this::tick, every, every, TimeUnit.MILLISECONDS);
    }

    private void tick() {
        if (!writer.tryLock()) {
            LOG.info("maintenance.skipped reason=file_in_progress");
            return;
        }
        try {
            runOnce();
        } catch (LeaseLostException | CommitStateUnknownException | CheckpointTamperedException e) {
            String reason = e instanceof LeaseLostException l ? "LEASE_LOST:" + l.reason() : e.getClass().getSimpleName();
            LOG.error("maintenance.fatal reason={}", reason);
            fatal.exit(reason);
        } catch (RuntimeException e) {
            LOG.warn("maintenance.failed error={}; retrying next interval", e.getClass().getSimpleName());
        } finally {
            writer.unlock();
        }
    }

    /** One maintenance pass over every table. The caller must hold the writer lock (or know ingest is idle). */
    public List<TableMaintenance.Report> runOnce() {
        Instant now = clock.instant();
        boolean orphans = props.orphans().enabled()
                && !now.isBefore(lastOrphanRun.plus(props.orphans().interval()));
        List<TableMaintenance.Report> reports = new ArrayList<>();
        for (Target t : List.of(
                new Target(runtime.sections(), SectionsTable.INGESTED_AT,
                        Optional.ofNullable(props.retention().sections())),
                new Target(runtime.sourceFiles(), ArchiveTables.ARCHIVED_AT,
                        Optional.ofNullable(props.retention().sourceFiles())),
                new Target(runtime.failedFiles(), ArchiveTables.ARCHIVED_AT,
                        Optional.ofNullable(props.retention().failedFiles())))) {
            reports.add(maintenance.run(t.table(), t.timeColumn(), t.retention(), orphans));
        }
        if (orphans) {
            lastOrphanRun = now;
        }
        return reports;
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
