/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.core.checkpoint.TableCommitter;
import com.sdh.healthcare.connector.core.maintenance.ItemPurge;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Carries out a scope's item purges (CONTROL-API-DESIGN.md §11, HIPAA.md
 * P4) in the stage's table maintenance, on the pod holding the stage's
 * duties, through the same commit fence as every other maintenance
 * commit. Its maintenance asks at every tick whether any is asked for
 * ({@link PurgeRequests}), and if so runs at once over every table with an
 * {@link ItemPurge} of them all added to each:
 *
 * <ol>
 *   <li>Each run rewrites, per table, up to {@link ItemPurge#BUDGET} of the
 *       data files holding rows of the items, and the run's snapshot
 *       expiry deletes the files replaced.</li>
 *   <li>Once a run finds nothing left to rewrite, the items' corrections,
 *       whose content is a document's, are deleted from the database.</li>
 *   <li>Then it is checked that no table holds a row of each item, and
 *       only then is its purge done, and audited.</li>
 * </ol>
 *
 * <p>The items' failures are kept: their reasons never quote a document's
 * values. Progress is kept in the rows, so a pod taking over the duties
 * carries on. A run that fails is tried again at the next tick; after
 * {@link #TRIES} failures in a row, the purges in it are failed, with the
 * failure's class.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PurgePass implements MaintenanceScheduler.Pass
{

    /** Runs failed in a row before the purges are failed. */
    static final int TRIES = 12;

    /** How long a statement may run, in seconds. */
    private static final int TIMEOUT_SECONDS = 30;

    /** Logs each run, without values. */
    private static final Logger LOG = LoggerFactory.getLogger(
            PurgePass.class);

    /** The purges asked for, and where they stand. */
    private final PurgeRequests requests;

    /** Commits the rewrites, under the duties' fence. */
    private final TableCommitter committer;

    /** The database, writing, for the items' corrections. */
    private final DataSource database;

    /** The corrections table, with its schema; null if the stage has none. */
    private final String corrections;

    /** The stage, in the corrections table. */
    private final String stage;

    /** The most bytes rewritten in a table each run. */
    private final long budget;

    /** The tables, by name, a purge leaves as they are. */
    private final Set<String> kept;

    /** Runs failed in a row. */
    private int failures;

    /**
     * Creates the pass.
     *
     * @param requests    the purges asked for
     * @param committer   commits under the duties' fence
     * @param database    the database, writing
     * @param corrections the corrections table, with its schema; null if
     *                    the stage has none
     * @param stage       the stage, in the corrections table
     * @param budget      the most bytes rewritten in a table each run
     */
    public PurgePass(PurgeRequests requests, TableCommitter committer,
            DataSource database, String corrections, String stage,
            long budget)
    {
        this(requests, committer, database, corrections, stage, budget,
                Set.of());
    }

    /**
     * Creates the pass, leaving some tables as they are: those that hold
     * no PHI, such as the landing ledger.
     *
     * @param requests    the purges asked for
     * @param committer   commits under the duties' fence
     * @param database    the database, writing
     * @param corrections the corrections table, with its schema; null if
     *                    the stage has none
     * @param stage       the stage, in the corrections table
     * @param budget      the most bytes rewritten in a table each run
     * @param kept        the tables, by name, a purge leaves as they are
     */
    public PurgePass(PurgeRequests requests, TableCommitter committer,
            DataSource database, String corrections, String stage,
            long budget, Set<String> kept)
    {
        this.kept = Set.copyOf(kept);
        this.requests = Objects.requireNonNull(requests, "requests");
        this.committer = Objects.requireNonNull(committer, "committer");
        this.database = Objects.requireNonNull(database, "database");
        this.corrections = corrections;
        this.stage = stage;
        this.budget = budget;
    }

    /** One run of the purges: those asked for, and their purge. */
    private static final class Run implements TableMaintenance.Purge
    {

        /** The tables, by name, left as they are. */
        private final Set<String> kept;

        /** The purges asked for. */
        private final List<PurgeRequests.Purge> asked;

        /** Rewrites the tables' files. */
        private final ItemPurge purge;

        /**
         * Creates a run.
         *
         * @param asked the purges asked for
         * @param purge rewrites the tables' files
         * @param kept  the tables, by name, left as they are
         */
        Run(List<PurgeRequests.Purge> asked, ItemPurge purge,
                Set<String> kept)
        {
            this.kept = kept;
            this.asked = List.copyOf(asked);
            this.purge = purge;
        }

        @Override
        public int purge(Table table, Instant now)
        {
            if (kept.contains(table.name()))
            {
                return 0;
            }
            return purge.purge(table, now);
        }
    }

    /** A run of the purges under way, if any is asked for. */
    @Override
    public Optional<TableMaintenance.Purge> due()
    {
        List<PurgeRequests.Purge> asked = requests.active();
        if (asked.isEmpty())
        {
            failures = 0;
            return Optional.empty();
        }
        Set<UUID> items = new LinkedHashSet<>();
        asked.forEach(each -> items.add(each.itemId()));
        return Optional.of(new Run(asked, new ItemPurge(committer, items,
                budget), kept));
    }

    /**
     * Records the run's progress; once a run finds no file left, deletes
     * the items' corrections, checks no row of each is left, and ends its
     * purge.
     */
    @Override
    public void ran(TableMaintenance.Purge purge,
            List<TableMaintenance.Report> reports)
    {
        Run run = (Run) purge;
        long files = 0;
        Map<UUID, Long> rows = new TreeMap<>();
        for (ItemPurge.Done done : run.purge.done().values())
        {
            files += done.files();
            done.rows().forEach((item, count) -> rows.merge(item, count,
                    Long::sum));
        }
        if (!run.purge.quiet())
        {
            failures = 0;
            for (PurgeRequests.Purge asked : run.asked)
            {
                requests.advanced(asked.id(), files,
                        rows.getOrDefault(asked.itemId(), 0L));
            }
            LOG.info("data.purge_run scope={} items={} files={}",
                    requests.scope(), run.asked.size(), files);
            return;
        }
        Set<UUID> items = new LinkedHashSet<>();
        run.asked.forEach(each -> items.add(each.itemId()));
        Map<UUID, Long> left = new TreeMap<>();
        for (Table table : run.purge.tables())
        {
            table.refresh();
            ItemPurge.left(table, items).forEach((item, count) ->
                    left.merge(item, count, Long::sum));
        }
        for (PurgeRequests.Purge asked : run.asked)
        {
            if (left.containsKey(asked.itemId()))
            {
                LOG.warn("data.purge_left scope={} operation={} rows={}",
                        requests.scope(), asked.id(),
                        left.get(asked.itemId()));
                continue;
            }
            long deleted = deleteCorrections(asked.itemId());
            requests.advanced(asked.id(), 0, 0);
            if (requests.ended(asked.id(), PurgeRequests.State.DONE, null))
            {
                PurgeRequests.Purge ended = requests.get(asked.id())
                        .orElse(asked);
                LOG.info("data.purged scope={} operation={} files={} rows={}"
                        + " corrections={}", requests.scope(), asked.id(),
                        ended.files(), ended.rows(), deleted);
                Telemetry.get().audit("data.purged", Fields.of(
                        Attr.CONTROL_SCOPE, requests.scope())
                        .with(Attr.OPERATION_ID, asked.id())
                        .with(Attr.ITEM_ID, asked.itemId().toString())
                        .with(Attr.TICKET, asked.ticket())
                        .with(Attr.FILES, ended.files())
                        .with(Attr.ROWS, ended.rows())
                        .with(Attr.COUNT, deleted));
            }
        }
        if (left.isEmpty())
        {
            failures = 0;
        }
        else
        {
            // Rows a quiet run didn't find: counted as a failed run.
            failed(purge, new IllegalStateException("Rows left"));
        }
    }

    /** Fails the purges after {@link #TRIES} failed runs in a row. */
    @Override
    public void failed(TableMaintenance.Purge purge,
            RuntimeException failure)
    {
        Run run = (Run) purge;
        failures++;
        String reason = failure.getClass().getSimpleName();
        LOG.warn("data.purge_run_failed scope={} error={} tries={}",
                requests.scope(), reason, failures);
        if (failures < TRIES)
        {
            return;
        }
        failures = 0;
        for (PurgeRequests.Purge asked : run.asked)
        {
            try
            {
                if (requests.ended(asked.id(), PurgeRequests.State.FAILED,
                        reason))
                {
                    Telemetry.get().auditWarn("data.purge_failed", Fields.of(
                            Attr.CONTROL_SCOPE, requests.scope())
                            .with(Attr.OPERATION_ID, asked.id())
                            .with(Attr.ITEM_ID, asked.itemId().toString())
                            .with(Attr.REASON, reason));
                }
            }
            catch (RuntimeException unrecorded)
            {
                // The next tick finds it still asked for, and tries again.
            }
        }
    }

    /**
     * Deletes an item's corrections, whose content is a document's.
     *
     * @param item the item
     * @return how many were deleted
     * @throws IllegalStateException if the database can't be reached
     */
    private long deleteCorrections(UUID item)
    {
        if (corrections == null)
        {
            return 0;
        }
        String sql = "DELETE FROM " + corrections
                + " WHERE stage = ? AND item_id = ?";
        try (Connection connection = database.getConnection();
                PreparedStatement delete = connection.prepareStatement(sql))
        {
            delete.setQueryTimeout(TIMEOUT_SECONDS);
            delete.setString(1, stage);
            delete.setObject(2, item);
            return delete.executeUpdate();
        }
        catch (SQLException failure)
        {
            if ("42P01".equals(failure.getSQLState()))
            {
                return 0;
            }
            throw new IllegalStateException("Corrections unavailable",
                    failure);
        }
    }
}
