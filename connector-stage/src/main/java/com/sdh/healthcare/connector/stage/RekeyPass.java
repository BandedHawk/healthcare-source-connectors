/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.checkpoint.TableCommitter;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.WrappedRows;
import com.sdh.healthcare.connector.core.maintenance.Rekey;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Carries out a scope's re-key (CONTROL-API-DESIGN.md §11) in the stage's
 * table maintenance, on the pod holding the stage's duties, through the
 * same commit fence as every other maintenance commit. Its maintenance
 * asks at every tick whether one is asked for ({@link ControlState}), and
 * if so runs at once over every table with a {@link Rekey} added to each:
 *
 * <ol>
 *   <li>Each run rewrites, per table, up to {@link Rekey#BUDGET} of the
 *       data files holding rows wrapped with the old KEK, and the run's
 *       snapshot expiry deletes the files replaced.</li>
 *   <li>Once a run finds nothing left to rewrite, the database rows with
 *       wrapped keys ({@link WrappedRows}) and the tables' ID keys
 *       ({@link IdKeys}) are re-wrapped.</li>
 *   <li>Then it is checked that nothing the stage stores is still wrapped
 *       with the old KEK, and only then is the re-key done.</li>
 * </ol>
 *
 * <p>Progress is kept in the row, so a pod taking over the duties carries
 * on. A run that fails is tried again at the next tick; after
 * {@link #TRIES} failures in a row, the re-key is failed, with the
 * failure's class. The old KEK is never disabled here: that is the key
 * custodian's act, in the KMS, once the re-key is done.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RekeyPass implements MaintenanceScheduler.Pass
{

    /** Runs failed in a row before the re-key is failed. */
    static final int TRIES = 12;

    /** Logs each run, without values. */
    private static final Logger LOG = LoggerFactory.getLogger(
            RekeyPass.class);

    /** The scope's state, where the re-key is asked for and kept. */
    private final ControlState state;

    /** Commits the rewrites and ID keys, under the duties' fence. */
    private final TableCommitter committer;

    /** Unwraps with the old KEK and wraps with the new. */
    private final KeyProvider keys;

    /** The database, writing, for the rows with wrapped keys. */
    private final DataSource database;

    /** The stage, for the database tables stages share. */
    private final String stage;

    /** The database tables whose rows hold wrapped keys. */
    private final List<WrappedRows> rows;

    /** The most bytes rewritten in a table each run. */
    private final long budget;

    /** Runs failed in a row. */
    private int failures;

    /**
     * Creates the pass.
     *
     * @param state     the scope's state
     * @param committer commits under the duties' fence
     * @param keys      unwraps with the old KEK and wraps with the new
     * @param database  the database, writing
     * @param stage     the stage, for the tables stages share
     * @param rows      the database tables whose rows hold wrapped keys
     * @param budget    the most bytes rewritten in a table each run
     */
    public RekeyPass(ControlState state, TableCommitter committer,
            KeyProvider keys, DataSource database, String stage,
            List<WrappedRows> rows, long budget)
    {
        this.state = Objects.requireNonNull(state, "state");
        this.committer = Objects.requireNonNull(committer, "committer");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.database = Objects.requireNonNull(database, "database");
        this.stage = stage;
        this.rows = List.copyOf(rows);
        this.budget = budget;
    }

    /** One run of a re-key: the re-key asked for, and its purge. */
    private static final class Run implements TableMaintenance.Purge
    {

        /** The re-key asked for. */
        private final ControlState.Rekeying asked;

        /** Rewrites the tables' files. */
        private final Rekey rekey;

        /**
         * Creates a run.
         *
         * @param asked the re-key asked for
         * @param rekey rewrites the tables' files
         */
        Run(ControlState.Rekeying asked, Rekey rekey)
        {
            this.asked = asked;
            this.rekey = rekey;
        }

        @Override
        public int purge(Table table, Instant now)
        {
            return rekey.purge(table, now);
        }
    }

    /** A run of the re-key under way, if one is asked for. */
    @Override
    public Optional<TableMaintenance.Purge> due()
    {
        Optional<ControlState.Rekeying> asked = state.rekeying();
        if (asked.isEmpty() || !asked.get().state().active())
        {
            failures = 0;
            return Optional.empty();
        }
        ControlState.Rekeying rekeying = asked.get();
        return Optional.of(new Run(rekeying, new Rekey(committer, keys,
                rekeying.from(), rekeying.to(), budget)));
    }

    /**
     * Records the run's progress; once a run finds no file left, re-wraps
     * the database rows and ID keys, checks nothing is left, and ends it.
     */
    @Override
    public void ran(TableMaintenance.Purge purge,
            List<TableMaintenance.Report> reports)
    {
        Run run = (Run) purge;
        ControlState.Rekeying asked = run.asked;
        long files = 0;
        long changed = 0;
        for (Rekey.Done done : run.rekey.done().values())
        {
            files += done.files();
            changed += done.rows();
        }
        if (!run.rekey.quiet())
        {
            state.rekeyAdvanced(asked.id(), files, changed);
            LOG.info("keys.rekey_run scope={} files={} rows={}",
                    state.scope(), files, changed);
            failures = 0;
            return;
        }
        long rewrapped = 0;
        for (WrappedRows each : rows)
        {
            rewrapped += each.rekey(database, stage, keys, asked.from(),
                    asked.to());
        }
        for (Table table : run.rekey.tables())
        {
            if (IdKeys.rekey(table, committer, keys, asked.from(),
                    asked.to()))
            {
                rewrapped++;
            }
        }
        long left = left(run.rekey.tables(), asked.from());
        state.rekeyAdvanced(asked.id(), 0, rewrapped);
        failures = 0;
        if (left > 0)
        {
            LOG.warn("keys.rekey_left scope={} rows={}", state.scope(),
                    left);
            return;
        }
        if (state.rekeyEnded(asked.id(), ControlState.RekeyState.DONE,
                null))
        {
            ControlState.Rekeying ended = state.rekeying().orElse(asked);
            LOG.info("keys.rekeyed scope={} files={} rows={}", state.scope(),
                    ended.files(), ended.rows());
            Telemetry.get().audit("keys.rekeyed", Fields.of(
                    Attr.CONTROL_SCOPE, state.scope())
                    .with(Attr.OPERATION_ID, asked.id())
                    .with(Attr.KEK_FROM, asked.from())
                    .with(Attr.KEK_TO, asked.to())
                    .with(Attr.FILES, ended.files())
                    .with(Attr.ROWS, ended.rows()));
        }
    }

    /** Fails the re-key after {@link #TRIES} failed runs in a row. */
    @Override
    public void failed(TableMaintenance.Purge purge,
            RuntimeException failure)
    {
        Run run = (Run) purge;
        failures++;
        String reason = failure.getClass().getSimpleName();
        LOG.warn("keys.rekey_run_failed scope={} error={} tries={}",
                state.scope(), reason, failures);
        if (failures < TRIES)
        {
            return;
        }
        failures = 0;
        try
        {
            if (state.rekeyEnded(run.asked.id(),
                    ControlState.RekeyState.FAILED, reason))
            {
                Telemetry.get().auditWarn("keys.rekey_failed", Fields.of(
                        Attr.CONTROL_SCOPE, state.scope())
                        .with(Attr.OPERATION_ID, run.asked.id())
                        .with(Attr.KEK_FROM, run.asked.from())
                        .with(Attr.KEK_TO, run.asked.to())
                        .with(Attr.REASON, reason));
            }
        }
        catch (RuntimeException unrecorded)
        {
            // The next tick finds it still asked for, and tries again.
        }
    }

    /**
     * How much the stage still stores wrapped with a KEK: rows of its
     * tables and database, and ID keys.
     *
     * @param tables its tables
     * @param kek    the KEK
     * @return how many
     */
    private long left(List<Table> tables, String kek)
    {
        long left = 0;
        for (Table table : tables)
        {
            left += Rekey.left(table, kek);
            if (IdKeys.kek(table).filter(kek::equals).isPresent())
            {
                left++;
            }
        }
        for (WrappedRows each : rows)
        {
            left += each.byKek(database, stage).getOrDefault(kek, 0L);
        }
        return left;
    }
}
