/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.failure;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * A stage's corrections (STAGES.md §15.4, §15.6c): an operator's edit of
 * what the stage failed, kept encrypted in {@code <schema>.stage_correction}
 * until one of the stage's workers processes it, as any input, and then
 * kept as the record of what was submitted. Submitting one moves its
 * failure to {@code REVISION_PENDING} in the same transaction, so only
 * one is in flight per failure. The content is encrypted with a data key of
 * its own, wrapped with the stage's records KEK, and bound to its item,
 * record number and correction number.
 * <p>
 * A retry (STAGES.md §15.6e) is a correction without an edit: what failed,
 * submitted unchanged, of kind {@link Kind#RETRY}.
 * <p>
 * Either makes the next revision of its place (§15.8.5), noted when it is
 * submitted: the failed object's line with this stage's revision one more.
 * Once processed, its failure is {@code SUPERSEDED} by that revision
 * whether or not it passed; a revision that failed is a failure of its
 * own, open, of its own line.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class CorrectionStore
{

    /** A correction's state. */
    public enum State
    {
        /** Submitted, not yet processed. */
        PENDING,
        /** Processed, and passed. */
        PASSED,
        /** Processed, and failed: its revision is a failure of its own. */
        FAILED
    }

    /** What was submitted. */
    public enum Kind
    {
        /** An operator's edit of what failed. */
        CORRECTION,
        /** What failed, unchanged, to be processed again. */
        RETRY;

        /**
         * Reads a kind as stored.
         *
         * @param stored the column; null for a correction, as before kinds
         * @return the kind
         */
        static Kind of(String stored)
        {
            return stored == null ? CORRECTION : valueOf(stored);
        }

        /**
         * What it is called in audit events and refusals.
         *
         * @return {@code corrected} or {@code retried}
         */
        public String action()
        {
            return this == RETRY ? "retried" : "corrected";
        }
    }

    /** The states a failure may be corrected from. */
    public static final Set<FailureStore.State> CORRECTABLE = EnumSet.of(
            FailureStore.State.FAILED);

    /** Makes data keys. */
    private static final SecureRandom RANDOM = new SecureRandom();

    /** Connections to the database. */
    private final DataSource dataSource;

    /** The corrections' table, with its schema. */
    private final String table;

    /** The failures' table, with its schema. */
    private final String failures;

    /** The failures' events, with its schema. */
    private final String events;

    /** The stage, such as {@code convert}. */
    private final String stage;

    /** How long a statement may run, in seconds. */
    private final int timeout;

    /**
     * Creates the store.
     *
     * @param dataSource   connections to the database
     * @param schema       the stage's registry schema
     * @param stage        the stage
     * @param queryTimeout how long a statement may run
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier
     */
    public CorrectionStore(DataSource dataSource, String schema, String stage,
            Duration queryTimeout)
    {
        this.dataSource = dataSource;
        this.failures = FailureStore.table(schema);
        this.events = FailureStore.eventsTable(schema);
        this.table = table(schema);
        this.stage = stage;
        this.timeout = (int) Math.max(1, queryTimeout.toSeconds());
    }

    /**
     * The table's name in a schema.
     *
     * @param schema the schema
     * @return {@code <schema>.stage_correction}
     */
    public static String table(String schema)
    {
        FailureStore.table(schema);
        return schema + ".stage_correction";
    }

    /**
     * The statements that make the table, and the revision counters
     * corrections take their numbers from ({@link Revisions}).
     *
     * @param table the table, with its schema
     * @return the statements, in order
     */
    public static List<String> ddls(String table)
    {
        return List.of(Revisions.ddl(table.substring(0,
                table.indexOf('.'))), """
                CREATE TABLE IF NOT EXISTS %s (
                  stage            text        NOT NULL,
                  item_id          uuid        NOT NULL,
                  seq              bigint      NOT NULL,
                  correction       integer     NOT NULL,
                  state            text        NOT NULL,
                  submitted_at     timestamptz NOT NULL,
                  submitted_by     text        NOT NULL,
                  content_enc      bytea       NOT NULL,
                  content_key      bytea       NOT NULL,
                  content_kek      text        NOT NULL,
                  content_length   bigint      NOT NULL,
                  finished_at      timestamptz,
                  result_record_id text,
                  error_code       text,
                  PRIMARY KEY (stage, item_id, seq, correction)
                )""".formatted(table),
                "CREATE INDEX IF NOT EXISTS stage_correction_pending ON "
                        + table + " (stage, state, submitted_at)",
                // When the stage last sent its item's end after the
                // correction: the split stage's, which has no revisit
                // (STAGES.md §15.6d).
                "ALTER TABLE " + table
                        + " ADD COLUMN IF NOT EXISTS notified_at timestamptz",
                // A retry or a correction (§15.6e); null for a correction.
                "ALTER TABLE " + table
                        + " ADD COLUMN IF NOT EXISTS kind text",
                // Added 2026-11-09 (§15.8.5): the failed object's record ID,
                // and the revision the correction makes; null on rows from
                // before, filled when the fail store's states are moved.
                "ALTER TABLE " + table
                        + " ADD COLUMN IF NOT EXISTS record_id text",
                "ALTER TABLE " + table
                        + " ADD COLUMN IF NOT EXISTS generation integer",
                "ALTER TABLE " + table
                        + " ADD COLUMN IF NOT EXISTS line integer[]");
    }

    /**
     * Creates the table if it isn't there, for a stage without a stage
     * registry (the split stage), and moves the fail store's failures out
     * of states that have gone ({@link #moveStates}); the fail store's
     * table is made first.
     *
     * @throws IllegalStateException if it can't be created
     */
    public void createTableIfMissing()
    {
        try (Connection connection = dataSource.getConnection();
                java.sql.Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(timeout);
            for (String ddl : ddls(table))
            {
                statement.execute(ddl);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the correction store", failure);
        }
        Moved moved;
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                moved = moveStates(connection, schema(), stage, timeout);
                connection.commit();
            }
            catch (SQLException | RuntimeException failure)
            {
                connection.rollback();
                throw failure;
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not move the fail store's states", failure);
        }
        moved.audit(stage);
    }

    /**
     * What moving the fail store's states once (STAGES.md §15.8.5) did.
     *
     * @param renamed    failures moved from {@code CORRECTION_PENDING} to
     *                   {@code REVISION_PENDING}
     * @param superseded failures moved from {@code CORRECTED} or
     *                   {@code RETRIED} to {@code SUPERSEDED}
     * @param relabelled failures an earlier build superseded, named again:
     *                   by the revision rather than the record that passed,
     *                   and changed by {@code pipeline}
     * @param refailed   per item, failures moved from
     *                   {@code FAILED_CORRECTION} to {@code SUPERSEDED},
     *                   each with its correction's failure added as one of
     *                   its own
     */
    public record Moved(int renamed, int superseded, int relabelled,
            Map<UUID, Long> refailed)
    {

        /**
         * Whether anything was moved.
         *
         * @return true if it was
         */
        public boolean any()
        {
            return renamed + superseded + relabelled > 0
                    || !refailed.isEmpty();
        }

        /**
         * Audits the move, {@code failure.states_moved}, if anything was
         * moved.
         *
         * @param stage the stage
         */
        public void audit(String stage)
        {
            if (!any())
            {
                return;
            }
            long refailedCount = refailed.values().stream()
                    .mapToLong(Long::longValue).sum();
            Telemetry.get().audit("failure.states_moved",
                    Fields.of(Attr.STAGE, stage)
                            .with(Attr.COUNT, renamed + superseded
                                    + relabelled + refailedCount)
                            .with(Attr.FAILED_COUNT, refailedCount));
        }
    }

    /**
     * Moves a stage's failures out of the states that went in revision (cn)
     * (STAGES.md §15.8.5), once, in the caller's transaction:
     * {@code CORRECTION_PENDING} becomes {@code REVISION_PENDING};
     * {@code CORRECTED} and {@code RETRIED} become {@code SUPERSEDED} by
     * their correction's revision (the record that passed stays in
     * {@code resolved_by}); {@code FAILED_CORRECTION} becomes
     * {@code SUPERSEDED} by its correction's revision, whose failure is
     * added as a {@code FAILED} row of its own. Corrections still pending
     * have their revision noted. Each change of state adds an event, by
     * {@code pipeline}; earlier events keep the names they were written
     * with. Every failure superseded is changed by {@code pipeline}, and
     * one an earlier build of this move named by its record is named by
     * its revision instead. Rows already moved are left alone, so it may
     * run at every start.
     *
     * @param connection the transaction's connection
     * @param schema     the stage's registry schema
     * @param stage      the stage
     * @param timeout    how long a statement may run, in seconds
     * @return what was moved
     * @throws SQLException if a statement fails
     */
    public static Moved moveStates(Connection connection, String schema,
            String stage, int timeout) throws SQLException
    {
        String failures = FailureStore.table(schema);
        String events = FailureStore.eventsTable(schema);
        String corrections = table(schema);
        int place = Lineage.stageOf(stage);
        int renamed;
        try (PreparedStatement update = connection.prepareStatement("""
                UPDATE %s SET state = 'REVISION_PENDING'
                 WHERE stage = ? AND state = 'CORRECTION_PENDING'"""
                .formatted(failures)))
        {
            update.setQueryTimeout(timeout);
            update.setString(1, stage);
            renamed = update.executeUpdate();
        }
        // What passed: superseded by its correction's revision. Rows from
        // before generations are one per number, so one event each.
        String event = """
                INSERT INTO %1$s (stage, item_id, seq, event, at, operator,
                       from_state, to_state, generation, line)
                SELECT f.stage, f.item_id, f.seq,
                       (SELECT count(*) + 1 FROM %1$s e
                         WHERE e.stage = f.stage AND e.item_id = f.item_id
                           AND e.seq = f.seq),
                       clock_timestamp(), 'pipeline', f.state, 'SUPERSEDED',
                       f.generation, f.line
                  FROM %2$s f
                 WHERE f.stage = ? AND f.state IN (%3$s)""";
        try (PreparedStatement add = connection.prepareStatement(
                event.formatted(events, failures, "'CORRECTED', 'RETRIED'")))
        {
            add.setQueryTimeout(timeout);
            add.setString(1, stage);
            add.executeUpdate();
        }
        int superseded = supersede(connection, failures, stage, place,
                "state IN ('CORRECTED', 'RETRIED')", true, timeout);
        // Moved by an earlier build of this move: named by the record that
        // passed, and changed by the operator who corrected it.
        int relabelled = supersede(connection, failures, stage, place,
                "state = 'SUPERSEDED' AND (changed_by IS DISTINCT FROM"
                        + " 'pipeline' OR (superseded_how = 'REVISION'"
                        + " AND superseded_by = resolved_by))", false,
                timeout);
        Map<UUID, Long> refailed = refail(connection, schema, stage, timeout,
                event.formatted(events, failures, "'FAILED_CORRECTION'"));
        // Pending corrections: the revision each makes, read once their
        // failures are in the states of now.
        List<Object[]> unnoted = new ArrayList<>();
        try (PreparedStatement select = connection.prepareStatement("""
                SELECT item_id, seq, correction FROM %s
                 WHERE stage = ? AND state = 'PENDING'
                   AND generation IS NULL""".formatted(corrections)))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    unnoted.add(new Object[] {rows.getObject(1, UUID.class),
                        rows.getLong(2), rows.getInt(3)});
                }
            }
        }
        try (PreparedStatement update = connection.prepareStatement("""
                UPDATE %s SET record_id = ?, generation = ?, line = ?
                 WHERE stage = ? AND item_id = ? AND seq = ?
                   AND correction = ?""".formatted(corrections)))
        {
            update.setQueryTimeout(timeout);
            for (Object[] row : unnoted)
            {
                Optional<FailureStore.Stored> failed = FailureStore.latest(
                        connection, failures, stage, (UUID) row[0],
                        (Long) row[1], timeout);
                Lineage revision = failed.map(stored -> stored.failure()
                        .lineage()).orElse(Lineage.FIRST).nextRevision(place);
                update.setString(1, failed.map(stored -> stored.failure()
                        .recordId()).orElse(null));
                update.setInt(2, revision.generation());
                update.setArray(3, FailureStore.line(connection, revision));
                update.setString(4, stage);
                update.setObject(5, row[0]);
                update.setLong(6, (Long) row[1]);
                update.setInt(7, (Integer) row[2]);
                update.executeUpdate();
            }
        }
        return new Moved(renamed, superseded, relabelled, refailed);
    }

    /**
     * Supersedes the failures that match, as changed by {@code pipeline}:
     * one that passed by a correction (named by its record, in
     * {@code resolved_by}) by that correction's revision, the next of its
     * own at this stage; any other keeps what superseded it.
     *
     * @param connection the transaction's connection
     * @param failures   the fail store's table
     * @param stage      the stage
     * @param place      the stage's place in a lineage
     * @param where      which of the stage's failures
     * @param now        whether they change now, rather than keeping when
     *                   they were superseded
     * @param timeout    how long a statement may run, in seconds
     * @return how many were changed
     * @throws SQLException if a statement fails
     */
    private static int supersede(Connection connection, String failures,
            String stage, int place, String where, boolean now, int timeout)
            throws SQLException
    {
        List<Object[]> found = new ArrayList<>();
        try (PreparedStatement select = connection.prepareStatement("""
                SELECT item_id, seq, generation, line, superseded_how,
                       superseded_by, resolved_by
                  FROM %s
                 WHERE stage = ? AND %s
                   FOR UPDATE""".formatted(failures, where)))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    Lineage lineage = FailureStore.lineage(rows, 3);
                    String how = rows.getString(5);
                    String by = rows.getString(6);
                    // Named by its record, or not yet superseded: the
                    // revision its correction made.
                    boolean byRecord = how == null || ("REVISION".equals(how)
                            && Objects.equals(by, rows.getString(7)));
                    found.add(new Object[] {rows.getObject(1, UUID.class),
                        rows.getLong(2), lineage, how == null ? "REVISION"
                                : how,
                        byRecord ? lineage.nextRevision(place).toString()
                                : by});
                }
            }
        }
        try (PreparedStatement update = connection.prepareStatement("""
                UPDATE %s SET state = 'SUPERSEDED', superseded_how = ?,
                       superseded_by = ?, changed_by = 'pipeline',
                       changed_at = CASE WHEN ? THEN clock_timestamp()
                           ELSE COALESCE(changed_at, clock_timestamp()) END
                 WHERE stage = ? AND item_id = ? AND seq = ?
                   AND generation = ? AND line = ?""".formatted(failures)))
        {
            update.setQueryTimeout(timeout);
            for (Object[] row : found)
            {
                Lineage lineage = (Lineage) row[2];
                update.setString(1, (String) row[3]);
                update.setString(2, (String) row[4]);
                update.setBoolean(3, now);
                update.setString(4, stage);
                update.setObject(5, row[0]);
                update.setLong(6, (Long) row[1]);
                update.setInt(7, lineage.generation());
                update.setArray(8, FailureStore.line(connection, lineage));
                update.executeUpdate();
            }
        }
        return found.size();
    }

    /**
     * Moves failures from {@code FAILED_CORRECTION}: each superseded by its
     * correction's revision, and that revision's failure, with the last
     * failed correction's error, added as a {@code FAILED} row of its own.
     *
     * @param connection the transaction's connection
     * @param schema     the stage's registry schema
     * @param stage      the stage
     * @param timeout    how long a statement may run, in seconds
     * @param event      adds an event to each failure moved
     * @return how many were moved, per item
     * @throws SQLException if a statement fails
     */
    private static Map<UUID, Long> refail(Connection connection,
            String schema, String stage, int timeout, String event)
            throws SQLException
    {
        String failures = FailureStore.table(schema);
        int place = Lineage.stageOf(stage);
        List<FailureStore.Failure> moving = new ArrayList<>();
        try (PreparedStatement select = connection.prepareStatement("""
                SELECT item_id, seq, record_id, error_code, source_offset,
                       failed_version, generation, line
                  FROM %s
                 WHERE stage = ? AND state = 'FAILED_CORRECTION'
                   FOR UPDATE""".formatted(failures)))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    moving.add(new FailureStore.Failure(
                            rows.getObject(1, UUID.class), rows.getLong(2),
                            rows.getString(3), rows.getString(4),
                            rows.getLong(5), rows.getString(6),
                            FailureStore.lineage(rows, 7)));
                }
            }
        }
        Map<UUID, Long> moved = new TreeMap<>();
        if (moving.isEmpty())
        {
            return moved;
        }
        try (PreparedStatement add = connection.prepareStatement(event))
        {
            add.setQueryTimeout(timeout);
            add.setString(1, stage);
            add.executeUpdate();
        }
        String lastError = """
                SELECT error_code FROM %s
                 WHERE stage = ? AND item_id = ? AND seq = ?
                   AND state = 'FAILED'
                 ORDER BY correction DESC
                 LIMIT 1""".formatted(table(schema));
        String supersede = """
                UPDATE %s SET state = 'SUPERSEDED',
                       superseded_how = 'REVISION', superseded_by = ?,
                       changed_by = 'pipeline', changed_at = clock_timestamp()
                 WHERE stage = ? AND item_id = ? AND seq = ?
                   AND generation = ? AND line = ?""".formatted(failures);
        try (PreparedStatement error = connection.prepareStatement(lastError);
                PreparedStatement update =
                        connection.prepareStatement(supersede))
        {
            error.setQueryTimeout(timeout);
            update.setQueryTimeout(timeout);
            List<FailureStore.Failure> added = new ArrayList<>();
            for (FailureStore.Failure failure : moving)
            {
                Lineage revision = failure.lineage().nextRevision(place);
                String code = failure.errorCode();
                error.setString(1, stage);
                error.setObject(2, failure.itemId());
                error.setLong(3, failure.seq());
                try (ResultSet rows = error.executeQuery())
                {
                    if (rows.next() && rows.getString(1) != null)
                    {
                        code = rows.getString(1);
                    }
                }
                update.setString(1, revision.toString());
                update.setString(2, stage);
                update.setObject(3, failure.itemId());
                update.setLong(4, failure.seq());
                update.setInt(5, failure.lineage().generation());
                update.setArray(6, FailureStore.line(connection,
                        failure.lineage()));
                update.executeUpdate();
                added.add(new FailureStore.Failure(failure.itemId(),
                        failure.seq(), failure.recordId(), code,
                        failure.sourceOffset(), failure.version(), revision));
                moved.merge(failure.itemId(), 1L, Long::sum);
            }
            FailureStore.insert(connection, failures, stage, added, timeout);
        }
        return moved;
    }

    /**
     * Notes a correction's outcome in a transaction of its own, and audits
     * it: for a stage without a stage registry (the split stage), once its
     * output is committed and sent. A revision that failed is recorded as
     * a failure of its own.
     *
     * @param outcome what it came to
     * @throws FailureStore.RefusedException if the correction isn't
     *                                       pending
     * @throws IllegalStateException         if the database can't be
     *                                       reached
     */
    public void finish(Outcome outcome)
    {
        finish(outcome, true);
    }

    /**
     * Notes a correction's outcome as {@link #finish(Outcome)} does,
     * choosing whether one that failed is recorded as a failure of its
     * own: not for one never processed, as a whole-item correction a newer
     * generation came before (STAGES.md §15.8.5), or for a generation whose
     * failure the stage records itself.
     *
     * @param outcome       what it came to
     * @param recordFailure whether to record one that failed as a failure
     * @throws FailureStore.RefusedException if the correction isn't
     *                                       pending
     * @throws IllegalStateException         if the database can't be
     *                                       reached
     */
    public void finish(Outcome outcome, boolean recordFailure)
    {
        Finished finished;
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                finished = finish(connection, schema(), stage, outcome,
                        recordFailure, timeout);
                connection.commit();
            }
            catch (SQLException | RuntimeException failure)
            {
                connection.rollback();
                throw failure;
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Correction store unavailable",
                    failure);
        }
        audit(stage, outcome, finished);
    }

    /**
     * The schema the store's tables are in.
     *
     * @return its name
     */
    private String schema()
    {
        return table.substring(0, table.indexOf('.'));
    }

    /**
     * The items with a correction processed whose end the stage hasn't
     * sent since, for longer than a while: a pod stopped between the two.
     *
     * @param olderThan how long ago it was processed, at least
     * @return the items, oldest first
     * @throws IllegalStateException if the database can't be reached
     */
    public List<UUID> unnotified(Duration olderThan)
    {
        String sql = """
                SELECT item_id FROM %s
                 WHERE stage = ? AND state <> 'PENDING'
                   AND notified_at IS NULL
                   AND finished_at <= clock_timestamp()
                                      - ? * interval '1 millisecond'
                 GROUP BY item_id
                 ORDER BY min(finished_at)
                 LIMIT 100""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            select.setLong(2, olderThan.toMillis());
            List<UUID> found = new ArrayList<>();
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.add(rows.getObject(1, UUID.class));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Correction store unavailable",
                    failure);
        }
    }

    /**
     * Whether a correction is still pending: for a stage without a stage
     * registry, read again once its item's lease is held, as another pod
     * may have applied it since it was listed.
     *
     * @param pending the correction, as listed
     * @return true if it is still {@code PENDING}
     * @throws IllegalStateException if the database can't be reached
     */
    public boolean stillPending(Pending pending)
    {
        String sql = """
                SELECT 1 FROM %s
                 WHERE stage = ? AND item_id = ? AND seq = ?
                   AND correction = ? AND state = 'PENDING'"""
                .formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            select.setObject(2, pending.itemId());
            select.setLong(3, pending.seq());
            select.setInt(4, pending.correction());
            try (ResultSet rows = select.executeQuery())
            {
                return rows.next();
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Correction store unavailable",
                    failure);
        }
    }

    /**
     * Notes that the stage sent an item's end after its corrections.
     *
     * @param itemId the item
     * @throws IllegalStateException if the database can't be reached
     */
    public void markNotified(UUID itemId)
    {
        String sql = """
                UPDATE %s SET notified_at = clock_timestamp()
                 WHERE stage = ? AND item_id = ? AND state <> 'PENDING'
                   AND notified_at IS NULL""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(timeout);
            update.setString(1, stage);
            update.setObject(2, itemId);
            update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Correction store unavailable",
                    failure);
        }
    }

    /**
     * A correction as submitted, encrypted.
     *
     * @param itemId      the failure's item
     * @param seq         its record number
     * @param correction  the correction's number for the failure, from 1
     * @param submittedBy who submitted it
     * @param submittedAt when
     * @param sealed      the content, encrypted
     * @param key         its data key, wrapped
     * @param keyRef      the KEK it is wrapped with
     * @param length      the content's length, in bytes
     * @param kind        a correction or a retry
     * @param recordId    the failed object's record ID; null for the item
     *                    as a whole, or if not known
     * @param lineage     the revision it makes (STAGES.md §15.8.5); null if
     *                    not known
     */
    public record Pending(UUID itemId, long seq, int correction,
            String submittedBy, Instant submittedAt, byte[] sealed,
            byte[] key, String keyRef, long length, Kind kind,
            String recordId, Lineage lineage)
    {

        /**
         * A correction or retry, with no revision noted.
         *
         * @param itemId      the failure's item
         * @param seq         its record number
         * @param correction  the correction's number for the failure
         * @param submittedBy who submitted it
         * @param submittedAt when
         * @param sealed      the content, encrypted
         * @param key         its data key, wrapped
         * @param keyRef      the KEK it is wrapped with
         * @param length      the content's length, in bytes
         * @param kind        a correction or a retry
         */
        public Pending(UUID itemId, long seq, int correction,
                String submittedBy, Instant submittedAt, byte[] sealed,
                byte[] key, String keyRef, long length, Kind kind)
        {
            this(itemId, seq, correction, submittedBy, submittedAt, sealed,
                    key, keyRef, length, kind, null, null);
        }

        /**
         * A correction, as submitted before kinds.
         *
         * @param itemId      the failure's item
         * @param seq         its record number
         * @param correction  the correction's number for the failure
         * @param submittedBy who submitted it
         * @param submittedAt when
         * @param sealed      the content, encrypted
         * @param key         its data key, wrapped
         * @param keyRef      the KEK it is wrapped with
         * @param length      the content's length, in bytes
         */
        public Pending(UUID itemId, long seq, int correction,
                String submittedBy, Instant submittedAt, byte[] sealed,
                byte[] key, String keyRef, long length)
        {
            this(itemId, seq, correction, submittedBy, submittedAt, sealed,
                    key, keyRef, length, Kind.CORRECTION);
        }

        /**
         * Decrypts the content, for the stage that processes it.
         *
         * @param keys unwraps the data key
         * @return the content; the caller wipes it once done
         */
        public byte[] open(KeyProvider keys)
        {
            byte[] dataKey = keys.unwrap(new WrappedKey(keyRef, key));
            try
            {
                return RecordCrypto.decryptChunk(
                        RecordCrypto.recordKey(dataKey, itemId, seq), itemId,
                        seq, correction, correction + 1,
                        Math.toIntExact(length), sealed);
            }
            finally
            {
                Arrays.fill(dataKey, (byte) 0);
            }
        }

        /**
         * The name its output is claimed under in the stage's registry: as
         * any object of its place and line is (STAGES.md §15.8.2), or, with
         * no revision noted, one of its own per correction.
         *
         * @return the record ID with its lineage's suffix, or
         *         {@code correction:<item>:<seq>:<n>}
         */
        public String claimId()
        {
            if (recordId != null && lineage != null)
            {
                return recordId + lineage.suffix();
            }
            return "correction:" + itemId + ":" + seq + ":" + correction;
        }
    }

    /**
     * Submits a correction: stores its content, encrypted, and moves its
     * failure to {@code REVISION_PENDING}, in one transaction. Audited
     * {@code failure.correction_submitted}, with no content.
     *
     * @param itemId   the failure's item
     * @param seq      its record number
     * @param content  the corrected input; not kept by this method
     * @param keys     wraps the data key
     * @param keyRef   the stage's records KEK
     * @param operator who submitted it
     * @return the correction's number
     * @throws FailureStore.RefusedException if the failure isn't there, or
     *                                       not in a state it may be
     *                                       corrected from
     * @throws IllegalStateException         if the database can't be
     *                                       reached
     */
    public int submit(UUID itemId, long seq, byte[] content, KeyProvider keys,
            String keyRef, String operator)
    {
        return submit(itemId, seq, content, keys, keyRef, operator,
                Kind.CORRECTION);
    }

    /**
     * Submits a correction or a retry: stores its content, encrypted, with
     * the revision it makes, the failed object's line with this stage's
     * next revision from the place's counter ({@link Revisions}), and moves
     * its failure to {@code REVISION_PENDING},
     * in one transaction. Audited {@code failure.correction_submitted} or
     * {@code failure.retry_submitted}, with no content.
     *
     * @param itemId   the failure's item
     * @param seq      its record number
     * @param content  the corrected input, or for a retry what failed,
     *                 unchanged; not kept by this method
     * @param keys     wraps the data key
     * @param keyRef   the stage's records KEK
     * @param operator who submitted it
     * @param kind     a correction or a retry
     * @return the correction's number
     * @throws FailureStore.RefusedException if the failure isn't there, or
     *                                       not in a state it may be
     *                                       corrected from
     * @throws IllegalStateException         if the database can't be
     *                                       reached
     */
    public int submit(UUID itemId, long seq, byte[] content, KeyProvider keys,
            String keyRef, String operator, Kind kind)
    {
        String next = """
                SELECT COALESCE(max(correction), 0) + 1 FROM %s
                 WHERE stage = ? AND item_id = ? AND seq = ?"""
                .formatted(table);
        String insert = """
                INSERT INTO %s (stage, item_id, seq, correction, state,
                       submitted_at, submitted_by, content_enc, content_key,
                       content_kek, content_length, kind, record_id,
                       generation, line)
                VALUES (?, ?, ?, ?, 'PENDING', clock_timestamp(), ?, ?, ?,
                        ?, ?, ?, ?, ?, ?)""".formatted(table);
        int[] number = new int[1];
        FailureStore.Changed changed;
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                changed = FailureStore.changeIn(connection, failures, events,
                        stage, itemId, seq, CORRECTABLE,
                        FailureStore.State.REVISION_PENDING, kind.action(),
                        operator, null, null, timeout);
                // From the place's counter, so a reprocess queued meanwhile
                // never shares its line (STAGES.md §15.8.4).
                Lineage failed = changed.before().failure().lineage();
                // At the split stage, the item as a whole is cut again as
                // its next generation (STAGES.md §15.8.5).
                Lineage revision = generational(stage, seq)
                        ? Lineage.of(Revisions.nextGeneration(connection,
                                schema(), stage, itemId, timeout), List.of())
                        : Revisions.next(connection, schema(), stage, itemId,
                                seq, failed,
                                failed.revision(Lineage.stageOf(stage)),
                                timeout);
                try (PreparedStatement select =
                        connection.prepareStatement(next))
                {
                    select.setQueryTimeout(timeout);
                    select.setString(1, stage);
                    select.setObject(2, itemId);
                    select.setLong(3, seq);
                    try (ResultSet rows = select.executeQuery())
                    {
                        rows.next();
                        number[0] = rows.getInt(1);
                    }
                }
                byte[] dataKey = new byte[32];
                RANDOM.nextBytes(dataKey);
                try (PreparedStatement add =
                        connection.prepareStatement(insert))
                {
                    WrappedKey wrapped = keys.wrap(keyRef, dataKey);
                    byte[] sealed = RecordCrypto.encryptChunk(
                            RecordCrypto.recordKey(dataKey, itemId, seq),
                            itemId, seq, number[0], number[0] + 1, content);
                    add.setQueryTimeout(timeout);
                    add.setString(1, stage);
                    add.setObject(2, itemId);
                    add.setLong(3, seq);
                    add.setInt(4, number[0]);
                    add.setString(5, operator);
                    add.setBytes(6, sealed);
                    add.setBytes(7, wrapped.wrapped());
                    add.setString(8, wrapped.keyRef());
                    add.setLong(9, content.length);
                    add.setString(10, kind == Kind.RETRY ? kind.name() : null);
                    add.setString(11, changed.before().failure().recordId());
                    add.setInt(12, revision.generation());
                    add.setArray(13, FailureStore.line(connection, revision));
                    add.executeUpdate();
                }
                finally
                {
                    Arrays.fill(dataKey, (byte) 0);
                }
                connection.commit();
            }
            catch (SQLException | RuntimeException failure)
            {
                connection.rollback();
                throw failure;
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Correction store unavailable",
                    failure);
        }
        FailureStore.auditChange(stage, itemId, seq,
                FailureStore.State.REVISION_PENDING, kind == Kind.RETRY
                        ? "retry_submitted" : "correction_submitted",
                operator, changed);
        return number[0];
    }

    /**
     * The corrections not yet processed, oldest first.
     *
     * @param limit the most returned
     * @return the corrections, with their content encrypted
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Pending> pending(int limit)
    {
        String sql = """
                SELECT item_id, seq, correction, submitted_by, submitted_at,
                       content_enc, content_key, content_kek, content_length,
                       kind, record_id, generation, line
                  FROM %s
                 WHERE stage = ? AND state = 'PENDING'
                 ORDER BY submitted_at
                 LIMIT ?""".formatted(table);
        return readPending(sql, select ->
        {
            select.setString(1, stage);
            select.setInt(2, limit);
        });
    }

    /**
     * Finds one correction still pending (STAGES.md §15.8.5): the one a
     * generation is cut from, when it is resumed.
     *
     * @param itemId     the item
     * @param seq        the failure's number
     * @param correction the correction's number
     * @return it; empty if it isn't pending
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<Pending> find(UUID itemId, long seq, int correction)
    {
        String sql = """
                SELECT item_id, seq, correction, submitted_by, submitted_at,
                       content_enc, content_key, content_kek, content_length,
                       kind, record_id, generation, line
                  FROM %s
                 WHERE stage = ? AND item_id = ? AND seq = ?
                   AND correction = ? AND state = 'PENDING'"""
                .formatted(table);
        return readPending(sql, select ->
        {
            select.setString(1, stage);
            select.setObject(2, itemId);
            select.setLong(3, seq);
            select.setInt(4, correction);
        }).stream().findFirst();
    }

    /**
     * Whether an operator has ever submitted a correction or retry of the
     * item at this stage. Until one is, the stage's table holds no records
     * made from corrections, so the stage needn't look for them
     * (PERFORMANCE.md §3.2): this reads the primary key's first columns.
     *
     * @param itemId the item
     * @return true if one has been
     * @throws IllegalStateException if the database can't be reached
     */
    public boolean any(UUID itemId)
    {
        String sql = """
                SELECT seq
                  FROM %s
                 WHERE stage = ? AND item_id = ?""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setMaxRows(1);
            select.setString(1, stage);
            select.setObject(2, itemId);
            try (ResultSet rows = select.executeQuery())
            {
                return rows.next();
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Correction store unavailable",
                    failure);
        }
    }

    /** Sets a query's parameters. */
    @FunctionalInterface
    private interface Parameters
    {

        /**
         * Sets them.
         *
         * @param select the query
         * @throws SQLException if one can't be set
         */
        void set(PreparedStatement select) throws SQLException;
    }

    /**
     * Reads corrections as {@link #pending(int)} lists them.
     *
     * @param sql        the query, selecting the columns pending reads
     * @param parameters sets its parameters
     * @return the corrections
     * @throws IllegalStateException if the database can't be reached
     */
    private List<Pending> readPending(String sql, Parameters parameters)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            parameters.set(select);
            List<Pending> found = new ArrayList<>();
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.add(new Pending(rows.getObject(1, UUID.class),
                            rows.getLong(2), rows.getInt(3),
                            rows.getString(4),
                            rows.getTimestamp(5).toInstant(),
                            rows.getBytes(6), rows.getBytes(7),
                            rows.getString(8), rows.getLong(9),
                            Kind.of(rows.getString(10)), rows.getString(11),
                            rows.getObject(12) == null ? null
                                    : FailureStore.lineage(rows, 12)));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Correction store unavailable",
                    failure);
        }
    }

    /**
     * What processing a correction came to, to note with the stage's
     * output, in its transaction.
     *
     * @param pending   the correction
     * @param passed    whether it passed
     * @param recordId  the record it made, if it passed; null otherwise
     * @param errorCode why it failed, if it did; null otherwise
     * @param version   the version it was processed under
     */
    public record Outcome(Pending pending, boolean passed, String recordId,
            String errorCode, String version)
    {

        /**
         * The audit event's last part.
         *
         * @return {@code corrected}, {@code correction_failed},
         *         {@code retried} or {@code retry_failed}
         */
        String action()
        {
            if (pending.kind() == Kind.RETRY)
            {
                return passed ? "retried" : "retry_failed";
            }
            return passed ? "corrected" : "correction_failed";
        }
    }

    /**
     * What noting a correction's outcome changed, to audit once committed.
     *
     * @param superseded the failure it was made for, if this superseded it;
     *                   empty if something newer had first
     * @param added      the failure of its revision, if this recorded one
     */
    public record Finished(List<FailureStore.Stored> superseded,
            List<FailureStore.Failure> added)
    {
    }

    /**
     * Notes a correction's outcome within the stage's transaction
     * (STAGES.md §15.8.5): the correction {@code PASSED} or {@code FAILED},
     * and the failure it was made for {@code SUPERSEDED} by its revision
     * either way, unless something newer superseded it first. A stage with
     * no stage registry (the split stage) also records a revision that
     * failed as a failure of its own here; a stage registry records it
     * with the revision's other outputs. Not audited: the caller audits
     * once it commits ({@link #audit}).
     *
     * @param connection    the transaction's connection
     * @param schema        the stage's registry schema
     * @param stage         the stage
     * @param outcome       what it came to
     * @param recordFailure whether to record a revision that failed as a
     *                      failure here
     * @param timeout       how long a statement may run, in seconds
     * @return what changed
     * @throws FailureStore.RefusedException if the correction isn't
     *                                       pending
     * @throws SQLException                  if a statement fails
     */
    public static Finished finish(Connection connection, String schema,
            String stage, Outcome outcome, boolean recordFailure,
            int timeout) throws SQLException
    {
        Pending pending = outcome.pending();
        String failures = FailureStore.table(schema);
        String sql = """
                UPDATE %s SET state = ?, finished_at = clock_timestamp(),
                       result_record_id = ?, error_code = ?
                 WHERE stage = ? AND item_id = ? AND seq = ?
                   AND correction = ? AND state = 'PENDING'"""
                .formatted(table(schema));
        try (PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(timeout);
            update.setString(1, outcome.passed() ? State.PASSED.name()
                    : State.FAILED.name());
            update.setString(2, outcome.recordId());
            update.setString(3, outcome.errorCode());
            update.setString(4, stage);
            update.setObject(5, pending.itemId());
            update.setLong(6, pending.seq());
            update.setInt(7, pending.correction());
            if (update.executeUpdate() != 1)
            {
                throw new FailureStore.RefusedException("Correction "
                        + pending.correction() + " isn't pending");
            }
        }
        Lineage revision = revision(connection, failures, stage, pending,
                timeout);
        List<FailureStore.Stored> superseded = FailureStore.supersedePending(
                connection, failures, stage, pending.itemId(), pending.seq(),
                revision, timeout);
        List<FailureStore.Failure> added = List.of();
        if (!outcome.passed() && recordFailure)
        {
            Original original = original(connection, failures, stage,
                    pending.itemId(), pending.seq(), revision, timeout);
            added = FailureStore.insert(connection, failures, stage,
                    List.of(new FailureStore.Failure(pending.itemId(),
                            pending.seq(), original.recordId(),
                            outcome.errorCode(), original.sourceOffset(),
                            outcome.version(), revision)),
                    superseded.isEmpty() ? original.supersededBy() : null,
                    timeout);
        }
        return new Finished(superseded, added);
    }

    /**
     * Whether a correction at a number makes its item's next generation
     * rather than a revision (STAGES.md §15.8.5): one of the item as a
     * whole, at the split stage.
     *
     * @param stage the stage
     * @param seq   the number
     * @return true if it does
     */
    public static boolean generational(String stage, long seq)
    {
        return seq == FailureStore.WHOLE_ITEM
                && Lineage.stageOf(stage) == Lineage.SPLIT;
    }

    /**
     * The revision a correction makes: as noted when it was submitted, or,
     * for one submitted before revisions were noted, the line of the
     * failure it was made for with this stage's revision one more.
     *
     * @param connection the transaction's connection
     * @param failures   the failures' table, with its schema
     * @param stage      the stage
     * @param pending    the correction
     * @param timeout    how long a statement may run, in seconds
     * @return the revision's lineage
     * @throws SQLException if a statement fails
     */
    public static Lineage revision(Connection connection, String failures,
            String stage, Pending pending, int timeout) throws SQLException
    {
        if (pending.lineage() != null)
        {
            return pending.lineage();
        }
        Lineage failed = FailureStore.latest(connection, failures, stage,
                pending.itemId(), pending.seq(), timeout)
                .map(stored -> stored.failure().lineage())
                .orElse(Lineage.FIRST);
        return failed.nextRevision(Lineage.stageOf(stage));
    }

    /**
     * What a revision's failure takes from the failure it followed.
     *
     * @param recordId     the failed object's record ID
     * @param sourceOffset where in the item it starts, in bytes
     * @param supersededBy what superseded that failure, if something did
     *                     before the revision was stored; null otherwise
     */
    private record Original(String recordId, long sourceOffset,
            String supersededBy)
    {
    }

    /**
     * Reads the failure a revision followed: the one of the line with this
     * stage's revision one less.
     *
     * @param connection the transaction's connection
     * @param failures   the failures' table, with its schema
     * @param stage      the stage
     * @param itemId     the item
     * @param seq        the number
     * @param revision   the revision's lineage
     * @param timeout    how long a statement may run, in seconds
     * @return what the revision's failure takes from it
     * @throws SQLException if a statement fails
     */
    private static Original original(Connection connection, String failures,
            String stage, UUID itemId, long seq, Lineage revision,
            int timeout) throws SQLException
    {
        int place = Lineage.stageOf(stage);
        Lineage older = revision.withRevision(place,
                Math.max(1, revision.revision(place) - 1));
        String sql = """
                SELECT record_id, source_offset, superseded_by FROM %s
                 WHERE stage = ? AND item_id = ? AND seq = ?
                   AND generation = ? AND line = ?""".formatted(failures);
        try (PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            select.setObject(2, itemId);
            select.setLong(3, seq);
            select.setInt(4, older.generation());
            select.setArray(5, FailureStore.line(connection, older));
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    return new Original(null, 0, null);
                }
                return new Original(rows.getString(1), rows.getLong(2),
                        rows.getString(3));
            }
        }
    }

    /**
     * Audits a correction's outcome, once noted: {@code failure.corrected},
     * {@code failure.correction_failed}, {@code failure.retried} or
     * {@code failure.retry_failed}, with the stage, item, record number,
     * the revision it made, its number and the operator; then the failure
     * it superseded ({@code failure.superseded}) and the failure of its
     * revision, if recorded ({@code failure.failed}). No content.
     *
     * @param stage    the stage
     * @param outcome  what it came to
     * @param finished what noting it changed
     */
    public static void audit(String stage, Outcome outcome,
            Finished finished)
    {
        Pending pending = outcome.pending();
        Fields fields = Fields.of(Attr.STAGE, stage)
                .with(Attr.ITEM_ID, pending.itemId())
                .with(Attr.SEQ, pending.seq())
                .with(Attr.CORRECTION, pending.correction())
                .with(Attr.OPERATOR, pending.submittedBy());
        if (pending.lineage() != null)
        {
            fields = fields.with(Attr.LINEAGE, pending.lineage().toString());
        }
        if (outcome.errorCode() != null)
        {
            fields = fields.with(Attr.ERROR_CODE, outcome.errorCode());
        }
        Telemetry.get().audit("failure." + outcome.action(), fields);
        FailureStore.auditSuperseded(stage, finished.superseded(),
                FailureStore.Superseded.REVISION, pending.lineage() == null
                        ? null : pending.lineage().toString());
        FailureStore.audit(stage, finished.added());
    }

    /**
     * The corrections of a failure, oldest first, without their content.
     *
     * @param itemId the failure's item
     * @param seq    its record number
     * @return each correction's number, state, who and when, and what it
     *         came to
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Listed> list(UUID itemId, long seq)
    {
        String sql = """
                SELECT correction, state, submitted_by, submitted_at,
                       finished_at, result_record_id, error_code,
                       content_length, kind, generation, line
                  FROM %s
                 WHERE stage = ? AND item_id = ? AND seq = ?
                 ORDER BY correction""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            select.setObject(2, itemId);
            select.setLong(3, seq);
            List<Listed> found = new ArrayList<>();
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    java.sql.Timestamp finished = rows.getTimestamp(5);
                    found.add(new Listed(rows.getInt(1),
                            State.valueOf(rows.getString(2)),
                            rows.getString(3),
                            rows.getTimestamp(4).toInstant(),
                            finished == null ? null : finished.toInstant(),
                            rows.getString(6), rows.getString(7),
                            rows.getLong(8), Kind.of(rows.getString(9)),
                            rows.getObject(10) == null ? null
                                    : FailureStore.lineage(rows, 10)));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Correction store unavailable",
                    failure);
        }
    }

    /**
     * A correction, without its content.
     *
     * @param correction  its number
     * @param state       its state
     * @param submittedBy who submitted it
     * @param submittedAt when
     * @param finishedAt  when it was processed; null if not yet
     * @param recordId    the record it made; null unless it passed
     * @param errorCode   why it failed; null unless it did
     * @param length      its content's length, in bytes
     * @param kind        a correction or a retry
     * @param lineage     the revision it makes, the next of the failure it
     *                    was submitted for (STAGES.md §15.8.5); null for
     *                    one submitted before revisions were noted
     */
    public record Listed(int correction, State state, String submittedBy,
            Instant submittedAt, Instant finishedAt, String recordId,
            String errorCode, long length, Kind kind, Lineage lineage)
    {
    }
}
