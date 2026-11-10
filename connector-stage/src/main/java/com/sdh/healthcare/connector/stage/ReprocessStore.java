/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureReason;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.failure.Revisions;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import javax.sql.DataSource;

/**
 * A stage's reprocess runs (STAGES.md §15.8.4): each run an operator asked
 * for, and the objects it chose, a fixed list, with what became of each.
 * The command records a run; the stage's pods queue its objects, a batch
 * each renewal interval, and note each one's outcome as they store it.
 * <ul>
 * <li>{@code <schema>.reprocess_run}: the run's ID (a UUIDv7), stage, what
 * it chose by, who asked and when, the reason (encrypted), the version it
 * was asked under, and its state: {@code PENDING} until an object is
 * queued, {@code QUEUED}, then {@code DONE} or {@code CANCELLED}.</li>
 * <li>{@code <schema>.reprocess_object}: per object, its place, the input
 * chosen, its state ({@code PENDING}, {@code QUEUED}, {@code DONE},
 * {@code REFUSED} with the reason, {@code CANCELLED}), the line it was
 * given, when it was sent, and its outcome.</li>
 * </ul>
 * No content is kept: places, record IDs (keyed digests) and numbers.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ReprocessStore
{

    /** The cause every reprocess's outputs carry (STAGES.md §15.8.7). */
    public static final String CAUSE = "reprocess";

    /** Refused: the place holds something newer than the run chose. */
    public static final String SUPERSEDED = "superseded";

    /** Refused: the input is no longer stored, so it can't be read. */
    public static final String PAST_RETENTION = "past_retention";

    /** A run's state. */
    public enum RunState
    {

        /** Recorded; nothing queued yet. */
        PENDING,

        /** Some of its objects queued. */
        QUEUED,

        /** Every object refused, cancelled or processed. */
        DONE,

        /** Cancelled before any object was queued. */
        CANCELLED
    }

    /** An object's state in a run. */
    public enum ObjectState
    {

        /** Not yet queued. */
        PENDING,

        /** Given its line and sent to the stage's input. */
        QUEUED,

        /** Processed: stored, or failed. */
        DONE,

        /** Not queued, with the reason. */
        REFUSED,

        /** Withdrawn before it was queued. */
        CANCELLED
    }

    /**
     * An object a run chose: its place, and the input there when chosen.
     *
     * @param itemId   the item
     * @param seq      the record's number in its generation
     * @param input    the input's lineage, as the stage before stored it
     * @param recordId the input's record ID
     */
    public record Chosen(UUID itemId, long seq, Lineage input,
            String recordId)
    {

        /**
         * Reads no lineage as the first.
         */
        public Chosen
        {
            input = Lineage.orFirst(input);
        }
    }

    /**
     * A run, as recorded.
     *
     * @param runId    its ID
     * @param selector what it chose by, as the command was given it
     * @param operator who asked for it
     * @param askedAt  when
     * @param version  the stage's version it was asked under
     * @param state    its state
     * @param doneAt   when it ended; null until it did
     */
    public record Run(UUID runId, String selector, String operator,
            Instant askedAt, String version, RunState state, Instant doneAt)
    {
    }

    /**
     * An object of a run, as recorded.
     *
     * @param chosen     the object
     * @param state      its state
     * @param line       the line it was given; null until queued
     * @param reason     why it was refused; null unless it was
     * @param outcome    {@code stored} or {@code failed}; null until done
     * @param outputId   the output's record ID; null unless stored
     * @param sameOutput whether the output is the same as what it
     *                   superseded; null if not known
     */
    public record Listed(Chosen chosen, ObjectState state, Lineage line,
            String reason, String outcome, String outputId,
            Boolean sameOutput)
    {
    }

    /**
     * An object queued, to send.
     *
     * @param runId  its run
     * @param chosen the object
     * @param line   the line it was given
     */
    public record Queued(UUID runId, Chosen chosen, Lineage line)
    {
    }

    /** The registry's database. */
    private final DataSource dataSource;

    /** The stage's registry schema. */
    private final String schema;

    /** The stage. */
    private final String stage;

    /** How long a statement may run, in seconds. */
    private final int timeout;

    /** {@code <schema>.reprocess_run}. */
    private final String runs;

    /** {@code <schema>.reprocess_object}. */
    private final String objects;

    /**
     * A stage's reprocess runs.
     *
     * @param dataSource the registry's database
     * @param schema     the stage's registry schema
     * @param stage      the stage
     * @param timeout    how long a statement may run
     */
    public ReprocessStore(DataSource dataSource, String schema, String stage,
            Duration timeout)
    {
        FailureStore.table(schema);
        this.dataSource = dataSource;
        this.schema = schema;
        this.stage = stage;
        this.timeout = (int) Math.max(1, timeout.toSeconds());
        this.runs = runs(schema);
        this.objects = objects(schema);
    }

    /**
     * The runs' table.
     *
     * @param schema the stage's registry schema
     * @return the table, with its schema
     */
    static String runs(String schema)
    {
        return schema + ".reprocess_run";
    }

    /**
     * The objects' table.
     *
     * @param schema the stage's registry schema
     * @return the table, with its schema
     */
    static String objects(String schema)
    {
        return schema + ".reprocess_object";
    }

    /**
     * Makes the tables, for a stage with no stage registry (the split
     * stage, STAGES.md §15.8.4); a registry makes them itself.
     *
     * @throws IllegalStateException if the database can't be reached
     */
    public void createTablesIfMissing()
    {
        try (Connection connection = dataSource.getConnection();
                java.sql.Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(timeout);
            for (String ddl : ddls(schema))
            {
                statement.execute(ddl);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the reprocess tables", failure);
        }
    }

    /**
     * The statements that make the tables.
     *
     * @param schema the stage's registry schema
     * @return the statements, in order
     */
    public static List<String> ddls(String schema)
    {
        return List.of("""
                CREATE TABLE IF NOT EXISTS %s (
                  run_id      uuid        PRIMARY KEY,
                  stage       text        NOT NULL,
                  selector    text        NOT NULL,
                  operator    text        NOT NULL,
                  asked_at    timestamptz NOT NULL,
                  reason_enc  bytea       NOT NULL,
                  reason_key  bytea       NOT NULL,
                  reason_kek  text        NOT NULL,
                  version     text        NOT NULL,
                  state       text        NOT NULL,
                  done_at     timestamptz
                )""".formatted(runs(schema)), """
                CREATE TABLE IF NOT EXISTS %s (
                  run_id      uuid        NOT NULL,
                  stage       text        NOT NULL,
                  item_id     uuid        NOT NULL,
                  seq         bigint      NOT NULL,
                  generation  integer     NOT NULL,
                  input_line  integer[]   NOT NULL,
                  record_id   text        NOT NULL,
                  state       text        NOT NULL,
                  line        integer[],
                  reason      text,
                  queued_at   timestamptz,
                  sent_at     timestamptz,
                  outcome     text,
                  output_id   text,
                  same_output boolean,
                  finished_at timestamptz,
                  PRIMARY KEY (run_id, item_id, seq)
                )""".formatted(objects(schema)),
                // Added 2026-11-09 (STAGES.md §15.8.4): the generation an
                // item reprocessed at the split stage is given; null for a
                // record, whose line keeps its input's generation.
                "ALTER TABLE " + objects(schema)
                        + " ADD COLUMN IF NOT EXISTS made_generation integer",
                "CREATE INDEX IF NOT EXISTS reprocess_object_state ON "
                        + objects(schema) + " (stage, state)",
                "CREATE INDEX IF NOT EXISTS reprocess_object_place ON "
                        + objects(schema)
                        + " (stage, item_id, seq, generation)");
    }

    /**
     * Records a run and the objects it chose, in one transaction. The
     * reason is sealed under the run's ID before anything is written.
     *
     * @param selector what it chose by, as given (item IDs, numbers and
     *                 versions only)
     * @param operator who asked for it
     * @param version  the stage's version it is asked under
     * @param reason   seals the reason, given the run's ID
     * @param chosen   the objects, at least one
     * @return the run's ID
     * @throws IllegalArgumentException if nothing is chosen
     * @throws IllegalStateException    if the database can't be reached
     */
    public UUID record(String selector, String operator, String version,
            Function<UUID, FailureReason> reason, List<Chosen> chosen)
    {
        if (chosen.isEmpty())
        {
            throw new IllegalArgumentException("Nothing was chosen");
        }
        UUID runId = UuidV7.next(System.currentTimeMillis());
        FailureReason sealed = reason.apply(runId);
        String addRun = """
                INSERT INTO %s (run_id, stage, selector, operator, asked_at,
                       reason_enc, reason_key, reason_kek, version, state)
                VALUES (?, ?, ?, ?, clock_timestamp(), ?, ?, ?, ?,
                        'PENDING')""".formatted(runs);
        String addObject = """
                INSERT INTO %s (run_id, stage, item_id, seq, generation,
                       input_line, record_id, state)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')
                ON CONFLICT DO NOTHING""".formatted(objects);
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try (PreparedStatement run = connection.prepareStatement(addRun);
                    PreparedStatement object =
                            connection.prepareStatement(addObject))
            {
                run.setQueryTimeout(timeout);
                run.setObject(1, runId);
                run.setString(2, stage);
                run.setString(3, selector);
                run.setString(4, operator);
                run.setBytes(5, sealed.sealed());
                run.setBytes(6, sealed.key());
                run.setString(7, sealed.keyRef());
                run.setString(8, version);
                run.executeUpdate();
                object.setQueryTimeout(timeout);
                for (Chosen each : chosen)
                {
                    object.setObject(1, runId);
                    object.setString(2, stage);
                    object.setObject(3, each.itemId());
                    object.setLong(4, each.seq());
                    object.setInt(5, each.input().generation());
                    object.setArray(6, FailureStore.line(connection,
                            each.input()));
                    object.setString(7, each.recordId());
                    object.addBatch();
                }
                object.executeBatch();
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
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
        return runId;
    }

    /**
     * Decides whether an object may be queued.
     */
    @FunctionalInterface
    public interface Check
    {

        /**
         * Why an object can't be queued.
         *
         * @param chosen the object
         * @return {@link #SUPERSEDED} or {@link #PAST_RETENTION}; null if it
         *         may be queued
         * @throws Exception if its input can't be looked at now; it is
         *                   tried again later
         */
        String refusal(Chosen chosen) throws Exception;
    }

    /**
     * Takes up to a batch of objects not yet queued, of any run, oldest
     * run first, each locked so no other pod takes it ({@code SKIP
     * LOCKED}), and in one transaction checks each, refusing it or giving
     * it its line from the place's counter ({@link Revisions}). The lines
     * are committed before anything is sent, so a pod that stops before
     * sending leaves them to {@link #unsent}. A run with an object queued
     * is {@code QUEUED}. Refusals are audited
     * ({@code stage.reprocess_refused}), and queued objects
     * ({@code stage.reprocess_queued}), once committed.
     *
     * @param limit the most taken
     * @param check decides each one
     * @return the objects queued, to send
     * @throws Exception if the database can't be reached, or an input
     *                   can't be looked at; nothing is changed
     */
    public List<Queued> queue(int limit, Check check) throws Exception
    {
        String take = """
                SELECT run_id, item_id, seq, generation, input_line,
                       record_id
                  FROM %s
                 WHERE stage = ? AND state = 'PENDING'
                 ORDER BY run_id, item_id, seq
                 LIMIT ?
                   FOR UPDATE SKIP LOCKED""".formatted(objects);
        String queued = """
                UPDATE %s SET state = 'QUEUED', line = ?,
                       made_generation = ?, queued_at = clock_timestamp()
                 WHERE run_id = ? AND item_id = ? AND seq = ?"""
                .formatted(objects);
        String refused = """
                UPDATE %s SET state = 'REFUSED', reason = ?,
                       finished_at = clock_timestamp()
                 WHERE run_id = ? AND item_id = ? AND seq = ?"""
                .formatted(objects);
        String running = """
                UPDATE %s SET state = 'QUEUED'
                 WHERE run_id = ? AND state = 'PENDING'""".formatted(runs);
        List<Queued> given = new ArrayList<>();
        List<Object[]> refusals = new ArrayList<>();
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                List<Object[]> taken = new ArrayList<>();
                try (PreparedStatement select =
                        connection.prepareStatement(take))
                {
                    select.setQueryTimeout(timeout);
                    select.setString(1, stage);
                    select.setInt(2, limit);
                    try (ResultSet rows = select.executeQuery())
                    {
                        while (rows.next())
                        {
                            taken.add(new Object[] {
                                rows.getObject(1, UUID.class),
                                new Chosen(rows.getObject(2, UUID.class),
                                        rows.getLong(3),
                                        FailureStore.lineage(rows, 4),
                                        rows.getString(6))});
                        }
                    }
                }
                try (PreparedStatement queue =
                        connection.prepareStatement(queued);
                        PreparedStatement refuse =
                                connection.prepareStatement(refused);
                        PreparedStatement run =
                                connection.prepareStatement(running))
                {
                    queue.setQueryTimeout(timeout);
                    refuse.setQueryTimeout(timeout);
                    run.setQueryTimeout(timeout);
                    for (Object[] row : taken)
                    {
                        UUID runId = (UUID) row[0];
                        Chosen chosen = (Chosen) row[1];
                        String reason = check.refusal(chosen);
                        if (reason != null)
                        {
                            refuse.setString(1, reason);
                            refuse.setObject(2, runId);
                            refuse.setObject(3, chosen.itemId());
                            refuse.setLong(4, chosen.seq());
                            refuse.executeUpdate();
                            refusals.add(new Object[] {runId, chosen,
                                reason});
                            continue;
                        }
                        // An item at the split stage is given its next
                        // generation, a record its place's next revision.
                        boolean generational = CorrectionStore.generational(
                                stage, chosen.seq());
                        Lineage line = generational
                                ? Lineage.of(Revisions.nextGeneration(
                                        connection, schema, stage,
                                        chosen.itemId(), timeout), List.of())
                                : Revisions.next(connection, schema, stage,
                                        chosen.itemId(), chosen.seq(),
                                        chosen.input(), 0, timeout);
                        queue.setArray(1, FailureStore.line(connection,
                                line));
                        if (generational)
                        {
                            queue.setInt(2, line.generation());
                        }
                        else
                        {
                            queue.setNull(2, java.sql.Types.INTEGER);
                        }
                        queue.setObject(3, runId);
                        queue.setObject(4, chosen.itemId());
                        queue.setLong(5, chosen.seq());
                        queue.executeUpdate();
                        run.setObject(1, runId);
                        run.executeUpdate();
                        given.add(new Queued(runId, chosen, line));
                    }
                }
                connection.commit();
            }
            catch (Exception | Error failure)
            {
                connection.rollback();
                throw failure;
            }
        }
        Telemetry telemetry = Telemetry.get();
        for (Object[] refusal : refusals)
        {
            Chosen chosen = (Chosen) refusal[1];
            telemetry.audit("stage.reprocess_refused",
                    place((UUID) refusal[0], chosen)
                            .with(Attr.LINEAGE, chosen.input())
                            .with(Attr.REASON, refusal[2]));
        }
        for (Queued each : given)
        {
            telemetry.audit("stage.reprocess_queued",
                    place(each.runId(), each.chosen())
                            .with(Attr.LINEAGE, each.line()));
        }
        return given;
    }

    /**
     * Objects queued but not marked sent a while after: a pod stopped
     * between giving them their lines and sending them. Sent again under
     * the same lines, so the same message IDs; one sent after all is
     * dropped as a repeat.
     *
     * @param grace how long after queueing an object is taken as unsent
     * @param limit the most returned
     * @return the objects
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Queued> unsent(Duration grace, int limit)
    {
        String sql = """
                SELECT run_id, item_id, seq, generation, input_line,
                       record_id, line,
                       COALESCE(made_generation, generation)
                  FROM %s
                 WHERE stage = ? AND state = 'QUEUED' AND sent_at IS NULL
                   AND queued_at < clock_timestamp() - ?::interval
                 ORDER BY queued_at
                 LIMIT ?""".formatted(objects);
        List<Queued> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            select.setString(2, grace.toMillis() + " milliseconds");
            select.setInt(3, limit);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    Chosen chosen = new Chosen(rows.getObject(2, UUID.class),
                            rows.getLong(3), FailureStore.lineage(rows, 4),
                            rows.getString(6));
                    found.add(new Queued(rows.getObject(1, UUID.class),
                            chosen, lineOf(rows, 8, 7)));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
    }

    /**
     * Notes objects as sent.
     *
     * @param sent the objects
     * @throws IllegalStateException if the database can't be reached
     */
    public void markSent(List<Queued> sent)
    {
        if (sent.isEmpty())
        {
            return;
        }
        String sql = """
                UPDATE %s SET sent_at = clock_timestamp()
                 WHERE run_id = ? AND item_id = ? AND seq = ?"""
                .formatted(objects);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(timeout);
            for (Queued each : sent)
            {
                update.setObject(1, each.runId());
                update.setObject(2, each.chosen().itemId());
                update.setLong(3, each.chosen().seq());
                update.addBatch();
            }
            update.executeBatch();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
    }

    /**
     * Refuses an object queued but not sent, whose input is gone since it
     * was queued, so it isn't sent again each interval. Its line is kept,
     * given and never used. Audited {@code stage.reprocess_refused}.
     *
     * @param queued the object
     * @param reason why
     * @throws IllegalStateException if the database can't be reached
     */
    public void refuse(Queued queued, String reason)
    {
        String sql = """
                UPDATE %s SET state = 'REFUSED', reason = ?,
                       finished_at = clock_timestamp()
                 WHERE run_id = ? AND item_id = ? AND seq = ?
                   AND state = 'QUEUED' AND sent_at IS NULL"""
                .formatted(objects);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(timeout);
            update.setString(1, reason);
            update.setObject(2, queued.runId());
            update.setObject(3, queued.chosen().itemId());
            update.setLong(4, queued.chosen().seq());
            if (update.executeUpdate() == 1)
            {
                Telemetry.get().audit("stage.reprocess_refused",
                        place(queued.runId(), queued.chosen())
                                .with(Attr.LINEAGE, queued.line())
                                .with(Attr.REASON, reason));
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
    }

    /**
     * Notes what came of an object reprocessed, in the registry's
     * transaction as it is stored: done, stored or failed, with its
     * output's record ID and whether its output's digest is that of the
     * object it supersedes at the place. An object not queued under that
     * line, or already done, is left as it is.
     *
     * @param connection the registry's transaction
     * @param schema     the stage's registry schema
     * @param runId      the run
     * @param itemId     the item
     * @param seq        the record's number
     * @param line       the line it was made under
     * @param failed     whether it failed
     * @param outputId   its output's record ID; null if it failed
     * @param sameOutput whether its output is what it supersedes; null if
     *                   it supersedes nothing
     * @param timeout    how long a statement may run, in seconds
     * @return whether an object was noted
     * @throws SQLException if the statement fails
     */
    static boolean noteOutcome(Connection connection, String schema,
            UUID runId, UUID itemId, long seq, Lineage line, boolean failed,
            String outputId, Boolean sameOutput, int timeout)
            throws SQLException
    {
        String sql = """
                UPDATE %s SET state = 'DONE', outcome = ?, output_id = ?,
                       same_output = ?, finished_at = clock_timestamp()
                 WHERE run_id = ? AND item_id = ? AND seq = ?
                   AND state = 'QUEUED' AND line = ?""".formatted(
                objects(schema));
        try (PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(timeout);
            update.setString(1, failed ? "failed" : "stored");
            update.setString(2, outputId);
            if (sameOutput == null)
            {
                update.setNull(3, java.sql.Types.BOOLEAN);
            }
            else
            {
                update.setBoolean(3, sameOutput);
            }
            update.setObject(4, runId);
            update.setObject(5, itemId);
            update.setLong(6, seq);
            update.setArray(7, FailureStore.line(connection, line));
            return update.executeUpdate() == 1;
        }
    }

    /**
     * Notes what came of an item reprocessed at the split stage (STAGES.md
     * §15.8.4), once its generation's end is sent: {@code stored} if it
     * was split to its end, {@code failed} if it failed as a whole, or
     * {@code interrupted} if a newer generation stopped it. Audited
     * {@code stage.reprocessed} and counted. An object not queued under
     * that generation, or already done, is left as it is.
     *
     * @param runId      the run
     * @param itemId     the item
     * @param generation the generation it was given
     * @param outcome    what came of it
     * @return whether an object was noted
     * @throws IllegalStateException if the database can't be reached
     */
    public boolean noteGeneration(UUID runId, UUID itemId, int generation,
            String outcome)
    {
        String sql = """
                UPDATE %s SET state = 'DONE', outcome = ?,
                       finished_at = clock_timestamp()
                 WHERE run_id = ? AND item_id = ? AND seq = -1
                   AND stage = ? AND state = 'QUEUED'
                   AND made_generation = ?""".formatted(objects);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(timeout);
            update.setString(1, outcome);
            update.setObject(2, runId);
            update.setObject(3, itemId);
            update.setString(4, stage);
            update.setInt(5, generation);
            if (update.executeUpdate() != 1)
            {
                return false;
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
        Telemetry telemetry = Telemetry.get();
        telemetry.audit("stage.reprocessed", Fields.of(Attr.STAGE, stage)
                .with(Attr.RUN_ID, runId).with(Attr.ITEM_ID, itemId)
                .with(Attr.SEQ, -1).with(Attr.GENERATION, generation)
                .with(Attr.CAUSE, CAUSE).with(Attr.OUTCOME, outcome));
        telemetry.reprocessed(stage, CAUSE, outcome);
        return true;
    }

    /**
     * The newest generation queued for an item at the split stage and not
     * yet done (STAGES.md §15.8.6): one past the generation being split
     * stops it between batches.
     *
     * @param itemId     the item
     * @param generation the generation being split
     * @return the newer generation; 0 if none is queued
     * @throws IllegalStateException if the database can't be reached
     */
    public int newerQueued(UUID itemId, int generation)
    {
        String sql = """
                SELECT COALESCE(max(made_generation), 0) FROM %s
                 WHERE stage = ? AND item_id = ? AND seq = -1
                   AND state = 'QUEUED' AND made_generation > ?"""
                .formatted(objects);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            select.setObject(2, itemId);
            select.setInt(3, generation);
            try (ResultSet rows = select.executeQuery())
            {
                rows.next();
                return rows.getInt(1);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
    }

    /**
     * How many objects of the stage's runs are still to be processed
     * ({@code PENDING} or {@code QUEUED}), for the drain check's list of
     * work waiting on an operator (STAGES.md §14.4a).
     *
     * @return how many
     * @throws IllegalStateException if the database can't be reached
     */
    public long waiting()
    {
        String sql = """
                SELECT count(*) FROM %1$s o JOIN %2$s r
                    ON r.run_id = o.run_id
                 WHERE r.stage = ? AND o.state IN ('PENDING', 'QUEUED')"""
                .formatted(objects, runs);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            try (ResultSet rows = select.executeQuery())
            {
                rows.next();
                return rows.getLong(1);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
    }

    /**
     * Ends the runs with nothing left to do: every object refused,
     * cancelled or done. Each is audited {@code stage.reprocess_done},
     * with its counts.
     *
     * @return the runs ended
     * @throws IllegalStateException if the database can't be reached
     */
    public List<UUID> finishRuns()
    {
        String sql = """
                UPDATE %1$s r SET state = 'DONE', done_at = clock_timestamp()
                 WHERE r.stage = ? AND r.state IN ('PENDING', 'QUEUED')
                   AND NOT EXISTS (SELECT 1 FROM %2$s o
                                    WHERE o.run_id = r.run_id
                                      AND o.state IN ('PENDING', 'QUEUED'))
                RETURNING r.run_id,
                       (SELECT count(*) FROM %2$s o WHERE o.run_id = r.run_id
                          AND o.outcome = 'stored'),
                       (SELECT count(*) FROM %2$s o WHERE o.run_id = r.run_id
                          AND o.outcome = 'failed'),
                       (SELECT count(*) FROM %2$s o WHERE o.run_id = r.run_id
                          AND o.state = 'REFUSED')"""
                .formatted(runs, objects);
        List<UUID> done = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(timeout);
            update.setString(1, stage);
            try (ResultSet rows = update.executeQuery())
            {
                while (rows.next())
                {
                    UUID runId = rows.getObject(1, UUID.class);
                    done.add(runId);
                    Telemetry.get().audit("stage.reprocess_done",
                            Fields.of(Attr.STAGE, stage)
                                    .with(Attr.RUN_ID, runId)
                                    .with(Attr.COUNT, rows.getLong(2))
                                    .with(Attr.FAILED_COUNT, rows.getLong(3))
                                    .with(Attr.REFUSED_COUNT,
                                            rows.getLong(4)));
                }
            }
            return done;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
    }

    /**
     * A run.
     *
     * @param runId its ID
     * @return the run; empty if the stage has none of that ID
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<Run> run(UUID runId)
    {
        String sql = """
                SELECT selector, operator, asked_at, version, state, done_at
                  FROM %s WHERE run_id = ? AND stage = ?""".formatted(runs);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setObject(1, runId);
            select.setString(2, stage);
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    return Optional.empty();
                }
                Timestamp done = rows.getTimestamp(6);
                return Optional.of(new Run(runId, rows.getString(1),
                        rows.getString(2), rows.getTimestamp(3).toInstant(),
                        rows.getString(4), RunState.valueOf(rows.getString(5)),
                        done == null ? null : done.toInstant()));
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
    }

    /**
     * A run's objects, by place.
     *
     * @param runId the run
     * @return its objects
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Listed> objects(UUID runId)
    {
        String sql = """
                SELECT item_id, seq, generation, input_line, record_id,
                       state, line, reason, outcome, output_id, same_output,
                       COALESCE(made_generation, generation)
                  FROM %s WHERE run_id = ? AND stage = ?
                 ORDER BY item_id, seq""".formatted(objects);
        List<Listed> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setObject(1, runId);
            select.setString(2, stage);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    Object same = rows.getObject(11);
                    found.add(new Listed(new Chosen(
                            rows.getObject(1, UUID.class), rows.getLong(2),
                            FailureStore.lineage(rows, 3), rows.getString(5)),
                            ObjectState.valueOf(rows.getString(6)),
                            rows.getArray(7) == null ? null
                                    : lineOf(rows, 12, 7),
                            rows.getString(8), rows.getString(9),
                            rows.getString(10),
                            same == null ? null : (Boolean) same));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
    }

    /**
     * Cancels what of a run isn't yet queued; a run with nothing queued is
     * {@code CANCELLED}, and one with something queued ends once that is
     * done.
     *
     * @param runId the run
     * @return how many objects were cancelled
     * @throws IllegalStateException if the database can't be reached
     */
    public int cancel(UUID runId)
    {
        String objectsSql = """
                UPDATE %s SET state = 'CANCELLED',
                       finished_at = clock_timestamp()
                 WHERE run_id = ? AND stage = ? AND state = 'PENDING'"""
                .formatted(objects);
        String runSql = """
                UPDATE %s SET state = 'CANCELLED', done_at = clock_timestamp()
                 WHERE run_id = ? AND stage = ? AND state = 'PENDING'"""
                .formatted(runs);
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try (PreparedStatement cancelObjects =
                    connection.prepareStatement(objectsSql);
                    PreparedStatement cancelRun =
                            connection.prepareStatement(runSql))
            {
                cancelObjects.setQueryTimeout(timeout);
                cancelObjects.setObject(1, runId);
                cancelObjects.setString(2, stage);
                int cancelled = cancelObjects.executeUpdate();
                cancelRun.setQueryTimeout(timeout);
                cancelRun.setObject(1, runId);
                cancelRun.setString(2, stage);
                cancelRun.executeUpdate();
                connection.commit();
                return cancelled;
            }
            catch (SQLException | RuntimeException failure)
            {
                connection.rollback();
                throw failure;
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
    }

    /**
     * The places whose current object here was made by a version
     * ({@code --made-by}): stored or failed, neither superseded nor late.
     *
     * @param version the version, as the stage stores it
     * @param itemIds only these items; empty for any
     * @param limit   the most returned
     * @return the places, by item then record number, each its item and
     *         record number
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Object[]> madeBy(String version, List<UUID> itemIds,
            int limit)
    {
        String sql = """
                SELECT DISTINCT item_id, seq FROM %s
                 WHERE stage = ? AND output_version = ?
                   AND state IN ('STORED', 'FAILED') AND seq IS NOT NULL
                   AND NOT late AND superseded_at IS NULL
                   AND (cardinality(?::uuid[]) = 0
                        OR item_id = ANY (?::uuid[]))
                 ORDER BY item_id, seq LIMIT ?""".formatted(
                schema + ".stage_record");
        List<Object[]> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            java.sql.Array items = connection.createArrayOf("uuid",
                    itemIds.toArray());
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            select.setString(2, version);
            select.setArray(3, items);
            select.setArray(4, items);
            select.setInt(5, limit);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.add(new Object[] {rows.getObject(1, UUID.class),
                        rows.getLong(2)});
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
    }

    /**
     * How many operators' corrections are still pending at places chosen:
     * a reprocess queued first takes the next revision, and the
     * correction, processed later, the one after, superseding it.
     *
     * @param chosen the objects
     * @return the count; 0 if the stage takes no corrections
     * @throws IllegalStateException if the database can't be reached
     */
    public int pendingCorrections(List<Chosen> chosen)
    {
        if (chosen.isEmpty())
        {
            return 0;
        }
        String table = CorrectionStore.table(schema);
        String sql = """
                SELECT count(*) FROM %s c
                  JOIN unnest(?::uuid[], ?::bigint[]) AS p (item_id, seq)
                    ON c.item_id = p.item_id AND c.seq = p.seq
                 WHERE c.stage = ? AND c.state = 'PENDING'"""
                .formatted(table);
        try (Connection connection = dataSource.getConnection())
        {
            try (PreparedStatement exists = connection.prepareStatement(
                    "SELECT to_regclass(?) IS NOT NULL"))
            {
                exists.setString(1, table);
                try (ResultSet rows = exists.executeQuery())
                {
                    rows.next();
                    if (!rows.getBoolean(1))
                    {
                        return 0;
                    }
                }
            }
            Object[] items = new Object[chosen.size()];
            Object[] seqs = new Object[chosen.size()];
            for (int index = 0; index < chosen.size(); index++)
            {
                items[index] = chosen.get(index).itemId();
                seqs[index] = chosen.get(index).seq();
            }
            try (PreparedStatement select = connection.prepareStatement(sql))
            {
                select.setQueryTimeout(timeout);
                select.setArray(1, connection.createArrayOf("uuid", items));
                select.setArray(2, connection.createArrayOf("bigint", seqs));
                select.setString(3, stage);
                try (ResultSet rows = select.executeQuery())
                {
                    rows.next();
                    return rows.getInt(1);
                }
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Reprocess store unavailable",
                    failure);
        }
    }

    /**
     * An object's audit fields: the stage, the run and the place.
     *
     * @param runId  the run
     * @param chosen the object
     * @return the fields
     */
    private Fields place(UUID runId, Chosen chosen)
    {
        return Fields.of(Attr.STAGE, stage).with(Attr.RUN_ID, runId)
                .with(Attr.ITEM_ID, chosen.itemId())
                .with(Attr.SEQ, chosen.seq())
                .with(Attr.CAUSE, CAUSE);
    }

    /**
     * Reads a line given: the input's generation, with the line column.
     *
     * @param rows       the rows
     * @param generation the generation column's index
     * @param line       the line column's index
     * @return the lineage
     * @throws SQLException if it can't be read
     */
    private static Lineage lineOf(ResultSet rows, int generation, int line)
            throws SQLException
    {
        List<Integer> revisions = new ArrayList<>();
        java.sql.Array array = rows.getArray(line);
        if (array != null)
        {
            for (Object revision : (Object[]) array.getArray())
            {
                revisions.add(((Number) revision).intValue());
            }
        }
        return Lineage.of(rows.getInt(generation), revisions);
    }
}
