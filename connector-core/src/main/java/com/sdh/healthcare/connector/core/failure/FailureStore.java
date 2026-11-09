/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.failure;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * A stage's fail store, the part that changes: the state of each failure,
 * in PostgreSQL, {@code <schema>.stage_failure} (STAGES.md §15.3). The
 * failed object itself stays where the stage stored it, in its output table
 * ({@code split_records}, {@code records}), encrypted, with its error code,
 * in the same commit as the records that passed; it is not announced
 * downstream (§15.1, rule 2). Decided with the user on 2026-11-08, in place
 * of a separate Iceberg table per stage, so a batch stays one commit.
 * <p>
 * A failure is known by its stage, its item and its sequence number in the
 * item; {@code -1} is the item as a whole (a split stage that couldn't cut
 * further). A row is added once, as {@link State#FAILED}; adding it again
 * does nothing, so a stage that crashed and resumed records it once, and
 * audits {@code failure.failed} once. Its later states are an operator's
 * (§15.3, §15.7 step 3).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class FailureStore
{

    /** A schema name: a plain identifier, as it is put in the SQL. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");

    /** The sequence number of a failure of the item as a whole. */
    public static final long WHOLE_ITEM = -1;

    /**
     * The states of a failure (STAGES.md §15.3).
     */
    public enum State
    {
        /** Failed, not acted on. */
        FAILED,

        /** A correction or retry is in flight. */
        CORRECTION_PENDING,

        /** A correction that failed; names the original. */
        FAILED_CORRECTION,

        /** A correction passed; names the object that passed. */
        CORRECTED,

        /** The same input passed on a retry. */
        RETRIED,

        /** The operator chose not to correct it, with a reason. */
        DECLINED,

        /** Resolved by reprocessing an earlier stage. */
        SUPERSEDED;

        /**
         * Whether a failure in this state keeps its item incomplete: it has
         * neither passed nor been declined (§15.6).
         *
         * @return true if open
         */
        public boolean open()
        {
            return this == FAILED || this == CORRECTION_PENDING
                    || this == FAILED_CORRECTION;
        }
    }

    /**
     * A failure, as a stage records it. It holds no content.
     *
     * @param itemId       the item
     * @param seq          the record's number in the item, from 0;
     *                     {@link #WHOLE_ITEM} for the item as a whole
     * @param recordId     the failed record's id; null for the item as a
     *                     whole
     * @param errorCode    why it failed
     * @param sourceOffset where in the item it starts, in bytes
     * @param version      the stage's version that failed it; null if not
     *                     known
     */
    public record Failure(UUID itemId, long seq, String recordId,
            String errorCode, long sourceOffset, String version)
    {
    }

    /**
     * An item's failures at a stage, counted by what keeps it open.
     *
     * @param open     those neither passed nor declined
     * @param declined those an operator declined to correct
     * @param resolved those that later passed
     */
    public record Counts(long open, long declined, long resolved)
    {

        /** No failures. */
        public static final Counts NONE = new Counts(0, 0, 0);

        /**
         * The failures in all.
         *
         * @return their number
         */
        public long total()
        {
            return open + declined + resolved;
        }
    }

    /**
     * A stage's open failures, for alerting (§15.6).
     *
     * @param count  how many are open
     * @param oldest when the oldest of them failed; null if none is open
     */
    public record Open(long count, Instant oldest)
    {
    }

    /** Connections to the database. */
    private final DataSource dataSource;

    /** The table, with its schema. */
    private final String table;

    /** Each change of a failure's state, with its schema. */
    private final String events;

    /** The stage whose failures these are, such as {@code split}. */
    private final String stage;

    /** How long a statement may run, in seconds. */
    private final int queryTimeoutSeconds;

    /**
     * Creates the store.
     *
     * @param dataSource   connections to the database
     * @param schema       the schema its table is in: the stage's own
     * @param stage        the stage, such as {@code split}
     * @param queryTimeout how long a statement may run; at least a second
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier
     */
    public FailureStore(DataSource dataSource, String schema, String stage,
            Duration queryTimeout)
    {
        this.table = table(schema);
        this.events = schema + ".stage_failure_event";
        this.dataSource = dataSource;
        this.stage = stage;
        this.queryTimeoutSeconds =
                (int) Math.max(1, queryTimeout.toSeconds());
    }

    /**
     * The table's name in a schema.
     *
     * @param schema the schema
     * @return {@code <schema>.stage_failure}
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier
     */
    public static String table(String schema)
    {
        if (!IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException(
                    "Invalid fail store schema name");
        }
        return schema + ".stage_failure";
    }

    /**
     * The stage whose failures these are.
     *
     * @return its name
     */
    public String stage()
    {
        return stage;
    }

    /**
     * Creates the table if it isn't there.
     *
     * @throws IllegalStateException if it can't be created
     */
    public void createTableIfMissing()
    {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(queryTimeoutSeconds);
            statement.execute("CREATE SCHEMA IF NOT EXISTS "
                    + table.substring(0, table.indexOf('.')));
            for (String ddl : ddls(table,
                    table.substring(0, table.indexOf('.'))
                            + ".stage_failure_event"))
            {
                statement.execute(ddl);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the fail store table", failure);
        }
    }

    /**
     * The table's definition. The reason a failure was declined is kept
     * encrypted ({@code reason_enc}), since it may describe the data; the
     * columns for resolving a failure are filled by the operator commands
     * and the stages once a later run passes it (§15.4a).
     *
     * @param table the table, with its schema
     * @return the statement
     */
    public static String ddl(String table)
    {
        return """
                CREATE TABLE IF NOT EXISTS %s (
                  stage            text        NOT NULL,
                  item_id          uuid        NOT NULL,
                  seq              bigint      NOT NULL,
                  record_id        text,
                  error_code       text        NOT NULL,
                  source_offset    bigint      NOT NULL,
                  failed_version   text,
                  failed_at        timestamptz NOT NULL,
                  state            text        NOT NULL,
                  changed_at       timestamptz NOT NULL,
                  changed_by       text,
                  reason_enc       bytea,
                  resolved_by      text,
                  resolved_at      timestamptz,
                  resolved_version text,
                  PRIMARY KEY (stage, item_id, seq)
                )""".formatted(table);
    }

    /**
     * Every statement that makes the fail store's tables, or brings older
     * ones up to date: the failures, the key for the reason of the last
     * change (added 2026-11-08, step 3), an index to find failures changed
     * lately, and each change of state as an event of its own, with who,
     * when and why, so the line of a failure is kept whole (§15.3).
     *
     * @param table  the failures' table, with its schema
     * @param events the events' table, with its schema
     * @return the statements, in order
     */
    public static List<String> ddls(String table, String events)
    {
        // An index is made in its table's schema, so its name is plain.
        return List.of(ddl(table),
                "ALTER TABLE " + table
                        + " ADD COLUMN IF NOT EXISTS reason_key bytea",
                "ALTER TABLE " + table
                        + " ADD COLUMN IF NOT EXISTS reason_kek text",
                "CREATE INDEX IF NOT EXISTS stage_failure_changed ON "
                        + table + " (stage, changed_at)",
                """
                CREATE TABLE IF NOT EXISTS %s (
                  stage       text        NOT NULL,
                  item_id     uuid        NOT NULL,
                  seq         bigint      NOT NULL,
                  event       integer     NOT NULL,
                  at          timestamptz NOT NULL,
                  operator    text        NOT NULL,
                  from_state  text        NOT NULL,
                  to_state    text        NOT NULL,
                  reason_enc  bytea,
                  reason_key  bytea,
                  reason_kek  text,
                  PRIMARY KEY (stage, item_id, seq, event)
                )""".formatted(events));
    }

    /**
     * What an operator's change did.
     *
     * @param before the failure as it was
     * @param event  the change's number for the failure, from 1; the
     *               reason is bound to it
     */
    public record Changed(Stored before, int event)
    {
    }

    /**
     * Refused: the failure isn't there, or isn't in a state the change
     * can be made from.
     */
    public static final class RefusedException extends RuntimeException
    {

        /** Serial version. */
        private static final long serialVersionUID = 1L;

        /**
         * Creates the refusal.
         *
         * @param message why, free of PHI
         */
        public RefusedException(String message)
        {
            super(message);
        }
    }

    /** Makes an operator's reason once the event's number is known. */
    @FunctionalInterface
    public interface ReasonSealer
    {

        /**
         * Encrypts the reason for an event.
         *
         * @param event the event's number for the failure
         * @return the reason, encrypted
         */
        FailureReason seal(int event);
    }

    /**
     * An operator changes a failure's state (§15.3): in one transaction,
     * with the row locked, it checks the state is one of {@code from},
     * moves it to {@code to}, and adds the change as an event, with who,
     * when and the reason, encrypted. Audited {@code failure.<action>},
     * with the stage, item, record number, both states and the operator;
     * the reason stays out of telemetry.
     *
     * @param itemId   the failure's item
     * @param seq      its record number; {@link #WHOLE_ITEM} for the item
     * @param from     the states it may be changed from
     * @param to       the state it moves to
     * @param action   the audit event's last part, such as
     *                 {@code declined}
     * @param operator who made the change
     * @param reason   encrypts the reason, given the event's number
     * @return the failure as it was, and the event's number
     * @throws RefusedException      if it isn't there, or not in a state
     *                               it may be changed from
     * @throws IllegalStateException if the database can't be reached
     */
    public Changed change(UUID itemId, long seq, java.util.Set<State> from,
            State to, String action, String operator, ReasonSealer reason)
    {
        return change(itemId, seq, from, to, action, operator, reason, null);
    }

    /**
     * Something done in the same transaction as a change of state, such
     * as storing a correction (§15.4).
     */
    @FunctionalInterface
    public interface InTransaction
    {

        /**
         * Does it.
         *
         * @param connection the transaction's connection
         * @param changed    the failure as it was, and the event's number
         * @throws SQLException if a statement fails; the change is undone
         */
        void run(Connection connection, Changed changed) throws SQLException;
    }

    /**
     * How a failure was resolved (§15.4a): the object that passed, and
     * the version it passed under.
     *
     * @param resolvedBy the object that passed, such as its record ID
     * @param version    the version it passed under
     */
    public record Resolution(String resolvedBy, String version)
    {
    }

    /**
     * Changes a failure's state as {@link #change(UUID, long,
     * java.util.Set, State, String, String, ReasonSealer)} does, and does
     * something more in the same transaction.
     *
     * @param itemId   the failure's item
     * @param seq      its record number
     * @param from     the states it may be changed from
     * @param to       the state it moves to
     * @param action   the audit event's last part
     * @param operator who made the change
     * @param reason   encrypts the reason; null for none
     * @param also     done in the same transaction; null for nothing
     * @return the failure as it was, and the event's number
     * @throws RefusedException      if it isn't there, or not in a state
     *                               it may be changed from
     * @throws IllegalStateException if the database can't be reached
     */
    public Changed change(UUID itemId, long seq, java.util.Set<State> from,
            State to, String action, String operator, ReasonSealer reason,
            InTransaction also)
    {
        Changed changed;
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                changed = changeIn(connection, table, events, stage, itemId,
                        seq, from, to, action, operator, reason, null,
                        queryTimeoutSeconds);
                if (also != null)
                {
                    also.run(connection, changed);
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
            throw new IllegalStateException("Fail store unavailable",
                    failure);
        }
        auditChange(stage, itemId, seq, to, action, operator, changed);
        return changed;
    }

    /**
     * Audits a change of state, once its transaction has committed:
     * {@code failure.<action>}, with no content and no reason.
     *
     * @param stage    the stage
     * @param itemId   the failure's item
     * @param seq      its record number
     * @param to       the state it moved to
     * @param action   the audit event's last part
     * @param operator who made the change
     * @param changed  the failure as it was
     */
    public static void auditChange(String stage, UUID itemId, long seq,
            State to, String action, String operator, Changed changed)
    {
        Telemetry.get().audit("failure." + action,
                Fields.of(Attr.STAGE, stage).with(Attr.ITEM_ID, itemId)
                        .with(Attr.SEQ, seq)
                        .with(Attr.STATE, to.name())
                        .with(Attr.FROM_STATE, changed.before().state()
                                .name())
                        .with(Attr.OPERATOR, operator)
                        .with(Attr.ERROR_CODE,
                                changed.before().failure().errorCode()));
    }

    /**
     * Changes a failure's state within a caller's transaction: locks its
     * row, checks its state, updates it and adds the event. Not audited:
     * the caller audits once it commits ({@link #auditChange}).
     *
     * @param connection   the transaction's connection
     * @param table        the failures' table, with its schema
     * @param events       the events' table, with its schema
     * @param stage        the stage
     * @param itemId       the failure's item
     * @param seq          its record number
     * @param from         the states it may be changed from
     * @param to           the state it moves to
     * @param action       what the change is, for a refusal
     * @param operator     who made the change
     * @param reason       encrypts the reason; null for none
     * @param resolution   how it was resolved, for a resolved state; null
     *                     otherwise
     * @param timeout      how long a statement may run, in seconds
     * @return the failure as it was, and the event's number
     * @throws RefusedException if it isn't there, or not in a state it may
     *                          be changed from
     * @throws SQLException     if a statement fails
     */
    public static Changed changeIn(Connection connection, String table,
            String events, String stage, UUID itemId, long seq,
            java.util.Set<State> from, State to, String action,
            String operator, ReasonSealer reason, Resolution resolution,
            int timeout) throws SQLException
    {
        String lock = """
                SELECT record_id, error_code, source_offset, failed_version,
                       state,
                       (SELECT count(*) FROM %2$s e
                         WHERE e.stage = f.stage AND e.item_id = f.item_id
                           AND e.seq = f.seq)
                  FROM %1$s f
                 WHERE stage = ? AND item_id = ? AND seq = ?
                   FOR UPDATE""".formatted(table, events);
        String update = """
                UPDATE %s
                   SET state = ?, changed_at = clock_timestamp(),
                       changed_by = ?, reason_enc = ?, reason_key = ?,
                       reason_kek = ?,
                       resolved_by = COALESCE(?, resolved_by),
                       resolved_at = CASE WHEN ? THEN clock_timestamp()
                                          ELSE resolved_at END,
                       resolved_version = COALESCE(?, resolved_version)
                 WHERE stage = ? AND item_id = ? AND seq = ?"""
                .formatted(table);
        String insert = """
                INSERT INTO %s (stage, item_id, seq, event, at, operator,
                       from_state, to_state, reason_enc, reason_key,
                       reason_kek)
                VALUES (?, ?, ?, ?, clock_timestamp(), ?, ?, ?, ?, ?, ?)"""
                .formatted(events);
        Stored before;
        int event;
        try (PreparedStatement select = connection.prepareStatement(lock))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            select.setObject(2, itemId);
            select.setLong(3, seq);
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    throw new RefusedException("The " + stage
                            + " stage has no failure " + seq
                            + " for this item");
                }
                before = new Stored(new Failure(itemId, seq,
                        rows.getString(1), rows.getString(2),
                        rows.getLong(3), rows.getString(4)),
                        State.valueOf(rows.getString(5)));
                event = rows.getInt(6) + 1;
            }
        }
        if (!from.contains(before.state()))
        {
            throw new RefusedException("The failure is " + before.state()
                    + ": it can be " + action + " only from " + from);
        }
        FailureReason sealed = reason == null ? null : reason.seal(event);
        try (PreparedStatement set = connection.prepareStatement(update))
        {
            set.setQueryTimeout(timeout);
            set.setString(1, to.name());
            set.setString(2, operator);
            set.setBytes(3, sealed == null ? null : sealed.sealed());
            set.setBytes(4, sealed == null ? null : sealed.key());
            set.setString(5, sealed == null ? null : sealed.keyRef());
            set.setString(6, resolution == null ? null
                    : resolution.resolvedBy());
            set.setBoolean(7, resolution != null);
            set.setString(8, resolution == null ? null
                    : resolution.version());
            set.setString(9, stage);
            set.setObject(10, itemId);
            set.setLong(11, seq);
            set.executeUpdate();
        }
        try (PreparedStatement add = connection.prepareStatement(insert))
        {
            add.setQueryTimeout(timeout);
            add.setString(1, stage);
            add.setObject(2, itemId);
            add.setLong(3, seq);
            add.setInt(4, event);
            add.setString(5, operator);
            add.setString(6, before.state().name());
            add.setString(7, to.name());
            add.setBytes(8, sealed == null ? null : sealed.sealed());
            add.setBytes(9, sealed == null ? null : sealed.key());
            add.setString(10, sealed == null ? null : sealed.keyRef());
            add.executeUpdate();
        }
        return new Changed(before, event);
    }

    /**
     * The events' table in a schema.
     *
     * @param schema the schema
     * @return {@code <schema>.stage_failure_event}
     */
    public static String eventsTable(String schema)
    {
        table(schema);
        return schema + ".stage_failure_event";
    }

    /**
     * A failure's events, oldest first: each change of its state.
     *
     * @param itemId the item
     * @param seq    its record number
     * @return the events
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Event> events(UUID itemId, long seq)
    {
        String sql = """
                SELECT event, at, operator, from_state, to_state, reason_enc,
                       reason_key, reason_kek
                  FROM %s
                 WHERE stage = ? AND item_id = ? AND seq = ?
                 ORDER BY event""".formatted(events);
        List<Event> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            select.setObject(2, itemId);
            select.setLong(3, seq);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    byte[] sealed = rows.getBytes(6);
                    found.add(new Event(rows.getInt(1),
                            rows.getTimestamp(2).toInstant(),
                            rows.getString(3),
                            State.valueOf(rows.getString(4)),
                            State.valueOf(rows.getString(5)),
                            sealed == null ? null : new FailureReason(sealed,
                                    rows.getBytes(7), rows.getString(8))));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Fail store unavailable",
                    failure);
        }
    }

    /**
     * A change of a failure's state.
     *
     * @param event    its number for the failure, from 1
     * @param at       when
     * @param operator who made it
     * @param from     the state before
     * @param to       the state after
     * @param reason   why, encrypted; null if none was given
     */
    public record Event(int event, Instant at, String operator, State from,
            State to, FailureReason reason)
    {
    }

    /**
     * The stage's failures, open ones only or all, newest first.
     *
     * @param openOnly whether to list only open ones
     * @param limit    the most listed
     * @return the failures, with when each failed
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Listed> list(boolean openOnly, int limit)
    {
        String sql = """
                SELECT item_id, seq, record_id, error_code, source_offset,
                       failed_version, state, failed_at, changed_at,
                       changed_by
                  FROM %s
                 WHERE stage = ?%s
                 ORDER BY failed_at DESC, item_id, seq
                 LIMIT ?""".formatted(table, openOnly
                        ? " AND state IN ('FAILED', 'CORRECTION_PENDING',"
                                + " 'FAILED_CORRECTION')" : "");
        List<Listed> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            select.setInt(2, limit);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.add(listed(rows));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Fail store unavailable",
                    failure);
        }
    }

    /**
     * An item's failures, in sequence order, with when each failed and was
     * last changed.
     *
     * @param itemId the item
     * @return the failures
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Listed> listItem(UUID itemId)
    {
        String sql = """
                SELECT item_id, seq, record_id, error_code, source_offset,
                       failed_version, state, failed_at, changed_at,
                       changed_by
                  FROM %s
                 WHERE stage = ? AND item_id = ?
                 ORDER BY seq""".formatted(table);
        List<Listed> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            select.setObject(2, itemId);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.add(listed(rows));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Fail store unavailable",
                    failure);
        }
    }

    /**
     * Reads a listed failure.
     *
     * @param rows the rows, on one
     * @return the failure
     * @throws SQLException if it can't be read
     */
    private static Listed listed(ResultSet rows) throws SQLException
    {
        return new Listed(new Stored(new Failure(
                rows.getObject(1, UUID.class), rows.getLong(2),
                rows.getString(3), rows.getString(4), rows.getLong(5),
                rows.getString(6)), State.valueOf(rows.getString(7))),
                rows.getTimestamp(8).toInstant(),
                rows.getTimestamp(9).toInstant(), rows.getString(10));
    }

    /**
     * A failure as listed for an operator. It holds no content.
     *
     * @param stored    the failure and its state
     * @param failedAt  when it failed
     * @param changedAt when its state last changed
     * @param changedBy who changed it last; null if no operator has
     */
    public record Listed(Stored stored, Instant failedAt, Instant changedAt,
            String changedBy)
    {
    }

    /**
     * Records failures, each once, and audits each new one,
     * {@code failure.failed}.
     *
     * @param failures the failures
     * @return those not recorded before
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Failure> record(List<Failure> failures)
    {
        if (failures.isEmpty())
        {
            return List.of();
        }
        try (Connection connection = dataSource.getConnection())
        {
            List<Failure> added = insert(connection, table, stage, failures,
                    queryTimeoutSeconds);
            audit(stage, added);
            return added;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Fail store unavailable",
                    failure);
        }
    }

    /**
     * Records failures on a connection, as part of the caller's
     * transaction; for a stage that marks its records stored in the same
     * one, so a record is never stored failed without its failure. The
     * caller audits those added, once committed, with {@link #audit}.
     *
     * @param connection          the connection
     * @param table               the table, with its schema
     * @param stage               the stage
     * @param failures            the failures
     * @param queryTimeoutSeconds how long the statement may run
     * @return those not recorded before
     * @throws SQLException if the statement fails
     */
    public static List<Failure> insert(Connection connection, String table,
            String stage, List<Failure> failures, int queryTimeoutSeconds)
            throws SQLException
    {
        String sql = """
                INSERT INTO %s (stage, item_id, seq, record_id, error_code,
                       source_offset, failed_version, failed_at, state,
                       changed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, clock_timestamp(), 'FAILED',
                        clock_timestamp())
                ON CONFLICT (stage, item_id, seq) DO NOTHING"""
                .formatted(table);
        List<Failure> added = new ArrayList<>();
        try (PreparedStatement insert = connection.prepareStatement(sql))
        {
            insert.setQueryTimeout(queryTimeoutSeconds);
            for (Failure failure : failures)
            {
                insert.setString(1, stage);
                insert.setObject(2, failure.itemId());
                insert.setLong(3, failure.seq());
                insert.setString(4, failure.recordId());
                insert.setString(5, failure.errorCode());
                insert.setLong(6, failure.sourceOffset());
                insert.setString(7, failure.version());
                if (insert.executeUpdate() == 1)
                {
                    added.add(failure);
                }
            }
        }
        return added;
    }

    /**
     * Audits failures newly recorded, {@code failure.failed}, and counts
     * them. Names each by its item and sequence number, never its record
     * id, and holds no content.
     *
     * @param stage    the stage
     * @param failures the failures
     */
    public static void audit(String stage, List<Failure> failures)
    {
        Telemetry telemetry = Telemetry.get();
        for (Failure failure : failures)
        {
            Fields fields = Fields.of(Attr.STAGE, stage)
                    .with(Attr.ITEM_ID, failure.itemId())
                    .with(Attr.SEQ, failure.seq())
                    .with(Attr.ERROR_CODE, failure.errorCode())
                    .with(Attr.OFFSET, failure.sourceOffset());
            if (failure.version() != null)
            {
                fields = fields.with(Attr.VERSION, failure.version());
            }
            telemetry.auditWarn("failure.failed", fields);
            telemetry.failureRecorded(stage);
        }
    }

    /**
     * An item's failures, counted.
     *
     * @param itemId the item
     * @return the counts; {@link Counts#NONE} if it has none
     * @throws IllegalStateException if the database can't be reached
     */
    public Counts counts(UUID itemId)
    {
        try (Connection connection = dataSource.getConnection())
        {
            return counts(connection, table, stage, itemId,
                    queryTimeoutSeconds);
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Fail store unavailable",
                    failure);
        }
    }

    /**
     * An item's failures, counted, on a connection.
     *
     * @param connection          the connection
     * @param table               the table, with its schema
     * @param stage               the stage
     * @param itemId              the item
     * @param queryTimeoutSeconds how long the statement may run
     * @return the counts
     * @throws SQLException if the statement fails
     */
    public static Counts counts(Connection connection, String table,
            String stage, UUID itemId, int queryTimeoutSeconds)
            throws SQLException
    {
        String sql = """
                SELECT count(*) FILTER (WHERE state IN
                           ('FAILED', 'CORRECTION_PENDING',
                            'FAILED_CORRECTION')),
                       count(*) FILTER (WHERE state = 'DECLINED'),
                       count(*) FILTER (WHERE state IN
                           ('CORRECTED', 'RETRIED', 'SUPERSEDED'))
                  FROM %s
                 WHERE stage = ? AND item_id = ?""".formatted(table);
        try (PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            select.setObject(2, itemId);
            try (ResultSet rows = select.executeQuery())
            {
                rows.next();
                return new Counts(rows.getLong(1), rows.getLong(2),
                        rows.getLong(3));
            }
        }
    }

    /**
     * The stage's open failures, for {@code sdh.stage.failures_open} and
     * the age of the oldest (§15.6).
     *
     * @return how many are open, and the oldest
     * @throws IllegalStateException if the database can't be reached
     */
    public Open open()
    {
        String sql = """
                SELECT count(*), min(failed_at)
                  FROM %s
                 WHERE stage = ? AND state IN
                       ('FAILED', 'CORRECTION_PENDING', 'FAILED_CORRECTION')"""
                .formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            try (ResultSet rows = select.executeQuery())
            {
                rows.next();
                java.sql.Timestamp oldest = rows.getTimestamp(2);
                return new Open(rows.getLong(1),
                        oldest == null ? null : oldest.toInstant());
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Fail store unavailable",
                    failure);
        }
    }

    /**
     * Adds the stage's gauges of its open failures (§15.6):
     * {@code sdh.stage.failures_open} and
     * {@code sdh.stage.failures_oldest_age}, in seconds, for alerting on a
     * failure no one has acted on. The store is read at most once a
     * {@code refresh}; while it can't be read, the last values stay.
     *
     * @param refresh how long a reading is kept
     */
    public void gauges(Duration refresh)
    {
        OpenReading reading = new OpenReading(this, refresh);
        Fields fields = Fields.of(Attr.STAGE, stage);
        Telemetry telemetry = Telemetry.get();
        telemetry.gauge("sdh.stage.failures_open", "{failure}",
                "Failures neither passed nor declined, by stage", fields,
                () -> reading.get().count());
        telemetry.gauge("sdh.stage.failures_oldest_age", "s",
                "How long the oldest open failure has been open, by stage",
                fields, () ->
                {
                    Instant oldest = reading.get().oldest();
                    return oldest == null ? 0 : Math.max(0,
                            Duration.between(oldest, Instant.now())
                                    .toSeconds());
                });
    }

    /**
     * The open failures, read at most once a refresh.
     */
    private static final class OpenReading
    {

        /** The store. */
        private final FailureStore store;

        /** How long a reading is kept, in nanoseconds. */
        private final long refreshNanos;

        /** The last reading. */
        private Open last = new Open(0, null);

        /** When it was read; 0 before the first. */
        private long readAt;

        /**
         * Creates the reading.
         *
         * @param store   the store
         * @param refresh how long a reading is kept
         */
        OpenReading(FailureStore store, Duration refresh)
        {
            this.store = store;
            this.refreshNanos = refresh.toNanos();
        }

        /**
         * The reading, read again if it is old.
         *
         * @return the open failures
         */
        synchronized Open get()
        {
            long now = System.nanoTime();
            if (readAt == 0 || now - readAt >= refreshNanos)
            {
                readAt = now;
                try
                {
                    last = store.open();
                }
                catch (IllegalStateException unavailable)
                {
                    // Kept: the database is down, which its own telemetry
                    // reports.
                }
            }
            return last;
        }
    }

    /**
     * An item's failures, in sequence order, for the operator commands.
     *
     * @param itemId the item
     * @return each failure and its state
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Stored> failures(UUID itemId)
    {
        String sql = """
                SELECT seq, record_id, error_code, source_offset,
                       failed_version, state
                  FROM %s
                 WHERE stage = ? AND item_id = ?
                 ORDER BY seq""".formatted(table);
        List<Stored> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            select.setObject(2, itemId);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.add(new Stored(new Failure(itemId, rows.getLong(1),
                            rows.getString(2), rows.getString(3),
                            rows.getLong(4), rows.getString(5)),
                            State.valueOf(rows.getString(6))));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Fail store unavailable",
                    failure);
        }
    }

    /**
     * A failure as stored, with its state.
     *
     * @param failure the failure
     * @param state   its state
     */
    public record Stored(Failure failure, State state)
    {
    }

    /**
     * One failure, by its item and sequence number.
     *
     * @param itemId the item
     * @param seq    its sequence number
     * @return it and its state, if recorded
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<Stored> failure(UUID itemId, long seq)
    {
        return failures(itemId).stream()
                .filter(stored -> stored.failure().seq() == seq)
                .findFirst();
    }
}
