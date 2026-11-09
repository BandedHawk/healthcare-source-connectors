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
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * A stage's corrections (STAGES.md §15.4, §15.6c): an operator's edit of
 * what the stage failed, kept encrypted in {@code <schema>.stage_correction}
 * until one of the stage's workers processes it, as any input, and then
 * kept as the record of what was submitted. Submitting one moves its
 * failure to {@code CORRECTION_PENDING} in the same transaction, so only
 * one is in flight per failure. The content is encrypted with a data key of
 * its own, wrapped with the stage's records KEK, and bound to its item,
 * record number and correction number.
 * <p>
 * A retry (STAGES.md §15.6e) is a correction without an edit: what failed,
 * submitted unchanged, of kind {@link Kind#RETRY}. It is processed as a
 * correction; if it passes its failure is {@code RETRIED}, and if it fails
 * its failure returns to {@code FAILED}.
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
        /** Processed, and failed: its failure is {@code FAILED_CORRECTION}. */
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
            FailureStore.State.FAILED, FailureStore.State.FAILED_CORRECTION);

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
     * The statements that make the table.
     *
     * @param table the table, with its schema
     * @return the statements, in order
     */
    public static List<String> ddls(String table)
    {
        return List.of("""
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
                        + " ADD COLUMN IF NOT EXISTS kind text");
    }

    /**
     * Creates the table if it isn't there, for a stage without a stage
     * registry (the split stage).
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
    }

    /**
     * Notes a correction's outcome in a transaction of its own, and audits
     * it: for a stage without a stage registry (the split stage), once its
     * output is committed and sent.
     *
     * @param outcome what it came to
     * @throws FailureStore.RefusedException if the failure is no longer
     *                                       pending a correction
     * @throws IllegalStateException         if the database can't be
     *                                       reached
     */
    public void finish(Outcome outcome)
    {
        FailureStore.Changed changed;
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                changed = finish(connection, schema(), stage, outcome,
                        timeout);
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
        audit(stage, outcome, changed);
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
     */
    public record Pending(UUID itemId, long seq, int correction,
            String submittedBy, Instant submittedAt, byte[] sealed,
            byte[] key, String keyRef, long length, Kind kind)
    {

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
         * The name its output is claimed under in the stage's registry, in
         * place of a record ID: one per correction.
         *
         * @return {@code correction:<item>:<seq>:<n>}
         */
        public String claimId()
        {
            return "correction:" + itemId + ":" + seq + ":" + correction;
        }
    }

    /**
     * Submits a correction: stores its content, encrypted, and moves its
     * failure to {@code CORRECTION_PENDING}, in one transaction. Audited
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
     * Submits a correction or a retry: stores its content, encrypted, and
     * moves its failure to {@code CORRECTION_PENDING}, in one transaction.
     * Audited {@code failure.correction_submitted} or
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
                       content_kek, content_length, kind)
                VALUES (?, ?, ?, ?, 'PENDING', clock_timestamp(), ?, ?, ?,
                        ?, ?, ?)""".formatted(table);
        int[] number = new int[1];
        FailureStore.Changed changed;
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                changed = FailureStore.changeIn(connection, failures, events,
                        stage, itemId, seq, CORRECTABLE,
                        FailureStore.State.CORRECTION_PENDING, kind.action(),
                        operator, null, null, timeout);
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
                FailureStore.State.CORRECTION_PENDING, kind == Kind.RETRY
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
                       kind
                  FROM %s
                 WHERE stage = ? AND state = 'PENDING'
                 ORDER BY submitted_at
                 LIMIT ?""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(timeout);
            select.setString(1, stage);
            select.setInt(2, limit);
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
                            Kind.of(rows.getString(10))));
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
         * The state its failure moves to: for a retry, {@code RETRIED}, or
         * back to {@code FAILED}, as nothing was edited (STAGES.md §15.6e).
         *
         * @return {@code CORRECTED}, {@code RETRIED},
         *         {@code FAILED_CORRECTION} or {@code FAILED}
         */
        public FailureStore.State failureState()
        {
            if (pending.kind() == Kind.RETRY)
            {
                return passed ? FailureStore.State.RETRIED
                        : FailureStore.State.FAILED;
            }
            return passed ? FailureStore.State.CORRECTED
                    : FailureStore.State.FAILED_CORRECTION;
        }

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
     * Notes a correction's outcome within the stage's transaction: the
     * correction {@code PASSED} or {@code FAILED}, and its failure
     * {@code CORRECTED} (naming the record that passed) or
     * {@code FAILED_CORRECTION}. Not audited: the caller audits once it
     * commits ({@link #audit}).
     *
     * @param connection the transaction's connection
     * @param schema     the stage's registry schema
     * @param stage      the stage
     * @param outcome    what it came to
     * @param timeout    how long a statement may run, in seconds
     * @return the failure as it was
     * @throws FailureStore.RefusedException if the failure is no longer
     *                                       pending a correction
     * @throws SQLException                  if a statement fails
     */
    public static FailureStore.Changed finish(Connection connection,
            String schema, String stage, Outcome outcome, int timeout)
            throws SQLException
    {
        Pending pending = outcome.pending();
        FailureStore.Changed changed = FailureStore.changeIn(connection,
                FailureStore.table(schema), FailureStore.eventsTable(schema),
                stage, pending.itemId(), pending.seq(),
                EnumSet.of(FailureStore.State.CORRECTION_PENDING),
                outcome.failureState(), pending.kind().action(),
                pending.submittedBy(),
                null, outcome.passed()
                        ? new FailureStore.Resolution(outcome.recordId(),
                                outcome.version())
                        : null, timeout);
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
            update.executeUpdate();
        }
        return changed;
    }

    /**
     * Audits a correction's outcome, once noted: {@code failure.corrected},
     * {@code failure.correction_failed}, {@code failure.retried} or
     * {@code failure.retry_failed}, with the stage, item, record number,
     * states and operator; no content.
     *
     * @param stage   the stage
     * @param outcome what it came to
     * @param changed the failure as it was
     */
    public static void audit(String stage, Outcome outcome,
            FailureStore.Changed changed)
    {
        Pending pending = outcome.pending();
        FailureStore.auditChange(stage, pending.itemId(), pending.seq(),
                outcome.failureState(), outcome.action(),
                pending.submittedBy(), changed);
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
                       content_length, kind
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
                            rows.getLong(8), Kind.of(rows.getString(9))));
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
     */
    public record Listed(int correction, State state, String submittedBy,
            Instant submittedAt, Instant finishedAt, String recordId,
            String errorCode, long length, Kind kind)
    {
    }
}
