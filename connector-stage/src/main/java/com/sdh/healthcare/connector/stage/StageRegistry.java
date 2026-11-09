/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * A stage's registry in PostgreSQL (STAGES.md §5–§6): a row per record,
 * which says who has claimed it and whether it is stored and sent, and a
 * row per item, which counts its records. Every expiry check uses the
 * database clock, so clock skew between pods can't make two owners.
 * <p>
 * A record moves from {@code CLAIMED} to {@code STORED}, with the sealed
 * message it is to be sent as, and then to {@code SENT}, when the message
 * is dropped. A record stored but not yet sent can be sent again by
 * whichever worker receives it next, so a crash between the commit and
 * the send loses nothing.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StageRegistry
{

    /** A schema name. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[a-z_][a-z0-9_]{0,62}");

    /** Connections to the database. */
    private final DataSource dataSource;

    /** The stage whose rows these are. */
    private final String stage;

    /** This worker's name in the claims. */
    private final String owner;

    /** The record table, with its schema. */
    private final String records;

    /** The item table, with its schema. */
    private final String items;

    /** The reconciler's watermark table, with its schema. */
    private final String reconcile;

    /**
     * When a record older than the reconciler's grace last came off the
     * queue, and since when the reconciler has waited for that to stop,
     * with its schema.
     */
    private final String backlog;

    /** The stage's fail store's states (STAGES.md §15.3), with its schema. */
    private final String failures;

    /** How long a statement may run. */
    private final int queryTimeoutSeconds;

    /**
     * What a worker got when it claimed a record.
     */
    public sealed interface Claim
    {
    }

    /**
     * The record is this worker's to process.
     *
     * @param attempts how many workers have claimed it, this one included
     */
    public record Owned(int attempts) implements Claim
    {
    }

    /**
     * The record was stored already.
     *
     * @param digest   the digest of the output stored
     * @param version  the version it was made with (STAGES.md §6); null
     *                 for a row from before versions were kept
     * @param attempts how many workers had claimed it when it was stored
     * @param pending  the message it is to be sent as, sealed, if it
     *                 hasn't been sent; null once it has
     * @param mine     whether this worker stored it, as when a batch is
     *                 tried again after its send failed
     */
    public record Stored(String digest, String version, int attempts,
            OutboundMessage pending, boolean mine)
            implements Claim
    {

        /**
         * The row's state, as telemetry names it.
         *
         * @return {@code STORED} while its message is still to be sent,
         *         {@code SENT} after
         */
        public String state()
        {
            return pending == null ? "SENT" : "STORED";
        }
    }

    /** Another worker holds a live claim on the record. */
    public record Busy() implements Claim
    {
    }

    /**
     * What a record is known by: its item and its name in the registry. A
     * record id comes from the content alone (design §7.3), so the same
     * content in two items is a record of each, counted and stored for
     * both (decided 2026-11-05). The name is the record id and the suffix
     * of the lineage this stage makes it under ({@link Lineage#suffix},
     * STAGES.md §15.8.2), so the same content in two generations, or in
     * two revisions, is an object of each; for the first of everything it
     * is the record id alone, as before lineage was kept.
     *
     * @param itemId   the id of the record's item
     * @param recordId the record's name: its id, and its lineage's suffix
     */
    public record Key(UUID itemId, String recordId)
    {

        /**
         * The key of a record made under a lineage.
         *
         * @param itemId   the record's item
         * @param recordId its id
         * @param lineage  the lineage this stage makes it under
         * @return the key
         */
        public static Key of(UUID itemId, String recordId, Lineage lineage)
        {
            return new Key(itemId, recordId + lineage.suffix());
        }
    }

    /**
     * A record to mark stored.
     *
     * @param recordId the record's id
     * @param itemId   the id of its item
     * @param digest   its output's digest
     * @param version  the version its output was made with
     * @param failed   whether it failed, and was stored as it was
     * @param message  the message it is to be sent as, sealed; null for a
     *                 failed record, which isn't sent on (STAGES.md §15.1)
     * @param failure  the failure to record in the fail store, in the same
     *                 transaction; null if it passed
     * @param correction what an operator's correction came to, for an
     *                 entry that is one (STAGES.md §15.6c): noted in the
     *                 same transaction, its failure resolved or failed
     *                 again, and not counted on its item, whose count of
     *                 records is unchanged; null for a record
     * @param seq      its place in its item's generation, from 0; -1 if it
     *                 has none, when it is never weighed against another
     *                 at its place
     * @param lineage  the lineage the stage made it under (STAGES.md
     *                 §15.8.2): with the place, what decides whether it is
     *                 current (§15.8.3)
     */
    public record Entry(String recordId, UUID itemId, String digest,
            String version, boolean failed, OutboundMessage message,
            FailureStore.Failure failure, CorrectionStore.Outcome correction,
            long seq, Lineage lineage)
    {

        /**
         * Reads no lineage as the first.
         */
        public Entry
        {
            lineage = Lineage.orFirst(lineage);
        }

        /**
         * An entry with no place, of the first lineage: a correction, or a
         * record never weighed against another.
         *
         * @param recordId   the record's id
         * @param itemId     the id of its item
         * @param digest     its output's digest
         * @param version    the version its output was made with
         * @param failed     whether it failed
         * @param message    the message it is to be sent as, sealed
         * @param failure    the failure to record, or null
         * @param correction what a correction came to, or null
         */
        public Entry(String recordId, UUID itemId, String digest,
                String version, boolean failed, OutboundMessage message,
                FailureStore.Failure failure,
                CorrectionStore.Outcome correction)
        {
            this(recordId, itemId, digest, version, failed, message, failure,
                    correction, -1, null);
        }

        /**
         * A record, not a correction.
         *
         * @param recordId the record's id
         * @param itemId   the id of its item
         * @param digest   its output's digest
         * @param version  the version its output was made with
         * @param failed   whether it failed
         * @param message  the message it is to be sent as, sealed
         * @param failure  the failure to record, or null
         */
        public Entry(String recordId, UUID itemId, String digest,
                String version, boolean failed, OutboundMessage message,
                FailureStore.Failure failure)
        {
            this(recordId, itemId, digest, version, failed, message, failure,
                    null);
        }

        /**
         * A record with no failure to record.
         *
         * @param recordId the record's id
         * @param itemId   the id of its item
         * @param digest   its output's digest
         * @param version  the version its output was made with
         * @param failed   whether it failed
         * @param message  the message it is to be sent as, sealed
         */
        public Entry(String recordId, UUID itemId, String digest,
                String version, boolean failed, OutboundMessage message)
        {
            this(recordId, itemId, digest, version, failed, message, null,
                    null);
        }

        /**
         * What the record is known by.
         *
         * @return its key
         */
        public Key key()
        {
            return Key.of(itemId, recordId, lineage);
        }
    }

    /**
     * Creates a registry.
     *
     * @param dataSource   connections to the database
     * @param schema       the schema its tables are in
     * @param stage        the stage whose rows these are
     * @param owner        this worker's name in the claims, unique to it
     * @param queryTimeout how long a statement may run; at least a second
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier
     */
    public StageRegistry(DataSource dataSource, String schema, String stage,
            String owner, Duration queryTimeout)
    {
        if (!IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException("Invalid registry schema name");
        }
        this.dataSource = dataSource;
        this.stage = stage;
        this.owner = owner;
        this.records = schema + ".stage_record";
        this.items = schema + ".stage_item";
        this.reconcile = schema + ".stage_reconcile";
        this.backlog = schema + ".stage_backlog";
        this.failures = FailureStore.table(schema);
        this.queryTimeoutSeconds =
                (int) Math.max(1, queryTimeout.toSeconds());
    }

    /**
     * This worker's name in the claims.
     *
     * @return the name
     */
    public String owner()
    {
        return owner;
    }

    /**
     * Creates the tables if they aren't there.
     *
     * @throws IllegalStateException if they can't be created
     */
    public void createTablesIfMissing()
    {
        String recordDdl = """
                CREATE TABLE IF NOT EXISTS %s (
                  stage           text        NOT NULL,
                  item_id         uuid        NOT NULL,
                  record_id       text        NOT NULL,
                  state           text        NOT NULL,
                  owner           text        NOT NULL,
                  attempts        integer     NOT NULL,
                  claim_expires   timestamptz,
                  output_digest   text,
                  output_version  text,
                  message_id      text,
                  message_type    text,
                  message_key     text,
                  pending_message bytea,
                  stored_at       timestamptz,
                  PRIMARY KEY (stage, item_id, record_id)
                )""".formatted(records);
        String itemDdl = """
                CREATE TABLE IF NOT EXISTS %s (
                  stage        text        NOT NULL,
                  item_id      uuid        NOT NULL,
                  expected     bigint,
                  total_message bytea,
                  done         bigint      NOT NULL DEFAULT 0,
                  failed       bigint      NOT NULL DEFAULT 0,
                  completed_at timestamptz,
                  PRIMARY KEY (stage, item_id)
                )""".formatted(items);
        // Added 2026-11-07 (STAGES.md §9): how far the reconciler has
        // checked, so an outage longer than its lookback is caught up on.
        String reconcileDdl = """
                CREATE TABLE IF NOT EXISTS %s (
                  stage        text        PRIMARY KEY,
                  checked_to   timestamptz NOT NULL,
                  updated_at   timestamptz NOT NULL
                )""".formatted(reconcile);
        // Added 2026-11-07 (STAGES.md §9): the reconciler waits while a
        // backlog is still coming off the queue. Times are the database's,
        // so the pods' clocks don't matter. seen_at is null until a backlog
        // is seen: the reconciler's wait at startup makes the row first.
        String backlogDdl = """
                CREATE TABLE IF NOT EXISTS %s (
                  stage           text        PRIMARY KEY,
                  seen_at         timestamptz,
                  deferring_since timestamptz
                )""".formatted(backlog);
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(queryTimeoutSeconds);
            statement.execute("CREATE SCHEMA IF NOT EXISTS "
                    + records.substring(0, records.indexOf('.')));
            statement.execute(recordDdl);
            // Kept since 2026-11-06 (STAGES.md §6); added to older tables.
            statement.execute("ALTER TABLE " + records
                    + " ADD COLUMN IF NOT EXISTS output_version text");
            statement.execute(itemDdl);
            statement.execute(reconcileDdl);
            statement.execute(backlogDdl);
            // Made NOT NULL by the first build of it, which then never
            // recorded a wait at startup with no backlog yet seen.
            statement.execute("ALTER TABLE " + backlog
                    + " ALTER COLUMN seen_at DROP NOT NULL");
            // Added 2026-11-08 (STAGES.md §15.6): a total from the stage
            // before's item.progress, which may grow, and the counts of the
            // last item.progress this stage sent.
            statement.execute("ALTER TABLE " + items
                    + " ADD COLUMN IF NOT EXISTS upstream_open boolean"
                    + " NOT NULL DEFAULT false");
            statement.execute("ALTER TABLE " + items
                    + " ADD COLUMN IF NOT EXISTS progress_key text");
            statement.execute("ALTER TABLE " + items
                    + " ADD COLUMN IF NOT EXISTS progress_at timestamptz");
            // Added 2026-11-09 (STAGES.md §15.6e): how many times this
            // stage completed the item, and the completion of the stage
            // before its total is from, so an item reopened upstream is
            // reopened here and a stale message changes nothing.
            statement.execute("ALTER TABLE " + items
                    + " ADD COLUMN IF NOT EXISTS completion integer"
                    + " NOT NULL DEFAULT 0");
            statement.execute("ALTER TABLE " + items
                    + " ADD COLUMN IF NOT EXISTS upstream_completion integer"
                    + " NOT NULL DEFAULT 0");
            // Added 2026-11-09 (STAGES.md §15.8): each record's place and
            // lineage, so the stage can tell which object at a place is
            // current, and whether one arrived after a newer one; and each
            // item's generation, whose records alone it counts. Rows from
            // before are the first of everything.
            statement.execute("ALTER TABLE " + records
                    + " ADD COLUMN IF NOT EXISTS seq bigint");
            statement.execute("ALTER TABLE " + records
                    + " ADD COLUMN IF NOT EXISTS generation integer"
                    + " NOT NULL DEFAULT 1");
            statement.execute("ALTER TABLE " + records
                    + " ADD COLUMN IF NOT EXISTS line integer[]"
                    + " NOT NULL DEFAULT '{}'");
            statement.execute("ALTER TABLE " + records
                    + " ADD COLUMN IF NOT EXISTS late boolean"
                    + " NOT NULL DEFAULT false");
            statement.execute("ALTER TABLE " + records
                    + " ADD COLUMN IF NOT EXISTS superseded_at timestamptz");
            statement.execute("CREATE INDEX IF NOT EXISTS stage_record_place"
                    + " ON " + records + " (stage, item_id, generation, seq)");
            // Added 2026-11-09 (§15.8.5): the correction a row was made
            // from, for one claimed under its place's key.
            statement.execute("ALTER TABLE " + records
                    + " ADD COLUMN IF NOT EXISTS correction integer");
            statement.execute("ALTER TABLE " + items
                    + " ADD COLUMN IF NOT EXISTS generation integer"
                    + " NOT NULL DEFAULT 1");
            numberEarlierCompletions(statement);
            for (String ddl : FailureStore.ddls(failures,
                    failures.substring(0, failures.indexOf('.'))
                            + ".stage_failure_event"))
            {
                statement.execute(ddl);
            }
            // Added 2026-11-08 (STAGES.md §15.6c): operators' corrections.
            for (String ddl : CorrectionStore.ddls(
                    CorrectionStore.table(schema())))
            {
                statement.execute(ddl);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the stage registry", failure);
        }
        moveFailureStates();
    }

    /**
     * Moves the fail store's failures out of the states that went
     * (STAGES.md §15.8.5; {@link CorrectionStore#moveStates}), and counts
     * each failure added for a failed correction among its item's failed
     * records, as a failed revision is counted, in one transaction.
     * Changes nothing once done.
     *
     * @throws IllegalStateException if it can't
     */
    private void moveFailureStates()
    {
        CorrectionStore.Moved moved;
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                moved = CorrectionStore.moveStates(connection, schema(),
                        stage, queryTimeoutSeconds);
                try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE " + items + " SET failed = failed + ?"
                                + " WHERE stage = ? AND item_id = ?"))
                {
                    update.setQueryTimeout(queryTimeoutSeconds);
                    for (Map.Entry<UUID, Long> item
                            : moved.refailed().entrySet())
                    {
                        update.setLong(1, item.getValue());
                        update.setString(2, stage);
                        update.setObject(3, item.getKey());
                        update.executeUpdate();
                    }
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
            throw new IllegalStateException(
                    "Could not move the fail store's states", failure);
        }
        moved.audit(stage);
    }

    /**
     * Numbers the completions of rows written before completions were
     * (STAGES.md §15.6f): an item completed here, or a complete total taken
     * from the stage before, was its first completion, so it is numbered 1,
     * as this build would have numbered it. Without it, reopening such an
     * item would send messages under IDs already sent, which the stages
     * after drop as repeats. Nothing this build writes matches: it numbers
     * a completion, and a complete total, as it stores them. Runs at every
     * start, and changes nothing once done.
     *
     * @param statement where to run it
     * @throws SQLException if it can't
     */
    private void numberEarlierCompletions(Statement statement)
            throws SQLException
    {
        Connection connection = statement.getConnection();
        number(connection, "stage.completions_numbered", """
                UPDATE %s SET completion = 1
                 WHERE stage = ? AND completion = 0
                   AND completed_at IS NOT NULL""".formatted(items));
        number(connection, "stage.totals_numbered", """
                UPDATE %s SET upstream_completion = 1
                 WHERE stage = ? AND upstream_completion = 0
                   AND expected IS NOT NULL AND NOT upstream_open"""
                .formatted(items));
    }

    /**
     * Runs one of {@link #numberEarlierCompletions}'s updates, and audits
     * how many rows it numbered, if any.
     *
     * @param connection where to run it
     * @param event      the audit event's name
     * @param sql        the update, taking the stage
     * @throws SQLException if it can't
     */
    private void number(Connection connection, String event, String sql)
            throws SQLException
    {
        try (PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setString(1, stage);
            int numbered = update.executeUpdate();
            if (numbered > 0)
            {
                Telemetry.get().audit(event, Fields.of(Attr.STAGE, stage)
                        .with(Attr.COUNT, numbered));
            }
        }
    }

    /**
     * Claims a record: a new claim, this worker's own again, or one whose
     * holder let it expire, taken over.
     *
     * @param recordId the record's id
     * @param itemId   the id of its item
     * @param ttl      how long the claim lasts unless renewed
     * @return what this worker got
     * @throws IllegalStateException if the database can't be reached
     */
    public Claim claim(String recordId, UUID itemId, Duration ttl)
    {
        // A claim is taken over only once it has expired; a taken-over
        // claim counts one more attempt, a worker's own does not. ON
        // CONFLICT checks the WHERE clause again under a row lock, so two
        // workers at once can't both win.
        String sql = """
                INSERT INTO %1$s AS r
                       (stage, item_id, record_id, state, owner, attempts,
                        claim_expires)
                VALUES (?, ?, ?, 'CLAIMED', ?, 1,
                        clock_timestamp() + ? * interval '1 millisecond')
                ON CONFLICT (stage, item_id, record_id) DO UPDATE
                   SET owner = EXCLUDED.owner,
                       attempts = r.attempts
                           + CASE WHEN r.owner = EXCLUDED.owner
                                  THEN 0 ELSE 1 END,
                       claim_expires = EXCLUDED.claim_expires
                 WHERE r.state = 'CLAIMED'
                   AND (r.owner = EXCLUDED.owner
                        OR r.claim_expires <= clock_timestamp())
                RETURNING attempts""".formatted(records);
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(true);
            try (PreparedStatement insert = connection.prepareStatement(sql))
            {
                insert.setQueryTimeout(queryTimeoutSeconds);
                insert.setString(1, stage);
                insert.setObject(2, itemId);
                insert.setString(3, recordId);
                insert.setString(4, owner);
                insert.setLong(5, ttl.toMillis());
                try (ResultSet rows = insert.executeQuery())
                {
                    if (rows.next())
                    {
                        return new Owned(rows.getInt(1));
                    }
                }
            }
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT state, output_digest, owner, message_id,"
                            + " message_type, message_key, pending_message,"
                            + " output_version, attempts FROM " + records
                            + " WHERE stage = ? AND item_id = ?"
                            + " AND record_id = ?"))
            {
                select.setQueryTimeout(queryTimeoutSeconds);
                select.setString(1, stage);
                select.setObject(2, itemId);
                select.setString(3, recordId);
                try (ResultSet rows = select.executeQuery())
                {
                    if (rows.next() && !"CLAIMED".equals(rows.getString(1)))
                    {
                        byte[] body = rows.getBytes(7);
                        return new Stored(rows.getString(2),
                                rows.getString(8), rows.getInt(9),
                                body == null ? null
                                        : new OutboundMessage(
                                                rows.getString(4),
                                                MessageType.fromWire(
                                                        rows.getString(5)),
                                                itemId, rows.getString(6),
                                                body),
                                owner.equals(rows.getString(3)));
                    }
                }
            }
            return new Busy();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * Renews every live claim this worker holds.
     *
     * @param ttl how long they last from now
     * @return how many were renewed
     * @throws IllegalStateException if the database can't be reached
     */
    public int renew(Duration ttl)
    {
        String sql = """
                UPDATE %s
                   SET claim_expires = clock_timestamp()
                                       + ? * interval '1 millisecond'
                 WHERE stage = ? AND owner = ? AND state = 'CLAIMED'"""
                .formatted(records);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setLong(1, ttl.toMillis());
            update.setString(2, stage);
            update.setString(3, owner);
            return update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * Ends every live claim this worker holds now, rather than when it
     * would expire: at shutdown, once nothing is being processed, so that
     * another worker can take its records over at once. The rows are kept:
     * a record taken over counts one more attempt, as after an expiry.
     *
     * @return how many were released
     * @throws IllegalStateException if the database can't be reached
     */
    public int release()
    {
        String sql = """
                UPDATE %s
                   SET claim_expires = clock_timestamp()
                 WHERE stage = ? AND owner = ? AND state = 'CLAIMED'
                   AND claim_expires > clock_timestamp()"""
                .formatted(records);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setString(1, stage);
            update.setString(2, owner);
            return update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * Marks records stored, each with the message it is to be sent as, and
     * counts them on their items, in one transaction. A failed record has
     * no message: it is marked {@code FAILED}, and its failure recorded in
     * the fail store in the same transaction, so a record is never stored
     * failed without it (STAGES.md §15.3); each new failure is audited,
     * {@code failure.failed}, once committed. A record whose claim this
     * worker no longer holds is left alone: its new holder stores and
     * counts it.
     * <p>
     * Each record is weighed against what the stage holds (STAGES.md
     * §15.8.3), oldest first, so a batch holding two of one place is
     * judged as if they came one after the other:
     * <ul>
     * <li>of a later generation than its item's here: the item moves to
     *     that generation, its counts start again, and the failures of the
     *     generations before are superseded;</li>
     * <li>of an earlier generation, or older than the object stored at its
     *     place: it is stored, marked late ({@code late}), not counted, and
     *     its failure, if it failed, is recorded superseded; audited
     *     {@code record.superseded_on_arrival};</li>
     * <li>newer than the object stored at its place: that object is
     *     superseded, and its failure, if it had one; the place isn't
     *     counted again, and this record's failure, if it failed, is
     *     counted as a new one.</li>
     * </ul>
     *
     * @param entries the records, all committed
     * @return the keys of those marked; the others' claims were lost
     * @throws IllegalStateException if the database can't be reached
     */
    public Set<Key> store(List<Entry> entries)
    {
        String mark = """
                UPDATE %s
                   SET state = ?, output_digest = ?,
                       output_version = ?,
                       message_id = ?, message_type = ?, message_key = ?,
                       pending_message = ?, stored_at = clock_timestamp(),
                       claim_expires = NULL, seq = ?, generation = ?,
                       line = ?, late = ?, correction = ?
                 WHERE stage = ? AND item_id = ? AND record_id = ?
                   AND owner = ? AND state = 'CLAIMED'""".formatted(records);
        // A record whose claim was lost changes nothing of its item's.
        String holds = """
                SELECT 1 FROM %s
                 WHERE stage = ? AND item_id = ? AND record_id = ?
                   AND owner = ? AND state = 'CLAIMED'
                   FOR UPDATE""".formatted(records);
        // Item rows are locked in one order, so two workers can't deadlock.
        List<Entry> ordered = new ArrayList<>(entries);
        ordered.sort(Comparator.comparing(Entry::itemId)
                .thenComparing(Entry::lineage));
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                Set<Key> marked = new HashSet<>();
                List<FailureStore.Failure> failed = new ArrayList<>();
                List<Entry> corrections = new ArrayList<>();
                Currency currency = new Currency(connection);
                try (PreparedStatement update =
                        connection.prepareStatement(mark);
                        PreparedStatement held =
                                connection.prepareStatement(holds))
                {
                    update.setQueryTimeout(queryTimeoutSeconds);
                    held.setQueryTimeout(queryTimeoutSeconds);
                    for (Entry entry : ordered)
                    {
                        if (!holds(held, entry))
                        {
                            continue;
                        }
                        Placement placed = currency.place(entry);
                        OutboundMessage message = entry.message();
                        update.setString(1,
                                message == null ? "FAILED" : "STORED");
                        update.setString(2, entry.digest());
                        update.setString(3, entry.version());
                        update.setString(4, message == null ? null
                                : message.messageId());
                        update.setString(5, message == null ? null
                                : message.type().wireName());
                        update.setString(6, message == null ? null
                                : message.key());
                        update.setBytes(7, message == null ? null
                                : message.body());
                        if (entry.seq() < 0)
                        {
                            update.setNull(8, java.sql.Types.BIGINT);
                        }
                        else
                        {
                            update.setLong(8, entry.seq());
                        }
                        update.setInt(9, entry.lineage().generation());
                        update.setArray(10, FailureStore.line(connection,
                                entry.lineage()));
                        update.setBoolean(11, placed.late());
                        if (entry.correction() == null)
                        {
                            update.setNull(12, java.sql.Types.INTEGER);
                        }
                        else
                        {
                            update.setInt(12, entry.correction().pending()
                                    .correction());
                        }
                        update.setString(13, stage);
                        update.setObject(14, entry.itemId());
                        update.setString(15, entry.key().recordId());
                        update.setString(16, owner);
                        if (update.executeUpdate() != 1)
                        {
                            continue;
                        }
                        marked.add(entry.key());
                        if (entry.correction() != null)
                        {
                            // A revision of its place (§15.8.5), counted
                            // as one; its correction is noted below.
                            corrections.add(entry);
                        }
                        currency.stored(entry, placed);
                        if (!placed.late() && entry.failure() != null)
                        {
                            failed.add(entry.failure());
                        }
                    }
                }
                currency.flush();
                List<FailureStore.Failure> added = FailureStore.insert(
                        connection, failures, stage, failed,
                        queryTimeoutSeconds);
                List<FailureStore.Failure> late = new ArrayList<>();
                for (Map.Entry<FailureStore.Failure, String> lateFailure
                        : currency.lateFailures.entrySet())
                {
                    late.addAll(FailureStore.insert(connection, failures,
                            stage, List.of(lateFailure.getKey()),
                            lateFailure.getValue(), queryTimeoutSeconds));
                }
                List<CorrectionStore.Finished> finished = new ArrayList<>();
                for (Entry entry : corrections)
                {
                    // Its failure, if it failed, is recorded above, with
                    // the batch's.
                    finished.add(CorrectionStore.finish(connection, schema(),
                            stage, entry.correction(), false,
                            queryTimeoutSeconds));
                }
                connection.commit();
                FailureStore.audit(stage, added);
                currency.audit(late);
                for (int index = 0; index < corrections.size(); index++)
                {
                    CorrectionStore.audit(stage,
                            corrections.get(index).correction(),
                            finished.get(index));
                }
                return marked;
            }
            catch (SQLException failure)
            {
                connection.rollback();
                throw failure;
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * Where a record stands against what the stage holds at its place
     * (STAGES.md §15.8.3).
     *
     * @param late       whether something newer was there first: it is
     *                   stored, not counted
     * @param supersedes the name of the older object it replaces at its
     *                   place, which is then superseded; null if none
     * @param older      that object's lineage; null if none
     * @param newer      for a late record, what was there first, as text;
     *                   null otherwise
     */
    private record Placement(boolean late, String supersedes, Lineage older,
            String newer)
    {

        /** The first at its place, in its item's current generation. */
        static final Placement NEW = new Placement(false, null, null, null);
    }

    /**
     * An item's row as one {@link #store} weighs records against: its
     * generation, and the counts to add, or to start again from.
     */
    private static final class ItemRow
    {

        /** The generation it had when read. */
        private final int readGeneration;

        /** Its records counted when read. */
        private final long readDone;

        /** Its failed records counted when read. */
        private final long readFailed;

        /** Whether the transaction made it. */
        private final boolean created;

        /** Its generation now. */
        private int generation;

        /** Whether its counts start again, for a new generation. */
        private boolean reset;

        /** Records to add to its count, or to start it from. */
        private long done;

        /** Failed records to add, or to start from. */
        private long failed;

        /**
         * An item's row as read.
         *
         * @param generation its generation
         * @param done       its records counted
         * @param failed     its failed records counted
         * @param created    whether the transaction made it
         */
        ItemRow(int generation, long done, long failed, boolean created)
        {
            this.created = created;
            this.readGeneration = generation;
            this.readDone = done;
            this.readFailed = failed;
            this.generation = generation;
        }
    }

    /**
     * Whether this registry still holds a record's claim, locking its row
     * until the transaction ends so the claim can't be lost meanwhile.
     *
     * @param held  the query, on the open transaction
     * @param entry the record
     * @return true if the claim is held
     * @throws SQLException if the query fails
     */
    private boolean holds(PreparedStatement held, Entry entry)
            throws SQLException
    {
        held.setString(1, stage);
        held.setObject(2, entry.itemId());
        held.setString(3, entry.key().recordId());
        held.setString(4, owner);
        try (ResultSet found = held.executeQuery())
        {
            return found.next();
        }
    }

    /**
     * Weighs one transaction's records against what the stage holds
     * (STAGES.md §15.8.3), keeps their items' counts, and gathers what to
     * audit once it commits.
     */
    private final class Currency
    {

        /** The transaction's connection. */
        private final Connection connection;

        /** The items' rows, locked, as read and changed so far. */
        private final Map<UUID, ItemRow> rows = new HashMap<>();

        /**
         * Failures of records superseded on arrival, with what was there
         * first, in order.
         */
        private final Map<FailureStore.Failure, String> lateFailures =
                new LinkedHashMap<>();

        /** Failures superseded, with how and by what, to audit. */
        private final List<Object[]> superseded = new ArrayList<>();

        /** Events to audit once committed, with their fields. */
        private final List<Object[]> events = new ArrayList<>();

        /**
         * Weighs records in a transaction.
         *
         * @param connection its connection
         */
        Currency(Connection connection)
        {
            this.connection = connection;
        }

        /**
         * Weighs a record against its item's generation and what is stored
         * at its place, moving the item to a later generation if the record
         * is of one.
         *
         * @param entry the record
         * @return where it stands
         * @throws SQLException if a statement fails
         */
        Placement place(Entry entry) throws SQLException
        {
            ItemRow item = item(entry.itemId(), entry.lineage().generation());
            int generation = entry.lineage().generation();
            if (generation < item.generation)
            {
                return new Placement(true, null, null, "g" + item.generation);
            }
            if (generation > item.generation)
            {
                newGeneration(entry.itemId(), item, generation);
            }
            if (entry.seq() < 0)
            {
                return Placement.NEW;
            }
            String sql = """
                    SELECT record_id, generation, line FROM %s
                     WHERE stage = ? AND item_id = ? AND generation = ?
                       AND seq = ? AND record_id <> ? AND state <> 'CLAIMED'
                       AND NOT late AND superseded_at IS NULL
                       FOR UPDATE""".formatted(records);
            try (PreparedStatement select = connection.prepareStatement(sql))
            {
                select.setQueryTimeout(queryTimeoutSeconds);
                select.setString(1, stage);
                select.setObject(2, entry.itemId());
                select.setInt(3, generation);
                select.setLong(4, entry.seq());
                select.setString(5, entry.key().recordId());
                try (ResultSet found = select.executeQuery())
                {
                    while (found.next())
                    {
                        Lineage there = FailureStore.lineage(found, 2);
                        int order = there.compareTo(entry.lineage());
                        if (order > 0)
                        {
                            return new Placement(true, null, null,
                                    there.toString());
                        }
                        if (order < 0)
                        {
                            return new Placement(false, found.getString(1),
                                    there, null);
                        }
                    }
                }
            }
            return Placement.NEW;
        }

        /**
         * Notes a record marked stored: counts it on its item unless it was
         * late or replaced another at its place, and supersedes what it
         * replaced.
         *
         * @param entry  the record
         * @param placed where it stood
         * @throws SQLException if a statement fails
         */
        void stored(Entry entry, Placement placed) throws SQLException
        {
            long seq = entry.seq();
            if (placed.late())
            {
                if (entry.failure() != null)
                {
                    lateFailures.put(withLineage(entry.failure(),
                            entry.lineage()), placed.newer());
                }
                events.add(new Object[] {"record.superseded_on_arrival",
                    Fields.of(Attr.STAGE, stage)
                            .with(Attr.ITEM_ID, entry.itemId())
                            .with(Attr.SEQ, seq < 0 ? null : seq)
                            .with(Attr.LINEAGE, entry.lineage().toString())
                            .with(Attr.SUPERSEDED_BY, placed.newer())});
                Telemetry.get().supersededOnArrival(stage);
                return;
            }
            ItemRow item = rows.get(entry.itemId());
            item.failed += entry.failed() ? 1 : 0;
            if (placed.supersedes() == null)
            {
                // A correction is a revision of a place already counted,
                // even where the row before it has no place noted, as one
                // stored before lineage was kept (STAGES.md §15.8.5).
                if (entry.correction() == null)
                {
                    item.done++;
                }
                return;
            }
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE " + records + " SET superseded_at"
                            + " = clock_timestamp() WHERE stage = ?"
                            + " AND item_id = ? AND record_id = ?"))
            {
                update.setQueryTimeout(queryTimeoutSeconds);
                update.setString(1, stage);
                update.setObject(2, entry.itemId());
                update.setString(3, placed.supersedes());
                update.executeUpdate();
            }
            String by = entry.lineage().toString();
            superseded.add(new Object[] {FailureStore.supersedeRevision(
                    connection, failures, stage, entry.itemId(), seq,
                    placed.older(), entry.lineage(), queryTimeoutSeconds),
                FailureStore.Superseded.REVISION, by});
        }

        /**
         * An item's row, read and locked once per transaction; made, at the
         * given generation, if the item has none yet.
         *
         * @param itemId     the item
         * @param generation the generation to make it at
         * @return the row
         * @throws SQLException if a statement fails
         */
        private ItemRow item(UUID itemId, int generation) throws SQLException
        {
            ItemRow known = rows.get(itemId);
            if (known != null)
            {
                return known;
            }
            ItemRow row = lockItem(connection, itemId, generation);
            rows.put(itemId, row);
            if (row.created && generation > 1)
            {
                startedGeneration(itemId, generation);
            }
            return row;
        }

        /**
         * Moves an item to a later generation: its counts start again, and
         * the failures of its generations before are superseded.
         *
         * @param itemId     the item
         * @param item       its row
         * @param generation the generation
         * @throws SQLException if a statement fails
         */
        private void newGeneration(UUID itemId, ItemRow item, int generation)
                throws SQLException
        {
            long done = item.reset ? item.done : item.readDone + item.done;
            long failedCount = item.reset ? item.failed
                    : item.readFailed + item.failed;
            events.add(new Object[] {"stage.generation_superseded",
                Fields.of(Attr.STAGE, stage).with(Attr.ITEM_ID, itemId)
                        .with(Attr.GENERATION, item.generation)
                        .with(Attr.SUPERSEDED_BY, "g" + generation)
                        .with(Attr.COUNT, done)
                        .with(Attr.FAILED_COUNT, failedCount)});
            superseded.add(new Object[] {FailureStore.supersedeGeneration(
                    connection, failures, stage, itemId, generation,
                    queryTimeoutSeconds),
                FailureStore.Superseded.GENERATION, "g" + generation});
            item.generation = generation;
            item.reset = true;
            item.done = 0;
            item.failed = 0;
            startedGeneration(itemId, generation);
        }

        /**
         * Notes, to audit, that the stage has its first object of an
         * item's later generation.
         *
         * @param itemId     the item
         * @param generation the generation
         */
        private void startedGeneration(UUID itemId, int generation)
        {
            events.add(new Object[] {"stage.generation_started",
                Fields.of(Attr.STAGE, stage).with(Attr.ITEM_ID, itemId)
                        .with(Attr.GENERATION, generation)});
        }

        /**
         * Writes the items' generations and counts.
         *
         * @throws SQLException if a statement fails
         */
        void flush() throws SQLException
        {
            for (Map.Entry<UUID, ItemRow> entry : rows.entrySet())
            {
                writeItem(connection, entry.getKey(), entry.getValue());
            }
        }

        /**
         * Audits, once committed, what the transaction superseded.
         *
         * @param late the failures recorded superseded on arrival
         */
        void audit(List<FailureStore.Failure> late)
        {
            Telemetry telemetry = Telemetry.get();
            for (Object[] event : events)
            {
                telemetry.audit((String) event[0], (Fields) event[1]);
            }
            for (Object[] change : superseded)
            {
                @SuppressWarnings("unchecked")
                List<FailureStore.Stored> stored =
                        (List<FailureStore.Stored>) change[0];
                FailureStore.auditSuperseded(stage, stored,
                        (FailureStore.Superseded) change[1],
                        (String) change[2]);
            }
            for (FailureStore.Failure failure : late)
            {
                FailureStore.auditSuperseded(stage, List.of(
                        new FailureStore.Stored(failure,
                                FailureStore.State.FAILED)),
                        FailureStore.Superseded.ARRIVAL,
                        lateFailures.get(failure));
            }
        }
    }

    /**
     * A failure with the lineage of the object that failed.
     *
     * @param failure the failure
     * @param lineage the lineage
     * @return the failure
     */
    private static FailureStore.Failure withLineage(
            FailureStore.Failure failure, Lineage lineage)
    {
        return new FailureStore.Failure(failure.itemId(), failure.seq(),
                failure.recordId(), failure.errorCode(),
                failure.sourceOffset(), failure.version(), lineage);
    }

    /**
     * Reads and locks an item's row, making it at a generation if the item
     * has none yet.
     *
     * @param connection the transaction's connection
     * @param itemId     the item
     * @param generation the generation to make it at
     * @return the row
     * @throws SQLException if a statement fails
     */
    private ItemRow lockItem(Connection connection, UUID itemId,
            int generation) throws SQLException
    {
        boolean created;
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + items + " (stage, item_id, generation)"
                        + " VALUES (?, ?, ?) ON CONFLICT DO NOTHING"))
        {
            insert.setQueryTimeout(queryTimeoutSeconds);
            insert.setString(1, stage);
            insert.setObject(2, itemId);
            insert.setInt(3, generation);
            created = insert.executeUpdate() == 1;
        }
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT generation, done, failed FROM " + items
                        + " WHERE stage = ? AND item_id = ? FOR UPDATE"))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            select.setObject(2, itemId);
            try (ResultSet row = select.executeQuery())
            {
                row.next();
                return new ItemRow(row.getInt(1), row.getLong(2),
                        row.getLong(3), created);
            }
        }
    }

    /**
     * Writes an item's row as a transaction changed it: its counts added
     * to, or, for a new generation, started again, with its total and
     * completions cleared, since they were the generation before's.
     *
     * @param connection the transaction's connection
     * @param itemId     the item
     * @param item       its row
     * @throws SQLException if a statement fails
     */
    private void writeItem(Connection connection, UUID itemId, ItemRow item)
            throws SQLException
    {
        String sql = item.reset ? """
                UPDATE %s SET generation = ?, done = ?, failed = ?,
                       expected = NULL, total_message = NULL,
                       upstream_open = false, upstream_completion = 0,
                       completion = 0, completed_at = NULL,
                       progress_key = NULL, progress_at = NULL
                 WHERE stage = ? AND item_id = ?""".formatted(items)
                : """
                UPDATE %s SET generation = ?, done = done + ?,
                       failed = failed + ?
                 WHERE stage = ? AND item_id = ?""".formatted(items);
        if (!item.reset && item.done == 0 && item.failed == 0)
        {
            return;
        }
        try (PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setInt(1, item.generation);
            update.setLong(2, item.done);
            update.setLong(3, item.failed);
            update.setString(4, stage);
            update.setObject(5, itemId);
            update.executeUpdate();
        }
    }

    /**
     * Which of some records something newer at the stage already
     * supersedes (STAGES.md §15.8.3): their item is of a later generation
     * here, or a newer object is stored at their place. For marking a
     * record's message superseded before it is sealed; {@link #store}
     * decides again, under its locks, whether it is counted.
     *
     * @param itemIds     each record's item
     * @param seqs        each record's place; -1 for none
     * @param lineages    each record's lineage
     * @return the indexes of those superseded
     * @throws IllegalStateException if the database can't be reached
     */
    public Set<Integer> supersededAlready(List<UUID> itemIds, List<Long> seqs,
            List<Lineage> lineages)
    {
        Set<Integer> found = new HashSet<>();
        if (itemIds.isEmpty())
        {
            return found;
        }
        String sql = """
                WITH p (idx, item_id, seq, generation, line) AS (
                  SELECT * FROM unnest(?::integer[], ?::uuid[], ?::bigint[],
                                       ?::integer[], ?::text[]))
                SELECT p.idx FROM p
                 WHERE EXISTS (SELECT 1 FROM %1$s i
                                WHERE i.stage = ? AND i.item_id = p.item_id
                                  AND i.generation > p.generation)
                    OR (p.seq >= 0 AND EXISTS (
                        SELECT 1 FROM %2$s r
                         WHERE r.stage = ? AND r.item_id = p.item_id
                           AND r.generation = p.generation AND r.seq = p.seq
                           AND r.state <> 'CLAIMED' AND NOT r.late
                           AND r.superseded_at IS NULL
                           AND r.line > COALESCE(string_to_array(
                                   NULLIF(p.line, ''), '.')::integer[],
                                   '{}')))""".formatted(items, records);
        Object[] indexes = new Object[itemIds.size()];
        Object[] lines = new Object[itemIds.size()];
        Object[] generations = new Object[itemIds.size()];
        for (int index = 0; index < itemIds.size(); index++)
        {
            indexes[index] = index;
            lines[index] = lineages.get(index).lineText();
            generations[index] = lineages.get(index).generation();
        }
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setArray(1, connection.createArrayOf("integer", indexes));
            select.setArray(2, connection.createArrayOf("uuid",
                    itemIds.toArray()));
            select.setArray(3, connection.createArrayOf("bigint",
                    seqs.toArray()));
            select.setArray(4, connection.createArrayOf("integer",
                    generations));
            select.setArray(5, connection.createArrayOf("text", lines));
            select.setString(6, stage);
            select.setString(7, stage);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.add(rows.getInt(1));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * A record stored with its message, not yet sent.
     *
     * @param key     the record
     * @param message its message
     */
    public record Unsent(Key key, OutboundMessage message)
    {
    }

    /**
     * Corrections stored but not sent for longer than a claim lasts: the
     * worker that stored one stopped before sending it (STAGES.md §15.6c).
     * A record has its message sent again when it is delivered again; a
     * correction isn't, so it is found here.
     *
     * @param olderThan how long ago it was stored, at least
     * @return the corrections, with their messages
     * @throws IllegalStateException if the database can't be reached
     */
    public List<Unsent> unsentCorrections(Duration olderThan)
    {
        String sql = """
                SELECT item_id, record_id, message_id, message_type,
                       message_key, pending_message
                  FROM %s
                 WHERE stage = ?
                   AND (correction IS NOT NULL
                        OR record_id LIKE 'correction:%%')
                   AND state = 'STORED' AND pending_message IS NOT NULL
                   AND stored_at <= clock_timestamp()
                                    - ? * interval '1 millisecond'
                 LIMIT 100""".formatted(records);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            select.setLong(2, olderThan.toMillis());
            List<Unsent> found = new ArrayList<>();
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    UUID itemId = rows.getObject(1, UUID.class);
                    found.add(new Unsent(new Key(itemId, rows.getString(2)),
                            new OutboundMessage(rows.getString(3),
                                    MessageType.fromWire(rows.getString(4)),
                                    itemId, rows.getString(5),
                                    rows.getBytes(6))));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * Marks records sent, dropping their messages.
     *
     * @param keys the records
     * @throws IllegalStateException if the database can't be reached
     */
    public void markSent(Collection<Key> keys)
    {
        if (keys.isEmpty())
        {
            return;
        }
        String sql = """
                UPDATE %s
                   SET state = 'SENT', pending_message = NULL
                 WHERE stage = ?
                   AND (item_id, record_id) IN
                       (SELECT * FROM unnest(?::uuid[], ?::text[]))
                   AND state = 'STORED'""".formatted(records);
        List<UUID> itemIds = new ArrayList<>();
        List<String> recordIds = new ArrayList<>();
        for (Key key : keys)
        {
            itemIds.add(key.itemId());
            recordIds.add(key.recordId());
        }
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setString(1, stage);
            Array items = connection.createArrayOf("uuid", itemIds.toArray());
            Array ids = connection.createArrayOf("text", recordIds.toArray());
            update.setArray(2, items);
            update.setArray(3, ids);
            update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * An item whose records are all stored, as kept.
     *
     * @param progress    its counts, with its failures, without the
     *                    total's message
     * @param progressKey the counts of the last {@code item.progress} sent
     *                    for it; null if none
     * @param readAt      when the database read its counts: a change to
     *                    its failures after then is looked at again
     * @param total    the message that gave its total, sealed as received;
     *                 null if it wasn't kept
     */
    public record DueItem(ItemProgress progress, byte[] total,
            String progressKey, Instant readAt)
    {

        /**
         * An item with no progress sent.
         *
         * @param progress its counts
         * @param total    the message that gave its total, or null
         */
        public DueItem(ItemProgress progress, byte[] total)
        {
            this(progress, total, null, null);
        }

        /**
         * Whether its counts changed since the last {@code item.progress}
         * this stage sent for it.
         *
         * @return true if one is to be sent
         */
        public boolean progressChanged()
        {
            return !progress.progressKey().equals(progressKey);
        }
    }

    /**
     * Notes how many records an item has. The first total given is kept: a
     * repeat of the same message gives the same one.
     *
     * @param itemId   the item's id
     * @param expected how many records it has
     * @throws IllegalStateException if the database can't be reached
     */
    public void expect(UUID itemId, long expected)
    {
        expect(itemId, expected, null);
    }

    /**
     * Notes how many records an item has, and keeps the message that said
     * so, sealed as received, for the stage's item-complete step. The first
     * given is kept: a repeat of the same message gives the same one.
     *
     * @param itemId   the item's id
     * @param expected how many records it has
     * @param total    the message, sealed; null to keep none
     * @throws IllegalStateException if the database can't be reached
     */
    public void expect(UUID itemId, long expected, byte[] total)
    {
        expect(itemId, expected, total, false);
    }

    /**
     * Notes how many records an item has, or has so far, and keeps the
     * message that said so. A final total (the stage before's
     * item-complete message) is kept once given. One from the stage
     * before's {@code item.progress} ({@code partial}) may grow, while that
     * stage still has failures open (STAGES.md §15.6): a larger one
     * replaces it, and the final one replaces it whatever its size.
     *
     * @param itemId   the item's id
     * @param expected how many records it has, or has so far
     * @param total    the message, sealed; null to keep none
     * @param partial  whether the total is from {@code item.progress}
     * @throws IllegalStateException if the database can't be reached
     */
    public void expect(UUID itemId, long expected, byte[] total,
            boolean partial)
    {
        expect(itemId, expected, total, partial, partial ? 0 : 1);
    }

    /**
     * Notes how many records an item has, or has so far, and keeps the
     * message that said so, as {@link #expect(UUID, long, byte[], boolean)}
     * does, by the stage before's completion number (STAGES.md §15.6e). A
     * final total is taken if it is of a later completion than the one
     * taken; a total from {@code item.progress} grows the one taken while
     * it is open, and reopens the item if it reopens the completion taken
     * (or a later one): the item is then incomplete here again, and
     * reported so. One from an earlier completion is stale, and changes
     * nothing, so the messages need no order.
     *
     * @param itemId     the item's id
     * @param expected   how many records it has, or has so far
     * @param total      the message, sealed; null to keep none
     * @param partial    whether the total is from {@code item.progress}
     * @param completion the stage before's completion number, as
     *                   {@link StageInput.ItemTotal#completion}
     */
    public void expect(UUID itemId, long expected, byte[] total,
            boolean partial, int completion)
    {
        expect(itemId, expected, total, partial, completion, 1);
    }

    /**
     * Notes how many records an item's generation has, or has so far, as
     * {@link #expect(UUID, long, byte[], boolean, int)} does, weighed by
     * generation first (STAGES.md §15.8.3): a total of an earlier
     * generation than the item's here is stale, and changes nothing; one of
     * a later generation moves the item to it, as its first record would,
     * its counts starting again and the failures of the generations before
     * superseded, and is then taken.
     *
     * @param itemId     the item's id
     * @param expected   how many records it has, or has so far
     * @param total      the message, sealed; null to keep none
     * @param partial    whether the total is from {@code item.progress}
     * @param completion the stage before's completion number
     * @param generation the generation the total is of, from 1
     * @throws IllegalStateException if the database can't be reached
     */
    public void expect(UUID itemId, long expected, byte[] total,
            boolean partial, int completion, int generation)
    {
        // take: the row's total is replaced, a new round of the item;
        // grow: a total from item.progress, the same round, still open.
        String take = """
                (i.expected IS NULL
                 OR (NOT EXCLUDED.upstream_open
                     AND EXCLUDED.upstream_completion
                         > i.upstream_completion)
                 OR (EXCLUDED.upstream_open
                     AND EXCLUDED.upstream_completion
                         >= i.upstream_completion
                     AND (NOT i.upstream_open
                          OR EXCLUDED.upstream_completion
                             > i.upstream_completion)))""";
        String grow = """
                (EXCLUDED.upstream_open AND i.upstream_open
                 AND EXCLUDED.upstream_completion = i.upstream_completion)""";
        // Whether it reopens the item here: complete before, not after.
        String sql = """
                WITH prior AS (SELECT completed_at FROM %1$s
                                WHERE stage = ? AND item_id = ?)
                INSERT INTO %1$s AS i (stage, item_id, expected,
                                       total_message, upstream_open,
                                       upstream_completion)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (stage, item_id) DO UPDATE
                   SET expected = CASE
                           WHEN %2$s THEN EXCLUDED.expected
                           WHEN %3$s
                                THEN GREATEST(i.expected, EXCLUDED.expected)
                           ELSE i.expected END,
                       total_message = CASE
                           WHEN %2$s THEN EXCLUDED.total_message
                           WHEN %3$s AND EXCLUDED.expected >= i.expected
                                THEN EXCLUDED.total_message
                           ELSE i.total_message END,
                       upstream_open = CASE
                           WHEN %2$s THEN EXCLUDED.upstream_open
                           ELSE i.upstream_open END,
                       upstream_completion = CASE
                           WHEN %2$s THEN EXCLUDED.upstream_completion
                           ELSE i.upstream_completion END,
                       completed_at = CASE
                           WHEN %2$s AND i.expected IS NOT NULL THEN NULL
                           ELSE i.completed_at END
                RETURNING (SELECT completed_at FROM prior) IS NOT NULL
                          AND i.completed_at IS NULL"""
                .formatted(items, take, grow);
        boolean reopens = false;
        Currency currency;
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                currency = new Currency(connection);
                ItemRow item = currency.item(itemId, generation);
                if (generation < item.generation)
                {
                    // A total of a generation superseded here: stale.
                    connection.rollback();
                    Telemetry.get().audit("stage.total_superseded",
                            Fields.of(Attr.STAGE, stage)
                                    .with(Attr.ITEM_ID, itemId)
                                    .with(Attr.GENERATION, generation)
                                    .with(Attr.SUPERSEDED_BY,
                                            "g" + item.generation));
                    return;
                }
                if (generation > item.generation)
                {
                    currency.newGeneration(itemId, item, generation);
                }
                currency.flush();
                try (PreparedStatement upsert =
                        connection.prepareStatement(sql))
                {
                    upsert.setQueryTimeout(queryTimeoutSeconds);
                    upsert.setString(1, stage);
                    upsert.setObject(2, itemId);
                    upsert.setString(3, stage);
                    upsert.setObject(4, itemId);
                    upsert.setLong(5, expected);
                    upsert.setBytes(6, total);
                    upsert.setBoolean(7, partial);
                    upsert.setInt(8, completion);
                    try (ResultSet rows = upsert.executeQuery())
                    {
                        reopens = rows.next() && rows.getBoolean(1);
                    }
                }
                connection.commit();
            }
            catch (SQLException failure)
            {
                connection.rollback();
                throw failure;
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
        currency.audit(List.of());
        if (reopens)
        {
            Telemetry.get().audit("stage.item_reopened",
                    Fields.of(Attr.STAGE, stage).with(Attr.ITEM_ID, itemId)
                            .with(Attr.REASON, "upstream"));
        }
    }

    /**
     * The items, of those given, whose records are all stored and whose
     * item-complete message hasn't been sent.
     *
     * @param itemIds the items to look at
     * @return those that are done, with their counts and the total's
     *         message
     * @throws IllegalStateException if the database can't be reached
     */
    public List<DueItem> due(Collection<UUID> itemIds)
    {
        List<DueItem> due = new ArrayList<>();
        if (itemIds.isEmpty())
        {
            return due;
        }
        // With the versions its records were made under, in the order first
        // used (STAGES.md §14.3).
        // And with its failures as the fail store has them (§15.6).
        String sql = """
                SELECT i.item_id, i.expected, i.done, i.failed,
                       i.total_message,
                       ARRAY(SELECT r.output_version FROM %2$s r
                              WHERE r.stage = i.stage
                                AND r.item_id = i.item_id
                                AND r.generation = i.generation
                                AND NOT r.late
                                AND r.output_version IS NOT NULL
                              GROUP BY r.output_version
                              ORDER BY min(r.stored_at)),
                       i.upstream_open, i.progress_key,
                       (SELECT count(*) FROM %3$s f
                         WHERE f.stage = i.stage AND f.item_id = i.item_id
                           AND f.generation = i.generation
                           AND f.state IN %4$s),
                       (SELECT count(*) FROM %3$s f
                         WHERE f.stage = i.stage AND f.item_id = i.item_id
                           AND f.generation = i.generation
                           AND f.state = 'DECLINED'),
                       clock_timestamp(),
                       (SELECT count(*) FROM %3$s f
                         WHERE f.stage = i.stage AND f.item_id = i.item_id
                           AND f.generation = i.generation
                           AND f.state = 'SUPERSEDED'
                           AND COALESCE(f.superseded_how, 'REVISION')
                               = 'REVISION'),
                       i.completion, i.generation
                  FROM %1$s i
                 WHERE i.stage = ? AND i.item_id = ANY (?)
                   AND i.completed_at IS NULL AND i.expected IS NOT NULL
                   AND i.done >= i.expected""".formatted(items, records,
                failures, FailureStore.State.OPEN_SQL);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            select.setArray(2, connection.createArrayOf("uuid",
                    itemIds.toArray()));
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    java.sql.Array versions = rows.getArray(6);
                    due.add(new DueItem(new ItemProgress(
                            rows.getObject(1, UUID.class), rows.getLong(2),
                            rows.getLong(3), rows.getLong(4))
                            .withVersions(versions == null ? List.of()
                                    : List.of((String[]) versions.getArray()))
                            .withFailures(rows.getLong(9), rows.getLong(10),
                                    rows.getBoolean(7))
                            .withResolved(rows.getLong(12))
                            .withCompletion(rows.getInt(13))
                            .withGeneration(rows.getInt(14)),
                            rows.getBytes(5), rows.getString(8),
                            rows.getTimestamp(11).toInstant()));
                }
            }
            return due;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * Notes the counts of the {@code item.progress} just sent for an item,
     * so it isn't sent again until they change.
     *
     * @param itemId the item's id
     * @param key    its counts, as {@link ItemProgress#progressKey}
     * @throws IllegalStateException if the database can't be reached
     */
    public void markProgressed(UUID itemId, String key)
    {
        markProgressed(itemId, key, null);
    }

    /**
     * Notes the counts of the {@code item.progress} just sent for an item,
     * and when they were read, so a failure changed after then (an
     * operator's decline, say) has the item looked at again
     * ({@link #changedItems}).
     *
     * @param itemId the item's id
     * @param key    its counts, as {@link ItemProgress#progressKey}
     * @param readAt when they were read; null for now
     * @throws IllegalStateException if the database can't be reached
     */
    public void markProgressed(UUID itemId, String key, Instant readAt)
    {
        String sql = """
                UPDATE %s SET progress_key = ?,
                       progress_at = COALESCE(?, clock_timestamp())
                 WHERE stage = ? AND item_id = ? AND completed_at IS NULL"""
                .formatted(items);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setString(1, key);
            update.setTimestamp(2, readAt == null ? null
                    : Timestamp.from(readAt));
            update.setString(3, stage);
            update.setObject(4, itemId);
            update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * The items not complete whose records are all handled and one of whose
     * failures changed since their counts were last reported: an operator
     * declined or reopened it (STAGES.md §15.3), or the report never went.
     * The worker looks at them again, to complete them or report their new
     * counts.
     *
     * @param limit the most returned
     * @return their ids
     * @throws IllegalStateException if the database can't be reached
     */
    public List<UUID> changedItems(int limit)
    {
        String sql = """
                SELECT i.item_id FROM %1$s i
                 WHERE i.stage = ? AND i.completed_at IS NULL
                   AND i.expected IS NOT NULL AND i.done >= i.expected
                   AND EXISTS (SELECT 1 FROM %2$s f
                                WHERE f.stage = i.stage
                                  AND f.item_id = i.item_id
                                  AND f.changed_at > COALESCE(i.progress_at,
                                      '-infinity'::timestamptz))
                 LIMIT ?""".formatted(items, failures);
        List<UUID> found = new ArrayList<>();
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
                    found.add(rows.getObject(1, UUID.class));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * Reopens the items complete here one of whose failures was opened
     * again since they completed (an operator's {@code reopen}, STAGES.md
     * §15.6e): each is incomplete again, so the worker reports it in
     * progress, and completes it again, numbered after the last, once
     * nothing is open. Audited {@code stage.item_reopened}.
     *
     * @param limit the most reopened
     * @return their ids
     * @throws IllegalStateException if the database can't be reached
     */
    public List<UUID> reopenChanged(int limit)
    {
        String sql = """
                UPDATE %1$s i SET completed_at = NULL
                 WHERE i.stage = ? AND i.item_id IN (
                       SELECT c.item_id FROM %1$s c
                        WHERE c.stage = ? AND c.completed_at IS NOT NULL
                          AND EXISTS (SELECT 1 FROM %2$s f
                                       WHERE f.stage = c.stage
                                         AND f.item_id = c.item_id
                                         AND f.generation = c.generation
                                         AND f.state IN %3$s
                                         AND f.changed_at > c.completed_at)
                        LIMIT ?)
                   AND i.completed_at IS NOT NULL
                RETURNING i.item_id""".formatted(items, failures,
                FailureStore.State.OPEN_SQL);
        List<UUID> reopened = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setString(1, stage);
            update.setString(2, stage);
            update.setInt(3, limit);
            try (ResultSet rows = update.executeQuery())
            {
                while (rows.next())
                {
                    reopened.add(rows.getObject(1, UUID.class));
                }
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
        for (UUID itemId : reopened)
        {
            Telemetry.get().audit("stage.item_reopened",
                    Fields.of(Attr.STAGE, stage).with(Attr.ITEM_ID, itemId)
                            .with(Attr.REASON, "failure_reopened"));
        }
        return reopened;
    }

    /**
     * The stage's corrections (STAGES.md §15.6c), in the registry's schema.
     *
     * @return the store
     */
    public CorrectionStore correctionStore()
    {
        return new CorrectionStore(dataSource, schema(), stage,
                java.time.Duration.ofSeconds(queryTimeoutSeconds));
    }

    /**
     * The registry's schema.
     *
     * @return its name
     */
    private String schema()
    {
        return failures.substring(0, failures.indexOf('.'));
    }

    /**
     * The stage's fail store (STAGES.md §15.3), in the registry's schema.
     *
     * @return the store
     */
    public FailureStore failureStore()
    {
        return new FailureStore(dataSource,
                failures.substring(0, failures.indexOf('.')), stage,
                java.time.Duration.ofSeconds(queryTimeoutSeconds));
    }

    /**
     * Marks an item's item-complete message sent, and counts the
     * completion (STAGES.md §15.6e): the next is numbered after it.
     *
     * @param itemId the item's id
     * @throws IllegalStateException if the database can't be reached
     */
    public void markCompleted(UUID itemId)
    {
        String sql = """
                UPDATE %s SET completed_at = clock_timestamp(),
                       completion = completion + 1
                 WHERE stage = ? AND item_id = ? AND completed_at IS NULL"""
                .formatted(items);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setString(1, stage);
            update.setObject(2, itemId);
            update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * The records of an item the stage has stored, or holds a live claim
     * on, by any worker: for a reconciler, to find those whose messages
     * never came. A claim left to expire with nothing stored isn't known:
     * its message may have been lost with the batch that claimed it, and
     * a worker handed it again takes the claim over (STAGES.md §10).
     *
     * @param itemId the item's id
     * @return their names: each record ID with its lineage's suffix
     *         ({@link Key})
     * @throws IllegalStateException if the database can't be reached
     */
    public Set<String> knownRecords(UUID itemId)
    {
        String sql = "SELECT record_id FROM " + records
                + " WHERE stage = ? AND item_id = ?"
                + " AND (state <> 'CLAIMED'"
                + " OR claim_expires > clock_timestamp())";
        Set<String> found = new HashSet<>();
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
                    found.add(rows.getString(1));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * The items, of those given, whose item-complete message was sent.
     *
     * @param itemIds the items to look at
     * @return those completed
     * @throws IllegalStateException if the database can't be reached
     */
    public Set<UUID> completed(Collection<UUID> itemIds)
    {
        Set<UUID> found = new HashSet<>();
        if (itemIds.isEmpty())
        {
            return found;
        }
        String sql = """
                SELECT item_id FROM %s
                 WHERE stage = ? AND item_id = ANY (?)
                   AND completed_at IS NOT NULL""".formatted(items);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            select.setArray(2, connection.createArrayOf("uuid",
                    itemIds.toArray()));
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
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * An item's row.
     *
     * @param itemId the item's id
     * @return its counts, with -1 for a total not yet known, the
     *         completions sent and its generation; empty if it has no row
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<ItemProgress> item(UUID itemId)
    {
        String sql = "SELECT expected, done, failed, completion, generation"
                + " FROM " + items + " WHERE stage = ? AND item_id = ?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            select.setObject(2, itemId);
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    return Optional.empty();
                }
                long expected = rows.getLong(1);
                boolean unknown = rows.wasNull();
                return Optional.of(new ItemProgress(itemId,
                        unknown ? -1 : expected, rows.getLong(2),
                        rows.getLong(3)).withCompletion(rows.getInt(4))
                        .withGeneration(rows.getInt(5)));
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * How far the stage's reconciler has checked: every record committed
     * before it was looked at, after its grace (STAGES.md §9).
     *
     * @return the time; empty if the reconciler has never checked a day
     *         here
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<Instant> checkedTo()
    {
        String sql = "SELECT checked_to FROM " + reconcile
                + " WHERE stage = ?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            try (ResultSet rows = select.executeQuery())
            {
                return rows.next()
                        ? Optional.of(rows.getTimestamp(1).toInstant())
                        : Optional.empty();
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * Notes that the reconciler has checked up to a time. It only moves
     * forward, so a pod that lost the duties lease part way can't move it
     * back.
     *
     * @param to the time every record committed before has been checked
     * @throws IllegalStateException if the database can't be reached
     */
    public void markChecked(Instant to)
    {
        String sql = """
                INSERT INTO %1$s AS r (stage, checked_to, updated_at)
                VALUES (?, ?, clock_timestamp())
                ON CONFLICT (stage) DO UPDATE
                   SET checked_to = GREATEST(r.checked_to,
                                             EXCLUDED.checked_to),
                       updated_at = EXCLUDED.updated_at""".formatted(
                reconcile);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement upsert = connection.prepareStatement(sql))
        {
            upsert.setQueryTimeout(queryTimeoutSeconds);
            upsert.setString(1, stage);
            upsert.setTimestamp(2, Timestamp.from(to));
            upsert.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * Notes that a record older than the reconciler's grace has just come
     * off the queue: a backlog is still draining (STAGES.md §9). Stamped
     * with the database's clock.
     *
     * @throws IllegalStateException if the database can't be reached
     */
    public void noteBacklog()
    {
        update("""
                INSERT INTO %1$s AS b (stage, seen_at)
                VALUES (?, clock_timestamp())
                ON CONFLICT (stage) DO UPDATE
                   SET seen_at = GREATEST(b.seen_at, EXCLUDED.seen_at)"""
                .formatted(backlog));
    }

    /**
     * How long ago a record older than the grace last came off the queue,
     * by the database's clock.
     *
     * @return the time since; empty if none ever has
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<Duration> sinceBacklog()
    {
        return since("SELECT extract(epoch FROM clock_timestamp() - seen_at)"
                + " FROM " + backlog + " WHERE stage = ?");
    }

    /**
     * Notes that the reconciler is waiting, if it wasn't already, and says
     * for how long it has been: for a backlog, or at startup, when there
     * may be no row yet.
     *
     * @return how long it has waited, by the database's clock
     * @throws IllegalStateException if the database can't be reached
     */
    public Duration deferring()
    {
        update("""
                INSERT INTO %1$s AS b (stage, deferring_since)
                VALUES (?, clock_timestamp())
                ON CONFLICT (stage) DO UPDATE
                   SET deferring_since = COALESCE(b.deferring_since,
                                                  EXCLUDED.deferring_since)"""
                .formatted(backlog));
        return since("SELECT extract(epoch FROM clock_timestamp()"
                + " - deferring_since) FROM " + backlog
                + " WHERE stage = ? AND deferring_since IS NOT NULL")
                .orElse(Duration.ZERO);
    }

    /**
     * Notes that the reconciler has stopped waiting for the backlog.
     *
     * @throws IllegalStateException if the database can't be reached
     */
    public void endDeferral()
    {
        update("UPDATE " + backlog + " SET deferring_since = NULL"
                + " WHERE stage = ? AND deferring_since IS NOT NULL");
    }

    /**
     * Runs a statement whose only parameter is the stage.
     *
     * @param sql the statement
     * @throws IllegalStateException if the database can't be reached
     */
    private void update(String sql)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setString(1, stage);
            update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }

    /**
     * Reads a span in seconds, by a query whose only parameter is the
     * stage.
     *
     * @param sql the query
     * @return the span; empty if there is no row, or the span is null
     * @throws IllegalStateException if the database can't be reached
     */
    private Optional<Duration> since(String sql)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, stage);
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    return Optional.empty();
                }
                double seconds = rows.getDouble(1);
                return rows.wasNull() ? Optional.empty()
                        : Optional.of(Duration.ofMillis(
                                Math.round(seconds * 1000)));
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
        }
    }
}
