/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
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
     * What a record is known by: its item and its id. A record id comes
     * from the content alone (design §7.3), so the same content in two
     * items is a record of each, counted and stored for both (decided
     * 2026-11-05).
     *
     * @param itemId   the id of the record's item
     * @param recordId the record's id
     */
    public record Key(UUID itemId, String recordId)
    {
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
     */
    public record Entry(String recordId, UUID itemId, String digest,
            String version, boolean failed, OutboundMessage message,
            FailureStore.Failure failure, CorrectionStore.Outcome correction)
    {

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
            return new Key(itemId, recordId);
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
                       claim_expires = NULL
                 WHERE stage = ? AND item_id = ? AND record_id = ?
                   AND owner = ? AND state = 'CLAIMED'""".formatted(records);
        String count = """
                INSERT INTO %1$s AS i (stage, item_id, done, failed)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (stage, item_id) DO UPDATE
                   SET done = i.done + EXCLUDED.done,
                       failed = i.failed + EXCLUDED.failed"""
                .formatted(items);
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                Set<Key> marked = new HashSet<>();
                Map<UUID, long[]> counts = new LinkedHashMap<>();
                List<FailureStore.Failure> failed = new ArrayList<>();
                List<Entry> corrections = new ArrayList<>();
                try (PreparedStatement update =
                        connection.prepareStatement(mark))
                {
                    update.setQueryTimeout(queryTimeoutSeconds);
                    for (Entry entry : entries)
                    {
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
                        update.setString(8, stage);
                        update.setObject(9, entry.itemId());
                        update.setString(10, entry.recordId());
                        update.setString(11, owner);
                        if (update.executeUpdate() == 1)
                        {
                            marked.add(entry.key());
                            if (entry.correction() != null)
                            {
                                // Not a record of the item's: its count
                                // stays; its failure moves on instead.
                                corrections.add(entry);
                                continue;
                            }
                            if (entry.failure() != null)
                            {
                                failed.add(entry.failure());
                            }
                            long[] itemCounts = counts.computeIfAbsent(
                                    entry.itemId(), id -> new long[2]);
                            itemCounts[0]++;
                            itemCounts[1] += entry.failed() ? 1 : 0;
                        }
                    }
                }
                try (PreparedStatement upsert =
                        connection.prepareStatement(count))
                {
                    upsert.setQueryTimeout(queryTimeoutSeconds);
                    for (Map.Entry<UUID, long[]> item : counts.entrySet())
                    {
                        upsert.setString(1, stage);
                        upsert.setObject(2, item.getKey());
                        upsert.setLong(3, item.getValue()[0]);
                        upsert.setLong(4, item.getValue()[1]);
                        upsert.addBatch();
                    }
                    upsert.executeBatch();
                }
                List<FailureStore.Failure> added = FailureStore.insert(
                        connection, failures, stage, failed,
                        queryTimeoutSeconds);
                List<FailureStore.Changed> finished = new ArrayList<>();
                for (Entry entry : corrections)
                {
                    finished.add(CorrectionStore.finish(connection, schema(),
                            stage, entry.correction(), queryTimeoutSeconds));
                }
                connection.commit();
                FailureStore.audit(stage, added);
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
                 WHERE stage = ? AND record_id LIKE 'correction:%%'
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
        String sql = """
                INSERT INTO %1$s AS i (stage, item_id, expected,
                                       total_message, upstream_open)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (stage, item_id) DO UPDATE
                   SET expected = CASE
                           WHEN i.expected IS NULL THEN EXCLUDED.expected
                           WHEN NOT i.upstream_open THEN i.expected
                           WHEN NOT EXCLUDED.upstream_open
                                THEN EXCLUDED.expected
                           ELSE GREATEST(i.expected, EXCLUDED.expected) END,
                       total_message = CASE
                           WHEN i.expected IS NULL
                                THEN EXCLUDED.total_message
                           WHEN NOT i.upstream_open THEN i.total_message
                           WHEN NOT EXCLUDED.upstream_open
                                OR EXCLUDED.expected >= i.expected
                                THEN EXCLUDED.total_message
                           ELSE i.total_message END,
                       upstream_open = CASE
                           WHEN i.expected IS NULL
                                THEN EXCLUDED.upstream_open
                           WHEN NOT i.upstream_open THEN false
                           ELSE EXCLUDED.upstream_open END"""
                .formatted(items);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement upsert = connection.prepareStatement(sql))
        {
            upsert.setQueryTimeout(queryTimeoutSeconds);
            upsert.setString(1, stage);
            upsert.setObject(2, itemId);
            upsert.setLong(3, expected);
            upsert.setBytes(4, total);
            upsert.setBoolean(5, partial);
            upsert.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
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
                                AND r.output_version IS NOT NULL
                              GROUP BY r.output_version
                              ORDER BY min(r.stored_at)),
                       i.upstream_open, i.progress_key,
                       (SELECT count(*) FROM %3$s f
                         WHERE f.stage = i.stage AND f.item_id = i.item_id
                           AND f.state IN ('FAILED', 'CORRECTION_PENDING',
                                           'FAILED_CORRECTION')),
                       (SELECT count(*) FROM %3$s f
                         WHERE f.stage = i.stage AND f.item_id = i.item_id
                           AND f.state = 'DECLINED'),
                       clock_timestamp(),
                       (SELECT count(*) FROM %3$s f
                         WHERE f.stage = i.stage AND f.item_id = i.item_id
                           AND f.state IN ('CORRECTED', 'RETRIED',
                                           'SUPERSEDED'))
                  FROM %1$s i
                 WHERE i.stage = ? AND i.item_id = ANY (?)
                   AND i.completed_at IS NULL AND i.expected IS NOT NULL
                   AND i.done >= i.expected""".formatted(items, records,
                failures);
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
                            .withResolved(rows.getLong(12)),
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
     * Marks an item's item-complete message sent.
     *
     * @param itemId the item's id
     * @throws IllegalStateException if the database can't be reached
     */
    public void markCompleted(UUID itemId)
    {
        String sql = """
                UPDATE %s SET completed_at = clock_timestamp()
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
     * @return their record IDs
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
     * @return its counts, with -1 for a total not yet known; empty if it
     *         has no row
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<ItemProgress> item(UUID itemId)
    {
        String sql = "SELECT expected, done, failed FROM " + items
                + " WHERE stage = ? AND item_id = ?";
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
                return Optional.of(new ItemProgress(itemId,
                        rows.wasNull() ? -1 : expected, rows.getLong(2),
                        rows.getLong(3)));
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
