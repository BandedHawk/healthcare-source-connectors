/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
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
     * @param digest  the digest of the output stored
     * @param pending the message it is to be sent as, sealed, if it hasn't
     *                been sent; null once it has
     * @param mine    whether this worker stored it, as when a batch is
     *                tried again after its send failed
     */
    public record Stored(String digest, OutboundMessage pending,
            boolean mine)
            implements Claim
    {
    }

    /** Another worker holds a live claim on the record. */
    public record Busy() implements Claim
    {
    }

    /**
     * A record to mark stored.
     *
     * @param recordId the record's id
     * @param itemId   the id of its item
     * @param digest   its output's digest
     * @param failed   whether it failed, and was stored as it was
     * @param message  the message it is to be sent as, sealed
     */
    public record Entry(String recordId, UUID itemId, String digest,
            boolean failed, OutboundMessage message)
    {
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
                  record_id       text        NOT NULL,
                  item_id         uuid        NOT NULL,
                  state           text        NOT NULL,
                  owner           text        NOT NULL,
                  attempts        integer     NOT NULL,
                  claim_expires   timestamptz,
                  output_digest   text,
                  message_id      text,
                  message_type    text,
                  pending_message bytea,
                  stored_at       timestamptz,
                  PRIMARY KEY (stage, record_id)
                )""".formatted(records);
        String itemDdl = """
                CREATE TABLE IF NOT EXISTS %s (
                  stage        text        NOT NULL,
                  item_id      uuid        NOT NULL,
                  expected     bigint,
                  done         bigint      NOT NULL DEFAULT 0,
                  failed       bigint      NOT NULL DEFAULT 0,
                  completed_at timestamptz,
                  PRIMARY KEY (stage, item_id)
                )""".formatted(items);
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(queryTimeoutSeconds);
            statement.execute("CREATE SCHEMA IF NOT EXISTS "
                    + records.substring(0, records.indexOf('.')));
            statement.execute(recordDdl);
            statement.execute(itemDdl);
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
                       (stage, record_id, item_id, state, owner, attempts,
                        claim_expires)
                VALUES (?, ?, ?, 'CLAIMED', ?, 1,
                        clock_timestamp() + ? * interval '1 millisecond')
                ON CONFLICT (stage, record_id) DO UPDATE
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
                insert.setString(2, recordId);
                insert.setObject(3, itemId);
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
                            + " message_type, pending_message"
                            + " FROM " + records
                            + " WHERE stage = ? AND record_id = ?"))
            {
                select.setQueryTimeout(queryTimeoutSeconds);
                select.setString(1, stage);
                select.setString(2, recordId);
                try (ResultSet rows = select.executeQuery())
                {
                    if (rows.next() && !"CLAIMED".equals(rows.getString(1)))
                    {
                        byte[] body = rows.getBytes(6);
                        return new Stored(rows.getString(2), body == null
                                ? null
                                : new OutboundMessage(rows.getString(4),
                                        MessageType.fromWire(
                                                rows.getString(5)),
                                        itemId, body),
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
     * Marks records stored, each with the message it is to be sent as, and
     * counts them on their items, in one transaction. A record whose claim
     * this worker no longer holds is left alone: its new holder stores and
     * counts it.
     *
     * @param entries the records, all committed
     * @return the ids of those marked; the others' claims were lost
     * @throws IllegalStateException if the database can't be reached
     */
    public Set<String> store(List<Entry> entries)
    {
        String mark = """
                UPDATE %s
                   SET state = 'STORED', output_digest = ?,
                       message_id = ?, message_type = ?,
                       pending_message = ?, stored_at = clock_timestamp(),
                       claim_expires = NULL
                 WHERE stage = ? AND record_id = ? AND owner = ?
                   AND state = 'CLAIMED'""".formatted(records);
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
                Set<String> marked = new HashSet<>();
                Map<UUID, long[]> counts = new LinkedHashMap<>();
                try (PreparedStatement update =
                        connection.prepareStatement(mark))
                {
                    update.setQueryTimeout(queryTimeoutSeconds);
                    for (Entry entry : entries)
                    {
                        update.setString(1, entry.digest());
                        update.setString(2, entry.message().messageId());
                        update.setString(3,
                                entry.message().type().wireName());
                        update.setBytes(4, entry.message().body());
                        update.setString(5, stage);
                        update.setString(6, entry.recordId());
                        update.setString(7, owner);
                        if (update.executeUpdate() == 1)
                        {
                            marked.add(entry.recordId());
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
                connection.commit();
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
     * Marks records sent, dropping their messages.
     *
     * @param recordIds the records
     * @throws IllegalStateException if the database can't be reached
     */
    public void markSent(Collection<String> recordIds)
    {
        if (recordIds.isEmpty())
        {
            return;
        }
        String sql = """
                UPDATE %s
                   SET state = 'SENT', pending_message = NULL
                 WHERE stage = ? AND record_id = ANY (?)
                   AND state = 'STORED'""".formatted(records);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setString(1, stage);
            Array ids = connection.createArrayOf("text",
                    recordIds.toArray());
            update.setArray(2, ids);
            update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Stage registry unavailable",
                    failure);
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
        String sql = """
                INSERT INTO %1$s AS i (stage, item_id, expected)
                VALUES (?, ?, ?)
                ON CONFLICT (stage, item_id) DO UPDATE
                   SET expected = COALESCE(i.expected, EXCLUDED.expected)"""
                .formatted(items);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement upsert = connection.prepareStatement(sql))
        {
            upsert.setQueryTimeout(queryTimeoutSeconds);
            upsert.setString(1, stage);
            upsert.setObject(2, itemId);
            upsert.setLong(3, expected);
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
     * @return those that are done, with their counts
     * @throws IllegalStateException if the database can't be reached
     */
    public List<ItemProgress> due(Collection<UUID> itemIds)
    {
        List<ItemProgress> due = new ArrayList<>();
        if (itemIds.isEmpty())
        {
            return due;
        }
        String sql = """
                SELECT item_id, expected, done, failed FROM %s
                 WHERE stage = ? AND item_id = ANY (?)
                   AND completed_at IS NULL AND expected IS NOT NULL
                   AND done >= expected""".formatted(items);
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
                    due.add(new ItemProgress(rows.getObject(1, UUID.class),
                            rows.getLong(2), rows.getLong(3),
                            rows.getLong(4)));
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
}
