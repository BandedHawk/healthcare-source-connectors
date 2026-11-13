/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import com.sdh.healthcare.connector.core.failure.FailureStore;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * The split stage's checkpoint per item, in PostgreSQL (STAGES.md §8): a
 * row in {@code <schema>.split_item}, which holds the item's checkpoint,
 * MAC'd ({@link CheckpointCodec}), the checkpoint of a batch being
 * committed ({@code pending}), the checkpoint's state in plain text, for
 * counting, and when the item's splitting ended. That time is noted once,
 * and every {@code item.split} for the item carries it, so a message sent
 * again is the same as the first.
 * <p>
 * Every write is fenced by the item's lease: the row keeps the epoch of the
 * lease it was last written under, and a write under a lower epoch (from a
 * worker whose lease was taken over) changes nothing and throws
 * {@link LeaseLostException}.
 * <p>
 * A batch is committed in three steps: {@link #prepare} keeps the
 * checkpoint after it as {@code pending}, the batch is appended, and
 * {@link #save} makes that the checkpoint. The caller resolves a pending
 * checkpoint found at resume by looking for the batch's last record in the
 * table.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ItemCheckpoints
{

    /** A schema name: a plain identifier, as it is put in the SQL. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");

    /**
     * What is stored for an item.
     *
     * @param checkpoint its checkpoint
     * @param pending    the checkpoint of a batch that was being committed;
     *                   null if none
     * @param endedAt    when its splitting ended: it first reached
     *                   {@code ARCHIVING}, {@code FAILING} or a final
     *                   state; null before
     */
    public record Stored(Checkpoint checkpoint, Checkpoint pending,
            Instant endedAt)
    {
    }

    /** Connections to the database. */
    private final DataSource dataSource;

    /** The table, with its schema. */
    private final String table;

    /**
     * The table of the items that are exact copies (STAGES.md §15.5a),
     * with its schema.
     */
    private final String copies;

    /**
     * The table of when each item landed, from its summary row in the
     * landing table, with its schema: for measuring an item's time
     * through the stages from the registries (docs/PERFORMANCE.md §10).
     */
    private final String landed;

    /** Writes and verifies checkpoints. */
    private final CheckpointCodec codec;

    /** How long a statement may run, in seconds. */
    private final int queryTimeoutSeconds;

    /**
     * Creates the store.
     *
     * @param dataSource   connections to the database
     * @param schema       the schema its table is in
     * @param codec        writes and verifies checkpoints
     * @param queryTimeout how long a statement may run; at least a second
     * @throws IllegalArgumentException if the schema isn't a plain
     *                                  identifier
     */
    public ItemCheckpoints(DataSource dataSource, String schema,
            CheckpointCodec codec, Duration queryTimeout)
    {
        if (!IDENTIFIER.matcher(schema).matches())
        {
            throw new IllegalArgumentException(
                    "Invalid checkpoint schema name");
        }
        this.dataSource = dataSource;
        this.table = schema + ".split_item";
        this.copies = schema + ".split_copy";
        this.landed = schema + ".split_landed";
        this.codec = codec;
        this.queryTimeoutSeconds =
                (int) Math.max(1, queryTimeout.toSeconds());
    }

    /**
     * Creates the table if it isn't there.
     *
     * @throws IllegalStateException if it can't be created
     */
    public void createTableIfMissing()
    {
        String ddl = """
                CREATE TABLE IF NOT EXISTS %s (
                  item_id      uuid        PRIMARY KEY,
                  epoch        bigint      NOT NULL,
                  checkpoint   text        NOT NULL,
                  pending      text,
                  state        text        NOT NULL,
                  ended_at     timestamptz,
                  updated_at   timestamptz NOT NULL
                )""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(queryTimeoutSeconds);
            statement.execute("CREATE SCHEMA IF NOT EXISTS "
                    + table.substring(0, table.indexOf('.')));
            statement.execute(ddl);
            // Added 2026-11-09 (STAGES.md §15.6e): how many times the item's
            // end was sent as item.split, and whether the last end sent was
            // one, so a reopened item completes again under a new number.
            statement.execute("ALTER TABLE " + table
                    + " ADD COLUMN IF NOT EXISTS completion integer"
                    + " NOT NULL DEFAULT 0");
            statement.execute("ALTER TABLE " + table
                    + " ADD COLUMN IF NOT EXISTS completed boolean"
                    + " NOT NULL DEFAULT false");
            // Added 2026-11-09 (STAGES.md §15.8): the generation the
            // checkpoint is of, and the version cutting it, in plain text
            // for queries; a row from before is of the first generation.
            statement.execute("ALTER TABLE " + table
                    + " ADD COLUMN IF NOT EXISTS generation integer"
                    + " NOT NULL DEFAULT 1");
            statement.execute("ALTER TABLE " + table
                    + " ADD COLUMN IF NOT EXISTS version text");
            // Added 2026-11-09 (STAGES.md §15.8.10 step 5): when the
            // failures last counted in an end sent were read, so a pod
            // sends the end again once one changes (an operator's decline
            // or reopen). Rows from before take the time it was added, as
            // their ends were sent by the operator's command.
            statement.execute("ALTER TABLE " + table
                    + " ADD COLUMN IF NOT EXISTS announced_at timestamptz"
                    + " DEFAULT clock_timestamp()");
            statement.execute("ALTER TABLE " + table
                    + " ALTER COLUMN announced_at DROP DEFAULT");
            // Added 2026-11-10 (STAGES.md §15.5a): the earlier item each
            // copy is the same as, from its item.landed, so every end sent
            // for it names that item.
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      item_id  uuid        PRIMARY KEY,
                      same_as  uuid        NOT NULL,
                      noted_at timestamptz NOT NULL
                    )""".formatted(copies));
            // Added 2026-11-13 (docs/PERFORMANCE.md §10): when each item
            // landed, so its time to gold is a query on the registries.
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      item_id   uuid        PRIMARY KEY,
                      landed_at timestamptz NOT NULL
                    )""".formatted(landed));
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the checkpoint table", failure);
        }
    }

    /**
     * Numbers the completions of items whose end was sent before ends were
     * numbered (STAGES.md §15.6f): an ended item none of whose failures is
     * open in the fail store was sent as {@code item.split}, its first
     * completion, so it is numbered 1 and marked complete, as this build
     * would have done. An ended item with no failure in the store is one
     * from before the store, whose end was {@code item.split} too. Without
     * it, reopening such an item would send {@code item.progress} under a
     * number the stage after takes as stale.
     * <p>
     * A row this build wrote matches only if its end, complete, wasn't sent
     * yet (a stop between the two); sending it then gives the number this
     * gave it. Runs at every start, after the fail store's tables are made,
     * and changes nothing once done.
     *
     * @param failures the fail store's schema
     * @param stage    the stage's name in the fail store
     * @return how many items were numbered
     * @throws IllegalStateException if the database can't be reached
     */
    public int numberEarlierCompletions(String failures, String stage)
    {
        // Open in the states of now and of before revision (cn), as this
        // may run before the fail store's states are moved (§15.8.5).
        String sql = """
                UPDATE %s s SET completion = 1, completed = true
                 WHERE completion = 0 AND NOT completed
                   AND state IN ('COMPLETED', 'FAILED')
                   AND NOT EXISTS (
                       SELECT 1 FROM %s f
                        WHERE f.stage = ? AND f.item_id = s.item_id
                          AND f.state IN ('FAILED', 'REVISION_PENDING',
                                          'CORRECTION_PENDING',
                                          'FAILED_CORRECTION'))"""
                .formatted(table, FailureStore.table(failures));
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setString(1, stage);
            int numbered = update.executeUpdate();
            if (numbered > 0)
            {
                Telemetry.get().audit("split.completions_numbered",
                        Fields.of(Attr.COUNT, numbered));
            }
            return numbered;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not number the items' completions", failure);
        }
    }

    /**
     * Reads an item's checkpoint, verified.
     *
     * @param itemId the item
     * @return what is stored; empty if it was never started
     * @throws CheckpointTamperedException if a checkpoint doesn't verify,
     *                                     or is another item's
     * @throws IllegalStateException       if the database can't be reached
     */
    public Optional<Stored> load(UUID itemId)
    {
        String sql = "SELECT checkpoint, pending, ended_at FROM " + table
                + " WHERE item_id = ?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setObject(1, itemId);
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    return Optional.empty();
                }
                Checkpoint checkpoint = decode(rows.getString(1), itemId);
                String pending = rows.getString(2);
                Timestamp endedAt = rows.getTimestamp(3);
                return Optional.of(new Stored(checkpoint,
                        pending == null ? null : decode(pending, itemId),
                        endedAt == null ? null : endedAt.toInstant()));
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * Writes an item's checkpoint, dropping any pending one. The first
     * state after {@code SPLITTING} also notes when splitting ended. A
     * checkpoint of a later generation than the row's (STAGES.md §15.8)
     * starts the item's end again: when it ended, and its completions,
     * are of its generation.
     *
     * @param checkpoint the checkpoint
     * @param lease      the item's lease
     * @throws LeaseLostException    if the lease was lost or taken over
     * @throws IllegalStateException if the database can't be reached
     */
    public void save(Checkpoint checkpoint, LeaseToken lease)
    {
        Checkpoint.State state = checkpoint.state();
        boolean ended = state == Checkpoint.State.ARCHIVING
                || state == Checkpoint.State.FAILING
                || state == Checkpoint.State.COMPLETED
                || state == Checkpoint.State.FAILED
                || state == Checkpoint.State.INTERRUPTED;
        String sql = """
                INSERT INTO %1$s AS s
                       (item_id, epoch, checkpoint, pending, state, ended_at,
                        updated_at, generation, version)
                VALUES (?, ?, ?, NULL, ?,
                        CASE WHEN ? THEN clock_timestamp() END,
                        clock_timestamp(), ?, ?)
                ON CONFLICT (item_id) DO UPDATE
                   SET epoch = EXCLUDED.epoch,
                       checkpoint = EXCLUDED.checkpoint,
                       pending = NULL,
                       state = EXCLUDED.state,
                       ended_at = CASE
                           WHEN EXCLUDED.generation > s.generation
                                THEN EXCLUDED.ended_at
                           ELSE COALESCE(s.ended_at, EXCLUDED.ended_at) END,
                       completion = CASE
                           WHEN EXCLUDED.generation > s.generation THEN 0
                           ELSE s.completion END,
                       completed = s.completed
                           AND EXCLUDED.generation <= s.generation,
                       updated_at = EXCLUDED.updated_at,
                       generation = EXCLUDED.generation,
                       version = EXCLUDED.version
                 WHERE s.epoch <= EXCLUDED.epoch
                   AND s.generation <= EXCLUDED.generation"""
                .formatted(table);
        write(sql, checkpoint, lease, ended);
    }

    /**
     * Replaces an item's checkpoint while it is splitting, and leaves any
     * pending one as it is, for resume to settle.
     *
     * @param checkpoint the checkpoint, {@code SPLITTING}
     * @param lease      the item's lease
     * @throws IllegalArgumentException if it isn't {@code SPLITTING}
     * @throws LeaseLostException       if the lease was lost or taken over,
     *                                  or the item has no checkpoint
     * @throws IllegalStateException    if the database can't be reached
     */
    public void replace(Checkpoint checkpoint, LeaseToken lease)
    {
        if (checkpoint.state() != Checkpoint.State.SPLITTING)
        {
            throw new IllegalArgumentException("Only while splitting");
        }
        String sql = """
                UPDATE %s
                   SET epoch = ?, checkpoint = ?,
                       updated_at = clock_timestamp()
                 WHERE item_id = ? AND epoch <= ?
                   AND state = 'SPLITTING' AND generation = ?"""
                .formatted(table);
        lease.checkHeld();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setLong(1, lease.epoch());
            update.setString(2, codec.encode(checkpoint));
            update.setObject(3, checkpoint.itemId());
            update.setLong(4, lease.epoch());
            update.setInt(5, checkpoint.generation());
            if (update.executeUpdate() != 1)
            {
                throw new LeaseLostException(LeaseLostException.Reason.FENCED);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * Keeps the checkpoint after a batch as pending, before the batch is
     * appended. The item must have a checkpoint already.
     *
     * @param next  the checkpoint after the batch
     * @param lease the item's lease
     * @throws LeaseLostException    if the lease was lost or taken over
     * @throws IllegalStateException if the database can't be reached
     */
    public void prepare(Checkpoint next, LeaseToken lease)
    {
        String sql = """
                UPDATE %s
                   SET epoch = ?, pending = ?, updated_at = clock_timestamp()
                 WHERE item_id = ? AND epoch <= ? AND generation = ?"""
                .formatted(table);
        lease.checkHeld();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setLong(1, lease.epoch());
            update.setString(2, codec.encode(next));
            update.setObject(3, next.itemId());
            update.setLong(4, lease.epoch());
            update.setInt(5, next.generation());
            if (update.executeUpdate() != 1)
            {
                throw new LeaseLostException(LeaseLostException.Reason.FENCED);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * Tells which of some items have a checkpoint: they were started, by
     * this stage or a worker of it.
     *
     * @param itemIds the items
     * @return those with a row
     * @throws IllegalStateException if the database can't be reached
     */
    public Set<UUID> known(Collection<UUID> itemIds)
    {
        Set<UUID> found = new HashSet<>();
        if (itemIds.isEmpty())
        {
            return found;
        }
        String sql = "SELECT item_id FROM " + table
                + " WHERE item_id = ANY (?)";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            Array ids = connection.createArrayOf("uuid", itemIds.toArray());
            select.setArray(1, ids);
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
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * Lists the items left part way: in progress ({@code SPLITTING},
     * {@code ARCHIVING} or {@code FAILING}) with a checkpoint not written
     * since a time, as when the pod splitting one stopped and its message
     * was then lost or purged, so that nothing would take it on again. One
     * being split now may be among them if its batches are slow; its lease
     * keeps it to its pod. Oldest first.
     *
     * @param before the time a checkpoint was last written before
     * @return the items
     * @throws IllegalStateException if the database can't be reached
     */
    public List<UUID> stalled(Instant before)
    {
        String sql = "SELECT item_id FROM " + table
                + " WHERE state IN ('SPLITTING', 'ARCHIVING', 'FAILING')"
                + " AND updated_at < ? ORDER BY updated_at";
        List<UUID> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setTimestamp(1, Timestamp.from(before));
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
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * Notes the end about to be sent for an item, and numbers it (STAGES.md
     * §15.6e): an {@code item.split} after one that wasn't is the next
     * completion, and one after one that was is the same again; an
     * {@code item.progress} after an {@code item.split} reopens that
     * completion. Noted before the message is sent, so a resend after a
     * stop is numbered the same. Not fenced by the item's lease: the
     * number only grows, and an operator's command sends an item's end
     * too.
     *
     * @param itemId   the item
     * @param complete whether the end is {@code item.split}
     * @return for {@code item.split}, the completion's number, from 1; for
     *         {@code item.progress}, the completions sent before it
     * @throws IllegalStateException if the item has no row, or the
     *                               database can't be reached
     */
    public int announced(UUID itemId, boolean complete)
    {
        String sql = """
                UPDATE %s
                   SET completion = CASE WHEN ? AND NOT completed
                                         THEN completion + 1
                                         ELSE completion END,
                       completed = ?
                 WHERE item_id = ?
                RETURNING completion""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setBoolean(1, complete);
            update.setBoolean(2, complete);
            update.setObject(3, itemId);
            try (ResultSet rows = update.executeQuery())
            {
                if (!rows.next())
                {
                    throw new IllegalStateException("The item has no row");
                }
                return rows.getInt(1);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * The database's time now, to note with an end when its failures are
     * read ({@link #markAnnounced}): the fail store notes its changes by
     * the database's clock too.
     *
     * @return the time
     * @throws IllegalStateException if the database can't be reached
     */
    public Instant now()
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(
                        "SELECT clock_timestamp()"))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            try (ResultSet rows = select.executeQuery())
            {
                rows.next();
                return rows.getTimestamp(1).toInstant();
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * The items part way at the split stage (STAGES.md §14.4a), by the
     * version of their checkpoint: {@code SPLITTING}, {@code ARCHIVING} or
     * {@code FAILING}. A held item waits for an operator, and isn't counted.
     * Rows from before versions were kept name none, and are left out.
     *
     * @param first how many item IDs to give for each version
     * @return per version: the version, how many items and the first IDs
     * @throws IllegalStateException if the database can't be reached
     */
    public List<ByVersion> partWay(int first)
    {
        return byVersion("state IN ('SPLITTING', 'ARCHIVING', 'FAILING')",
                first);
    }

    /**
     * The items held across versions, by the version they were begun
     * under (STAGES.md §14.3), for the drain check's list of work waiting
     * on an operator.
     *
     * @param first how many item IDs to give for each version
     * @return per version: the version, how many items and the first IDs
     * @throws IllegalStateException if the database can't be reached
     */
    public List<ByVersion> held(int first)
    {
        return byVersion("state = 'HELD'", first);
    }

    /**
     * Items counted by version.
     *
     * @param version the version
     * @param count   how many items
     * @param first   the first of them, by ID
     */
    public record ByVersion(String version, long count, List<UUID> first)
    {
    }

    /**
     * Counts the items in some states by version.
     *
     * @param where which items, as SQL on the table's columns
     * @param first how many item IDs to give for each version
     * @return per version
     * @throws IllegalStateException if the database can't be reached
     */
    private List<ByVersion> byVersion(String where, int first)
    {
        String sql = """
                SELECT version, count(*),
                       (array_agg(item_id ORDER BY item_id))[1:?]
                  FROM %s
                 WHERE %s AND version IS NOT NULL
                 GROUP BY version ORDER BY version""".formatted(table, where);
        List<ByVersion> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setInt(1, first);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    List<UUID> ids = new ArrayList<>();
                    for (Object each
                            : (Object[]) rows.getArray(3).getArray())
                    {
                        ids.add((UUID) each);
                    }
                    found.add(new ByVersion(rows.getString(1),
                            rows.getLong(2), ids));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * Notes the earlier item an item is an exact copy of (STAGES.md
     * §15.5a), as its {@code item.landed} named it. The first noted stays.
     *
     * @param itemId the item
     * @param sameAs the earlier item
     * @throws IllegalStateException if the database can't be reached
     */
    public void noteSameAs(UUID itemId, UUID sameAs)
    {
        String sql = """
                INSERT INTO %s (item_id, same_as, noted_at)
                VALUES (?, ?, clock_timestamp())
                ON CONFLICT (item_id) DO NOTHING""".formatted(copies);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement insert = connection.prepareStatement(sql))
        {
            insert.setQueryTimeout(queryTimeoutSeconds);
            insert.setObject(1, itemId);
            insert.setObject(2, sameAs);
            insert.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * Notes when an item landed, from its summary row in the landing table
     * (docs/PERFORMANCE.md §10). The first noted stays: an item split
     * again, or reprocessed, landed when it first did.
     *
     * @param itemId   the item
     * @param landedAt when it landed
     * @throws IllegalStateException if the database can't be reached
     */
    public void noteLanded(UUID itemId, Instant landedAt)
    {
        String sql = """
                INSERT INTO %s (item_id, landed_at) VALUES (?, ?)
                ON CONFLICT (item_id) DO NOTHING""".formatted(landed);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement insert = connection.prepareStatement(sql))
        {
            insert.setQueryTimeout(queryTimeoutSeconds);
            insert.setObject(1, itemId);
            insert.setTimestamp(2, Timestamp.from(landedAt));
            insert.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * The earlier item an item is an exact copy of, as noted by
     * {@link #noteSameAs}.
     *
     * @param itemId the item
     * @return the earlier item; empty if none was noted
     * @throws IllegalStateException if the database can't be reached
     */
    public Optional<UUID> sameAs(UUID itemId)
    {
        String sql = "SELECT same_as FROM " + copies + " WHERE item_id = ?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setObject(1, itemId);
            try (ResultSet rows = select.executeQuery())
            {
                return rows.next()
                        ? Optional.of(rows.getObject(1, UUID.class))
                        : Optional.empty();
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * Notes that an item's end was sent, counting its failures as they
     * were when read: a failure changed after then has the end sent again
     * ({@link #changedEnds}). Never moves the time back.
     *
     * @param itemId the item
     * @param readAt when its failures were read, by {@link #now}
     * @throws IllegalStateException if the database can't be reached
     */
    public void markAnnounced(UUID itemId, Instant readAt)
    {
        String sql = """
                UPDATE %s
                   SET announced_at = GREATEST(announced_at, ?)
                 WHERE item_id = ?""".formatted(table);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setTimestamp(1, Timestamp.from(readAt));
            update.setObject(2, itemId);
            update.executeUpdate();
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * The items whose splitting has ended ({@code COMPLETED} or
     * {@code FAILED}) one of whose failures of the item's generation
     * changed since its end was last sent: an operator declined or
     * reopened it, or the end never went (STAGES.md §15.8.10 step 5). A
     * pod sends their ends again, as the convert stage's workers do.
     *
     * @param stage the stage, as the fail store names it
     * @param limit the most returned
     * @return their ids, longest waiting first
     * @throws IllegalStateException if the database can't be reached
     */
    public List<UUID> changedEnds(String stage, int limit)
    {
        String failures = FailureStore.table(
                table.substring(0, table.indexOf('.')));
        String sql = """
                SELECT s.item_id FROM %1$s s
                 WHERE s.state IN ('COMPLETED', 'FAILED')
                   AND EXISTS (SELECT 1 FROM %2$s f
                                WHERE f.stage = ? AND f.item_id = s.item_id
                                  AND f.generation = s.generation
                                  AND f.changed_at > COALESCE(
                                      s.announced_at, s.ended_at,
                                      '-infinity'::timestamptz))
                 ORDER BY COALESCE(s.announced_at, s.ended_at)
                 LIMIT ?""".formatted(table, failures);
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
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * Counts the items that have finished, {@code COMPLETED} or
     * {@code FAILED}, by any worker of this stage.
     *
     * @return how many there are
     * @throws IllegalStateException if the database can't be reached
     */
    public long countFinished()
    {
        String sql = "SELECT count(*) FROM " + table
                + " WHERE state IN ('COMPLETED', 'FAILED')";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            try (ResultSet rows = select.executeQuery())
            {
                rows.next();
                return rows.getLong(1);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * The generations of some items (STAGES.md §15.8), as their rows hold
     * them.
     *
     * @param itemIds the items
     * @return each one's generation; those never started are left out
     * @throws IllegalStateException if the database can't be reached
     */
    public Map<UUID, Integer> generations(Collection<UUID> itemIds)
    {
        Map<UUID, Integer> found = new HashMap<>();
        if (itemIds.isEmpty())
        {
            return found;
        }
        String sql = "SELECT item_id, generation FROM " + table
                + " WHERE item_id = ANY (?)";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setArray(1, connection.createArrayOf("uuid",
                    itemIds.toArray()));
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    found.put(rows.getObject(1, UUID.class), rows.getInt(2));
                }
            }
            return found;
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * The items whose current generation was begun under a version
     * (STAGES.md §15.8.4), oldest change first. An item whose row hasn't
     * been written since versions were kept in plain text isn't found.
     *
     * @param version the version
     * @param itemIds narrows it to these items; empty for any
     * @param limit   the most returned
     * @return the items
     * @throws IllegalStateException if the database can't be reached
     */
    public List<UUID> madeBy(String version, Collection<UUID> itemIds,
            int limit)
    {
        String sql = "SELECT item_id FROM " + table + " WHERE version = ?"
                + (itemIds.isEmpty() ? "" : " AND item_id = ANY (?)")
                + " ORDER BY updated_at, item_id LIMIT ?";
        List<UUID> found = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            int index = 1;
            select.setString(index++, version);
            if (!itemIds.isEmpty())
            {
                select.setArray(index++, connection.createArrayOf("uuid",
                        itemIds.toArray()));
            }
            select.setInt(index, limit);
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
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * Runs a fenced write of a checkpoint.
     *
     * @param sql        the statement: item, epoch, checkpoint, state,
     *                   ended
     * @param checkpoint the checkpoint
     * @param lease      the item's lease
     * @param ended      whether the item's splitting has ended
     * @throws LeaseLostException    if no row was written
     * @throws IllegalStateException if the database can't be reached
     */
    private void write(String sql, Checkpoint checkpoint, LeaseToken lease,
            boolean ended)
    {
        lease.checkHeld();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement upsert = connection.prepareStatement(sql))
        {
            upsert.setQueryTimeout(queryTimeoutSeconds);
            upsert.setObject(1, checkpoint.itemId());
            upsert.setLong(2, lease.epoch());
            upsert.setString(3, codec.encode(checkpoint));
            upsert.setString(4, checkpoint.state().name());
            upsert.setBoolean(5, ended);
            upsert.setInt(6, checkpoint.generation());
            upsert.setString(7, checkpoint.version());
            if (upsert.executeUpdate() != 1)
            {
                throw new LeaseLostException(LeaseLostException.Reason.FENCED);
            }
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException("Checkpoint store unavailable",
                    failure);
        }
    }

    /**
     * Reads a stored checkpoint, and checks it is the item's.
     *
     * @param text   the stored text
     * @param itemId the item whose row it is in
     * @return the checkpoint
     * @throws CheckpointTamperedException if it doesn't verify, or is
     *                                     another item's
     */
    private Checkpoint decode(String text, UUID itemId)
    {
        Checkpoint checkpoint = codec.decode(text);
        if (!checkpoint.itemId().equals(itemId))
        {
            throw new CheckpointTamperedException();
        }
        return checkpoint;
    }
}
