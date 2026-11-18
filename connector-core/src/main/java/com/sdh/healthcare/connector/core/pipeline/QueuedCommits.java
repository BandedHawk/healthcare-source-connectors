/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits every pod's groups of files to a table together, one pod at a
 * time, through the stage database (docs/PERFORMANCE.md §3.7). A pod's
 * group is written to a manifest of its own and queued as a row; the pod
 * then takes the table's lock row, and if no other pod has committed its
 * group meanwhile, commits every group waiting, from every pod, in one
 * fast append. Only the lock's holder commits, so the pods' commits never
 * conflict, and each takes all the work that waits.
 * <p>
 * What makes it exactly once:
 * <ul>
 * <li>A group is committed only while each of its item leases is current
 * in the lease table (same epoch, and not within the margin of running
 * out); otherwise it is refused and its pod told, as a lost lease.</li>
 * <li>Each commit names its groups in its snapshot's summary
 * ({@value #SUMMARY}). A pod that died after its commit but before
 * marking the rows leaves them waiting; the next holder of the lock finds
 * them named in a snapshot and marks them, rather than committing them
 * again.</li>
 * <li>When an item is leased again, {@link #settle} takes the lock and
 * refuses any group waiting under an older epoch of its lease, or marks it
 * if a snapshot names it. The new holder then checks the table, and
 * nothing of the old holder's is committed after.</li>
 * </ul>
 * One window stays, as it does for a pod committing alone: a lease can
 * run out between the check and the catalog's compare-and-swap, and the
 * margin covers it. A pod whose database connection drops while it goes on
 * to commit could commit a group a second holder of the lock commits too;
 * the catalog is in the same database, so its commit would fail with it.
 * <p>
 * The SQL is Postgres's, as the other stage tables' is (docs/PERFORMANCE.md
 * §8).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class QueuedCommits implements GroupAppender.Committer
{

    /** The snapshot summary property naming a commit's groups. */
    public static final String SUMMARY = "sdh.appends";

    /** The snapshot ID a queued manifest is encoded with: none yet. */
    static final long NO_SNAPSHOT = -1;

    /** The most groups one commit takes. */
    static final int MAX_GROUPS = 64;

    /** Commits tried before the groups are refused. */
    static final int ATTEMPTS = 10;

    /**
     * How far before a group was queued its commit's snapshot may seem to
     * be: the pods' clocks and the database's differ.
     */
    static final Duration SKEW = Duration.ofMinutes(5);

    /** How long settled rows are kept. */
    static final Duration KEPT = Duration.ofDays(1);

    /** How often a pod deletes settled rows. */
    static final Duration CLEAN_EVERY = Duration.ofMinutes(10);

    /** How long to wait before taking a turn again after a failure. */
    static final Duration BACKOFF = Duration.ofMillis(200);

    /** How long a pod may wait for the lock. */
    static final Duration LOCK_WAIT = Duration.ofMinutes(2);

    /** A table name: a plain identifier, or a schema and one. */
    private static final Pattern NAME = Pattern.compile(
            "[A-Za-z_][A-Za-z0-9_]{0,62}(\\.[A-Za-z_][A-Za-z0-9_]{0,62})?");

    /** Logs commits and settlements, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(QueuedCommits.class);

    /** A queued group's state. */
    enum State
    {
        /** Not yet committed or refused. */
        WAITING,

        /** In the table, in the snapshot noted. */
        COMMITTED,

        /** Not to be committed: a lease was lost, or the commit failed. */
        REFUSED
    }

    /**
     * A queued group, as its pod finds it.
     *
     * @param state      its state
     * @param snapshotId the snapshot holding it, if committed
     * @param reason     why it was refused, if it was
     */
    record Found(State state, long snapshotId, String reason)
    {
    }

    /**
     * A waiting group, as the lock's holder reads it.
     *
     * @param id       its ID
     * @param manifest its manifest, encoded
     * @param queuedAt when it was queued, by the database's clock
     */
    record Waiting(long id, byte[] manifest, Instant queuedAt)
    {
    }

    /** Connections to the stage database. */
    private final DataSource dataSource;

    /** The lock rows' table. */
    private final String locks;

    /** The queue's table. */
    private final String queue;

    /** The queued groups' leases' table. */
    private final String queueLeases;

    /** The lease table. */
    private final String leaseTable;

    /** How long before a lease runs out its files are no longer taken. */
    private final Duration margin;

    /** The least time between two commits of a table; zero for none. */
    private final Duration interval;

    /** How long a statement may run. */
    private final int queryTimeoutSeconds;

    /** The stage whose commits these are, for its timings. */
    private final String stage;

    /** How many of a pod's groups may be queued at once. */
    private final int groupsAtOnce;

    /** Makes group IDs. */
    private final SecureRandom random = new SecureRandom();

    /** The tables whose lock row is known to exist. */
    private final Set<String> locked = ConcurrentHashMap.newKeySet();

    /** When this pod last deleted settled rows, from System.nanoTime(). */
    private volatile long cleaned = System.nanoTime();

    /**
     * Creates the split stage's committer.
     *
     * @param dataSource   connections to the stage database, of their own:
     *                     a pod waiting for the lock holds one; closed with
     *                     the committer if it can be
     * @param schema       the schema its tables are in
     * @param leaseTable   the item leases' table
     * @param margin       how long before a lease runs out its files are no
     *                     longer committed
     * @param interval     the least time between two commits of a table;
     *                     zero for none
     * @param queryTimeout how long a statement may run; at least a second
     * @throws IllegalArgumentException if a name isn't a plain identifier
     */
    public QueuedCommits(DataSource dataSource, String schema,
            String leaseTable, Duration margin, Duration interval,
            Duration queryTimeout)
    {
        this(ItemProcessor.STAGE, dataSource, schema, leaseTable, margin,
                interval, queryTimeout);
    }

    /**
     * Creates a stage's committer. A stage that commits groups with no
     * leases, as the record stages do (their registry settles what was
     * stored, STAGES.md §6), has every group committed.
     *
     * @param stage        the stage, for its timings
     * @param dataSource   connections to the stage database, of their own:
     *                     a pod waiting for the lock holds one; closed with
     *                     the committer if it can be
     * @param schema       the schema its tables are in
     * @param leaseTable   the item leases' table
     * @param margin       how long before a lease runs out its files are no
     *                     longer committed
     * @param interval     the least time between two commits of a table;
     *                     zero for none
     * @param queryTimeout how long a statement may run; at least a second
     * @throws IllegalArgumentException if a name isn't a plain identifier
     */
    public QueuedCommits(String stage, DataSource dataSource, String schema,
            String leaseTable, Duration margin, Duration interval,
            Duration queryTimeout)
    {
        this(stage, dataSource, schema, leaseTable, margin, interval,
                queryTimeout, 1);
    }

    /**
     * Creates a stage's committer that takes several of a pod's groups at
     * once ({@link GroupAppender.Committer#groupsAtOnce}): each waits for a
     * turn beside the others, and a turn commits all that are waiting, so
     * a pod's next group needn't wait behind its last (docs/PERFORMANCE.md
     * §10, 2026-11-18).
     *
     * @param stage        the stage, for its timings
     * @param dataSource   connections to the stage database, of their own:
     *                     each group waiting for the lock holds one, so
     *                     at least {@code groupsAtOnce}; closed with the
     *                     committer if it can be
     * @param schema       the schema its tables are in
     * @param leaseTable   the item leases' table
     * @param margin       how long before a lease runs out its files are no
     *                     longer committed
     * @param interval     the least time between two commits of a table;
     *                     zero for none
     * @param queryTimeout how long a statement may run; at least a second
     * @param groupsAtOnce how many of a pod's groups may be queued at once
     * @throws IllegalArgumentException if a name isn't a plain identifier,
     *                                  or the count is under 1
     */
    public QueuedCommits(String stage, DataSource dataSource, String schema,
            String leaseTable, Duration margin, Duration interval,
            Duration queryTimeout, int groupsAtOnce)
    {
        if (groupsAtOnce < 1)
        {
            throw new IllegalArgumentException(
                    "A committer takes at least one group at once");
        }
        if (!NAME.matcher(schema).matches() || schema.contains(".")
                || !NAME.matcher(leaseTable).matches())
        {
            throw new IllegalArgumentException("Invalid table name");
        }
        this.stage = stage;
        this.groupsAtOnce = groupsAtOnce;
        this.dataSource = dataSource;
        this.locks = schema + ".append_lock";
        this.queue = schema + ".append_queue";
        this.queueLeases = schema + ".append_queue_lease";
        this.leaseTable = leaseTable;
        this.margin = margin;
        this.interval = interval;
        this.queryTimeoutSeconds =
                (int) Math.max(1, queryTimeout.toSeconds());
    }

    /**
     * {@inheritDoc}
     *
     * @return the count given when it was made; 1 unless given
     */
    @Override
    public int groupsAtOnce()
    {
        return groupsAtOnce;
    }

    /**
     * Creates the tables if they aren't there.
     *
     * @throws IllegalStateException if they can't be created
     */
    public void createTablesIfMissing()
    {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.setQueryTimeout(queryTimeoutSeconds);
            statement.execute("CREATE SCHEMA IF NOT EXISTS "
                    + queue.substring(0, queue.indexOf('.')));
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      table_name     text PRIMARY KEY,
                      last_commit_at timestamptz
                    )""".formatted(locks));
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      id          bigint      PRIMARY KEY,
                      table_name  text        NOT NULL,
                      manifest    bytea       NOT NULL,
                      state       text        NOT NULL,
                      snapshot_id bigint,
                      reason      text,
                      queued_at   timestamptz NOT NULL,
                      updated_at  timestamptz NOT NULL
                    )""".formatted(queue));
            statement.execute("CREATE INDEX IF NOT EXISTS "
                    + "append_queue_waiting ON " + queue
                    + " (table_name, state)");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                      append_id  bigint NOT NULL
                                 REFERENCES %s (id) ON DELETE CASCADE,
                      lease_name text   NOT NULL,
                      epoch      bigint NOT NULL,
                      PRIMARY KEY (append_id, lease_name)
                    )""".formatted(queueLeases, queue));
            statement.execute("CREATE INDEX IF NOT EXISTS "
                    + "append_queue_lease_name ON " + queueLeases
                    + " (lease_name)");
        }
        catch (SQLException failure)
        {
            throw new IllegalStateException(
                    "Could not create the append queue's tables", failure);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Writes the files to a manifest, queues it, and waits for its turn:
     * then commits every group waiting, or finds its own committed or
     * refused by another pod's turn.
     *
     * @throws IllegalArgumentException if a lease has no name, or the files
     *                                  have more than one partition spec
     * @throws CommitFailedException    if the commit that took the group
     *                                  failed; nothing of it is committed
     */
    @Override
    public Snapshot commit(Table table, List<DataFile> files,
            List<LeaseToken> leases)
    {
        Telemetry telemetry = Telemetry.get();
        long started = System.nanoTime();
        Map<String, Long> epochs = new LinkedHashMap<>();
        for (LeaseToken lease : leases)
        {
            if (lease.name() == null)
            {
                throw new IllegalArgumentException(
                        "A queued commit needs leases kept in the store");
            }
            epochs.put(lease.name(), lease.epoch());
        }
        ManifestFile manifest = writeManifest(table, files);
        long id = random.nextLong() & Long.MAX_VALUE;
        try
        {
            enqueue(table.name(), id, manifest, epochs);
        }
        catch (SQLException failure)
        {
            delete(table, manifest);
            throw new IllegalStateException("Could not queue a commit",
                    failure);
        }
        telemetry.phaseDuration(stage, "commit_manifest",
                System.nanoTime() - started);
        Found found = awaitTurns(table, id);
        if (found.state() == State.REFUSED)
        {
            delete(table, manifest);
            if ("lease".equals(found.reason()))
            {
                throw new LeaseLostException(
                        LeaseLostException.Reason.FENCED);
            }
            throw new CommitFailedException(
                    "The commit that took this group failed");
        }
        return snapshot(table, found.snapshotId());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Takes the table's lock, so that no commit is under way, then settles
     * the item's groups queued under an older epoch: committed if a
     * snapshot names them, refused if not.
     *
     * @throws IllegalStateException if the database can't be reached
     */
    @Override
    public void settle(Table table, LeaseToken lease)
    {
        if (lease.name() == null)
        {
            return;
        }
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                lock(connection, table.name());
                List<Waiting> older = olderGroups(connection, table.name(),
                        lease.name(), lease.epoch());
                if (!older.isEmpty())
                {
                    table.refresh();
                    Map<Long, Long> done = named(table, older);
                    commitWaiting(connection, done);
                    Set<Long> refused = new HashSet<>();
                    for (Waiting group : older)
                    {
                        if (!done.containsKey(group.id()))
                        {
                            refused.add(group.id());
                        }
                    }
                    refuse(connection, refused, "lease");
                    LOG.info("append.settled committed={} refused={}",
                            done.size(), refused.size());
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
                    "Could not settle an item's queued commits", failure);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Closes the connections' pool, if it can be closed.
     */
    @Override
    public void close()
    {
        if (dataSource instanceof AutoCloseable pool)
        {
            try
            {
                pool.close();
            }
            catch (Exception failure)
            {
                LOG.debug("append.pool_not_closed error={}",
                        failure.getClass().getSimpleName());
            }
        }
    }

    /**
     * Takes turns until the group is committed or refused.
     *
     * @param table the table
     * @param id    the group
     * @return how it ended
     * @throws IllegalStateException if interrupted while waiting; the group
     *                               may be committed all the same, and is
     *                               settled when its item is leased again
     */
    private Found awaitTurns(Table table, long id)
    {
        while (true)
        {
            if (Thread.currentThread().isInterrupted())
            {
                throw new IllegalStateException(
                        "Interrupted waiting for a queued commit");
            }
            try
            {
                Found found = turn(table, id);
                if (found.state() != State.WAITING)
                {
                    return found;
                }
                // Still waiting: more groups than one commit takes, or an
                // outcome unknown that the next turn settles.
                continue;
            }
            catch (SQLException failure)
            {
                LOG.warn("append.turn_failed sqlState={}",
                        failure.getSQLState());
            }
            try
            {
                Thread.sleep(BACKOFF);
            }
            catch (InterruptedException interrupted)
            {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted waiting for a queued commit");
            }
        }
    }

    /**
     * One turn: takes the table's lock; if the group is still waiting,
     * commits every group waiting.
     *
     * @param table the table
     * @param id    the group
     * @return the group as it is after the turn; still waiting if the
     *         commit's outcome is unknown
     * @throws SQLException if the database fails
     */
    private Found turn(Table table, long id) throws SQLException
    {
        Telemetry telemetry = Telemetry.get();
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                long waited = System.nanoTime();
                Long sinceLast = lock(connection, table.name());
                telemetry.phaseDuration(stage, "commit_turn",
                        System.nanoTime() - waited);
                Found found = find(connection, id);
                if (found.state() == State.WAITING)
                {
                    long combined = System.nanoTime();
                    combine(connection, table, sinceLast);
                    telemetry.phaseDuration(stage,
                            "commit_combine", System.nanoTime() - combined);
                    found = find(connection, id);
                }
                cleanIfDue(connection, table.name());
                connection.commit();
                return found;
            }
            catch (SQLException | RuntimeException failure)
            {
                connection.rollback();
                throw failure;
            }
        }
    }

    /**
     * Commits every group waiting for the table, as the lock's holder:
     * first marking those a snapshot already names, and refusing those
     * whose leases aren't current.
     *
     * @param connection the connection holding the lock
     * @param table      the table
     * @param sinceLast  milliseconds since the table's last commit; null if
     *                   none
     * @throws SQLException if the database fails
     */
    private void combine(Connection connection, Table table, Long sinceLast)
            throws SQLException
    {
        if (!interval.isZero() && sinceLast != null
                && sinceLast < interval.toMillis())
        {
            try
            {
                Thread.sleep(interval.toMillis() - sinceLast);
            }
            catch (InterruptedException interrupted)
            {
                Thread.currentThread().interrupt();
                return;
            }
        }
        List<Waiting> groups = waiting(connection, table.name());
        if (groups.isEmpty())
        {
            return;
        }
        table.refresh();
        Map<Long, Long> done = named(table, groups);
        if (!done.isEmpty())
        {
            LOG.warn("append.found_committed groups={}", done.size());
            commitWaiting(connection, done);
        }
        List<Long> open = new ArrayList<>();
        for (Waiting group : groups)
        {
            if (!done.containsKey(group.id()))
            {
                open.add(group.id());
            }
        }
        Set<Long> lost = lostLeases(connection, open);
        refuse(connection, lost, "lease");
        open.removeAll(lost);
        if (open.isEmpty())
        {
            return;
        }
        Map<Long, byte[]> manifests = new HashMap<>();
        groups.forEach(group -> manifests.put(group.id(), group.manifest()));
        Snapshot snapshot;
        try
        {
            snapshot = append(table, open, manifests);
        }
        catch (CommitStateUnknownException unknown)
        {
            // Left waiting: the next turn finds them named, or commits them.
            LOG.warn("append.state_unknown groups={}", open.size());
            return;
        }
        catch (RuntimeException failure)
        {
            LOG.warn("append.failed groups={} error={}", open.size(),
                    failure.getClass().getSimpleName());
            refuse(connection, new HashSet<>(open), "failed");
            return;
        }
        Map<Long, Long> committed = new HashMap<>();
        open.forEach(id -> committed.put(id, snapshot.snapshotId()));
        commitWaiting(connection, committed);
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE " + locks + " SET last_commit_at = clock_timestamp()"
                        + " WHERE table_name = ?"))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            update.setString(1, table.name());
            update.executeUpdate();
        }
        LOG.debug("append.combined groups={}", open.size());
    }

    /**
     * Appends groups' manifests in one fast append, naming them in the
     * snapshot's summary, and finds the snapshot that does.
     *
     * @param table     the table
     * @param ids       the groups
     * @param manifests each group's manifest, encoded
     * @return the snapshot
     * @throws CommitFailedException       if every attempt conflicts
     * @throws CommitStateUnknownException if the outcome is unknown
     */
    private Snapshot append(Table table, List<Long> ids,
            Map<Long, byte[]> manifests)
    {
        String names = ids.stream().map(id -> Long.toString(id, 36))
                .collect(Collectors.joining(","));
        for (int attempt = 1; ; attempt++)
        {
            AppendFiles append = table.newFastAppend();
            for (long id : ids)
            {
                append.appendManifest(decode(manifests.get(id)));
            }
            append.set(SUMMARY, names);
            try
            {
                append.commit();
                break;
            }
            catch (CommitFailedException conflict)
            {
                LOG.warn("append.conflict attempt={} table={}", attempt,
                        table.name());
                if (attempt >= ATTEMPTS)
                {
                    throw conflict;
                }
            }
        }
        long first = ids.get(0);
        for (Snapshot snapshot = table.currentSnapshot(); snapshot != null;
                snapshot = snapshot.parentId() == null ? null
                        : table.snapshot(snapshot.parentId()))
        {
            if (names(snapshot).contains(first))
            {
                return snapshot;
            }
        }
        throw new IllegalStateException(
                "The commit's snapshot isn't in the table");
    }

    /**
     * The groups a snapshot names.
     *
     * @param snapshot the snapshot
     * @return their IDs
     */
    static Set<Long> names(Snapshot snapshot)
    {
        String names = snapshot.summary() == null ? null
                : snapshot.summary().get(SUMMARY);
        if (names == null || names.isEmpty())
        {
            return Set.of();
        }
        Set<Long> ids = new HashSet<>();
        for (String name : names.split(","))
        {
            ids.add(Long.parseLong(name, 36));
        }
        return ids;
    }

    /**
     * Which of some groups a snapshot made since they were queued names.
     *
     * @param table  the table, refreshed
     * @param groups the groups
     * @return each named group's ID, with the snapshot naming it
     */
    static Map<Long, Long> named(Table table, Collection<Waiting> groups)
    {
        Map<Long, Long> found = new HashMap<>();
        if (groups.isEmpty())
        {
            return found;
        }
        Set<Long> ids = new HashSet<>();
        Instant earliest = Instant.MAX;
        for (Waiting group : groups)
        {
            ids.add(group.id());
            if (group.queuedAt().isBefore(earliest))
            {
                earliest = group.queuedAt();
            }
        }
        long since = earliest.minus(SKEW).toEpochMilli();
        for (Snapshot snapshot : table.snapshots())
        {
            if (snapshot.timestampMillis() < since)
            {
                continue;
            }
            for (long id : names(snapshot))
            {
                if (ids.contains(id))
                {
                    found.put(id, snapshot.snapshotId());
                }
            }
        }
        return found;
    }

    /**
     * Writes a group's files to a manifest of their own, with no snapshot
     * ID: the commit's ID and sequence number are inherited (format 2).
     *
     * @param table the table
     * @param files the files, of one partition spec
     * @return the manifest
     * @throws IllegalArgumentException if the files have more than one spec
     * @throws UncheckedIOException     if it can't be written
     */
    static ManifestFile writeManifest(Table table, List<DataFile> files)
    {
        int specId = files.get(0).specId();
        for (DataFile file : files)
        {
            if (file.specId() != specId)
            {
                throw new IllegalArgumentException(
                        "A group's files must have one partition spec");
            }
        }
        TableOperations operations =
                ((HasTableOperations) table).operations();
        PartitionSpec spec = table.specs().get(specId);
        OutputFile out = table.io().newOutputFile(
                operations.metadataFileLocation(FileFormat.AVRO.addExtension(
                        UUID.randomUUID() + "-m0")));
        ManifestWriter<DataFile> writer = ManifestFiles.write(
                operations.current().formatVersion(), spec, out, null);
        try (writer)
        {
            files.forEach(writer::add);
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException("Could not write a manifest",
                    failure);
        }
        return writer.toManifestFile();
    }

    /**
     * Encodes a manifest to queue. Iceberg's encoding needs a snapshot ID,
     * which a manifest waiting for its commit hasn't: it is encoded with
     * {@value #NO_SNAPSHOT}, and {@link #decode} takes that off again.
     *
     * @param manifest the manifest, with no snapshot ID
     * @return its bytes
     * @throws UncheckedIOException if it can't be encoded
     */
    static byte[] encode(ManifestFile manifest)
    {
        try
        {
            return ManifestFiles.encode(GenericManifestFile.copyOf(manifest)
                    .withSnapshotId(NO_SNAPSHOT).build());
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException("Could not encode a manifest",
                    failure);
        }
    }

    /**
     * Decodes a queued manifest, with no snapshot ID, so that the commit
     * that takes it gives it its own without copying it (format 2).
     *
     * @param encoded its bytes
     * @return the manifest
     * @throws UncheckedIOException if the bytes aren't a manifest
     */
    static ManifestFile decode(byte[] encoded)
    {
        try
        {
            return GenericManifestFile.copyOf(ManifestFiles.decode(encoded))
                    .withSnapshotId(null).build();
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException("A queued manifest is unreadable",
                    failure);
        }
    }

    /**
     * Deletes a refused group's manifest, if it can.
     *
     * @param table    the table
     * @param manifest the manifest
     */
    private static void delete(Table table, ManifestFile manifest)
    {
        try
        {
            table.io().deleteFile(manifest.path());
        }
        catch (RuntimeException failure)
        {
            LOG.debug("append.manifest_not_deleted error={}",
                    failure.getClass().getSimpleName());
        }
    }

    /**
     * The snapshot of a given ID, reading the table again if this pod
     * hasn't seen it.
     *
     * @param table the table
     * @param id    the snapshot's ID
     * @return it
     * @throws IllegalStateException if the table hasn't it
     */
    private static Snapshot snapshot(Table table, long id)
    {
        Snapshot snapshot = table.snapshot(id);
        if (snapshot == null)
        {
            table.refresh();
            snapshot = table.snapshot(id);
        }
        if (snapshot == null)
        {
            throw new IllegalStateException(
                    "A queued commit's snapshot isn't in the table");
        }
        return snapshot;
    }

    /**
     * Queues a group, with its leases, in one transaction.
     *
     * @param tableName the table
     * @param id        the group's ID
     * @param manifest  its manifest
     * @param epochs    each lease's name and epoch
     * @throws SQLException if the database fails
     */
    private void enqueue(String tableName, long id, ManifestFile manifest,
            Map<String, Long> epochs) throws SQLException
    {
        byte[] encoded = encode(manifest);
        try (Connection connection = dataSource.getConnection())
        {
            connection.setAutoCommit(false);
            try
            {
                if (!locked.contains(tableName))
                {
                    try (PreparedStatement insert =
                            connection.prepareStatement("INSERT INTO "
                                    + locks + " (table_name) VALUES (?)"
                                    + " ON CONFLICT (table_name) DO NOTHING"))
                    {
                        insert.setQueryTimeout(queryTimeoutSeconds);
                        insert.setString(1, tableName);
                        insert.executeUpdate();
                    }
                }
                try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + queue + " (id, table_name, manifest,"
                                + " state, queued_at, updated_at)"
                                + " VALUES (?, ?, ?, 'WAITING',"
                                + " clock_timestamp(), clock_timestamp())"))
                {
                    insert.setQueryTimeout(queryTimeoutSeconds);
                    insert.setLong(1, id);
                    insert.setString(2, tableName);
                    insert.setBytes(3, encoded);
                    insert.executeUpdate();
                }
                try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + queueLeases
                                + " (append_id, lease_name, epoch)"
                                + " VALUES (?, ?, ?)"))
                {
                    insert.setQueryTimeout(queryTimeoutSeconds);
                    for (Map.Entry<String, Long> lease : epochs.entrySet())
                    {
                        insert.setLong(1, id);
                        insert.setString(2, lease.getKey());
                        insert.setLong(3, lease.getValue());
                        insert.addBatch();
                    }
                    insert.executeBatch();
                }
                connection.commit();
                locked.add(tableName);
            }
            catch (SQLException | RuntimeException failure)
            {
                connection.rollback();
                throw failure;
            }
        }
    }

    /**
     * Takes the table's lock row, waiting while another pod holds it.
     *
     * @param connection a connection in a transaction
     * @param tableName  the table
     * @return milliseconds since the table's last commit; null if none
     * @throws SQLException if the database fails, or the wait is too long
     */
    private Long lock(Connection connection, String tableName)
            throws SQLException
    {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT CAST(EXTRACT(EPOCH FROM (clock_timestamp()"
                        + " - last_commit_at)) * 1000 AS bigint)"
                        + " FROM " + locks + " WHERE table_name = ?"
                        + " FOR UPDATE"))
        {
            select.setQueryTimeout((int) LOCK_WAIT.toSeconds());
            select.setString(1, tableName);
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    throw new SQLException("No lock row for the table");
                }
                long since = rows.getLong(1);
                return rows.wasNull() ? null : since;
            }
        }
    }

    /**
     * A group's state.
     *
     * @param connection a connection
     * @param id         the group
     * @return its state
     * @throws SQLException if the database fails, or the group is gone
     */
    private Found find(Connection connection, long id) throws SQLException
    {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT state, snapshot_id, reason FROM " + queue
                        + " WHERE id = ?"))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setLong(1, id);
            try (ResultSet rows = select.executeQuery())
            {
                if (!rows.next())
                {
                    throw new SQLException("A queued group is gone");
                }
                return new Found(State.valueOf(rows.getString(1)),
                        rows.getLong(2), rows.getString(3));
            }
        }
    }

    /**
     * The groups waiting for a table, oldest first.
     *
     * @param connection a connection holding the lock
     * @param tableName  the table
     * @return them, at most {@link #MAX_GROUPS}
     * @throws SQLException if the database fails
     */
    private List<Waiting> waiting(Connection connection, String tableName)
            throws SQLException
    {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT id, manifest, queued_at FROM " + queue
                        + " WHERE table_name = ? AND state = 'WAITING'"
                        + " ORDER BY queued_at LIMIT " + MAX_GROUPS))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, tableName);
            return read(select);
        }
    }

    /**
     * An item's groups waiting under older epochs of its lease.
     *
     * @param connection a connection holding the lock
     * @param tableName  the table
     * @param leaseName  the item's lease
     * @param epoch      the lease's epoch now
     * @return the groups
     * @throws SQLException if the database fails
     */
    private List<Waiting> olderGroups(Connection connection,
            String tableName, String leaseName, long epoch)
            throws SQLException
    {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT q.id, q.manifest, q.queued_at FROM " + queue + " q"
                        + " JOIN " + queueLeases + " l ON l.append_id = q.id"
                        + " WHERE q.table_name = ? AND q.state = 'WAITING'"
                        + " AND l.lease_name = ? AND l.epoch < ?"))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setString(1, tableName);
            select.setString(2, leaseName);
            select.setLong(3, epoch);
            return read(select);
        }
    }

    /**
     * Reads groups.
     *
     * @param select a query of id, manifest and queued_at
     * @return the groups
     * @throws SQLException if the database fails
     */
    private static List<Waiting> read(PreparedStatement select)
            throws SQLException
    {
        List<Waiting> groups = new ArrayList<>();
        try (ResultSet rows = select.executeQuery())
        {
            while (rows.next())
            {
                groups.add(new Waiting(rows.getLong(1), rows.getBytes(2),
                        rows.getTimestamp(3).toInstant()));
            }
        }
        return groups;
    }

    /**
     * Which of some groups have a lease that isn't current: a newer epoch,
     * gone, or running out within the margin.
     *
     * @param connection a connection
     * @param ids        the groups
     * @return their IDs
     * @throws SQLException if the database fails
     */
    private Set<Long> lostLeases(Connection connection, List<Long> ids)
            throws SQLException
    {
        Set<Long> lost = new HashSet<>();
        if (ids.isEmpty())
        {
            return lost;
        }
        String in = ids.stream().map(id -> "?")
                .collect(Collectors.joining(","));
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT DISTINCT q.append_id FROM " + queueLeases + " q"
                        + " LEFT JOIN " + leaseTable + " l"
                        + " ON l.name = q.lease_name AND l.epoch = q.epoch"
                        + " AND l.expires_at > clock_timestamp()"
                        + " + ? * interval '1 millisecond'"
                        + " WHERE q.append_id IN (" + in + ")"
                        + " AND l.name IS NULL"))
        {
            select.setQueryTimeout(queryTimeoutSeconds);
            select.setLong(1, margin.toMillis());
            for (int index = 0; index < ids.size(); index++)
            {
                select.setLong(index + 2, ids.get(index));
            }
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    lost.add(rows.getLong(1));
                }
            }
        }
        return lost;
    }

    /**
     * Marks groups committed.
     *
     * @param connection a connection holding the lock
     * @param snapshots  each group's ID, with the snapshot holding it
     * @throws SQLException if the database fails
     */
    private void commitWaiting(Connection connection,
            Map<Long, Long> snapshots) throws SQLException
    {
        if (snapshots.isEmpty())
        {
            return;
        }
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE " + queue + " SET state = 'COMMITTED',"
                        + " snapshot_id = ?, updated_at = clock_timestamp()"
                        + " WHERE id = ?"))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            for (Map.Entry<Long, Long> group : snapshots.entrySet())
            {
                update.setLong(1, group.getValue());
                update.setLong(2, group.getKey());
                update.addBatch();
            }
            update.executeBatch();
        }
    }

    /**
     * Marks groups refused.
     *
     * @param connection a connection holding the lock
     * @param ids        the groups
     * @param reason     {@code lease} or {@code failed}
     * @throws SQLException if the database fails
     */
    private void refuse(Connection connection, Set<Long> ids, String reason)
            throws SQLException
    {
        if (ids.isEmpty())
        {
            return;
        }
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE " + queue + " SET state = 'REFUSED', reason = ?,"
                        + " updated_at = clock_timestamp() WHERE id = ?"))
        {
            update.setQueryTimeout(queryTimeoutSeconds);
            for (long id : ids)
            {
                update.setString(1, reason);
                update.setLong(2, id);
                update.addBatch();
            }
            update.executeBatch();
        }
    }

    /**
     * Deletes the table's settled rows older than {@link #KEPT}, every
     * {@link #CLEAN_EVERY} on this pod.
     *
     * @param connection a connection holding the lock
     * @param tableName  the table
     * @throws SQLException if the database fails
     */
    private void cleanIfDue(Connection connection, String tableName)
            throws SQLException
    {
        long now = System.nanoTime();
        if (now - cleaned < CLEAN_EVERY.toNanos())
        {
            return;
        }
        cleaned = now;
        try (PreparedStatement delete = connection.prepareStatement(
                "DELETE FROM " + queue + " WHERE table_name = ?"
                        + " AND state <> 'WAITING' AND updated_at"
                        + " < clock_timestamp() - ? * interval '1 second'"))
        {
            delete.setQueryTimeout(queryTimeoutSeconds);
            delete.setString(1, tableName);
            delete.setLong(2, KEPT.toSeconds());
            delete.executeUpdate();
        }
    }
}
