/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.TestCatalogs;
import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.LeaseGrant;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link QueuedCommits}: two pods' appenders on one table, on
 * embedded Postgres and a JDBC catalog. Each test holds the table's lock
 * itself while pods queue, so it decides what happens before their turns.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class QueuedCommitsTest
{

    /** The database. */
    DataSource database;

    /** The schema the queue's tables are in. */
    String schema;

    /** The lease table. */
    String leaseTable;

    /** The leases. */
    JdbcLeaseStore leases;

    /** The catalog. */
    Catalog catalog;

    /** The table, as the first pod loaded it. */
    Table table;

    /** The table, as the second pod loaded it. */
    Table other;

    /** Runs the pods' senders. */
    ExecutorService senders;

    /** Creates the tests; JUnit runs each test on a new instance. */
    QueuedCommitsTest()
    {
    }

    /**
     * A lease as the store keeps it.
     *
     * @param name  its name
     * @param epoch its epoch
     */
    record Held(String name, long epoch) implements LeaseToken
    {

        /**
         * Holds a grant.
         *
         * @param grant the grant
         */
        Held(LeaseGrant grant)
        {
            this(grant.leaseName(), grant.epoch());
        }

        @Override
        public void checkHeld()
        {
        }
    }

    /** Creates the tables, and the two pods' copies of the table. */
    @BeforeEach
    void setUp()
    {
        database = TestPostgres.dataSource();
        String suffix = UUID.randomUUID().toString().replace("-", "")
                .substring(0, 12);
        schema = "q_" + suffix;
        leaseTable = "l_" + suffix;
        leases = new JdbcLeaseStore(database, leaseTable,
                Duration.ofSeconds(10));
        leases.createTableIfMissing();
        catalog = TestCatalogs.create(TestCatalogs.Kind.JDBC).newClient();
        table = IngestTables.splitRecords(catalog, "ns_" + suffix);
        other = catalog.loadTable(TableIdentifier.of("ns_" + suffix,
                IngestTables.SPLIT_RECORDS));
        senders = Executors.newVirtualThreadPerTaskExecutor();
    }

    /** Stops the senders. */
    @AfterEach
    void tearDown()
    {
        senders.shutdownNow();
    }

    /**
     * A pod's committer.
     *
     * @return it
     */
    QueuedCommits committer()
    {
        QueuedCommits commits = new QueuedCommits(database, schema,
                leaseTable, Duration.ofSeconds(1), Duration.ZERO,
                Duration.ofSeconds(10));
        commits.createTablesIfMissing();
        return commits;
    }

    /**
     * A lease on an item, from the store.
     *
     * @return it
     * @throws Exception if it can't be had
     */
    Held lease() throws Exception
    {
        return new Held(leases.acquire("split:" + UUID.randomUUID(), "pod",
                Duration.ofMinutes(5)));
    }

    /**
     * A data file to append, that needn't exist.
     *
     * @param name its name
     * @return the file
     */
    DataFile file(String name)
    {
        return DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/" + name + ".parquet")
                .withFormat(FileFormat.PARQUET).withFileSizeInBytes(10)
                .withRecordCount(1).build();
    }

    /**
     * The data files the table holds.
     *
     * @return their paths
     * @throws Exception if the table can't be read
     */
    Set<String> files() throws Exception
    {
        table.refresh();
        try (CloseableIterable<FileScanTask> tasks = table.newScan()
                .planFiles())
        {
            List<String> paths = new ArrayList<>();
            tasks.forEach(task -> paths.add(task.file().location()));
            assertThat(paths).doesNotHaveDuplicates();
            return paths.stream().collect(Collectors.toSet());
        }
    }

    /**
     * Takes the table's lock as a pod would, in a transaction the caller
     * ends.
     *
     * @return the connection holding it
     * @throws SQLException if the database fails
     */
    Connection holdLock() throws SQLException
    {
        Connection connection = database.getConnection();
        connection.setAutoCommit(false);
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + schema + ".append_lock (table_name)"
                        + " VALUES (?) ON CONFLICT DO NOTHING"))
        {
            insert.setString(1, table.name());
            insert.executeUpdate();
        }
        connection.commit();
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT 1 FROM " + schema + ".append_lock"
                        + " WHERE table_name = ? FOR UPDATE"))
        {
            select.setString(1, table.name());
            select.executeQuery().close();
        }
        return connection;
    }

    /**
     * Waits until a number of groups are queued.
     *
     * @param count how many
     * @throws Exception if they aren't, in ten seconds
     */
    void awaitQueued(int count) throws Exception
    {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < until)
        {
            try (Connection connection = database.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet rows = statement.executeQuery(
                            "SELECT count(*) FROM " + schema
                                    + ".append_queue"))
            {
                rows.next();
                if (rows.getInt(1) >= count)
                {
                    return;
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Not queued: " + count);
    }

    /**
     * Groups queued by two pods while the lock is held go in one commit,
     * whichever pod takes the turn, and each pod is told its snapshot.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void twoPodsGroupsGoInOneCommit() throws Exception
    {
        try (GroupAppender one = new GroupAppender(table, committer());
                GroupAppender two = new GroupAppender(other, committer()))
        {
            Held first = lease();
            Held second = lease();
            Connection lock = holdLock();
            Future<Snapshot> a = senders.submit(
                    () -> one.append(file("a"), first));
            Future<Snapshot> b = senders.submit(
                    () -> two.append(file("b"), second));
            awaitQueued(2);
            lock.commit();
            lock.close();
            long snapshot = a.get(30, TimeUnit.SECONDS).snapshotId();
            assertThat(b.get(30, TimeUnit.SECONDS).snapshotId())
                    .isEqualTo(snapshot);
            assertThat(files()).hasSize(2);
            assertThat(QueuedCommits.names(table.snapshot(snapshot)))
                    .hasSize(2);
        }
    }

    /**
     * Many files from two pods at once are all committed, once each.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void manyFilesFromTwoPodsAllCommitOnce() throws Exception
    {
        int each = 30;
        try (GroupAppender one = new GroupAppender(table, committer());
                GroupAppender two = new GroupAppender(other, committer()))
        {
            List<Future<Snapshot>> sent = new ArrayList<>();
            for (int index = 0; index < each; index++)
            {
                Held first = lease();
                Held second = lease();
                String name = "file-" + index;
                sent.add(senders.submit(
                        () -> one.append(file("one-" + name), first)));
                sent.add(senders.submit(
                        () -> two.append(file("two-" + name), second)));
            }
            for (Future<Snapshot> future : sent)
            {
                assertThat(future.get(60, TimeUnit.SECONDS)).isNotNull();
            }
        }
        assertThat(files()).hasSize(2 * each);
    }

    /**
     * A group whose lease isn't current in the store is refused and its
     * pod told, as for a lost lease; the other pod's group goes in.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aGroupWithoutItsLeaseIsRefused() throws Exception
    {
        try (GroupAppender one = new GroupAppender(table, committer());
                GroupAppender two = new GroupAppender(other, committer()))
        {
            Held kept = lease();
            Held stale = new Held(lease().name(), 7);
            Connection lock = holdLock();
            Future<Snapshot> a = senders.submit(
                    () -> one.append(file("kept"), kept));
            Future<Snapshot> b = senders.submit(
                    () -> two.append(file("stale"), stale));
            awaitQueued(2);
            lock.commit();
            lock.close();
            assertThat(a.get(30, TimeUnit.SECONDS)).isNotNull();
            assertThatThrownBy(() -> b.get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(LeaseLostException.class);
            assertThat(files()).containsExactly(
                    table.location() + "/data/kept.parquet");
        }
    }

    /**
     * When an item is leased again, the group its earlier holder queued is
     * refused before the new holder looks at the table, and never
     * committed after.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aNewLeaseHolderSettlesTheOldOnesGroup() throws Exception
    {
        QueuedCommits newPod = committer();
        try (GroupAppender old = new GroupAppender(table, committer()))
        {
            Held before = lease();
            Connection lock = holdLock();
            Future<Snapshot> sent = senders.submit(
                    () -> old.append(file("old"), before));
            awaitQueued(1);
            // The lease runs out and another pod takes it, epoch + 1.
            try (Connection connection = database.getConnection();
                    PreparedStatement update = connection.prepareStatement(
                            "UPDATE " + leaseTable + " SET epoch = epoch + 1"
                                    + " WHERE name = ?"))
            {
                update.setString(1, before.name());
                update.executeUpdate();
            }
            Held after = new Held(before.name(), before.epoch() + 1);
            Future<?> settled = senders.submit(() ->
            {
                newPod.settle(other, after);
                return null;
            });
            Thread.sleep(200);
            lock.commit();
            lock.close();
            settled.get(30, TimeUnit.SECONDS);
            assertThat(files()).isEmpty();
            assertThatThrownBy(() -> sent.get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(LeaseLostException.class);
            assertThat(files()).isEmpty();
        }
    }

    /**
     * A pod that died after its Iceberg commit but before marking the rows
     * leaves them waiting; the next turn finds them named in the snapshot
     * and marks them, rather than committing them twice.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aCommitWhosePodDiedIsFoundNotRepeated() throws Exception
    {
        try (GroupAppender one = new GroupAppender(table, committer()))
        {
            Held held = lease();
            Connection lock = holdLock();
            Future<Snapshot> sent = senders.submit(
                    () -> one.append(file("once"), held));
            awaitQueued(1);
            // What the dead pod did: the Iceberg commit, and no more.
            long id;
            byte[] manifest;
            try (PreparedStatement select = lock.prepareStatement(
                    "SELECT id, manifest FROM " + schema + ".append_queue"))
            {
                try (ResultSet rows = select.executeQuery())
                {
                    rows.next();
                    id = rows.getLong(1);
                    manifest = rows.getBytes(2);
                }
            }
            AppendFiles append = other.newFastAppend();
            append.appendManifest(QueuedCommits.decode(manifest));
            append.set(QueuedCommits.SUMMARY, Long.toString(id, 36));
            append.commit();
            long dead = other.currentSnapshot().snapshotId();
            lock.rollback();
            lock.close();
            assertThat(sent.get(30, TimeUnit.SECONDS).snapshotId())
                    .isEqualTo(dead);
            assertThat(files()).hasSize(1);
        }
    }

    /**
     * A committer refuses a lease that isn't kept in a store.
     */
    @Test
    void anUnnamedLeaseIsRefused()
    {
        LeaseToken unnamed = new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return 1;
            }

            @Override
            public void checkHeld()
            {
            }
        };
        assertThatThrownBy(() -> committer().commit(table,
                List.of(file("x")), List.of(unnamed)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
