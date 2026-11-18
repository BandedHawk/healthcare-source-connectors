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
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link GroupAppender}, on a JDBC catalog.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class GroupAppenderTest
{

    /** The catalog the table is in. */
    Catalog catalog;

    /** The {@code split_records} table the files are appended to. */
    Table table;

    /** Creates the tests; JUnit runs each test on a new instance. */
    GroupAppenderTest()
    {
    }

    /** A lease held until a test says otherwise. */
    static class Lease implements LeaseToken
    {

        /** Whether it is lost. */
        volatile boolean lost;

        /** Creates a held lease. */
        Lease()
        {
        }

        @Override
        public long epoch()
        {
            return 1;
        }

        @Override
        public void checkHeld()
        {
            if (lost)
            {
                throw new LeaseLostException(
                        LeaseLostException.Reason.EXPIRED);
            }
        }
    }

    /** Creates the table. */
    @BeforeEach
    void setUp()
    {
        catalog = TestCatalogs.create(TestCatalogs.Kind.JDBC).newClient();
        table = IngestTables.splitRecords(catalog,
                "ns_" + UUID.randomUUID().toString().replace("-", ""));
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
     * How many data files the table's current snapshot holds.
     *
     * @return the count
     */
    long files()
    {
        table.refresh();
        return Long.parseLong(table.currentSnapshot().summary()
                .getOrDefault("total-data-files", "0"));
    }

    /**
     * A file sent while nothing is committing is committed at once; the
     * files sent while a commit runs all go in the next one.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void filesSentDuringACommitShareTheNext() throws Exception
    {
        CountDownLatch committing = new CountDownLatch(1);
        CountDownLatch others = new CountDownLatch(1);
        Lease first = new Lease()
        {
            @Override
            public void checkHeld()
            {
                committing.countDown();
                try
                {
                    others.await(10, TimeUnit.SECONDS);
                }
                catch (InterruptedException interrupted)
                {
                    Thread.currentThread().interrupt();
                }
            }
        };
        ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor();
        try (GroupAppender appender = new GroupAppender(table))
        {
            Future<Snapshot> alone = senders.submit(
                    () -> appender.append(file("first"), first));
            assertThat(committing.await(10, TimeUnit.SECONDS)).isTrue();
            List<Future<Snapshot>> grouped = new ArrayList<>();
            for (int index = 0; index < 5; index++)
            {
                String name = "grouped-" + index;
                grouped.add(senders.submit(
                        () -> appender.append(file(name), new Lease())));
            }
            Thread.sleep(200);
            others.countDown();

            long firstSnapshot = alone.get(10, TimeUnit.SECONDS).snapshotId();
            Set<Long> snapshots = new HashSet<>();
            for (Future<Snapshot> each : grouped)
            {
                snapshots.add(each.get(10, TimeUnit.SECONDS).snapshotId());
            }
            assertThat(snapshots).hasSize(1).doesNotContain(firstSnapshot);
            assertThat(files()).isEqualTo(6);
        }
        finally
        {
            senders.shutdownNow();
        }
    }

    /**
     * A committer that takes groups at once: two groups of the pod commit
     * at the same time, each on a thread of its own, rather than the second
     * waiting for the first; with one at a time, as by default, the second
     * waits.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void groupsAtOnceCommitSideBySide() throws Exception
    {
        assertThat(atOnce(2)).isEqualTo(2);
        assertThat(atOnce(1)).isEqualTo(1);
    }

    /**
     * Sends two files, one at a time, to an appender whose committer takes
     * a number of groups at once and holds each commit until two are under
     * way or a moment has passed, and says how many were under way at most.
     *
     * @param groups the groups the committer takes at once
     * @return the most commits under way together
     * @throws Exception if the test fails to run
     */
    int atOnce(int groups) throws Exception
    {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger most = new AtomicInteger();
        CountDownLatch both = new CountDownLatch(2);
        GroupAppender.Committer committer = new GroupAppender.Committer()
        {
            @Override
            public Snapshot commit(Table into, List<DataFile> files,
                    List<LeaseToken> leases)
            {
                most.accumulateAndGet(running.incrementAndGet(), Math::max);
                both.countDown();
                try
                {
                    both.await(500, TimeUnit.MILLISECONDS);
                    synchronized (this)
                    {
                        AppendFiles append = into.newFastAppend();
                        files.forEach(append::appendFile);
                        append.commit();
                        return into.currentSnapshot();
                    }
                }
                catch (InterruptedException interrupted)
                {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                finally
                {
                    running.decrementAndGet();
                }
            }

            @Override
            public int groupsAtOnce()
            {
                return groups;
            }
        };
        ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor();
        try (GroupAppender appender = new GroupAppender(table, committer))
        {
            Future<Snapshot> first = senders.submit(
                    () -> appender.append(file("first-" + groups),
                            new Lease()));
            // The first is taken alone before the second is sent.
            Thread.sleep(100);
            Future<Snapshot> second = senders.submit(
                    () -> appender.append(file("second-" + groups),
                            new Lease()));
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
        }
        finally
        {
            senders.shutdownNow();
        }
        return most.get();
    }

    /**
     * A committer that takes fewer than one group at once is refused.
     */
    @Test
    void refusesACommitterTakingNoGroups()
    {
        GroupAppender.Committer none = new GroupAppender.Committer()
        {
            @Override
            public Snapshot commit(Table into, List<DataFile> files,
                    List<LeaseToken> leases)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public int groupsAtOnce()
            {
                return 0;
            }
        };
        assertThatThrownBy(() -> new GroupAppender(table, none))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * A file whose lease is lost is left out, and its sender told; the
     * rest of its group is committed.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void leavesOutAFileWhoseLeaseIsLost() throws Exception
    {
        Lease lost = new Lease();
        lost.lost = true;
        try (GroupAppender appender = new GroupAppender(table))
        {
            assertThatThrownBy(() -> appender.append(file("lost"), lost))
                    .isInstanceOf(LeaseLostException.class);
            Snapshot kept = appender.append(file("kept"), new Lease());
            assertThat(kept).isNotNull();
            assertThat(files()).isEqualTo(1);
        }
    }

    /**
     * Appenders on two copies of the table, as two pods have, appending at
     * once: their commits conflict, each tries again on top of the other's,
     * and every file is committed once. The table's metadata is gzipped.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void appendersOnTwoPodsAllCommit() throws Exception
    {
        Table other = catalog.loadTable(TableIdentifier.parse(table.name()
                .substring(table.name().indexOf('.') + 1)));
        int each = 40;
        ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor();
        try (GroupAppender one = new GroupAppender(table);
                GroupAppender two = new GroupAppender(other))
        {
            List<Future<Snapshot>> sent = new ArrayList<>();
            for (int index = 0; index < each; index++)
            {
                String name = "file-" + index;
                sent.add(senders.submit(
                        () -> one.append(file("one-" + name), new Lease())));
                sent.add(senders.submit(
                        () -> two.append(file("two-" + name), new Lease())));
            }
            for (Future<Snapshot> future : sent)
            {
                assertThat(future.get(60, TimeUnit.SECONDS)).isNotNull();
            }
        }
        finally
        {
            senders.shutdownNow();
        }
        assertThat(files()).isEqualTo(2L * each);
        Set<String> paths = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan()
                .planFiles())
        {
            tasks.forEach(task -> paths.add(task.file().location()));
        }
        assertThat(paths).hasSize(2 * each);
        assertThat(((HasTableOperations) table).operations().current()
                .metadataFileLocation()).endsWith(".gz.metadata.json");
    }

    /**
     * A closed appender takes no more files.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aClosedAppenderRefusesFiles() throws Exception
    {
        GroupAppender appender = new GroupAppender(table);
        appender.close();
        assertThatThrownBy(() -> appender.append(file("late"), new Lease()))
                .isInstanceOf(IllegalStateException.class);
    }
}
