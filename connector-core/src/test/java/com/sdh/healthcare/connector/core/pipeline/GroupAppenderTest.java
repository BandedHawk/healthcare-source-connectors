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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
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
        table = IngestTables.splitRecords(
                TestCatalogs.create(TestCatalogs.Kind.JDBC).newClient(),
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
