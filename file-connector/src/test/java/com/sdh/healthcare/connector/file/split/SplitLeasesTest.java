/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.split;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.LeaseHeldException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseKeeper;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the split stage's leases, on a real Postgres: one pod per item
 * ({@link ItemSplitter}) and one pod for the duties ({@link SplitDuties}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SplitLeasesTest
{

    /** Short timings, so a test doesn't wait long for a lease. */
    static final LeaseKeeper.Settings TIMINGS = new LeaseKeeper.Settings(
            Duration.ofSeconds(3), Duration.ofMillis(500),
            Duration.ofMillis(500));

    /** The lease database. */
    static EmbeddedPostgres postgres;

    /** The test's lease store, in a table of its own. */
    JdbcLeaseStore leases;

    /** The duties started, by pod. */
    final List<String> started = new CopyOnWriteArrayList<>();

    /** Fatal exits asked for. */
    final List<String> fatal = new CopyOnWriteArrayList<>();

    /** Creates the tests; JUnit runs each test on a new instance. */
    SplitLeasesTest()
    {
    }

    /**
     * Starts the database.
     *
     * @throws Exception if it can't start
     */
    @BeforeAll
    static void database() throws Exception
    {
        postgres = EmbeddedPostgres.start();
    }

    /**
     * Stops the database.
     *
     * @throws Exception if it can't stop
     */
    @AfterAll
    static void stop() throws Exception
    {
        postgres.close();
    }

    /** Gives each test its own lease table. */
    @BeforeEach
    void table()
    {
        leases = new JdbcLeaseStore(postgres.getPostgresDatabase(),
                "lease_" + UUID.randomUUID().toString().replace('-', '_'),
                Duration.ofSeconds(10));
        leases.createTableIfMissing();
    }

    /**
     * An item whose lease another pod holds isn't split here: the caller
     * is told to try again later.
     *
     * @throws Exception if the lease can't be taken
     */
    @Test
    void leavesAnItemAnotherPodHolds() throws Exception
    {
        UUID item = UUID.randomUUID();
        ItemSplitter splitter = new ItemSplitter(null, null, null, leases,
                "test", TIMINGS);
        try (LeaseKeeper other = LeaseKeeper.tryAcquire(leases,
                "split/test/" + item, "pod-b", TIMINGS, lost -> { }))
        {
            assertThatThrownBy(() -> splitter.split(item, "digest"))
                    .isInstanceOf(LeaseHeldException.class);
        }
    }

    /**
     * One pod does the duties; when it stops, another takes them over,
     * and nothing stops the process.
     *
     * @throws Exception if interrupted
     */
    @Test
    void oneDutiesPodAtATimeWithTakeOver() throws Exception
    {
        SplitDuties first = duties("pod-a");
        SplitDuties second = duties("pod-b");
        first.start();
        awaitStarted(1);
        second.start();
        Thread.sleep(1000);
        assertThat(started).containsExactly("pod-a");

        first.close();
        awaitStarted(2);
        assertThat(started).containsExactly("pod-a", "pod-b");
        second.close();
        assertThat(fatal).isEmpty();
    }

    /** Only errors that leave the stage unsafe stop the process. */
    @Test
    void stopsOnlyOnErrorsThatLeaveTheStageUnsafe()
    {
        assertThat(SplitHandler.fatalReason(new CheckpointTamperedException()))
                .hasValue("CHECKPOINT_TAMPERED");
        assertThat(SplitHandler.fatalReason(
                new CommitStateUnknownException(new RuntimeException())))
                .hasValue("COMMIT_STATE_UNKNOWN");
        assertThat(SplitHandler.fatalReason(new OutOfMemoryError()))
                .hasValue("OutOfMemoryError");
        assertThat(SplitHandler.fatalReason(new LeaseLostException(
                LeaseLostException.Reason.FENCED))).isEmpty();
        assertThat(SplitHandler.fatalReason(
                new IllegalStateException("database down"))).isEmpty();
    }

    /**
     * Duties for a pod that note when they start.
     *
     * @param pod the pod's name
     * @return the duties, not started
     */
    SplitDuties duties(String pod)
    {
        return new SplitDuties(leases, "test", TIMINGS, null,
                Duration.ofMillis(100), (committer, exit) ->
                {
                    started.add(pod);
                    return new MaintenanceScheduler(committer,
                            MaintenanceSettings.DEFAULT,
                            new MaintenanceScheduler.Schedule(false,
                                    Duration.ofHours(1), false,
                                    Duration.ofHours(1)),
                            List::of, new ReentrantLock(), exit,
                            Clock.systemUTC());
                }, fatal::add);
    }

    /**
     * Waits until the duties have started a number of times.
     *
     * @param count how many
     * @throws InterruptedException if interrupted
     */
    void awaitStarted(int count) throws InterruptedException
    {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (started.size() < count && System.nanoTime() < deadline)
        {
            Thread.sleep(50);
        }
        assertThat(started).hasSize(count);
    }
}
