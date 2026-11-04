/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.TestPostgres;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JdbcLeaseStore}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class JdbcLeaseStoreTest
{

    /** The store under test. */
    private JdbcLeaseStore store;

    /** A lease name of this test's own. */
    private String name;

    /** Creates the store and table, and picks a fresh lease name. */
    @BeforeEach
    void setUp()
    {
        store = new JdbcLeaseStore(TestPostgres.dataSource(), "sdh_lease",
                Duration.ofSeconds(5));
        store.createTableIfMissing();
        name = "lease-" + UUID.randomUUID();
    }

    /**
     * A live lease can't be taken, and its holder can renew it.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void secondOwnerIsRefusedWhileLeaseIsLive() throws Exception
    {
        LeaseGrant first = store.acquire(name, "a", Duration.ofSeconds(30));
        assertThat(first.epoch()).isEqualTo(1);
        assertThatThrownBy(() -> store.acquire(name, "b",
                Duration.ofSeconds(30)))
                .isInstanceOf(LeaseHeldException.class)
                .satisfies(error -> assertThat(
                        ((LeaseHeldException) error).holder())
                        .isEqualTo("a"));
        assertThat(store.renew(first, Duration.ofSeconds(30))).isTrue();
    }

    /**
     * An expired lease goes to a new holder with a higher epoch, and the
     * old grant can't renew.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void expiredLeaseIsTakenOverWithHigherEpochAndOldGrantCannotRenew()
            throws Exception
    {
        LeaseGrant first = store.acquire(name, "a", Duration.ofMillis(200));
        Thread.sleep(400);
        LeaseGrant second = store.acquire(name, "b",
                Duration.ofSeconds(30));
        assertThat(second.epoch()).isEqualTo(first.epoch() + 1);
        assertThat(store.renew(first, Duration.ofSeconds(30))).isFalse();
        assertThat(store.renew(second, Duration.ofSeconds(30))).isTrue();
    }

    /**
     * A release frees the lease at once, and the epoch keeps rising.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void releaseFreesTheLeaseButEpochKeepsRising() throws Exception
    {
        LeaseGrant first = store.acquire(name, "a", Duration.ofSeconds(30));
        store.release(first);
        LeaseGrant second = store.acquire(name, "b",
                Duration.ofSeconds(30));
        assertThat(second.epoch()).isEqualTo(2);
        store.release(second);
        assertThat(store.acquire(name, "c", Duration.ofSeconds(30)).epoch())
                .isEqualTo(3);
    }

    /**
     * Of many callers at once, exactly one gets the lease.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void concurrentAcquireHasExactlyOneWinner() throws Exception
    {
        int contenders = 16;
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (int index = 0; index < contenders; index++)
        {
            String owner = "o" + index;
            tasks.add(() ->
            {
                start.await();
                try
                {
                    store.acquire(name, owner, Duration.ofSeconds(30));
                    return true;
                }
                catch (LeaseHeldException held)
                {
                    return false;
                }
            });
        }
        try (ExecutorService pool = Executors.newFixedThreadPool(contenders))
        {
            List<Future<Boolean>> results = new ArrayList<>();
            for (Callable<Boolean> task : tasks)
            {
                results.add(pool.submit(task));
            }
            start.countDown();
            long winners = 0;
            for (Future<Boolean> result : results)
            {
                if (result.get())
                {
                    winners++;
                }
            }
            assertThat(winners).isEqualTo(1);
        }
    }
}
