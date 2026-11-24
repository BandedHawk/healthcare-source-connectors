/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.TestPostgres;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Runs {@link JdbcLeaseStoreTest}'s tests on a store that gathers leases
 * asked for at once (STAGES.md §18.3, part B), and tests that each lease in
 * a gathered statement is still its own.
 */
class GatheredLeaseStoreTest extends JdbcLeaseStoreTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    GatheredLeaseStoreTest()
    {
    }

    /**
     * Makes a store that gathers up to 64 leases a statement.
     *
     * @return it
     */
    @Override
    JdbcLeaseStore newStore()
    {
        return new JdbcLeaseStore(TestPostgres.dataSource(), "sdh_lease",
                Duration.ofSeconds(5), 64);
    }

    /**
     * Many callers at once, a third asking for leases another owner holds:
     * each of those alone is refused, each other caller gets its lease, and
     * renewals and releases at once each change only their own grant's row.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void eachLeaseInAGatheredStatementIsItsOwn() throws Exception
    {
        JdbcLeaseStore gathered = newStore();
        String prefix = "gathered-" + UUID.randomUUID() + "-";
        int count = 90;
        for (int i = 0; i < count; i += 3)
        {
            gathered.acquire(prefix + i, "holder", Duration.ofSeconds(30));
        }
        try (ExecutorService callers =
                Executors.newVirtualThreadPerTaskExecutor())
        {
            List<Future<Object>> asked = new ArrayList<>();
            for (int i = 0; i < count; i++)
            {
                String lease = prefix + i;
                asked.add(callers.submit(() ->
                {
                    try
                    {
                        return gathered.acquire(lease, "caller",
                                Duration.ofSeconds(30));
                    }
                    catch (LeaseHeldException held)
                    {
                        return held;
                    }
                }));
            }
            List<LeaseGrant> grants = new ArrayList<>();
            for (int i = 0; i < count; i++)
            {
                Object got = asked.get(i).get(30, TimeUnit.SECONDS);
                if (i % 3 == 0)
                {
                    assertThat(got).isInstanceOf(LeaseHeldException.class);
                    assertThat(((LeaseHeldException) got).holder())
                            .isEqualTo("holder");
                }
                else
                {
                    assertThat(got).isInstanceOf(LeaseGrant.class);
                    LeaseGrant grant = (LeaseGrant) got;
                    assertThat(grant.leaseName()).isEqualTo(prefix + i);
                    assertThat(grant.owner()).isEqualTo("caller");
                    assertThat(grant.epoch()).isEqualTo(1);
                    grants.add(grant);
                }
            }
            // A stale grant among them: its renewal alone is refused.
            LeaseGrant stale = new LeaseGrant(grants.get(0).leaseName(),
                    "caller", 0, grants.get(0).expiresAt());
            Future<Boolean> staleRenewal = callers.submit(() ->
                    gathered.renew(stale, Duration.ofSeconds(30)));
            List<Future<Boolean>> renewals = new ArrayList<>();
            for (LeaseGrant grant : grants.subList(1, grants.size()))
            {
                renewals.add(callers.submit(() ->
                        gathered.renew(grant, Duration.ofSeconds(30))));
            }
            assertThat(staleRenewal.get(30, TimeUnit.SECONDS)).isFalse();
            for (Future<Boolean> renewal : renewals)
            {
                assertThat(renewal.get(30, TimeUnit.SECONDS)).isTrue();
            }
            List<Future<?>> releases = new ArrayList<>();
            for (LeaseGrant grant : grants)
            {
                releases.add(callers.submit(() -> gathered.release(grant)));
            }
            for (Future<?> release : releases)
            {
                release.get(30, TimeUnit.SECONDS);
            }
        }
        for (int i = 1; i < count; i += 3)
        {
            LeaseGrant again = gathered.acquire(prefix + i, "next",
                    Duration.ofSeconds(30));
            assertThat(again.epoch()).isEqualTo(2);
        }
    }
}
