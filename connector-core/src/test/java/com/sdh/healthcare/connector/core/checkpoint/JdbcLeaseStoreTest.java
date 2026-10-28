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

class JdbcLeaseStoreTest {
    private JdbcLeaseStore store;
    private String name;

    @BeforeEach
    void setUp() {
        store = new JdbcLeaseStore(TestPostgres.dataSource(), "sdh_lease", Duration.ofSeconds(5));
        store.createTableIfMissing();
        name = "lease-" + UUID.randomUUID();
    }

    @Test
    void secondOwnerIsRefusedWhileLeaseIsLive() throws Exception {
        LeaseGrant a = store.acquire(name, "a", Duration.ofSeconds(30));
        assertThat(a.epoch()).isEqualTo(1);
        assertThatThrownBy(() -> store.acquire(name, "b", Duration.ofSeconds(30)))
                .isInstanceOf(LeaseHeldException.class)
                .satisfies(e -> assertThat(((LeaseHeldException) e).holder()).isEqualTo("a"));
        assertThat(store.renew(a, Duration.ofSeconds(30))).isTrue();
    }

    @Test
    void expiredLeaseIsTakenOverWithHigherEpochAndOldGrantCannotRenew() throws Exception {
        LeaseGrant a = store.acquire(name, "a", Duration.ofMillis(200));
        Thread.sleep(400);
        LeaseGrant b = store.acquire(name, "b", Duration.ofSeconds(30));
        assertThat(b.epoch()).isEqualTo(a.epoch() + 1);
        assertThat(store.renew(a, Duration.ofSeconds(30))).isFalse();
        assertThat(store.renew(b, Duration.ofSeconds(30))).isTrue();
    }

    @Test
    void releaseFreesTheLeaseButEpochKeepsRising() throws Exception {
        LeaseGrant a = store.acquire(name, "a", Duration.ofSeconds(30));
        store.release(a);
        LeaseGrant b = store.acquire(name, "b", Duration.ofSeconds(30));
        assertThat(b.epoch()).isEqualTo(2);
        store.release(b);
        assertThat(store.acquire(name, "c", Duration.ofSeconds(30)).epoch()).isEqualTo(3);
    }

    @Test
    void concurrentAcquireHasExactlyOneWinner() throws Exception {
        int contenders = 16;
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (int i = 0; i < contenders; i++) {
            String owner = "o" + i;
            tasks.add(() -> {
                start.await();
                try {
                    store.acquire(name, owner, Duration.ofSeconds(30));
                    return true;
                } catch (LeaseHeldException e) {
                    return false;
                }
            });
        }
        try (ExecutorService pool = Executors.newFixedThreadPool(contenders)) {
            List<Future<Boolean>> results = new ArrayList<>();
            for (Callable<Boolean> t : tasks) {
                results.add(pool.submit(t));
            }
            start.countDown();
            long winners = 0;
            for (Future<Boolean> f : results) {
                if (f.get()) {
                    winners++;
                }
            }
            assertThat(winners).isEqualTo(1);
        }
    }
}
