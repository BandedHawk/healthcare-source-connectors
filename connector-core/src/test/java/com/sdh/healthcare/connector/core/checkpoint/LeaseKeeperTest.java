package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class LeaseKeeperTest {
    static final LeaseKeeper.Settings SETTINGS =
            new LeaseKeeper.Settings(Duration.ofSeconds(60), Duration.ofSeconds(15), Duration.ofSeconds(10));

    /** Store whose renew behaviour the test controls. */
    static final class FakeStore implements LeaseStore {
        volatile String renewMode = "ok"; // ok | fail | gone
        volatile boolean released;

        @Override
        public LeaseGrant acquire(String n, String o, Duration ttl) {
            return new LeaseGrant(n, o, 7, Instant.now().plus(ttl));
        }

        @Override
        public boolean renew(LeaseGrant g, Duration ttl) {
            return switch (renewMode) {
                case "fail" -> throw new IllegalStateException("store down");
                case "gone" -> false;
                default -> true;
            };
        }

        @Override
        public void release(LeaseGrant g) {
            released = true;
        }
    }

    final AtomicLong now = new AtomicLong(1_000_000_000L);
    final List<LeaseLostException> lost = new ArrayList<>();

    LeaseKeeper keeper(FakeStore store) throws Exception {
        return LeaseKeeper.acquire(store, "l", "me", SETTINGS, now::get, lost::add, false);
    }

    void advance(Duration d) {
        now.addAndGet(d.toNanos());
    }

    @Test
    void successfulRenewalsKeepTheLease() throws Exception {
        FakeStore store = new FakeStore();
        LeaseKeeper k = keeper(store);
        for (int i = 0; i < 20; i++) {
            advance(Duration.ofSeconds(15));
            k.renewOnce();
        }
        k.checkHeld();
        assertThat(k.epoch()).isEqualTo(7);
        k.close();
        assertThat(store.released).isTrue();
    }

    @Test
    void selfFencesWhenStoreIsUnreachableForTtlMinusMargin() throws Exception {
        FakeStore store = new FakeStore();
        LeaseKeeper k = keeper(store);
        store.renewMode = "fail";
        advance(Duration.ofSeconds(15));
        k.renewOnce();
        advance(Duration.ofSeconds(15));
        k.renewOnce();
        advance(Duration.ofSeconds(15));
        k.renewOnce();
        k.checkHeld(); // 45 s since the last good renewal (acquire): still inside 60 - 10
        advance(Duration.ofSeconds(5));
        assertThatThrownBy(k::checkHeld)
                .isInstanceOf(LeaseLostException.class)
                .extracting(e -> ((LeaseLostException) e).reason())
                .isEqualTo(LeaseLostException.Reason.EXPIRED);
        assertThat(lost).hasSize(1);
        k.close();
        assertThat(store.released).as("a lost lease is not released").isFalse();
    }

    @Test
    void takeOverIsDetectedOnNextRenewal() throws Exception {
        FakeStore store = new FakeStore();
        LeaseKeeper k = keeper(store);
        store.renewMode = "gone";
        advance(Duration.ofSeconds(15));
        k.renewOnce();
        assertThat(k.isHeld()).isFalse();
        assertThat(lost).singleElement().extracting(LeaseLostException::reason)
                .isEqualTo(LeaseLostException.Reason.TAKEN_OVER);
    }

    @Test
    void settingsMustAllowTwoMissedRenewals() {
        assertThatThrownBy(() -> new LeaseKeeper.Settings(Duration.ofSeconds(30), Duration.ofSeconds(10),
                Duration.ofSeconds(10))).isInstanceOf(IllegalArgumentException.class);
    }
}
