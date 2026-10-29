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
    static class FakeStore implements LeaseStore {
        volatile String renewMode = "ok"; // ok | fail | gone
        volatile boolean released;

        @Override
        public LeaseGrant acquire(String n, String o, Duration ttl) throws LeaseHeldException {
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
        return LeaseKeeper.acquire(store, "l", "me", SETTINGS, now::get, this::sleep, lost::add, false);
    }

    void advance(Duration d) {
        now.addAndGet(d.toNanos());
    }

    final List<Duration> sleeps = new ArrayList<>();

    void sleep(Duration d) {
        sleeps.add(d);
        advance(d);
    }

    /** Held by another instance until the test's clock reaches {@code freeAt} (nanos). */
    final class HeldStore extends FakeStore {
        volatile long freeAt;
        int attempts;

        @Override
        public LeaseGrant acquire(String n, String o, Duration ttl) throws LeaseHeldException {
            attempts++;
            if (now.get() < freeAt) {
                throw new LeaseHeldException("other/1/x", Instant.now());
            }
            return super.acquire(n, o, ttl);
        }
    }

    @Test
    void waitsForALeaseThatLapses() throws Exception {
        // A crashed holder: its lease lapses 40 s from now, well within the 70 s startup wait (ttl + margin).
        HeldStore store = new HeldStore();
        store.freeAt = now.get() + Duration.ofSeconds(40).toNanos();
        long start = now.get();
        LeaseKeeper k = LeaseKeeper.acquire(store, "l", "me", SETTINGS, now::get, this::sleep, lost::add, false);
        assertThat(k.epoch()).isEqualTo(7);
        assertThat(Duration.ofNanos(now.get() - start)).isEqualTo(Duration.ofSeconds(40));
        assertThat(sleeps).allSatisfy(d -> assertThat(d).isEqualTo(LeaseKeeper.STARTUP_POLL));
        assertThat(store.attempts).isEqualTo(21);
        // Validity counts from the attempt that succeeded, not from the start of the wait.
        advance(Duration.ofSeconds(49));
        k.checkHeld();
    }

    @Test
    void refusesWhenTheHolderIsAliveAfterTheWait() {
        // A live holder keeps renewing, so the lease never lapses: refused after ttl + margin.
        HeldStore store = new HeldStore();
        store.freeAt = Long.MAX_VALUE;
        long start = now.get();
        assertThatThrownBy(() -> LeaseKeeper.acquire(store, "l", "me", SETTINGS, now::get, this::sleep, lost::add,
                false)).isInstanceOf(LeaseHeldException.class);
        assertThat(Duration.ofNanos(now.get() - start)).isEqualTo(SETTINGS.startupWait())
                .isEqualTo(Duration.ofSeconds(70));
        assertThat(sleeps.getLast()).isLessThanOrEqualTo(LeaseKeeper.STARTUP_POLL);
    }

    @Test
    void anInterruptedWaitGivesUp() {
        HeldStore store = new HeldStore();
        store.freeAt = Long.MAX_VALUE;
        assertThatThrownBy(() -> LeaseKeeper.acquire(store, "l", "me", SETTINGS, now::get, d -> {
            throw new InterruptedException();
        }, lost::add, false)).isInstanceOf(LeaseHeldException.class);
        assertThat(Thread.interrupted()).as("interrupt flag restored").isTrue();
        assertThat(store.attempts).isEqualTo(1);
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
