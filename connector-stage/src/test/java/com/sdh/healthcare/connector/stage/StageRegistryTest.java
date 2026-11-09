/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StageRegistry}, on a real Postgres.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StageRegistryTest
{

    /** A claim that outlives any test. */
    static final Duration LONG = Duration.ofMinutes(5);

    /** A claim that is over almost at once. */
    static final Duration BRIEF = Duration.ofMillis(1);

    /** The first worker's registry. */
    StageRegistry first;

    /** The second worker's registry, on the same tables. */
    StageRegistry second;

    /** The item the records are from. */
    final UUID item = UUID.randomUUID();

    /** Creates the tests; JUnit runs each test on a new instance. */
    StageRegistryTest()
    {
    }

    /** Gives each test its own tables. */
    @BeforeEach
    void tables()
    {
        String schema = StageTestSupport.freshSchema();
        first = StageTestSupport.registry(schema, "pod-a");
        second = StageTestSupport.registry(schema, "pod-b");
    }

    /**
     * A record to mark stored.
     *
     * @param recordId the record
     * @param failed   whether it failed
     * @return the entry
     */
    StageRegistry.Entry entry(String recordId, boolean failed)
    {
        if (failed)
        {
            // Not sent on, and recorded in the fail store (STAGES.md §15).
            return new StageRegistry.Entry(recordId, item, "d-" + recordId,
                    "v1", true, null, new FailureStore.Failure(item,
                            Long.parseLong(recordId.substring(1)), recordId,
                            "TEST_FAILED", 10, "v1"));
        }
        return new StageRegistry.Entry(recordId, item, "d-" + recordId,
                "v1", false, new OutboundMessage("m-" + recordId,
                        MessageType.RECORD_STORED, item, recordId,
                        new byte[] {1, 2, 3}));
    }

    /**
     * A record's key in the test's item.
     *
     * @param recordId the record
     * @return its key
     */
    StageRegistry.Key key(String recordId)
    {
        return new StageRegistry.Key(item, recordId);
    }

    /**
     * A new record is claimed, its owner may claim it again without an
     * attempt counted, and another worker finds it busy.
     */
    @Test
    void claimsARecordOnce()
    {
        assertThat(first.claim("r1", item, LONG))
                .isEqualTo(new StageRegistry.Owned(1));
        assertThat(first.claim("r1", item, LONG))
                .isEqualTo(new StageRegistry.Owned(1));
        assertThat(second.claim("r1", item, LONG))
                .isInstanceOf(StageRegistry.Busy.class);
    }

    /**
     * A claim left to expire is taken over, with one more attempt; one
     * renewed stays its owner's.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void takesOverAnExpiredClaim() throws InterruptedException
    {
        first.claim("r1", item, BRIEF);
        first.claim("r2", item, BRIEF);
        assertThat(first.renew(LONG)).isEqualTo(2);
        first.claim("r3", item, BRIEF);
        Thread.sleep(50);
        assertThat(second.claim("r1", item, LONG))
                .isInstanceOf(StageRegistry.Busy.class);
        assertThat(second.claim("r3", item, LONG))
                .isEqualTo(new StageRegistry.Owned(2));
        assertThat(first.claim("r3", item, LONG))
                .isInstanceOf(StageRegistry.Busy.class);
    }

    /**
     * Released claims are taken over at once, with one more attempt; a
     * stored record and another worker's claim are left alone.
     */
    @Test
    void releasesItsClaimsForAnotherWorker()
    {
        first.claim("r1", item, LONG);
        first.claim("r2", item, LONG);
        first.store(List.of(entry("r2", false)));
        second.claim("r3", item, LONG);
        assertThat(first.release()).isEqualTo(1);
        assertThat(second.claim("r1", item, LONG))
                .isEqualTo(new StageRegistry.Owned(2));
        assertThat(second.claim("r2", item, LONG))
                .isInstanceOf(StageRegistry.Stored.class);
        assertThat(first.claim("r3", item, LONG))
                .isInstanceOf(StageRegistry.Busy.class);
        assertThat(first.release()).isZero();
    }

    /**
     * Stored records are counted on their item, keep their message until
     * it is sent, and say who stored them.
     */
    @Test
    void storesCountsAndKeepsTheMessageUntilSent()
    {
        first.claim("r1", item, LONG);
        first.claim("r2", item, LONG);
        assertThat(first.store(List.of(entry("r1", false),
                entry("r2", true)))).containsExactlyInAnyOrder(key("r1"),
                        key("r2"));
        assertThat(first.item(item)).contains(
                new ItemProgress(item, -1, 2, 1));

        StageRegistry.Claim stored = second.claim("r1", item, LONG);
        assertThat(stored).isInstanceOfSatisfying(StageRegistry.Stored.class,
                row ->
                {
                    assertThat(row.digest()).isEqualTo("d-r1");
                    assertThat(row.version()).isEqualTo("v1");
                    assertThat(row.attempts()).isEqualTo(1);
                    assertThat(row.state()).isEqualTo("STORED");
                    assertThat(row.mine()).isFalse();
                    assertThat(row.pending().messageId()).isEqualTo("m-r1");
                    assertThat(row.pending().type())
                            .isEqualTo(MessageType.RECORD_STORED);
                    assertThat(row.pending().itemId()).isEqualTo(item);
                    assertThat(row.pending().key()).isEqualTo("r1");
                    assertThat(row.pending().body())
                            .containsExactly(1, 2, 3);
                });
        assertThat(first.claim("r1", item, LONG)).isInstanceOfSatisfying(
                StageRegistry.Stored.class, row ->
                        assertThat(row.mine()).isTrue());

        first.markSent(Set.of(key("r1")));
        assertThat(second.claim("r1", item, LONG)).isInstanceOfSatisfying(
                StageRegistry.Stored.class, row ->
                        assertThat(row.pending()).isNull());
        // A failed record has nothing to send: failures aren't sent on
        // (STAGES.md §15.1).
        assertThat(second.claim("r2", item, LONG)).isInstanceOfSatisfying(
                StageRegistry.Stored.class, row ->
                        assertThat(row.pending()).isNull());
    }

    /**
     * A worker whose claim was taken over doesn't store or count the
     * record: its new owner does.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void leavesARecordWhoseClaimWasLost() throws InterruptedException
    {
        first.claim("r1", item, BRIEF);
        Thread.sleep(50);
        second.claim("r1", item, LONG);
        assertThat(first.store(List.of(entry("r1", false)))).isEmpty();
        assertThat(first.item(item)).isEmpty();
        assertThat(second.store(List.of(entry("r1", false))))
                .containsExactly(key("r1"));
        assertThat(second.item(item)).contains(
                new ItemProgress(item, -1, 1, 0));
    }

    /**
     * An item made under two versions names both, in the order first used,
     * each once (STAGES.md §14.3).
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void aDueItemNamesTheVersionsItWasMadeUnder() throws InterruptedException
    {
        first.expect(item, 3, new byte[] {1});
        for (String[] made : new String[][] {{"r1", "v1"}, {"r2", "v2"},
            {"r3", "v1"}})
        {
            first.claim(made[0], item, LONG);
            first.store(List.of(new StageRegistry.Entry(made[0], item,
                    "d-" + made[0], made[1], false, new OutboundMessage(
                            "m-" + made[0], MessageType.RECORD_STORED, item,
                            made[0], new byte[] {1}))));
            // stored_at orders them: apart, so the order is the test's.
            Thread.sleep(5);
        }
        assertThat(first.due(List.of(item))).singleElement()
                .extracting(due -> due.progress().versions())
                .isEqualTo(List.of("v1", "v2"));
    }

    /**
     * A total from the stage before's item.progress may grow, and is
     * replaced by the final one, which is then kept (STAGES.md §15.6).
     */
    @Test
    void aTotalStillOpenUpstreamGrowsUntilTheFinalOne()
    {
        first.expect(item, 2, new byte[] {1}, true);
        first.expect(item, 1, new byte[] {2}, true);
        first.claim("r1", item, LONG);
        first.claim("r2", item, LONG);
        first.store(List.of(entry("r1", false), entry("r2", false)));
        assertThat(first.due(List.of(item))).singleElement().satisfies(due ->
        {
            assertThat(due.progress().expected()).isEqualTo(2);
            assertThat(due.progress().upstreamOpen()).isTrue();
            assertThat(due.progress().complete()).isFalse();
            assertThat(due.total()).isEqualTo(new byte[] {1});
        });

        first.expect(item, 3, new byte[] {3}, true);
        assertThat(first.due(List.of(item))).isEmpty();
        first.expect(item, 2, new byte[] {4}, false);
        first.expect(item, 3, new byte[] {5}, true);
        assertThat(first.due(List.of(item))).singleElement().satisfies(due ->
        {
            assertThat(due.progress().expected()).isEqualTo(2);
            assertThat(due.progress().complete()).isTrue();
            assertThat(due.total()).isEqualTo(new byte[] {4});
        });
    }

    /**
     * An item is due once its total is known and all its records are
     * stored, until its message is marked sent; the first total, and the
     * message that gave it, are kept.
     */
    @Test
    void anItemIsDueOnceItsRecordsAreAllStored()
    {
        UUID other = UUID.randomUUID();
        byte[] firstTotal = {1, 2, 3};
        first.expect(item, 2, firstTotal);
        first.expect(other, 0);
        first.claim("r1", item, LONG);
        first.store(List.of(entry("r1", false)));
        assertThat(first.due(List.of(item, other)))
                .extracting(StageRegistry.DueItem::progress)
                .containsExactly(new ItemProgress(other, 0, 0, 0));

        first.claim("r2", item, LONG);
        first.store(List.of(entry("r2", true)));
        first.expect(item, 5, new byte[] {9});
        List<StageRegistry.DueItem> due = first.due(List.of(item));
        // With the version its records were made under (STAGES.md §14.3).
        assertThat(due).extracting(StageRegistry.DueItem::progress)
                .containsExactly(new ItemProgress(item, 2, 2, 1)
                        .withVersions(List.of("v1")));
        assertThat(due.get(0).total()).isEqualTo(firstTotal);
        assertThat(second.due(List.of(item))).hasSize(1);

        // Its failure is open, so it reports progress rather than
        // completing; once noted, the same counts aren't reported again.
        assertThat(due.get(0).progress().complete()).isFalse();
        assertThat(due.get(0).progressChanged()).isTrue();
        first.markProgressed(item, due.get(0).progress().progressKey());
        assertThat(first.due(List.of(item))).singleElement()
                .satisfies(again -> assertThat(again.progressChanged())
                        .isFalse());
        assertThat(first.failureStore().failures(item)).singleElement()
                .satisfies(stored -> assertThat(stored.failure().seq())
                        .isEqualTo(2));

        first.markCompleted(item);
        assertThat(first.due(List.of(item, other)))
                .extracting(StageRegistry.DueItem::progress)
                .containsExactly(new ItemProgress(other, 0, 0, 0));
        assertThat(first.due(List.of())).isEmpty();
    }

    /**
     * The same content in another item is a record of that item: claimed,
     * stored and counted there too.
     */
    @Test
    void theSameRecordInAnotherItemIsItsOwn()
    {
        UUID other = UUID.randomUUID();
        first.claim("r1", item, LONG);
        first.store(List.of(entry("r1", false)));
        assertThat(second.claim("r1", other, LONG))
                .isEqualTo(new StageRegistry.Owned(1));
        assertThat(second.store(List.of(new StageRegistry.Entry("r1", other,
                "d-r1", "v1", false, new OutboundMessage("m-r1-other",
                        MessageType.RECORD_STORED, other, "r1",
                        new byte[] {4}))))).containsExactly(
                                new StageRegistry.Key(other, "r1"));
        assertThat(second.item(other)).contains(
                new ItemProgress(other, -1, 1, 0));
        assertThat(first.item(item)).contains(
                new ItemProgress(item, -1, 1, 0));
    }

    /** A schema name that isn't a plain identifier is refused. */
    @Test
    void refusesAnOddSchemaName()
    {
        assertThatThrownBy(() -> new StageRegistry(
                StageTestSupport.database(), "x; drop table y", "test",
                "pod-a", LONG)).hasMessageContaining("schema");
    }

    /**
     * The reconciler's watermark starts out empty, is shared by the
     * stage's workers, and only moves forward: a pod that lost the duties
     * part way can't move it back.
     */
    @Test
    void keepsTheReconcilersWatermarkMovingForward()
    {
        assertThat(first.checkedTo()).isEmpty();
        Instant later = Instant.parse("2026-11-07T06:00:00Z");
        first.markChecked(later);
        assertThat(second.checkedTo()).hasValue(later);
        second.markChecked(later.minus(Duration.ofDays(3)));
        assertThat(first.checkedTo()).hasValue(later);
        first.markChecked(later.plusSeconds(60));
        assertThat(second.checkedTo()).hasValue(later.plusSeconds(60));
    }

    /**
     * A backlog noted by one worker is seen by the others, by the
     * database's clock; a deferral starts once, grows until it is ended,
     * and starts again afterwards.
     *
     * @throws Exception if interrupted
     */
    @Test
    void sharesWhenABacklogWasLastSeenAndHowLongTheReconcilerHasWaited()
            throws Exception
    {
        assertThat(first.sinceBacklog()).isEmpty();
        first.endDeferral();
        first.noteBacklog();
        assertThat(second.sinceBacklog()).hasValueSatisfying(since ->
                assertThat(since).isBetween(Duration.ZERO,
                        Duration.ofSeconds(5)));

        Duration started = second.deferring();
        assertThat(started).isLessThan(Duration.ofSeconds(1));
        Thread.sleep(150);
        assertThat(first.deferring()).as("not started again")
                .isGreaterThanOrEqualTo(Duration.ofMillis(140));
        Thread.sleep(150);
        assertThat(first.sinceBacklog().orElseThrow())
                .isGreaterThanOrEqualTo(Duration.ofMillis(290));
        second.noteBacklog();
        assertThat(first.sinceBacklog().orElseThrow())
                .isLessThan(Duration.ofMillis(290));

        first.endDeferral();
        assertThat(second.deferring()).isLessThan(Duration.ofMillis(140));
    }

    /**
     * A wait at startup is kept with no backlog ever seen, when there is no
     * row yet; and a row made that way shows no backlog.
     *
     * @throws Exception if interrupted
     */
    @Test
    void keepsAWaitWithNoBacklogEverSeen() throws Exception
    {
        assertThat(first.deferring()).isLessThan(Duration.ofSeconds(1));
        assertThat(second.sinceBacklog()).as("none seen").isEmpty();
        Thread.sleep(150);
        assertThat(second.deferring())
                .isGreaterThanOrEqualTo(Duration.ofMillis(140));
    }
}
