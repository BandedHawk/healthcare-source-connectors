/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

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

    /** The registry's schema for this test. */
    String schema;

    /** The item the records are from. */
    final UUID item = UUID.randomUUID();

    /** The audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** Creates the tests; JUnit runs each test on a new instance. */
    StageRegistryTest()
    {
    }

    /** Gives each test its own tables. */
    @BeforeEach
    void tables()
    {
        schema = StageTestSupport.freshSchema();
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
     * An item is part way under each version of its output while it isn't
     * done; one done, or with no output yet, isn't (STAGES.md §14.4a).
     */
    @Test
    void anUnfinishedItemIsPartWayUnderItsOutputsVersions()
    {
        UUID done = UUID.randomUUID();
        UUID untouched = UUID.randomUUID();
        first.expect(item, 3, new byte[] {1});
        first.expect(done, 1, new byte[] {1});
        first.expect(untouched, 2, new byte[] {1});
        stored("r1", item, "v1");
        stored("r2", item, "v2");
        stored("d1", done, "v1");

        assertThat(first.partWay(5)).containsExactly(
                new PartWay.Items("v1", 1, List.of(item)),
                new PartWay.Items("v2", 1, List.of(item)));
        assertThat(first.partWay(0)).extracting(PartWay.Items::first)
                .containsOnly(List.of());
    }

    /**
     * Stores a record under a version.
     *
     * @param recordId the record
     * @param itemId   its item
     * @param version  the version
     */
    void stored(String recordId, UUID itemId, String version)
    {
        first.claim(recordId, itemId, LONG);
        first.store(List.of(new StageRegistry.Entry(recordId, itemId,
                "d-" + recordId, version, false, new OutboundMessage(
                        "m-" + recordId, MessageType.RECORD_STORED, itemId,
                        recordId, new byte[] {1}))));
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
     * Totals are taken by the stage before's completion number (STAGES.md
     * §15.6e): an item.progress that reopens the completion taken reopens
     * the item here, one from before it or a repeat of the completion
     * withdrawn changes nothing, and the next completion is taken.
     */
    @Test
    void anItemReopenedUpstreamIsReopenedHere()
    {
        first.expect(item, 1, new byte[] {1}, true, 0);
        first.claim("r1", item, LONG);
        first.claim("r2", item, LONG);
        first.store(List.of(entry("r1", false), entry("r2", false)));
        first.expect(item, 2, new byte[] {2}, false, 1);
        assertThat(first.due(List.of(item))).singleElement().satisfies(due ->
        {
            assertThat(due.progress().complete()).isTrue();
            assertThat(due.progress().nextCompletion()).isEqualTo(1);
        });
        first.markCompleted(item);
        assertThat(first.due(List.of(item))).isEmpty();

        // From before the completion, or a repeat of it: stale.
        first.expect(item, 1, new byte[] {3}, true, 0);
        first.expect(item, 2, new byte[] {2}, false, 1);
        assertThat(first.due(List.of(item))).isEmpty();

        // Reopening it: due again, open upstream, its next completion 2.
        first.expect(item, 2, new byte[] {4}, true, 1);
        assertThat(audit.names()).contains("stage.item_reopened");
        assertThat(first.due(List.of(item))).singleElement().satisfies(due ->
        {
            assertThat(due.progress().upstreamOpen()).isTrue();
            assertThat(due.progress().complete()).isFalse();
            assertThat(due.progress().completion()).isEqualTo(1);
            assertThat(due.total()).isEqualTo(new byte[] {4});
        });
        // The completion it withdrew, delivered again: still open.
        first.expect(item, 2, new byte[] {2}, false, 1);
        assertThat(first.due(List.of(item))).singleElement().satisfies(due ->
                assertThat(due.progress().upstreamOpen()).isTrue());

        first.expect(item, 2, new byte[] {5}, false, 2);
        assertThat(first.due(List.of(item))).singleElement().satisfies(due ->
        {
            assertThat(due.progress().complete()).isTrue();
            assertThat(due.progress().nextCompletion()).isEqualTo(2);
            assertThat(due.total()).isEqualTo(new byte[] {5});
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
                StageTestSupport.database(), "x; drop table y", "convert",
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

    /**
     * Rows from before completions were numbered (STAGES.md §15.6f) are
     * numbered 1 when the tables are next made: an item completed then is
     * its first completion, and a complete total taken then the first of
     * the stage before's. A repeat of that total is then stale, and
     * reopening the item withdraws completion 1. A row whose total is
     * still open upstream isn't touched.
     *
     * @throws java.sql.SQLException if the rows can't be set up
     */
    @Test
    void numbersTheCompletionsOfEarlierRows() throws java.sql.SQLException
    {
        UUID open = UUID.randomUUID();
        first.expect(open, 3, new byte[] {9}, true, 0);
        first.expect(item, 1, new byte[] {1}, false, 1);
        first.claim("r1", item, LONG);
        first.store(List.of(entry("r1", false)));
        assertThat(first.due(List.of(item))).hasSize(1);
        first.markCompleted(item);
        // As an earlier build left them: no numbers.
        try (java.sql.Connection connection =
                StageTestSupport.database().getConnection();
                java.sql.Statement statement = connection.createStatement())
        {
            statement.executeUpdate("UPDATE " + schema + ".stage_item"
                    + " SET completion = 0, upstream_completion = 0");
        }

        StageRegistry restarted = StageTestSupport.registry(schema, "pod-c");
        assertThat(audit.names()).contains("stage.completions_numbered",
                "stage.totals_numbered");
        assertThat(restarted.item(item)).hasValueSatisfying(row ->
                assertThat(row.completion()).isEqualTo(1));

        // The same total again: stale, nothing reopened.
        restarted.expect(item, 1, new byte[] {1}, false, 1);
        assertThat(audit.names()).doesNotContain("stage.item_reopened");
        assertThat(restarted.due(List.of(item))).isEmpty();
        // Reopened upstream: completion 1 withdrawn, the next is 2.
        restarted.expect(item, 1, new byte[] {2}, true, 1);
        assertThat(restarted.due(List.of(item))).singleElement()
                .satisfies(due ->
                {
                    assertThat(due.progress().completion()).isEqualTo(1);
                    assertThat(due.progress().nextCompletion())
                            .isEqualTo(2);
                });

        // Open upstream: left as it was, and numbering again changes
        // nothing.
        StageTestSupport.registry(schema, "pod-d");
        try (java.sql.Connection connection =
                StageTestSupport.database().getConnection();
                java.sql.Statement statement = connection.createStatement();
                java.sql.ResultSet rows = statement.executeQuery(
                        "SELECT item_id, completion, upstream_completion"
                        + " FROM " + schema + ".stage_item"))
        {
            while (rows.next())
            {
                boolean isOpen = open.equals(rows.getObject(1));
                assertThat(rows.getInt(2)).isEqualTo(isOpen ? 0 : 1);
                assertThat(rows.getInt(3)).isEqualTo(isOpen ? 0 : 1);
            }
        }
    }

    /**
     * A record at a place, of a lineage, claimed and stored by the first
     * worker.
     *
     * @param recordId the record
     * @param seq      its place
     * @param lineage  its lineage
     * @param failed   whether it failed
     * @return the keys marked stored
     */
    Set<StageRegistry.Key> storeAt(String recordId, long seq,
            Lineage lineage, boolean failed)
    {
        String id = StageRegistry.Key.of(item, recordId, lineage).recordId();
        first.claim(id, item, LONG);
        FailureStore.Failure failure = failed ? new FailureStore.Failure(
                item, seq, recordId, "TEST_FAILED", 10, "v1", lineage)
                : null;
        OutboundMessage message = failed ? null : new OutboundMessage(
                "m-" + id, MessageType.RECORD_STORED, item, id,
                new byte[] {1});
        return first.store(List.of(new StageRegistry.Entry(recordId, item,
                "d-" + id, "v1", failed, message, failure, null, seq,
                lineage)));
    }

    /**
     * The first record of a later generation starts its item again here:
     * the counts and total of the generation before are dropped, and its
     * failures superseded, not counted as resolved (STAGES.md §15.8.3).
     */
    @Test
    void aLaterGenerationStartsTheItemAgain()
    {
        first.expect(item, 2, new byte[] {1});
        storeAt("r1", 1, Lineage.FIRST, false);
        storeAt("r2", 2, Lineage.FIRST, true);
        Lineage second = Lineage.of(2, null);
        assertThat(storeAt("r1", 1, second, false))
                .containsExactly(StageRegistry.Key.of(item, "r1", second));
        ItemProgress progress = first.item(item).orElseThrow();
        assertThat(progress.generation()).isEqualTo(2);
        assertThat(progress.expected()).isEqualTo(-1);
        assertThat(progress.done()).isEqualTo(1);
        assertThat(progress.failed()).isZero();
        assertThat(first.failureStore().counts(item).resolved()).isZero();
        assertThat(first.failureStore().failures(item)).singleElement()
                .extracting(FailureStore.Stored::state)
                .isEqualTo(FailureStore.State.SUPERSEDED);
        assertThat(audit.names()).contains("stage.generation_superseded",
                "stage.generation_started", "failure.superseded");
    }

    /**
     * A record of a generation the stage has already moved past is kept,
     * not counted, and its failure kept superseded; before sealing it is
     * found superseded already.
     */
    @Test
    void aRecordOfAnEarlierGenerationIsSupersededOnArrival()
    {
        storeAt("r1", 1, Lineage.of(2, null), false);
        assertThat(first.supersededAlready(List.of(item, item),
                List.of(1L, 1L), List.of(Lineage.FIRST, Lineage.of(3, null))))
                .containsExactly(0);
        assertThat(storeAt("r1", 1, Lineage.FIRST, true))
                .containsExactly(key("r1"));
        ItemProgress progress = first.item(item).orElseThrow();
        assertThat(progress.generation()).isEqualTo(2);
        assertThat(progress.done()).isEqualTo(1);
        assertThat(progress.failed()).isZero();
        assertThat(first.failureStore().failures(item)).singleElement()
                .satisfies(stored ->
                {
                    assertThat(stored.state())
                            .isEqualTo(FailureStore.State.SUPERSEDED);
                    assertThat(stored.failure().lineage())
                            .isEqualTo(Lineage.FIRST);
                });
        assertThat(audit.names()).contains("record.superseded_on_arrival");
    }

    /**
     * A newer revision at a place replaces the one there: it isn't
     * counted again, a failure it replaces counts as resolved, and an
     * older revision arriving after it is superseded on arrival.
     */
    @Test
    void aNewerRevisionReplacesTheOneAtItsPlace()
    {
        Lineage second = Lineage.FIRST.withRevision(Lineage.CONVERT, 2);
        Lineage third = Lineage.FIRST.withRevision(Lineage.CONVERT, 3);
        storeAt("r1", 1, Lineage.FIRST, true);
        storeAt("r1", 1, third, false);
        ItemProgress progress = first.item(item).orElseThrow();
        assertThat(progress.done()).isEqualTo(1);
        assertThat(progress.failed()).isEqualTo(1);
        assertThat(first.failureStore().counts(item).resolved()).isEqualTo(1);
        assertThat(first.failureStore().failure(item, 1)).get()
                .extracting(FailureStore.Stored::state)
                .isEqualTo(FailureStore.State.SUPERSEDED);
        storeAt("r1", 1, second, true);
        progress = first.item(item).orElseThrow();
        assertThat(progress.done()).isEqualTo(1);
        assertThat(progress.failed()).isEqualTo(1);
        assertThat(first.failureStore().failures(item))
                .extracting(FailureStore.Stored::state)
                .containsOnly(FailureStore.State.SUPERSEDED);
        assertThat(audit.names()).contains("record.superseded_on_arrival");
    }

    /**
     * A newer revision that fails, in place of one that passed, leaves the
     * place failed.
     */
    @Test
    void aFailedRevisionTakesThePlaceOfOneThatPassed()
    {
        storeAt("r1", 1, Lineage.FIRST, false);
        storeAt("r1", 1, Lineage.FIRST.withRevision(Lineage.CONVERT, 2),
                true);
        ItemProgress progress = first.item(item).orElseThrow();
        assertThat(progress.done()).isEqualTo(1);
        assertThat(progress.failed()).isEqualTo(1);
        assertThat(first.failureStore().counts(item).resolved()).isZero();
        assertThat(first.failureStore().failure(item, 1)).get()
                .extracting(FailureStore.Stored::state)
                .isEqualTo(FailureStore.State.FAILED);
    }

    /**
     * A total of a generation the stage has moved past is dropped; one of
     * a later generation starts the item again.
     */
    @Test
    void aTotalIsWeighedByItsGeneration()
    {
        storeAt("r1", 1, Lineage.FIRST, false);
        first.expect(item, 3, new byte[] {1}, false, 0, 2);
        ItemProgress progress = first.item(item).orElseThrow();
        assertThat(progress.generation()).isEqualTo(2);
        assertThat(progress.expected()).isEqualTo(3);
        assertThat(progress.done()).isZero();
        first.expect(item, 5, new byte[] {1}, false, 1, 1);
        assertThat(first.item(item).orElseThrow().expected()).isEqualTo(3);
        assertThat(audit.names()).contains("stage.total_superseded");
    }

    /**
     * Runs statements on the test's database.
     *
     * @param sql the statements, each formatted with the schema
     * @throws java.sql.SQLException if one fails
     */
    void sql(String... sql) throws java.sql.SQLException
    {
        try (java.sql.Connection connection = StageTestSupport.database()
                .getConnection();
                java.sql.Statement statement = connection.createStatement())
        {
            for (String each : sql)
            {
                statement.execute(each.formatted(schema));
            }
        }
    }

    /**
     * One value from the test's item's failure at a number that is
     * superseded.
     *
     * @param seq    the failure's number
     * @param column the column, as SQL
     * @return its value, as text
     * @throws java.sql.SQLException if it can't be read
     */
    String superseded(int seq, String column) throws java.sql.SQLException
    {
        try (java.sql.Connection connection = StageTestSupport.database()
                .getConnection();
                java.sql.Statement statement = connection.createStatement();
                java.sql.ResultSet rows = statement.executeQuery(
                        "SELECT " + column + " FROM " + schema
                                + ".stage_failure WHERE item_id = '" + item
                                + "' AND seq = " + seq
                                + " AND state = 'SUPERSEDED'"))
        {
            rows.next();
            return rows.getString(1);
        }
    }

    /**
     * An item's count of failed records, as the registry holds it.
     *
     * @return the count
     * @throws java.sql.SQLException if it can't be read
     */
    long failedCount() throws java.sql.SQLException
    {
        try (java.sql.Connection connection = StageTestSupport.database()
                .getConnection();
                java.sql.Statement statement = connection.createStatement();
                java.sql.ResultSet rows = statement.executeQuery(
                        "SELECT failed FROM " + schema + ".stage_item"
                                + " WHERE item_id = '" + item + "'"))
        {
            rows.next();
            return rows.getLong(1);
        }
    }

    /**
     * A failure of the test's item as written before revision (cn).
     *
     * @param seq        its number
     * @param state      its state then
     * @param resolvedBy what passed in its place, as SQL
     * @return the statement, formatted with the schema
     */
    String oldFailure(int seq, String state, String resolvedBy)
    {
        return "INSERT INTO %s.stage_failure (stage, item_id, seq,"
                + " record_id, error_code, source_offset, failed_version,"
                + " failed_at, state, changed_at, resolved_by) VALUES"
                + " ('convert', '" + item + "', " + seq + ", 'r" + seq
                + "', 'TEST_FAILED', 10, 'v1', now(), '" + state
                + "', now(), " + resolvedBy + ")";
    }

    /**
     * A correction of the test's item as written before revision (cn).
     *
     * @param seq        its failure's number
     * @param state      its state
     * @param finishedAt when it was processed, as SQL
     * @param errorCode  why it failed, as SQL
     * @return the statement, formatted with the schema
     */
    String oldCorrection(int seq, String state, String finishedAt,
            String errorCode)
    {
        return "INSERT INTO %s.stage_correction (stage, item_id, seq,"
                + " correction, state, submitted_at, submitted_by,"
                + " content_enc, content_key, content_kek, content_length,"
                + " finished_at, error_code) VALUES ('convert', '" + item
                + "', " + seq + ", 1, '" + state + "', now(), 'alice',"
                + " '\\x00', '\\x00', 'kek', 1, " + finishedAt + ", "
                + errorCode + ")";
    }

    /**
     * Failures in the states that went are moved once, at start (STAGES.md
     * §15.8.5): CORRECTION_PENDING to REVISION_PENDING, its correction's
     * revision noted; CORRECTED to SUPERSEDED by its correction's
     * revision, the record that passed kept; FAILED_CORRECTION to
     * SUPERSEDED, with its correction's failure added as a FAILED row of
     * the next revision, and counted among the item's failed records. Each
     * is changed by the pipeline, and a row an earlier build moved naming
     * its record is named by its revision, keeping when it changed. A
     * second start moves nothing.
     *
     * @throws java.sql.SQLException if the rows can't be written
     */
    @Test
    void movesFailuresOutOfTheStatesThatWent() throws java.sql.SQLException
    {
        sql(oldFailure(1, "CORRECTION_PENDING", "NULL"),
                oldFailure(2, "CORRECTED", "'r2-fixed'"),
                oldFailure(3, "FAILED_CORRECTION", "NULL"),
                oldFailure(4, "SUPERSEDED", "'r4-fixed'"),
                "UPDATE %s.stage_failure SET superseded_how = 'REVISION',"
                        + " superseded_by = 'r4-fixed', changed_by = 'alice',"
                        + " changed_at = '2026-11-09 10:00:00+00'"
                        + " WHERE seq = 4",
                oldCorrection(1, "PENDING", "NULL", "NULL"),
                oldCorrection(3, "FAILED", "now()", "'FIX_FAILED'"),
                "INSERT INTO %s.stage_item (stage, item_id, done, failed)"
                        + " VALUES ('convert', '" + item + "', 3, 3)");

        StageTestSupport.registry(schema, "pod-c");
        Lineage second = Lineage.FIRST.nextRevision(Lineage.CONVERT);
        FailureStore store = first.failureStore();
        assertThat(store.failure(item, 1)).get()
                .extracting(FailureStore.Stored::state)
                .isEqualTo(FailureStore.State.REVISION_PENDING);
        assertThat(first.correctionStore().pending(10)).singleElement()
                .satisfies(pending ->
                {
                    assertThat(pending.lineage()).isEqualTo(second);
                    assertThat(pending.recordId()).isEqualTo("r1");
                });
        assertThat(store.failure(item, 2)).get()
                .extracting(FailureStore.Stored::state)
                .isEqualTo(FailureStore.State.SUPERSEDED);
        assertThat(store.events(item, 2)).singleElement().satisfies(event ->
        {
            assertThat(event.from()).isEqualTo("CORRECTED");
            assertThat(event.to()).isEqualTo("SUPERSEDED");
            assertThat(event.lineage()).isEqualTo(Lineage.FIRST);
        });
        for (int seq : new int[] {2, 3, 4})
        {
            assertThat(superseded(seq, "superseded_by"))
                    .isEqualTo(second.toString());
            assertThat(superseded(seq, "changed_by")).isEqualTo("pipeline");
        }
        assertThat(superseded(2, "resolved_by")).isEqualTo("r2-fixed");
        assertThat(superseded(4, "resolved_by")).isEqualTo("r4-fixed");
        assertThat(superseded(4, "changed_at = '2026-11-09 10:00:00+00'"))
                .isEqualTo("t");
        assertThat(store.failures(item))
                .filteredOn(stored -> stored.failure().seq() == 3)
                .satisfiesExactly(
                        open ->
                        {
                            assertThat(open.state())
                                    .isEqualTo(FailureStore.State.FAILED);
                            assertThat(open.failure().lineage())
                                    .isEqualTo(second);
                            assertThat(open.failure().errorCode())
                                    .isEqualTo("FIX_FAILED");
                        },
                        moved -> assertThat(moved.state())
                                .isEqualTo(FailureStore.State.SUPERSEDED));
        assertThat(store.counts(item))
                .isEqualTo(new FailureStore.Counts(2, 0, 3));
        assertThat(failedCount()).isEqualTo(4);
        assertThat(audit.names()).contains("failure.states_moved");

        StageTestSupport.registry(schema, "pod-d");
        assertThat(failedCount()).isEqualTo(4);
        assertThat(store.failures(item)).hasSize(5);
    }

    /**
     * A correction whose failure something newer superseded while it
     * waited is still processed and stored, superseded on arrival, not
     * counted, and its correction noted (STAGES.md §15.8.5).
     */
    @Test
    void aCorrectionOvertakenByANewerRevisionIsStoredLate()
    {
        storeAt("r1", 1, Lineage.FIRST, true);
        first.correctionStore().submit(item, 1, new byte[] {1},
                StageTestSupport.CORRECTION_KEYS,
                StageTestSupport.CORRECTION_KEK, "alice");
        storeAt("r1", 1, Lineage.FIRST.withRevision(Lineage.CONVERT, 3),
                false);
        CorrectionStore.Pending pending = first.correctionStore()
                .pending(10).get(0);
        assertThat(pending.lineage()).isEqualTo(Lineage.FIRST
                .nextRevision(Lineage.CONVERT));

        first.claim(pending.claimId(), item, LONG);
        StageRegistry.Entry entry = new StageRegistry.Entry("r1", item,
                "d-c1", "v1", false, new OutboundMessage("m-c1",
                        MessageType.RECORD_STORED, item, pending.claimId(),
                        new byte[] {1}), null, new CorrectionStore.Outcome(
                                pending, true, "r1", null, "v1"), 1,
                pending.lineage());
        assertThat(first.store(List.of(entry))).containsExactly(entry.key());
        ItemProgress progress = first.item(item).orElseThrow();
        assertThat(progress.done()).isEqualTo(1);
        assertThat(progress.failed()).isEqualTo(1);
        assertThat(first.correctionStore().list(item, 1)).singleElement()
                .extracting(CorrectionStore.Listed::state)
                .isEqualTo(CorrectionStore.State.PASSED);
        assertThat(first.failureStore().failures(item))
                .extracting(FailureStore.Stored::state)
                .containsOnly(FailureStore.State.SUPERSEDED);
        assertThat(audit.names()).contains("record.superseded_on_arrival",
                "failure.corrected");
    }
}
