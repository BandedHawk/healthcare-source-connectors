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
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.time.Duration;
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
        return new StageRegistry.Entry(recordId, item, "d-" + recordId,
                "v1", failed, new OutboundMessage("m-" + recordId,
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
        assertThat(second.claim("r2", item, LONG)).isInstanceOfSatisfying(
                StageRegistry.Stored.class, row ->
                        assertThat(row.pending()).isNotNull());
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
        assertThat(due).extracting(StageRegistry.DueItem::progress)
                .containsExactly(new ItemProgress(item, 2, 2, 1));
        assertThat(due.get(0).total()).isEqualTo(firstTotal);
        assertThat(second.due(List.of(item))).hasSize(1);

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
}
