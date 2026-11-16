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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for {@link ItemEndResender}, on a real Postgres: the stage before
 * reads the marks the stage after's reconciler makes, and sends the ends
 * they ask for again (STAGES.md §13.2).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ItemEndResenderTest
{

    /** A claim that outlives any test. */
    static final Duration LONG = Duration.ofMinutes(5);

    /** The audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** The stage after's registry. */
    StageRegistry next;

    /** Its schema. */
    String schema;

    /** Creates the tests; JUnit runs each test on a new instance. */
    ItemEndResenderTest()
    {
    }

    /** Gives each test its own tables. */
    @BeforeEach
    void tables()
    {
        schema = StageTestSupport.freshSchema();
        next = StageTestSupport.registry(schema, "pod-a");
    }

    /**
     * An item with a record stored at the stage after, and its total
     * marked as wanted.
     *
     * @return the item
     */
    UUID wanting()
    {
        UUID itemId = UUID.randomUUID();
        next.claim("r1", itemId, LONG);
        next.store(List.of(new StageRegistry.Entry("r1", itemId, "d-r1",
                "v1", false, new OutboundMessage("m-r1",
                        MessageType.RECORD_STORED, itemId, "r1",
                        new byte[] {1}))));
        next.wantTotal(itemId);
        return itemId;
    }

    /**
     * Each wanted item is asked for, the longest wanted first; one sent is
     * counted and audited, one the stage hasn't ended isn't, and one that
     * fails is left for the next run without stopping the others.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void sendsAgainEachEndTheStageAfterWants() throws Exception
    {
        UUID failing = wanting();
        UUID unended = wanting();
        UUID ended = wanting();
        List<UUID> asked = new ArrayList<>();
        ItemEndResender resender = ItemEndResender.of(
                StageTestSupport.database(), schema, "split", itemId ->
                {
                    asked.add(itemId);
                    if (itemId.equals(failing))
                    {
                        throw new IllegalStateException("broker down");
                    }
                    return itemId.equals(ended);
                }, Duration.ofSeconds(10));

        assertThat(resender.resend()).isOne();
        assertThat(asked).containsExactly(failing, unended, ended);
        assertThat(audit.names()).containsExactly("stage.item_end_resent");
        assertThat(audit.attributes("stage.item_end_resent"))
                .containsEntry("sdh.stage", "split")
                .containsEntry("sdh.item_id", ended.toString());
    }

    /**
     * There is no resender without the next stage's schema; a stage with
     * no record stage after it can't have one; one whose next stage hasn't
     * made its registry yet finds nothing wanted; the name must be plain.
     */
    @Test
    void isMadeOnlyForAStageWithOneAfterIt()
    {
        assertThat(ItemEndResender.of(StageTestSupport.database(), null,
                "split", itemId -> true, LONG)).isNull();
        assertThat(ItemEndResender.of(StageTestSupport.database(), " ",
                "convert", itemId -> true, LONG)).isNull();
        assertThat(ItemEndResender.of(StageTestSupport.database(), schema,
                "convert", itemId -> true, LONG)).isNotNull();
        assertThatThrownBy(() -> ItemEndResender.of(
                StageTestSupport.database(), schema, "transform",
                itemId -> true, LONG))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sdh.stage.next-registry-schema");
        // A stage after that hasn't started yet wants nothing.
        assertThat(ItemEndResender.of(StageTestSupport.database(),
                StageTestSupport.freshSchema(), "split", itemId -> true,
                LONG).wanted()).isEmpty();
        assertThatThrownBy(() -> ItemEndResender.of(
                StageTestSupport.database(), "x; drop", "split",
                itemId -> true, LONG))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
