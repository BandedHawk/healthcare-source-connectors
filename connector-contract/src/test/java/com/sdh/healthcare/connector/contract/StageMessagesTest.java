/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemConverted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemEnd;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemTransformed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordLocation;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordTransformed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.contract.message.StageMessages;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Each record stage's messages (STAGES.md §17.4): what it takes from the
 * stage before, and what it makes.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StageMessagesTest
{

    /** The item the messages are about. */
    final UUID itemId = UUID.randomUUID();

    /** Where the records are stored. */
    final StorageRef ref = new StorageRef("sdh", "sdh_ingest", "records",
            LocalDate.of(2026, 11, 10), 42L, "records-kek");

    /** Creates the tests; JUnit runs each test on a new instance. */
    StageMessagesTest()
    {
    }

    /**
     * A stage is found by its name, and only a record stage is.
     */
    @Test
    void findsAStageByName()
    {
        assertThat(StageMessages.of("convert"))
                .isEqualTo(StageMessages.CONVERT);
        assertThat(StageMessages.of("transform"))
                .isEqualTo(StageMessages.TRANSFORM);
        assertThat(StageMessages.TRANSFORM.upstream()).isEqualTo("convert");
        assertThatThrownBy(() -> StageMessages.of("split"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The convert stage takes the split stage's output, and the transform
     * stage the convert stage's: each its record messages, its item ends
     * and its progress, and nothing of any other stage.
     */
    @Test
    void takesOnlyTheStageBeforesOutput()
    {
        RecordSplit split = new RecordSplit("ID1", itemId, 0, 0, 10, "ccda",
                "application/xml", 10, 1, ref, null, null, null, -1, null,
                null);
        ItemSplit splitEnd = new ItemSplit(itemId, "claims", 1, 0, 10, "d",
                null, null, null, Instant.EPOCH, Instant.EPOCH);
        RecordStored stored = new RecordStored("ID2", itemId, 0, 0, 10,
                "ccda", "application/json", "ccda/2", 20, 1, ref);
        ItemConverted converted = new ItemConverted(itemId, "claims", 1, 0,
                Instant.EPOCH);

        assertThat(StageMessages.CONVERT.takes(split)).isTrue();
        assertThat(StageMessages.CONVERT.takes(splitEnd)).isTrue();
        assertThat(StageMessages.CONVERT.takes(progress("split"))).isTrue();
        assertThat(StageMessages.CONVERT.takes(stored)).isFalse();
        assertThat(StageMessages.CONVERT.takes(progress("convert")))
                .isFalse();

        assertThat(StageMessages.TRANSFORM.takes(stored)).isTrue();
        assertThat(StageMessages.TRANSFORM.takes(converted)).isTrue();
        assertThat(StageMessages.TRANSFORM.takes(progress("convert")))
                .isTrue();
        assertThat(StageMessages.TRANSFORM.takes(split)).isFalse();
        assertThat(StageMessages.TRANSFORM.takes(splitEnd)).isFalse();
        assertThat(StageMessages.TRANSFORM.takes(progress("split")))
                .isFalse();
    }

    /**
     * Each stage makes its own record message and item end, of the types it
     * says, with the producer's references carried on.
     */
    @Test
    void makesEachStagesOwnMessages()
    {
        Lineage lineage = new Lineage(1, List.of(1, 2));
        for (StageMessages stage : StageMessages.values())
        {
            RecordInStorage record = stage.record("ID3", itemId, 4, 30, 20,
                    "ccda", "application/json", "m/1", 40, 1, ref, null, "v",
                    null, lineage);
            assertThat(record.type()).isEqualTo(stage.record());
            assertThat(record.lineage()).isEqualTo(lineage);
            assertThat(record.superseded()).isFalse();
            assertThat(record.cause()).isNull();
            UUID replaces = UUID.randomUUID();
            ItemEnd end = stage.itemEnd(itemId, "claims", 5, 1,
                    Instant.EPOCH, List.of("v"), 1, 1, null, "a.xml",
                    replaces);
            assertThat(end.type()).isEqualTo(stage.end());
            assertThat(end.producerRef()).isEqualTo("a.xml");
            assertThat(end.replaces()).isEqualTo(replaces);
        }
        assertThat(StageMessages.CONVERT.record("ID3", itemId, 4, 30, 20,
                "ccda", "application/json", "ccda/2", 40, 1, ref, null, "v",
                null, null)).isInstanceOf(RecordStored.class);
        assertThat(StageMessages.TRANSFORM.record("ID3", itemId, 4, 30, 20,
                "ccda", "application/fhir+json", "us-core-6.1@ab", 40, 1, ref,
                null, "v", null, null))
                .isInstanceOf(RecordTransformed.class);
        assertThat(StageMessages.TRANSFORM.itemEnd(itemId, "claims", 5, 0,
                Instant.EPOCH, null, 1, 1, null, null, null))
                .isInstanceOf(ItemTransformed.class);
        assertThat(StageMessages.TRANSFORM.end())
                .isEqualTo(MessageType.ITEM_TRANSFORMED);
    }

    /**
     * Every record message gives its location, and a cause set on it
     * through the shared interface stays on the message's own type.
     */
    @Test
    void recordMessagesGiveTheirLocationAndTakeACause()
    {
        RecordTransformed mapped = new RecordTransformed("ID4", itemId, 2,
                10, 20, "ccda", "application/fhir+json", "us-core-6.1@ab",
                900, 3, ref, null, "v", null, null, false, null);
        assertThat(mapped.location()).isEqualTo(new RecordLocation("ID4",
                itemId, 2, 900, 3, ref));

        ConnectorMessage.Cause cause = new ConnectorMessage.Cause(
                ConnectorMessage.Cause.REPROCESS, UUID.randomUUID(), null);
        RecordInStorage again = ((RecordInStorage) mapped).withCause(cause);
        assertThat(again).isInstanceOf(RecordTransformed.class);
        assertThat(again.cause()).isEqualTo(cause);
        assertThat(again.messageId()).isEqualTo(mapped.messageId());
    }

    /**
     * An item's progress, as a stage reports it.
     *
     * @param stage the stage reporting
     * @return the message
     */
    ItemProgressed progress(String stage)
    {
        return new ItemProgressed(itemId, "claims", stage, 3, 2, 1, 0, null,
                null, null, Instant.EPOCH, null);
    }
}
