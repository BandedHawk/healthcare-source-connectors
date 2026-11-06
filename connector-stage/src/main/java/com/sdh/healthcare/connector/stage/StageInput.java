/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import java.util.UUID;

/**
 * What a message received by a stage asks of it, as its processor reads
 * it: a record to process, the number of records an item has, or nothing.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public sealed interface StageInput
{

    /**
     * A record to process.
     *
     * @param recordId the record's id
     * @param itemId   the id of the item it came from; with the record id,
     *                 the registry's key (STAGES.md §6)
     */
    record RecordInput(String recordId, UUID itemId) implements StageInput
    {

        /**
         * What the registry knows the record by.
         *
         * @return its key
         */
        StageRegistry.Key key()
        {
            return new StageRegistry.Key(itemId, recordId);
        }
    }

    /**
     * How many records an item has, from the stage before's item-complete
     * message (STAGES.md §5).
     *
     * @param itemId   the item's id
     * @param expected how many records of it the stage will receive
     */
    record ItemTotal(UUID itemId, long expected) implements StageInput
    {
    }

    /**
     * A message the stage doesn't take, such as one of another type. It is
     * audited and acknowledged: delivering it again won't change it.
     *
     * @param reason why, free of PHI, such as {@code wrong_type}
     */
    record Unwanted(String reason) implements StageInput
    {
    }
}
