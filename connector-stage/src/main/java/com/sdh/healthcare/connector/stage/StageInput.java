/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.message.Lineage;
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
     * @param seq      its sequence number in the item, from 0: what
     *                 telemetry names it by, with the item, never by its
     *                 id; -1 if it has none
     * @param lineage  the lineage the stage makes it under (STAGES.md
     *                 §15.8.2): the input's, with this stage's revision;
     *                 with the record id, its name in the registry
     */
    record RecordInput(String recordId, UUID itemId, long seq,
            Lineage lineage)
            implements StageInput
    {

        /**
         * Reads no lineage as the first.
         */
        public RecordInput
        {
            lineage = Lineage.orFirst(lineage);
        }

        /**
         * A record of the first lineage.
         *
         * @param recordId the record's id
         * @param itemId   the id of the item it came from
         * @param seq      its sequence number in the item, or -1
         */
        public RecordInput(String recordId, UUID itemId, long seq)
        {
            this(recordId, itemId, seq, null);
        }

        /**
         * What the registry knows the record by.
         *
         * @return its key
         */
        StageRegistry.Key key()
        {
            return StageRegistry.Key.of(itemId, recordId, lineage);
        }
    }

    /**
     * How many records an item has, from the stage before's item-complete
     * message (STAGES.md §5), or its {@code item.progress}.
     *
     * @param itemId   the item's id
     * @param expected how many records of it the stage will receive, or
     *                 has so far
     * @param partial    whether it is from the stage before's
     *                   {@code item.progress}: that stage still has
     *                   failures open, so the total may grow, and the item
     *                   can't complete here yet (STAGES.md §15.6)
     * @param completion the stage before's completion number: of its
     *                   item-complete message, from 1, or for
     *                   {@code item.progress} its completions before it, so
     *                   one reopening a completion taken here reopens the
     *                   item (STAGES.md §15.6e), and one from before it is
     *                   stale
     * @param generation the item's generation the total is of, from 1
     *                   (STAGES.md §15.8): one of an earlier generation than
     *                   the item's here is stale, and one of a later moves
     *                   the item to it
     */
    record ItemTotal(UUID itemId, long expected, boolean partial,
            int completion, int generation)
            implements StageInput
    {

        /**
         * Reads a generation not given as the first.
         */
        public ItemTotal
        {
            generation = Math.max(1, generation);
        }

        /**
         * A total of the item's first generation.
         *
         * @param itemId     the item's id
         * @param expected   how many records of it the stage will receive,
         *                   or has so far
         * @param partial    whether it is from {@code item.progress}
         * @param completion the stage before's completion number
         */
        public ItemTotal(UUID itemId, long expected, boolean partial,
                int completion)
        {
            this(itemId, expected, partial, completion, 1);
        }

        /**
         * A total from the stage before's first completion, or from its
         * {@code item.progress} before that.
         *
         * @param itemId   the item's id
         * @param expected how many records of it the stage will receive,
         *                 or has so far
         * @param partial  whether it is from {@code item.progress}
         */
        public ItemTotal(UUID itemId, long expected, boolean partial)
        {
            this(itemId, expected, partial, partial ? 0 : 1);
        }

        /**
         * A final total, from the stage before's item-complete message.
         *
         * @param itemId   the item's id
         * @param expected how many records of it the stage will receive
         */
        public ItemTotal(UUID itemId, long expected)
        {
            this(itemId, expected, false);
        }
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
