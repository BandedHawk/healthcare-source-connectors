/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.Lineage;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A stage's input as the stage before stored it (STAGES.md §15.8.4): what
 * a reprocess chooses from, checks again when it queues, and sends.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@FunctionalInterface
public interface ReprocessInputs
{

    /**
     * An object of the input, current at its place.
     *
     * @param chosen  its place, lineage and record ID
     * @param failed  whether the stage before failed it, so it was never
     *                sent on and can't be reprocessed
     * @param message its message as it would have been sent, at its own
     *                lineage; null if it failed
     */
    record Input(ReprocessStore.Chosen chosen, boolean failed,
            ConnectorMessage.RecordInStorage message)
    {
    }

    /**
     * The current object at each place of an item's newest generation, by
     * record number.
     *
     * @param itemId the item
     * @return the objects, in record order; empty if the item has none
     * @throws Exception if the input can't be read now
     */
    List<Input> current(UUID itemId) throws Exception;

    /**
     * Why an object chosen can't be queued, given its item's current
     * objects: {@link ReprocessStore#SUPERSEDED} if its place, or its item,
     * holds something newer; {@link ReprocessStore#PAST_RETENTION} if it is
     * no longer stored.
     *
     * @param chosen  the object
     * @param current its item's current objects ({@link #current})
     * @return the reason; null if it may be queued
     */
    static String refusal(ReprocessStore.Chosen chosen, List<Input> current)
    {
        for (Input input : current)
        {
            Lineage now = input.chosen().input();
            if (now.generation() > chosen.input().generation())
            {
                return ReprocessStore.SUPERSEDED;
            }
            if (input.chosen().seq() != chosen.seq())
            {
                continue;
            }
            if (now.compareTo(chosen.input()) > 0)
            {
                return ReprocessStore.SUPERSEDED;
            }
            if (now.equals(chosen.input()) && !input.failed()
                    && input.chosen().recordId().equals(chosen.recordId()))
            {
                return null;
            }
        }
        return ReprocessStore.PAST_RETENTION;
    }

    /**
     * The message to reprocess an object queued as: its input at the line
     * given.
     *
     * @param queued  the object and its line
     * @param current its item's current objects
     * @return the message; null if its input isn't stored any more
     */
    static ConnectorMessage.RecordInStorage rebuild(
            ReprocessStore.Queued queued, Map<UUID, List<Input>> current)
    {
        ReprocessStore.Chosen chosen = queued.chosen();
        for (Input input : current.getOrDefault(chosen.itemId(), List.of()))
        {
            if (input.message() != null && input.chosen().seq() == chosen.seq()
                    && input.chosen().input().equals(chosen.input())
                    && input.chosen().recordId().equals(chosen.recordId()))
            {
                return input.message().withLineage(queued.line(), false);
            }
        }
        return null;
    }
}
