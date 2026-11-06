/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import java.util.UUID;

/**
 * An item whose records have all been stored by a stage (STAGES.md §5).
 *
 * @param itemId   the item's id
 * @param expected how many records it has
 * @param done     how many the stage stored, failed ones included
 * @param failed   how many of those failed
 * @param total    the message that gave its total, opened; null if it
 *                 isn't kept or no longer opens
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record ItemProgress(UUID itemId, long expected, long done,
        long failed, ConnectorMessage total)
{

    /**
     * An item's counts, without the message that gave its total.
     *
     * @param itemId   the item's id
     * @param expected how many records it has
     * @param done     how many the stage stored, failed ones included
     * @param failed   how many of those failed
     */
    public ItemProgress(UUID itemId, long expected, long done, long failed)
    {
        this(itemId, expected, done, failed, null);
    }

    /**
     * The same counts, with the message that gave the total.
     *
     * @param message the message, opened; null if none
     * @return the progress
     */
    public ItemProgress withTotal(ConnectorMessage message)
    {
        return new ItemProgress(itemId, expected, done, failed, message);
    }
}
