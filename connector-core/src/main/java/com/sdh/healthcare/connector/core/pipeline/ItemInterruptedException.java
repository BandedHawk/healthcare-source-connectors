/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import java.util.UUID;

/**
 * Splitting an item stopped between batches because a newer generation of
 * it is waiting (STAGES.md §15.8.6). Everything committed so far stays;
 * the caller ends the generation as {@code INTERRUPTED}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ItemInterruptedException extends Exception
{

    /** For serialization. */
    private static final long serialVersionUID = 1L;

    /** The item. */
    private final UUID itemId;

    /** The generation waiting. */
    private final int newer;

    /**
     * Creates the exception.
     *
     * @param itemId the item
     * @param newer  the generation waiting
     */
    public ItemInterruptedException(UUID itemId, int newer)
    {
        super("Item " + itemId + " interrupted for generation " + newer);
        this.itemId = itemId;
        this.newer = newer;
    }

    /**
     * The item.
     *
     * @return its ID
     */
    public UUID itemId()
    {
        return itemId;
    }

    /**
     * The generation waiting.
     *
     * @return its number
     */
    public int newer()
    {
        return newer;
    }
}
