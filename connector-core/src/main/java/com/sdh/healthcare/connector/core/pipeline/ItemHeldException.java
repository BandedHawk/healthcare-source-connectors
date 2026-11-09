/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import java.util.UUID;

/**
 * An item part way through splitting, begun under another version of the
 * split stage, is held rather than resumed (STAGES.md §14.3): a changed
 * splitter can cut differently, and records are named by their content,
 * so cuts from two versions in one item could overlap or leave gaps unseen.
 * Nothing is wrong with the item; it waits, {@code HELD}, for an operator to
 * roll back, resume it under this version, or land it again. Other items
 * carry on.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ItemHeldException extends Exception
{

    /** Serial version. */
    private static final long serialVersionUID = 1L;

    /** The item held. */
    private final UUID itemId;

    /** The version it was begun under; null if not known. */
    private final String heldVersion;

    /**
     * Creates the exception.
     *
     * @param itemId      the item
     * @param heldVersion the version it was begun under
     */
    public ItemHeldException(UUID itemId, String heldVersion)
    {
        super("The item is held: it was begun under another version of the"
                + " split stage");
        this.itemId = itemId;
        this.heldVersion = heldVersion;
    }

    /**
     * The item held.
     *
     * @return its ID
     */
    public UUID itemId()
    {
        return itemId;
    }

    /**
     * The version it was begun under.
     *
     * @return the version; null if not known
     */
    public String heldVersion()
    {
        return heldVersion;
    }
}
