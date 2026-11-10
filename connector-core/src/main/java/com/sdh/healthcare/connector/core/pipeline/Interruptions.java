/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import java.util.UUID;

/**
 * Tells the split stage, between batches, whether a newer generation of the
 * item it is splitting is waiting (STAGES.md §15.8.6), so it stops where it
 * is rather than cut the rest of a generation that is already superseded.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@FunctionalInterface
public interface Interruptions
{

    /** Never interrupts: for a stage that doesn't reprocess. */
    Interruptions NONE = (itemId, generation) -> 0;

    /**
     * The newer generation waiting for an item, if any.
     *
     * @param itemId     the item
     * @param generation the generation being split
     * @return the newer generation; 0 if none is waiting
     * @throws Exception if it can't be looked at; splitting goes on
     */
    int newer(UUID itemId, int generation) throws Exception;
}
