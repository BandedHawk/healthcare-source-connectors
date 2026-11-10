/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import java.util.List;
import java.util.UUID;

/**
 * Finds a stage's items part way (STAGES.md §14.4a): those with output
 * under a version that the stage would still add to without an operator.
 * An item waiting only on the stage before, with no output yet, or on an
 * operator (an open failure, a held item, a pending correction, a queued
 * reprocess) isn't part way.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@FunctionalInterface
public interface PartWay
{

    /**
     * The items part way under one version.
     *
     * @param version the version
     * @param count   how many items
     * @param first   the first of them, by ID, at most as many as asked
     */
    record Items(String version, long count, List<UUID> first)
    {

        /**
         * Copies the items.
         */
        public Items
        {
            first = List.copyOf(first);
        }
    }

    /**
     * The items part way, by version.
     *
     * @param first how many item IDs to give for each version
     * @return a line per version, each version once
     * @throws IllegalStateException if the database can't be reached
     */
    List<Items> partWay(int first);
}
