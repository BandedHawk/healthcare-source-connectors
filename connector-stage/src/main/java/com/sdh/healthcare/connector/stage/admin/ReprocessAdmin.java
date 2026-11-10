/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.stage.ReprocessInputs;
import com.sdh.healthcare.connector.stage.ReprocessStore;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * What the {@code reprocess} command ({@link ReprocessCommand}) needs of a
 * stage (STAGES.md §15.8.4): its runs, its input, and its version, with
 * the keys an operator's reasons are encrypted with.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface ReprocessAdmin extends FailureAdmin
{

    /**
     * The stage's runs, to read: over the commands' read-only connection.
     *
     * @return the store
     * @throws Exception if it can't be opened
     */
    ReprocessStore reprocessRuns() throws Exception;

    /**
     * The stage's runs, to record or cancel one: over the connection that
     * may write.
     *
     * @return the store
     * @throws Exception if it can't be opened
     */
    ReprocessStore reprocessRunsToChange() throws Exception;

    /**
     * The stage's input, as the stage before stored it.
     *
     * @return the input
     * @throws Exception if it can't be opened
     */
    ReprocessInputs reprocessInputs() throws Exception;

    /**
     * The stage's version a run is asked under: its build.
     *
     * @return the version
     */
    String version();

    /**
     * Whether the stage reprocesses whole items only, each as its next
     * generation (the split stage, STAGES.md §15.8.4), so {@code --seq} is
     * refused.
     *
     * @return whether it does
     */
    default boolean wholeItems()
    {
        return false;
    }

    /**
     * The places whose current output here was made by a version, by item.
     *
     * @param version the version
     * @param itemIds narrows it to these items; empty for any
     * @param limit   the most places returned
     * @return the record numbers of each item's places, in order
     * @throws Exception if the stage's tables can't be read
     */
    default Map<UUID, Set<Long>> madeBy(String version, List<UUID> itemIds,
            int limit) throws Exception
    {
        Map<UUID, Set<Long>> places = new LinkedHashMap<>();
        for (Object[] place : reprocessRuns().madeBy(version, itemIds, limit))
        {
            places.computeIfAbsent((UUID) place[0],
                    item -> new LinkedHashSet<>()).add((Long) place[1]);
        }
        return places;
    }

    /**
     * The items landed in a range of time, for {@code --landed-from} and
     * {@code --landed-to}: the split stage's only (STAGES.md §15.8.4).
     *
     * @param intake the intake given; null for the stage's own
     * @param from   the first instant, included
     * @param to     the last instant, excluded
     * @param limit  the most returned
     * @return the items, earliest first
     * @throws AdminArgs.UsageException if the stage doesn't choose by
     *                                  landing time, or the intake isn't
     *                                  its own
     * @throws Exception                if landing can't be read
     */
    default List<UUID> landedBetween(String intake, Instant from, Instant to,
            int limit) throws Exception
    {
        throw new AdminArgs.UsageException("--landed-from and --landed-to"
                + " choose at the split stage only: here choose by"
                + " --item-id or --made-by");
    }
}
