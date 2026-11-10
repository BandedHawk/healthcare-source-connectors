/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.stage.PartWay;
import com.sdh.healthcare.connector.stage.StageReleases;
import java.util.Collection;
import java.util.List;

/**
 * What the drain check's commands, {@code drain-check} and
 * {@code version}, read from and change (STAGES.md §14.4a): the stage's
 * releases, its items part way, and the work waiting on an operator.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface DrainAdmin extends ReprocessAdmin
{

    /**
     * The versions a pod with these settings stamps: its release.
     *
     * @return the versions
     * @throws Exception if the settings can't be read
     */
    Collection<String> releaseVersions() throws Exception;

    /**
     * The stage's releases, to read: over the commands' read-only
     * connection.
     *
     * @return the store
     * @throws Exception if it can't be opened
     */
    StageReleases stageReleases() throws Exception;

    /**
     * The stage's releases, to accept one: over the connection that may
     * write.
     *
     * @return the store
     * @throws Exception if it can't be opened
     */
    StageReleases stageReleasesToChange() throws Exception;

    /**
     * Finds the stage's items part way, as its pods do.
     *
     * @return the finder
     * @throws Exception if it can't be opened
     */
    PartWay partWay() throws Exception;

    /**
     * The items held for an operator, by version: the split stage's held
     * items; none elsewhere.
     *
     * @param first how many item IDs to give for each version
     * @return a line per version
     * @throws Exception if they can't be read
     */
    default List<PartWay.Items> held(int first) throws Exception
    {
        return List.of();
    }
}
