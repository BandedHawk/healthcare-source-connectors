/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.stage.ReprocessInputs;
import com.sdh.healthcare.connector.stage.ReprocessStore;

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
}
