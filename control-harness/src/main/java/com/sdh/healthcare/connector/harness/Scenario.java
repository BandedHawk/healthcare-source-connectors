/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

/**
 * One scenario of CONTROL-API.md, written once and run by any target:
 * driven through the control API, and checked by its answers, the
 * pipeline's results and the audit. After it, the harness checks every
 * answer it got against the contract, and that the stages audited each
 * call with its principal.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface Scenario
{

    /**
     * The scenario's name, as {@code scripts/harness.sh} takes it.
     *
     * @return the name, such as {@code access}
     */
    String name();

    /**
     * What it shows, for the report.
     *
     * @return one sentence
     */
    String summary();

    /**
     * Runs it.
     *
     * @param harness the pipeline and the clients of its stages
     * @throws Exception if a check fails or the pipeline can't be reached
     */
    void run(Harness harness) throws Exception;
}
