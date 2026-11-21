/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import java.util.Map;
import java.util.Optional;

/**
 * A long operation that the stage's workers carry out, which the control
 * API starts and then follows (CONTROL-API-DESIGN.md §8.1): a reprocess
 * run, a resubmit. The API records it as an operation, answers
 * {@code 202} with where to find it, and reads where it stands from the
 * stage whenever it is asked, until it is finished.
 *
 * @param <C> what it reads from
 * @param <Q> its request
 * @param <R> its result
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface Followed<C, Q, R> extends Operation<C, Q, R>
{

    /** Where a followed operation stands. */
    enum State
    {

        /** Not finished. */
        RUNNING,

        /** Finished. */
        DONE,

        /** Finished, and refused or failed. */
        FAILED
    }

    /**
     * Where it stands.
     *
     * @param state   finished or not, and how
     * @param details counts and identifiers, never PHI, built with
     *                {@link Answers}
     */
    record Progress(State state, Map<String, Object> details)
    {
    }

    /**
     * Starts it, without waiting for the workers. The CLI's run may wait;
     * this never does.
     *
     * @param request    the request
     * @param context    what it reads from
     * @param invocation who asks, and the audit event
     * @return the result
     * @throws Exception if it can't be started
     */
    default R start(Q request, C context, Invocation invocation)
            throws Exception
    {
        return run(request, context, invocation);
    }

    /**
     * What identifies what was started, for the operation record: run and
     * item IDs, a selector, counts, never PHI.
     *
     * @param result the result
     * @return the parameters; empty if nothing was started, as for a dry
     *         run, when the result is answered as it is
     */
    Optional<Map<String, Object>> started(R result);

    /**
     * Where it stands now, read from the stage.
     *
     * @param parameters what {@link #started} gave
     * @param context    what it reads from
     * @return where it stands
     * @throws Exception if it can't be read
     */
    Progress progress(Map<String, Object> parameters, C context)
            throws Exception;
}
