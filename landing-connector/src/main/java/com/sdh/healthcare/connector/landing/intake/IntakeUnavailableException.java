/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import java.io.IOException;

/**
 * A remote source couldn't be reached, logged into, or listed (design
 * §5.3.4). The library's own exception isn't kept as the cause: its
 * message can hold a path or file name. Only its class name is kept.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class IntakeUnavailableException extends IOException
{

    /** For serialization. */
    private static final long serialVersionUID = 1L;

    /** Why the source is unavailable. */
    private final IntakeProblem problem;

    /**
     * Creates the exception.
     *
     * @param problem why the source is unavailable
     * @param cause   what the library threw; only its class name is kept
     */
    public IntakeUnavailableException(IntakeProblem problem, Throwable cause)
    {
        super(problem.name() + (cause == null ? ""
                : " (" + cause.getClass().getSimpleName() + ")"));
        this.problem = problem;
    }

    /**
     * Why the source is unavailable.
     *
     * @return the problem
     */
    public IntakeProblem problem()
    {
        return problem;
    }
}
