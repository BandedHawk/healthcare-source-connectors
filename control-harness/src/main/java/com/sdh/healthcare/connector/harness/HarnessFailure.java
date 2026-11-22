/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

/**
 * A scenario's check that didn't hold. Its message names calls, statuses
 * and IDs: never a document's content.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class HarnessFailure extends RuntimeException
{

    /** Serialization version. */
    private static final long serialVersionUID = 1L;

    /**
     * Creates the failure.
     *
     * @param message what didn't hold
     */
    public HarnessFailure(String message)
    {
        super(message);
    }

    /**
     * Creates the failure, with its cause.
     *
     * @param message what didn't hold
     * @param cause   why
     */
    public HarnessFailure(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Fails unless a condition holds.
     *
     * @param condition the condition
     * @param message   what it means, for the failure
     * @throws HarnessFailure if it doesn't hold
     */
    public static void check(boolean condition, String message)
    {
        if (!condition)
        {
            throw new HarnessFailure(message);
        }
    }
}
