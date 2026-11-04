/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;

/**
 * This instance may no longer write. The pipeline stops and the process
 * exits with a non-zero status, so the orchestrator restarts it and it
 * competes for the lease again.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class LeaseLostException extends RuntimeException
{

    /** How the lease was lost. */
    public enum Reason
    {
        /** It couldn't be renewed before the local validity window ran out. */
        EXPIRED,

        /** The store reports another holder. */
        TAKEN_OVER,

        /** A table records a newer epoch: a successor has fenced us out. */
        FENCED
    }

    /** How the lease was lost. */
    private final Reason reason;

    /**
     * Creates the exception. Its message is the error code
     * ({@code LEASE_FENCED} or {@code LEASE_LOST}) and the reason.
     *
     * @param reason how the lease was lost
     */
    public LeaseLostException(Reason reason)
    {
        super((reason == Reason.FENCED
                ? ErrorCode.LEASE_FENCED : ErrorCode.LEASE_LOST).name()
                + ":" + reason);
        this.reason = reason;
    }

    /**
     * How the lease was lost.
     *
     * @return the reason
     */
    public Reason reason()
    {
        return reason;
    }
}
