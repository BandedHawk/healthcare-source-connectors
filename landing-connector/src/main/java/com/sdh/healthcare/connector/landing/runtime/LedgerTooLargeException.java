/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.runtime;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;

/**
 * The ledger's index would take more of the heap than it may, found before
 * the ledger is loaded. The message gives the heap needed, and no names.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LedgerTooLargeException extends IllegalStateException
{

    /** For serialization. */
    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param items      how many entries the ledger has
     * @param heapNeeded the smallest heap that would hold its index, in
     *                   bytes
     */
    public LedgerTooLargeException(long items, long heapNeeded)
    {
        super("The ledger's " + items + " entries need a heap of at least "
                + ((heapNeeded + (1 << 20) - 1) >> 20)
                + " MiB; raise the memory limit");
    }

    /**
     * The error code operators see.
     *
     * @return {@link ErrorCode#LEDGER_TOO_LARGE}
     */
    public ErrorCode code()
    {
        return ErrorCode.LEDGER_TOO_LARGE;
    }
}
