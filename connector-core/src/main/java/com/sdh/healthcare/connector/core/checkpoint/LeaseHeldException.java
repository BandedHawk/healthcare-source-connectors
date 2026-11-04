/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.time.Instant;

/**
 * Another live instance holds the lease. The service logs
 * {@code LEASE_HELD} and exits with a non-zero status.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class LeaseHeldException extends Exception
{

    /** Who holds the lease. */
    private final String holder;

    /** When their lease expires, in the store's clock. */
    private final Instant expiresAt;

    /**
     * Creates the exception, with the error code as its message.
     *
     * @param holder    who holds the lease
     * @param expiresAt when their lease expires, in the store's clock
     */
    public LeaseHeldException(String holder, Instant expiresAt)
    {
        super(ErrorCode.LEASE_HELD.name());
        this.holder = holder;
        this.expiresAt = expiresAt;
    }

    /**
     * Who holds the lease.
     *
     * @return the holder
     */
    public String holder()
    {
        return holder;
    }

    /**
     * When the holder's lease expires.
     *
     * @return the time, in the store's clock
     */
    public Instant expiresAt()
    {
        return expiresAt;
    }
}
