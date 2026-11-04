/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

/**
 * What a writer needs from the lease: the fencing epoch, and a check
 * before each commit.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface LeaseToken
{

    /**
     * The fencing token.
     *
     * @return the lease's epoch
     */
    long epoch();

    /**
     * Checks that this instance can still be sure it holds the lease.
     *
     * @throws LeaseLostException if it can't
     */
    void checkHeld();
}
