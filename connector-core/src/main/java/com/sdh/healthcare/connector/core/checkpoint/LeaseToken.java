/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.checkpoint;

/**
 * What a writer needs from the lease: the fencing epoch, and a check before each commit.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface LeaseToken {

    long epoch();

    /** Throws {@link LeaseLostException} if this instance can no longer be sure it holds the lease. */
    void checkHeld();
}
