/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import java.time.Instant;

/**
 * A held lease.
 *
 * @param leaseName the lease's name
 * @param owner     who holds it
 * @param epoch     the fencing token
 * @param expiresAt when it expires, in the store's clock; for information
 *                  only, since the holder tracks validity with its own
 *                  monotonic clock
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record LeaseGrant(String leaseName, String owner, long epoch,
        Instant expiresAt)
{
}
