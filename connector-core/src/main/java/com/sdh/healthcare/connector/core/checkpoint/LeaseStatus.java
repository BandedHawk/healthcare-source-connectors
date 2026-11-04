/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import java.time.Duration;
import java.time.Instant;

/**
 * A lease row as the store sees it, for operator status (design §11.4).
 * Times are in the store's clock; {@code remaining} is computed there too,
 * so it is accurate whatever the caller's clock says.
 *
 * @param name       the lease's name
 * @param owner      who holds it
 * @param epoch      the fencing token
 * @param acquiredAt when it was taken
 * @param renewedAt  when it was last renewed
 * @param expiresAt  when it expires
 * @param remaining  the time left before it expires; negative once
 *                   expired
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record LeaseStatus(String name, String owner, long epoch,
        Instant acquiredAt, Instant renewedAt, Instant expiresAt,
        Duration remaining)
{

    /**
     * Tells whether the lease is still held.
     *
     * @return true if it hasn't expired
     */
    public boolean live()
    {
        return remaining.isPositive();
    }
}
