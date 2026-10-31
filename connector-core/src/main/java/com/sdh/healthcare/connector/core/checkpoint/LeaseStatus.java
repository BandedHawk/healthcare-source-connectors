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
 * A lease row as the store sees it, for operator status (design §11.4). Times are in the store's clock;
 * {@code remaining} is computed there too, so it is accurate regardless of the caller's clock.
 *
 * @param remaining time left before it expires; negative once expired
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record LeaseStatus(String name, String owner, long epoch, Instant acquiredAt, Instant renewedAt,
        Instant expiresAt, Duration remaining) {

    public boolean live() {
        return remaining.isPositive();
    }
}
