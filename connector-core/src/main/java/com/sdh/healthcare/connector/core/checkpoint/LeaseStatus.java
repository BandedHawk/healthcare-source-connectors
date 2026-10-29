package com.sdh.healthcare.connector.core.checkpoint;

import java.time.Duration;
import java.time.Instant;

/**
 * A lease row as the store sees it, for operator status (design §11.4). Times are in the store's clock;
 * {@code remaining} is computed there too, so it is accurate regardless of the caller's clock.
 *
 * @param remaining time left before it expires; negative once expired
 */
public record LeaseStatus(String name, String owner, long epoch, Instant acquiredAt, Instant renewedAt,
        Instant expiresAt, Duration remaining) {

    public boolean live() {
        return remaining.isPositive();
    }
}
