package com.sdh.healthcare.connector.core.checkpoint;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.time.Instant;

/** Another live instance holds the lease. The service logs {@code LEASE_HELD} and exits non-zero. */
public class LeaseHeldException extends Exception {
    private final String holder;
    private final Instant expiresAt;

    public LeaseHeldException(String holder, Instant expiresAt) {
        super(ErrorCode.LEASE_HELD.name());
        this.holder = holder;
        this.expiresAt = expiresAt;
    }

    public String holder() {
        return holder;
    }

    public Instant expiresAt() {
        return expiresAt;
    }
}
