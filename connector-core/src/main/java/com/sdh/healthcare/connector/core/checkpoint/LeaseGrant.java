package com.sdh.healthcare.connector.core.checkpoint;

import java.time.Instant;

/**
 * A held lease. {@code epoch} is the fencing token; {@code expiresAt} is in the store's clock and is
 * informational only (the holder tracks validity with its own monotonic clock).
 */
public record LeaseGrant(String leaseName, String owner, long epoch, Instant expiresAt) {}
