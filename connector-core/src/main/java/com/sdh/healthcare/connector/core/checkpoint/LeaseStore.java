/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.checkpoint;

import java.time.Duration;

/**
 * Durable store for the single-instance lease (design §9.3).
 *
 * <p>Requirements for an implementation:
 *
 * <ul>
 *   <li>Acquire, renew and release are atomic compare-and-set operations.
 *   <li>Expiry is judged by the store's own clock, never the caller's.
 *   <li>Every successful acquisition returns a strictly larger {@link LeaseGrant#epoch()} than any earlier
 *       one for the same lease name. The epoch is the fencing token, so it must never go backwards, even
 *       after a release.
 * </ul>
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface LeaseStore {

    /** Takes the lease if it is free or expired, otherwise throws {@link LeaseHeldException}. */
    LeaseGrant acquire(String leaseName, String owner, Duration ttl) throws LeaseHeldException;

    /** Extends the lease. Returns false if the grant is no longer the current holder. */
    boolean renew(LeaseGrant grant, Duration ttl);

    /** Ends the lease early so a successor need not wait for expiry. Keeps the epoch. */
    void release(LeaseGrant grant);
}
