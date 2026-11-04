/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import java.time.Duration;

/**
 * A durable store for the single-instance lease (design §9.3).
 * <p>
 * What an implementation must do:
 * <ul>
 * <li>Acquire, renew and release are atomic compare-and-set
 * operations.</li>
 * <li>Expiry is judged by the store's own clock, never the caller's.</li>
 * <li>Every successful acquisition returns a strictly larger
 * {@link LeaseGrant#epoch()} than any earlier one for the same lease name.
 * The epoch is the fencing token, so it must never go back, even after a
 * release.</li>
 * </ul>
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface LeaseStore
{

    /**
     * Takes the lease if it is free or expired.
     *
     * @param leaseName the lease's name
     * @param owner     who is taking it
     * @param ttl       how long it is held without a renewal
     * @return the grant
     * @throws LeaseHeldException if another live instance holds it
     */
    LeaseGrant acquire(String leaseName, String owner, Duration ttl)
            throws LeaseHeldException;

    /**
     * Extends the lease.
     *
     * @param grant the grant
     * @param ttl   how long it is held from now
     * @return false if the grant is no longer the current holder
     */
    boolean renew(LeaseGrant grant, Duration ttl);

    /**
     * Ends the lease early, so a successor needn't wait for it to expire.
     * The epoch is kept.
     *
     * @param grant the grant
     */
    void release(LeaseGrant grant);
}
