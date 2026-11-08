/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import java.util.concurrent.locks.ReentrantLock;

/**
 * What the runtime needs of an intake, whether it lists a source
 * ({@link IntakeService}) or is pushed to ({@link PushService}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface LandingIntake
{

    /**
     * Recovers from a crash, before items are taken in. If storage or the
     * broker is down, recovery is tried again before the next item.
     */
    void recoverAtStartup();

    /**
     * The lock held while the intake writes to landing storage: table
     * maintenance takes it only when it is free.
     *
     * @return the lock
     */
    ReentrantLock writerLock();

    /**
     * How many items the intake's ledger holds, for the
     * {@code sdh.landing.ledger_items} gauge.
     *
     * @return the number of ledger entries
     */
    long ledgerItems();

    /**
     * Lands an item already landed again, as a new item linked to it, for
     * an operator's request (STAGES.md §13.5), or finds the item an
     * earlier try of the request landed; then records and announces it as
     * the intake does its own. Holds the writer lock, after the intake's
     * recovery.
     *
     * @param resubmitter lands the item
     * @param request     the request
     * @return the new item
     * @throws Resubmitter.RefusedException if the request can never be
     *                                      carried out
     * @throws Resubmitter.BusyException    if the intake has an unfinished
     *                                      item of its own to resume
     * @throws Exception                    if storage, the key service or
     *                                      the broker fails: the request
     *                                      is tried again later
     */
    Resubmitter.Landed resubmit(Resubmitter resubmitter,
            ResubmitRequests.Request request) throws Exception;
}
