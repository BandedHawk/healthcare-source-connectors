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
}
