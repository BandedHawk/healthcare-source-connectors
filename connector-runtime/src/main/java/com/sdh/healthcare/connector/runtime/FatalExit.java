/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;

/**
 * Stops the process with a non-zero status so the orchestrator restarts it
 * (design §9.3, §11). Used for lease loss, unknown commit outcomes, a
 * tampered checkpoint and broken invariants.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@FunctionalInterface
public interface FatalExit
{

    /** The exit status: {@code EX_SOFTWARE} from {@code sysexits.h}. */
    int STATUS = 70;

    /**
     * Stops the process.
     *
     * @param reason why, for the audit event; it must hold no PHI
     */
    void exit(String reason);

    /**
     * The production hook: records a {@code connector.fatal} audit event,
     * then exits with {@link #STATUS} from another thread, so Spring's
     * shutdown hooks can run (and stop the routes) without deadlock.
     *
     * @return the hook
     */
    static FatalExit exitProcess()
    {
        return reason ->
        {
            Telemetry.get().auditError("connector.fatal",
                    Fields.of(Attr.REASON, reason));
            Thread.ofPlatform().name("sdh-fatal-exit")
                    .start(() -> System.exit(STATUS));
        };
    }
}
