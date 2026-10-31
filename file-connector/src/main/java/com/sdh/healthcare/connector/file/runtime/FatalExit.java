/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.file.runtime;

/**
 * Stops the process with a non-zero status so the orchestrator restarts it (design §9.3, §11). Used for lease loss,
 * unknown commit outcomes, a tampered checkpoint and broken invariants. {@code reason} must be PHI-free.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@FunctionalInterface
public interface FatalExit {
    int STATUS = 70;

    void exit(String reason);
}
