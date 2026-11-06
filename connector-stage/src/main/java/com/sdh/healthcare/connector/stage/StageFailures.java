/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import java.util.Optional;
import org.apache.iceberg.exceptions.CommitStateUnknownException;

/**
 * Which errors stop a stage's process (STAGES.md §8).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StageFailures
{

    /** Not instantiated: this class only has static methods. */
    private StageFailures()
    {
    }

    /**
     * Tells whether an error must stop the process, and why: a tampered
     * checkpoint, a commit whose outcome is unknown, or a JVM error. A lost
     * lease is not one: only that work stops. Nor is a database being down
     * ({@link IllegalStateException}): the work is held until it is back,
     * rather than every pod restarting.
     *
     * @param failure the error
     * @return the reason, for the exit; empty if the error can be retried
     */
    public static Optional<String> fatalReason(Throwable failure)
    {
        if (failure instanceof CheckpointTamperedException)
        {
            return Optional.of("CHECKPOINT_TAMPERED");
        }
        if (failure instanceof CommitStateUnknownException)
        {
            return Optional.of("COMMIT_STATE_UNKNOWN");
        }
        if (failure instanceof Error)
        {
            return Optional.of(failure.getClass().getSimpleName());
        }
        return Optional.empty();
    }
}
