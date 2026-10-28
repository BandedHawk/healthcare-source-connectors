package com.sdh.healthcare.connector.file.runtime;

/**
 * Stops the process with a non-zero status so the orchestrator restarts it (design §9.3, §11). Used for lease loss,
 * unknown commit outcomes, a tampered checkpoint and broken invariants. {@code reason} must be PHI-free.
 */
@FunctionalInterface
public interface FatalExit {
    int STATUS = 70;

    void exit(String reason);
}
