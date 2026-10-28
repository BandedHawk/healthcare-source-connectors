package com.sdh.healthcare.connector.core.checkpoint;

/** What a writer needs from the lease: the fencing epoch, and a check before each commit. */
public interface LeaseToken {

    long epoch();

    /** Throws {@link LeaseLostException} if this instance can no longer be sure it holds the lease. */
    void checkHeld();
}
