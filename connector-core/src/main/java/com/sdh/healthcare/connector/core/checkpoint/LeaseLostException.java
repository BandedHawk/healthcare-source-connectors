package com.sdh.healthcare.connector.core.checkpoint;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;

/**
 * This instance may no longer write. The pipeline stops and the process exits non-zero, so the
 * orchestrator restarts it and it competes for the lease again.
 */
public class LeaseLostException extends RuntimeException {
    public enum Reason {
        /** Could not renew before the local validity window ran out. */
        EXPIRED,
        /** The store reports another holder. */
        TAKEN_OVER,
        /** A table records a newer epoch: a successor has already fenced us out. */
        FENCED
    }

    private final Reason reason;

    public LeaseLostException(Reason reason) {
        super((reason == Reason.FENCED ? ErrorCode.LEASE_FENCED : ErrorCode.LEASE_LOST).name() + ":" + reason);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
