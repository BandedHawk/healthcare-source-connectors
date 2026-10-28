package com.sdh.healthcare.connector.core.checkpoint;

/** The stored checkpoint fails MAC verification or can't be parsed. The service stops rather than guess (§9.1). */
public class CheckpointTamperedException extends RuntimeException {
    public CheckpointTamperedException() {
        super("CHECKPOINT_TAMPERED");
    }
}
