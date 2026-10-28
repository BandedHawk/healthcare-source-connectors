package com.sdh.healthcare.connector.contract.crypto;

/** A stored chunk failed authentication or did not match the expected section layout. */
public class ChunkIntegrityException extends RuntimeException {
    public ChunkIntegrityException(String message) {
        super(message);
    }
}
