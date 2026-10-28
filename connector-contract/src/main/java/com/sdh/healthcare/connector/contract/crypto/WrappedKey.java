package com.sdh.healthcare.connector.contract.crypto;

/** A data key wrapped by the KEK named {@code keyRef} (key ID and version). */
public record WrappedKey(String keyRef, byte[] wrapped) {}
