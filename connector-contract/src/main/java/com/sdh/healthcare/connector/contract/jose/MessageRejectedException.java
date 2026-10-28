package com.sdh.healthcare.connector.contract.jose;

import com.sdh.healthcare.connector.contract.message.UnsupportedContractVersionException;

/** A message could not be trusted or understood. The message text never contains payload content. */
public class MessageRejectedException extends Exception {
    public enum Reason {
        MALFORMED,
        ALGORITHM_NOT_ALLOWED,
        UNKNOWN_RECIPIENT_KEY,
        DECRYPT_FAILED,
        UNKNOWN_SIGNER,
        BAD_SIGNATURE,
        UNSUPPORTED_VERSION,
        BAD_PAYLOAD
    }

    private final Reason reason;

    public MessageRejectedException(Reason reason) {
        super(reason.name());
        this.reason = reason;
    }

    public MessageRejectedException(Reason reason, Throwable cause) {
        super(reason.name(), cause instanceof UnsupportedContractVersionException ? cause : null);
        this.reason = cause instanceof UnsupportedContractVersionException ? Reason.UNSUPPORTED_VERSION : reason;
    }

    public Reason reason() {
        return reason;
    }
}
