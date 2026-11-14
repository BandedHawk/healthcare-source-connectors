/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.jose;

import com.sdh.healthcare.connector.contract.message.UnsupportedContractVersionException;

/**
 * A message couldn't be trusted or understood. The message text never
 * contains payload content: it is the reason's name.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class MessageRejectedException extends Exception
{

    /** Why a message was rejected. */
    public enum Reason
    {
        /** It isn't a well-formed JWE holding a JWS. */
        MALFORMED,

        /** It uses an algorithm outside the allowed list. */
        ALGORITHM_NOT_ALLOWED,

        /** It is encrypted to a key ID this reader doesn't have. */
        UNKNOWN_RECIPIENT_KEY,

        /** It couldn't be decrypted. */
        DECRYPT_FAILED,

        /** It is signed with a key ID this reader doesn't trust. */
        UNKNOWN_SIGNER,

        /** Its signature doesn't verify. */
        BAD_SIGNATURE,

        /**
         * Its contract major version isn't supported: a later one, which an
         * upgrade of this reader will read, or one that isn't a version.
         */
        UNSUPPORTED_VERSION,

        /**
         * Its contract major version is earlier than this reader's, and no
         * build from now on reads it: holding it would wait for good.
         */
        EARLIER_VERSION,

        /** Its payload couldn't be read as a message. */
        BAD_PAYLOAD,

        /**
         * The pod statement it carries isn't well formed, or doesn't cover
         * the time the message says it was signed (docs/PERFORMANCE.md
         * §3.5).
         */
        STATEMENT_INVALID,

        /** It is signed under a pod statement that was revoked. */
        REVOKED_SIGNER
    }

    /** Why the message was rejected. */
    private final Reason reason;

    /**
     * Creates the exception.
     *
     * @param reason why the message was rejected
     */
    public MessageRejectedException(Reason reason)
    {
        super(reason.name());
        this.reason = reason;
    }

    /**
     * Creates the exception from a failure. An unsupported contract version
     * becomes {@link Reason#EARLIER_VERSION} if it is an earlier major
     * version, otherwise {@link Reason#UNSUPPORTED_VERSION}, and is kept as
     * the cause; any other cause is dropped, since its message could hold
     * payload content.
     *
     * @param reason why the message was rejected
     * @param cause  the failure
     */
    public MessageRejectedException(Reason reason, Throwable cause)
    {
        super(reason.name(),
                cause instanceof UnsupportedContractVersionException
                        ? cause : null);
        this.reason = cause instanceof UnsupportedContractVersionException
                unsupported
                ? (unsupported.earlier() ? Reason.EARLIER_VERSION
                        : Reason.UNSUPPORTED_VERSION)
                : reason;
    }

    /**
     * Why the message was rejected.
     *
     * @return the reason
     */
    public Reason reason()
    {
        return reason;
    }
}
