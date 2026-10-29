package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;

/**
 * A section that can't be converted. Carries a stable code and the byte position within the section only: the
 * message never holds content, so it is safe to log (design §7.5).
 */
public final class ConversionException extends Exception {
    private final ErrorCode code;
    private final long position;

    public ConversionException(ErrorCode code, long position) {
        super(code + " at section byte " + position, null, false, false);
        this.code = code;
        this.position = position;
    }

    public ErrorCode code() {
        return code;
    }

    /** Byte position within the section where the problem was found. */
    public long position() {
        return position;
    }
}
