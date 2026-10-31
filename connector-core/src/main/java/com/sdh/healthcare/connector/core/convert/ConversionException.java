/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;

/**
 * A section that can't be converted. Carries a stable code and the byte position within the section only: the
 * message never holds content, so it is safe to log (design §7.5).
 *
 * @author Jonas Barnett
 * @since 0.1.0
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
