/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.split;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;

/**
 * The file can't be split. Carries only a stable error code and a byte offset, never content (design §7.5).
 * Not retryable: the pipeline fails the file.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class SplitException extends Exception {
    private final ErrorCode code;
    private final long offset;

    public SplitException(ErrorCode code, long offset) {
        super(code.name() + " at offset " + offset);
        this.code = code;
        this.offset = offset;
    }

    public ErrorCode code() {
        return code;
    }

    public long offset() {
        return offset;
    }
}
