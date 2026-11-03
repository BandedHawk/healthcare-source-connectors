/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;

/**
 * A read-write source that can't be changed, found by the startup probe.
 * The message names the setting to check, never a path.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class IntakeNotWritableException extends IOException
{

    /** For serialization. */
    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param setting the setting whose directory can't be written
     * @param cause   the failure
     */
    public IntakeNotWritableException(String setting, Throwable cause)
    {
        super("The directory in " + setting + " can't be written", cause);
    }

    /**
     * The error code operators see.
     *
     * @return {@link ErrorCode#INTAKE_NOT_WRITABLE}
     */
    public ErrorCode code()
    {
        return ErrorCode.INTAKE_NOT_WRITABLE;
    }
}
