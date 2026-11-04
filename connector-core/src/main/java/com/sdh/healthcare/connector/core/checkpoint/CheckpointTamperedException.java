/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

/**
 * The stored checkpoint fails MAC verification or can't be parsed. The
 * service stops rather than guess (design §9.1).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class CheckpointTamperedException extends RuntimeException
{

    /** Creates the exception, with the error code as its message. */
    public CheckpointTamperedException()
    {
        super("CHECKPOINT_TAMPERED");
    }
}
