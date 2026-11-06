/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.crypto;

/**
 * A stored chunk failed authentication, or didn't match the expected
 * record layout.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class ChunkIntegrityException extends RuntimeException
{

    /**
     * Creates the exception.
     *
     * @param message what was wrong, without content
     */
    public ChunkIntegrityException(String message)
    {
        super(message);
    }
}
