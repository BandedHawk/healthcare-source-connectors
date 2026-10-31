/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.contract.crypto;

/**
 * A stored chunk failed authentication or did not match the expected section layout.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class ChunkIntegrityException extends RuntimeException {
    public ChunkIntegrityException(String message) {
        super(message);
    }
}
