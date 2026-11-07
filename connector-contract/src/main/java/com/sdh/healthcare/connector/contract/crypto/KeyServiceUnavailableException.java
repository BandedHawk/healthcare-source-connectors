/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.crypto;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * The key service can't be reached, or isn't serving: Vault sealed,
 * standing by, or behind a gateway that can't reach it. Nothing is wrong
 * with the data, so the work is tried again later and isn't held against
 * the item (STAGES.md §12).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class KeyServiceUnavailableException extends UncheckedIOException
{

    /**
     * Creates the exception.
     *
     * @param message which call, without data
     * @param cause   the failure, or a stand-in for an HTTP status
     */
    public KeyServiceUnavailableException(String message, IOException cause)
    {
        super(message, cause);
    }
}
