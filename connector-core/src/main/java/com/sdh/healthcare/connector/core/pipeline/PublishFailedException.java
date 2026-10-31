/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.pipeline;

/**
 * The broker didn't acknowledge a message after all retries (error code {@code PUBLISH_FAILED}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class PublishFailedException extends Exception {
    public PublishFailedException(Throwable cause) {
        super("PUBLISH_FAILED", cause);
    }
}
