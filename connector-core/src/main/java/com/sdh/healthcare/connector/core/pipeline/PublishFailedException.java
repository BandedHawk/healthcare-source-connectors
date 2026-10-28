package com.sdh.healthcare.connector.core.pipeline;

/** The broker didn't acknowledge a message after all retries (error code {@code PUBLISH_FAILED}). */
public class PublishFailedException extends Exception {
    public PublishFailedException(Throwable cause) {
        super("PUBLISH_FAILED", cause);
    }
}
