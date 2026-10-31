/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.pipeline;

/**
 * Fault-injection points for crash/resume tests (design §15). A hook that throws simulates a crash there.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
interface PipelineHooks {
    PipelineHooks NONE = new PipelineHooks() {};

    /** The batch's data file is written but not committed. */
    default void batchWritten(long lastSeq) {}

    /** The batch is committed; its messages are not yet published. */
    default void batchCommitted(long lastSeq) {}

    /** All sections are committed and published; the end-of-file commit hasn't happened. */
    default void beforeEndOfFile() {}
}
