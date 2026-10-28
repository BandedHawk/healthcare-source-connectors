package com.sdh.healthcare.connector.core.pipeline;

/** Fault-injection points for crash/resume tests (design §15). A hook that throws simulates a crash there. */
interface PipelineHooks {
    PipelineHooks NONE = new PipelineHooks() {};

    /** The batch's data file is written but not committed. */
    default void batchWritten(long lastSeq) {}

    /** The batch is committed; its messages are not yet published. */
    default void batchCommitted(long lastSeq) {}

    /** All sections are committed and published; the end-of-file commit hasn't happened. */
    default void beforeEndOfFile() {}
}
