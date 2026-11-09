/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import java.util.List;

/**
 * A stage's own work: what makes a stage split, convert or transform. The
 * {@link StageWorker} does the rest (receiving, claims, item rows, sending
 * and acknowledging), and knows nothing of formats or mappings.
 * <p>
 * A processor is called from several threads at once for
 * {@link #process}, and from one at a time for {@link #commit}.
 *
 * @param <O> what the processor stores for a record
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface StageProcessor<O>
{

    /**
     * The stage's name, such as {@code convert}: the registry's key, and
     * the {@code stage} of audit events and failures.
     *
     * @return the name
     */
    String stage();

    /**
     * Reads what a message asks of the stage.
     *
     * @param message the message, opened and verified
     * @return a record to process, an item's total, or nothing
     */
    StageInput classify(ConnectorMessage message);

    /**
     * The version of everything a record's output depends on beyond the
     * record itself: the stage's build, and for the convert stage the
     * converter's mapping and the settings that change its output. Found
     * without doing the work, it is stored with the output's digest and
     * with the data (STAGES.md §14). A record received again is a
     * duplicate whatever its version, and is not processed again; a
     * version that differs from the stored one is audited
     * ({@code record.version_differs}), so a version that leaves out
     * something the output depends on hides a change, and it should err on
     * the side of changing.
     *
     * @param message the record's message
     * @return the version; never null, and short: it is stored with every
     *         record and goes in telemetry, so it must hold no content
     */
    String version(ConnectorMessage message);

    /**
     * Processes a record. An error in the record itself is not thrown, but
     * returned as a failed output, with its error code, stored with the
     * record and recorded in the stage's fail store (STAGES.md §15.3);
     * whatever is thrown is taken as passing (storage or a dependency
     * down), and the batch is tried again.
     *
     * @param message the record's message
     * @param attempt how many workers have claimed the record, this one
     *                included: above 1, an earlier one stopped part-way,
     *                perhaps on this very record
     * @return the output, or the failed record
     * @throws InterruptedException if interrupted
     */
    StageOutput<O> process(ConnectorMessage message, int attempt)
            throws InterruptedException;

    /**
     * Stores outputs, in one commit, and makes the message each is sent as:
     * null for a failed output, which isn't sent on (§15.1).
     * The commit must be one append that is safe to retry when another
     * worker committed first. Nothing is acknowledged until it returns.
     *
     * @param outputs the outputs, in order
     * @return the message for each output, in the same order
     * @throws InterruptedException if interrupted
     */
    List<ConnectorMessage> commit(List<StageOutput<O>> outputs)
            throws InterruptedException;

    /**
     * The message that says an item is done at this stage.
     *
     * @param progress the item and its counts
     * @return the message; its id must be the same each time, so a repeat
     *         is dropped
     */
    ConnectorMessage itemComplete(ItemProgress progress);

    /**
     * The message that says an item has had each of its records handled
     * once here, but isn't complete: a failure here is open, or the stage
     * before still has failures open (STAGES.md §15.6). Sent in place of
     * the item-complete message, and again whenever the counts change.
     *
     * @param progress the item, its counts and its failures
     * @return the message; its id must come from the counts, so the same
     *         counts are a repeat, dropped, and a change is a new message
     */
    ConnectorMessage itemProgress(ItemProgress progress);

    /**
     * Called once {@code item.progress} is sent, before its counts are
     * noted, so a failure here has both tried again; for the producer's
     * result, reporting the failures (§15.6). Whatever it sends must be
     * safe to send twice. Does nothing unless overridden.
     *
     * @param progress the item, its counts and its failures
     * @throws InterruptedException if interrupted
     * @throws Exception            if what it does fails; the batch is
     *                              tried again
     */
    default void itemProgressed(ItemProgress progress) throws Exception
    {
    }

    /**
     * A correction processed: its output, to commit as any, and the
     * version it was made with.
     *
     * @param <O>     what the processor stores for a record
     * @param output  the output, failed or not
     * @param version the version, as {@link #version} gives for a record
     */
    record Corrected<O>(StageOutput<O> output, String version)
    {
    }

    /**
     * Whether the stage processes operators' corrections (STAGES.md
     * §15.6c). A stage that doesn't leaves them pending.
     *
     * @return false unless overridden
     */
    default boolean corrects()
    {
        return false;
    }

    /**
     * Processes an operator's correction of a record this stage failed:
     * its corrected input, in place of the record's own, as the record
     * would have been processed, so its output, committed by
     * {@link #commit}, is stored as a new record linked to the one that
     * failed (§15.4a) and sent on as any. Called from one thread at a
     * time.
     *
     * @param correction the correction, its content encrypted
     * @return the output and its version
     * @throws InterruptedException if interrupted
     */
    default Corrected<O> correct(CorrectionStore.Pending correction)
            throws InterruptedException
    {
        throw new UnsupportedOperationException(stage()
                + " doesn't process corrections");
    }

    /**
     * Called once the item-complete message is sent, before the item is
     * marked complete, so a failure here has both tried again. Whatever it
     * sends must be safe to send twice. Does nothing unless overridden.
     *
     * @param progress the item and its counts
     * @throws InterruptedException if interrupted
     * @throws Exception            if what it does fails; the batch is
     *                              tried again
     */
    default void itemCompleted(ItemProgress progress) throws Exception
    {
    }
}
