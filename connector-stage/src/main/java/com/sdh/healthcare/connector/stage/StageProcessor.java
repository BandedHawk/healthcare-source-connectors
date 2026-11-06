/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
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
     * the {@code stage} of audit events and conflicts.
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
     * Processes a record. An error in the record itself is not thrown, but
     * returned as a failed output, stored with the record; whatever is
     * thrown is taken as passing (storage or a dependency down), and the
     * batch is tried again.
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
     * Stores outputs, in one commit, and makes the message each is sent as.
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
}
