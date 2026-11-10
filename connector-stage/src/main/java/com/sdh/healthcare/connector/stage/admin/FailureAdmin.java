/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.UUID;

/**
 * What a stage's failure commands ({@link FailureCommands}) need of it: its
 * fail store, the keys an operator's reasons are encrypted with, and what
 * it does once a failure's state has changed (STAGES.md §15.3).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface FailureAdmin
{

    /**
     * The stage, as its fail store names it.
     *
     * @return the stage, such as {@code split}
     */
    String stage();

    /**
     * The stage's fail store, to read: over the commands' read-only
     * connection.
     *
     * @return the store
     * @throws Exception if it can't be opened
     */
    FailureStore failures() throws Exception;

    /**
     * The stage's fail store, to change a failure's state: over a
     * connection of its own that may write, opened only for
     * {@code decline} and {@code reopen}.
     *
     * @return the store
     * @throws Exception if it can't be opened
     */
    FailureStore failuresToChange() throws Exception;

    /**
     * Wraps the data keys of an operator's reasons.
     *
     * @return the stage's key provider
     * @throws Exception if the keys can't be loaded
     */
    KeyProvider reasonKeys() throws Exception;

    /**
     * The KEK an operator's reasons' data keys are wrapped with: the
     * stage's own.
     *
     * @return its reference
     */
    String reasonKek();

    /**
     * Where an operator's input is read from (a reason, a corrected record,
     * the public key an export is encrypted to): the command's standard
     * input, so it never appears in a command line, a shell history or the
     * cluster's audit log of {@code kubectl exec}.
     *
     * @return the input
     */
    default InputStream reasonInput()
    {
        return System.in;
    }

    /**
     * Whether the stage takes operators' corrections (STAGES.md §15.6c).
     *
     * @return false unless overridden
     */
    default boolean corrects()
    {
        return false;
    }

    /**
     * The largest correction taken: the largest record the stage
     * processes.
     *
     * @return the size, in bytes
     */
    default long maxCorrectionBytes()
    {
        return 0;
    }

    /**
     * The stage's corrections, to submit one: over the connection that may
     * write.
     *
     * @return the store
     * @throws Exception if it can't be opened
     */
    default CorrectionStore correctionsToChange() throws Exception
    {
        throw new IllegalStateException("The " + stage()
                + " stage doesn't take corrections yet");
    }

    /**
     * The stage's corrections, to list.
     *
     * @return the store; null if the stage takes none
     * @throws Exception if it can't be opened
     */
    default CorrectionStore corrections() throws Exception
    {
        return null;
    }

    /**
     * What a failure failed on, decrypted, for {@code export}, which
     * encrypts it to the operator's key at once.
     *
     * @param itemId the failure's item
     * @param seq    its record number
     * @return the failed input; the caller wipes it
     * @throws Exception if it can't be read, or the stage can't export it
     */
    default byte[] failedContent(UUID itemId, long seq) throws Exception
    {
        throw new IllegalStateException("The " + stage()
                + " stage can't export its failures yet");
    }

    /**
     * Acts on a change to one of an item's failures: the split stage sends
     * the item's end again ({@code item.split} once none is open, or
     * {@code item.progress} with the new counts); a stage of the stage
     * library leaves it to its workers, which look at changed items again
     * within a renewal interval.
     *
     * @param itemId the item
     * @param audit  the command's audit record
     * @param out    where to say what was done
     * @throws Exception if it fails; the change stays made, and the
     *                   stage's pods act on it
     */
    void changed(UUID itemId, AuditRecord audit, PrintStream out)
            throws Exception;
}
