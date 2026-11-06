/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import java.util.function.Consumer;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.CommitFailedException;

/**
 * Commits to Iceberg tables only while a lease is held. A table with one
 * writer is fenced by the lease's epoch ({@link FencedCommitter}); a table
 * many stage workers append to has no single writer to fence, so its
 * commits only check the lease ({@link LeasedCommitter}, STAGES.md §7).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface TableCommitter
{

    /**
     * Applies changes to a fresh transaction and commits it, while the
     * lease is held. The callback may run more than once (once per
     * attempt), so it must have no side effects outside the transaction.
     *
     * @param table   the table
     * @param changes makes the changes on the transaction
     * @return the new current snapshot
     * @throws LeaseLostException    if the lease was lost, or a successor
     *                               fenced this instance out
     * @throws CommitFailedException if the commit keeps conflicting
     */
    Snapshot commit(Table table, Consumer<Transaction> changes);

    /**
     * Checks, without committing, that this instance may still commit to
     * the table. Used right before operations that commit through
     * Iceberg's own path (snapshot expiry, design §8.4).
     *
     * @param table the table
     * @throws LeaseLostException if the lease was lost or fenced
     */
    void verifyCurrent(Table table);
}
