/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import java.util.Objects;
import java.util.function.Consumer;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits to a table that many workers write to, such as a stage's
 * {@code split_records} (STAGES.md §7–§8), while a lease is held. There is
 * no table epoch: Iceberg's own commit applies the changes again on the
 * newest metadata when another worker committed first, which is what
 * appends from many workers need. The lease (an item's, or the stage's
 * duties lease for maintenance) is checked before each attempt.
 * <p>
 * A worker that stalls between the check and its commit for longer than
 * the lease lasts can still commit once; STAGES.md §8 says why that is
 * harmless for the split stage's appends.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LeasedCommitter implements TableCommitter
{

    /** Attempts before a conflicting commit gives up, by default. */
    private static final int DEFAULT_ATTEMPTS = 10;

    /** Logs conflicts, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(LeasedCommitter.class);

    /** The lease that must be held. */
    private final LeaseToken lease;

    /** Attempts before giving up on conflicts. */
    private final int maxAttempts;

    /**
     * Creates a committer.
     *
     * @param lease       the lease that must be held
     * @param maxAttempts attempts before giving up on conflicts; at least 1
     * @throws IllegalArgumentException if maxAttempts is below 1
     */
    public LeasedCommitter(LeaseToken lease, int maxAttempts)
    {
        this.lease = Objects.requireNonNull(lease);
        if (maxAttempts < 1)
        {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
    }

    /**
     * Creates a committer that tries up to ten times.
     *
     * @param lease the lease that must be held
     */
    public LeasedCommitter(LeaseToken lease)
    {
        this(lease, DEFAULT_ATTEMPTS);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Iceberg retries a conflict itself, as the table's
     * {@code commit.retry.num-retries} says; this tries again on top of
     * that, after checking the lease.
     */
    @Override
    public Snapshot commit(Table table, Consumer<Transaction> changes)
    {
        for (int attempt = 1; ; attempt++)
        {
            lease.checkHeld();
            table.refresh();
            Transaction transaction = table.newTransaction();
            changes.accept(transaction);
            try
            {
                lease.checkHeld();
                transaction.commitTransaction();
                return table.currentSnapshot();
            }
            catch (CommitFailedException conflict)
            {
                LOG.warn("commit.conflict attempt={} table={}", attempt,
                        table.name());
                if (attempt >= maxAttempts)
                {
                    throw conflict;
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Only the lease is checked: the table carries no epoch.
     */
    @Override
    public void verifyCurrent(Table table)
    {
        lease.checkHeld();
    }
}
