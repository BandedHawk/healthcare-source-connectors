/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits to Iceberg tables only while this instance holds the lease,
 * checked with a fencing token (the lease epoch) stored in the table
 * property {@value #EPOCH_PROPERTY}.
 * <p>
 * Iceberg's own {@code Transaction.commitTransaction()} can't do this.
 * Before committing, it refreshes the table and applies our changes again
 * onto whatever metadata is newest, so a stale instance's changes would be
 * laid over a successor's. This class instead commits with a strict
 * compare-and-swap through the table's {@code TableOperations}:
 * <ol>
 * <li>Refresh, and require the table's epoch to equal ours (equal or lower
 * when establishing).</li>
 * <li>Build the change as a transaction on that exact metadata.</li>
 * <li>Make sure the change adds a snapshot to {@code main}. If the caller's
 * change doesn't (a checkpoint update that only sets properties, say), an
 * empty marker append is added. With a REST catalog the commit then
 * carries an {@code assert-ref-snapshot-id} requirement, so the server
 * rejects it if anyone else committed in between; a change to properties
 * alone would carry no such check. JDBC and Hive catalogs already compare
 * the whole metadata location.</li>
 * <li>Commit from the start metadata to the new metadata with
 * {@code TableOperations.commit}, which fails if the table moved. On
 * failure, go back to step 1. If the epoch has changed, this instance has
 * been fenced out.</li>
 * </ol>
 * <p>
 * Every writer of these tables, maintenance included, must commit through
 * this class.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class FencedCommitter implements TableCommitter
{

    /** The table property that holds the epoch. */
    public static final String EPOCH_PROPERTY = "sdh.lease.epoch";

    /** The snapshot summary key set on marker snapshots, for audit. */
    public static final String MARKER_SUMMARY = "sdh.fence-marker";

    /** How many times a commit is tried by default. */
    private static final int DEFAULT_ATTEMPTS = 5;

    /** Logs commit conflicts, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(FencedCommitter.class);

    /** The lease commits are made under. */
    private final LeaseToken lease;

    /** How many times a commit is tried before giving up. */
    private final int maxAttempts;

    /**
     * Creates a committer.
     *
     * @param lease       the lease commits are made under
     * @param maxAttempts how many times a commit is tried
     * @throws IllegalArgumentException if {@code maxAttempts} is below 1
     */
    public FencedCommitter(LeaseToken lease, int maxAttempts)
    {
        this.lease = Objects.requireNonNull(lease);
        if (maxAttempts < 1)
        {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
    }

    /**
     * Creates a committer that tries each commit five times.
     *
     * @param lease the lease commits are made under
     */
    public FencedCommitter(LeaseToken lease)
    {
        this(lease, DEFAULT_ATTEMPTS);
    }

    /**
     * Records our epoch in the table, fencing out every earlier holder.
     * Call it once per table straight after taking the lease, before any
     * other commit. Running it again changes nothing.
     *
     * @param table the table
     * @throws LeaseLostException    if a later holder has fenced this one
     *                               out
     * @throws CommitFailedException if the commit keeps conflicting
     */
    public void establish(Table table)
    {
        for (int attempt = 1; ; attempt++)
        {
            lease.checkHeld();
            table.refresh();
            long current = epochOf(table.properties());
            if (current == lease.epoch())
            {
                return;
            }
            if (current > lease.epoch())
            {
                throw new LeaseLostException(LeaseLostException.Reason.FENCED);
            }
            Transaction transaction = table.newTransaction();
            transaction.updateProperties()
                    .set(EPOCH_PROPERTY, Long.toString(lease.epoch()))
                    .commit();
            addMarker(transaction, lease.epoch());
            try
            {
                compareAndSwap(transaction, current);
                Telemetry.get().audit("lease.fenced_table",
                        Fields.of(Attr.EPOCH, lease.epoch())
                                .with(Attr.TABLE, table.name()));
                return;
            }
            catch (CommitFailedException conflict)
            {
                if (attempt >= maxAttempts)
                {
                    throw conflict;
                }
            }
        }
    }

    /**
     * Applies changes to a fresh transaction, and commits it only if our
     * epoch is still current. The callback may run more than once (once
     * per attempt), so it must have no side effects outside the
     * transaction.
     *
     * @param table   the table
     * @param changes makes the changes on the transaction
     * @return the new current snapshot
     * @throws LeaseLostException    if the lease was lost, or a successor
     *                               fenced this instance out
     * @throws CommitFailedException if the commit keeps conflicting
     */
    @Override
    public Snapshot commit(Table table, Consumer<Transaction> changes)
    {
        for (int attempt = 1; ; attempt++)
        {
            lease.checkHeld();
            table.refresh();
            long current = epochOf(table.properties());
            if (current != lease.epoch())
            {
                throw new LeaseLostException(LeaseLostException.Reason.FENCED);
            }
            Snapshot before = table.currentSnapshot();
            Transaction transaction = table.newTransaction();
            changes.accept(transaction);
            Snapshot after = transaction.table().currentSnapshot();
            if (after == null || (before != null
                    && before.snapshotId() == after.snapshotId()))
            {
                addMarker(transaction, lease.epoch());
            }
            try
            {
                lease.checkHeld();
                compareAndSwap(transaction, lease.epoch());
                table.refresh();
                return table.currentSnapshot();
            }
            catch (CommitFailedException conflict)
            {
                LOG.warn("commit.conflict attempt={} epoch={}", attempt,
                        lease.epoch());
                if (attempt >= maxAttempts)
                {
                    throw conflict;
                }
            }
        }
    }

    /**
     * Checks, without committing, that this instance still holds the lease
     * and the table carries our epoch. Used right before operations that
     * commit through Iceberg's own path (snapshot expiry, design §8.4).
     *
     * @param table the table
     * @throws LeaseLostException if the lease was lost or fenced
     */
    @Override
    public void verifyCurrent(Table table)
    {
        lease.checkHeld();
        table.refresh();
        if (epochOf(table.properties()) != lease.epoch())
        {
            throw new LeaseLostException(LeaseLostException.Reason.FENCED);
        }
    }

    /**
     * The epoch a table records.
     *
     * @param properties the table's properties
     * @return the epoch; 0 if none is recorded
     */
    public static long epochOf(Map<String, String> properties)
    {
        String value = properties.get(EPOCH_PROPERTY);
        return value == null ? 0L : Long.parseLong(value);
    }

    /**
     * Adds an empty append, marked with the epoch, so the commit adds a
     * snapshot.
     *
     * @param transaction the transaction
     * @param epoch       the epoch to mark it with
     */
    private static void addMarker(Transaction transaction, long epoch)
    {
        transaction.newFastAppend().set(MARKER_SUMMARY, Long.toString(epoch))
                .commit();
    }

    /**
     * Commits a transaction from exactly the metadata it started on.
     *
     * @param transaction   the transaction
     * @param expectedEpoch the epoch its start metadata must record
     * @throws LeaseLostException    if it records another
     * @throws CommitFailedException if the table moved since
     */
    private static void compareAndSwap(Transaction transaction,
            long expectedEpoch)
    {
        if (!(transaction instanceof BaseTransaction base))
        {
            throw new IllegalStateException("Unsupported transaction type "
                    + transaction.getClass().getName());
        }
        if (epochOf(base.startMetadata().properties()) != expectedEpoch)
        {
            throw new LeaseLostException(LeaseLostException.Reason.FENCED);
        }
        commitStrictly(transaction);
    }

    /**
     * Commits a transaction from exactly the metadata it started on, with
     * {@code TableOperations.commit}: it fails if anyone else committed in
     * between, where Iceberg's own commit would apply the changes again on
     * top. For a REST catalog the transaction must add a snapshot (see the
     * class notes) for the server to check that.
     *
     * @param transaction the transaction, from {@code Table.newTransaction}
     * @throws CommitFailedException if the table moved
     * @throws IllegalStateException if it isn't Iceberg's own transaction
     */
    public static void commitStrictly(Transaction transaction)
    {
        if (!(transaction instanceof BaseTransaction base))
        {
            throw new IllegalStateException("Unsupported transaction type "
                    + transaction.getClass().getName());
        }
        TableMetadata start = base.startMetadata();
        base.underlyingOps().commit(start, base.currentMetadata());
    }
}
