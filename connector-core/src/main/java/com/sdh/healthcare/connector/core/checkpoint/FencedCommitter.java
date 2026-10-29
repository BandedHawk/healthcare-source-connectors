package com.sdh.healthcare.connector.core.checkpoint;

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
 * Commits to Iceberg tables only while this instance holds the lease, checked with a fencing token
 * (the lease epoch) stored in the table property {@value #EPOCH_PROPERTY}.
 *
 * <p>Iceberg's own {@code Transaction.commitTransaction()} cannot do this. Before committing, it
 * refreshes the table and re-applies our changes onto whatever metadata is newest, so a stale
 * instance's changes would be laid over a successor's. This class instead commits with a strict
 * compare-and-swap through the table's {@code TableOperations}:
 *
 * <ol>
 *   <li>Refresh, and require the table's epoch to equal ours (equal or lower when establishing).
 *   <li>Build the change as a transaction on that exact metadata.
 *   <li>Make sure the change adds a snapshot to {@code main}. If the caller's change doesn't (for example
 *       a properties-only checkpoint update), an empty marker append is added. With a REST catalog the
 *       commit then carries an {@code assert-ref-snapshot-id} requirement, so the server rejects it if
 *       anyone else committed in between. A properties-only change would carry no such check.
 *       JDBC and Hive catalogs already compare the whole metadata location.
 *   <li>Commit start → new metadata with {@code TableOperations.commit}, which fails if the table moved.
 *       On failure, go back to step 1. If the epoch has changed, this instance has been fenced out.
 * </ol>
 *
 * <p>Every writer of these tables, maintenance included, must commit through this class.
 */
public final class FencedCommitter {
    private static final Logger LOG = LoggerFactory.getLogger(FencedCommitter.class);

    public static final String EPOCH_PROPERTY = "sdh.lease.epoch";
    /** Snapshot summary key set on marker snapshots, for audit. */
    public static final String MARKER_SUMMARY = "sdh.fence-marker";

    private final LeaseToken lease;
    private final int maxAttempts;

    public FencedCommitter(LeaseToken lease, int maxAttempts) {
        this.lease = Objects.requireNonNull(lease);
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
    }

    public FencedCommitter(LeaseToken lease) {
        this(lease, 5);
    }

    /**
     * Records our epoch in the table, fencing out every earlier holder. Call once per table straight
     * after acquiring the lease, before any other commit. Idempotent.
     */
    public void establish(Table table) {
        for (int attempt = 1; ; attempt++) {
            lease.checkHeld();
            table.refresh();
            long current = epochOf(table.properties());
            if (current == lease.epoch()) {
                return;
            }
            if (current > lease.epoch()) {
                throw new LeaseLostException(LeaseLostException.Reason.FENCED);
            }
            Transaction tx = table.newTransaction();
            tx.updateProperties().set(EPOCH_PROPERTY, Long.toString(lease.epoch())).commit();
            addMarker(tx, lease.epoch());
            try {
                compareAndSwap(tx, current);
                LOG.info("lease.fenced_table epoch={} previous={}", lease.epoch(), current);
                return;
            } catch (CommitFailedException e) {
                if (attempt >= maxAttempts) {
                    throw e;
                }
            }
        }
    }

    /**
     * Applies {@code changes} to a fresh transaction and commits it only if our epoch is still current.
     * The callback may run more than once (once per attempt). It must not have side effects outside the
     * transaction.
     *
     * @return the new current snapshot
     * @throws LeaseLostException if the lease was lost or a successor fenced this instance out
     */
    public Snapshot commit(Table table, Consumer<Transaction> changes) {
        for (int attempt = 1; ; attempt++) {
            lease.checkHeld();
            table.refresh();
            long current = epochOf(table.properties());
            if (current != lease.epoch()) {
                throw new LeaseLostException(LeaseLostException.Reason.FENCED);
            }
            Snapshot before = table.currentSnapshot();
            Transaction tx = table.newTransaction();
            changes.accept(tx);
            Snapshot after = tx.table().currentSnapshot();
            if (after == null || (before != null && before.snapshotId() == after.snapshotId())) {
                addMarker(tx, lease.epoch());
            }
            try {
                lease.checkHeld();
                compareAndSwap(tx, lease.epoch());
                table.refresh();
                return table.currentSnapshot();
            } catch (CommitFailedException e) {
                LOG.warn("commit.conflict attempt={} epoch={}", attempt, lease.epoch());
                if (attempt >= maxAttempts) {
                    throw e;
                }
            }
        }
    }

    /**
     * Checks that this instance still holds the lease and that the table carries our epoch, without committing.
     * Used right before operations that commit through Iceberg's own path (snapshot expiry, design §8.4).
     */
    public void verifyCurrent(Table table) {
        lease.checkHeld();
        table.refresh();
        if (epochOf(table.properties()) != lease.epoch()) {
            throw new LeaseLostException(LeaseLostException.Reason.FENCED);
        }
    }

    public static long epochOf(Map<String, String> properties) {
        String v = properties.get(EPOCH_PROPERTY);
        return v == null ? 0L : Long.parseLong(v);
    }

    private static void addMarker(Transaction tx, long epoch) {
        tx.newFastAppend().set(MARKER_SUMMARY, Long.toString(epoch)).commit();
    }

    private static void compareAndSwap(Transaction tx, long expectedEpoch) {
        if (!(tx instanceof BaseTransaction bt)) {
            throw new IllegalStateException("Unsupported transaction type " + tx.getClass().getName());
        }
        TableMetadata start = bt.startMetadata();
        if (epochOf(start.properties()) != expectedEpoch) {
            throw new LeaseLostException(LeaseLostException.Reason.FENCED);
        }
        bt.underlyingOps().commit(start, bt.currentMetadata());
    }
}
