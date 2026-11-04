/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.TestCatalogs;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Fencing on both catalog families. "A" is a stale instance (epoch 1), "B"
 * its successor (epoch 2). Each uses its own catalog client, as two
 * processes would.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class FencedCommitterTest
{

    /** The checkpoint property the tests write. */
    static final String CHECKPOINT = "sdh.checkpoint.file-connector";

    /**
     * A lease that is always believed held: a stale instance that thinks
     * it still holds the lease (resumed after a long pause, say).
     *
     * @param epoch the lease epoch
     */
    record Token(long epoch) implements LeaseToken
    {

        /**
         * Does nothing: the lease is always believed held.
         */
        @Override
        public void checkHeld()
        {
        }
    }

    /**
     * The same new sections table, through two catalog clients.
     *
     * @param kind the catalog kind
     * @return the table for A, then for B
     */
    Table[] twoProcesses(TestCatalogs.Kind kind)
    {
        TestCatalogs.Backend backend = TestCatalogs.create(kind);
        String namespace = "ns_" + UUID.randomUUID().toString()
                .replace("-", "");
        Table first = IngestTables.sections(backend.newClient(), namespace);
        Table second = backend.newClient()
                .loadTable(TableIdentifier.of(namespace, "sections"));
        return new Table[] {first, second};
    }

    /**
     * A properties-only commit by the holder adds a marker snapshot.
     *
     * @param kind the catalog kind
     */
    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void holderCommitsPropertiesOnlyCheckpointAsSnapshot(
            TestCatalogs.Kind kind)
    {
        Table table = twoProcesses(kind)[0];
        FencedCommitter committer = new FencedCommitter(new Token(1));
        committer.establish(table);
        int snapshots = size(table);
        committer.commit(table, transaction -> transaction
                .updateProperties().set(CHECKPOINT, "seq=10").commit());
        assertThat(table.properties())
                .containsEntry(CHECKPOINT, "seq=10")
                .containsEntry(FencedCommitter.EPOCH_PROPERTY, "1");
        assertThat(size(table)).as("marker snapshot added")
                .isEqualTo(snapshots + 1);
    }

    /**
     * Once the successor has established, the stale holder's commit is
     * refused.
     *
     * @param kind the catalog kind
     */
    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void staleHolderIsFencedAfterSuccessorEstablishes(TestCatalogs.Kind kind)
    {
        Table[] tables = twoProcesses(kind);
        FencedCommitter stale = new FencedCommitter(new Token(1));
        stale.establish(tables[0]);
        new FencedCommitter(new Token(2)).establish(tables[1]);

        assertThatThrownBy(() -> stale.commit(tables[0], transaction ->
                transaction.updateProperties().set(CHECKPOINT, "A").commit()))
                .isInstanceOf(LeaseLostException.class)
                .extracting(error -> ((LeaseLostException) error).reason())
                .isEqualTo(LeaseLostException.Reason.FENCED);
        tables[1].refresh();
        assertThat(tables[1].properties()).doesNotContainKey(CHECKPOINT);
    }

    /**
     * The successor establishes after A has refreshed and built its
     * change, but before A commits.
     *
     * @param kind the catalog kind
     */
    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void raceBetweenBuildAndCommitIsFenced(TestCatalogs.Kind kind)
    {
        Table[] tables = twoProcesses(kind);
        FencedCommitter stale = new FencedCommitter(new Token(1));
        stale.establish(tables[0]);
        FencedCommitter successor = new FencedCommitter(new Token(2));
        AtomicBoolean once = new AtomicBoolean();

        assertThatThrownBy(() -> stale.commit(tables[0], transaction ->
        {
            transaction.updateProperties().set(CHECKPOINT, "A").commit();
            if (once.compareAndSet(false, true))
            {
                successor.establish(tables[1]);
            }
        })).isInstanceOf(LeaseLostException.class);
        tables[1].refresh();
        assertThat(tables[1].properties())
                .containsEntry(FencedCommitter.EPOCH_PROPERTY, "2")
                .doesNotContainKey(CHECKPOINT);
    }

    /**
     * A lower epoch can't establish over a higher one.
     *
     * @param kind the catalog kind
     */
    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void lowerEpochCannotEstablish(TestCatalogs.Kind kind)
    {
        Table[] tables = twoProcesses(kind);
        new FencedCommitter(new Token(2)).establish(tables[1]);
        assertThatThrownBy(() -> new FencedCommitter(new Token(1))
                .establish(tables[0]))
                .isInstanceOf(LeaseLostException.class);
    }

    /**
     * Control: why the committer bypasses commitTransaction(). Iceberg
     * re-applies a stale transaction on top of the successor's metadata,
     * so the stale write lands even though the epoch moved on.
     *
     * @param kind the catalog kind
     */
    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void plainIcebergTransactionIsNotFenced(TestCatalogs.Kind kind)
    {
        Table[] tables = twoProcesses(kind);
        new FencedCommitter(new Token(1)).establish(tables[0]);
        tables[0].refresh();
        Transaction stale = tables[0].newTransaction();
        stale.updateProperties().set(CHECKPOINT, "A").commit();
        stale.newFastAppend().commit();

        new FencedCommitter(new Token(2)).establish(tables[1]);
        stale.commitTransaction(); // no error

        tables[1].refresh();
        assertThat(tables[1].properties())
                .containsEntry(FencedCommitter.EPOCH_PROPERTY, "2")
                .containsEntry(CHECKPOINT, "A");
    }

    /**
     * At most 100 old metadata files are kept.
     *
     * @param kind the catalog kind
     */
    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void oldMetadataFilesAreCappedAt100(TestCatalogs.Kind kind)
    {
        Table table = twoProcesses(kind)[0];
        FencedCommitter committer = new FencedCommitter(new Token(1));
        committer.establish(table);
        String first = ((HasTableOperations) table).operations().current()
                .metadataFileLocation();
        for (int commit = 0; commit < 130; commit++)
        {
            int seq = commit;
            committer.commit(table, transaction -> transaction
                    .updateProperties().set(CHECKPOINT, "seq=" + seq)
                    .commit());
        }
        TableMetadata metadata =
                ((HasTableOperations) table).operations().current();
        assertThat(metadata.previousFiles()).hasSizeLessThanOrEqualTo(100);
        if (kind == TestCatalogs.Kind.JDBC)
        {
            // Client-side cleanup; a REST server applies its own policy.
            // InMemoryFileIO: deleted.
            assertThatThrownBy(() -> table.io().newInputFile(first).exists())
                    .isInstanceOf(NotFoundException.class);
        }
    }

    /**
     * How many snapshots a table has.
     *
     * @param table the table
     * @return the count
     */
    static int size(Table table)
    {
        int count = 0;
        for (Object ignored : table.snapshots())
        {
            count++;
        }
        return count;
    }
}
