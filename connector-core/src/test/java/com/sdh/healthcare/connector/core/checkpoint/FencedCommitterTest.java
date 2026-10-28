package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.TestCatalogs;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Fencing on both catalog families. "A" is a stale instance (epoch 1), "B" its successor (epoch 2).
 * Each uses its own catalog client, as two processes would.
 */
class FencedCommitterTest {
    static final String CHECKPOINT = "sdh.checkpoint.file-connector";

    record Token(long epoch) implements LeaseToken {
        @Override
        public void checkHeld() {
            // A stale instance that believes it still holds the lease (e.g. resumed after a long pause).
        }
    }

    Table[] twoProcesses(TestCatalogs.Kind kind) {
        TestCatalogs.Backend backend = TestCatalogs.create(kind);
        String ns = "ns_" + UUID.randomUUID().toString().replace("-", "");
        Table a = IngestTables.sections(backend.newClient(), ns);
        Table b = backend.newClient().loadTable(TableIdentifier.of(ns, "sections"));
        return new Table[] {a, b};
    }

    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void holderCommitsPropertiesOnlyCheckpointAsSnapshot(TestCatalogs.Kind kind) {
        Table a = twoProcesses(kind)[0];
        FencedCommitter fa = new FencedCommitter(new Token(1));
        fa.establish(a);
        int snapshots = size(a);
        fa.commit(a, tx -> tx.updateProperties().set(CHECKPOINT, "seq=10").commit());
        assertThat(a.properties()).containsEntry(CHECKPOINT, "seq=10").containsEntry(FencedCommitter.EPOCH_PROPERTY, "1");
        assertThat(size(a)).as("marker snapshot added").isEqualTo(snapshots + 1);
    }

    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void staleHolderIsFencedAfterSuccessorEstablishes(TestCatalogs.Kind kind) {
        Table[] t = twoProcesses(kind);
        FencedCommitter fa = new FencedCommitter(new Token(1));
        fa.establish(t[0]);
        new FencedCommitter(new Token(2)).establish(t[1]);

        assertThatThrownBy(() -> fa.commit(t[0], tx -> tx.updateProperties().set(CHECKPOINT, "A").commit()))
                .isInstanceOf(LeaseLostException.class)
                .extracting(e -> ((LeaseLostException) e).reason())
                .isEqualTo(LeaseLostException.Reason.FENCED);
        t[1].refresh();
        assertThat(t[1].properties()).doesNotContainKey(CHECKPOINT);
    }

    /** The successor establishes after A has refreshed and built its change, but before A commits. */
    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void raceBetweenBuildAndCommitIsFenced(TestCatalogs.Kind kind) {
        Table[] t = twoProcesses(kind);
        FencedCommitter fa = new FencedCommitter(new Token(1));
        fa.establish(t[0]);
        FencedCommitter fb = new FencedCommitter(new Token(2));
        AtomicBoolean once = new AtomicBoolean();

        assertThatThrownBy(() -> fa.commit(t[0], tx -> {
                    tx.updateProperties().set(CHECKPOINT, "A").commit();
                    if (once.compareAndSet(false, true)) {
                        fb.establish(t[1]);
                    }
                }))
                .isInstanceOf(LeaseLostException.class);
        t[1].refresh();
        assertThat(t[1].properties())
                .containsEntry(FencedCommitter.EPOCH_PROPERTY, "2")
                .doesNotContainKey(CHECKPOINT);
    }

    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void lowerEpochCannotEstablish(TestCatalogs.Kind kind) {
        Table[] t = twoProcesses(kind);
        new FencedCommitter(new Token(2)).establish(t[1]);
        assertThatThrownBy(() -> new FencedCommitter(new Token(1)).establish(t[0]))
                .isInstanceOf(LeaseLostException.class);
    }

    /**
     * Control: why the committer bypasses commitTransaction(). Iceberg re-applies a stale transaction on
     * top of the successor's metadata, so the stale write lands even though the epoch moved on.
     */
    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void plainIcebergTransactionIsNotFenced(TestCatalogs.Kind kind) {
        Table[] t = twoProcesses(kind);
        new FencedCommitter(new Token(1)).establish(t[0]);
        t[0].refresh();
        Transaction stale = t[0].newTransaction();
        stale.updateProperties().set(CHECKPOINT, "A").commit();
        stale.newFastAppend().commit();

        new FencedCommitter(new Token(2)).establish(t[1]);
        stale.commitTransaction(); // no error

        t[1].refresh();
        assertThat(t[1].properties())
                .containsEntry(FencedCommitter.EPOCH_PROPERTY, "2")
                .containsEntry(CHECKPOINT, "A");
    }

    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void oldMetadataFilesAreCappedAt100(TestCatalogs.Kind kind) {
        Table a = twoProcesses(kind)[0];
        FencedCommitter fa = new FencedCommitter(new Token(1));
        fa.establish(a);
        String first = ((org.apache.iceberg.HasTableOperations) a).operations().current().metadataFileLocation();
        for (int i = 0; i < 130; i++) {
            int seq = i;
            fa.commit(a, tx -> tx.updateProperties().set(CHECKPOINT, "seq=" + seq).commit());
        }
        var meta = ((org.apache.iceberg.HasTableOperations) a).operations().current();
        assertThat(meta.previousFiles()).hasSizeLessThanOrEqualTo(100);
        if (kind == TestCatalogs.Kind.JDBC) {
            // Client-side cleanup; a REST server applies its own policy.
            assertThatThrownBy(() -> a.io().newInputFile(first).exists())
                    .isInstanceOf(org.apache.iceberg.exceptions.NotFoundException.class); // InMemoryFileIO: deleted
        }
    }

    static int size(Table t) {
        int n = 0;
        for (var ignored : t.snapshots()) {
            n++;
        }
        return n;
    }
}
