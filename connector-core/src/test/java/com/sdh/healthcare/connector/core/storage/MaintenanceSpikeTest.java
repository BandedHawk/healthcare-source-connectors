package com.sdh.healthcare.connector.core.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.core.TestCatalogs;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.Test;

/** Spike §16.4: compaction and snapshot expiry without Spark, under fencing. */
class MaintenanceSpikeTest {
    final FencedCommitter committer = new FencedCommitter(new LeaseToken() {
        public long epoch() { return 1; }
        public void checkHeld() {}
    });

    Table tableWithSmallFiles(int files) throws Exception {
        Table table = IngestTables.sections(TestCatalogs.jdbc().newClient(), "ns_" + UUID.randomUUID().toString().replace("-", ""));
        committer.establish(table);
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        AesKeyWrapProvider keys = new AesKeyWrapProvider(Map.of("k", kg.generateKey()));
        Instant day = Instant.parse("2026-10-28T10:00:00Z");
        for (int i = 0; i < files; i++) {
            SectionBatchWriter w = new SectionBatchWriter(table, keys, "k", new SectionIds(new byte[32]), day);
            w.add(new SectionBatchWriter.SectionInput(UUID.randomUUID(), 0, 0, "csv", ("row" + i).getBytes()));
            DataFile df = w.finish();
            committer.commit(table, tx -> tx.newFastAppend().appendFile(df).commit());
        }
        return table;
    }

    /** Bin-pack: read the small files of one partition, write one file, swap with RewriteFiles. */
    @Test
    void binPackRewriteCommitsThroughFencedCommitter() throws Exception {
        Table table = tableWithSmallFiles(5);
        List<DataFile> small = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            tasks.forEach(t -> small.add(t.file()));
        }
        assertThat(small).hasSize(5);

        String path = table.locationProvider().newDataLocation(table.spec(), small.get(0).partition(),
                FileFormat.PARQUET.addExtension("compact-" + UUID.randomUUID()));
        DataWriter<Record> out = Parquet.writeData(table.io().newOutputFile(path))
                .schema(table.schema()).withSpec(table.spec()).withPartition(small.get(0).partition())
                .withSortOrder(table.sortOrder()).setAll(table.properties())
                .createWriterFunc(GenericParquetWriter::create).overwrite().build();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record r : rows) {
                out.write(r.copy());
            }
        }
        out.close();
        DataFile merged = out.toDataFile();

        long snapshotBefore = table.currentSnapshot().snapshotId();
        committer.commit(table, tx -> {
            var rewrite = tx.newRewrite().validateFromSnapshot(snapshotBefore);
            small.forEach(rewrite::deleteFile);
            rewrite.addFile(merged);
            rewrite.commit();
        });

        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            assertThat(tasks).hasSize(1);
        }
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            assertThat(rows).hasSize(5);
        }
    }

    /**
     * Expiry inside our own compare-and-swap commit updates metadata, but Iceberg only deletes the
     * expired files from commitTransaction()/commit(), which the fenced path skips. So physical
     * removal (needed for PHI retention) must come from a standalone expireSnapshots() run.
     */
    @Test
    void expiryRemovesFilesOnlyWhenRunStandalone() throws Exception {
        Table table = tableWithSmallFiles(2);
        List<DataFile> old = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            tasks.forEach(t -> old.add(t.file()));
        }
        // Retention: drop everything, then expire all but the current snapshot.
        committer.commit(table, tx -> tx.newDelete().deleteFromRowFilter(
                org.apache.iceberg.expressions.Expressions.alwaysTrue()).commit());
        long now = System.currentTimeMillis() + 1;

        committer.commit(table, tx -> tx.expireSnapshots().expireOlderThan(now).retainLast(1).commit());
        assertThat(exists(table, old.get(0).location())).as("fenced path leaves files").isTrue();

        table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1).retainLast(1).commit();
        assertThat(exists(table, old.get(0).location())).as("standalone expiry deletes PHI files").isFalse();
    }

    static boolean exists(Table t, String location) throws IOException {
        try {
            return t.io().newInputFile(location).exists();
        } catch (NotFoundException e) {
            return false;
        }
    }
}
