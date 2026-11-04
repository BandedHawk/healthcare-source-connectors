/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.Test;

/**
 * Spike §16.4: compaction and snapshot expiry without Spark, under fencing.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class MaintenanceSpikeTest
{

    /** Commits under a lease that is always held, at epoch 1. */
    final FencedCommitter committer = new FencedCommitter(new LeaseToken()
    {
        @Override
        public long epoch()
        {
            return 1;
        }

        @Override
        public void checkHeld()
        {
        }
    });

    /**
     * A new sections table with one small data file per commit.
     *
     * @param files how many files
     * @return the table
     * @throws Exception if it can't be made
     */
    Table tableWithSmallFiles(int files) throws Exception
    {
        Table table = IngestTables.sections(TestCatalogs.jdbc().newClient(),
                "ns_" + UUID.randomUUID().toString().replace("-", ""));
        committer.establish(table);
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        AesKeyWrapProvider keys = new AesKeyWrapProvider(
                Map.of("k", generator.generateKey()));
        Instant day = Instant.parse("2026-10-28T10:00:00Z");
        for (int file = 0; file < files; file++)
        {
            SectionBatchWriter writer = new SectionBatchWriter(table, keys,
                    "k", new SectionIds(new byte[32]), day);
            writer.add(new SectionBatchWriter.SectionInput(UUID.randomUUID(),
                    0, 0, "csv", ("row" + file).getBytes()));
            DataFile dataFile = writer.finish();
            committer.commit(table, transaction -> transaction
                    .newFastAppend().appendFile(dataFile).commit());
        }
        return table;
    }

    /**
     * Bin-pack: read the small files of one partition, write one file, and
     * swap them with RewriteFiles.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void binPackRewriteCommitsThroughFencedCommitter() throws Exception
    {
        Table table = tableWithSmallFiles(5);
        List<DataFile> small = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().planFiles())
        {
            tasks.forEach(task -> small.add(task.file()));
        }
        assertThat(small).hasSize(5);

        String path = table.locationProvider().newDataLocation(table.spec(),
                small.get(0).partition(), FileFormat.PARQUET.addExtension(
                        "compact-" + UUID.randomUUID()));
        DataWriter<Record> out = Parquet.writeData(
                table.io().newOutputFile(path))
                .schema(table.schema()).withSpec(table.spec())
                .withPartition(small.get(0).partition())
                .withSortOrder(table.sortOrder()).setAll(table.properties())
                .createWriterFunc(GenericParquetWriter::create).overwrite()
                .build();
        try (CloseableIterable<Record> rows =
                IcebergGenerics.read(table).build())
        {
            for (Record row : rows)
            {
                out.write(row.copy());
            }
        }
        out.close();
        DataFile merged = out.toDataFile();

        long snapshotBefore = table.currentSnapshot().snapshotId();
        committer.commit(table, transaction ->
        {
            RewriteFiles rewrite = transaction.newRewrite()
                    .validateFromSnapshot(snapshotBefore);
            small.forEach(rewrite::deleteFile);
            rewrite.addFile(merged);
            rewrite.commit();
        });

        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().planFiles())
        {
            assertThat(tasks).hasSize(1);
        }
        try (CloseableIterable<Record> rows =
                IcebergGenerics.read(table).build())
        {
            assertThat(rows).hasSize(5);
        }
    }

    /**
     * Expiry inside our own compare-and-swap commit updates metadata, but
     * Iceberg only deletes the expired files from
     * commitTransaction()/commit(), which the fenced path skips. So
     * physical removal (needed for PHI retention) must come from a
     * standalone expireSnapshots() run.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void expiryRemovesFilesOnlyWhenRunStandalone() throws Exception
    {
        Table table = tableWithSmallFiles(2);
        List<DataFile> old = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().planFiles())
        {
            tasks.forEach(task -> old.add(task.file()));
        }
        // Retention: drop everything, then expire all but the current
        // snapshot.
        committer.commit(table, transaction -> transaction.newDelete()
                .deleteFromRowFilter(Expressions.alwaysTrue()).commit());
        long now = System.currentTimeMillis() + 1;

        committer.commit(table, transaction -> transaction.expireSnapshots()
                .expireOlderThan(now).retainLast(1).commit());
        assertThat(exists(table, old.get(0).location()))
                .as("fenced path leaves files").isTrue();

        table.expireSnapshots()
                .expireOlderThan(System.currentTimeMillis() + 1)
                .retainLast(1).commit();
        assertThat(exists(table, old.get(0).location()))
                .as("standalone expiry deletes PHI files").isFalse();
    }

    /**
     * Tells whether a table's file exists.
     *
     * @param table    the table
     * @param location the file
     * @return true if it exists
     * @throws IOException if it can't be checked
     */
    static boolean exists(Table table, String location) throws IOException
    {
        try
        {
            return table.io().newInputFile(location).exists();
        }
        catch (NotFoundException missing)
        {
            return false;
        }
    }
}
