/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.core.TestCatalogs;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for {@link LeasedCommitter} and the shared ID key
 * ({@link IdKeys#loadOrCreateShared}): a table many workers write to, each
 * through a catalog client of its own (STAGES.md §8).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class LeasedCommitterTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    LeasedCommitterTest()
    {
    }

    /**
     * A lease held until a test says otherwise.
     */
    static final class Lease implements LeaseToken
    {

        /** Whether it is lost. */
        volatile boolean lost;

        /** Creates a held lease. */
        Lease()
        {
        }

        @Override
        public long epoch()
        {
            return 1;
        }

        @Override
        public void checkHeld()
        {
            if (lost)
            {
                throw new LeaseLostException(
                        LeaseLostException.Reason.EXPIRED);
            }
        }
    }

    /**
     * The same new {@code split_records} table, through several catalog
     * clients.
     *
     * @param kind    the catalog kind
     * @param clients how many
     * @return a table handle per client
     */
    static List<Table> workers(TestCatalogs.Kind kind, int clients)
    {
        TestCatalogs.Backend backend = TestCatalogs.create(kind);
        String namespace = "ns_" + UUID.randomUUID().toString()
                .replace("-", "");
        List<Table> tables = new ArrayList<>();
        tables.add(IngestTables.splitRecords(backend.newClient(), namespace));
        for (int client = 1; client < clients; client++)
        {
            Catalog catalog = backend.newClient();
            tables.add(catalog.loadTable(TableIdentifier.of(namespace,
                    IngestTables.SPLIT_RECORDS)));
        }
        return tables;
    }

    /**
     * A data file to append, that needn't exist.
     *
     * @param table the table
     * @param name  its name
     * @return the file
     */
    static DataFile file(Table table, String name)
    {
        return DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/" + name + ".parquet")
                .withFormat(FileFormat.PARQUET).withFileSizeInBytes(10)
                .withRecordCount(1).build();
    }

    /**
     * Two workers appending from stale views both land: the second's
     * append is applied on top of the first's. A lost lease commits
     * nothing.
     *
     * @param kind the catalog kind
     */
    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void appendsFromManyWorkersAllLand(TestCatalogs.Kind kind)
    {
        List<Table> tables = workers(kind, 2);
        Lease lease = new Lease();
        LeasedCommitter committer = new LeasedCommitter(lease);
        Table first = tables.get(0);
        Table second = tables.get(1);
        second.refresh();
        committer.commit(first, transaction -> transaction.newFastAppend()
                .appendFile(file(first, "a")).commit());
        committer.commit(second, transaction -> transaction.newFastAppend()
                .appendFile(file(second, "b")).commit());
        first.refresh();
        assertThat(first.currentSnapshot().summary())
                .containsEntry("total-data-files", "2");

        lease.lost = true;
        assertThatThrownBy(() -> committer.commit(first, transaction ->
                transaction.newFastAppend().appendFile(file(first, "c"))
                        .commit())).isInstanceOf(LeaseLostException.class);
        assertThatThrownBy(() -> committer.verifyCurrent(first))
                .isInstanceOf(LeaseLostException.class);
        first.refresh();
        assertThat(first.currentSnapshot().summary())
                .containsEntry("total-data-files", "2");
    }

    /**
     * Workers starting together on a new table all end up with the same
     * ID key, whichever stored it.
     *
     * @param kind the catalog kind
     * @throws Exception if a worker fails
     */
    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void workersStartingTogetherShareOneIdKey(TestCatalogs.Kind kind)
            throws Exception
    {
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        AesKeyWrapProvider keys = new AesKeyWrapProvider(
                Map.of("kek", generator.generateKey()));
        List<Table> tables = workers(kind, 4);
        ExecutorService pool = Executors.newFixedThreadPool(tables.size());
        try
        {
            List<Callable<byte[]>> starts = new ArrayList<>();
            for (Table table : tables)
            {
                starts.add(() -> IdKeys.loadOrCreateShared(table, keys,
                        "kek"));
            }
            List<byte[]> found = new ArrayList<>();
            for (Future<byte[]> start : pool.invokeAll(starts))
            {
                found.add(start.get());
            }
            assertThat(found).allSatisfy(key ->
                    assertThat(key).isEqualTo(found.getFirst()));
            assertThat(IdKeys.load(tables.getFirst(), keys))
                    .isEqualTo(found.getFirst());
        }
        finally
        {
            pool.shutdownNow();
        }
    }
}
