/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * When a batch's Parquet file fails to close, the error that caused it
 * must be the one reported. Parquet refuses a second close of a failed
 * writer ("Cannot return unfinished footer"); the cleanup path used to
 * close again and throw that instead, which hid an out-of-memory error
 * during the heap-sizing runs (design §13.1).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class AbandonedBatchTest
{

    /** The message of the injected write failure. */
    static final String FAILURE = "disk full (test)";

    /** The data keys' KEK. */
    AesKeyWrapProvider keys;

    /** A fresh in-memory catalog. */
    InMemoryCatalog catalog;

    /**
     * Creates the catalog and a KEK.
     *
     * @throws Exception if the key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(
                CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of("k", generator.generateKey()));
    }

    /** Landing reports the write failure, not the failed second close. */
    @Test
    void landingReportsTheWriteFailureNotTheSecondClose()
    {
        Table table = failingClose(
                LandingTables.landedItems(catalog, "sdh_landing"));
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
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
        committer.establish(table);
        ItemLander lander = new ItemLander(table, committer, keys,
                new RecordIds(new byte[32]),
                ItemLander.Settings.defaults("intake", "k")
                        .withSizes(1024, 4096, 4096)
                        .withRetry(new RetryPolicy(1, Duration.ofMillis(1),
                                Duration.ofMillis(1))),
                Clock.systemUTC());

        assertThatThrownBy(() -> lander.land(new NewItem(UUID.randomUUID(),
                ItemKind.FILE, null, null, null),
                offset -> new ByteArrayInputStream(new byte[5000])))
                .hasRootCauseMessage(FAILURE);
    }

    /** Closing a record batch after its finish failed doesn't throw. */
    @Test
    void recordBatchCloseAfterFailedFinishDoesNotThrow()
    {
        Table table = failingClose(IngestTables.records(catalog,
                "sdh_ingest"));
        RecordBatchWriter writer = new RecordBatchWriter(table, keys, "k",
                new RecordIds(new byte[32]), Instant.now());
        writer.add(new RecordBatchWriter.RecordInput(UUID.randomUUID(), 0,
                0, "csv", "a,b".getBytes()));

        assertThatThrownBy(writer::finish).hasRootCauseMessage(FAILURE);
        assertThatCode(writer::close).doesNotThrowAnyException();
    }

    /**
     * The table, with every Parquet data file failing once its row data is
     * written: after the 4-byte header, before the footer, as an
     * out-of-memory error part-way through a row group would. Metadata
     * files are untouched.
     *
     * @param table the table
     * @return the same table, through failing storage
     */
    static Table failingClose(Table table)
    {
        TableOperations operations =
                ((HasTableOperations) table).operations();
        FileIO io = new FileIO()
        {
            @Override
            public InputFile newInputFile(String path)
            {
                return operations.io().newInputFile(path);
            }

            @Override
            public OutputFile newOutputFile(String path)
            {
                OutputFile real = operations.io().newOutputFile(path);
                if (!path.endsWith(".parquet"))
                {
                    return real;
                }
                return new OutputFile()
                {
                    @Override
                    public PositionOutputStream create()
                    {
                        return failing(real.create());
                    }

                    @Override
                    public PositionOutputStream createOrOverwrite()
                    {
                        return failing(real.createOrOverwrite());
                    }

                    @Override
                    public String location()
                    {
                        return real.location();
                    }

                    @Override
                    public InputFile toInputFile()
                    {
                        return real.toInputFile();
                    }
                };
            }

            @Override
            public void deleteFile(String path)
            {
                operations.io().deleteFile(path);
            }
        };
        TableOperations failing = new TableOperations()
        {
            @Override
            public TableMetadata current()
            {
                return operations.current();
            }

            @Override
            public TableMetadata refresh()
            {
                return operations.refresh();
            }

            @Override
            public void commit(TableMetadata base, TableMetadata metadata)
            {
                operations.commit(base, metadata);
            }

            @Override
            public FileIO io()
            {
                return io;
            }

            @Override
            public String metadataFileLocation(String fileName)
            {
                return operations.metadataFileLocation(fileName);
            }

            @Override
            public LocationProvider locationProvider()
            {
                return operations.locationProvider();
            }
        };
        return new BaseTable(failing, table.name());
    }

    /**
     * A stream that fails once more than 4 bytes would have been written.
     *
     * @param out the real stream
     * @return the failing stream
     */
    static PositionOutputStream failing(PositionOutputStream out)
    {
        return new PositionOutputStream()
        {
            @Override
            public long getPos() throws IOException
            {
                return out.getPos();
            }

            @Override
            public void write(int value) throws IOException
            {
                check(1);
                out.write(value);
            }

            @Override
            public void write(byte[] bytes, int offset, int length)
                    throws IOException
            {
                check(length);
                out.write(bytes, offset, length);
            }

            private void check(int length) throws IOException
            {
                if (out.getPos() + length > 4)
                {
                    throw new IOException(FAILURE);
                }
            }

            @Override
            public void close() throws IOException
            {
                out.close();
            }
        };
    }
}
