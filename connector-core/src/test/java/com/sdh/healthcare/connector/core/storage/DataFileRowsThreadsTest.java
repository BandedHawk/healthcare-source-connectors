/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.table.DataFileRows;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Many virtual threads reading rows at once, as the split stage's items do
 * just after it starts, in a JVM that hasn't read Parquet before: none may
 * wait for ever (docs/PERFORMANCE.md §10, 2026-11-15).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class DataFileRowsThreadsTest
{

    /** The file's schema. */
    static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "payload",
                    Types.BinaryType.get()));

    /** How many threads read at once. */
    static final int THREADS = 32;

    /** Where the file is written. */
    @TempDir
    Path folder;

    /**
     * Writes a file compressed with zstd, as the tables' are, then reads it
     * from a new JVM with two carrier threads, as a pod with two CPUs has.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void manyVirtualThreadsReadAtOnceInANewJvm() throws Exception
    {
        File file = folder.resolve("rows.parquet").toFile();
        Random random = new Random(3);
        try (FileAppender<Record> appender = Parquet
                .write(Files.localOutput(file)).schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::create)
                .set(TableProperties.PARQUET_COMPRESSION, "zstd").build())
        {
            for (long id = 0; id < THREADS; id++)
            {
                byte[] payload = new byte[4_000];
                random.nextBytes(payload);
                GenericRecord row = GenericRecord.create(SCHEMA);
                row.setField("id", id);
                row.setField("payload", ByteBuffer.wrap(payload));
                appender.add(row);
            }
        }
        for (int run = 0; run < 3; run++)
        {
            Path log = folder.resolve("reader-" + run + ".log");
            Process reader = new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java")
                            .toString(),
                    "-Djdk.virtualThreadScheduler.parallelism=2",
                    "-Djdk.virtualThreadScheduler.maxPoolSize=2",
                    "-cp", System.getProperty("java.class.path"),
                    Reader.class.getName(), file.getAbsolutePath(),
                    Long.toString(file.length()))
                    .redirectErrorStream(true)
                    .redirectOutput(log.toFile())
                    .start();
            boolean ended = reader.waitFor(60, TimeUnit.SECONDS);
            if (!ended)
            {
                reader.destroyForcibly();
            }
            Path threads = Path.of(file.getAbsolutePath() + ".threads");
            String output = java.nio.file.Files.readString(log)
                    + (java.nio.file.Files.exists(threads)
                            ? java.nio.file.Files.readString(threads) : "");
            assertThat(ended).as("run %d ended: %s", run, output).isTrue();
            assertThat(reader.exitValue()).as("run %d: %s", run, output)
                    .isZero();
        }
    }

    /** Reads every row, each on its own virtual thread, all at once. */
    static final class Reader
    {

        /** Not instantiated. */
        private Reader()
        {
        }

        /**
         * Reads the rows.
         *
         * @param args the file and its size
         * @throws Exception if a read fails or the threads don't end; if
         *         they don't, every thread's stack is in a file beside the
         *         data file
         */
        public static void main(String[] args) throws Exception
        {
            String path = args[0];
            long size = Long.parseLong(args[1]);
            FileIO io = new FileIO()
            {
                @Override
                public InputFile newInputFile(String location)
                {
                    return Files.localInput(location);
                }

                @Override
                public OutputFile newOutputFile(String location)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void deleteFile(String location)
                {
                    throw new UnsupportedOperationException();
                }
            };
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger read = new AtomicInteger();
            List<Thread> threads = new ArrayList<>();
            for (int index = 0; index < THREADS; index++)
            {
                long row = index;
                threads.add(Thread.ofVirtual().start(() ->
                {
                    try
                    {
                        start.await();
                        List<Record> rows = DataFileRows.read(io, SCHEMA,
                                path, size, row, 1,
                                List.of("id", "payload"));
                        if (rows.size() == 1
                                && rows.get(0).getField("id").equals(row))
                        {
                            read.incrementAndGet();
                        }
                    }
                    catch (Exception failed)
                    {
                        failed.printStackTrace();
                    }
                }));
            }
            start.countDown();
            for (Thread thread : threads)
            {
                if (!thread.join(java.time.Duration.ofSeconds(45)))
                {
                    System.err.println("A reader is stuck");
                    String dump = path + ".threads";
                    java.lang.management.ManagementFactory
                            .getPlatformMXBean(com.sun.management
                                    .HotSpotDiagnosticMXBean.class)
                            .dumpThreads(dump, com.sun.management
                                    .HotSpotDiagnosticMXBean
                                    .ThreadDumpFormat.TEXT_PLAIN);
                    System.err.println("Threads in " + dump);
                    System.exit(2);
                }
            }
            System.exit(read.get() == THREADS ? 0 : 1);
        }
    }
}
