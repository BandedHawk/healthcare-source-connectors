/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import static com.sdh.healthcare.connector.contract.table.RecordsTable.*;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.KeyServiceUnavailableException;
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.position.RecordOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.HoldReleases;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.convert.ConversionException;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.convert.RecordConverter;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for {@link IngestPipeline}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class IngestPipelineTest
{

    /** The key encryption key that wraps the records' data keys. */
    static final String KEK = "records-kek/1";

    /** How many rows {@link #csvRows()} makes. */
    static final int CSV_ROWS = 250;

    /** How many records a batch commits at most. */
    static final int BATCH = 100;

    /** The in-memory catalog, new for each test. */
    InMemoryCatalog catalog;

    /** The {@code split_records} table's identifier. */
    TableIdentifier tableId = TableIdentifier.of("sdh_split",
            IngestTables.SPLIT_RECORDS);

    /** The item each test splits. */
    final UUID item = UUID.randomUUID();

    /** The schema of this test's checkpoint table. */
    final String schema = SplitTestSupport.freshSchema();

    /** The item's lease in each process. */
    final Map<IngestPipeline, SplitTestSupport.TestLease> leases =
            new HashMap<>();

    /** Wraps and unwraps data keys with {@link #KEK}. */
    AesKeyWrapProvider keys;

    /** The key pair the connector signs its messages with. */
    KeyPair signer;

    /** The key pair the messages are encrypted to. */
    KeyPair recipient;

    /** Signs and encrypts the connector's messages. */
    MessageSealer sealer;

    /** Opens the connector's messages, as the reader downstream would. */
    MessageOpener opener;

    /** The last item lease epoch given to a process; each is higher. */
    long epochs;

    /** The pipelines the test made, closed after it. */
    final List<IngestPipeline> open = new ArrayList<>();

    /** Creates the tests; JUnit runs each test on a new instance. */
    IngestPipelineTest()
    {
    }

    /**
     * Makes a new catalog with an empty {@code split_records} table, a key
     * encryption key, and the key pairs that seal and open messages.
     *
     * @throws Exception if a key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        IngestTables.splitRecords(catalog, "sdh_split");
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(KEK, generator.generateKey()));
        signer = ec();
        recipient = ec();
        sealer = new MessageSealer(
                new ECDSASigner((ECPrivateKey) signer.getPrivate()),
                "connector", recipient.getPublic(), "downstream");
        opener = new MessageOpener(
                Map.of("downstream", recipient.getPrivate()),
                Map.of("connector", (ECPublicKey) signer.getPublic()));
    }

    /** Closes the pipelines the test made. */
    @AfterEach
    void tearDown()
    {
        open.forEach(IngestPipeline::close);
    }

    /**
     * Makes settings for testing the pipeline: records are stored as cut.
     *
     * @return the settings
     */
    PipelineSettings settings()
    {
        return PipelineSettings.defaults("file-connector", "csv", "prod", KEK)
                .withCommit(BATCH, 1L << 30, Duration.ofHours(1))
                .withWorkers(4)
                .withRetry(new RetryPolicy(3, Duration.ofMillis(1),
                        Duration.ofMillis(5)))
                .withConversion(ConverterRegistry.none(), 64L << 20);
    }

    /**
     * Starts a fresh "process": its own table handle, and the item's lease
     * at a higher epoch, as a worker taking the item over would.
     *
     * @param settings  the pipeline's settings
     * @param publisher where its messages go
     * @param hooks     where it can be made to stop
     * @return the pipeline, closed after the test
     */
    IngestPipeline process(PipelineSettings settings,
            MessagePublisher publisher, PipelineHooks hooks)
    {
        Table table = catalog.loadTable(tableId);
        RecordIds ids = new RecordIds(
                IdKeys.loadOrCreateShared(table, keys, KEK));
        IngestPipeline pipeline = new IngestPipeline(settings, table,
                SplitTestSupport.checkpoints(schema, ids), keys, ids,
                SplitterRegistry.load(), sealer, publisher, Clock.systemUTC());
        pipeline.setHooks(hooks);
        open.add(pipeline);
        leases.put(pipeline, new SplitTestSupport.TestLease(++epochs));
        return pipeline;
    }

    /**
     * Splits the test's item with a process, under its lease.
     *
     * @param pipeline the process
     * @param source   the item's bytes
     * @return the outcome
     * @throws Exception if the pipeline throws
     */
    IngestPipeline.SplitResult split(IngestPipeline pipeline,
            SeekableByteSource source) throws Exception
    {
        return pipeline.split(source, item, leases.get(pipeline));
    }

    /**
     * Makes {@link #CSV_ROWS} CSV rows, each with a quoted note of two
     * lines that has a comma in it.
     *
     * @return the rows, each ending in a line break
     */
    static List<String> csvRows()
    {
        List<String> rows = new ArrayList<>();
        for (int index = 0; index < CSV_ROWS; index++)
        {
            rows.add(index + ",name-" + index + ",\"note " + index
                    + "\nsecond line, with comma\"\n");
        }
        return rows;
    }

    /**
     * Joins CSV rows into a file.
     *
     * @param rows the rows, each ending in a line break
     * @return the file
     */
    static SeekableByteSource csv(List<String> rows)
    {
        return SeekableByteSource.of(String.join("", rows).getBytes(UTF_8));
    }

    // ---- happy path ----

    /**
     * Splits a file, encrypts its records, commits them in batches and
     * publishes a message for each, all readable back.
     *
     * @throws Exception if the file can't be processed or read back
     */
    @Test
    void splitsEncryptsCommitsAndPublishesInBatches() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(settings(), publisher,
                PipelineHooks.NONE);
        IngestPipeline.SplitResult result = split(pipeline, csv(csvRows()));

        assertThat(result.recordCount()).isEqualTo(CSV_ROWS);
        Checkpoint checkpoint = pipeline.checkpoint(item).orElseThrow();
        assertThat(checkpoint.state()).isEqualTo(Checkpoint.State.ARCHIVING);
        assertThat(checkpoint.publishedSeq()).isEqualTo(CSV_ROWS - 1);

        List<ConnectorMessage> messages = decode(publisher.sent);
        List<RecordSplit> records = records(messages);
        assertThat(records).hasSameSizeAs(messages);
        assertThat(records).extracting(RecordSplit::seq)
                .containsExactlyElementsOf(
                        LongStream.range(0, CSV_ROWS).boxed().toList());
        assertThat(records).allSatisfy(record ->
        {
            assertThat(record.itemId()).isEqualTo(result.itemId());
            assertThat(record.storage().catalog()).isEqualTo("prod");
            assertThat(record.storage().namespace()).isEqualTo("sdh_split");
            assertThat(record.storage().table())
                    .isEqualTo(IngestTables.SPLIT_RECORDS);
            assertThat(record.errorCode()).isNull();
        });
        // Three batches → three data snapshots, each referenced by its
        // messages.
        assertThat(records.stream()
                .map(record -> record.storage().snapshotId()).distinct())
                .hasSize(3);

        Table table = catalog.loadTable(tableId);
        RecordIds ids =
                new RecordIds(IdKeys.load(table, keys));
        List<String> expected = csvRows();
        for (RecordSplit record : List.of(records.get(0),
                records.get(137), records.get(249)))
        {
            byte[] plaintext =
                    expected.get((int) record.seq()).getBytes(UTF_8);
            assertThat(record.recordId()).isEqualTo(ids.of(plaintext));
            assertThat(decrypt(table, record.itemId(), record.seq()))
                    .isEqualTo(plaintext);
        }
        assertThat(dataRows(table, result.itemId())).hasSize(CSV_ROWS);
    }

    /**
     * A record longer than a chunk is stored in more than one, and reads
     * back whole, as do the records around it.
     *
     * @throws Exception if the file can't be processed or read back
     */
    @Test
    void multiChunkRecordIsStreamedInTwoPasses() throws Exception
    {
        String big = "1,\"" + "x".repeat(RecordCrypto.CHUNK_SIZE + 500)
                + "\"\n";
        String small = "2,y\n";
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(settings(), publisher,
                PipelineHooks.NONE);
        UUID itemId = split(pipeline, csv(List.of(small, big, small))).itemId();

        List<RecordSplit> stored = records(decode(publisher.sent));
        assertThat(stored).extracting(RecordSplit::chunkCount)
                .containsExactly(1, 2, 1);
        Table table = catalog.loadTable(tableId);
        assertThat(decrypt(table, itemId, 1)).isEqualTo(big.getBytes(UTF_8));
        assertThat(decrypt(table, itemId, 2)).isEqualTo(small.getBytes(UTF_8));
    }

    /**
     * With no time between commits, every record is committed on its own.
     *
     * @throws Exception if the file can't be processed or a message opened
     */
    @Test
    void zeroIntervalCommitsEveryRecord() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(
                settings().withCommit(BATCH, 1L << 30, Duration.ZERO),
                publisher, PipelineHooks.NONE);
        split(pipeline, csv(csvRows().subList(0, 5)));
        assertThat(records(decode(publisher.sent)).stream()
                .map(record -> record.storage().snapshotId()).distinct())
                .hasSize(5);
    }

    /**
     * An item split whole with none failed is noted clean in its
     * checkpoint, so its end sent again, by any pod, reads no rows for
     * failed pieces (docs/PERFORMANCE.md §3.4): with the table gone, a
     * clean checkpoint still answers none, and one not known to be clean
     * has to read it.
     *
     * @throws Exception if the file can't be processed
     */
    @Test
    void aCleanItemsEndSentAgainReadsNoRows() throws Exception
    {
        IngestPipeline pipeline = process(settings(),
                new RecordingPublisher(), PipelineHooks.NONE);
        split(pipeline, csv(csvRows()));
        Checkpoint clean = pipeline.checkpoint(item).orElseThrow();
        assertThat(clean.clean()).isTrue();

        IngestPipeline another = process(settings(),
                new RecordingPublisher(), PipelineHooks.NONE);
        Checkpoint unknown = new Checkpoint(clean.itemId(),
                clean.fingerprint(), clean.format(), clean.itemSize(),
                clean.state(), clean.resumeAt(), clean.nextSeq(),
                clean.committedSeq(), clean.publishedSeq(),
                clean.startedAt(), clean.failure(), clean.restarts(),
                clean.version(), clean.cut());
        // Its files deleted too, so reading it fails.
        catalog.dropTable(tableId, true);
        assertThat(another.failedRows(clean)).isEmpty();
        assertThatThrownBy(() -> another.failedRows(unknown))
                .isInstanceOf(RuntimeException.class);
    }

    /**
     * An empty or blank item can't be split: nothing was sent, so it fails
     * for an operator to decide, with nothing stored or announced
     * (STAGES.md §16).
     *
     * @throws Exception if the pipeline can't be made
     */
    @Test
    void emptyFileFails() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(settings(), publisher,
                PipelineHooks.NONE);
        for (byte[] bytes : List.of(new byte[0], " \r\n\t\n".getBytes(UTF_8)))
        {
            assertThatThrownBy(() -> split(pipeline,
                    SeekableByteSource.of(bytes)))
                    .isInstanceOfSatisfying(SplitException.class,
                            failure -> assertThat(failure.code())
                                    .isEqualTo(ErrorCode.SPLIT_EMPTY));
        }
        assertThat(publisher.sent).isEmpty();
    }

    /**
     * A send the broker fails is retried, and every record is still sent.
     *
     * @throws Exception if the file can't be processed or a message opened
     */
    @Test
    void transientBrokerFailuresAreRetried() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        // Every third send fails once; its retry is a later call.
        publisher.fail = call -> call % 3 == 1;
        IngestPipeline pipeline = process(settings(), publisher,
                PipelineHooks.NONE);
        split(pipeline, csv(csvRows()));
        assertThat(records(decode(publisher.sent)).stream()
                .map(RecordSplit::seq).distinct()).hasSize(CSV_ROWS);
    }

    // ---- crash and resume (design §15) ----

    /** Where a test of resuming makes the connector stop. */
    enum Crash
    {

        /** After the first batch's data file is written, before it commits. */
        AFTER_WRITE_FIRST_BATCH,

        /** After the second batch's data file is written, before it commits. */
        AFTER_WRITE_SECOND_BATCH,

        /**
         * After the first batch's checkpoint is kept as pending, before the
         * batch is appended: the pending checkpoint is dropped at resume.
         */
        AFTER_PREPARE_FIRST_BATCH,

        /**
         * After the second batch is appended, before its checkpoint is
         * written: the pending checkpoint is kept at resume.
         */
        AFTER_APPEND_SECOND_BATCH,

        /** After the first batch commits, before its messages are sent. */
        AFTER_COMMIT_FIRST_BATCH,

        /** After the second batch commits, before its messages are sent. */
        AFTER_COMMIT_SECOND_BATCH,

        /** After every record is sent, before the end-of-file commit. */
        BEFORE_END_OF_FILE,

        /** The broker stops taking messages from the 151st send on. */
        BROKER_DOWN_MID_PUBLISH
    }

    /**
     * After a worker stops anywhere in an item, a new process resumes the
     * same item. Every record is stored exactly once and announced at least
     * once, with at most one batch announced twice.
     *
     * @param crash where the connector stops
     * @throws Exception if the file can't be processed or read back
     */
    @ParameterizedTest
    @EnumSource(Crash.class)
    void resumesAfterACrashWithNoGapsAndAtMostOneBatchOfDuplicates(Crash crash)
            throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        PipelineHooks hooks = new PipelineHooks()
        {

            /**
             * Stops the connector after the batch the crash names is written.
             */
            @Override
            public void batchWritten(long lastSeq)
            {
                if ((crash == Crash.AFTER_WRITE_FIRST_BATCH
                        && lastSeq == BATCH - 1)
                        || (crash == Crash.AFTER_WRITE_SECOND_BATCH
                                && lastSeq == 2 * BATCH - 1))
                {
                    throw new SimulatedCrash();
                }
            }

            /**
             * Stops the connector after the batch the crash names is kept
             * as pending.
             */
            @Override
            public void batchPrepared(long lastSeq)
            {
                if (crash == Crash.AFTER_PREPARE_FIRST_BATCH
                        && lastSeq == BATCH - 1)
                {
                    throw new SimulatedCrash();
                }
            }

            /**
             * Stops the connector after the batch the crash names is
             * appended.
             */
            @Override
            public void batchAppended(long lastSeq)
            {
                if (crash == Crash.AFTER_APPEND_SECOND_BATCH
                        && lastSeq == 2 * BATCH - 1)
                {
                    throw new SimulatedCrash();
                }
            }

            /** Stops the connector after the batch the crash names commits. */
            @Override
            public void batchCommitted(long lastSeq)
            {
                if ((crash == Crash.AFTER_COMMIT_FIRST_BATCH
                        && lastSeq == BATCH - 1)
                        || (crash == Crash.AFTER_COMMIT_SECOND_BATCH
                                && lastSeq == 2 * BATCH - 1))
                {
                    throw new SimulatedCrash();
                }
            }

            /** Stops the connector before the end of the file, if asked to. */
            @Override
            public void beforeEndOfFile()
            {
                if (crash == Crash.BEFORE_END_OF_FILE)
                {
                    throw new SimulatedCrash();
                }
            }
        };
        if (crash == Crash.BROKER_DOWN_MID_PUBLISH)
        {
            publisher.fail = call -> call >= 150;
        }
        List<String> fileRows = csvRows();
        IngestPipeline first = process(settings(), publisher, hooks);
        assertThatThrownBy(() -> split(first, csv(fileRows)))
                .isInstanceOfAny(SimulatedCrash.class,
                        PublishFailedException.class);
        first.close();
        UUID firstItemId = item;
        assertThat(first.checkpoint(item).orElseThrow().state())
                .isEqualTo(Checkpoint.State.SPLITTING);

        publisher.fail = call -> false;
        IngestPipeline second = process(settings(), publisher,
                PipelineHooks.NONE);
        IngestPipeline.SplitResult result = split(second, csv(fileRows));
        assertThat(result.itemId()).isEqualTo(firstItemId);
        assertThat(result.recordCount()).isEqualTo(CSV_ROWS);

        List<RecordSplit> sent = records(decode(publisher.sent));
        Map<Long, List<RecordSplit>> bySeq = sent.stream()
                .collect(Collectors.groupingBy(RecordSplit::seq));
        assertThat(bySeq.keySet()).hasSize(CSV_ROWS)
                .allMatch(seq -> seq >= 0 && seq < CSV_ROWS);
        bySeq.values().forEach(copies -> assertThat(copies.stream()
                .map(RecordSplit::recordId).distinct()).hasSize(1));
        assertThat(sent.size() - CSV_ROWS).as("duplicates")
                .isLessThanOrEqualTo(BATCH);

        Table table = catalog.loadTable(tableId);
        List<Record> rows = dataRows(table, firstItemId);
        assertThat(rows).hasSize(CSV_ROWS);
        assertThat(rows.stream().map(row -> row.getField(SEQ)).distinct())
                .hasSize(CSV_ROWS);
        RecordIds ids =
                new RecordIds(IdKeys.load(table, keys));
        assertThat(rows).allSatisfy(row -> assertThat(row.getField(RECORD_ID))
                .isEqualTo(ids.of(fileRows
                        .get(((Long) row.getField(SEQ)).intValue())
                        .getBytes(UTF_8))));
    }

    // ---- restarts without progress (design §9.2) ----

    /**
     * Stops the connector in its first batch, before the batch commits, as
     * an out-of-memory exit would.
     */
    static final PipelineHooks STOP_IN_FIRST_BATCH = new PipelineHooks()
    {

        /** Stops the connector once the first batch is written. */
        @Override
        public void batchWritten(long lastSeq)
        {
            if (lastSeq == BATCH - 1)
            {
                throw new SimulatedCrash();
            }
        }
    };

    /**
     * Makes a CSV item of {@link #CSV_ROWS} records.
     *
     * @return its bytes
     */
    static byte[] csvFile()
    {
        return String.join("", csvRows()).getBytes(UTF_8);
    }

    /**
     * Runs the file in a fresh process that stops as {@code hooks} says, and
     * checks that it did stop.
     *
     * @param file  the file's bytes
     * @param hooks where the process stops
     * @throws Exception if the process can't be made
     */
    void stopsWhileSplitting(byte[] file, PipelineHooks hooks) throws Exception
    {
        IngestPipeline pipeline = process(settings(),
                new RecordingPublisher(), hooks);
        assertThatThrownBy(() -> split(pipeline, SeekableByteSource.of(file)))
                .isInstanceOf(SimulatedCrash.class);
        pipeline.close();
    }

    /**
     * If workers keep stopping in the same batch, the next start fails the
     * item instead of resuming it, so one item can't keep being taken up.
     *
     * @throws Exception if a process can't be made
     */
    @Test
    void aFileThatKeepsStoppingTheConnectorFails() throws Exception
    {
        byte[] file = csvFile();
        for (int attempt = 0; attempt < IngestPipeline.FAIL_AFTER_RESTARTS;
                attempt++)
        {
            stopsWhileSplitting(file, STOP_IN_FIRST_BATCH);
        }
        IngestPipeline pipeline = process(settings(),
                new RecordingPublisher(), PipelineHooks.NONE);
        assertThatThrownBy(() -> split(pipeline, SeekableByteSource.of(file)))
                .isInstanceOfSatisfying(SplitException.class, error ->
                {
                    assertThat(error.code())
                            .isEqualTo(ErrorCode.RESUME_NO_PROGRESS);
                    assertThat(error.offset()).isZero();
                });
        assertThat(pipeline.checkpoint(item).orElseThrow().state())
                .isEqualTo(Checkpoint.State.SPLITTING);
    }

    /**
     * A stop after a batch has committed is progress: it never counts
     * towards failing the item.
     *
     * @throws Exception if the file can't be processed
     */
    @Test
    void restartsAfterProgressDontCount() throws Exception
    {
        byte[] file = csvFile();
        for (long stopAfter : new long[] {BATCH - 1, 2 * BATCH - 1})
        {
            stopsWhileSplitting(file, new PipelineHooks()
            {

                /**
                 * Stops the connector once the batch ending at
                 * {@code stopAfter} commits.
                 */
                @Override
                public void batchCommitted(long lastSeq)
                {
                    if (lastSeq == stopAfter)
                    {
                        throw new SimulatedCrash();
                    }
                }
            });
            assertThat(process(settings(), new RecordingPublisher(),
                    PipelineHooks.NONE).checkpoint(item).orElseThrow()
                    .restarts()).isZero();
        }
        IngestPipeline pipeline = process(settings(),
                new RecordingPublisher(), PipelineHooks.NONE);
        IngestPipeline.SplitResult result =
                split(pipeline, SeekableByteSource.of(file));
        assertThat(result.recordCount()).isEqualTo(CSV_ROWS);
        assertThat(pipeline.failedRecordCount(result.itemId(), 1)).isZero();
    }

    /**
     * An item part way, begun under one version of the split stage, is held
     * by a pod of another (STAGES.md §14.3): not resumed, so no cuts of two
     * versions mix in it, and no restart counted, since nothing is wrong
     * with it. It stays held when delivered again. Its records so far, and
     * its checkpoint, name the version it was begun under.
     *
     * @throws Exception if a process can't be made
     */
    @Test
    void holdsAnItemBegunUnderAnotherVersion() throws Exception
    {
        byte[] file = csvFile();
        IngestPipeline first = process(settings().withVersion("split-v1"),
                new RecordingPublisher(), new PipelineHooks()
                {

                    /** Stops once the first batch commits. */
                    @Override
                    public void batchCommitted(long lastSeq)
                    {
                        throw new SimulatedCrash();
                    }
                });
        assertThatThrownBy(() -> split(first, SeekableByteSource.of(file)))
                .isInstanceOf(SimulatedCrash.class);
        first.close();
        Checkpoint before = first.checkpoint(item).orElseThrow();
        assertThat(before.version()).isEqualTo("split-v1");
        assertThat(CommittedRecords.all(catalog.loadTable(tableId), item))
                .isNotEmpty()
                .allSatisfy(row -> assertThat(row.stageVersion())
                        .isEqualTo("split-v1"));

        RecordingPublisher published = new RecordingPublisher();
        IngestPipeline changed = process(settings().withVersion("split-v2"),
                published, PipelineHooks.NONE);
        assertThatThrownBy(() -> split(changed, SeekableByteSource.of(file)))
                .isInstanceOfSatisfying(ItemHeldException.class, held ->
                        assertThat(held.heldVersion()).isEqualTo("split-v1"));
        Checkpoint held = changed.checkpoint(item).orElseThrow();
        assertThat(held.state()).isEqualTo(Checkpoint.State.HELD);
        assertThat(held.restarts()).isEqualTo(before.restarts());
        assertThat(held.nextSeq()).isEqualTo(before.nextSeq());
        assertThat(published.sent).isEmpty();

        assertThatThrownBy(() -> split(process(settings()
                .withVersion("split-v2"), published, PipelineHooks.NONE),
                SeekableByteSource.of(file)))
                .isInstanceOf(ItemHeldException.class);
        assertThat(changed.checkpoint(item).orElseThrow().state())
                .isEqualTo(Checkpoint.State.HELD);
    }

    /**
     * Holds an item under split-v1's successor, and returns the process of
     * the new version.
     *
     * @param file      the item's bytes
     * @param published where the new process sends
     * @return the process of split-v2, the item held
     * @throws Exception if a process can't be made
     */
    IngestPipeline heldUnderAnotherVersion(byte[] file,
            RecordingPublisher published) throws Exception
    {
        IngestPipeline first = process(settings().withVersion("split-v1"),
                new RecordingPublisher(), new PipelineHooks()
                {

                    /** Stops once the first batch commits. */
                    @Override
                    public void batchCommitted(long lastSeq)
                    {
                        throw new SimulatedCrash();
                    }
                });
        assertThatThrownBy(() -> split(first, SeekableByteSource.of(file)))
                .isInstanceOf(SimulatedCrash.class);
        first.close();
        IngestPipeline changed = process(settings().withVersion("split-v2"),
                published, PipelineHooks.NONE);
        assertThatThrownBy(() -> split(changed, SeekableByteSource.of(file)))
                .isInstanceOf(ItemHeldException.class);
        return changed;
    }

    /**
     * An operator's release with resume (STAGES.md §15.6e) has a held item
     * split on under the version running now: its records keep the version
     * that made each, so it is made under two.
     *
     * @throws Exception if a process can't be made
     */
    @Test
    void resumesAHeldItemUnderTheNewVersionWhenReleased() throws Exception
    {
        byte[] file = csvFile();
        RecordingPublisher published = new RecordingPublisher();
        IngestPipeline changed = heldUnderAnotherVersion(file, published);

        assertThat(changed.release(item, HoldReleases.Action.RESUME, "alice",
                leases.get(changed))).hasValueSatisfying(held ->
                        assertThat(held.version()).isEqualTo("split-v1"));
        Checkpoint released = changed.checkpoint(item).orElseThrow();
        assertThat(released.state()).isEqualTo(Checkpoint.State.SPLITTING);
        assertThat(released.version()).isEqualTo("split-v2");

        split(changed, SeekableByteSource.of(file));
        assertThat(changed.checkpoint(item).orElseThrow().state())
                .isEqualTo(Checkpoint.State.ARCHIVING);
        assertThat(CommittedRecords.all(catalog.loadTable(tableId), item))
                .hasSize(CSV_ROWS)
                .extracting(CommittedRecords.Row::stageVersion)
                .contains("split-v1", "split-v2");
        // Released once: a second request finds it no longer held.
        assertThat(changed.release(item, HoldReleases.Action.RESUME, "alice",
                leases.get(changed))).isEmpty();
    }

    /**
     * An operator's release with close ends a held item, CLOSED: neither
     * resumed nor held, its records kept.
     *
     * @throws Exception if a process can't be made
     */
    @Test
    void closesAHeldItemWhenReleased() throws Exception
    {
        byte[] file = csvFile();
        RecordingPublisher published = new RecordingPublisher();
        IngestPipeline changed = heldUnderAnotherVersion(file, published);
        long stored = CommittedRecords.all(catalog.loadTable(tableId), item)
                .size();

        assertThat(changed.release(item, HoldReleases.Action.CLOSE, "alice",
                leases.get(changed))).isPresent();
        assertThat(changed.checkpoint(item).orElseThrow().state())
                .isEqualTo(Checkpoint.State.CLOSED);
        assertThatThrownBy(() -> split(changed, SeekableByteSource.of(file)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(CommittedRecords.all(catalog.loadTable(tableId), item))
                .hasSize((int) stored);
        assertThat(published.sent).isEmpty();
    }

    /**
     * An item split under one version names it on every record, its
     * message and its checkpoint (STAGES.md §14.3).
     *
     * @throws Exception if the file can't be processed
     */
    @Test
    void namesItsVersionOnEveryRecord() throws Exception
    {
        RecordingPublisher published = new RecordingPublisher();
        IngestPipeline pipeline = process(settings().withVersion("split-v1"),
                published, PipelineHooks.NONE);
        split(pipeline, SeekableByteSource.of(csvFile()));
        assertThat(CommittedRecords.all(catalog.loadTable(tableId), item))
                .hasSize(CSV_ROWS)
                .allSatisfy(row -> assertThat(row.stageVersion())
                        .isEqualTo("split-v1"));
        assertThat(pipeline.checkpoint(item).orElseThrow().version())
                .isEqualTo("split-v1");
        assertThat(records(decode(published.sent))).hasSize(CSV_ROWS)
                .allSatisfy(record -> assertThat(record.version())
                        .isEqualTo("split-v1"));
    }

    /**
     * Ends an attempt in the first batch with an error, at the step a hook
     * names.
     *
     * @param error    what the attempt ends with
     * @param prepared whether to end it once the batch is pending, rather
     *                 than once it is written
     * @return the hooks
     */
    static PipelineHooks endInFirstBatch(RuntimeException error,
            boolean prepared)
    {
        return new PipelineHooks()
        {

            /** Ends the attempt once the first batch is written. */
            @Override
            public void batchWritten(long lastSeq)
            {
                if (!prepared && lastSeq == BATCH - 1)
                {
                    throw error;
                }
            }

            /** Ends the attempt once the first batch is pending. */
            @Override
            public void batchPrepared(long lastSeq)
            {
                if (prepared && lastSeq == BATCH - 1)
                {
                    throw error;
                }
            }
        };
    }

    /**
     * Tries the file in a fresh process whose attempt ends with an error.
     *
     * @param file  the file's bytes
     * @param hooks where and how the attempt ends
     * @return the error
     * @throws Exception if the process can't be made
     */
    Throwable endsWithAnError(byte[] file, PipelineHooks hooks)
            throws Exception
    {
        IngestPipeline pipeline = process(settings(),
                new RecordingPublisher(), hooks);
        Throwable error = catchThrowable(
                () -> split(pipeline, SeekableByteSource.of(file)));
        pipeline.close();
        return error;
    }

    /**
     * The restart count stored for the test's item.
     *
     * @return the count
     * @throws Exception if a process can't be made
     */
    int storedRestarts() throws Exception
    {
        return process(settings(), new RecordingPublisher(),
                PipelineHooks.NONE).checkpoint(item).orElseThrow()
                .restarts();
    }

    /**
     * An attempt that ends because a service was down (the key service,
     * here) isn't counted, however often it happens: an outage doesn't fail
     * a valid item. Once the service is back, the item is split.
     *
     * @throws Exception if the file can't be processed
     */
    @Test
    void anOutageDoesntCountTowardsFailingAnItem() throws Exception
    {
        byte[] file = csvFile();
        stopsWhileSplitting(file, STOP_IN_FIRST_BATCH);
        RuntimeException outage = new KeyServiceUnavailableException(
                "Vault Transit encrypt unreachable",
                new ConnectException("Connection refused"));
        for (int attempt = 0; attempt < 2 * IngestPipeline.FAIL_AFTER_RESTARTS;
                attempt++)
        {
            assertThat(endsWithAnError(file, endInFirstBatch(outage, false)))
                    .isSameAs(outage);
            assertThat(storedRestarts()).isZero();
        }
        IngestPipeline pipeline = process(settings(),
                new RecordingPublisher(), PipelineHooks.NONE);
        assertThat(split(pipeline, SeekableByteSource.of(file))
                .recordCount()).isEqualTo(CSV_ROWS);
    }

    /**
     * An outage while a batch is being committed takes the restart back
     * and leaves the batch pending, for resume to settle.
     *
     * @throws Exception if the file can't be processed
     */
    @Test
    void anOutageMidCommitLeavesTheBatchForResume() throws Exception
    {
        byte[] file = csvFile();
        stopsWhileSplitting(file, STOP_IN_FIRST_BATCH);
        RuntimeException outage = new UncheckedIOException(
                new ConnectException("Connection refused"));
        for (int attempt = 0; attempt < 2 * IngestPipeline.FAIL_AFTER_RESTARTS;
                attempt++)
        {
            endsWithAnError(file, endInFirstBatch(outage, true));
            assertThat(storedRestarts()).isZero();
        }
        IngestPipeline pipeline = process(settings(),
                new RecordingPublisher(), PipelineHooks.NONE);
        assertThat(pipeline.stored(item).orElseThrow().pending())
                .isNotNull();
        assertThat(split(pipeline, SeekableByteSource.of(file))
                .recordCount()).isEqualTo(CSV_ROWS);
    }

    /**
     * Any other error still counts, so an item that leads to the same
     * error every time is failed in the end.
     *
     * @throws Exception if a process can't be made
     */
    @Test
    void otherErrorsStillCount() throws Exception
    {
        byte[] file = csvFile();
        stopsWhileSplitting(file, STOP_IN_FIRST_BATCH);
        RuntimeException bug = new IllegalStateException("not an outage");
        for (int attempt = 1; attempt < IngestPipeline.FAIL_AFTER_RESTARTS;
                attempt++)
        {
            assertThat(endsWithAnError(file, endInFirstBatch(bug, false)))
                    .isSameAs(bug);
            assertThat(storedRestarts()).isEqualTo(attempt);
        }
        assertThat(endsWithAnError(file, endInFirstBatch(bug, false)))
                .isInstanceOfSatisfying(SplitException.class, error ->
                        assertThat(error.code())
                                .isEqualTo(ErrorCode.RESUME_NO_PROGRESS));
    }

    // ---- guards ----

    /**
     * Each item has a checkpoint of its own: one stopped part-way doesn't
     * hold up another, and resumes later. An item being archived isn't
     * split again.
     *
     * @throws Exception if an item can't be processed
     */
    @Test
    void eachItemHasItsOwnCheckpoint() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        PipelineHooks stopAtCommit = new PipelineHooks()
        {

            /** Stops the worker as soon as a batch commits. */
            @Override
            public void batchCommitted(long lastSeq)
            {
                throw new SimulatedCrash();
            }
        };
        IngestPipeline first = process(settings(), publisher, stopAtCommit);
        assertThatThrownBy(() -> split(first, csv(csvRows())))
                .isInstanceOf(SimulatedCrash.class);

        IngestPipeline second = process(settings(), publisher,
                PipelineHooks.NONE);
        UUID other = UUID.randomUUID();
        assertThat(second.split(csv(List.of("9,other\n")), other,
                new SplitTestSupport.TestLease(1)).recordCount()).isEqualTo(1);
        assertThat(second.checkpoint(item).orElseThrow().state())
                .isEqualTo(Checkpoint.State.SPLITTING);
        assertThat(split(second, csv(csvRows())).recordCount())
                .isEqualTo(CSV_ROWS);
        assertThat(second.checkpoint(other).orElseThrow().state())
                .isEqualTo(Checkpoint.State.ARCHIVING);
    }

    /**
     * A process whose item lease a successor has taken over is stopped
     * before it writes or sends anything: the successor's checkpoint
     * carries the higher epoch. One that finds its lease lost stops too.
     *
     * @throws Exception if a process can't be made
     */
    @Test
    void staleInstanceIsFencedOutBeforeWritingAnything() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline stale = process(settings(), publisher,
                PipelineHooks.NONE);
        IngestPipeline successor = process(settings(), publisher,
                new PipelineHooks()
                {

                    /** Stops the successor once its first batch commits. */
                    @Override
                    public void batchCommitted(long lastSeq)
                    {
                        throw new SimulatedCrash();
                    }
                });
        assertThatThrownBy(() -> split(successor, csv(csvRows())))
                .isInstanceOf(SimulatedCrash.class);
        int sent = publisher.sent.size();
        assertThatThrownBy(() -> split(stale, csv(csvRows())))
                .isInstanceOf(LeaseLostException.class);
        assertThat(publisher.sent).hasSize(sent);

        IngestPipeline lost = process(settings(), publisher,
                PipelineHooks.NONE);
        leases.get(lost).lost = true;
        assertThatThrownBy(() -> split(lost, csv(csvRows())))
                .isInstanceOf(LeaseLostException.class);
        assertThat(publisher.sent).hasSize(sent);
    }

    /**
     * A checkpoint changed by anyone but the stage is detected.
     *
     * @throws Exception if the file can't be processed
     */
    @Test
    void tamperedCheckpointStopsTheService() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(settings(), publisher,
                PipelineHooks.NONE);
        split(pipeline, csv(csvRows().subList(0, 3)));
        try (Connection connection =
                SplitTestSupport.database().getConnection();
                PreparedStatement forge = connection.prepareStatement(
                        "UPDATE " + schema + ".split_item SET checkpoint ="
                                + " replace(checkpoint, '\"ARCHIVING\"',"
                                + " '\"SPLITTING\"')"))
        {
            assertThat(forge.executeUpdate()).isEqualTo(1);
        }
        assertThatThrownBy(() -> pipeline.checkpoint(item))
                .isInstanceOf(CheckpointTamperedException.class);
    }

    // ---- helpers ----

    /** Stands for the connector stopping; thrown from a hook. */
    static final class SimulatedCrash extends RuntimeException
    {

        /** Creates the exception. */
        SimulatedCrash()
        {
        }
    }

    /**
     * Records the messages it is given. A test can make some sends fail or
     * take a while.
     */
    static final class RecordingPublisher implements MessagePublisher
    {

        /** The messages sent, in order. */
        final List<OutboundMessage> sent =
                Collections.synchronizedList(new ArrayList<>());

        /** How many times {@link #publish} has been called. */
        final AtomicInteger calls = new AtomicInteger();

        /** Which calls fail, numbered from 0. */
        volatile IntPredicate fail = call -> false;

        /**
         * Which calls take a while, numbered from 0. A send interrupted
         * midway never reaches the broker.
         */
        volatile IntPredicate slow = call -> false;

        /** Creates a publisher whose sends all succeed at once. */
        RecordingPublisher()
        {
        }

        /**
         * {@inheritDoc}
         *
         * @param message the message
         * @return a future that has already completed; failed if the call
         *         is one that fails, or if it is interrupted while slow
         */
        @Override
        public CompletableFuture<Void> publish(OutboundMessage message)
        {
            int call = calls.getAndIncrement();
            if (fail.test(call))
            {
                return CompletableFuture.failedFuture(
                        new IOException("broker unavailable"));
            }
            if (slow.test(call))
            {
                try
                {
                    Thread.sleep(300);
                }
                catch (InterruptedException interrupted)
                {
                    Thread.currentThread().interrupt();
                    return CompletableFuture.failedFuture(interrupted);
                }
            }
            sent.add(message);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Opens the messages sent, checking that each has the id it was sent
     * with.
     *
     * @param sent the messages as sent
     * @return the messages, opened
     * @throws Exception if a message can't be opened
     */
    List<ConnectorMessage> decode(List<OutboundMessage> sent) throws Exception
    {
        List<ConnectorMessage> decoded = new ArrayList<>();
        synchronized (sent)
        {
            for (OutboundMessage message : sent)
            {
                ConnectorMessage opened = opener.open(message.body());
                assertThat(opened.messageId()).isEqualTo(message.messageId());
                decoded.add(opened);
            }
        }
        return decoded;
    }

    /**
     * Picks out the messages announcing a stored record.
     *
     * @param messages the messages
     * @return those that are {@link RecordSplit}, in order
     */
    static List<RecordSplit> records(List<ConnectorMessage> messages)
    {
        return messages.stream().filter(RecordSplit.class::isInstance)
                .map(RecordSplit.class::cast).toList();
    }

    /**
     * Reads a file's rows from the {@code records} table.
     *
     * @param table  the table
     * @param itemId the file's id
     * @return copies of the rows, every chunk's
     * @throws IOException if the table can't be read
     */
    static List<Record> dataRows(Table table, UUID itemId) throws IOException
    {
        List<Record> found = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(ITEM_ID, itemId)).build())
        {
            rows.forEach(row -> found.add(row.copy()));
        }
        return found;
    }

    /**
     * Reads a record's chunks from the table and decrypts them.
     *
     * @param table  the {@code records} table
     * @param itemId the file's id
     * @param seq    the record's number in the file
     * @return the record's stored plaintext
     * @throws IOException if the table can't be read
     */
    byte[] decrypt(Table table, UUID itemId, long seq) throws IOException
    {
        Map<Integer, Record> chunks = new HashMap<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(Expressions.equal(ITEM_ID, itemId),
                        Expressions.equal(SEQ, seq)))
                .build())
        {
            rows.forEach(row -> chunks.put((Integer) row.getField(CHUNK_INDEX),
                    row.copy()));
        }
        ByteArrayOutputStream plaintext = new ByteArrayOutputStream();
        for (int chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++)
        {
            Record row = chunks.get(chunkIndex);
            byte[] batchDataKey = keys.unwrap(new WrappedKey(
                    (String) row.getField(KEK_REF),
                    bytes(row.getField(BDK_WRAPPED))));
            int chunkCount = (Integer) row.getField(CHUNK_COUNT);
            long plaintextLength = (Long) row.getField(PLAINTEXT_LENGTH);
            plaintext.writeBytes(RecordCrypto.decryptChunk(
                    RecordCrypto.recordKey(batchDataKey, itemId, seq),
                    itemId, seq, chunkIndex, chunkCount,
                    RecordCrypto.expectedChunkLength(plaintextLength,
                            chunkIndex),
                    bytes(row.getField(CIPHERTEXT))));
        }
        return plaintext.toByteArray();
    }

    /**
     * Copies the bytes out of a binary column's value.
     *
     * @param value the value, a {@link ByteBuffer}
     * @return its remaining bytes
     */
    static byte[] bytes(Object value)
    {
        ByteBuffer buffer = ((ByteBuffer) value).duplicate();
        byte[] copy = new byte[buffer.remaining()];
        buffer.get(copy);
        return copy;
    }

    /**
     * Makes an elliptic-curve key pair on P-256.
     *
     * @return a new key pair
     * @throws Exception if the JDK has no such key generator
     */
    static KeyPair ec() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        return generator.generateKeyPair();
    }
}
