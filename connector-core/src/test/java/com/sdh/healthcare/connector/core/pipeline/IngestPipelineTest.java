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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordFailed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.position.RecordOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
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
import java.nio.ByteBuffer;
import java.security.KeyPair;
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

    /** The {@code records} table's identifier. */
    TableIdentifier tableId = TableIdentifier.of("sdh_ingest", DEFAULT_NAME);

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

    /** The last lease epoch given to a process; each new one is higher. */
    long epochs;

    /** The pipelines the test made, closed after it. */
    final List<IngestPipeline> open = new ArrayList<>();

    /** Creates the tests; JUnit runs each test on a new instance. */
    IngestPipelineTest()
    {
    }

    /**
     * Makes a new catalog with an empty {@code records} table, a key
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
        IngestTables.records(catalog, "sdh_ingest");
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
     * Makes settings for testing the pipeline's mechanics, on records stored
     * as split. Conversion has its own tests below.
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
     * Makes settings for CSV with a header, converted to JSON (design §6.4).
     *
     * @return the settings
     */
    PipelineSettings converting()
    {
        return settings()
                .withSplit(SplitConfig.defaults("csv")
                        .withOption("csv.header", "true"))
                .withConversion(ConverterRegistry.load(), 64L << 20);
    }

    // ---- conversion (design §6.4) ----

    /** The header line of the CSV files the conversion tests use. */
    static final String HEADER = "id,name,note\n";

    /**
     * Converts records to JSON, and stores those that fail in their
     * original form. Every record keeps its lineage: its byte range in the
     * file, and the lines and columns of its start and of any error.
     *
     * @throws Exception if the file can't be processed or read back
     */
    @Test
    void convertsToJsonAndStoresFailedRecordsInTheirOriginalFormWithLineage()
            throws Exception
    {
        String good = "1,Ann,\"two\nlines\"\n";
        String shortRow = "2,Bob\n";
        byte[] latin1 = "3,Jos\u00e9,x\n".getBytes(ISO_8859_1);
        String last = "4,Cy,\n";
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.writeBytes(HEADER.getBytes(UTF_8));
        file.writeBytes(good.getBytes(UTF_8));
        file.writeBytes(shortRow.getBytes(UTF_8));
        file.writeBytes(latin1);
        file.writeBytes(last.getBytes(UTF_8));
        byte[] bytes = file.toByteArray();

        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(converting(), publisher,
                PipelineHooks.NONE);
        IngestPipeline.SplitResult result =
                pipeline.split(SeekableByteSource.of(bytes));
        assertThat(result.recordCount()).isEqualTo(4);

        List<ConnectorMessage> messages = decode(publisher.sent);
        List<RecordInStorage> inStorage = messages.stream()
                .filter(RecordInStorage.class::isInstance)
                .map(RecordInStorage.class::cast).toList();
        assertThat(inStorage)
                .extracting(message -> message.getClass().getSimpleName())
                .containsExactly("RecordStored", "RecordFailed",
                        "RecordFailed", "RecordStored");
        RecordStored first = (RecordStored) inStorage.get(0);
        assertThat(first.contentType()).isEqualTo("application/json");
        assertThat(first.mapping()).isEqualTo("csv-row/1");
        RecordFailed mismatch = (RecordFailed) inStorage.get(1);
        assertThat(mismatch.errorCode()).isEqualTo("CONVERT_CSV_FIELD_COUNT");
        assertThat(mismatch.contentType()).isEqualTo("text/csv");
        assertThat(((RecordFailed) inStorage.get(2)).errorCode())
                .isEqualTo("CONVERT_INVALID_ENCODING");

        // Lineage: offsets point into the original file, whether converted
        // or not.
        int goodAt = HEADER.length();
        int shortAt = goodAt + good.length();
        int latinAt = shortAt + shortRow.length();
        assertThat(inStorage).extracting(RecordInStorage::sourceOffset)
                .containsExactly((long) goodAt, (long) shortAt,
                        (long) latinAt, (long) latinAt + latin1.length);
        assertThat(inStorage).extracting(RecordInStorage::sourceLength)
                .containsExactly((long) good.length(),
                        (long) shortRow.length(), (long) latin1.length,
                        (long) last.length());

        // Stored bytes: JSON for converted records, and the record as split
        // (header included) for failed ones. The record ID is the keyed hash
        // of what is stored.
        Table table = catalog.loadTable(tableId);
        RecordIds ids =
                new RecordIds(IdKeys.loadOrCreate(table, null, keys, KEK));
        byte[] json = "[\"1\",\"Ann\",\"two\\nlines\"]".getBytes(UTF_8);
        assertThat(decrypt(table, result.itemId(), 0)).isEqualTo(json);
        assertThat(first.recordId()).isEqualTo(ids.of(json));
        byte[] raw = (HEADER + shortRow).getBytes(UTF_8);
        assertThat(decrypt(table, result.itemId(), 1)).isEqualTo(raw);
        assertThat(mismatch.recordId()).isEqualTo(ids.of(raw));
        assertThat(decrypt(table, result.itemId(), 3))
                .isEqualTo("[\"4\",\"Cy\",\"\"]".getBytes(UTF_8));
        assertThat(dataRows(table, result.itemId()))
                .extracting(row -> row.getField(ERROR_CODE))
                .containsExactly(null, "CONVERT_CSV_FIELD_COUNT",
                        "CONVERT_INVALID_ENCODING", null);
        assertThat(pipeline.failedRecordCount(result.itemId())).isEqualTo(2);

        // Lines and columns: each record starts after the header the
        // splitter added (record line 2), at its row's line in the file. The
        // quoted line break in the first row moves the later ones down a
        // line.
        assertThat(inStorage)
                .extracting(message -> message.origin().inRecord())
                .containsOnly(new SourcePosition(2, 1));
        assertThat(inStorage)
                .extracting(message -> message.origin().inItem().line())
                .containsExactly(2L, 4L, 5L, 6L);
        // The invalid byte is "é" in ISO 8859-1, the sixth character of
        // line 5.
        assertThat(((RecordFailed) inStorage.get(2)).errorAt())
                .isEqualTo(new SourcePosition(5, 6));
        assertThat(mismatch.errorAt().line()).isEqualTo(4L);
        assertThat(dataRows(table, result.itemId()))
                .extracting(row -> row.getField(SOURCE_LINE))
                .containsExactly(2L, 4L, 5L, 6L);
        assertThat(dataRows(table, result.itemId()))
                .extracting(row -> row.getField(ERROR_LINE))
                .containsExactly(null, 4L, 5L, null);

        // The failed records, as the producer's result lists them.
        assertThat(pipeline.failedRecords(result.itemId())).containsExactly(
                new CommittedRecords.Failed(1, mismatch.sourceOffset(),
                        "CONVERT_CSV_FIELD_COUNT", mismatch.errorAt()),
                new CommittedRecords.Failed(2, latinAt,
                        "CONVERT_INVALID_ENCODING",
                        new SourcePosition(5, 6)));
    }

    /**
     * A C-CDA element's position, from the converter, moved by its record's
     * origin, is where the element is in the file. Here the file has a
     * stylesheet line and a blank line before the document, and the record
     * has the XML declaration the splitter adds instead.
     *
     * @throws Exception if the file can't be processed or read back
     */
    @Test
    void cCdaElementPositionsLeadToTheirPlaceInTheFile() throws Exception
    {
        String file = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<?xml-stylesheet type=\"text/xsl\" href=\"cda.xsl\"?>\n"
                + "\n"
                + "  <ClinicalDocument xmlns=\"urn:hl7-org:v3\">\n"
                + "    <title>\u00e9t\u00e9</title> <code code=\"34133-9\"/>\n"
                + "  </ClinicalDocument>\n";
        PipelineSettings ccda = PipelineSettings
                .defaults("file-connector", "ccda", "prod", KEK)
                .withCommit(BATCH, 1L << 30, Duration.ofHours(1))
                .withWorkers(2)
                .withRetry(new RetryPolicy(3, Duration.ofMillis(1),
                        Duration.ofMillis(5)))
                .withConversion(ConverterRegistry.load(), 64L << 20);
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline.SplitResult result =
                process(ccda, publisher, PipelineHooks.NONE)
                        .split(SeekableByteSource.of(file.getBytes(UTF_8)));

        RecordStored stored =
                (RecordStored) records(decode(publisher.sent)).getFirst();
        assertThat(stored.mapping()).isEqualTo("ccda/2");
        assertThat(stored.origin()).isEqualTo(new RecordOrigin(
                new SourcePosition(2, 1), new SourcePosition(4, 3)));
        JsonNode document = new ObjectMapper()
                .readTree(decrypt(catalog.loadTable(tableId), result.itemId(),
                        0))
                .get("ClinicalDocument");
        assertThat(stored.origin().toFile(position(document)))
                .isEqualTo(new SourcePosition(4, 3));
        // "    <title>été</title> " is 23 characters, so <code is in column
        // 24 of line 5.
        assertThat(stored.origin().toFile(position(document.get("code"))))
                .isEqualTo(new SourcePosition(5, 24));
    }

    /**
     * Reads a C-CDA element's position from its {@code #line} and
     * {@code #col} keys.
     *
     * @param element the element's JSON object
     * @return the position, within its record
     */
    private static SourcePosition position(JsonNode element)
    {
        return new SourcePosition(element.get("#line").asLong(),
                element.get("#col").asInt());
    }

    /**
     * After a restart, the message for a failed record committed before it
     * is rebuilt from the table, with its lines and columns.
     *
     * @throws Exception if the file can't be processed or a message opened
     */
    @Test
    void failedRecordMessagesAreRebuiltAfterARestart() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        PipelineHooks stopAtCommit = new PipelineHooks()
        {

            /** Stops the connector as soon as a batch commits. */
            @Override
            public void batchCommitted(long lastSeq)
            {
                throw new SimulatedCrash();
            }
        };
        IngestPipeline first = process(converting(), publisher, stopAtCommit);
        byte[] file = (HEADER + "1,a,b\n2,b\n").getBytes(UTF_8);
        assertThatThrownBy(() -> first.split(SeekableByteSource.of(file)))
                .isInstanceOf(SimulatedCrash.class);
        assertThat(records(decode(publisher.sent))).isEmpty();

        RecordingPublisher again = new RecordingPublisher();
        process(converting(), again, PipelineHooks.NONE)
                .split(SeekableByteSource.of(file));
        List<ConnectorMessage> messages = decode(again.sent);
        assertThat(messages.stream().filter(message ->
                message instanceof RecordFailed failed
                        && failed.errorCode().equals("CONVERT_CSV_FIELD_COUNT")
                        && failed.seq() == 1))
                .singleElement()
                .satisfies(message ->
                {
                    // Rebuilt from the table, with its lines and columns.
                    RecordFailed failed = (RecordFailed) message;
                    assertThat(failed.origin()).isEqualTo(new RecordOrigin(
                            new SourcePosition(2, 1),
                            new SourcePosition(3, 1)));
                    assertThat(failed.errorAt().line()).isEqualTo(3L);
                });
        assertThat(records(messages)).singleElement()
                .satisfies(stored ->
                        assertThat(stored.mapping()).isEqualTo("csv-row/1"));
    }

    /**
     * JSON longer than a chunk is stored in more than one.
     *
     * @throws Exception if the file can't be processed or read back
     */
    @Test
    void largeJsonIsStoredInChunks() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        String big = "1,\"" + "x".repeat(RecordCrypto.CHUNK_SIZE + 500)
                + "\",z\n";
        IngestPipeline pipeline = process(converting(), publisher,
                PipelineHooks.NONE);
        UUID itemId = pipeline.split(SeekableByteSource.of(
                (HEADER + "2,short,ok\n" + big).getBytes(UTF_8))).itemId();
        List<RecordStored> stored = records(decode(publisher.sent));
        assertThat(stored).extracting(RecordStored::chunkCount)
                .containsExactly(1, 2);
        assertThat(decrypt(catalog.loadTable(tableId), itemId, 1)).isEqualTo(
                ("[\"1\",\"" + "x".repeat(RecordCrypto.CHUNK_SIZE + 500)
                        + "\",\"z\"]").getBytes(UTF_8));
    }

    /**
     * A record larger than the conversion limit is stored as split, and
     * announced as failed with {@code CONVERT_TOO_LARGE}.
     *
     * @throws Exception if the file can't be processed or a message opened
     */
    @Test
    void recordsOverTheConversionLimitAreStoredRaw() throws Exception
    {
        RecordingPublisher limited = new RecordingPublisher();
        IngestPipeline pipeline = process(
                converting().withConversion(ConverterRegistry.load(), 30),
                limited, PipelineHooks.NONE);
        pipeline.split(SeekableByteSource.of(
                (HEADER + "3,a,b\n4,a much longer record,c\n")
                        .getBytes(UTF_8)));
        List<ConnectorMessage> messages = decode(limited.sent);
        assertThat(messages.stream().filter(RecordFailed.class::isInstance)
                .map(message -> ((RecordFailed) message).errorCode()))
                .containsExactly("CONVERT_TOO_LARGE");
    }

    /**
     * Records whose conversion would need more than the conversion budget
     * run one at a time (design §6.4), so several large records can't
     * exhaust the heap together. The file still completes, with every
     * record converted.
     *
     * @throws Exception if the file can't be processed
     */
    @Test
    void conversionsThatDontFitTheBudgetTogetherRunOneAtATime()
            throws Exception
    {
        RecordConverter csv =
                ConverterRegistry.load().get("csv").orElseThrow();
        AtomicInteger running = new AtomicInteger();
        AtomicInteger most = new AtomicInteger();
        RecordConverter greedy = new RecordConverter()
        {

            /** Converts CSV, as the real converter does. */
            @Override
            public String format()
            {
                return "csv";
            }

            /** Writes the real converter's mapping. */
            @Override
            public String mapping()
            {
                return csv.mapping();
            }

            /** Claims the whole conversion budget for each record. */
            @Override
            public double memoryFactor()
            {
                return 1e6;
            }

            /**
             * Converts with the real converter, counting the conversions
             * that run at once.
             */
            @Override
            public byte[] convert(byte[] record, SplitConfig config)
                    throws ConversionException
            {
                most.accumulateAndGet(running.incrementAndGet(), Math::max);
                try
                {
                    Thread.sleep(2);
                }
                catch (InterruptedException interrupted)
                {
                    Thread.currentThread().interrupt();
                }
                finally
                {
                    running.decrementAndGet();
                }
                return csv.convert(record, config);
            }
        };
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(converting()
                .withConversion(ConverterRegistry.of(greedy), 64L << 20)
                .withConvertMemory(64L << 20), publisher, PipelineHooks.NONE);
        IngestPipeline.SplitResult result =
                pipeline.split(SeekableByteSource.of(convertibleFile()));
        assertThat(result.recordCount()).isEqualTo(CSV_ROWS);
        assertThat(pipeline.failedRecordCount(result.itemId())).isZero();
        assertThat(most.get()).isEqualTo(1);
    }

    /**
     * A converter that fails unexpectedly (a bug, not a problem it
     * recognises) fails only that record: it is stored as split, announced
     * as failed with {@code CONVERT_FAILED}, and the rest of the file is
     * converted.
     *
     * @throws Exception if the file can't be processed or read back
     */
    @Test
    void aConverterBugFailsTheRecordNotTheFile() throws Exception
    {
        RecordConverter csv =
                ConverterRegistry.load().get("csv").orElseThrow();
        RecordConverter buggy = new RecordConverter()
        {

            /** Converts CSV, as the real converter does. */
            @Override
            public String format()
            {
                return "csv";
            }

            /** Writes the real converter's mapping. */
            @Override
            public String mapping()
            {
                return csv.mapping();
            }

            /**
             * Converts with the real converter, but throws an unexpected
             * exception on a record that has {@code boom} in it.
             */
            @Override
            public byte[] convert(byte[] record, SplitConfig config)
                    throws ConversionException
            {
                if (new String(record, UTF_8).contains("boom"))
                {
                    throw new IndexOutOfBoundsException(
                            "would quote the record: boom");
                }
                return csv.convert(record, config);
            }
        };
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(
                converting().withConversion(ConverterRegistry.of(buggy),
                        64L << 20),
                publisher, PipelineHooks.NONE);
        String bad = "2,boom,x\n";
        IngestPipeline.SplitResult result = pipeline.split(SeekableByteSource
                .of((HEADER + "1,a,b\n" + bad + "3,c,d\n").getBytes(UTF_8)));
        assertThat(result.recordCount()).isEqualTo(3);
        Table table = catalog.loadTable(tableId);
        assertThat(dataRows(table, result.itemId()))
                .extracting(row -> row.getField(ERROR_CODE))
                .containsExactly(null, "CONVERT_FAILED", null);
        assertThat(decrypt(table, result.itemId(), 1))
                .isEqualTo((HEADER + bad).getBytes(UTF_8));
        List<ConnectorMessage> messages = decode(publisher.sent);
        assertThat(messages.stream().filter(RecordFailed.class::isInstance)
                .map(message -> ((RecordFailed) message).errorCode()))
                .containsExactly("CONVERT_FAILED");
        assertThat(records(messages)).hasSize(2);
    }

    /**
     * Starts a fresh "process": its own table handle, a higher lease epoch,
     * and the ID key loaded from the table.
     *
     * @param settings  the pipeline's settings
     * @param publisher where its messages go
     * @param hooks     where it can be made to stop
     * @return the pipeline, closed after the test
     */
    IngestPipeline process(PipelineSettings settings,
            MessagePublisher publisher, PipelineHooks hooks)
    {
        long epoch = ++epochs;
        Table table = catalog.loadTable(tableId);
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
        {

            /** Holds the epoch this process was given. */
            public long epoch()
            {
                return epoch;
            }

            /** Never finds the lease lost. */
            public void checkHeld()
            {
            }
        });
        committer.establish(table);
        byte[] idKey = IdKeys.loadOrCreate(table, committer, keys, KEK);
        IngestPipeline pipeline = new IngestPipeline(settings, table,
                committer, keys, new RecordIds(idKey),
                SplitterRegistry.load(), sealer, publisher, Clock.systemUTC());
        pipeline.setHooks(hooks);
        open.add(pipeline);
        return pipeline;
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
        IngestPipeline.SplitResult result = pipeline.split(csv(csvRows()));

        assertThat(result.recordCount()).isEqualTo(CSV_ROWS);
        Checkpoint checkpoint = pipeline.checkpoint().orElseThrow();
        assertThat(checkpoint.state()).isEqualTo(Checkpoint.State.ARCHIVING);
        assertThat(checkpoint.publishedSeq()).isEqualTo(CSV_ROWS - 1);

        List<ConnectorMessage> messages = decode(publisher.sent);
        assertThat(messages.getFirst()).isInstanceOf(FileStarted.class);
        List<RecordStored> records = records(messages);
        assertThat(records).extracting(RecordStored::seq)
                .containsExactlyElementsOf(
                        LongStream.range(0, CSV_ROWS).boxed().toList());
        assertThat(records).allSatisfy(record ->
        {
            assertThat(record.itemId()).isEqualTo(result.itemId());
            assertThat(record.storage().catalog()).isEqualTo("prod");
            assertThat(record.storage().namespace()).isEqualTo("sdh_ingest");
            assertThat(record.storage().table()).isEqualTo(DEFAULT_NAME);
        });
        // Three batches → three data snapshots, each referenced by its
        // messages.
        assertThat(records.stream()
                .map(record -> record.storage().snapshotId()).distinct())
                .hasSize(3);

        Table table = catalog.loadTable(tableId);
        RecordIds ids =
                new RecordIds(IdKeys.loadOrCreate(table, null, keys, KEK));
        List<String> expected = csvRows();
        for (RecordStored record : List.of(records.get(0),
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
        UUID itemId = pipeline.split(csv(List.of(small, big, small))).itemId();

        List<RecordStored> stored = records(decode(publisher.sent));
        assertThat(stored).extracting(RecordStored::chunkCount)
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
        pipeline.split(csv(csvRows().subList(0, 5)));
        assertThat(records(decode(publisher.sent)).stream()
                .map(record -> record.storage().snapshotId()).distinct())
                .hasSize(5);
    }

    /**
     * An empty file is announced and goes straight to archiving.
     *
     * @throws Exception if the file can't be processed or a message opened
     */
    @Test
    void emptyFileGoesStraightToArchiving() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(settings(), publisher,
                PipelineHooks.NONE);
        IngestPipeline.SplitResult empty =
                pipeline.split(SeekableByteSource.of(new byte[0]));
        assertThat(empty.recordCount()).isZero();
        assertThat(empty.checkpoint().state())
                .isEqualTo(Checkpoint.State.ARCHIVING);
        assertThat(decode(publisher.sent)).singleElement()
                .isInstanceOf(FileStarted.class);
        // Splitting the same file again does nothing: it is already waiting
        // for archiving.
        assertThat(pipeline.split(SeekableByteSource.of(new byte[0])).itemId())
                .isEqualTo(empty.itemId());
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
        pipeline.split(csv(csvRows()));
        assertThat(records(decode(publisher.sent)).stream()
                .map(RecordStored::seq).distinct()).hasSize(CSV_ROWS);
    }

    // ---- crash and resume (design §15) ----

    /** Where a test of resuming makes the connector stop. */
    enum Crash
    {

        /** After the first batch's data file is written, before it commits. */
        AFTER_WRITE_FIRST_BATCH,

        /** After the second batch's data file is written, before it commits. */
        AFTER_WRITE_SECOND_BATCH,

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
     * After the connector stops anywhere in a file, a new process resumes the
     * same file. Every record is stored exactly once and announced at least
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
        assertThatThrownBy(() -> first.split(csv(fileRows)))
                .isInstanceOfAny(SimulatedCrash.class,
                        PublishFailedException.class);
        first.close();
        UUID firstItemId = first.checkpoint().orElseThrow().itemId();
        assertThat(first.checkpoint().orElseThrow().state())
                .isEqualTo(Checkpoint.State.SPLITTING);

        publisher.fail = call -> false;
        IngestPipeline second = process(settings(), publisher,
                PipelineHooks.NONE);
        IngestPipeline.SplitResult result = second.split(csv(fileRows));
        assertThat(result.itemId()).isEqualTo(firstItemId);
        assertThat(result.recordCount()).isEqualTo(CSV_ROWS);

        List<RecordStored> sent = records(decode(publisher.sent));
        Map<Long, List<RecordStored>> bySeq = sent.stream()
                .collect(Collectors.groupingBy(RecordStored::seq));
        assertThat(bySeq.keySet()).hasSize(CSV_ROWS)
                .allMatch(seq -> seq >= 0 && seq < CSV_ROWS);
        bySeq.values().forEach(copies -> assertThat(copies.stream()
                .map(RecordStored::recordId).distinct()).hasSize(1));
        assertThat(sent.size() - CSV_ROWS).as("duplicates")
                .isLessThanOrEqualTo(BATCH);

        Table table = catalog.loadTable(tableId);
        List<Record> rows = dataRows(table, firstItemId);
        assertThat(rows).hasSize(CSV_ROWS);
        assertThat(rows.stream().map(row -> row.getField(SEQ)).distinct())
                .hasSize(CSV_ROWS);
        RecordIds ids =
                new RecordIds(IdKeys.loadOrCreate(table, null, keys, KEK));
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
     * Makes a CSV file with a header and {@link #CSV_ROWS} records, all of
     * which convert.
     *
     * @return the file's bytes
     */
    static byte[] convertibleFile()
    {
        return (HEADER + String.join("", csvRows())).getBytes(UTF_8);
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
        IngestPipeline pipeline = process(converting(),
                new RecordingPublisher(), hooks);
        assertThatThrownBy(() -> pipeline.split(SeekableByteSource.of(file)))
                .isInstanceOf(SimulatedCrash.class);
        pipeline.close();
    }

    /**
     * After the connector stops twice in a row in the same batch, that batch
     * is stored without converting any of it, in case a conversion is what
     * stops it, and announced as failed. The batches after it are converted
     * again.
     *
     * @throws Exception if the file can't be processed or read back
     */
    @Test
    void aBatchTheConnectorKeepsStoppingInIsStoredUnconverted() throws Exception
    {
        byte[] file = convertibleFile();
        // The first attempt.
        stopsWhileSplitting(file, STOP_IN_FIRST_BATCH);
        // Restart 1: still converting.
        stopsWhileSplitting(file, STOP_IN_FIRST_BATCH);
        assertThat(process(converting(), new RecordingPublisher(),
                PipelineHooks.NONE).checkpoint().orElseThrow().restarts())
                .isEqualTo(1);

        RecordingPublisher publisher = new RecordingPublisher();
        // Restart 2: the batch is stored unconverted.
        IngestPipeline pipeline = process(converting(), publisher,
                PipelineHooks.NONE);
        IngestPipeline.SplitResult result =
                pipeline.split(SeekableByteSource.of(file));
        assertThat(result.recordCount()).isEqualTo(CSV_ROWS);
        assertThat(result.checkpoint().restarts()).isZero();

        List<Record> rows = dataRows(catalog.loadTable(tableId),
                result.itemId());
        assertThat(rows).hasSize(CSV_ROWS)
                .allSatisfy(row -> assertThat(row.getField(ERROR_CODE))
                        .isEqualTo((Long) row.getField(SEQ) < BATCH
                                ? "CONVERT_SKIPPED" : null));
        List<ConnectorMessage> messages = decode(publisher.sent);
        assertThat(messages.stream().filter(RecordFailed.class::isInstance))
                .hasSize(BATCH);
        assertThat(records(messages)).hasSize(CSV_ROWS - BATCH)
                .allSatisfy(stored ->
                        assertThat(stored.mapping()).isEqualTo("csv-row/1"));
        assertThat(pipeline.failedRecordCount(result.itemId()))
                .isEqualTo(BATCH);
    }

    /**
     * If the connector still stops with conversion off, the next start fails
     * the file instead of resuming it, so one file can't keep the connector
     * from ever moving on.
     *
     * @throws Exception if a process can't be made
     */
    @Test
    void aFileThatKeepsStoppingTheConnectorFails() throws Exception
    {
        byte[] file = convertibleFile();
        for (int attempt = 0; attempt < IngestPipeline.FAIL_AFTER_RESTARTS;
                attempt++)
        {
            stopsWhileSplitting(file, STOP_IN_FIRST_BATCH);
        }
        IngestPipeline pipeline = process(converting(),
                new RecordingPublisher(), PipelineHooks.NONE);
        assertThatThrownBy(() -> pipeline.split(SeekableByteSource.of(file)))
                .isInstanceOfSatisfying(SplitException.class, error ->
                {
                    assertThat(error.code())
                            .isEqualTo(ErrorCode.RESUME_NO_PROGRESS);
                    assertThat(error.offset()).isZero();
                });
        assertThat(pipeline.checkpoint().orElseThrow().state())
                .isEqualTo(Checkpoint.State.SPLITTING);
    }

    /**
     * A stop after a batch has committed is progress: it never counts
     * towards skipping conversion or failing the file.
     *
     * @throws Exception if the file can't be processed
     */
    @Test
    void restartsAfterProgressDontCount() throws Exception
    {
        byte[] file = convertibleFile();
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
            assertThat(process(converting(), new RecordingPublisher(),
                    PipelineHooks.NONE).checkpoint().orElseThrow().restarts())
                    .isZero();
        }
        IngestPipeline pipeline = process(converting(),
                new RecordingPublisher(), PipelineHooks.NONE);
        IngestPipeline.SplitResult result =
                pipeline.split(SeekableByteSource.of(file));
        assertThat(result.recordCount()).isEqualTo(CSV_ROWS);
        assertThat(pipeline.failedRecordCount(result.itemId())).isZero();
    }

    // ---- guards ----

    /**
     * While a file is in progress, another file is refused until the first
     * is abandoned. A file still being archived is protected too.
     *
     * @throws Exception if a file can't be processed
     */
    @Test
    void anotherFileIsRefusedUntilTheInProgressOneIsAbandoned()
            throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        PipelineHooks stopAtCommit = new PipelineHooks()
        {

            /** Stops the connector as soon as a batch commits. */
            @Override
            public void batchCommitted(long lastSeq)
            {
                throw new SimulatedCrash();
            }
        };
        IngestPipeline first = process(settings(), publisher, stopAtCommit);
        assertThatThrownBy(() -> first.split(csv(csvRows())))
                .isInstanceOf(SimulatedCrash.class);

        IngestPipeline second = process(settings(), publisher,
                PipelineHooks.NONE);
        SeekableByteSource other = csv(List.of("9,other\n"));
        assertThatThrownBy(() -> second.split(other))
                .isInstanceOf(IllegalStateException.class);
        second.abandonInProgress();
        assertThat(second.checkpoint().orElseThrow().state())
                .isEqualTo(Checkpoint.State.ABANDONED);
        assertThat(second.split(other).recordCount()).isEqualTo(1);
        // A file still being archived is protected too.
        assertThatThrownBy(() -> second.split(csv(csvRows())))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * A process whose lease a successor has taken is stopped before it
     * writes or sends anything.
     *
     * @throws Exception if a process can't be made
     */
    @Test
    void staleInstanceIsFencedOutBeforeWritingAnything() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline stale = process(settings(), publisher,
                PipelineHooks.NONE);
        // Its successor establishes a higher epoch.
        process(settings(), publisher, PipelineHooks.NONE);
        assertThatThrownBy(() -> stale.split(csv(csvRows())))
                .isInstanceOf(LeaseLostException.class);
        assertThat(publisher.sent).isEmpty();
    }

    /**
     * A checkpoint changed by anyone but the connector is detected, which
     * stops the service.
     *
     * @throws Exception if the file can't be processed
     */
    @Test
    void tamperedCheckpointStopsTheService() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(settings(), publisher,
                PipelineHooks.NONE);
        pipeline.split(csv(csvRows().subList(0, 3)));
        Table table = catalog.loadTable(tableId);
        String key = Checkpoint.propertyKey("file-connector");
        String forged = table.properties().get(key).replace(
                "\"state\":\"ARCHIVING\"", "\"state\":\"SPLITTING\"");
        table.updateProperties().set(key, forged).commit();
        assertThatThrownBy(pipeline::checkpoint)
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
     * @return those that are {@link RecordStored}, in order
     */
    static List<RecordStored> records(List<ConnectorMessage> messages)
    {
        return messages.stream().filter(RecordStored.class::isInstance)
                .map(RecordStored.class::cast).toList();
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
