/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import static com.sdh.healthcare.connector.contract.table.SectionsTable.*;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionFailed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.position.SectionOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.convert.ConversionException;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.convert.SectionConverter;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
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

    /** The key encryption key that wraps the sections' data keys. */
    static final String KEK = "sections-kek/1";

    /** How many records {@link #records()} makes. */
    static final int RECORDS = 250;

    /** How many sections a batch commits at most. */
    static final int BATCH = 100;

    /** The in-memory catalog, new for each test. */
    InMemoryCatalog catalog;

    /** The {@code sections} table's identifier. */
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
     * Makes a new catalog with an empty {@code sections} table, a key
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
        IngestTables.sections(catalog, "sdh_ingest");
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
     * Makes settings for testing the pipeline's mechanics, on sections stored
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
     * Converts sections to JSON, and stores those that fail in their
     * original form. Every section keeps its lineage: its byte range in the
     * file, and the lines and columns of its start and of any error.
     *
     * @throws Exception if the file can't be processed or read back
     */
    @Test
    void convertsToJsonAndStoresFailedSectionsInTheirOriginalFormWithLineage()
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
        assertThat(result.sectionCount()).isEqualTo(4);

        List<ConnectorMessage> messages = decode(publisher.sent);
        List<SectionInStorage> inStorage = messages.stream()
                .filter(SectionInStorage.class::isInstance)
                .map(SectionInStorage.class::cast).toList();
        assertThat(inStorage)
                .extracting(message -> message.getClass().getSimpleName())
                .containsExactly("SectionStored", "SectionFailed",
                        "SectionFailed", "SectionStored");
        SectionStored first = (SectionStored) inStorage.get(0);
        assertThat(first.contentType()).isEqualTo("application/json");
        assertThat(first.mapping()).isEqualTo("csv-row/1");
        SectionFailed mismatch = (SectionFailed) inStorage.get(1);
        assertThat(mismatch.errorCode()).isEqualTo("CONVERT_CSV_FIELD_COUNT");
        assertThat(mismatch.contentType()).isEqualTo("text/csv");
        assertThat(((SectionFailed) inStorage.get(2)).errorCode())
                .isEqualTo("CONVERT_INVALID_ENCODING");

        // Lineage: offsets point into the original file, whether converted
        // or not.
        int goodAt = HEADER.length();
        int shortAt = goodAt + good.length();
        int latinAt = shortAt + shortRow.length();
        assertThat(inStorage).extracting(SectionInStorage::sourceOffset)
                .containsExactly((long) goodAt, (long) shortAt,
                        (long) latinAt, (long) latinAt + latin1.length);
        assertThat(inStorage).extracting(SectionInStorage::sourceLength)
                .containsExactly((long) good.length(),
                        (long) shortRow.length(), (long) latin1.length,
                        (long) last.length());

        // Stored bytes: JSON for converted sections, and the section as split
        // (header included) for failed ones. The section ID is the keyed hash
        // of what is stored.
        Table table = catalog.loadTable(tableId);
        SectionIds ids =
                new SectionIds(IdKeys.loadOrCreate(table, null, keys, KEK));
        byte[] json = "[\"1\",\"Ann\",\"two\\nlines\"]".getBytes(UTF_8);
        assertThat(decrypt(table, result.fileId(), 0)).isEqualTo(json);
        assertThat(first.sectionId()).isEqualTo(ids.of(json));
        byte[] raw = (HEADER + shortRow).getBytes(UTF_8);
        assertThat(decrypt(table, result.fileId(), 1)).isEqualTo(raw);
        assertThat(mismatch.sectionId()).isEqualTo(ids.of(raw));
        assertThat(decrypt(table, result.fileId(), 3))
                .isEqualTo("[\"4\",\"Cy\",\"\"]".getBytes(UTF_8));
        assertThat(dataRows(table, result.fileId()))
                .extracting(row -> row.getField(ERROR_CODE))
                .containsExactly(null, "CONVERT_CSV_FIELD_COUNT",
                        "CONVERT_INVALID_ENCODING", null);
        assertThat(pipeline.failedSectionCount(result.fileId())).isEqualTo(2);

        // Lines and columns: each section starts after the header the
        // splitter added (section line 2), at its row's line in the file. The
        // quoted line break in the first row moves the later ones down a
        // line.
        assertThat(inStorage)
                .extracting(message -> message.origin().inSection())
                .containsOnly(new SourcePosition(2, 1));
        assertThat(inStorage)
                .extracting(message -> message.origin().inFile().line())
                .containsExactly(2L, 4L, 5L, 6L);
        // The invalid byte is "é" in ISO 8859-1, the sixth character of
        // line 5.
        assertThat(((SectionFailed) inStorage.get(2)).errorAt())
                .isEqualTo(new SourcePosition(5, 6));
        assertThat(mismatch.errorAt().line()).isEqualTo(4L);
        assertThat(dataRows(table, result.fileId()))
                .extracting(row -> row.getField(SOURCE_LINE))
                .containsExactly(2L, 4L, 5L, 6L);
        assertThat(dataRows(table, result.fileId()))
                .extracting(row -> row.getField(ERROR_LINE))
                .containsExactly(null, 4L, 5L, null);
    }

    /**
     * A C-CDA element's position, from the converter, moved by its section's
     * origin, is where the element is in the file. Here the file has a
     * stylesheet line and a blank line before the document, and the section
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

        SectionStored stored =
                (SectionStored) sections(decode(publisher.sent)).getFirst();
        assertThat(stored.mapping()).isEqualTo("ccda/2");
        assertThat(stored.origin()).isEqualTo(new SectionOrigin(
                new SourcePosition(2, 1), new SourcePosition(4, 3)));
        JsonNode document = new ObjectMapper()
                .readTree(decrypt(catalog.loadTable(tableId), result.fileId(),
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
     * @return the position, within its section
     */
    private static SourcePosition position(JsonNode element)
    {
        return new SourcePosition(element.get("#line").asLong(),
                element.get("#col").asInt());
    }

    /**
     * After a restart, the message for a failed section committed before it
     * is rebuilt from the table, with its lines and columns.
     *
     * @throws Exception if the file can't be processed or a message opened
     */
    @Test
    void failedSectionMessagesAreRebuiltAfterARestart() throws Exception
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
        assertThat(sections(decode(publisher.sent))).isEmpty();

        RecordingPublisher again = new RecordingPublisher();
        process(converting(), again, PipelineHooks.NONE)
                .split(SeekableByteSource.of(file));
        List<ConnectorMessage> messages = decode(again.sent);
        assertThat(messages.stream().filter(message ->
                message instanceof SectionFailed failed
                        && failed.errorCode().equals("CONVERT_CSV_FIELD_COUNT")
                        && failed.seq() == 1))
                .singleElement()
                .satisfies(message ->
                {
                    // Rebuilt from the table, with its lines and columns.
                    SectionFailed failed = (SectionFailed) message;
                    assertThat(failed.origin()).isEqualTo(new SectionOrigin(
                            new SourcePosition(2, 1),
                            new SourcePosition(3, 1)));
                    assertThat(failed.errorAt().line()).isEqualTo(3L);
                });
        assertThat(sections(messages)).singleElement()
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
        String big = "1,\"" + "x".repeat(SectionCrypto.CHUNK_SIZE + 500)
                + "\",z\n";
        IngestPipeline pipeline = process(converting(), publisher,
                PipelineHooks.NONE);
        UUID fileId = pipeline.split(SeekableByteSource.of(
                (HEADER + "2,short,ok\n" + big).getBytes(UTF_8))).fileId();
        List<SectionStored> stored = sections(decode(publisher.sent));
        assertThat(stored).extracting(SectionStored::chunkCount)
                .containsExactly(1, 2);
        assertThat(decrypt(catalog.loadTable(tableId), fileId, 1)).isEqualTo(
                ("[\"1\",\"" + "x".repeat(SectionCrypto.CHUNK_SIZE + 500)
                        + "\",\"z\"]").getBytes(UTF_8));
    }

    /**
     * A section larger than the conversion limit is stored as split, and
     * announced as failed with {@code CONVERT_TOO_LARGE}.
     *
     * @throws Exception if the file can't be processed or a message opened
     */
    @Test
    void sectionsOverTheConversionLimitAreStoredRaw() throws Exception
    {
        RecordingPublisher limited = new RecordingPublisher();
        IngestPipeline pipeline = process(
                converting().withConversion(ConverterRegistry.load(), 30),
                limited, PipelineHooks.NONE);
        pipeline.split(SeekableByteSource.of(
                (HEADER + "3,a,b\n4,a much longer record,c\n")
                        .getBytes(UTF_8)));
        List<ConnectorMessage> messages = decode(limited.sent);
        assertThat(messages.stream().filter(SectionFailed.class::isInstance)
                .map(message -> ((SectionFailed) message).errorCode()))
                .containsExactly("CONVERT_TOO_LARGE");
    }

    /**
     * Sections whose conversion would need more than the conversion budget
     * run one at a time (design §6.4), so several large sections can't
     * exhaust the heap together. The file still completes, with every
     * section converted.
     *
     * @throws Exception if the file can't be processed
     */
    @Test
    void conversionsThatDontFitTheBudgetTogetherRunOneAtATime()
            throws Exception
    {
        SectionConverter csv =
                ConverterRegistry.load().get("csv").orElseThrow();
        AtomicInteger running = new AtomicInteger();
        AtomicInteger most = new AtomicInteger();
        SectionConverter greedy = new SectionConverter()
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

            /** Claims the whole conversion budget for each section. */
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
            public byte[] convert(byte[] section, SplitConfig config)
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
                return csv.convert(section, config);
            }
        };
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(converting()
                .withConversion(ConverterRegistry.of(greedy), 64L << 20)
                .withConvertMemory(64L << 20), publisher, PipelineHooks.NONE);
        IngestPipeline.SplitResult result =
                pipeline.split(SeekableByteSource.of(convertibleFile()));
        assertThat(result.sectionCount()).isEqualTo(RECORDS);
        assertThat(pipeline.failedSectionCount(result.fileId())).isZero();
        assertThat(most.get()).isEqualTo(1);
    }

    /**
     * A converter that fails unexpectedly (a bug, not a problem it
     * recognises) fails only that section: it is stored as split, announced
     * as failed with {@code CONVERT_FAILED}, and the rest of the file is
     * converted.
     *
     * @throws Exception if the file can't be processed or read back
     */
    @Test
    void aConverterBugFailsTheSectionNotTheFile() throws Exception
    {
        SectionConverter csv =
                ConverterRegistry.load().get("csv").orElseThrow();
        SectionConverter buggy = new SectionConverter()
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
             * exception on a section that has {@code boom} in it.
             */
            @Override
            public byte[] convert(byte[] section, SplitConfig config)
                    throws ConversionException
            {
                if (new String(section, UTF_8).contains("boom"))
                {
                    throw new IndexOutOfBoundsException(
                            "would quote the section: boom");
                }
                return csv.convert(section, config);
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
        assertThat(result.sectionCount()).isEqualTo(3);
        Table table = catalog.loadTable(tableId);
        assertThat(dataRows(table, result.fileId()))
                .extracting(row -> row.getField(ERROR_CODE))
                .containsExactly(null, "CONVERT_FAILED", null);
        assertThat(decrypt(table, result.fileId(), 1))
                .isEqualTo((HEADER + bad).getBytes(UTF_8));
        List<ConnectorMessage> messages = decode(publisher.sent);
        assertThat(messages.stream().filter(SectionFailed.class::isInstance)
                .map(message -> ((SectionFailed) message).errorCode()))
                .containsExactly("CONVERT_FAILED");
        assertThat(sections(messages)).hasSize(2);
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
                committer, keys, new SectionIds(idKey),
                SplitterRegistry.load(), sealer, publisher, Clock.systemUTC());
        pipeline.setHooks(hooks);
        open.add(pipeline);
        return pipeline;
    }

    /**
     * Makes {@link #RECORDS} CSV records, each with a quoted note of two
     * lines that has a comma in it.
     *
     * @return the records, each ending in a line break
     */
    static List<String> records()
    {
        List<String> records = new ArrayList<>();
        for (int index = 0; index < RECORDS; index++)
        {
            records.add(index + ",name-" + index + ",\"note " + index
                    + "\nsecond line, with comma\"\n");
        }
        return records;
    }

    /**
     * Joins CSV records into a file.
     *
     * @param records the records, each ending in a line break
     * @return the file
     */
    static SeekableByteSource csv(List<String> records)
    {
        return SeekableByteSource.of(String.join("", records).getBytes(UTF_8));
    }

    // ---- happy path ----

    /**
     * Splits a file, encrypts its sections, commits them in batches and
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
        IngestPipeline.SplitResult result = pipeline.split(csv(records()));

        assertThat(result.sectionCount()).isEqualTo(RECORDS);
        Checkpoint checkpoint = pipeline.checkpoint().orElseThrow();
        assertThat(checkpoint.state()).isEqualTo(Checkpoint.State.ARCHIVING);
        assertThat(checkpoint.publishedSeq()).isEqualTo(RECORDS - 1);

        List<ConnectorMessage> messages = decode(publisher.sent);
        assertThat(messages.getFirst()).isInstanceOf(FileStarted.class);
        List<SectionStored> sections = sections(messages);
        assertThat(sections).extracting(SectionStored::seq)
                .containsExactlyElementsOf(
                        LongStream.range(0, RECORDS).boxed().toList());
        assertThat(sections).allSatisfy(section ->
        {
            assertThat(section.fileId()).isEqualTo(result.fileId());
            assertThat(section.storage().catalog()).isEqualTo("prod");
            assertThat(section.storage().namespace()).isEqualTo("sdh_ingest");
            assertThat(section.storage().table()).isEqualTo(DEFAULT_NAME);
        });
        // Three batches → three data snapshots, each referenced by its
        // messages.
        assertThat(sections.stream()
                .map(section -> section.storage().snapshotId()).distinct())
                .hasSize(3);

        Table table = catalog.loadTable(tableId);
        SectionIds ids =
                new SectionIds(IdKeys.loadOrCreate(table, null, keys, KEK));
        List<String> expected = records();
        for (SectionStored section : List.of(sections.get(0),
                sections.get(137), sections.get(249)))
        {
            byte[] plaintext =
                    expected.get((int) section.seq()).getBytes(UTF_8);
            assertThat(section.sectionId()).isEqualTo(ids.of(plaintext));
            assertThat(decrypt(table, section.fileId(), section.seq()))
                    .isEqualTo(plaintext);
        }
        assertThat(dataRows(table, result.fileId())).hasSize(RECORDS);
    }

    /**
     * A section longer than a chunk is stored in more than one, and reads
     * back whole, as do the sections around it.
     *
     * @throws Exception if the file can't be processed or read back
     */
    @Test
    void multiChunkSectionIsStreamedInTwoPasses() throws Exception
    {
        String big = "1,\"" + "x".repeat(SectionCrypto.CHUNK_SIZE + 500)
                + "\"\n";
        String small = "2,y\n";
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(settings(), publisher,
                PipelineHooks.NONE);
        UUID fileId = pipeline.split(csv(List.of(small, big, small))).fileId();

        List<SectionStored> stored = sections(decode(publisher.sent));
        assertThat(stored).extracting(SectionStored::chunkCount)
                .containsExactly(1, 2, 1);
        Table table = catalog.loadTable(tableId);
        assertThat(decrypt(table, fileId, 1)).isEqualTo(big.getBytes(UTF_8));
        assertThat(decrypt(table, fileId, 2)).isEqualTo(small.getBytes(UTF_8));
    }

    /**
     * With no time between commits, every section is committed on its own.
     *
     * @throws Exception if the file can't be processed or a message opened
     */
    @Test
    void zeroIntervalCommitsEverySection() throws Exception
    {
        RecordingPublisher publisher = new RecordingPublisher();
        IngestPipeline pipeline = process(
                settings().withCommit(BATCH, 1L << 30, Duration.ZERO),
                publisher, PipelineHooks.NONE);
        pipeline.split(csv(records().subList(0, 5)));
        assertThat(sections(decode(publisher.sent)).stream()
                .map(section -> section.storage().snapshotId()).distinct())
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
        assertThat(empty.sectionCount()).isZero();
        assertThat(empty.checkpoint().state())
                .isEqualTo(Checkpoint.State.ARCHIVING);
        assertThat(decode(publisher.sent)).singleElement()
                .isInstanceOf(FileStarted.class);
        // Splitting the same file again does nothing: it is already waiting
        // for archiving.
        assertThat(pipeline.split(SeekableByteSource.of(new byte[0])).fileId())
                .isEqualTo(empty.fileId());
    }

    /**
     * A send the broker fails is retried, and every section is still sent.
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
        pipeline.split(csv(records()));
        assertThat(sections(decode(publisher.sent)).stream()
                .map(SectionStored::seq).distinct()).hasSize(RECORDS);
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

        /** After every section is sent, before the end-of-file commit. */
        BEFORE_END_OF_FILE,

        /** The broker stops taking messages from the 151st send on. */
        BROKER_DOWN_MID_PUBLISH
    }

    /**
     * After the connector stops anywhere in a file, a new process resumes the
     * same file. Every section is stored exactly once and announced at least
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
        List<String> fileRecords = records();
        IngestPipeline first = process(settings(), publisher, hooks);
        assertThatThrownBy(() -> first.split(csv(fileRecords)))
                .isInstanceOfAny(SimulatedCrash.class,
                        PublishFailedException.class);
        first.close();
        UUID firstFileId = first.checkpoint().orElseThrow().fileId();
        assertThat(first.checkpoint().orElseThrow().state())
                .isEqualTo(Checkpoint.State.SPLITTING);

        publisher.fail = call -> false;
        IngestPipeline second = process(settings(), publisher,
                PipelineHooks.NONE);
        IngestPipeline.SplitResult result = second.split(csv(fileRecords));
        assertThat(result.fileId()).isEqualTo(firstFileId);
        assertThat(result.sectionCount()).isEqualTo(RECORDS);

        List<SectionStored> sent = sections(decode(publisher.sent));
        Map<Long, List<SectionStored>> bySeq = sent.stream()
                .collect(Collectors.groupingBy(SectionStored::seq));
        assertThat(bySeq.keySet()).hasSize(RECORDS)
                .allMatch(seq -> seq >= 0 && seq < RECORDS);
        bySeq.values().forEach(copies -> assertThat(copies.stream()
                .map(SectionStored::sectionId).distinct()).hasSize(1));
        assertThat(sent.size() - RECORDS).as("duplicates")
                .isLessThanOrEqualTo(BATCH);

        Table table = catalog.loadTable(tableId);
        List<Record> rows = dataRows(table, firstFileId);
        assertThat(rows).hasSize(RECORDS);
        assertThat(rows.stream().map(row -> row.getField(SEQ)).distinct())
                .hasSize(RECORDS);
        SectionIds ids =
                new SectionIds(IdKeys.loadOrCreate(table, null, keys, KEK));
        assertThat(rows).allSatisfy(row -> assertThat(row.getField(SECTION_ID))
                .isEqualTo(ids.of(fileRecords
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
     * Makes a CSV file with a header and {@link #RECORDS} records, all of
     * which convert.
     *
     * @return the file's bytes
     */
    static byte[] convertibleFile()
    {
        return (HEADER + String.join("", records())).getBytes(UTF_8);
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
        assertThat(result.sectionCount()).isEqualTo(RECORDS);
        assertThat(result.checkpoint().restarts()).isZero();

        List<Record> rows = dataRows(catalog.loadTable(tableId),
                result.fileId());
        assertThat(rows).hasSize(RECORDS)
                .allSatisfy(row -> assertThat(row.getField(ERROR_CODE))
                        .isEqualTo((Long) row.getField(SEQ) < BATCH
                                ? "CONVERT_SKIPPED" : null));
        List<ConnectorMessage> messages = decode(publisher.sent);
        assertThat(messages.stream().filter(SectionFailed.class::isInstance))
                .hasSize(BATCH);
        assertThat(sections(messages)).hasSize(RECORDS - BATCH)
                .allSatisfy(stored ->
                        assertThat(stored.mapping()).isEqualTo("csv-row/1"));
        assertThat(pipeline.failedSectionCount(result.fileId()))
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
        assertThat(result.sectionCount()).isEqualTo(RECORDS);
        assertThat(pipeline.failedSectionCount(result.fileId())).isZero();
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
        assertThatThrownBy(() -> first.split(csv(records())))
                .isInstanceOf(SimulatedCrash.class);

        IngestPipeline second = process(settings(), publisher,
                PipelineHooks.NONE);
        SeekableByteSource other = csv(List.of("9,other\n"));
        assertThatThrownBy(() -> second.split(other))
                .isInstanceOf(IllegalStateException.class);
        second.abandonInProgress();
        assertThat(second.checkpoint().orElseThrow().state())
                .isEqualTo(Checkpoint.State.ABANDONED);
        assertThat(second.split(other).sectionCount()).isEqualTo(1);
        // A file still being archived is protected too.
        assertThatThrownBy(() -> second.split(csv(records())))
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
        assertThatThrownBy(() -> stale.split(csv(records())))
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
        pipeline.split(csv(records().subList(0, 3)));
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
     * Picks out the messages announcing a stored section.
     *
     * @param messages the messages
     * @return those that are {@link SectionStored}, in order
     */
    static List<SectionStored> sections(List<ConnectorMessage> messages)
    {
        return messages.stream().filter(SectionStored.class::isInstance)
                .map(SectionStored.class::cast).toList();
    }

    /**
     * Reads a file's rows from the {@code sections} table.
     *
     * @param table  the table
     * @param fileId the file's id
     * @return copies of the rows, every chunk's
     * @throws IOException if the table can't be read
     */
    static List<Record> dataRows(Table table, UUID fileId) throws IOException
    {
        List<Record> found = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(FILE_ID, fileId)).build())
        {
            rows.forEach(row -> found.add(row.copy()));
        }
        return found;
    }

    /**
     * Reads a section's chunks from the table and decrypts them.
     *
     * @param table  the {@code sections} table
     * @param fileId the file's id
     * @param seq    the section's number in the file
     * @return the section's stored plaintext
     * @throws IOException if the table can't be read
     */
    byte[] decrypt(Table table, UUID fileId, long seq) throws IOException
    {
        Map<Integer, Record> chunks = new HashMap<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.and(Expressions.equal(FILE_ID, fileId),
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
            plaintext.writeBytes(SectionCrypto.decryptChunk(
                    SectionCrypto.sectionKey(batchDataKey, fileId, seq),
                    fileId, seq, chunkIndex, chunkCount,
                    SectionCrypto.expectedChunkLength(plaintextLength,
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
