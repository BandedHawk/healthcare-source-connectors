/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemConverted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.CommittedRecords.Failed;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.RecordInput;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.WrittenRecord;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.stage.ItemProgress;
import com.sdh.healthcare.connector.stage.StageInput;
import com.sdh.healthcare.connector.stage.StageOutput;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ConvertProcessor}, on an in-memory catalog: records are
 * written to {@code split_records} as the split stage writes them, then
 * converted, stored in {@code records} and read back.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ConvertProcessorTest
{

    /** The namespace both tables are in. */
    static final String NS = "sdh_ingest";

    /** The KEK the tables' data keys are wrapped with. */
    static final String KEK = "records-kek/1";

    /** The time batches are stamped with. */
    static final Instant NOW = Instant.parse("2026-11-06T10:00:00Z");

    /** The CSV header every record carries. */
    static final String HEADER = "a,b\n";

    /** Holds the KEK. */
    AesKeyWrapProvider kms;

    /** The catalog, named "prod". */
    InMemoryCatalog catalog;

    /** The split stage's table. */
    Table splitRecords;

    /** The shared records table. */
    Table records;

    /** Reads records from both tables. */
    QueueReader reader;

    /** The item the records come from. */
    final UUID item = UUID.randomUUID();

    /** Creates the tests; JUnit runs each test on a new instance. */
    ConvertProcessorTest()
    {
    }

    /**
     * Creates the keys, catalog, tables and reader.
     *
     * @throws Exception if a key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        KeyGenerator aes = KeyGenerator.getInstance("AES");
        aes.init(256);
        kms = new AesKeyWrapProvider(Map.of(KEK, aes.generateKey()));
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        splitRecords = IngestTables.splitRecords(catalog, NS);
        records = IngestTables.sharedRecords(catalog, NS);
        reader = QueueReader.builder().catalog(catalog)
                .expectedCatalogName("prod").keyProvider(kms).build();
    }

    /** Closes the reader. */
    @AfterEach
    void tearDown()
    {
        reader.close();
    }

    /**
     * A processor with CSV headers on and a small size limit.
     *
     * @param maxBytes the largest record converted
     * @return the processor
     */
    ConvertProcessor processor(long maxBytes)
    {
        return new ConvertProcessor(reader, ConverterRegistry.load(),
                new ConvertProcessor.Settings(Map.of("csv.header", "true"),
                        maxBytes, 64L << 20, "prod", NS, "test-build"),
                new ConvertProcessor.Storage(records, kms, KEK,
                        new RecordIds(new byte[32])),
                null, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    /**
     * Writes records to {@code split_records} as the split stage does, and
     * makes their messages.
     *
     * @param errorCode the splitter's error for each record, or null
     * @param texts     the records
     * @return a {@code record.split} for each
     */
    List<RecordSplit> split(List<String> errorCode, String... texts)
    {
        RecordBatchWriter writer = new RecordBatchWriter(splitRecords, kms,
                KEK, new RecordIds(new byte[32]), NOW);
        long offset = 0;
        for (int seq = 0; seq < texts.length; seq++)
        {
            writer.add(new RecordInput(item, seq, offset, "csv",
                    texts[seq].getBytes(UTF_8)));
            offset += texts[seq].length();
        }
        DataFile file = writer.finish();
        splitRecords.newAppend().appendFile(file).commit();
        StorageRef ref = new StorageRef("prod", NS, IngestTables.SPLIT_RECORDS,
                writer.ingestedDate(),
                splitRecords.currentSnapshot().snapshotId(), writer.kekRef());
        List<RecordSplit> messages = new ArrayList<>();
        for (WrittenRecord record : writer.written())
        {
            int seq = (int) record.seq();
            messages.add(new RecordSplit(record.recordId(), item, seq,
                    record.sourceOffset(), record.sourceLength(), "csv",
                    "text/csv", record.plaintextLength(), record.chunkCount(),
                    ref, null, errorCode.get(seq), null, seq, "m" + seq,
                    seq == 1 ? "earlier/m0" : null));
        }
        return messages;
    }

    /**
     * Processes and commits records, as the worker does.
     *
     * @param processor the processor
     * @param messages  the records' messages
     * @param attempt   the claim they are on
     * @return the messages announcing them
     * @throws Exception if one can't be processed or committed
     */
    static List<ConnectorMessage> run(ConvertProcessor processor,
            List<RecordSplit> messages, int attempt) throws Exception
    {
        List<StageOutput<ConvertProcessor.Prepared>> outputs =
                new ArrayList<>();
        for (RecordSplit message : messages)
        {
            outputs.add(processor.process(message, attempt));
        }
        return processor.commit(outputs);
    }

    /**
     * A record is converted to JSON and stored; one that fails conversion
     * and a piece the splitter couldn't parse are stored as split, failed,
     * with the queue message each started in kept, its broker IDs
     * encrypted, on their rows, and have no message: failures aren't sent
     * on (STAGES.md §15.1).
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void convertsStoresAndKeepsWhereFailedRecordsAre() throws Exception
    {
        List<String> errors = new ArrayList<>();
        errors.add(null);
        errors.add(null);
        errors.add("PARSE_UNTERMINATED_QUOTE");
        List<RecordSplit> messages = split(errors, HEADER + "1,2\n",
                HEADER + "1\n", HEADER + "\"x\n");
        List<ConnectorMessage> sent = run(processor(1 << 20), messages, 1);

        assertThat(sent.get(0)).isInstanceOfSatisfying(RecordStored.class,
                stored ->
                {
                    assertThat(stored.mapping()).isNotNull();
                    assertThat(stored.storage().table())
                            .isEqualTo("records");
                    assertThat(new String(reader.readRecord(stored, 1 << 20),
                            UTF_8)).isEqualTo("[\"1\",\"2\"]");
                });
        // The failed ones are stored, and not sent on (STAGES.md §15.1).
        assertThat(sent.get(1)).isNull();
        assertThat(sent.get(2)).isNull();

        records.refresh();
        assertThat(CommittedRecords.failed(records, item, kms))
                .containsExactly(
                        new Failed(1, messages.get(1).sourceOffset(),
                                ErrorCode.CONVERT_CSV_FIELD_COUNT.name(),
                                null, 1, "m1", "earlier/m0"),
                        new Failed(2, messages.get(2).sourceOffset(),
                                "PARSE_UNTERMINATED_QUOTE", null, 2, "m2",
                                null));
        // Without the key, the message number is still there.
        assertThat(CommittedRecords.failed(records, item))
                .extracting(Failed::message).containsExactly(1, 2);
    }

    /**
     * A record on its third claim, and one over the size limit, are stored
     * as split with their reasons, and not read before they are committed.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void skipsRecordsOnTheirThirdClaimOrTooLarge() throws Exception
    {
        List<String> errors = new ArrayList<>();
        errors.add(null);
        List<RecordSplit> first = split(errors, HEADER + "1,2\n");
        ConvertProcessor skipping = processor(1 << 20);
        StageOutput<ConvertProcessor.Prepared> skipped = skipping.process(
                first.get(0), ConvertProcessor.SKIP_ON_ATTEMPT);
        assertThat(skipped.failed()).isTrue();
        assertThat(skipped.errorCode())
                .isEqualTo(ErrorCode.CONVERT_SKIPPED.name());
        assertThat(skipping.commit(List.of(skipped))).containsOnlyNulls();
        records.refresh();
        assertThat(CommittedRecords.all(records, item)).singleElement()
                .satisfies(row -> assertThat(row.errorCode())
                        .isEqualTo(ErrorCode.CONVERT_SKIPPED.name()));
        assertThat(processor(4).process(first.get(0), 1).errorCode())
                .isEqualTo(ErrorCode.CONVERT_TOO_LARGE.name());
    }

    /**
     * The same record gives the same digest each time; a different output
     * for it, such as a skipped one, a different digest.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void theSameOutputHasTheSameDigest() throws Exception
    {
        List<String> errors = new ArrayList<>();
        errors.add(null);
        RecordSplit message = split(errors, HEADER + "1,2\n").get(0);
        ConvertProcessor processor = processor(1 << 20);
        StageOutput<ConvertProcessor.Prepared> once =
                processor.process(message, 1);
        StageOutput<ConvertProcessor.Prepared> twice =
                processor.process(message, 2);
        StageOutput<ConvertProcessor.Prepared> skipped =
                processor.process(message, ConvertProcessor.SKIP_ON_ATTEMPT);
        assertThat(once.digest()).isEqualTo(twice.digest())
                .isNotEqualTo(skipped.digest());
        assertThat(once.failed()).isFalse();
        assertThat(skipped.failed()).isTrue();
    }

    /**
     * A record stored skipped and compared again at the attempt it was
     * stored at (STAGES.md §6) is skipped again, with the same digest: it
     * isn't converted, so it can't stop the worker as it did twice before,
     * and isn't taken for a conflict.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aSkippedRecordComparesAsSkipped() throws Exception
    {
        List<String> errors = new ArrayList<>();
        errors.add(null);
        RecordSplit message = split(errors, HEADER + "1,2\n").get(0);
        StageOutput<ConvertProcessor.Prepared> stored = processor(1 << 20)
                .process(message, ConvertProcessor.SKIP_ON_ATTEMPT);
        ConvertProcessor rebuilt = new ConvertProcessor(reader,
                ConverterRegistry.load(),
                new ConvertProcessor.Settings(Map.of("csv.header", "true"),
                        1 << 20, 64L << 20, "prod", NS, "another-build"),
                new ConvertProcessor.Storage(records, kms, KEK,
                        new RecordIds(new byte[32])),
                null, Clock.fixed(NOW, ZoneOffset.UTC));
        assertThat(rebuilt.version(message))
                .isNotEqualTo(processor(1 << 20).version(message));
        StageOutput<ConvertProcessor.Prepared> again = rebuilt.process(
                message, ConvertProcessor.SKIP_ON_ATTEMPT);
        assertThat(again.digest()).isEqualTo(stored.digest());
        assertThat(again.value().error())
                .isEqualTo(ErrorCode.CONVERT_SKIPPED);
    }

    /**
     * A record's version names the build and its converter's mapping, and
     * changes with the settings that change its output, but not with the
     * rest; it holds no setting's value.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void theVersionFollowsWhatDecidesTheOutput() throws Exception
    {
        List<String> errors = new ArrayList<>();
        errors.add(null);
        RecordSplit message = split(errors, HEADER + "1,2\n").get(0);
        String version = processor(1 << 20).version(message);
        assertThat(version).startsWith("test-build/csv-row/")
                .doesNotContain("csv.header")
                .isEqualTo(processor(1 << 20).version(message))
                .isNotEqualTo(processor(1 << 19).version(message));
        ConvertProcessor otherOptions = new ConvertProcessor(reader,
                ConverterRegistry.load(),
                new ConvertProcessor.Settings(Map.of("csv.header", "false"),
                        1 << 20, 32L << 20, "other", NS, "test-build"),
                new ConvertProcessor.Storage(records, kms, KEK,
                        new RecordIds(new byte[32])),
                null, Clock.fixed(NOW, ZoneOffset.UTC));
        assertThat(otherOptions.version(message)).isNotEqualTo(version);
        ConvertProcessor otherMemory = new ConvertProcessor(reader,
                ConverterRegistry.load(),
                new ConvertProcessor.Settings(Map.of("csv.header", "true"),
                        1 << 20, 32L << 20, "other", NS, "test-build"),
                new ConvertProcessor.Storage(records, kms, KEK,
                        new RecordIds(new byte[32])),
                null, Clock.fixed(NOW, ZoneOffset.UTC));
        assertThat(otherMemory.version(message)).isEqualTo(version);
    }

    /**
     * The release names every version the stage stamps: the one of each
     * converter's records, and the one of records stored as split
     * (STAGES.md §14.4a).
     */
    @Test
    void theReleaseHoldsEveryVersionStamped()
    {
        List<String> errors = new ArrayList<>();
        errors.add(null);
        RecordSplit message = split(errors, HEADER + "1,2\n").get(0);
        ConvertProcessor.Settings settings = new ConvertProcessor.Settings(
                Map.of("csv.header", "true"), 1 << 20, 32L << 20, "other",
                NS, "test-build");
        ConvertProcessor processor = new ConvertProcessor(reader,
                ConverterRegistry.load(), settings,
                new ConvertProcessor.Storage(records, kms, KEK,
                        new RecordIds(new byte[32])),
                null, Clock.fixed(NOW, ZoneOffset.UTC));
        String version = processor.version(message);
        assertThat(ConvertProcessor.versions(ConverterRegistry.load(),
                settings))
                .contains(version, "test-build/none/"
                        + version.substring(version.lastIndexOf('/') + 1))
                .hasSize(ConverterRegistry.load().mappings().size() + 1);
    }

    /**
     * The stage takes {@code record.split} and {@code item.split}, and
     * completes an item with its counts, and the intake and producer's
     * references from its {@code item.split} (STAGES.md §17.4).
     */
    @Test
    void classifiesAndCompletesItems()
    {
        ConvertProcessor processor = processor(1 << 20);
        ItemSplit total = new ItemSplit(item, "claims", 3, 1, 100, "digest",
                "ref-1", null, null, NOW, NOW);
        assertThat(processor.classify(total))
                .isEqualTo(new StageInput.ItemTotal(item, 3));
        assertThat(processor.classify(new ItemConverted(item, "claims", 1, 0,
                NOW))).isInstanceOf(StageInput.Unwanted.class);

        assertThat(processor.itemComplete(
                new ItemProgress(item, 3, 3, 1, total)))
                .isEqualTo(new ItemConverted(item, "claims", 3, 1, NOW,
                        List.of(), 1, 1, null, "ref-1", null));
        assertThat(((ItemConverted) processor.itemComplete(
                new ItemProgress(item, 3, 3, 1))).intake())
                .isEqualTo(ConvertProcessor.UNKNOWN_INTAKE);
        assertThat(ConvertProcessor.splitError("PARSE_FROM_THE_FUTURE"))
                .isEqualTo(ErrorCode.PARSE_UNRECOGNISED);
    }
}
