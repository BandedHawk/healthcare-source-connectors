/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemConverted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemTransformed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordTransformed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.core.convert.Representation;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.SplitRecord;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.CommittedRecords.Failed;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.RecordInput;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.WrittenRecord;
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
 * Tests for {@link RecordStageProcessor}, on an in-memory catalog, with
 * the {@link UpperTransform}: as the convert stage, from
 * {@code split_records} to {@code records}, and as the transform stage,
 * from {@code records} to a gold table.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RecordStageProcessorTest
{

    /** The namespace every table is in. */
    static final String NS = "sdh_ingest";

    /** The KEK the tables' data keys are wrapped with. */
    static final String KEK = "records-kek/1";

    /** The time batches are stamped with. */
    static final Instant NOW = Instant.parse("2026-11-10T10:00:00Z");

    /** The gold table's name. */
    static final String GOLD = "bundles";

    /** Holds the KEK. */
    AesKeyWrapProvider kms;

    /** The catalog, named "prod". */
    InMemoryCatalog catalog;

    /** The split stage's table. */
    Table splitRecords;

    /** The convert stage's table. */
    Table records;

    /** The transform stage's table. */
    Table gold;

    /** Reads records from every table. */
    QueueReader reader;

    /** The item the records come from. */
    final UUID item = UUID.randomUUID();

    /** Creates the tests; JUnit runs each test on a new instance. */
    RecordStageProcessorTest()
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
        gold = IngestTables.shared(catalog, NS, GOLD);
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
     * A table as the processor names it.
     *
     * @param name  its name
     * @param table the table
     * @return it
     */
    static RecordTable table(String name, Table table)
    {
        return new RecordTable("prod", NS, name, table);
    }

    /**
     * The processor as the convert stage.
     *
     * @return it
     */
    RecordStageProcessor convert()
    {
        return processor(StageMessages.CONVERT,
                table(IngestTables.SPLIT_RECORDS, splitRecords),
                table("records", records));
    }

    /**
     * The processor as the transform stage.
     *
     * @return it
     */
    RecordStageProcessor transform()
    {
        return processor(StageMessages.TRANSFORM, table("records", records),
                table(GOLD, gold));
    }

    /**
     * A processor with no results and nothing watching for a backlog.
     *
     * @param messages the stage's messages
     * @param source   its source
     * @param sink     its sink
     * @return it
     */
    RecordStageProcessor processor(StageMessages messages, RecordTable source,
            RecordTable sink)
    {
        return new RecordStageProcessor(new UpperTransform(messages), reader,
                new RecordStageProcessor.Tables(source,
                        new RecordStageProcessor.Sink(sink, kms, KEK,
                                new RecordIds(new byte[32])), null),
                64L << 20, null, Clock.fixed(NOW, ZoneOffset.UTC), null);
    }

    /**
     * Writes records to {@code split_records} as the split stage does, and
     * makes their messages, each in a queue message of its own.
     *
     * @param format    each record's format
     * @param errorCode the splitter's error for each record, or null
     * @param texts     the records
     * @return a {@code record.split} for each
     */
    List<RecordSplit> split(List<String> format, List<String> errorCode,
            String... texts)
    {
        RecordBatchWriter writer = new RecordBatchWriter(splitRecords, kms,
                KEK, new RecordIds(new byte[32]), NOW);
        long offset = 0;
        for (int seq = 0; seq < texts.length; seq++)
        {
            writer.add(new RecordInput(item, seq, offset, format.get(seq),
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
                    record.sourceOffset(), record.sourceLength(),
                    format.get(seq), "text/plain", record.plaintextLength(),
                    record.chunkCount(), ref, null, errorCode.get(seq), null,
                    seq, "m" + seq, seq == 1 ? "earlier/m0" : null));
        }
        return messages;
    }

    /**
     * Writes records to {@code records} as the convert stage does, and
     * makes their messages, each in a queue message of its own.
     *
     * @param texts the records' JSON
     * @return a {@code record.stored} for each
     * @throws Exception if one can't be written
     */
    List<RecordStored> converted(String... texts) throws Exception
    {
        RecordBatchWriter writer = new RecordBatchWriter(records, kms, KEK,
                new RecordIds(new byte[32]), NOW);
        long offset = 0;
        for (int seq = 0; seq < texts.length; seq++)
        {
            writer.addBytes(item, seq, "ccda", SplitRecord.ofRange(offset,
                    texts[seq].length(), ResumePoint.at(offset
                            + texts[seq].length())),
                    texts[seq].getBytes(UTF_8), Representation.json("ccda/2"),
                    null, -1, null);
            offset += texts[seq].length();
        }
        DataFile file = writer.finish();
        records.newAppend().appendFile(file).commit();
        StorageRef ref = new StorageRef("prod", NS, "records",
                writer.ingestedDate(), records.currentSnapshot().snapshotId(),
                writer.kekRef());
        List<RecordStored> messages = new ArrayList<>();
        for (WrittenRecord record : writer.written())
        {
            int seq = (int) record.seq();
            messages.add(new RecordStored(record.recordId(), item, seq,
                    record.sourceOffset(), record.sourceLength(), "ccda",
                    Representation.JSON, "ccda/2", record.plaintextLength(),
                    record.chunkCount(), ref, null, "convert-v", null, null,
                    false, null, seq, "m" + seq, null));
        }
        return messages;
    }

    /**
     * Processes and commits records, as the worker does.
     *
     * @param processor the processor
     * @param messages  the records' messages
     * @return the messages announcing them
     * @throws Exception if one can't be processed or committed
     */
    static List<ConnectorMessage> run(RecordStageProcessor processor,
            List<? extends ConnectorMessage> messages) throws Exception
    {
        List<StageOutput<RecordStageProcessor.Prepared>> outputs =
                new ArrayList<>();
        for (ConnectorMessage message : messages)
        {
            outputs.add(processor.process(message, 1));
        }
        return processor.commit(outputs);
    }

    /**
     * As the convert stage: a record is transformed, stored and announced
     * with its queue message carried on; one the transform fails, and a
     * piece the splitter couldn't parse, are stored as they came, failed,
     * with their queue message kept on their rows, and not sent on; a
     * record with nothing to apply passes as it came.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void transformsStoresAndCarriesTheMessageOn() throws Exception
    {
        List<String> errors = new ArrayList<>();
        errors.add(null);
        errors.add(null);
        errors.add(null);
        errors.add("PARSE_UNTERMINATED_QUOTE");
        List<RecordSplit> messages = split(List.of("csv", "csv", "raw",
                "csv"), errors, "hello", "a bad one", "as it is", "\"x");
        List<ConnectorMessage> sent = run(convert(), messages);

        assertThat(sent.get(0)).isInstanceOfSatisfying(RecordStored.class,
                stored ->
                {
                    assertThat(stored.mapping())
                            .isEqualTo(UpperTransform.MAPPING);
                    assertThat(stored.version())
                            .isEqualTo(UpperTransform.VERSION);
                    assertThat(stored.storage().table()).isEqualTo("records");
                    assertThat(stored.message()).isZero();
                    assertThat(stored.messageRef()).isEqualTo("m0");
                    assertThat(new String(reader.readRecord(stored, 1 << 10),
                            UTF_8)).isEqualTo("HELLO");
                });
        assertThat(sent.get(1)).isNull();
        assertThat(sent.get(2)).isInstanceOfSatisfying(RecordStored.class,
                stored ->
                {
                    assertThat(stored.mapping()).isNull();
                    assertThat(stored.contentType()).isEqualTo("text/plain");
                    assertThat(new String(reader.readRecord(stored, 1 << 10),
                            UTF_8)).isEqualTo("as it is");
                });
        assertThat(sent.get(3)).isNull();

        records.refresh();
        assertThat(CommittedRecords.failed(records, item, kms))
                .containsExactly(
                        new Failed(1, 5, "CONVERT_CSV_MALFORMED", null, 1,
                                "m1", "earlier/m0"),
                        new Failed(3, 22, "PARSE_UNTERMINATED_QUOTE", null, 3,
                                "m3", null));
    }

    /**
     * A record the transform throws on fails alone, with the transform's
     * code for a bug, and the rest of the batch is stored.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void failsARecordTheTransformThrowsOn() throws Exception
    {
        List<String> errors = new ArrayList<>();
        errors.add(null);
        errors.add(null);
        List<ConnectorMessage> sent = run(convert(), split(List.of("csv",
                "csv"), errors, "boom", "fine"));

        assertThat(sent.get(0)).isNull();
        assertThat(sent.get(1)).isInstanceOf(RecordStored.class);
        records.refresh();
        assertThat(CommittedRecords.failed(records, item, kms))
                .extracting(Failed::errorCode)
                .containsExactly("CONVERT_FAILED");
    }

    /**
     * A record message naming any table but the source is refused, as is
     * one of another catalog: the stage reads only where it was told to.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void refusesARecordOfAnotherTable() throws Exception
    {
        RecordSplit split = split(List.of("csv"), java.util.Arrays.asList(
                (String) null), "hello").getFirst();
        RecordStageProcessor processor = convert();
        assertThat(processor.classify(split))
                .isInstanceOf(StageInput.RecordInput.class);
        StorageRef ref = split.storage();
        for (StorageRef other : List.of(
                new StorageRef("prod", NS, "records", ref.ingestedDate(),
                        ref.snapshotId(), ref.kekRef()),
                new StorageRef("other", NS, IngestTables.SPLIT_RECORDS,
                        ref.ingestedDate(), ref.snapshotId(), ref.kekRef()),
                new StorageRef("prod", "elsewhere",
                        IngestTables.SPLIT_RECORDS, ref.ingestedDate(),
                        ref.snapshotId(), ref.kekRef())))
        {
            RecordSplit moved = new RecordSplit(split.recordId(), item, 0,
                    0, 5, "csv", "text/plain", 5, 1, other, null, null, null,
                    -1, null, null);
            assertThat(processor.classify(moved))
                    .isEqualTo(new StageInput.Unwanted("wrong_source"));
        }
    }

    /**
     * As the transform stage, a {@code record.stored} is mapped into the
     * gold table and announced as {@code record.transformed}, its queue
     * message carried on; one that fails is stored in gold as the JSON it
     * came as, with its message kept on its row.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void mapsStoredRecordsIntoItsSinkAsTheTransformStage() throws Exception
    {
        List<RecordStored> messages = converted("{\"a\":1}",
                "{\"bad\":2}");
        List<ConnectorMessage> sent = run(transform(), messages);

        assertThat(sent.get(0)).isInstanceOfSatisfying(
                RecordTransformed.class, mapped ->
                {
                    assertThat(mapped.contentType())
                            .isEqualTo("application/fhir+json");
                    assertThat(mapped.mapping())
                            .isEqualTo(UpperTransform.MAPPING);
                    assertThat(mapped.storage().table()).isEqualTo(GOLD);
                    assertThat(mapped.messageRef()).isEqualTo("m0");
                    assertThat(new String(reader.readRecord(mapped, 1 << 10),
                            UTF_8)).isEqualTo("{\"A\":1}");
                });
        assertThat(sent.get(1)).isNull();
        gold.refresh();
        assertThat(CommittedRecords.failed(gold, item, kms))
                .containsExactly(new Failed(1, 7, "CONVERT_CSV_MALFORMED",
                        null, 1, "m1", null));
        assertThat(CommittedRecords.all(gold, item)).filteredOn(
                row -> row.errorCode() != null)
                .singleElement().satisfies(row ->
                {
                    assertThat(row.contentType())
                            .isEqualTo(Representation.JSON);
                    assertThat(row.mapping()).isEqualTo("ccda/2");
                    assertThat(row.stageVersion())
                            .isEqualTo(UpperTransform.VERSION);
                });
    }

    /**
     * Each stage takes only the stage before's messages: its record
     * messages, its item end, waiting only for the records it sent on, and
     * its {@code item.progress}.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void takesOnlyTheStageBeforesMessages() throws Exception
    {
        RecordStageProcessor processor = transform();
        RecordStored stored = converted("{}").getFirst();
        assertThat(processor.classify(stored))
                .isInstanceOf(StageInput.RecordInput.class);
        // Two of five declined at the convert stage: three come.
        assertThat(processor.classify(new ItemConverted(item, "claims", 5, 2,
                NOW))).isEqualTo(new StageInput.ItemTotal(item, 3, false, 1,
                        1, null));
        assertThat(processor.classify(new ItemProgressed(item, "claims",
                "convert", 5, 3, 2, 0, null, null, null, NOW, null)))
                .isEqualTo(new StageInput.ItemTotal(item, 3, true, 0, 1,
                        null));
        assertThat(processor.classify(new ItemProgressed(item, "claims",
                "split", 5, 3, 2, 0, null, null, null, NOW, null)))
                .isInstanceOf(StageInput.Unwanted.class);
        assertThat(processor.classify(new ItemSplit(item, "claims", 5, 0, 9,
                "d", null, null, null, NOW, NOW)))
                .isInstanceOf(StageInput.Unwanted.class);
        assertThat(convert().classify(new ItemSplit(item, "claims", 5, 0, 9,
                "d", null, null, null, NOW, NOW)))
                .isEqualTo(new StageInput.ItemTotal(item, 5, false, 1, 1,
                        null));
    }

    /**
     * The stage's item end carries on what reached it from the stage
     * before: the producer's reference, the item it replaces, the item's
     * failure and whether a result was sent (STAGES.md §17.4, §17.5).
     */
    @Test
    void endsAnItemCarryingOnWhatReachedIt()
    {
        UUID replaced = UUID.randomUUID();
        Problem cut = new Problem("PARSE_XML_MALFORMED", "split", -1, 300,
                null, -1, null, null);
        ItemConverted total = new ItemConverted(item, "claims", 3, 0, NOW,
                List.of(), 1, 1, null, "a.xml", replaced, cut, true);
        ConnectorMessage end = transform().itemComplete(new ItemProgress(item,
                3, 3, 1, total));

        assertThat(end).isEqualTo(new ItemTransformed(item, "claims", 3, 1,
                NOW, List.of(), 1, 1, null, "a.xml", replaced, cut, true));
        assertThat(convert().itemProgress(new ItemProgress(item, 3, 3, 1,
                null))).isInstanceOfSatisfying(ItemProgressed.class,
                        progressed -> assertThat(progressed.intake())
                                .isEqualTo(RecordStageProcessor
                                        .UNKNOWN_INTAKE));
    }
}
