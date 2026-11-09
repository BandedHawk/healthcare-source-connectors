/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemConverted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemResult;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordFailed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.position.LineCounter;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.convert.ConversionBudget;
import com.sdh.healthcare.connector.core.convert.ConversionException;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.convert.RecordConverter;
import com.sdh.healthcare.connector.core.convert.Representation;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitRecord;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.MessagePlace;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.WrittenRecord;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.stage.ItemProgress;
import com.sdh.healthcare.connector.stage.StageInput;
import com.sdh.healthcare.connector.stage.StageOutput;
import com.sdh.healthcare.connector.stage.StageProcessor;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The convert stage's work on each record (STAGES.md §9): read it from
 * {@code split_records}, convert it to JSON with its format's converter
 * (design §6.4), store it in the shared {@code records} table, and announce
 * it with {@code record.stored}, or {@code record.failed} when it is stored
 * as split with an error. Once all of an item's records are stored it
 * sends {@code item.converted}, then the producer's result.
 * <p>
 * A record is stored as split, not converted, when the splitter couldn't
 * parse it (with the splitter's error), when it is larger than the most
 * converted ({@code CONVERT_TOO_LARGE}), when it is on its third claim, two
 * workers having stopped part-way through it ({@code CONVERT_SKIPPED}), or
 * when its format has no converter (no error). The conversions running at
 * once share a memory budget, so large records can't run the heap out
 * together. Never logs record content, names or IDs.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ConvertProcessor
        implements StageProcessor<ConvertProcessor.Prepared>
{

    /** The stage's name, in the registry, leases and audit events. */
    public static final String STAGE = "convert";

    /**
     * The claim on which a record is stored unconverted: two workers
     * stopped part-way through it before, perhaps because of it.
     */
    public static final int SKIP_ON_ATTEMPT = 3;

    /** The intake of an item whose {@code item.split} no longer opens. */
    public static final String UNKNOWN_INTAKE = "unknown";

    /** Logs converter bugs, by class name only. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ConvertProcessor.class);

    /**
     * A record, ready to store.
     *
     * @param split          the message it came in
     * @param bytes          what to store; null to store it as split, read
     *                       again when it is committed
     * @param representation what the stored bytes are
     * @param error          why it isn't converted; null if it is, or its
     *                       format has no converter
     * @param errorPosition  where in the record it failed, in bytes; -1
     *                       without an error
     * @param errorAt        where in the item it failed; null if unknown
     * @param correctionOf   for an operator's correction, the ID of the
     *                       failed row of {@code records} it corrects
     *                       (STAGES.md §15.6c); null otherwise
     */
    public record Prepared(RecordSplit split, byte[] bytes,
            Representation representation, ErrorCode error,
            long errorPosition, SourcePosition errorAt, String correctionOf)
    {

        /**
         * A record, not a correction.
         *
         * @param split          the message it came in
         * @param bytes          what to store, or null
         * @param representation what the stored bytes are
         * @param error          why it isn't converted, or null
         * @param errorPosition  where in the record it failed, or -1
         * @param errorAt        where in the item it failed, or null
         */
        public Prepared(RecordSplit split, byte[] bytes,
                Representation representation, ErrorCode error,
                long errorPosition, SourcePosition errorAt)
        {
            this(split, bytes, representation, error, errorPosition, errorAt,
                    null);
        }

        /**
         * The same record, made from a correction.
         *
         * @param of the failed row it corrects
         * @return the record
         */
        Prepared correcting(String of)
        {
            return new Prepared(split, bytes, representation, error,
                    errorPosition, errorAt, of);
        }
    }

    /**
     * How records are converted and where they are stored.
     *
     * @param options     format options (design §6.2), for every format
     * @param maxBytes    the largest record converted
     * @param memoryBytes the heap the conversions running at once may use
     * @param catalogName the catalog's name, put in messages
     * @param namespace   the namespace of {@code records}
     * @param build       the running build ({@code StageBuild}), the start
     *                    of each record's version
     */
    public record Settings(Map<String, String> options, long maxBytes,
            long memoryBytes, String catalogName, String namespace,
            String build)
    {

        /**
         * No options is none, not null.
         */
        public Settings
        {
            options = options == null ? Map.of() : Map.copyOf(options);
        }
    }

    /**
     * Where and how records are stored.
     *
     * @param records the shared {@code records} table
     * @param keys    wraps the batches' data keys, and unwraps them to read
     *                the failed records' broker IDs back
     * @param kekRef  the KEK the data keys are wrapped with
     * @param ids     makes record IDs with the table's ID key
     * @param splitRecords the split stage's {@code split_records}, to read
     *                the pieces it couldn't parse for the producer's result,
     *                since failures aren't sent on (STAGES.md §15.1); null
     *                to report only this stage's
     */
    public record Storage(Table records, KeyProvider keys, String kekRef,
            RecordIds ids, Table splitRecords)
    {

        /**
         * Storage that reports only this stage's failures.
         *
         * @param records the shared {@code records} table
         * @param keys    wraps and unwraps the batches' data keys
         * @param kekRef  the KEK the data keys are wrapped with
         * @param ids     makes record IDs with the table's ID key
         */
        public Storage(Table records, KeyProvider keys, String kekRef,
                RecordIds ids)
        {
            this(records, keys, kekRef, ids, null);
        }
    }

    /** Reads split records. */
    private final QueueReader reader;

    /** The converters, by format. */
    private final ConverterRegistry converters;

    /** How records are converted and where they are stored. */
    private final Settings settings;

    /** Where records are stored. */
    private final Storage storage;

    /** Heap shared by the conversions running at once. */
    private final ConversionBudget budget;

    /** Sends the producer's results; null if they are off. */
    private final ResultReporter results;

    /** Stamps batches and completions. */
    private final Clock clock;

    /** The format settings, by format, checked once each. */
    private final Map<String, SplitConfig> configs =
            new ConcurrentHashMap<>();

    /**
     * A digest of the settings that change a record's output: the format
     * options and the largest record converted.
     */
    private final String settingsDigest;

    /**
     * The records the reconciler is handing in, rebuilt, by item and
     * record ID (see {@link #rebuilding}).
     */
    private final Set<String> rebuilt = ConcurrentHashMap.newKeySet();

    /** Notes a backlog coming off the queue; null if nothing watches. */
    private final BacklogWatch backlog;

    /**
     * Creates the processor, with nothing watching for a backlog.
     *
     * @param reader     reads split records
     * @param converters the converters, by format
     * @param settings   how records are converted
     * @param storage    where they are stored
     * @param results    sends the producer's results; null if they are off
     * @param clock      stamps batches and completions
     */
    public ConvertProcessor(QueueReader reader, ConverterRegistry converters,
            Settings settings, Storage storage, ResultReporter results,
            Clock clock)
    {
        this(reader, converters, settings, storage, results, clock, null);
    }

    /**
     * Creates the processor.
     *
     * @param reader     reads split records
     * @param converters the converters, by format
     * @param settings   how records are converted
     * @param storage    where they are stored
     * @param results    sends the producer's results; null if they are off
     * @param clock      stamps batches and completions
     * @param backlog    told of each record that comes off the queue, for
     *                   the reconciler (STAGES.md §9); null if none
     */
    public ConvertProcessor(QueueReader reader, ConverterRegistry converters,
            Settings settings, Storage storage, ResultReporter results,
            Clock clock, BacklogWatch backlog)
    {
        this.backlog = backlog;
        this.reader = reader;
        this.converters = converters;
        this.settings = settings;
        this.storage = storage;
        this.budget = new ConversionBudget(settings.memoryBytes());
        this.results = results;
        this.clock = clock;
        this.settingsDigest = settingsDigest(settings);
    }

    @Override
    public String stage()
    {
        return STAGE;
    }

    /**
     * {@inheritDoc}
     *
     * A {@code record.split} off the queue, not one the reconciler rebuilt,
     * is shown to the backlog watch.
     *
     * @return a record for {@code record.split}, the item's total for
     *         {@code item.split}, nothing for any other type
     */
    @Override
    public StageInput classify(ConnectorMessage message)
    {
        if (backlog != null && message instanceof RecordSplit split
                && !rebuilt.contains(rebuiltKey(split)))
        {
            backlog.received(split);
        }
        return switch (message)
        {
            case RecordSplit split -> new StageInput.RecordInput(
                    split.recordId(), split.itemId(), split.seq());
            case ItemSplit item -> new StageInput.ItemTotal(item.itemId(),
                    item.recordCount(), false, item.completion());
            // The split stage has failures open: the records passed so far
            // (STAGES.md §15.6).
            case ItemProgressed progressed -> new StageInput.ItemTotal(
                    progressed.itemId(), progressed.passedCount(), true,
                    progressed.completion());
            default -> new StageInput.Unwanted("wrong_type");
        };
    }

    /**
     * {@inheritDoc}
     *
     * @return the build, the mapping of the record's converter ({@code none}
     *         if its format has none) and a digest of the format options
     *         and the largest record converted, such as
     *         {@code 0.1.0@2026-11-06T10:15:30Z/ccda/2/3f9a0c1d22b7}
     */
    @Override
    public String version(ConnectorMessage message)
    {
        RecordSplit split = (RecordSplit) message;
        String mapping = converters.get(split.format())
                .map(RecordConverter::mapping).orElse("none");
        return settings.build() + "/" + mapping + "/" + settingsDigest;
    }

    /**
     * Notes records the reconciler rebuilt from {@code split_records} and
     * is about to hand in (STAGES.md §9). A rebuilt {@code record.split}
     * doesn't know the queue message its record is in, so if one fails, it
     * is audited, {@code convert.placement_unknown}: for an item that is a
     * message batch, the producer's result (if results are on) names no
     * message for it.
     *
     * @param messages the rebuilt messages
     */
    void rebuilding(List<ConnectorMessage> messages)
    {
        for (ConnectorMessage message : messages)
        {
            rebuilt.add(rebuiltKey((RecordSplit) message));
        }
    }

    /**
     * Forgets records noted by {@link #rebuilding}, once handed in.
     *
     * @param messages the rebuilt messages
     */
    void rebuilt(List<ConnectorMessage> messages)
    {
        for (ConnectorMessage message : messages)
        {
            rebuilt.remove(rebuiltKey((RecordSplit) message));
        }
    }

    /**
     * What a rebuilt record is noted by.
     *
     * @param split its message
     * @return its item and record ID
     */
    private static String rebuiltKey(RecordSplit split)
    {
        return split.itemId() + "/" + split.recordId();
    }

    /**
     * {@inheritDoc}
     *
     * @return the record converted, or to be stored as split with the
     *         reason
     */
    @Override
    public StageOutput<Prepared> process(ConnectorMessage message,
            int attempt) throws InterruptedException
    {
        RecordSplit split = (RecordSplit) message;
        StageOutput<Prepared> output = prepare(split, attempt);
        if (output.failed() && rebuilt.contains(rebuiltKey(split)))
        {
            Telemetry.get().auditWarn("convert.placement_unknown",
                    Fields.of(Attr.STAGE, STAGE)
                            .with(Attr.ITEM_ID, split.itemId())
                            .with(Attr.SEQ, split.seq())
                            .with(Attr.ERROR_CODE, output.value().error()));
        }
        return output;
    }

    /**
     * {@inheritDoc}
     *
     * @return true: the stage converts operators' corrections
     */
    @Override
    public boolean corrects()
    {
        return true;
    }

    /**
     * Converts an operator's correction of a record that failed here
     * (STAGES.md §15.6c): the corrected record, in place of the one in
     * {@code split_records}, converted as the record was, with its place in
     * the item, its origin and its message taken from the failed row of
     * {@code records}. Its output is a new row there, marked
     * {@code correction_of} the failed one; if it passes, it is sent on as
     * {@code record.stored}.
     *
     * @param correction the correction
     * @return the output, and the version it is made with
     * @throws InterruptedException if interrupted waiting for memory
     * @throws IllegalStateException if the failed record isn't in
     *                               {@code records}
     */
    @Override
    public Corrected<Prepared> correct(CorrectionStore.Pending correction)
            throws InterruptedException
    {
        UUID itemId = correction.itemId();
        long seq = correction.seq();
        Table records = storage.records();
        records.refresh();
        CommittedRecords.Row original = CommittedRecords.all(records, itemId)
                .stream()
                .filter(row -> row.seq() == seq && row.errorCode() != null)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("The failed"
                        + " record isn't in " + RecordsTable.DEFAULT_NAME));
        CommittedRecords.Failed placed = CommittedRecords.failed(records,
                itemId, storage.keys()).stream()
                .filter(failed -> failed.seq() == seq)
                .findFirst().orElse(null);
        byte[] content = correction.open(storage.keys());
        RecordSplit split = new RecordSplit(original.recordId(), itemId, seq,
                original.sourceOffset(), original.sourceLength(),
                original.format(), original.contentType(), content.length, 1,
                null, original.origin(), null, null,
                placed == null ? -1 : placed.message(),
                placed == null ? null : placed.messageRef(),
                placed == null ? null : placed.messageReplaces(), null);
        String version = version(split);
        Optional<RecordConverter> found = converters.get(split.format());
        StageOutput<Prepared> made;
        if (found.isEmpty())
        {
            made = output(new Prepared(split, content,
                    Representation.raw(split.format()), null, -1, null),
                    digest(content, null, "as-split"));
        }
        else if (content.length > settings.maxBytes())
        {
            made = output(new Prepared(split, content,
                    Representation.raw(split.format()),
                    ErrorCode.CONVERT_TOO_LARGE, 0, null),
                    digest(content, ErrorCode.CONVERT_TOO_LARGE, null));
        }
        else
        {
            RecordConverter converter = found.get();
            int taken = budget.acquire((long) Math.ceil(
                    content.length * converter.memoryFactor()));
            try
            {
                made = convert(split, converter, content);
            }
            finally
            {
                budget.release(taken);
            }
        }
        return new Corrected<>(new StageOutput<>(made.digest(), made.failed(),
                made.value().correcting(original.recordId()),
                made.errorCode()), version);
    }

    /**
     * Converts a record, or decides to store it as split.
     *
     * @param split   the record's message
     * @param attempt how many workers have claimed it
     * @return the record converted, or to be stored as split with the
     *         reason
     * @throws InterruptedException if interrupted
     */
    private StageOutput<Prepared> prepare(RecordSplit split, int attempt)
            throws InterruptedException
    {
        if (split.errorCode() != null)
        {
            return asSplit(split, splitError(split.errorCode()),
                    split.errorAt());
        }
        Optional<RecordConverter> found = converters.get(split.format());
        if (found.isEmpty())
        {
            return asSplit(split, null, null);
        }
        if (attempt >= SKIP_ON_ATTEMPT)
        {
            Telemetry.get().auditWarn("convert.record_skipped",
                    Fields.of(Attr.ITEM_ID, split.itemId())
                            .with(Attr.SEQ, split.seq())
                            .with(Attr.FORMAT, split.format())
                            .with(Attr.ERROR_CODE, ErrorCode.CONVERT_SKIPPED));
            return asSplit(split, ErrorCode.CONVERT_SKIPPED, null);
        }
        if (split.plaintextLength() > settings.maxBytes())
        {
            return asSplit(split, ErrorCode.CONVERT_TOO_LARGE, null);
        }
        RecordConverter converter = found.get();
        int taken = budget.acquire((long) Math.ceil(
                split.plaintextLength() * converter.memoryFactor()));
        try
        {
            byte[] raw = reader.readRecord(split,
                    (int) Math.min(Integer.MAX_VALUE, settings.maxBytes()));
            return convert(split, converter, raw);
        }
        finally
        {
            budget.release(taken);
        }
    }

    /**
     * Converts a record; if that fails, keeps it as split, marked with the
     * error.
     *
     * @param split     the record's message
     * @param converter its format's converter
     * @param raw       its bytes, as split; wiped once converted
     * @return the output
     */
    private StageOutput<Prepared> convert(RecordSplit split,
            RecordConverter converter, byte[] raw)
    {
        try
        {
            byte[] json = converter.convert(raw, config(split.format()));
            Arrays.fill(raw, (byte) 0);
            Representation representation =
                    Representation.json(converter.mapping());
            return output(new Prepared(split, json, representation, null, -1,
                    null), digest(json, null, converter.mapping()));
        }
        catch (ConversionException failure)
        {
            SourcePosition at = lineAndColumn(split, raw,
                    failure.position());
            return output(new Prepared(split, raw,
                    Representation.raw(split.format()), failure.code(),
                    failure.position(), at),
                    digest(raw, failure.code(), null));
        }
        catch (RuntimeException bug)
        {
            // A converter bug: fail the record, not the batch. Only the class
            // name is logged; a message could quote the record.
            LOG.warn("convert.unexpected format={} error={}", split.format(),
                    bug.getClass().getName());
            return output(new Prepared(split, raw,
                    Representation.raw(split.format()),
                    ErrorCode.CONVERT_FAILED, 0, null),
                    digest(raw, ErrorCode.CONVERT_FAILED, null));
        }
    }

    /**
     * A record to store as split, read again when it is committed. Its
     * digest is of its record ID, a keyed hash of its bytes, so it isn't
     * read here.
     *
     * @param split   its message
     * @param error   why; null if its format has no converter
     * @param errorAt where in the item it failed; null if unknown
     * @return the output
     */
    private static StageOutput<Prepared> asSplit(RecordSplit split,
            ErrorCode error, SourcePosition errorAt)
    {
        String digest = digest(
                split.recordId().getBytes(StandardCharsets.UTF_8), error,
                "as-split");
        return output(new Prepared(split, null,
                Representation.raw(split.format()), error,
                error == null ? -1 : 0, errorAt), digest);
    }

    /**
     * Wraps a prepared record as the worker's output.
     *
     * @param prepared the record
     * @param digest   its output's digest
     * @return the output, failed if it has an error
     */
    private static StageOutput<Prepared> output(Prepared prepared,
            String digest)
    {
        return new StageOutput<>(digest, prepared.error() != null, prepared,
                prepared.error() == null ? null : prepared.error().name());
    }

    /**
     * The splitter's error, as a code. One this version doesn't know (a
     * newer splitter's) becomes {@code PARSE_UNRECOGNISED}, so the record is
     * still stored and announced as failed.
     *
     * @param code the code in the message
     * @return the code
     */
    static ErrorCode splitError(String code)
    {
        try
        {
            return ErrorCode.valueOf(code);
        }
        catch (IllegalArgumentException unknown)
        {
            return ErrorCode.PARSE_UNRECOGNISED;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * One data file, appended to {@code records}; the table's commit
     * retries cover the other workers appending at the same time.
     *
     * @return {@code record.stored} for each record converted; null for
     *         one that failed, which isn't sent on (STAGES.md §15.1)
     */
    @Override
    public List<ConnectorMessage> commit(List<StageOutput<Prepared>> outputs)
            throws InterruptedException
    {
        Table records = storage.records();
        RecordBatchWriter writer = new RecordBatchWriter(records,
                storage.keys(), storage.kekRef(), storage.ids(),
                clock.instant());
        List<WrittenRecord> written = new ArrayList<>(outputs.size());
        DataFile file;
        try
        {
            for (StageOutput<Prepared> output : outputs)
            {
                written.add(write(writer, output.value()));
            }
            file = writer.finish();
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        finally
        {
            writer.close();
        }
        AppendFiles append = records.newAppend();
        append.appendFile(file);
        append.commit();
        StorageRef ref = new StorageRef(settings.catalogName(),
                settings.namespace(), RecordsTable.DEFAULT_NAME,
                writer.ingestedDate(), records.currentSnapshot().snapshotId(),
                writer.kekRef());
        List<ConnectorMessage> messages = new ArrayList<>(written.size());
        for (int index = 0; index < written.size(); index++)
        {
            WrittenRecord record = written.get(index);
            if (record.error() == null)
            {
                Prepared prepared = outputs.get(index).value();
                messages.add(message(record, ref,
                        version(prepared.split()), prepared.correctionOf()));
            }
            else
            {
                // Kept in records and in the fail store, and not sent on
                // (STAGES.md §15.1).
                messages.add(null);
                Telemetry.get().auditWarn("record.failed",
                        Fields.of(Attr.ITEM_ID, record.itemId())
                                .with(Attr.SEQ, record.seq())
                                .with(Attr.FORMAT, record.format())
                                .with(Attr.ERROR_CODE, record.error()));
            }
        }
        return messages;
    }

    /**
     * Writes a record into the batch, with its queue message if it failed.
     *
     * @param writer   the batch
     * @param prepared the record
     * @return the record as written
     * @throws IOException          if it can't be written
     * @throws InterruptedException if interrupted waiting for memory
     */
    private WrittenRecord write(RecordBatchWriter writer, Prepared prepared)
            throws IOException, InterruptedException
    {
        RecordSplit split = prepared.split();
        long end = split.sourceOffset() + split.sourceLength();
        SplitRecord lineage = SplitRecord.ofRange(split.sourceOffset(),
                split.sourceLength(), ResumePoint.at(end))
                .withOrigin(split.origin());
        writer.versions(split.itemId(), split.seq(), version(split),
                split.version());
        if (prepared.correctionOf() != null)
        {
            writer.correction(split.itemId(), split.seq(),
                    prepared.correctionOf());
        }
        if (prepared.error() != null && split.message() >= 0)
        {
            writer.place(split.itemId(), split.seq(), new MessagePlace(
                    split.message(), split.messageRef(),
                    split.messageReplaces()));
        }
        if (prepared.bytes() != null)
        {
            return writer.addBytes(split.itemId(), split.seq(),
                    split.format(), lineage, prepared.bytes(),
                    prepared.representation(), prepared.error(),
                    prepared.errorPosition(), prepared.errorAt());
        }
        int taken = budget.acquire(split.plaintextLength());
        try
        {
            return writer.addBytes(split.itemId(), split.seq(),
                    split.format(), lineage,
                    reader.readRecord(split, Integer.MAX_VALUE - 8),
                    prepared.representation(), prepared.error(),
                    prepared.errorPosition(), prepared.errorAt());
        }
        finally
        {
            budget.release(taken);
        }
    }

    /**
     * The message announcing a stored record.
     *
     * @param record   the record
     * @param ref      where it is stored
     * @param version  the stage's version that made it
     * @param resolves for a correction or retry, the failed record it
     *                 resolves; null otherwise
     * @return {@code record.failed} if it has an error, else
     *         {@code record.stored}
     */
    private static ConnectorMessage message(WrittenRecord record,
            StorageRef ref, String version, String resolves)
    {
        Representation representation = record.representation();
        if (record.error() != null)
        {
            return new RecordFailed(record.recordId(), record.itemId(),
                    record.seq(), record.sourceOffset(),
                    record.sourceLength(), record.format(),
                    representation.contentType(), record.error().name(),
                    record.plaintextLength(), record.chunkCount(), ref,
                    record.origin(), record.errorAt(), version);
        }
        return new RecordStored(record.recordId(), record.itemId(),
                record.seq(), record.sourceOffset(), record.sourceLength(),
                record.format(), representation.contentType(),
                representation.mapping(), record.plaintextLength(),
                record.chunkCount(), ref, record.origin(), version, resolves);
    }

    /**
     * A completed item's output, rebuilt as the stage first sent it, for a
     * replay (STAGES.md §13.2): a {@code record.stored} for each record in
     * {@code records} that passed, in order, then its
     * {@code item.converted}; failed records are in the fail store, and
     * aren't sent on (§15.1). Each record names the table's
     * current snapshot rather than the one it was stored in, which reads
     * the same.
     *
     * @param itemId      the item
     * @param records     the {@code records} table, refreshed
     * @param catalog     the catalog's name, as messages give it
     * @param namespace   the table's namespace
     * @param intake      the item's intake, from its {@code item.split}
     * @param done        its records stored, from the registry
     * @param failed      those stored in their original form
     * @param completedAt when the stage completed it, from the registry
     * @return the messages
     */
    public static List<ConnectorMessage> replayMessages(UUID itemId,
            Table records, String catalog, String namespace, String intake,
            long done, long failed, Instant completedAt)
    {
        return replayMessages(itemId, records, catalog, namespace, intake,
                done, failed, completedAt, 1);
    }

    /**
     * A completed item's output, as {@link #replayMessages(UUID, Table,
     * String, String, String, long, long, Instant)} rebuilds it, its
     * {@code item.converted} numbered as the stage's last completion of it
     * (STAGES.md §15.6e).
     *
     * @param itemId      the item
     * @param records     the {@code records} table, refreshed
     * @param catalog     the catalog's name, as messages give it
     * @param namespace   the table's namespace
     * @param intake      the item's intake, from its {@code item.split}
     * @param done        its records stored, from the registry
     * @param failed      those stored in their original form
     * @param completedAt when the stage completed it, from the registry
     * @param completion  its last completion's number, from the registry
     * @return the messages
     */
    public static List<ConnectorMessage> replayMessages(UUID itemId,
            Table records, String catalog, String namespace, String intake,
            long done, long failed, Instant completedAt, int completion)
    {
        long snapshotId = records.currentSnapshot() == null ? -1
                : records.currentSnapshot().snapshotId();
        List<ConnectorMessage> messages = new ArrayList<>();
        Set<String> versions = new LinkedHashSet<>();
        for (CommittedRecords.Row row : CommittedRecords.all(records, itemId))
        {
            if (row.errorCode() != null)
            {
                // In the fail store, and not sent on (STAGES.md §15.1).
                continue;
            }
            messages.add(recordMessage(itemId, row, new StorageRef(catalog,
                    namespace, RecordsTable.DEFAULT_NAME, row.ingestedDate(),
                    snapshotId, row.kekRef())));
            if (row.stageVersion() != null)
            {
                versions.add(row.stageVersion());
            }
        }
        // Records made from operators' corrections that passed, after the
        // originals; each resolves a failure (STAGES.md §15.6c).
        Set<Long> corrected = new java.util.TreeSet<>();
        for (CommittedRecords.Row row : CommittedRecords.corrections(records,
                itemId))
        {
            if (row.errorCode() == null)
            {
                corrected.add(row.seq());
                messages.add(recordMessage(itemId, row, new StorageRef(
                        catalog, namespace, RecordsTable.DEFAULT_NAME,
                        row.ingestedDate(), snapshotId, row.kekRef())));
            }
        }
        messages.add(new ItemConverted(itemId, intake, done,
                failed - corrected.size(), completedAt,
                List.copyOf(versions), completion));
        return messages;
    }

    /**
     * A stored record's {@code record.stored} or {@code record.failed},
     * rebuilt from its row of {@code records} as the stage first sent it,
     * but for the snapshot it names: for a replay (STAGES.md §13.2).
     *
     * @param itemId its item
     * @param row    its row
     * @param ref    where it is stored: the row's day and KEK, and a
     *               snapshot that holds it
     * @return {@code record.failed} if it has an error, else
     *         {@code record.stored}
     */
    public static ConnectorMessage recordMessage(UUID itemId,
            CommittedRecords.Row row, StorageRef ref)
    {
        if (row.errorCode() != null)
        {
            return new RecordFailed(row.recordId(), itemId, row.seq(),
                    row.sourceOffset(), row.sourceLength(), row.format(),
                    row.contentType(), row.errorCode(), row.plaintextLength(),
                    row.chunkCount(), ref, row.origin(), row.errorAt(),
                    row.stageVersion());
        }
        return new RecordStored(row.recordId(), itemId, row.seq(),
                row.sourceOffset(), row.sourceLength(), row.format(),
                row.contentType(), row.mapping(), row.plaintextLength(),
                row.chunkCount(), ref, row.origin(), row.stageVersion(),
                row.correctionOf());
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code item.converted}, with the stage's counts
     */
    @Override
    public ConnectorMessage itemComplete(ItemProgress progress)
    {
        String intake = progress.total() instanceof ItemSplit split
                ? split.intake() : UNKNOWN_INTAKE;
        return new ItemConverted(progress.itemId(), intake, progress.done(),
                progress.unresolved(), clock.instant(), progress.versions(),
                progress.nextCompletion());
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code item.progress}, with the stage's counts
     */
    @Override
    public ConnectorMessage itemProgress(ItemProgress progress)
    {
        ResultReporter.Facts facts = ResultReporter.Facts.of(
                progress.total());
        return new ItemProgressed(progress.itemId(),
                facts == null ? UNKNOWN_INTAKE : facts.intake(), STAGE,
                progress.done(), progress.passed(), progress.open(),
                progress.declined(), facts == null ? null : facts.failure(),
                facts == null ? null : facts.producerRef(),
                facts == null ? null : facts.replaces(), clock.instant(),
                progress.versions(), progress.completion());
    }

    /**
     * Sends the producer's result, if results are on, as
     * {@code incomplete}: the item's failures at both stages, while one is
     * open (STAGES.md §15.6).
     *
     * @param progress the item and its counts
     * @throws Exception if a table can't be read or a part can't be sent
     */
    @Override
    public void itemProgressed(ItemProgress progress) throws Exception
    {
        report(progress, ItemResult.INCOMPLETE);
    }

    /**
     * Sends the producer's result, if results are on, from the item's
     * {@code item.split} and its failed records in {@code split_records}
     * and {@code records}.
     *
     * @param progress the item and its counts
     * @throws Exception if a table can't be read or a part can't be sent
     */
    @Override
    public void itemCompleted(ItemProgress progress) throws Exception
    {
        report(progress, ItemResult.COMPLETED);
    }

    /**
     * Sends the producer's result, if results are on.
     *
     * @param progress the item and its counts
     * @param status   {@code completed} or {@code incomplete}
     * @throws Exception if a table can't be read or a part can't be sent
     */
    private void report(ItemProgress progress, String status)
            throws Exception
    {
        if (results == null)
        {
            return;
        }
        ResultReporter.Facts facts = ResultReporter.Facts.of(
                progress.total());
        if (facts == null)
        {
            Telemetry.get().auditWarn("convert.result_not_sent",
                    Fields.of(Attr.ITEM_ID, progress.itemId())
                            .with(Attr.REASON, "item_split_unreadable"));
            return;
        }
        Table records = storage.records();
        records.refresh();
        // By sequence number: a row of records names a piece the splitter
        // couldn't parse when it was sent on before 2026-11-08, so it is
        // the same failure as its row of split_records.
        Map<Long, CommittedRecords.Failed> failed = new TreeMap<>();
        long splitFailed = 0;
        if (storage.splitRecords() != null)
        {
            Table splitRecords = storage.splitRecords();
            splitRecords.refresh();
            for (CommittedRecords.Failed piece : CommittedRecords.failed(
                    splitRecords, progress.itemId(), storage.keys()))
            {
                failed.put(piece.seq(), piece);
            }
            // A piece corrected at the split stage came here as a record
            // (STAGES.md §15.6d): counted in done, not a problem.
            for (CommittedRecords.Row row : CommittedRecords.corrections(
                    splitRecords, progress.itemId()))
            {
                if (row.errorCode() == null)
                {
                    failed.remove(row.seq());
                }
            }
            splitFailed = failed.size();
        }
        for (CommittedRecords.Failed record : CommittedRecords.failed(
                records, progress.itemId(), storage.keys()))
        {
            if (failed.put(record.seq(), record) != null)
            {
                splitFailed--;
            }
        }
        // A failure an operator's correction resolved is no longer a
        // problem of the item's (STAGES.md §15.6c).
        for (CommittedRecords.Row row : CommittedRecords.corrections(records,
                progress.itemId()))
        {
            if (row.errorCode() == null)
            {
                failed.remove(row.seq());
            }
        }
        results.report(facts, status, progress.done() + splitFailed,
                List.copyOf(failed.values()),
                ItemResult.COMPLETED.equals(status)
                        ? progress.nextCompletion() : progress.completion());
    }

    /**
     * The format's settings: its defaults with the stage's options,
     * checked by its converter the first time.
     *
     * @param format the format
     * @return the settings
     */
    private SplitConfig config(String format)
    {
        return configs.computeIfAbsent(format, name ->
        {
            SplitConfig config = SplitConfig.defaults(name);
            if (settings.options() != null)
            {
                for (Map.Entry<String, String> option
                        : settings.options().entrySet())
                {
                    config = config.withOption(option.getKey(),
                            option.getValue());
                }
            }
            SplitConfig checked = config;
            converters.get(name).ifPresent(
                    converter -> converter.validate(checked));
            return config;
        });
    }

    /**
     * Where a byte of a record is in the item, as a line and column: its
     * line and column in the record, moved by the record's origin.
     *
     * @param split    the record's message
     * @param raw      its bytes, as split
     * @param position the byte's index in them; kept within them
     * @return the position in the item; null if the record has no origin
     */
    private static SourcePosition lineAndColumn(RecordSplit split, byte[] raw,
            long position)
    {
        if (split.origin() == null)
        {
            return null;
        }
        int offset = (int) Math.max(0, Math.min(position, raw.length));
        return split.origin().toFile(LineCounter.positionOf(raw, offset));
    }

    /**
     * A short digest of the settings that change a record's output, for
     * its version: the format options, in order of name, and the largest
     * record converted. A digest, so no setting's value reaches telemetry.
     *
     * @param settings the settings
     * @return the first 12 hex digits of the SHA-256
     */
    private static String settingsDigest(Settings settings)
    {
        StringBuilder text = new StringBuilder();
        new TreeMap<>(settings.options()).forEach((name, value) ->
                text.append(name).append('=').append(value).append('\0'));
        text.append("max-bytes=").append(settings.maxBytes());
        return digest(text.toString().getBytes(StandardCharsets.UTF_8),
                null, null).substring(0, 12);
    }

    /**
     * The digest of an output: its stored bytes, its error and its mapping,
     * so a different converter's output is a conflict, not a duplicate.
     *
     * @param bytes   the stored bytes, or what stands for them
     * @param error   its error; null if none
     * @param mapping its mapping, or how it is stored; null if none
     * @return the SHA-256, in hex
     */
    private static String digest(byte[] bytes, ErrorCode error,
            String mapping)
    {
        try
        {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            sha.update((error == null ? "-" : error.name())
                    .getBytes(StandardCharsets.UTF_8));
            sha.update((byte) 0);
            sha.update((mapping == null ? "-" : mapping)
                    .getBytes(StandardCharsets.UTF_8));
            sha.update((byte) 0);
            sha.update(bytes);
            return HexFormat.of().formatHex(sha.digest());
        }
        catch (NoSuchAlgorithmException missing)
        {
            throw new IllegalStateException("SHA-256 unavailable", missing);
        }
    }
}
