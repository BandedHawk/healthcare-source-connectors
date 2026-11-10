/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemEnd;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemResult;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.contract.position.LineCounter;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.convert.ConversionBudget;
import com.sdh.healthcare.connector.core.convert.Representation;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.split.ResumePoint;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
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
 * A record stage's work on each record (STAGES.md §17.2): read it from
 * the source table, make its output with the stage's
 * {@link RecordTransform}, store the output in the sink table, and
 * announce it with the stage's record message. A record the transform
 * failed, or chose to store as it came with an error, is kept in the sink
 * and the fail store and not sent (§15.1). Once all of an item's records
 * are handled it sends the stage's item end, or {@code item.progress}
 * while a failure is open, then the producer's result if results are on.
 * <p>
 * A record message naming any table but the source is refused,
 * {@code stage.message_rejected} with reason {@code WRONG_SOURCE}, so the
 * stage reads only where it was told to (§17.7). The transforms running at
 * once share a memory budget, so large records can't run the heap out
 * together. Never logs record content, names or IDs.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RecordStageProcessor
        implements StageProcessor<RecordStageProcessor.Prepared>
{

    /** The intake of an item whose end no longer opens. */
    public static final String UNKNOWN_INTAKE = "unknown";

    /** The reason a message naming another table is refused. */
    public static final String WRONG_SOURCE = "WRONG_SOURCE";

    /**
     * What the digest of a record stored as it came is marked with, so
     * it is never the digest of an output. The convert stage's name for
     * it, kept so its digests didn't change when it moved here.
     */
    static final String AS_IS = "as-split";

    /** Logs bugs in a transform, by class name only. */
    private static final Logger LOG =
            LoggerFactory.getLogger(RecordStageProcessor.class);

    /**
     * A record, ready to store.
     *
     * @param input          the message it came in
     * @param bytes          what to store; null to store it as it came,
     *                       read again when it is committed
     * @param representation what the stored bytes are
     * @param error          why it isn't transformed; null if it is, or
     *                       there was nothing to apply to it
     * @param errorPosition  where in the record it failed, in bytes; -1
     *                       without an error
     * @param errorAt        where in the item it failed; null if unknown
     * @param correctionOf   for an operator's correction, the ID of the
     *                       failed row of the sink it corrects (STAGES.md
     *                       §15.6c); null otherwise
     * @param cause          why it was made again, kept on its row
     *                       (§15.8.7); null for a record made the first
     *                       time, or from a correction
     */
    public record Prepared(RecordInStorage input, byte[] bytes,
            Representation representation, ErrorCode error,
            long errorPosition, SourcePosition errorAt, String correctionOf,
            ConnectorMessage.Cause cause)
    {

        /**
         * The same record, made from a correction.
         *
         * @param of the failed row it corrects
         * @return the record
         */
        Prepared correcting(String of)
        {
            return new Prepared(input, bytes, representation, error,
                    errorPosition, errorAt, of, cause);
        }

        /**
         * The same record, made again for a cause.
         *
         * @param why the cause
         * @return the record
         */
        Prepared causedBy(ConnectorMessage.Cause why)
        {
            return new Prepared(input, bytes, representation, error,
                    errorPosition, errorAt, correctionOf, why);
        }
    }

    /**
     * Where the stage stores what it makes.
     *
     * @param table  the sink table
     * @param keys   wraps the batches' data keys, and unwraps them to read
     *               the failed records' broker IDs back, in every table
     * @param kekRef the KEK the sink's data keys are wrapped with
     * @param ids    makes record IDs with the sink's ID key
     */
    public record Sink(RecordTable table, KeyProvider keys, String kekRef,
            RecordIds ids)
    {
    }

    /**
     * A table before the source whose failures the producer's result
     * reports (STAGES.md §17.5): for the transform stage,
     * {@code split_records}.
     *
     * @param stage the stage whose table it is, such as {@code split}
     * @param table the table
     */
    public record Earlier(String stage, Table table)
    {
    }

    /**
     * The tables a stage reads, stores in and reports from.
     *
     * @param source  where it reads records: the stage before's sink
     * @param sink    where it stores what it makes
     * @param earlier the tables before the source, earliest first, whose
     *                failures its results report; empty for none
     */
    public record Tables(RecordTable source, Sink sink, List<Earlier> earlier)
    {

        /**
         * No earlier tables is none, not null.
         */
        public Tables
        {
            earlier = earlier == null ? List.of() : List.copyOf(earlier);
        }
    }

    /** The stage's own step. */
    private final RecordTransform transform;

    /** The stage's messages. */
    private final StageMessages messages;

    /** Reads records from the source. */
    private final QueueReader reader;

    /** Where records are read, stored and reported from. */
    private final Tables tables;

    /** Heap shared by the transforms running at once. */
    private final ConversionBudget budget;

    /** Sends the producer's results; null if they are off. */
    private final ResultReporter results;

    /** Stamps batches and completions. */
    private final Clock clock;

    /**
     * The records the reconciler is handing in, rebuilt, by item and
     * record ID (see {@link #rebuilding}).
     */
    private final Set<String> rebuilt = ConcurrentHashMap.newKeySet();

    /** Notes a backlog coming off the queue; null if nothing watches. */
    private final BacklogWatch backlog;

    /**
     * Creates the processor.
     *
     * @param transform   the stage's own step
     * @param reader      reads records from the source
     * @param tables      where records are read, stored and reported from
     * @param memoryBytes the heap the transforms running at once may use
     * @param results     sends the producer's results; null if they are off
     * @param clock       stamps batches and completions
     * @param backlog     told of each record that comes off the queue, for
     *                    the reconciler (STAGES.md §9); null if none
     */
    public RecordStageProcessor(RecordTransform transform, QueueReader reader,
            Tables tables, long memoryBytes, ResultReporter results,
            Clock clock, BacklogWatch backlog)
    {
        this.transform = transform;
        this.messages = transform.messages();
        this.reader = reader;
        this.tables = tables;
        this.budget = new ConversionBudget(memoryBytes);
        this.results = results;
        this.clock = clock;
        this.backlog = backlog;
    }

    @Override
    public String stage()
    {
        return messages.stage();
    }

    /**
     * {@inheritDoc}
     *
     * A record off the queue, not one the reconciler rebuilt, is shown to
     * the backlog watch.
     *
     * @return a record for the stage before's record message, the item's
     *         total for its item end or {@code item.progress}, nothing for
     *         any other message or a record of another table
     */
    @Override
    public StageInput classify(ConnectorMessage message)
    {
        if (message instanceof RecordInStorage record
                && record.type() == messages.inputRecord())
        {
            if (!tables.source().names(record.storage()))
            {
                Telemetry.get().auditError("stage.message_rejected",
                        Fields.of(Attr.STAGE, stage())
                                .with(Attr.ERROR_CODE,
                                        ErrorCode.MESSAGE_REJECTED)
                                .with(Attr.REASON, WRONG_SOURCE)
                                .with(Attr.MESSAGE_TYPE,
                                        record.type().wireName()));
                return new StageInput.Unwanted("wrong_source");
            }
            if (backlog != null && !rebuilt.contains(rebuiltKey(record)))
            {
                backlog.received(record);
            }
            // Made under the record's lineage, the stage's first revision
            // of it (STAGES.md §15.8.2).
            return new StageInput.RecordInput(record.recordId(),
                    record.itemId(), record.seq(), record.lineage());
        }
        if (message instanceof ItemEnd end
                && end.type() == messages.inputEnd())
        {
            // An interrupted generation's is an open total, as an
            // item.progress is (STAGES.md §15.8.6). Declined records
            // aren't sent on, so aren't waited for.
            return new StageInput.ItemTotal(end.itemId(), end.passedCount(),
                    end instanceof ItemSplit split && split.interrupted(),
                    end.completion(), end.generation(), end.sameAs());
        }
        if (message instanceof ItemProgressed progressed
                && messages.upstream().equals(progressed.stage()))
        {
            // The stage before has failures open: the records passed so
            // far (STAGES.md §15.6).
            return new StageInput.ItemTotal(progressed.itemId(),
                    progressed.passedCount(), true, progressed.completion(),
                    progressed.generation(), progressed.sameAs());
        }
        return new StageInput.Unwanted("wrong_type");
    }

    /**
     * {@inheritDoc}
     *
     * @return the transform's version for the record
     */
    @Override
    public String version(ConnectorMessage message)
    {
        return transform.version((RecordInStorage) message);
    }

    /**
     * Notes records the reconciler rebuilt from the source and is about to
     * hand in (STAGES.md §9). A rebuilt message doesn't know the queue
     * message its record is in, so if one fails, it is audited,
     * {@code stage.placement_unknown}: for an item that is a message
     * batch, the producer's result (if results are on) names no message
     * for it.
     *
     * @param rebuiltMessages the rebuilt messages
     */
    void rebuilding(List<ConnectorMessage> rebuiltMessages)
    {
        for (ConnectorMessage message : rebuiltMessages)
        {
            rebuilt.add(rebuiltKey((RecordInStorage) message));
        }
    }

    /**
     * Forgets records noted by {@link #rebuilding}, once handed in.
     *
     * @param rebuiltMessages the rebuilt messages
     */
    void rebuilt(List<ConnectorMessage> rebuiltMessages)
    {
        for (ConnectorMessage message : rebuiltMessages)
        {
            rebuilt.remove(rebuiltKey((RecordInStorage) message));
        }
    }

    /**
     * What a rebuilt record is noted by.
     *
     * @param record its message
     * @return its item and record ID
     */
    private static String rebuiltKey(RecordInStorage record)
    {
        return record.itemId() + "/" + record.recordId();
    }

    /**
     * {@inheritDoc}
     *
     * @return the record's output, or the record to be stored as it came,
     *         with the reason
     */
    @Override
    public StageOutput<Prepared> process(ConnectorMessage message,
            int attempt) throws InterruptedException
    {
        RecordInStorage input = (RecordInStorage) message;
        StageOutput<Prepared> output = prepare(input, attempt);
        if (output.failed() && rebuilt.contains(rebuiltKey(input)))
        {
            Telemetry.get().auditWarn("stage.placement_unknown",
                    Fields.of(Attr.STAGE, stage())
                            .with(Attr.ITEM_ID, input.itemId())
                            .with(Attr.SEQ, input.seq())
                            .with(Attr.ERROR_CODE, output.value().error()));
        }
        return output;
    }

    /**
     * {@inheritDoc}
     *
     * Its row of the sink keeps the cause and the run (STAGES.md §15.8.7).
     */
    @Override
    public StageOutput<Prepared> process(ConnectorMessage message,
            int attempt, ConnectorMessage.Cause cause)
            throws InterruptedException
    {
        StageOutput<Prepared> output = process(message, attempt);
        return new StageOutput<>(output.digest(), output.failed(),
                output.value().causedBy(cause), output.errorCode());
    }

    /**
     * {@inheritDoc}
     *
     * @return true: a record stage takes operators' corrections
     */
    @Override
    public boolean corrects()
    {
        return true;
    }

    /**
     * Makes the output of an operator's correction of a record that failed
     * here (STAGES.md §15.6c): the corrected record, in place of the one
     * in the source, made as the record was, with its place in the item,
     * its origin and its message taken from the failed row of the sink.
     * Its output is a new row there, marked {@code correction_of} the
     * failed one; if it passes, it is sent on as any record.
     *
     * @param correction the correction
     * @return the output, and the version it is made with
     * @throws InterruptedException  if interrupted waiting for memory
     * @throws IllegalStateException if the failed record isn't in the sink
     */
    @Override
    public Corrected<Prepared> correct(CorrectionStore.Pending correction)
            throws InterruptedException
    {
        UUID itemId = correction.itemId();
        long seq = correction.seq();
        Sink sink = tables.sink();
        Table table = sink.table().table();
        table.refresh();
        // The failure's own generation (STAGES.md §15.8); every one for a
        // correction submitted before lineages were kept.
        int generation = correction.lineage() == null ? 0
                : correction.lineage().generation();
        CommittedRecords.Row original = CommittedRecords.all(table, itemId,
                generation).stream()
                .filter(row -> row.seq() == seq && row.errorCode() != null)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("The failed"
                        + " record isn't in " + sink.table().name()));
        CommittedRecords.Failed placed = CommittedRecords.failed(table,
                itemId, generation, sink.keys()).stream()
                .filter(failed -> failed.seq() == seq)
                .findFirst().orElse(null);
        byte[] content = correction.open(sink.keys());
        RecordInStorage input = SourceInputs.message(messages, itemId,
                original, null, content.length, 1, null, null, null,
                correction.lineage(),
                placed == null ? -1 : placed.message(),
                placed == null ? null : placed.messageRef(),
                placed == null ? null : placed.messageReplaces());
        // At the record's next revision here (STAGES.md §15.8.5), if the
        // correction has one, stored and sent as such.
        String version = version(input);
        RecordTransform.Plan plan = transform.plan(input, 1);
        StageOutput<Prepared> made;
        if (plan instanceof RecordTransform.Plan.Apply apply)
        {
            int taken = budget.acquire(apply.memoryBytes());
            try
            {
                made = applyTo(input, content);
            }
            finally
            {
                budget.release(taken);
            }
        }
        else
        {
            RecordTransform.Plan.AsIs asIs = (RecordTransform.Plan.AsIs) plan;
            made = output(new Prepared(input, content,
                    transform.asIs(input), asIs.error(),
                    asIs.error() == null ? -1 : 0, asIs.errorAt(), null,
                    null), digest(content, asIs.error(),
                            asIs.error() == null ? AS_IS : null));
        }
        return new Corrected<>(new StageOutput<>(made.digest(), made.failed(),
                made.value().correcting(original.recordId()),
                made.errorCode()), version);
    }

    /**
     * Makes a record's output, or decides to store it as it came.
     *
     * @param input   the record's message
     * @param attempt how many workers have claimed it
     * @return the output, or the record to be stored as it came, with the
     *         reason
     * @throws InterruptedException if interrupted waiting for memory
     */
    private StageOutput<Prepared> prepare(RecordInStorage input, int attempt)
            throws InterruptedException
    {
        RecordTransform.Plan plan = transform.plan(input, attempt);
        if (plan instanceof RecordTransform.Plan.AsIs asIs)
        {
            return asIs(input, asIs.error(), asIs.errorAt());
        }
        RecordTransform.Plan.Apply apply = (RecordTransform.Plan.Apply) plan;
        int taken = budget.acquire(apply.memoryBytes());
        try
        {
            byte[] raw = reader.readRecord(input,
                    (int) Math.min(Integer.MAX_VALUE, transform.maxBytes()));
            return applyTo(input, raw);
        }
        finally
        {
            budget.release(taken);
        }
    }

    /**
     * Applies the transform to a record; if that fails, keeps the record
     * as it came, marked with the error.
     *
     * @param input the record's message
     * @param raw   its bytes, as it came; wiped once transformed
     * @return the output
     */
    private StageOutput<Prepared> applyTo(RecordInStorage input, byte[] raw)
    {
        RecordTransform.Result result;
        try
        {
            result = transform.apply(input, raw);
        }
        catch (RuntimeException bug)
        {
            // A bug in the transform: fail the record, not the batch. Only
            // the class name is logged; a message could quote the record.
            LOG.warn("stage.unexpected stage={} format={} error={}", stage(),
                    input.format(), bug.getClass().getName());
            ErrorCode code = transform.unexpected();
            return output(new Prepared(input, raw, transform.asIs(input),
                    code, 0, null, null, null), digest(raw, code, null));
        }
        return switch (result)
        {
            case RecordTransform.Result.Made made ->
            {
                Arrays.fill(raw, (byte) 0);
                yield output(new Prepared(input, made.bytes(),
                        made.representation(), null, -1, null, null, null),
                        digest(made.bytes(), null,
                                made.representation().mapping()));
            }
            case RecordTransform.Result.Failed failed -> output(
                    new Prepared(input, raw, transform.asIs(input),
                            failed.code(), failed.position(),
                            lineAndColumn(input, raw, failed.position()),
                            null, null),
                    digest(raw, failed.code(), null));
        };
    }

    /**
     * A record to store as it came, read again when it is committed. Its
     * digest is of its record ID, a keyed hash of its bytes, so it isn't
     * read here.
     *
     * @param input   its message
     * @param error   why; null if there was nothing to apply to it
     * @param errorAt where in the item it failed; null if unknown
     * @return the output
     */
    private StageOutput<Prepared> asIs(RecordInStorage input,
            ErrorCode error, SourcePosition errorAt)
    {
        String digest = digest(
                input.recordId().getBytes(StandardCharsets.UTF_8), error,
                AS_IS);
        return output(new Prepared(input, null, transform.asIs(input), error,
                error == null ? -1 : 0, errorAt, null, null), digest);
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
     * {@inheritDoc}
     * <p>
     * One data file, appended to the sink; the table's commit retries
     * cover the other workers appending at the same time.
     *
     * @return the stage's record message for each record that passed;
     *         null for one that failed, which isn't sent on (STAGES.md
     *         §15.1)
     */
    @Override
    public List<ConnectorMessage> commit(List<StageOutput<Prepared>> outputs)
            throws InterruptedException
    {
        Sink sink = tables.sink();
        Table table = sink.table().table();
        RecordBatchWriter writer = new RecordBatchWriter(table, sink.keys(),
                sink.kekRef(), sink.ids(), clock.instant());
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
        AppendFiles append = table.newAppend();
        append.appendFile(file);
        append.commit();
        StorageRef ref = sink.table().ref(writer.ingestedDate(),
                table.currentSnapshot().snapshotId(), writer.kekRef());
        List<ConnectorMessage> sent = new ArrayList<>(written.size());
        for (int index = 0; index < written.size(); index++)
        {
            WrittenRecord record = written.get(index);
            if (record.error() == null)
            {
                Prepared prepared = outputs.get(index).value();
                sent.add(message(record, ref, prepared.input(),
                        version(prepared.input()), prepared.correctionOf()));
            }
            else
            {
                // Kept in the sink and in the fail store, and not sent on
                // (STAGES.md §15.1).
                sent.add(null);
                Telemetry.get().auditWarn("record.failed",
                        Fields.of(Attr.STAGE, stage())
                                .with(Attr.ITEM_ID, record.itemId())
                                .with(Attr.SEQ, record.seq())
                                .with(Attr.FORMAT, record.format())
                                .with(Attr.ERROR_CODE, record.error()));
            }
        }
        return sent;
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
        RecordInStorage input = prepared.input();
        UUID itemId = input.itemId();
        long seq = input.seq();
        long end = input.sourceOffset() + input.sourceLength();
        SplitRecord place = SplitRecord.ofRange(input.sourceOffset(),
                input.sourceLength(), ResumePoint.at(end))
                .withOrigin(input.origin());
        writer.versions(itemId, seq, version(input), input.version());
        writer.lineage(itemId, seq, input.lineage());
        // Each given, or cleared, for every record: two of one place may
        // be in a batch.
        writer.correction(itemId, seq, prepared.correctionOf());
        if (prepared.cause() != null)
        {
            writer.cause(itemId, seq, prepared.cause().kind(),
                    prepared.cause().runId());
        }
        else
        {
            writer.cause(itemId, seq, prepared.correctionOf() == null ? null
                    : ConnectorMessage.Cause.CORRECTION, null);
        }
        if (prepared.error() != null && input.message() >= 0)
        {
            writer.place(itemId, seq, new MessagePlace(input.message(),
                    input.messageRef(), input.messageReplaces()));
        }
        if (prepared.bytes() != null)
        {
            return writer.addBytes(itemId, seq, input.format(), place,
                    prepared.bytes(), prepared.representation(),
                    prepared.error(), prepared.errorPosition(),
                    prepared.errorAt());
        }
        int taken = budget.acquire(input.plaintextLength());
        try
        {
            return writer.addBytes(itemId, seq, input.format(), place,
                    reader.readRecord(input, Integer.MAX_VALUE - 8),
                    prepared.representation(), prepared.error(),
                    prepared.errorPosition(), prepared.errorAt());
        }
        finally
        {
            budget.release(taken);
        }
    }

    /**
     * The message announcing a stored record: one stored without an
     * error, since a failed record is kept in the fail store and not sent
     * (STAGES.md §15.1). The queue message its input started in is carried
     * on (§17.4).
     *
     * @param record   the record
     * @param ref      where it is stored
     * @param input    the message it came in
     * @param version  the stage's version that made it
     * @param resolves for a correction or retry, the failed record it
     *                 resolves; null otherwise
     * @return the stage's record message, at the input's lineage
     */
    private RecordInStorage message(WrittenRecord record, StorageRef ref,
            RecordInStorage input, String version, String resolves)
    {
        Representation representation = record.representation();
        return messages.record(record.recordId(), record.itemId(),
                record.seq(), record.sourceOffset(), record.sourceLength(),
                record.format(), representation.contentType(),
                representation.mapping(), record.plaintextLength(),
                record.chunkCount(), ref, record.origin(), version, resolves,
                null, input.message(), input.messageRef(),
                input.messageReplaces())
                .withLineage(input.lineage(), false);
    }

    /**
     * {@inheritDoc}
     *
     * @return the stage's item end, with its counts and what it carries on
     *         from the end that reached it (STAGES.md §17.4)
     */
    @Override
    public ConnectorMessage itemComplete(ItemProgress progress)
    {
        ResultReporter.Facts facts = ResultReporter.Facts.of(
                progress.total());
        boolean resultsSent = results != null && facts != null
                || progress.total() instanceof ItemEnd end
                        && end.resultsSent();
        return messages.itemEnd(progress.itemId(),
                facts == null ? UNKNOWN_INTAKE : facts.intake(),
                progress.done(), progress.unresolved(), clock.instant(),
                progress.versions(), progress.nextCompletion(),
                progress.generation(), facts == null ? null : facts.sameAs(),
                facts == null ? null : facts.producerRef(),
                facts == null ? null : facts.replaces(),
                facts == null ? null : facts.failure(), resultsSent);
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
                facts == null ? UNKNOWN_INTAKE : facts.intake(), stage(),
                progress.done(), progress.passed(), progress.open(),
                progress.declined(), facts == null ? null : facts.failure(),
                facts == null ? null : facts.producerRef(),
                facts == null ? null : facts.replaces(), clock.instant(),
                progress.versions(), progress.completion(),
                progress.generation(), facts == null ? null : facts.sameAs());
    }

    /**
     * Sends the producer's result, if results are on, as
     * {@code incomplete}: the item's failures at this stage and those
     * before it, while one is open (STAGES.md §15.6).
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
     * Sends the producer's result, if results are on, from the item's end
     * and its failed records in the sink and every table before it.
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
     * Sends the producer's result, if results are on. A stage before that
     * sent one already is audited, {@code stage.results_twice}: results
     * are meant to be on at one stage of a pipeline, its last (STAGES.md
     * §17.5).
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
            Telemetry.get().auditWarn("stage.result_not_sent",
                    Fields.of(Attr.STAGE, stage())
                            .with(Attr.ITEM_ID, progress.itemId())
                            .with(Attr.REASON, "item_end_unreadable"));
            return;
        }
        if (ItemResult.COMPLETED.equals(status)
                && progress.total() instanceof ItemEnd end
                && end.resultsSent())
        {
            Telemetry.get().auditWarn("stage.results_twice",
                    Fields.of(Attr.STAGE, stage())
                            .with(Attr.ITEM_ID, progress.itemId()));
        }
        Failures failures = failures(progress.itemId(),
                progress.generation());
        results.report(facts, status, progress.done() + failures.before(),
                failures.list(), ItemResult.COMPLETED.equals(status)
                        ? progress.nextCompletion() : progress.completion());
    }

    /**
     * An item's failures, for its result.
     *
     * @param list   the failed records, by sequence number
     * @param before how many of them failed before this stage, so never
     *               reached it: not counted in its records handled
     */
    private record Failures(List<ResultReporter.Failure> list, long before)
    {
    }

    /**
     * Gathers an item's failed records from every table, earliest first,
     * less those an operator's correction resolved (STAGES.md §15.6c,
     * §15.6d). Each is put down to the stage whose table it is in. A
     * record failed with the same code in a later table is the same
     * failure carried on (rows stored before 2026-11-08), so keeps the
     * earlier stage.
     *
     * @param itemId     the item
     * @param generation the report's generation (STAGES.md §15.8): its
     *                   first may be of a later one, if the first was
     *                   interrupted
     * @return the failures
     */
    private Failures failures(UUID itemId, int generation)
    {
        Map<Long, ResultReporter.Failure> failed = new TreeMap<>();
        List<Earlier> before = new ArrayList<>(tables.earlier());
        before.add(new Earlier(messages.upstream(),
                tables.source().table()));
        for (Earlier table : before)
        {
            gather(failed, table, itemId, generation);
        }
        Set<Long> earlier = new HashSet<>(failed.keySet());
        Set<Long> here = gather(failed, new Earlier(stage(),
                tables.sink().table().table()), itemId, generation);
        earlier.removeAll(here);
        earlier.retainAll(failed.keySet());
        return new Failures(List.copyOf(failed.values()), earlier.size());
    }

    /**
     * Adds a table's failed records of an item, and takes out those its
     * corrections resolved.
     *
     * @param failed     the failures so far, by sequence number
     * @param from       the table, and its stage
     * @param itemId     the item
     * @param generation the generation
     * @return the sequence numbers of the table's failed records
     */
    private Set<Long> gather(Map<Long, ResultReporter.Failure> failed,
            Earlier from, UUID itemId, int generation)
    {
        Table table = from.table();
        table.refresh();
        Set<Long> seqs = new HashSet<>();
        for (CommittedRecords.Failed record : CommittedRecords.failed(table,
                itemId, generation, tables.sink().keys()))
        {
            ResultReporter.Failure held = failed.get(record.seq());
            String stage = held != null
                    && held.failed().errorCode().equals(record.errorCode())
                    ? held.stage() : from.stage();
            failed.put(record.seq(), new ResultReporter.Failure(stage,
                    record));
            seqs.add(record.seq());
        }
        for (CommittedRecords.Row row : CommittedRecords.corrections(table,
                itemId, generation))
        {
            if (row.errorCode() == null)
            {
                failed.remove(row.seq());
            }
        }
        return seqs;
    }

    /**
     * Where a byte of a record is in the item, as a line and column: its
     * line and column in the record, moved by the record's origin.
     *
     * @param input    the record's message
     * @param raw      its bytes, as it came
     * @param position the byte's index in them; kept within them
     * @return the position in the item; null if the record has no origin
     */
    private static SourcePosition lineAndColumn(RecordInStorage input,
            byte[] raw, long position)
    {
        if (input.origin() == null)
        {
            return null;
        }
        int offset = (int) Math.max(0, Math.min(position, raw.length));
        return input.origin().toFile(LineCounter.positionOf(raw, offset));
    }

    /**
     * The digest of an output: its stored bytes, its error and its mapping,
     * so a different transform's output is a conflict, not a duplicate.
     *
     * @param bytes   the stored bytes, or what stands for them
     * @param error   its error; null if none
     * @param mapping its mapping, or how it is stored; null if none
     * @return the SHA-256, in hex
     */
    static String digest(byte[] bytes, ErrorCode error, String mapping)
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
