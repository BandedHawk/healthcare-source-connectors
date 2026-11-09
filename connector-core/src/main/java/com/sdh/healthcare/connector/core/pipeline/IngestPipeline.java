/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.RecordCrypto;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.position.LineCounter;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.checkpoint.LeasedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.TableCommitter;
import com.sdh.healthcare.connector.core.convert.Representation;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.landing.LandedRefs;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.SplitRecord;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.split.SplitSession;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.EncryptedRecord;
import com.sdh.healthcare.connector.core.storage.RecordBatchWriter.WrittenRecord;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * The split stage's work on one item (design §4, §8.3, §9.2, STAGES.md §8):
 * read, split, hash and encrypt, write, commit, announce. One item at a
 * time per instance, under the item's lease, which the caller holds; other
 * pods split other items into the same table at the same time.
 * <p>
 * For each item:
 * <ol>
 * <li>A new item gets a first checkpoint ({@code SPLITTING}) written to
 * its row before anything else; an item with a checkpoint is resumed from
 * it, after settling a batch that was being committed.</li>
 * <li>On resume, the messages of records committed but not acknowledged
 * are rebuilt from the table's columns and sent again.</li>
 * <li>Records are encrypted on a worker pool (records of one chunk) or on
 * this thread (larger ones) and written in sequence order. A batch is
 * committed when it reaches {@code commitMaxRecords},
 * {@code commitMaxBytes} or {@code commitMaxInterval}: the checkpoint
 * after it is written as pending, the data file is appended, and the
 * pending checkpoint becomes the item's.</li>
 * <li>The batch's {@code record.split} messages are sent in the background
 * while the next batch is split. The next commit waits for that and
 * records the acknowledged sequence number as {@code publishedSeq}.</li>
 * <li>At the end, the checkpoint moves to {@code ARCHIVING}.</li>
 * </ol>
 * <p>
 * Errors: a {@link SplitException} means the item can't be split (fail the
 * item). Transient storage and broker errors are retried as the
 * {@link RetryPolicy} says. A lost lease, a tampered checkpoint and
 * Iceberg's {@code CommitStateUnknownException} are not caught: the item
 * is tried again from its checkpoint.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class IngestPipeline implements AutoCloseable
{

    /**
     * Restarts without progress after which the item is failed,
     * {@code RESUME_NO_PROGRESS}, rather than resumed again, so an item that
     * stops a worker every time can't keep being taken up (design §9.2).
     * One or two restarts are allowed for anything else, such as a
     * redeployment or a lost lease. An attempt that ends because a service
     * was down isn't counted ({@link Outage}).
     */
    public static final int FAIL_AFTER_RESTARTS = 3;

    /**
     * The outcome of the split phase. The checkpoint is in state
     * {@code ARCHIVING}.
     *
     * @param itemId      the file's id
     * @param recordCount how many records it was split into
     * @param checkpoint  the checkpoint after the split phase
     */
    public record SplitResult(UUID itemId, long recordCount,
            Checkpoint checkpoint)
    {
    }

    /** The pipeline's settings. */
    private final PipelineSettings settings;

    /** The {@code split_records} table. */
    private final Table table;

    /** The checkpoint of each item. */
    private final ItemCheckpoints checkpoints;

    /** Wraps each batch's data key. */
    private final KeyProvider keys;

    /** Makes record ids. */
    private final RecordIds ids;

    /** The splitters, by format. */
    private final SplitterRegistry splitters;

    /** Signs and encrypts messages. */
    private final MessageSealer sealer;

    /** Sends messages to the broker. */
    private final MessagePublisher publisher;

    /** The time, for checkpoints, batches and ids. */
    private final Clock clock;

    /** The table's namespace, for storage references. */
    private final String namespace;

    /** The table's name without its catalog and namespace. */
    private final String tableName;

    /** Reads, converts and encrypts records of one chunk. */
    private final ExecutorService workers;

    /**
     * Seals messages. It has its own pool: sharing the encryption pool let
     * each batch's 10,000 sealing jobs queue ahead of the next batch's
     * record encryption, so splitting stalled while the previous batch was
     * sealed (found by the throughput tests, design §13).
     */
    private final ExecutorService sealers;

    /** Publishes each committed batch's messages, one batch at a time. */
    private final ExecutorService publishing;

    /** Test hooks between the steps of a batch; none by default. */
    private PipelineHooks hooks = PipelineHooks.NONE;

    /**
     * Creates the pipeline and its thread pools, checking that the split and
     * conversion settings suit the format.
     *
     * @param settings     the pipeline's settings
     * @param recordsTable the {@code split_records} table
     * @param checkpoints  the checkpoint of each item
     * @param keys         wraps each batch's data key
     * @param ids          makes record ids and keys
     * @param splitters    the splitters, by format
     * @param sealer       signs and encrypts messages
     * @param publisher    sends messages to the broker
     * @param clock        the time
     */
    public IngestPipeline(PipelineSettings settings, Table recordsTable,
            ItemCheckpoints checkpoints, KeyProvider keys, RecordIds ids,
            SplitterRegistry splitters, MessageSealer sealer,
            MessagePublisher publisher, Clock clock)
    {
        this.settings = settings;
        this.table = recordsTable;
        this.checkpoints = checkpoints;
        this.keys = keys;
        this.ids = ids;
        this.splitters = splitters;
        this.sealer = sealer;
        this.publisher = publisher;
        this.clock = clock;
        String[] namespaceAndTable = namespaceAndTable(recordsTable.name());
        this.namespace = namespaceAndTable[0];
        this.tableName = namespaceAndTable[1];
        splitters.get(settings.format()).validate(settings.split());
        this.workers = Executors.newFixedThreadPool(settings.encryptWorkers(),
                Thread.ofPlatform().name("sdh-encrypt-", 0).daemon(true)
                        .factory());
        this.sealers = Executors.newFixedThreadPool(settings.encryptWorkers(),
                Thread.ofPlatform().name("sdh-seal-", 0).daemon(true)
                        .factory());
        this.publishing = Executors.newSingleThreadExecutor(
                Thread.ofPlatform().name("sdh-publish").daemon(true)
                        .factory());
    }

    /**
     * Sets the test hooks.
     *
     * @param hooks what to run between the steps of a batch
     */
    void setHooks(PipelineHooks hooks)
    {
        this.hooks = hooks;
    }

    /**
     * Reads an item's checkpoint, verified. A batch that was being
     * committed is left out: {@link #split} resolves it at resume.
     *
     * @param itemId the item
     * @return the checkpoint; empty if the item was never started
     */
    public Optional<Checkpoint> checkpoint(UUID itemId)
    {
        return checkpoints.load(itemId)
                .map(ItemCheckpoints.Stored::checkpoint);
    }

    /**
     * Reads what is stored for an item, verified.
     *
     * @param itemId the item
     * @return its checkpoint, any pending one, and when it finished; empty
     *         if it was never started
     */
    public Optional<ItemCheckpoints.Stored> stored(UUID itemId)
    {
        return checkpoints.load(itemId);
    }

    /**
     * Runs the split phase for an item with nothing kept by landing, such
     * as one that isn't a batch of queue messages.
     *
     * @param source the item's bytes
     * @param itemId the item's ID
     * @param lease  the item's lease, held by the caller
     * @return the outcome
     * @throws Exception as {@link #split(SeekableByteSource, UUID,
     *                   LeaseToken, LandedRefs)} does
     */
    public SplitResult split(SeekableByteSource source, UUID itemId,
            LeaseToken lease) throws Exception
    {
        return split(source, itemId, lease, LandedRefs.NONE);
    }

    /**
     * Runs the split phase for a landed item (design §5.3.6, STAGES.md
     * §8): from the start for a new item, or from its checkpoint. Returns
     * once every record is committed and published.
     *
     * @param source the item's bytes
     * @param itemId the item's ID
     * @param lease  the item's lease, held by the caller
     * @param refs   what landing kept about the item, to place each
     *               record in its message of a batch
     * @return the outcome
     * @throws IllegalStateException if the item is archiving or failing,
     *                               which the caller finishes instead
     * @throws SplitException        if the item is too large, can't be
     *                               split, or has stopped workers too often
     * @throws Exception             if storage or the broker fails beyond
     *                               retrying
     */
    public SplitResult split(SeekableByteSource source, UUID itemId,
            LeaseToken lease, LandedRefs refs) throws Exception
    {
        Checkpoint current = resume(itemId, lease).orElse(null);
        if (current != null && current.state() == Checkpoint.State.ARCHIVING)
        {
            return new SplitResult(current.itemId(), current.recordCount(),
                    current);
        }
        if (current != null && current.state() == Checkpoint.State.HELD)
        {
            throw new ItemHeldException(itemId, current.version());
        }
        if (current != null && current.state() != Checkpoint.State.SPLITTING)
        {
            throw new IllegalStateException(
                    "Item " + itemId + " is " + current.state());
        }
        if (current != null)
        {
            if (!current.format().equals(settings.format()))
            {
                throw new IllegalStateException("Split format changed while"
                        + " item " + current.itemId() + " is in progress");
            }
            if (!sameVersion(current))
            {
                hold(current, lease);
            }
            // Counted before any work, as a crash in this attempt (an
            // out-of-memory exit, say) can't record anything.
            current = current.resumed();
            if (current.restarts() >= FAIL_AFTER_RESTARTS)
            {
                throw new SplitException(ErrorCode.RESUME_NO_PROGRESS,
                        current.resumeAt().offset());
            }
            commitCheckpoint(current, lease);
            Telemetry.get().audit("item.resumed",
                    Fields.of(Attr.ITEM_ID, current.itemId())
                            .with(Attr.SEQ, current.nextSeq())
                            .with(Attr.OFFSET, current.resumeAt().offset())
                            .with(Attr.COUNT, current.restarts()));
        }
        else
        {
            if (source.size() > settings.split().limits().maxItemBytes())
            {
                // Rejected before ingestion (design §6.3): no checkpoint.
                throw new SplitException(ErrorCode.LIMIT_ITEM_SIZE, 0);
            }
            current = Checkpoint.start(itemId, itemFingerprint(itemId),
                    settings.format(), source.size(), clock.instant(),
                    settings.version());
            commitCheckpoint(current, lease);
            Telemetry.get().audit("item.started",
                    Fields.of(Attr.ITEM_ID, current.itemId())
                            .with(Attr.FORMAT, current.format())
                            .with(Attr.BYTES, current.itemSize()));
        }
        Checkpoint attempt = current;
        try
        {
            return run(attempt, source, lease, refs);
        }
        catch (Exception failure)
        {
            // An error is a clean end, not a crash: if a service was down,
            // it says nothing about the item, so the restart is taken back.
            if (attempt.restarts() > 0 && Outage.caused(failure))
            {
                notCounted(attempt, failure, lease);
            }
            throw failure;
        }
    }

    /**
     * Tells whether an item part way was begun under this version of the
     * split stage. One begun before versions were kept, or a stage with no
     * version set, is taken as the same, as before.
     *
     * @param current the item's checkpoint
     * @return true if it may be resumed here
     */
    private boolean sameVersion(Checkpoint current)
    {
        return current.version() == null || settings.version() == null
                || current.version().equals(settings.version());
    }

    /**
     * Holds an item begun under another version (STAGES.md §14.3): marks it
     * {@code HELD}, audits {@code split.version_held}, and stops. No restart
     * is counted: nothing is wrong with the item.
     *
     * @param current its checkpoint
     * @param lease   its lease
     * @throws ItemHeldException always
     */
    private void hold(Checkpoint current, LeaseToken lease)
            throws ItemHeldException
    {
        commitCheckpoint(current.withState(Checkpoint.State.HELD), lease);
        Telemetry telemetry = Telemetry.get();
        telemetry.itemHeld();
        telemetry.auditWarn("split.version_held",
                Fields.of(Attr.ITEM_ID, current.itemId())
                        .with(Attr.SEQ, current.nextSeq())
                        .with(Attr.VERSION, settings.version())
                        .with(Attr.STORED_VERSION, current.version()));
        throw new ItemHeldException(current.itemId(), current.version());
    }

    /**
     * Takes back the restart counted for an attempt that ended because a
     * service was down (STAGES.md §12), unless the checkpoint has moved on
     * since: progress has reset the count anyway. A batch being committed
     * is left for resume to settle. If the checkpoint can't be written
     * either, the restart stays counted.
     *
     * @param counted the checkpoint the attempt started from
     * @param failure why the attempt ended; a failure to take the restart
     *                back is added to it
     * @param lease   the item's lease
     */
    private void notCounted(Checkpoint counted, Exception failure,
            LeaseToken lease)
    {
        try
        {
            Checkpoint now = checkpoints.load(counted.itemId())
                    .map(ItemCheckpoints.Stored::checkpoint).orElse(null);
            if (now == null || now.state() != Checkpoint.State.SPLITTING
                    || now.restarts() != counted.restarts()
                    || now.nextSeq() != counted.nextSeq())
            {
                return;
            }
            Checkpoint back = now.notCounted();
            checkpoints.replace(back, lease);
            Telemetry.get().audit("item.restart_not_counted",
                    Fields.of(Attr.ITEM_ID, back.itemId())
                            .with(Attr.COUNT, back.restarts()));
        }
        catch (RuntimeException unwritten)
        {
            failure.addSuppressed(unwritten);
        }
    }

    /**
     * Reads an item's checkpoint for a resume, first settling a batch that
     * was being committed when a worker stopped (STAGES.md §8): its
     * checkpoint is kept if the batch's last record is in the table, and
     * dropped if not.
     *
     * @param itemId the item
     * @param lease  the item's lease
     * @return the checkpoint; empty if the item was never started
     */
    Optional<Checkpoint> resume(UUID itemId, LeaseToken lease)
    {
        Optional<ItemCheckpoints.Stored> stored = checkpoints.load(itemId);
        if (stored.isEmpty())
        {
            return Optional.empty();
        }
        Checkpoint pending = stored.get().pending();
        if (pending == null)
        {
            return Optional.of(stored.get().checkpoint());
        }
        table.refresh();
        Checkpoint settled =
                CommittedRecords.has(table, itemId, pending.committedSeq())
                        ? pending : stored.get().checkpoint();
        checkpoints.save(settled, lease);
        Telemetry.get().audit("batch.settled",
                Fields.of(Attr.ITEM_ID, itemId)
                        .with(Attr.SEQ, settled.committedSeq())
                        .with(Attr.RESULT, settled == pending
                                ? "committed" : "dropped"));
        return Optional.of(settled);
    }

    /**
     * What a landed item's checkpoint holds in place of a fingerprint.
     *
     * @param itemId the item's ID
     * @return {@code item:} and the ID
     */
    public static String itemFingerprint(UUID itemId)
    {
        return "item:" + itemId;
    }

    /**
     * Splits, stores and publishes a file from its checkpoint, then moves
     * the checkpoint to {@code ARCHIVING}.
     *
     * @param start  the checkpoint to start from
     * @param source the item
     * @param lease  the item's lease
     * @param refs   what landing kept about the item
     * @return the outcome
     * @throws Exception if splitting, storage or the broker fails
     */
    private SplitResult run(Checkpoint start, SeekableByteSource source,
            LeaseToken lease, LandedRefs refs) throws Exception
    {
        republish(start, refs);
        Checkpoint current = start.published(start.committedSeq());

        Batch batch = null;
        Future<Long> inFlightPublish = null;
        // Lines and columns, so failures and mapping issues can be found in
        // the file (RecordOrigin).
        try (SplitSession session = open(current, source);
                RecordOrigins origins = new RecordOrigins(source))
        {
            long seq = current.nextSeq();
            Optional<SplitRecord> next;
            while ((next = session.next()).isPresent())
            {
                if (batch == null)
                {
                    batch = new Batch(current.itemId(), current.format(),
                            refs);
                }
                batch.add(seq++, origins.withOrigin(next.get()), source);
                if (batch.full())
                {
                    BatchOutcome outcome = commitBatch(batch, current,
                            inFlightPublish, lease);
                    current = outcome.checkpoint();
                    inFlightPublish = outcome.publish();
                    batch = null;
                }
            }
            if (batch != null)
            {
                BatchOutcome outcome = commitBatch(batch, current,
                        inFlightPublish, lease);
                current = outcome.checkpoint();
                inFlightPublish = outcome.publish();
                batch = null;
            }
            current = current.published(
                    await(inFlightPublish, current.publishedSeq()));
            inFlightPublish = null;
            hooks.beforeEndOfFile();
            current = current.withState(Checkpoint.State.ARCHIVING);
            commitCheckpoint(current, lease);
            Telemetry.get().audit("item.split",
                    Fields.of(Attr.ITEM_ID, current.itemId())
                            .with(Attr.COUNT, current.recordCount()));
            return new SplitResult(current.itemId(), current.recordCount(),
                    current);
        }
        finally
        {
            if (batch != null)
            {
                batch.abandon();
            }
            if (inFlightPublish != null)
            {
                // Let the committed batch finish publishing rather than cut
                // it off: if this file now fails, nothing would publish it
                // again later (a restart does, a failure doesn't).
                try
                {
                    inFlightPublish.get();
                }
                catch (InterruptedException interrupted)
                {
                    Thread.currentThread().interrupt();
                }
                catch (ExecutionException ignored)
                {
                    // Reported by FileProcessor, which announces again
                    // anything committed but not acknowledged.
                }
            }
        }
    }

    /**
     * Opens the splitter at the checkpoint's resume point: message by
     * message for a message batch (design §5.3.16), otherwise over the
     * whole file.
     *
     * @param current the checkpoint
     * @param source  the file or item
     * @return the session
     * @throws IOException    if the source can't be read
     * @throws SplitException if it can't be split from there
     */
    private SplitSession open(Checkpoint current, SeekableByteSource source)
            throws IOException, SplitException
    {
        if (source instanceof LandedItemSource item
                && item.messageIndex().isPresent())
        {
            return splitters.openMessages(current.format(), source,
                    item.messageIndex().get().offsets(), current.resumeAt(),
                    current.nextSeq(), settings.split());
        }
        return splitters.open(current.format(), source, current.resumeAt(),
                current.nextSeq(), settings.split());
    }

    // ---- batches ----

    /**
     * What committing a batch leads to.
     *
     * @param checkpoint the checkpoint committed with the batch
     * @param publish    the publishing of the batch's messages, under way;
     *                   it gives the last sequence number published
     */
    private record BatchOutcome(Checkpoint checkpoint, Future<Long> publish)
    {
    }

    /**
     * One commit batch. Records are added in sequence order; those of one
     * chunk are read and encrypted on the worker pool.
     */
    private final class Batch
    {

        /** Writes the batch's data file. */
        private final RecordBatchWriter writer = new RecordBatchWriter(
                table, keys, settings.kekRef(), ids, clock.instant())
                .stageVersion(settings.version());

        /** Records being prepared on the workers, in sequence order. */
        private final Deque<Future<EncryptedRecord>> pending =
                new ArrayDeque<>();

        /** When the batch was started, in milliseconds. */
        private final long openedAt = clock.millis();

        /** The most records waiting on the workers before the writer
         * catches up. */
        private final int maxPending = settings.encryptWorkers() * 4;

        /** The id of the file being split. */
        private final UUID itemId;

        /** The name of its format. */
        private final String format;

        /** The batch's trace span. */
        private final Span span;

        /** The last record's sequence number; -1 before the first. */
        private long lastSeq = -1;

        /** The total plaintext length of the records added. */
        private long bytes;

        /** How many records have been added. */
        private int count;

        /** The last record added, whose resume point the checkpoint takes. */
        private SplitRecord last;

        /** What landing kept about the item, to place its records. */
        private final LandedRefs refs;

        /**
         * Starts a batch and its trace span.
         *
         * @param itemId the id of the item being split
         * @param format the name of its format
         * @param refs   what landing kept about the item
         */
        Batch(UUID itemId, String format, LandedRefs refs)
        {
            this.itemId = itemId;
            this.format = format;
            this.refs = refs;
            this.span = Telemetry.get().span("split.batch",
                    Fields.of(Attr.ITEM_ID, itemId).with(Attr.FORMAT, format));
        }

        /**
         * Adds a record. One of one chunk is read and encrypted on the
         * worker pool; a larger one on this thread, after the ones before
         * it, so memory stays bounded.
         *
         * @param seq    its number in the file, from 0
         * @param record the record, with its origin
         * @param source the file it is read from
         * @throws Exception if it can't be read, prepared or written
         */
        void add(long seq, SplitRecord record, SeekableByteSource source)
                throws Exception
        {
            long length = record.plaintextLength();
            if (record.failure() != null)
            {
                addUnsplit(seq, record, source);
            }
            else if (RecordCrypto.chunkCount(length) == 1)
            {
                // Read and encrypt on the worker pool.
                pending.add(workers.submit(() -> writer.encryptSmall(itemId,
                        seq, format, record, source)));
                while (pending.size() > maxPending)
                {
                    store(get(pending.poll()));
                }
            }
            else
            {
                // Larger records run on this thread, one at a time, so
                // memory stays bounded.
                drain();
                writer.add(itemId, seq, format, record, source);
            }
            lastSeq = seq;
            last = record;
            bytes += length;
            count++;
            Telemetry.get().bytesRead(record.sourceLength(), "split");
        }

        /**
         * Adds the rest of a message that couldn't be split (design
         * §5.3.16): stored as it is, marked with the splitter's error and
         * where it was found, which its {@code record.split} carries.
         *
         * @param seq    its number in the file, from 0
         * @param record the record, with its failure and origin
         * @param source the file it is read from
         * @throws Exception if it can't be read, prepared or written
         */
        private void addUnsplit(long seq, SplitRecord record,
                SeekableByteSource source) throws Exception
        {
            long length = record.plaintextLength();
            long position = Math.max(0, Math.min(length,
                    record.failure().offset() - record.sourceOffset()));
            ErrorCode code = record.failure().code();
            if (RecordCrypto.chunkCount(length) == 1)
            {
                pending.add(workers.submit(() ->
                {
                    byte[] raw = record.readAll(source);
                    return writer.encrypt(itemId, seq, format, record, raw,
                            Representation.raw(format), code, position,
                            itemLineAndColumn(record, raw, position));
                }));
                while (pending.size() > maxPending)
                {
                    store(get(pending.poll()));
                }
            }
            else
            {
                drain();
                writer.add(itemId, seq, format, record, source,
                        Representation.raw(format), code, position, null);
            }
        }

        /**
         * Tells whether the batch should be committed: it has reached the
         * most records or bytes a batch holds, or has been open as long as
         * one may be.
         *
         * @return true to commit it now
         */
        boolean full()
        {
            return count >= settings.commitMaxRecords()
                    || bytes >= settings.commitMaxBytes()
                    || clock.millis() - openedAt
                    >= settings.commitMaxInterval().toMillis();
        }

        /**
         * Writes the records still being prepared and closes the data file.
         *
         * @return the data file, to commit
         * @throws Exception if a record can't be prepared or written
         */
        DataFile finish() throws Exception
        {
            drain();
            return writer.finish();
        }

        /**
         * Writes every record still being prepared, in order.
         *
         * @throws Exception if a record can't be prepared or written
         */
        void drain() throws Exception
        {
            while (!pending.isEmpty())
            {
                store(get(pending.poll()));
            }
        }

        /**
         * Works out where a byte of a record is in the file, as a line and
         * column, for a conversion failure: its line and column in the
         * record, moved by the record's origin.
         *
         * @param record   the record
         * @param raw      its bytes, as split
         * @param position the byte's index in them; kept within them
         * @return the position in the file; null if the record has no
         *         origin
         */
        private static SourcePosition itemLineAndColumn(SplitRecord record,
                byte[] raw, long position)
        {
            if (record.origin() == null)
            {
                return null;
            }
            int offset = (int) Math.max(0, Math.min(position, raw.length));
            SourcePosition inRecord = LineCounter.positionOf(raw, offset);
            return record.origin().toFile(inRecord);
        }

        /**
         * Writes an encrypted record. Writer thread only; records must come
         * in sequence order.
         *
         * @param encrypted the record
         * @throws IOException if it can't be written
         */
        private void store(EncryptedRecord encrypted) throws IOException
        {
            writer.write(encrypted);
        }

        /**
         * Abandons the batch: cancels the work in progress, discards the
         * data file and ends the span as failed.
         */
        void abandon()
        {
            pending.forEach(task -> task.cancel(true));
            writer.close();
            span.setAttribute(Attr.RESULT.key(), "abandoned");
            span.setStatus(StatusCode.ERROR);
            span.end();
        }
    }

    /**
     * Commits a batch with its checkpoint, after waiting for the previous
     * batch's messages, then starts publishing this batch's messages in the
     * background.
     *
     * @param batch           the batch
     * @param current         the checkpoint before it
     * @param previousPublish the previous batch's publishing; null if none
     * @param lease           the item's lease
     * @return the new checkpoint and this batch's publishing
     * @throws Exception if the commit fails beyond retrying, or the previous
     *                   publishing failed
     */
    private BatchOutcome commitBatch(Batch batch, Checkpoint current,
            Future<Long> previousPublish, LeaseToken lease) throws Exception
    {
        Telemetry telemetry = Telemetry.get();
        try (Scope ignored = batch.span.makeCurrent())
        {
            DataFile dataFile = batch.finish();
            hooks.batchWritten(batch.lastSeq);
            long published = await(previousPublish, current.publishedSeq());
            Checkpoint next = current.committed(batch.last.resume(),
                    batch.lastSeq, published);
            // Kept as pending first, so a stop before the next step leaves
            // a checkpoint that resume can settle (STAGES.md §8).
            checkpoints.prepare(next, lease);
            hooks.batchPrepared(batch.lastSeq);
            TableCommitter committer = new LeasedCommitter(lease);
            Span commit = telemetry.span("storage.commit",
                    Fields.of(Attr.TABLE, table.name())
                            .with(Attr.COUNT, batch.count));
            long started = System.nanoTime();
            Snapshot snapshot;
            try (Scope commitScope = commit.makeCurrent())
            {
                snapshot = settings.retry().call("storage.commit",
                        RetryPolicy.TRANSIENT_STORAGE, attempt ->
                        {
                            if (attempt > 1 && alreadyCommitted(next))
                            {
                                return table.currentSnapshot();
                            }
                            return committer.commit(table, transaction ->
                                    transaction.newFastAppend()
                                            .appendFile(dataFile).commit());
                        });
                hooks.batchAppended(batch.lastSeq);
                checkpoints.save(next, lease);
            }
            catch (Exception | Error failure)
            {
                Telemetry.fail(commit, failure);
                throw failure;
            }
            finally
            {
                commit.end();
            }
            telemetry.commitDuration(System.nanoTime() - started,
                    table.name());
            telemetry.recordsStored(batch.count, batch.format);
            telemetry.progress(next.resumeAt().offset(), next.itemSize());
            telemetry.audit("batch.committed",
                    Fields.of(Attr.ITEM_ID, next.itemId())
                            .with(Attr.SEQ, next.committedSeq())
                            .with(Attr.OFFSET, next.resumeAt().offset())
                            .with(Attr.COUNT, batch.count));
            batch.span.setAttribute(Attr.SEQ.key(), next.committedSeq());
            batch.span.setAttribute(Attr.COUNT.key(), batch.count);
            hooks.batchCommitted(batch.lastSeq);

            LocalDate day = batch.writer.ingestedDate();
            StorageRef ref = new StorageRef(settings.catalogName(), namespace,
                    tableName, day, snapshot.snapshotId(),
                    batch.writer.kekRef());
            List<ConnectorMessage> messages = new ArrayList<>(batch.count);
            for (WrittenRecord record : batch.writer.written())
            {
                if (record.error() == null)
                {
                    messages.add(message(record, ref, batch.refs,
                            settings.version()));
                }
                else
                {
                    // Kept in split_records, in the fail store at the
                    // item's end, and not sent on (STAGES.md §15.1).
                    // Audited once committed, so a restart doesn't report it
                    // twice.
                    telemetry.auditWarn("record.failed",
                            Fields.of(Attr.ITEM_ID, record.itemId())
                                    .with(Attr.SEQ, record.seq())
                                    .with(Attr.FORMAT, record.format())
                                    .with(Attr.ERROR_CODE, record.error())
                                    .with(Attr.OFFSET, record.sourceOffset())
                                    .with(Attr.POSITION,
                                            record.errorPosition()));
                    telemetry.recordFailed(record.error(), record.format());
                }
            }
            long last = batch.lastSeq;
            // Carry the trace across to the publishing thread, so
            // publish.batch is a child of this batch.
            Future<Long> publish = publishing.submit(Context.current().wrap(
                    () ->
                    {
                        publish(messages);
                        return last;
                    }));
            return new BatchOutcome(next, publish);
        }
        catch (Exception | Error failure)
        {
            Telemetry.fail(batch.span, failure);
            throw failure;
        }
        finally
        {
            batch.span.end();
        }
    }

    /**
     * After an ambiguous failure, tells whether an earlier attempt of this
     * commit landed after all: the batch's last record is in the table.
     *
     * @param next the checkpoint after the batch
     * @return true if it landed
     */
    private boolean alreadyCommitted(Checkpoint next)
    {
        table.refresh();
        return CommittedRecords.has(table, next.itemId(),
                next.committedSeq());
    }

    /**
     * The pipeline's settings.
     *
     * @return the settings
     */
    PipelineSettings settings()
    {
        return settings;
    }

    /**
     * The pipeline's clock.
     *
     * @return the clock
     */
    Clock clock()
    {
        return clock;
    }

    /**
     * Publishes the messages of every record of a checkpoint's file that is
     * committed but not known to be acknowledged
     * ({@code publishedSeq < seq <= committedSeq}). Repeats are harmless:
     * readers drop them by message id.
     *
     * @param current the checkpoint
     * @param refs    what landing kept about the item
     * @throws Exception if the records can't be read or published
     */
    void announceCommitted(Checkpoint current, LandedRefs refs)
            throws Exception
    {
        republish(current, refs);
    }

    /**
     * Seals and publishes messages now, waiting for their acknowledgement.
     *
     * @param messages the messages, in order
     * @throws Exception if they can't be published
     */
    void publishNow(List<ConnectorMessage> messages) throws Exception
    {
        publish(messages);
    }

    /**
     * Writes a checkpoint without data (a new item, the end of an item, a
     * failure) to the item's row. Doing it twice is harmless.
     *
     * @param next  the checkpoint
     * @param lease the item's lease
     * @throws LeaseLostException    if the lease was lost or taken over
     * @throws IllegalStateException if the database can't be reached
     */
    void commitCheckpoint(Checkpoint next, LeaseToken lease)
    {
        checkpoints.save(next, lease);
    }

    // ---- publishing ----

    /**
     * The message announcing a record written in a batch: a
     * {@code record.split}, with the error code of a piece the splitter
     * couldn't parse (STAGES.md §8), placed in its message for a batch
     * (§9).
     *
     * @param record the record
     * @param ref    where it is stored
     * @param refs    what landing kept about its item
     * @param version the split stage's version
     * @return the message
     */
    private static RecordSplit message(WrittenRecord record, StorageRef ref,
            LandedRefs refs, String version)
    {
        int message = refs.message(record.sourceOffset());
        return new RecordSplit(record.recordId(), record.itemId(),
                record.seq(), record.sourceOffset(), record.sourceLength(),
                record.format(), record.representation().contentType(),
                record.plaintextLength(), record.chunkCount(), ref,
                record.origin(),
                record.error() == null ? null : record.error().name(),
                record.errorAt(), message, refs.messageRef(message),
                refs.messageReplaced(message), version);
    }

    /**
     * What splitting an operator's correction came to (STAGES.md §15.6d):
     * its outcome, to note in the fail store, and the {@code record.split}
     * of each record it stored, to send.
     *
     * @param outcome  the outcome
     * @param messages the records' messages; empty if it failed
     */
    public record CorrectionSplit(CorrectionStore.Outcome outcome,
            List<ConnectorMessage> messages)
    {
    }

    /**
     * Splits an operator's correction of what the split stage failed, and
     * stores what passed, as any record but marked {@code correction_of}
     * (STAGES.md §15.6d). Nothing already stored is changed.
     * <ul>
     * <li>A failed piece (its record number, from 0): the correction is one
     * document, split alone, which must give exactly one record that
     * passes ({@code CORRECTION_RECORD_COUNT} otherwise). It is stored
     * under the piece's item and number, in its place in the item, marked
     * as correcting the failed row.</li>
     * <li>The item as a whole ({@code -1}): the correction is the whole
     * item, split from its start. The records already stored must come out
     * of it unchanged, byte for byte ({@code CORRECTION_CHANGES_PASSED}
     * otherwise: only what failed is corrected), and every record after
     * them must pass. Those are stored, numbered on from the item's, their
     * places those in the correction, marked as correcting the item.</li>
     * </ul>
     * A correction that fails stores nothing. Committed under the item's
     * lease.
     *
     * @param checkpoint the item's checkpoint, its splitting ended
     * @param pending    the correction
     * @param refs       what landing kept about the item
     * @param lease      the item's lease
     * @return what it came to, and the messages to send
     * @throws Exception if storage fails; the correction is tried again
     */
    public CorrectionSplit splitCorrection(Checkpoint checkpoint,
            CorrectionStore.Pending pending, LandedRefs refs,
            LeaseToken lease) throws Exception
    {
        UUID itemId = checkpoint.itemId();
        String version = settings.version();
        String format = checkpoint.format();
        CorrectionSplit before = storedBefore(checkpoint, pending, refs);
        if (before != null)
        {
            return before;
        }
        byte[] content = pending.open(keys);
        try
        {
            SeekableByteSource source = SeekableByteSource.of(content);
            List<SplitRecord> split = new ArrayList<>();
            try (SplitSession session = splitters.open(format, source,
                    ResumePoint.at(0), 0, settings.split()))
            {
                Optional<SplitRecord> next;
                while ((next = session.next()).isPresent())
                {
                    split.add(next.get());
                }
            }
            catch (SplitException failure)
            {
                return failedCorrection(pending, failure.code(), version);
            }
            table.refresh();
            Map<Long, CommittedRecords.Row> stored = new TreeMap<>();
            for (CommittedRecords.Row row : CommittedRecords.all(table,
                    itemId))
            {
                stored.put(row.seq(), row);
            }
            RecordBatchWriter writer = new RecordBatchWriter(table, keys,
                    settings.kekRef(), ids, clock.instant())
                    .stageVersion(version);
            if (pending.seq() >= 0)
            {
                CommittedRecords.Row original = stored.get(pending.seq());
                if (original == null || original.errorCode() == null)
                {
                    throw new IllegalStateException("The failed piece isn't"
                            + " in " + tableName);
                }
                if (split.size() != 1)
                {
                    return failedCorrection(pending,
                            ErrorCode.CORRECTION_RECORD_COUNT, version);
                }
                SplitRecord record = split.get(0);
                if (record.failure() != null)
                {
                    return failedCorrection(pending, record.failure().code(),
                            version);
                }
                SplitRecord lineage = SplitRecord.ofRange(
                        original.sourceOffset(), original.sourceLength(),
                        ResumePoint.at(original.sourceOffset()
                                + original.sourceLength()))
                        .withOrigin(original.origin());
                writer.correction(itemId, pending.seq(), original.recordId());
                writer.addBytes(itemId, pending.seq(), format, lineage,
                        record.readAll(source), Representation.raw(format),
                        null, -1, null);
            }
            else
            {
                long kept = checkpoint.recordCount();
                if (split.size() < kept)
                {
                    return failedCorrection(pending,
                            ErrorCode.CORRECTION_CHANGES_PASSED, version);
                }
                for (int seq = 0; seq < kept; seq++)
                {
                    CommittedRecords.Row row = stored.get((long) seq);
                    SplitRecord record = split.get(seq);
                    boolean passed = row != null && row.errorCode() == null;
                    if (row == null || (passed && record.failure() != null)
                            || !row.recordId().equals(RecordIds.encode(ids
                                    .newMac().doFinal(record.readAll(
                                            source)))))
                    {
                        return failedCorrection(pending,
                                ErrorCode.CORRECTION_CHANGES_PASSED, version);
                    }
                }
                for (int seq = (int) kept; seq < split.size(); seq++)
                {
                    if (split.get(seq).failure() != null)
                    {
                        return failedCorrection(pending,
                                split.get(seq).failure().code(), version);
                    }
                }
                for (int seq = (int) kept; seq < split.size(); seq++)
                {
                    SplitRecord record = split.get(seq);
                    writer.correction(itemId, seq, itemId.toString());
                    writer.add(itemId, seq, format, record, source);
                }
            }
            if (writer.written().isEmpty())
            {
                writer.close();
                return new CorrectionSplit(new CorrectionStore.Outcome(
                        pending, true, null, null, version), List.of());
            }
            DataFile file = writer.finish();
            Snapshot snapshot = new LeasedCommitter(lease).commit(table,
                    transaction -> transaction.newFastAppend()
                            .appendFile(file).commit());
            StorageRef ref = new StorageRef(settings.catalogName(), namespace,
                    tableName, writer.ingestedDate(), snapshot.snapshotId(),
                    writer.kekRef());
            List<ConnectorMessage> messages = new ArrayList<>();
            for (WrittenRecord record : writer.written())
            {
                messages.add(message(record, ref, refs, version));
            }
            Telemetry.get().recordsStored(messages.size(), format);
            return new CorrectionSplit(new CorrectionStore.Outcome(pending,
                    true, writer.written().get(0).recordId(), null, version),
                    messages);
        }
        finally
        {
            Arrays.fill(content, (byte) 0);
        }
    }

    /**
     * A correction stored before a pod stopped, its outcome not yet noted:
     * only a pending correction can have rows (one that failed stores
     * none, and a failure once corrected takes no more), so its rows are
     * sent again and it is noted as passed, rather than stored twice.
     *
     * @param checkpoint the item's checkpoint
     * @param pending    the correction
     * @param refs       what landing kept about the item
     * @return what it came to; null if nothing was stored
     */
    private CorrectionSplit storedBefore(Checkpoint checkpoint,
            CorrectionStore.Pending pending, LandedRefs refs)
    {
        UUID itemId = checkpoint.itemId();
        List<CommittedRecords.Row> rows = new ArrayList<>();
        for (CommittedRecords.Row row : correctionRows(itemId))
        {
            if (pending.seq() >= 0 ? row.seq() == pending.seq()
                    : row.seq() >= checkpoint.recordCount())
            {
                rows.add(row);
            }
        }
        if (rows.isEmpty())
        {
            return null;
        }
        long snapshotId = table.currentSnapshot().snapshotId();
        List<ConnectorMessage> messages = new ArrayList<>();
        for (CommittedRecords.Row row : rows)
        {
            messages.add(SplitMessages.recordSplit(itemId, row,
                    new StorageRef(settings.catalogName(), namespace,
                            tableName, row.ingestedDate(), snapshotId,
                            row.kekRef()), refs));
        }
        return new CorrectionSplit(new CorrectionStore.Outcome(pending, true,
                rows.get(0).recordId(), null, rows.get(0).stageVersion()),
                messages);
    }

    /**
     * A correction that failed: nothing stored, nothing sent.
     *
     * @param pending the correction
     * @param code    why
     * @param version the version it was split under
     * @return the outcome
     */
    private static CorrectionSplit failedCorrection(
            CorrectionStore.Pending pending, ErrorCode code, String version)
    {
        return new CorrectionSplit(new CorrectionStore.Outcome(pending, false,
                null, code.name(), version), List.of());
    }

    /**
     * Reads an item's rows made from operators' corrections (STAGES.md
     * §15.6d).
     *
     * @param itemId the item
     * @return the rows, by record number
     */
    public List<CommittedRecords.Row> correctionRows(UUID itemId)
    {
        table.refresh();
        return CommittedRecords.corrections(table, itemId);
    }

    /**
     * Counts a file's committed records that failed conversion and are
     * stored in their original form.
     *
     * @param itemId the file's id
     * @return how many there are
     */
    public long failedRecordCount(UUID itemId)
    {
        table.refresh();
        return CommittedRecords.countFailed(table, itemId);
    }

    /**
     * Reads the rows of an item's committed records that the splitter
     * couldn't parse, for its fail store (STAGES.md §15.3).
     *
     * @param itemId the item
     * @return the rows, in sequence order
     */
    public List<CommittedRecords.Row> failedRows(UUID itemId)
    {
        table.refresh();
        List<CommittedRecords.Row> failed = new ArrayList<>();
        for (CommittedRecords.Row row : CommittedRecords.all(table, itemId))
        {
            if (!SplitMessages.announced(row))
            {
                failed.add(row);
            }
        }
        return failed;
    }

    /**
     * Reads a file's stored records that failed conversion.
     *
     * @param itemId the file
     * @return them, in sequence order
     */
    public List<CommittedRecords.Failed> failedRecords(UUID itemId)
    {
        table.refresh();
        return CommittedRecords.failed(table, itemId);
    }

    /**
     * Rebuilds and publishes again the messages of records committed but
     * not acknowledged before a restart.
     *
     * @param current the checkpoint
     * @param refs    what landing kept about the item
     * @throws Exception if the records can't be read or published
     */
    private void republish(Checkpoint current, LandedRefs refs)
            throws Exception
    {
        if (current.committedSeq() <= current.publishedSeq())
        {
            return;
        }
        long snapshotId = table.currentSnapshot().snapshotId();
        List<ConnectorMessage> messages = new ArrayList<>();
        for (CommittedRecords.Row row : CommittedRecords.read(table,
                current.itemId(), current.publishedSeq(),
                current.committedSeq()))
        {
            if (!SplitMessages.announced(row))
            {
                continue;
            }
            StorageRef ref = new StorageRef(settings.catalogName(), namespace,
                    tableName, row.ingestedDate(), snapshotId, row.kekRef());
            messages.add(SplitMessages.recordSplit(current.itemId(), row,
                    ref, refs));
        }
        Telemetry.get().audit("publish.republish",
                Fields.of(Attr.ITEM_ID, current.itemId())
                        .with(Attr.SEQ, current.publishedSeq() + 1)
                        .with(Attr.COUNT, messages.size()));
        publish(messages);
    }

    /**
     * Seals messages on the worker pool, sends them in order, and waits for
     * every acknowledgement. A message whose send fails is retried on its
     * own, with backoff.
     *
     * @param messages the messages, in order
     * @throws PublishFailedException if a message can't be sent even after
     *                                retrying
     * @throws Exception              if waiting is interrupted, or sealing
     *                                fails
     */
    private void publish(List<ConnectorMessage> messages) throws Exception
    {
        Telemetry telemetry = Telemetry.get();
        Span span = telemetry.span("publish.batch",
                Fields.of(Attr.COUNT, messages.size()));
        long started = System.nanoTime();
        try (Scope ignored = span.makeCurrent())
        {
            sendAll(messages);
        }
        catch (Exception | Error failure)
        {
            Telemetry.fail(span, failure instanceof PublishFailedException
                    ? ErrorCode.PUBLISH_FAILED : null);
            throw failure;
        }
        finally
        {
            span.end();
        }
        telemetry.publishDuration(System.nanoTime() - started);
        telemetry.recordsPublished(messages.stream()
                .filter(RecordInStorage.class::isInstance).count());
    }

    /**
     * Seals and sends messages, then waits for each acknowledgement,
     * retrying any message whose send failed.
     *
     * @param messages the messages, in order
     * @throws PublishFailedException if a message can't be sent even after
     *                                retrying
     * @throws Exception              if waiting is interrupted, or sealing
     *                                fails
     */
    private void sendAll(List<ConnectorMessage> messages) throws Exception
    {
        List<CompletableFuture<byte[]>> sealed =
                new ArrayList<>(messages.size());
        for (ConnectorMessage message : messages)
        {
            sealed.add(CompletableFuture.supplyAsync(
                    () -> sealer.seal(message), sealers));
        }
        List<OutboundMessage> outbound = new ArrayList<>(messages.size());
        List<CompletableFuture<Void>> acks = new ArrayList<>(messages.size());
        for (int index = 0; index < messages.size(); index++)
        {
            ConnectorMessage message = messages.get(index);
            OutboundMessage toSend = OutboundMessage.of(message,
                    get(sealed.get(index)));
            outbound.add(toSend);
            acks.add(send(toSend));
        }
        for (int index = 0; index < acks.size(); index++)
        {
            try
            {
                acks.get(index).get();
            }
            catch (ExecutionException sendFailed)
            {
                OutboundMessage toSend = outbound.get(index);
                try
                {
                    settings.retry().call("publish",
                            error -> !(error instanceof InterruptedException),
                            attempt -> get(send(toSend)));
                }
                catch (InterruptedException interrupted)
                {
                    throw interrupted;
                }
                catch (Exception failed)
                {
                    throw new PublishFailedException(failed);
                }
            }
        }
    }

    /**
     * Sends a message, turning a publisher that throws into a failed send.
     *
     * @param message the sealed message
     * @return its acknowledgement, to come
     */
    private CompletableFuture<Void> send(OutboundMessage message)
    {
        try
        {
            return publisher.publish(message);
        }
        catch (RuntimeException failure)
        {
            return CompletableFuture.failedFuture(failure);
        }
    }

    // ---- helpers ----

    /**
     * Waits for a task's result, throwing what the task threw.
     *
     * @param <T>    the result's type
     * @param future the task
     * @return its result
     * @throws Exception what the task threw, or {@link InterruptedException}
     */
    private static <T> T get(Future<T> future) throws Exception
    {
        try
        {
            return future.get();
        }
        catch (ExecutionException failed)
        {
            Throwable cause = failed.getCause();
            if (cause instanceof Exception exception)
            {
                throw exception;
            }
            throw (Error) cause;
        }
    }

    /**
     * Waits for a batch's publishing, if there is one.
     *
     * @param publish  the publishing; null if none
     * @param fallback what to return without one
     * @return the last sequence number published, or the fallback
     * @throws Exception what the publishing threw
     */
    private static long await(Future<Long> publish, long fallback)
            throws Exception
    {
        return publish == null ? fallback : get(publish);
    }

    /**
     * Splits Iceberg's {@code catalog.namespace.table} name into namespace
     * and table.
     *
     * @param fullName the table's full name
     * @return the namespace, then the table's name
     * @throws IllegalArgumentException if the name hasn't three parts
     */
    static String[] namespaceAndTable(String fullName)
    {
        int firstDot = fullName.indexOf('.');
        int lastDot = fullName.lastIndexOf('.');
        if (firstDot < 0 || firstDot == lastDot)
        {
            throw new IllegalArgumentException(
                    "Expected catalog.namespace.table, got " + fullName);
        }
        return new String[]
        {
            fullName.substring(firstDot + 1, lastDot),
            fullName.substring(lastDot + 1)
        };
    }

    /**
     * Stops the thread pools, waiting up to 30 seconds for each.
     */
    @Override
    public void close()
    {
        publishing.shutdownNow();
        sealers.shutdownNow();
        workers.shutdownNow();
        try
        {
            publishing.awaitTermination(30, TimeUnit.SECONDS);
            sealers.awaitTermination(30, TimeUnit.SECONDS);
            workers.awaitTermination(30, TimeUnit.SECONDS);
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
        }
    }
}
