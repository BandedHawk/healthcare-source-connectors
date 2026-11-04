/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionFailed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.position.LineCounter;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointCodec;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.convert.ConversionException;
import com.sdh.healthcare.connector.core.convert.Representation;
import com.sdh.healthcare.connector.core.convert.SectionConverter;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.publish.MessagePublisher;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.Section;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.split.SplitSession;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.storage.CommittedSections;
import com.sdh.healthcare.connector.core.storage.SectionBatchWriter;
import com.sdh.healthcare.connector.core.storage.SectionBatchWriter.EncryptedSection;
import com.sdh.healthcare.connector.core.storage.SectionBatchWriter.WrittenSection;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The split phase of ingestion, the same whatever the source (design §4,
 * §8.3, §9.2): read, split, hash and encrypt, write, commit with the
 * checkpoint under the lease's fence, publish. One file at a time; one
 * instance per process.
 * <p>
 * For each file:
 * <ol>
 * <li>A new file gets a UUIDv7 and a first checkpoint ({@code SPLITTING})
 * committed before anything else; a file matching the fingerprint of the
 * checkpoint in progress is resumed instead.</li>
 * <li>{@code file.started} is published (again on resume; brokers and
 * readers drop repeats by message id).</li>
 * <li>On resume, the messages of sections committed but not published are
 * rebuilt from the table's columns and published again.</li>
 * <li>Sections are encrypted on a worker pool (sections of one chunk) or on
 * this thread (larger ones) and written in sequence order. A batch is
 * committed when it reaches {@code commitMaxSections},
 * {@code commitMaxBytes} or {@code commitMaxInterval}: the data file and the
 * checkpoint in one fenced commit.</li>
 * <li>The batch's messages are published in the background while the next
 * batch is split. The next commit waits for that and records the
 * acknowledged sequence number as {@code publishedSeq}.</li>
 * <li>At the end, a commit without data moves the checkpoint to
 * {@code ARCHIVING}.</li>
 * </ol>
 * <p>
 * Errors: a {@link SplitException} means the file can't be split (fail the
 * file). Transient storage and broker errors are retried as the
 * {@link RetryPolicy} says. A lost lease and
 * {@code CommitStateUnknownException} are not caught: the process should
 * exit and restart from the checkpoint.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class IngestPipeline implements AutoCloseable
{

    /** Where the pipeline's diagnostics go (never section content). */
    private static final Logger LOG =
            LoggerFactory.getLogger(IngestPipeline.class);

    /**
     * Restarts without progress ({@link Checkpoint#restarts()}) after which
     * the uncommitted batch is stored without converting any of it,
     * {@code CONVERT_SKIPPED}, in case a conversion is what keeps stopping
     * the connector (design §9.2). One restart is allowed for anything else,
     * such as a redeployment or a lost lease.
     */
    public static final int SKIP_CONVERSION_AFTER_RESTARTS = 2;

    /**
     * Restarts without progress after which the file is failed,
     * {@code RESUME_NO_PROGRESS}, rather than resumed again, so a file that
     * stops the connector every time can't keep it from ever moving on
     * (design §9.2).
     */
    public static final int FAIL_AFTER_RESTARTS = 3;

    /**
     * The outcome of the split phase. The checkpoint is in state
     * {@code ARCHIVING}.
     *
     * @param fileId       the file's id
     * @param sectionCount how many sections it was split into
     * @param checkpoint   the checkpoint after the split phase
     */
    public record SplitResult(UUID fileId, long sectionCount,
            Checkpoint checkpoint)
    {
    }

    /** The pipeline's settings. */
    private final PipelineSettings settings;

    /** The {@code sections} table, which also holds the checkpoint. */
    private final Table table;

    /** Commits under the lease's fence. */
    private final FencedCommitter committer;

    /** Wraps each batch's data key. */
    private final KeyProvider keys;

    /** Makes section ids and keys the checkpoint and fingerprints. */
    private final SectionIds ids;

    /** Encodes and verifies the checkpoint. */
    private final CheckpointCodec codec;

    /** The splitters, by format. */
    private final SplitterRegistry splitters;

    /** Signs and encrypts messages. */
    private final MessageSealer sealer;

    /** Sends messages to the broker. */
    private final MessagePublisher publisher;

    /** The time, for checkpoints, batches and ids. */
    private final Clock clock;

    /** The table property the checkpoint is stored under. */
    private final String checkpointKey;

    /** The table's namespace, for storage references. */
    private final String namespace;

    /** The table's name without its catalog and namespace. */
    private final String tableName;

    /** Reads, converts and encrypts sections of one chunk. */
    private final ExecutorService workers;

    /**
     * Seals messages. It has its own pool: sharing the encryption pool let
     * each batch's 10,000 sealing jobs queue ahead of the next batch's
     * section encryption, so splitting stalled while the previous batch was
     * sealed (found by the throughput tests, design §13).
     */
    private final ExecutorService sealers;

    /** Publishes each committed batch's messages, one batch at a time. */
    private final ExecutorService publishing;

    /**
     * This format's JSON converter (design §6.4); null when sections are
     * stored as split.
     */
    private final SectionConverter converter;

    /** The heap for conversions running at once (design §6.4). */
    private final ConversionBudget budget;

    /** Test hooks between the steps of a batch; none by default. */
    private PipelineHooks hooks = PipelineHooks.NONE;

    /**
     * Creates the pipeline and its thread pools, checking that the split and
     * conversion settings suit the format.
     *
     * @param settings      the pipeline's settings
     * @param sectionsTable the {@code sections} table
     * @param committer     commits under the lease's fence
     * @param keys          wraps each batch's data key
     * @param ids           makes section ids and keys
     * @param splitters     the splitters, by format
     * @param sealer        signs and encrypts messages
     * @param publisher     sends messages to the broker
     * @param clock         the time
     */
    public IngestPipeline(PipelineSettings settings, Table sectionsTable,
            FencedCommitter committer, KeyProvider keys, SectionIds ids,
            SplitterRegistry splitters, MessageSealer sealer,
            MessagePublisher publisher, Clock clock)
    {
        this.settings = settings;
        this.table = sectionsTable;
        this.committer = committer;
        this.keys = keys;
        this.ids = ids;
        this.codec = new CheckpointCodec(ids);
        this.splitters = splitters;
        this.sealer = sealer;
        this.publisher = publisher;
        this.clock = clock;
        this.checkpointKey = Checkpoint.propertyKey(settings.connectorId());
        String[] namespaceAndTable = namespaceAndTable(sectionsTable.name());
        this.namespace = namespaceAndTable[0];
        this.tableName = namespaceAndTable[1];
        splitters.get(settings.format()).validate(settings.split());
        this.converter =
                settings.converters().get(settings.format()).orElse(null);
        if (converter != null)
        {
            converter.validate(settings.split());
        }
        this.budget =
                new ConversionBudget(settings.effectiveConvertMemoryBytes());
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
     * Reads the stored checkpoint, verified.
     *
     * @return the checkpoint; empty if this connector has never started a
     *         file
     */
    public Optional<Checkpoint> checkpoint()
    {
        table.refresh();
        String encoded = table.properties().get(checkpointKey);
        return encoded == null
                ? Optional.empty() : Optional.of(codec.decode(encoded));
    }

    /**
     * Closes a checkpoint in progress whose file has disappeared (design §5,
     * startup). Sections already stored stay where they are.
     *
     * @throws Exception if the checkpoint can't be committed
     */
    public void abandonInProgress() throws Exception
    {
        Optional<Checkpoint> current = checkpoint();
        if (current.isPresent()
                && current.get().state() == Checkpoint.State.SPLITTING)
        {
            commitCheckpoint(
                    current.get().withState(Checkpoint.State.ABANDONED));
            Telemetry.get().auditWarn("file.abandoned",
                    Fields.of(Attr.FILE_ID, current.get().fileId())
                            .with(Attr.SEQ, current.get().committedSeq()));
        }
    }

    /**
     * Runs the split phase for a file: from the start for a new file, or
     * from the checkpoint if this is the file in progress. Returns once every
     * section is committed and published.
     *
     * @param source the file
     * @return the outcome
     * @throws IllegalStateException if a different file is still splitting
     *                               or archiving; finish or abandon it first
     * @throws SplitException        if the file is too large, can't be split,
     *                               or has stopped the connector too often
     * @throws Exception             if storage or the broker fails beyond
     *                               retrying
     */
    public SplitResult split(SeekableByteSource source) throws Exception
    {
        return split(source, fingerprint(source), null);
    }

    /**
     * Runs the split phase for a landed item (design §5.3.6): as
     * {@link #split(SeekableByteSource)}, but the item's ID is the file ID,
     * and the checkpoint recognises the item by its ID instead of a
     * fingerprint of its content.
     *
     * @param source the item's bytes
     * @param itemId the item's ID
     * @return the outcome
     * @throws IllegalStateException if a different item is still in
     *                               progress
     * @throws SplitException        if the item is too large, can't be
     *                               split, or has stopped the connector too
     *                               often
     * @throws Exception             if storage or the broker fails beyond
     *                               retrying
     */
    public SplitResult split(SeekableByteSource source, UUID itemId)
            throws Exception
    {
        return split(source, itemFingerprint(itemId), itemId);
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
     * Runs the split phase.
     *
     * @param source      the file or item
     * @param fingerprint what recognises it in the checkpoint
     * @param fileId      the ID to give it if it is new; null for a new
     *                    UUIDv7
     * @return the outcome
     * @throws Exception as {@link #split(SeekableByteSource)}
     */
    private SplitResult split(SeekableByteSource source, String fingerprint,
            UUID fileId) throws Exception
    {
        Checkpoint current = checkpoint().orElse(null);
        if (current != null && current.fingerprint().equals(fingerprint)
                && current.state() == Checkpoint.State.ARCHIVING)
        {
            return new SplitResult(current.fileId(), current.sectionCount(),
                    current);
        }
        if (current != null && current.state().inProgress()
                && current.state() != Checkpoint.State.SPLITTING)
        {
            throw new IllegalStateException("File " + current.fileId()
                    + " is still " + current.state());
        }
        if (current != null && current.state() == Checkpoint.State.SPLITTING)
        {
            if (!current.fingerprint().equals(fingerprint))
            {
                throw new IllegalStateException(
                        "File " + current.fileId() + " is still in progress");
            }
            if (!current.format().equals(settings.format()))
            {
                throw new IllegalStateException("Split format changed while"
                        + " file " + current.fileId() + " is in progress");
            }
            // Counted before any work, as a crash in this attempt (an
            // out-of-memory exit, say) can't record anything.
            current = current.resumed();
            if (current.restarts() >= FAIL_AFTER_RESTARTS)
            {
                throw new SplitException(ErrorCode.RESUME_NO_PROGRESS,
                        current.resumeAt().offset());
            }
            commitCheckpoint(current);
            Telemetry.get().audit("file.resumed",
                    Fields.of(Attr.FILE_ID, current.fileId())
                            .with(Attr.SEQ, current.nextSeq())
                            .with(Attr.OFFSET, current.resumeAt().offset())
                            .with(Attr.COUNT, current.restarts()));
            if (converter != null
                    && current.restarts() >= SKIP_CONVERSION_AFTER_RESTARTS)
            {
                Telemetry.get().auditWarn("file.conversion_skipped",
                        Fields.of(Attr.FILE_ID, current.fileId())
                                .with(Attr.SEQ, current.nextSeq())
                                .with(Attr.COUNT, current.restarts()));
            }
        }
        else
        {
            if (source.size() > settings.split().limits().maxFileBytes())
            {
                // Rejected before ingestion (design §6.3): no checkpoint, no
                // file.started.
                throw new SplitException(ErrorCode.LIMIT_FILE_SIZE, 0);
            }
            current = Checkpoint.start(fileId != null ? fileId
                    : UuidV7.next(clock.millis()),
                    fingerprint, settings.format(), source.size(),
                    clock.instant());
            commitCheckpoint(current);
            Telemetry.get().audit("file.started",
                    Fields.of(Attr.FILE_ID, current.fileId())
                            .with(Attr.FORMAT, current.format())
                            .with(Attr.BYTES, current.fileSize()));
        }
        return run(current, source);
    }

    /**
     * Splits, stores and publishes a file from its checkpoint, then moves
     * the checkpoint to {@code ARCHIVING}.
     *
     * @param start  the checkpoint to start from
     * @param source the file
     * @return the outcome
     * @throws Exception if splitting, storage or the broker fails
     */
    private SplitResult run(Checkpoint start, SeekableByteSource source)
            throws Exception
    {
        publish(List.of(new FileStarted(start.fileId(), start.format(),
                start.fileSize(), start.startedAt(), settings.connectorId())));
        republish(start);
        Checkpoint current = start.published(start.committedSeq());

        Batch batch = null;
        Future<Long> inFlightPublish = null;
        // Lines and columns, so failures and mapping issues can be found in
        // the file (SectionOrigin).
        try (SplitSession session = open(current, source);
                SectionOrigins origins = new SectionOrigins(source))
        {
            long seq = current.nextSeq();
            Optional<Section> next;
            while ((next = session.next()).isPresent())
            {
                if (batch == null)
                {
                    // Only the batch the connector kept stopping in goes
                    // unconverted: once it commits, the count is back to 0.
                    batch = new Batch(current.fileId(), current.format(),
                            current.restarts()
                                    < SKIP_CONVERSION_AFTER_RESTARTS);
                }
                batch.add(seq++, origins.withOrigin(next.get()), source);
                if (batch.full())
                {
                    BatchOutcome outcome =
                            commitBatch(batch, current, inFlightPublish);
                    current = outcome.checkpoint();
                    inFlightPublish = outcome.publish();
                    batch = null;
                }
            }
            if (batch != null)
            {
                BatchOutcome outcome =
                        commitBatch(batch, current, inFlightPublish);
                current = outcome.checkpoint();
                inFlightPublish = outcome.publish();
                batch = null;
            }
            current = current.published(
                    await(inFlightPublish, current.publishedSeq()));
            inFlightPublish = null;
            hooks.beforeEndOfFile();
            current = current.withState(Checkpoint.State.ARCHIVING);
            commitCheckpoint(current);
            Telemetry.get().audit("file.split",
                    Fields.of(Attr.FILE_ID, current.fileId())
                            .with(Attr.COUNT, current.sectionCount()));
            return new SplitResult(current.fileId(), current.sectionCount(),
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
                && item.records().isPresent())
        {
            return splitters.openRecords(current.format(), source,
                    item.records().get().offsets(), current.resumeAt(),
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
     * A section ready to be written: encrypted already, or (over one chunk)
     * still in memory.
     */
    private sealed interface Prepared permits Ready, InMemory
    {
    }

    /**
     * A section encrypted on a worker, ready to be written as rows.
     *
     * @param encrypted the encrypted section
     */
    private record Ready(EncryptedSection encrypted) implements Prepared
    {
    }

    /**
     * A section of more than one chunk, converted (or not) and still in
     * memory, for the writer thread to encrypt and store.
     *
     * @param seq            its number in the file, from 0
     * @param lineage        the section as split
     * @param bytes          the bytes to store
     * @param representation what they are
     * @param error          why conversion failed; null if it didn't
     * @param position       where in the section it failed, in bytes; -1
     *                       without an error
     * @param errorAt        where in the file it failed; null if it didn't,
     *                       or the error has no place
     */
    private record InMemory(long seq, Section lineage, byte[] bytes,
            Representation representation, ErrorCode error, long position,
            SourcePosition errorAt) implements Prepared
    {
    }

    /**
     * One commit batch. Sections are added in sequence order; those of one
     * chunk are read, converted and encrypted on the worker pool.
     */
    private final class Batch
    {

        /** Writes the batch's data file. */
        private final SectionBatchWriter writer = new SectionBatchWriter(
                table, keys, settings.kekRef(), ids, clock.instant());

        /** Sections being prepared on the workers, in sequence order. */
        private final Deque<Future<Prepared>> pending = new ArrayDeque<>();

        /** When the batch was started, in milliseconds. */
        private final long openedAt = clock.millis();

        /** The most sections waiting on the workers before the writer
         * catches up. */
        private final int maxPending = settings.encryptWorkers() * 4;

        /** The id of the file being split. */
        private final UUID fileId;

        /** The name of its format. */
        private final String format;

        /**
         * False to store every section as split, {@code CONVERT_SKIPPED}
         * (see {@link #SKIP_CONVERSION_AFTER_RESTARTS}).
         */
        private final boolean convert;

        /** The batch's trace span. */
        private final Span span;

        /** The last section's sequence number; -1 before the first. */
        private long lastSeq = -1;

        /** The total plaintext length of the sections added. */
        private long bytes;

        /** How many sections have been added. */
        private int count;

        /** The last section added, whose resume point the checkpoint takes. */
        private Section last;

        /**
         * Starts a batch and its trace span.
         *
         * @param fileId  the id of the file being split
         * @param format  the name of its format
         * @param convert false to store every section as split
         */
        Batch(UUID fileId, String format, boolean convert)
        {
            this.fileId = fileId;
            this.format = format;
            this.convert = convert;
            this.span = Telemetry.get().span("split.batch",
                    Fields.of(Attr.FILE_ID, fileId).with(Attr.FORMAT, format));
        }

        /**
         * Adds a section. One of one chunk is read, converted and encrypted
         * on the worker pool; a larger one on this thread, after the ones
         * before it, so memory stays bounded.
         *
         * @param seq     its number in the file, from 0
         * @param section the section, with its origin
         * @param source  the file it is read from
         * @throws Exception if it can't be read, prepared or written
         */
        void add(long seq, Section section, SeekableByteSource source)
                throws Exception
        {
            long length = section.plaintextLength();
            if (section.failure() != null)
            {
                addUnsplit(seq, section, source);
            }
            else if (SectionCrypto.chunkCount(length) == 1)
            {
                // Read, convert (if any) and encrypt on the worker pool.
                pending.add(workers.submit(() ->
                {
                    if (converter == null)
                    {
                        return new Ready(writer.encryptSmall(fileId, seq,
                                format, section, source));
                    }
                    ErrorCode skip = skipReason(length);
                    if (skip != null)
                    {
                        return new Ready(writer.encrypt(fileId, seq, format,
                                section, section.readAll(source),
                                Representation.raw(format), skip, 0, null));
                    }
                    return converting(length, () ->
                            prepare(seq, section, section.readAll(source)));
                }));
                while (pending.size() > maxPending)
                {
                    store(get(pending.poll()));
                }
            }
            else
            {
                // Larger sections run on this thread, one at a time, so
                // memory stays bounded.
                drain();
                if (converter == null)
                {
                    writer.add(fileId, seq, format, section, source);
                }
                else if (skipReason(length) != null)
                {
                    writer.add(fileId, seq, format, section, source,
                            Representation.raw(format), skipReason(length), 0,
                            null);
                }
                else
                {
                    store(converting(length, () ->
                            prepare(seq, section, section.readAll(source))));
                }
            }
            lastSeq = seq;
            last = section;
            bytes += length;
            count++;
            Telemetry.get().bytesRead(section.sourceLength(), "split");
        }

        /**
         * Adds the rest of a message that couldn't be split (design
         * §5.3.16): stored as it is, never converted, marked with the
         * splitter's error and where it was found, so it is announced with
         * {@code section.failed} like a section that couldn't be converted.
         *
         * @param seq     its number in the file, from 0
         * @param section the section, with its failure and origin
         * @param source  the file it is read from
         * @throws Exception if it can't be read, prepared or written
         */
        private void addUnsplit(long seq, Section section,
                SeekableByteSource source) throws Exception
        {
            long length = section.plaintextLength();
            long position = Math.max(0, Math.min(length,
                    section.failure().offset() - section.sourceOffset()));
            ErrorCode code = section.failure().code();
            if (SectionCrypto.chunkCount(length) == 1)
            {
                pending.add(workers.submit(() ->
                {
                    byte[] raw = section.readAll(source);
                    return new Ready(writer.encrypt(fileId, seq, format,
                            section, raw, Representation.raw(format), code,
                            position, fileLineAndColumn(section, raw,
                                    position)));
                }));
                while (pending.size() > maxPending)
                {
                    store(get(pending.poll()));
                }
            }
            else
            {
                drain();
                writer.add(fileId, seq, format, section, source,
                        Representation.raw(format), code, position, null);
            }
        }

        /**
         * Reads and converts a section once there is room in the conversion
         * budget for it, so large sections converting together can't exhaust
         * the heap. The room is given back once the section is ready to
         * write.
         *
         * @param length the section's length
         * @param work   reads and converts it
         * @return the prepared section
         * @throws Exception if waiting is interrupted, or the work fails
         */
        private Prepared converting(long length,
                Callable<Prepared> work) throws Exception
        {
            int taken = budget.acquire(
                    (long) Math.ceil(length * converter.memoryFactor()));
            try
            {
                return work.call();
            }
            finally
            {
                budget.release(taken);
            }
        }

        /**
         * Tells why a section of a length isn't converted.
         *
         * @param length the section's length
         * @return {@code CONVERT_SKIPPED} or {@code CONVERT_TOO_LARGE}; null
         *         if it is converted
         */
        private ErrorCode skipReason(long length)
        {
            return !convert ? ErrorCode.CONVERT_SKIPPED
                    : length > settings.convertMaxBytes()
                    ? ErrorCode.CONVERT_TOO_LARGE : null;
        }

        /**
         * Tells whether the batch should be committed: it has reached the
         * most sections or bytes a batch holds, or has been open as long as
         * one may be.
         *
         * @return true to commit it now
         */
        boolean full()
        {
            return count >= settings.commitMaxSections()
                    || bytes >= settings.commitMaxBytes()
                    || clock.millis() - openedAt
                    >= settings.commitMaxInterval().toMillis();
        }

        /**
         * Writes the sections still being prepared and closes the data file.
         *
         * @return the data file, to commit
         * @throws Exception if a section can't be prepared or written
         */
        DataFile finish() throws Exception
        {
            drain();
            return writer.finish();
        }

        /**
         * Writes every section still being prepared, in order.
         *
         * @throws Exception if a section can't be prepared or written
         */
        void drain() throws Exception
        {
            while (!pending.isEmpty())
            {
                store(get(pending.poll()));
            }
        }

        /**
         * Converts a section to JSON; if that fails, keeps it as split,
         * marked with the error (design §6.4). Small results are encrypted
         * here (on a worker), larger ones are left for the writer thread.
         *
         * @param seq     its number in the file, from 0
         * @param section the section
         * @param raw     its bytes, as split; wiped once converted
         * @return the prepared section
         */
        private Prepared prepare(long seq, Section section, byte[] raw)
        {
            byte[] stored;
            Representation representation;
            ErrorCode error = null;
            long position = -1;
            SourcePosition errorAt = null;
            try
            {
                stored = converter.convert(raw, settings.split());
                representation = Representation.json(converter.mapping());
                Arrays.fill(raw, (byte) 0);
            }
            catch (ConversionException failure)
            {
                stored = raw;
                representation = Representation.raw(format);
                error = failure.code();
                position = failure.position();
                errorAt = fileLineAndColumn(section, raw, position);
            }
            catch (RuntimeException bug)
            {
                // A converter bug: fail the section, not the whole file
                // (which would also be blamed on storage). Only the class
                // name is logged; a message could quote the section.
                LOG.warn("convert.unexpected fileId={} seq={} format={}"
                        + " error={}", fileId, seq, format,
                        bug.getClass().getName());
                stored = raw;
                representation = Representation.raw(format);
                error = ErrorCode.CONVERT_FAILED;
                position = 0;
            }
            if (SectionCrypto.chunkCount(stored.length) == 1)
            {
                return new Ready(writer.encrypt(fileId, seq, format, section,
                        stored, representation, error, position, errorAt));
            }
            return new InMemory(seq, section, stored, representation, error,
                    position, errorAt);
        }

        /**
         * Works out where a byte of a section is in the file, as a line and
         * column, for a conversion failure: its line and column in the
         * section, moved by the section's origin.
         *
         * @param section  the section
         * @param raw      its bytes, as split
         * @param position the byte's index in them; kept within them
         * @return the position in the file; null if the section has no
         *         origin
         */
        private static SourcePosition fileLineAndColumn(Section section,
                byte[] raw, long position)
        {
            if (section.origin() == null)
            {
                return null;
            }
            int offset = (int) Math.max(0, Math.min(position, raw.length));
            SourcePosition inSection = LineCounter.positionOf(raw, offset);
            return section.origin().toFile(inSection);
        }

        /**
         * Writes a prepared section. Writer thread only; sections must come
         * in sequence order.
         *
         * @param prepared the section
         * @throws IOException if a section in memory can't be stored
         */
        private void store(Prepared prepared) throws IOException
        {
            switch (prepared)
            {
                case Ready ready -> writer.write(ready.encrypted());
                case InMemory inMemory -> writer.addBytes(fileId,
                        inMemory.seq(), format, inMemory.lineage(),
                        inMemory.bytes(), inMemory.representation(),
                        inMemory.error(), inMemory.position(),
                        inMemory.errorAt());
            }
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
     * @return the new checkpoint and this batch's publishing
     * @throws Exception if the commit fails beyond retrying, or the previous
     *                   publishing failed
     */
    private BatchOutcome commitBatch(Batch batch, Checkpoint current,
            Future<Long> previousPublish) throws Exception
    {
        Telemetry telemetry = Telemetry.get();
        try (Scope ignored = batch.span.makeCurrent())
        {
            DataFile dataFile = batch.finish();
            hooks.batchWritten(batch.lastSeq);
            long published = await(previousPublish, current.publishedSeq());
            Checkpoint next = current.committed(batch.last.resume(),
                    batch.lastSeq, published);
            String encoded = codec.encode(next);
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
                            {
                                transaction.newFastAppend()
                                        .appendFile(dataFile).commit();
                                transaction.updateProperties()
                                        .set(checkpointKey, encoded).commit();
                            });
                        });
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
            telemetry.sectionsStored(batch.count, batch.format);
            telemetry.progress(next.resumeAt().offset(), next.fileSize());
            telemetry.audit("batch.committed",
                    Fields.of(Attr.FILE_ID, next.fileId())
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
            for (WrittenSection section : batch.writer.written())
            {
                messages.add(message(section, ref));
                if (section.error() != null)
                {
                    // Audited once committed, so a restart doesn't report it
                    // twice.
                    telemetry.auditWarn("section.failed",
                            Fields.of(Attr.FILE_ID, section.fileId())
                                    .with(Attr.SEQ, section.seq())
                                    .with(Attr.FORMAT, section.format())
                                    .with(Attr.ERROR_CODE, section.error())
                                    .with(Attr.OFFSET, section.sourceOffset())
                                    .with(Attr.POSITION,
                                            section.errorPosition()));
                    telemetry.sectionFailed(section.error(), section.format());
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
     * commit landed after all.
     *
     * @param next the checkpoint the commit carries
     * @return true if the stored checkpoint already has it
     */
    private boolean alreadyCommitted(Checkpoint next)
    {
        Optional<Checkpoint> stored = checkpoint();
        return stored.isPresent()
                && stored.get().fileId().equals(next.fileId())
                && stored.get().committedSeq() >= next.committedSeq()
                && stored.get().state() == next.state();
    }

    /**
     * The keyed fingerprint that recognises a file after a restart (design
     * §5.1).
     *
     * @param source the file
     * @return its fingerprint
     * @throws IOException if the file can't be read
     */
    public String fingerprint(SeekableByteSource source) throws IOException
    {
        return SourceFingerprint.of(source, ids);
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
     * Publishes the messages of every section of a checkpoint's file that is
     * committed but not known to be acknowledged
     * ({@code publishedSeq < seq <= committedSeq}). Repeats are harmless:
     * readers drop them by message id.
     *
     * @param current the checkpoint
     * @throws Exception if the sections can't be read or published
     */
    void announceCommitted(Checkpoint current) throws Exception
    {
        republish(current);
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
     * Commits a checkpoint without data (a new file, the end of a file, an
     * abandoned file). Doing it twice is harmless.
     *
     * @param next the checkpoint
     * @throws Exception if the commit fails beyond retrying
     */
    void commitCheckpoint(Checkpoint next) throws Exception
    {
        String encoded = codec.encode(next);
        settings.retry().call("storage.commit", RetryPolicy.TRANSIENT_STORAGE,
                attempt ->
                {
                    if (attempt > 1)
                    {
                        table.refresh();
                        if (encoded.equals(
                                table.properties().get(checkpointKey)))
                        {
                            return null;
                        }
                    }
                    return committer.commit(table, transaction -> transaction
                            .updateProperties().set(checkpointKey, encoded)
                            .commit());
                });
    }

    // ---- publishing ----

    /**
     * The message announcing a section written in a batch.
     *
     * @param section the section
     * @param ref     where it is stored
     * @return {@code section.failed} if its conversion failed, else
     *         {@code section.stored}
     */
    private static SectionInStorage message(WrittenSection section,
            StorageRef ref)
    {
        Representation representation = section.representation();
        if (section.error() != null)
        {
            return new SectionFailed(section.sectionId(), section.fileId(),
                    section.seq(), section.sourceOffset(),
                    section.sourceLength(), section.format(),
                    representation.contentType(), section.error().name(),
                    section.plaintextLength(), section.chunkCount(), ref,
                    section.origin(), section.errorAt());
        }
        return new SectionStored(section.sectionId(), section.fileId(),
                section.seq(), section.sourceOffset(), section.sourceLength(),
                section.format(), representation.contentType(),
                representation.mapping(), section.plaintextLength(),
                section.chunkCount(), ref, section.origin());
    }

    /**
     * Counts a file's committed sections that failed conversion and are
     * stored in their original form.
     *
     * @param fileId the file's id
     * @return how many there are
     */
    public long failedSectionCount(UUID fileId)
    {
        table.refresh();
        return CommittedSections.countFailed(table, fileId);
    }

    /**
     * Reads a file's stored sections that failed conversion.
     *
     * @param fileId the file
     * @return them, in sequence order
     */
    public List<CommittedSections.Failed> failedSections(UUID fileId)
    {
        table.refresh();
        return CommittedSections.failed(table, fileId);
    }

    /**
     * Rebuilds and publishes again the messages of sections committed but
     * not acknowledged before a restart.
     *
     * @param current the checkpoint
     * @throws Exception if the sections can't be read or published
     */
    private void republish(Checkpoint current) throws Exception
    {
        if (current.committedSeq() <= current.publishedSeq())
        {
            return;
        }
        long snapshotId = table.currentSnapshot().snapshotId();
        List<ConnectorMessage> messages = new ArrayList<>();
        for (CommittedSections.Row row : CommittedSections.read(table,
                current.fileId(), current.publishedSeq(),
                current.committedSeq()))
        {
            StorageRef ref = new StorageRef(settings.catalogName(), namespace,
                    tableName, row.ingestedDate(), snapshotId, row.kekRef());
            messages.add(row.errorCode() != null
                    ? new SectionFailed(row.sectionId(), current.fileId(),
                            row.seq(), row.sourceOffset(), row.sourceLength(),
                            row.format(), row.contentType(), row.errorCode(),
                            row.plaintextLength(), row.chunkCount(), ref,
                            row.origin(), row.errorAt())
                    : new SectionStored(row.sectionId(), current.fileId(),
                            row.seq(), row.sourceOffset(), row.sourceLength(),
                            row.format(), row.contentType(), row.mapping(),
                            row.plaintextLength(), row.chunkCount(), ref,
                            row.origin()));
        }
        Telemetry.get().audit("publish.republish",
                Fields.of(Attr.FILE_ID, current.fileId())
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
        telemetry.sectionsPublished(messages.stream()
                .filter(SectionInStorage.class::isInstance).count());
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
            OutboundMessage toSend = new OutboundMessage(message.messageId(),
                    message.type(), message.fileId(), get(sealed.get(index)));
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
