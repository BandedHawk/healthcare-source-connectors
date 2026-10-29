package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointCodec;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
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
import java.util.Deque;
import java.util.List;
import java.util.Optional;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The source-agnostic split phase of ingestion (design §4, §8.3, §9.2): read → split → hash/encrypt → write →
 * fenced commit with checkpoint → publish. One file at a time; one instance per process.
 *
 * <p>Per file:
 * <ol>
 *   <li>A new file gets a UUIDv7 and a first checkpoint ({@code SPLITTING}) committed before anything else;
 *       a file matching an in-progress checkpoint's fingerprint is resumed instead.</li>
 *   <li>{@code file.started} is published (again on resume; brokers and downstream dedup by message ID).</li>
 *   <li>On resume, messages for committed-but-unpublished sections are rebuilt from table columns and
 *       republished.</li>
 *   <li>Sections are encrypted on a worker pool (single-chunk sections) or inline (larger ones) and written
 *       in seq order. A batch is committed when it reaches {@code commitMaxSections}, {@code commitMaxBytes} or
 *       {@code commitMaxInterval}: data file plus checkpoint in one fenced commit.</li>
 *   <li>The batch's messages are published in the background while the next batch is split. The next commit
 *       waits for that and records the acknowledged seq as {@code publishedSeq}.</li>
 *   <li>At the end, a data-free commit moves the checkpoint to {@code ARCHIVING}.</li>
 * </ol>
 *
 * <p>Errors: a {@link SplitException} means the file can't be split (fail the file). Transient storage and
 * broker errors are retried per {@link RetryPolicy}. Lease loss and {@code CommitStateUnknownException} are
 * not caught: the process should exit and restart from the checkpoint.
 */
public final class IngestPipeline implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(IngestPipeline.class);

    /** Outcome of the split phase. The checkpoint is in state {@code ARCHIVING}. */
    public record SplitResult(UUID fileId, long sectionCount, Checkpoint checkpoint) {}

    private final PipelineSettings settings;
    private final Table table;
    private final FencedCommitter committer;
    private final KeyProvider keys;
    private final SectionIds ids;
    private final CheckpointCodec codec;
    private final SplitterRegistry splitters;
    private final MessageSealer sealer;
    private final MessagePublisher publisher;
    private final Clock clock;
    private final String checkpointKey;
    private final String namespace;
    private final String tableName;
    private final ExecutorService workers;
    private final ExecutorService publishing;
    private PipelineHooks hooks = PipelineHooks.NONE;

    public IngestPipeline(PipelineSettings settings, Table sectionsTable, FencedCommitter committer, KeyProvider keys,
            SectionIds ids, SplitterRegistry splitters, MessageSealer sealer, MessagePublisher publisher, Clock clock) {
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
        String[] nt = namespaceAndTable(sectionsTable.name());
        this.namespace = nt[0];
        this.tableName = nt[1];
        splitters.get(settings.format()).validate(settings.split());
        this.workers = Executors.newFixedThreadPool(settings.encryptWorkers(),
                Thread.ofPlatform().name("sdh-encrypt-", 0).daemon(true).factory());
        this.publishing = Executors.newSingleThreadExecutor(
                Thread.ofPlatform().name("sdh-publish").daemon(true).factory());
    }

    void setHooks(PipelineHooks hooks) {
        this.hooks = hooks;
    }

    /** The stored checkpoint, verified. Empty if this connector has never started a file. */
    public Optional<Checkpoint> checkpoint() {
        table.refresh();
        String v = table.properties().get(checkpointKey);
        return v == null ? Optional.empty() : Optional.of(codec.decode(v));
    }

    /**
     * Closes an in-progress checkpoint whose file has disappeared (design §5, startup). Sections already
     * stored stay where they are.
     */
    public void abandonInProgress() throws Exception {
        Optional<Checkpoint> cp = checkpoint();
        if (cp.isPresent() && cp.get().state() == Checkpoint.State.SPLITTING) {
            commitCheckpoint(cp.get().withState(Checkpoint.State.ABANDONED));
            Telemetry.get().auditWarn("file.abandoned", Fields.of(Attr.FILE_ID, cp.get().fileId())
                    .with(Attr.SEQ, cp.get().committedSeq()));
        }
    }

    /**
     * Runs the split phase for {@code src}: from the start for a new file, or from the checkpoint if this is the
     * file in progress. Returns once every section is committed and published.
     *
     * @throws IllegalStateException if a different file is still splitting or archiving; finish or abandon it first
     */
    public SplitResult split(SeekableByteSource src) throws Exception {
        String fingerprint = fingerprint(src);
        Checkpoint cp = checkpoint().orElse(null);
        if (cp != null && cp.fingerprint().equals(fingerprint) && cp.state() == Checkpoint.State.ARCHIVING) {
            return new SplitResult(cp.fileId(), cp.sectionCount(), cp);
        }
        if (cp != null && cp.state().inProgress() && cp.state() != Checkpoint.State.SPLITTING) {
            throw new IllegalStateException("File " + cp.fileId() + " is still " + cp.state());
        }
        if (cp != null && cp.state() == Checkpoint.State.SPLITTING) {
            if (!cp.fingerprint().equals(fingerprint)) {
                throw new IllegalStateException("File " + cp.fileId() + " is still in progress");
            }
            if (!cp.format().equals(settings.format())) {
                throw new IllegalStateException("Split format changed while file " + cp.fileId() + " is in progress");
            }
            Telemetry.get().audit("file.resumed", Fields.of(Attr.FILE_ID, cp.fileId()).with(Attr.SEQ, cp.nextSeq())
                    .with(Attr.OFFSET, cp.resumeAt().offset()));
        } else {
            if (src.size() > settings.split().limits().maxFileBytes()) {
                // Rejected before ingestion (design §6.3): no checkpoint, no file.started.
                throw new SplitException(ErrorCode.LIMIT_FILE_SIZE, 0);
            }
            cp = Checkpoint.start(UuidV7.next(clock.millis()), fingerprint, settings.format(), src.size(),
                    clock.instant());
            commitCheckpoint(cp);
            Telemetry.get().audit("file.started", Fields.of(Attr.FILE_ID, cp.fileId()).with(Attr.FORMAT, cp.format())
                    .with(Attr.BYTES, cp.fileSize()));
        }
        return run(cp, src);
    }

    private SplitResult run(Checkpoint start, SeekableByteSource src) throws Exception {
        publish(List.of(new FileStarted(start.fileId(), start.format(), start.fileSize(), start.startedAt(),
                settings.connectorId())));
        republish(start);
        Checkpoint cp = start.published(start.committedSeq());

        Batch batch = null;
        Future<Long> inFlightPublish = null;
        try (SplitSession session = splitters.open(cp.format(), src, cp.resumeAt(), cp.nextSeq(), settings.split())) {
            long seq = cp.nextSeq();
            Optional<Section> next;
            while ((next = session.next()).isPresent()) {
                if (batch == null) {
                    batch = new Batch(cp.fileId(), cp.format());
                }
                batch.add(seq++, next.get(), src);
                if (batch.full()) {
                    BatchOutcome o = commitBatch(batch, cp, inFlightPublish);
                    cp = o.checkpoint();
                    inFlightPublish = o.publish();
                    batch = null;
                }
            }
            if (batch != null) {
                BatchOutcome o = commitBatch(batch, cp, inFlightPublish);
                cp = o.checkpoint();
                inFlightPublish = o.publish();
                batch = null;
            }
            cp = cp.published(await(inFlightPublish, cp.publishedSeq()));
            inFlightPublish = null;
            hooks.beforeEndOfFile();
            cp = cp.withState(Checkpoint.State.ARCHIVING);
            commitCheckpoint(cp);
            Telemetry.get().audit("file.split", Fields.of(Attr.FILE_ID, cp.fileId()).with(Attr.COUNT, cp.sectionCount()));
            return new SplitResult(cp.fileId(), cp.sectionCount(), cp);
        } finally {
            if (batch != null) {
                batch.abandon();
            }
            if (inFlightPublish != null) {
                // Let the committed batch finish publishing rather than cut it off: if this file now fails, nothing
                // would republish it later (a restart does, a failure doesn't).
                try {
                    inFlightPublish.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException ignored) {
                    // reported by FileProcessor, which re-announces anything committed but unacknowledged
                }
            }
        }
    }

    // ---- batches ----

    private record BatchOutcome(Checkpoint checkpoint, Future<Long> publish) {}

    /** One commit batch: sections are added in seq order; single-chunk ones are encrypted on the worker pool. */
    private final class Batch {
        private final SectionBatchWriter writer =
                new SectionBatchWriter(table, keys, settings.kekRef(), ids, clock.instant());
        private final Deque<Future<EncryptedSection>> pending = new ArrayDeque<>();
        private final long openedAt = clock.millis();
        private final int maxPending = settings.encryptWorkers() * 4;
        private final UUID fileId;
        private final String format;
        private final Span span;
        private long lastSeq = -1;
        private long bytes;
        private int count;
        private Section last;

        Batch(UUID fileId, String format) {
            this.fileId = fileId;
            this.format = format;
            this.span = Telemetry.get().span("split.batch", Fields.of(Attr.FILE_ID, fileId).with(Attr.FORMAT, format));
        }

        void add(long seq, Section section, SeekableByteSource src) throws Exception {
            long len = section.plaintextLength();
            if (SectionCrypto.chunkCount(len) == 1) {
                pending.add(workers.submit(() -> writer.encryptSmall(fileId, seq, format, section, src)));
                while (pending.size() > maxPending) {
                    writer.write(get(pending.poll()));
                }
            } else {
                drain();
                writer.add(fileId, seq, format, section, src);
            }
            lastSeq = seq;
            last = section;
            bytes += len;
            count++;
            Telemetry.get().bytesRead(section.sourceLength(), "split");
        }

        boolean full() {
            return count >= settings.commitMaxSections() || bytes >= settings.commitMaxBytes()
                    || clock.millis() - openedAt >= settings.commitMaxInterval().toMillis();
        }

        DataFile finish() throws Exception {
            drain();
            return writer.finish();
        }

        void drain() throws Exception {
            while (!pending.isEmpty()) {
                writer.write(get(pending.poll()));
            }
        }

        void abandon() {
            pending.forEach(f -> f.cancel(true));
            writer.close();
            span.setAttribute(Attr.RESULT.key(), "abandoned");
            span.setStatus(StatusCode.ERROR);
            span.end();
        }
    }

    private BatchOutcome commitBatch(Batch batch, Checkpoint cp, Future<Long> previousPublish) throws Exception {
        Telemetry t = Telemetry.get();
        try (Scope ignored = batch.span.makeCurrent()) {
            DataFile dataFile = batch.finish();
            hooks.batchWritten(batch.lastSeq);
            long published = await(previousPublish, cp.publishedSeq());
            Checkpoint next = cp.committed(batch.last.resume(), batch.lastSeq, published);
            String encoded = codec.encode(next);
            Span commit = t.span("storage.commit", Fields.of(Attr.TABLE, table.name()).with(Attr.COUNT, batch.count));
            long started = System.nanoTime();
            Snapshot snapshot;
            try (Scope c = commit.makeCurrent()) {
                snapshot = settings.retry().call("storage.commit", RetryPolicy.TRANSIENT_STORAGE, attempt -> {
                    if (attempt > 1 && alreadyCommitted(next)) {
                        return table.currentSnapshot();
                    }
                    return committer.commit(table, tx -> {
                        tx.newFastAppend().appendFile(dataFile).commit();
                        tx.updateProperties().set(checkpointKey, encoded).commit();
                    });
                });
            } catch (Exception | Error e) {
                Telemetry.fail(commit, e);
                throw e;
            } finally {
                commit.end();
            }
            t.commitDuration(System.nanoTime() - started, table.name());
            t.sectionsStored(batch.count, batch.format);
            t.progress(next.resumeAt().offset(), next.fileSize());
            t.audit("batch.committed", Fields.of(Attr.FILE_ID, next.fileId()).with(Attr.SEQ, next.committedSeq())
                    .with(Attr.OFFSET, next.resumeAt().offset()).with(Attr.COUNT, batch.count));
            batch.span.setAttribute(Attr.SEQ.key(), next.committedSeq());
            batch.span.setAttribute(Attr.COUNT.key(), batch.count);
            hooks.batchCommitted(batch.lastSeq);

            LocalDate day = batch.writer.ingestedDate();
            StorageRef ref = new StorageRef(settings.catalogName(), namespace, tableName, day, snapshot.snapshotId(),
                    batch.writer.kekRef());
            List<ConnectorMessage> messages = new ArrayList<>(batch.count);
            for (WrittenSection w : batch.writer.written()) {
                messages.add(new SectionStored(w.sectionId(), w.fileId(), w.seq(), w.sourceOffset(), w.sourceLength(),
                        w.format(), w.plaintextLength(), w.chunkCount(), ref));
            }
            long last = batch.lastSeq;
            // Carry the trace across to the publishing thread, so publish.batch is a child of this batch.
            Future<Long> publish = publishing.submit(Context.current().wrap(() -> {
                publish(messages);
                return last;
            }));
            return new BatchOutcome(next, publish);
        } catch (Exception | Error e) {
            Telemetry.fail(batch.span, e);
            throw e;
        } finally {
            batch.span.end();
        }
    }

    /** After an ambiguous failure: did a previous attempt of this commit land after all? */
    private boolean alreadyCommitted(Checkpoint next) {
        Optional<Checkpoint> stored = checkpoint();
        return stored.isPresent() && stored.get().fileId().equals(next.fileId())
                && stored.get().committedSeq() >= next.committedSeq()
                && stored.get().state() == next.state();
    }

    /** The keyed fingerprint used to recognise a source after a restart (design §5.1). */
    public String fingerprint(SeekableByteSource src) throws IOException {
        return SourceFingerprint.of(src, ids);
    }

    PipelineSettings settings() {
        return settings;
    }

    Clock clock() {
        return clock;
    }

    /**
     * Publishes {@code section.stored} for every section of {@code cp}'s file that is committed but not known to be
     * acknowledged ({@code publishedSeq < seq <= committedSeq}). Duplicates are harmless (message ID dedup).
     */
    void announceCommitted(Checkpoint cp) throws Exception {
        republish(cp);
    }

    /** Seals and publishes messages now, waiting for acknowledgement. */
    void publishNow(List<ConnectorMessage> messages) throws Exception {
        publish(messages);
    }

    /** A data-free checkpoint commit (new file, end of file, abandon). Idempotent. */
    void commitCheckpoint(Checkpoint next) throws Exception {
        String encoded = codec.encode(next);
        settings.retry().call("storage.commit", RetryPolicy.TRANSIENT_STORAGE, attempt -> {
            if (attempt > 1) {
                table.refresh();
                if (encoded.equals(table.properties().get(checkpointKey))) {
                    return null;
                }
            }
            return committer.commit(table, tx -> tx.updateProperties().set(checkpointKey, encoded).commit());
        });
    }

    // ---- publishing ----

    /** Rebuilds and republishes messages for sections committed but not acknowledged before a restart. */
    private void republish(Checkpoint cp) throws Exception {
        if (cp.committedSeq() <= cp.publishedSeq()) {
            return;
        }
        long snapshotId = table.currentSnapshot().snapshotId();
        List<ConnectorMessage> messages = new ArrayList<>();
        for (CommittedSections.Row r : CommittedSections.read(table, cp.fileId(), cp.publishedSeq(),
                cp.committedSeq())) {
            StorageRef ref = new StorageRef(settings.catalogName(), namespace, tableName, r.ingestedDate(), snapshotId,
                    r.kekRef());
            messages.add(new SectionStored(r.sectionId(), cp.fileId(), r.seq(), r.sourceOffset(), r.sourceLength(),
                    r.format(), r.plaintextLength(), r.chunkCount(), ref));
        }
        Telemetry.get().audit("publish.republish", Fields.of(Attr.FILE_ID, cp.fileId())
                .with(Attr.SEQ, cp.publishedSeq() + 1).with(Attr.COUNT, messages.size()));
        publish(messages);
    }

    /**
     * Seals messages on the worker pool, sends them in order, and waits for every acknowledgement. Messages
     * whose send fails are retried one by one with backoff.
     */
    private void publish(List<ConnectorMessage> messages) throws Exception {
        Telemetry t = Telemetry.get();
        Span span = t.span("publish.batch", Fields.of(Attr.COUNT, messages.size()));
        long started = System.nanoTime();
        try (Scope ignored = span.makeCurrent()) {
            sendAll(messages);
        } catch (Exception | Error e) {
            Telemetry.fail(span, e instanceof PublishFailedException ? ErrorCode.PUBLISH_FAILED : null);
            throw e;
        } finally {
            span.end();
        }
        t.publishDuration(System.nanoTime() - started);
        t.sectionsPublished(messages.stream().filter(SectionStored.class::isInstance).count());
    }

    private void sendAll(List<ConnectorMessage> messages) throws Exception {
        List<CompletableFuture<byte[]>> sealed = new ArrayList<>(messages.size());
        for (ConnectorMessage m : messages) {
            sealed.add(CompletableFuture.supplyAsync(() -> sealer.seal(m), workers));
        }
        List<OutboundMessage> outbound = new ArrayList<>(messages.size());
        List<CompletableFuture<Void>> acks = new ArrayList<>(messages.size());
        for (int i = 0; i < messages.size(); i++) {
            ConnectorMessage m = messages.get(i);
            OutboundMessage om = new OutboundMessage(m.messageId(), m.type(), m.fileId(), get(sealed.get(i)));
            outbound.add(om);
            acks.add(send(om));
        }
        for (int i = 0; i < acks.size(); i++) {
            try {
                acks.get(i).get();
            } catch (ExecutionException e) {
                OutboundMessage om = outbound.get(i);
                try {
                    settings.retry().call("publish", ex -> !(ex instanceof InterruptedException),
                            attempt -> get(send(om)));
                } catch (InterruptedException ie) {
                    throw ie;
                } catch (Exception failed) {
                    throw new PublishFailedException(failed);
                }
            }
        }
    }

    private CompletableFuture<Void> send(OutboundMessage om) {
        try {
            return publisher.publish(om);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // ---- helpers ----

    private static <T> T get(Future<T> f) throws Exception {
        try {
            return f.get();
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof Exception ex) {
                throw ex;
            }
            throw (Error) c;
        }
    }

    private static long await(Future<Long> publish, long fallback) throws Exception {
        return publish == null ? fallback : get(publish);
    }

    /** Splits Iceberg's {@code catalog.namespace.table} name into namespace and table. */
    static String[] namespaceAndTable(String fullName) {
        int firstDot = fullName.indexOf('.');
        int lastDot = fullName.lastIndexOf('.');
        if (firstDot < 0 || firstDot == lastDot) {
            throw new IllegalArgumentException("Expected catalog.namespace.table, got " + fullName);
        }
        return new String[] {fullName.substring(firstDot + 1, lastDot), fullName.substring(lastDot + 1)};
    }

    @Override
    public void close() {
        publishing.shutdownNow();
        workers.shutdownNow();
        try {
            publishing.awaitTermination(30, TimeUnit.SECONDS);
            workers.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
