/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint.Failure;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Takes one landed item through the splitter (design §4.1, §5.3.6):
 * SPLITTING → COMPLETED, or FAILING → FAILED. The item ID is the file ID.
 * <p>
 * <b>Completing:</b> once every record is stored and published, the
 * checkpoint is {@code ARCHIVING} (the name is kept from when originals
 * were archived; it now means "split, finishing"); {@code file.completed}
 * is published, with the item's digest, and {@code COMPLETED} committed.
 * There is no archive step: the landed item is the original.
 * <p>
 * <b>Failing:</b> only a problem with the item itself fails it: it can't be
 * split, is over a limit, has stopped the splitter too often, or its
 * landed chunks are missing or altered. The decision is committed first
 * ({@code FAILING}, with stage, code and offset), so a restart finishes
 * failing rather than trying again. Records already stored stay, and are
 * announced. The item stays in landing.
 * <p>
 * Anything else (storage or the broker down, lease loss) is thrown, and
 * the item is tried again later from its checkpoint: it is safe in
 * landing, so there is no reason to fail it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ItemProcessor
{

    /** How an item ended. */
    public enum Result
    {
        /** Every record was stored and published. */
        COMPLETED,

        /** The item couldn't be split; see the failure. */
        FAILED,

        /** It was given up without finishing. */
        ABANDONED
    }

    /**
     * What happened to an item.
     *
     * @param itemId      the item's ID
     * @param result      how it ended
     * @param recordCount the records stored from it
     * @param failure     why it failed; null otherwise
     */
    public record Outcome(UUID itemId, Result result, long recordCount,
            Failure failure)
    {
    }

    /** Opens an item's bytes, only when they are needed. */
    @FunctionalInterface
    public interface Opener
    {

        /**
         * Opens the item.
         *
         * @return its bytes
         * @throws IOException if it can't be opened
         */
        SeekableByteSource open() throws IOException;
    }

    /** Splits, stores and publishes. */
    private final IngestPipeline pipeline;

    /**
     * Creates a processor.
     *
     * @param pipeline splits, stores and publishes
     */
    public ItemProcessor(IngestPipeline pipeline)
    {
        this.pipeline = pipeline;
    }

    /**
     * The item that must be finished before any other, if there is one.
     *
     * @return its checkpoint; empty if none is in progress
     */
    public Optional<Checkpoint> inProgress()
    {
        return pipeline.checkpoint()
                .filter(current -> current.state().inProgress());
    }

    /**
     * The checkpoint of the last item started, finished or not.
     *
     * @return its checkpoint; empty if no item has been started
     */
    public Optional<Checkpoint> last()
    {
        return pipeline.checkpoint();
    }

    /**
     * Counts an item's stored records that failed conversion and are kept
     * in their original form.
     *
     * @param itemId the item
     * @return how many there are
     */
    public long failedRecordCount(UUID itemId)
    {
        return pipeline.failedRecordCount(itemId);
    }

    /**
     * Reads an item's stored records that failed conversion, with where
     * each failed.
     *
     * @param itemId the item
     * @return them, in sequence order
     */
    public List<CommittedRecords.Failed> failedRecords(UUID itemId)
    {
        return pipeline.failedRecords(itemId);
    }

    /**
     * Processes an item: a new one, the one in progress, or one already
     * finished (which changes nothing). Traced as {@code ingest.item}.
     *
     * @param itemId the item's ID
     * @param digest its digest, reported in {@code file.completed}
     * @param opener opens its bytes
     * @return the outcome
     * @throws IllegalStateException if another item is in progress
     * @throws Exception             if storage or the broker fails; the
     *                               item is tried again later
     */
    public Outcome process(UUID itemId, String digest, Opener opener)
            throws Exception
    {
        Telemetry telemetry = Telemetry.get();
        Span span = telemetry.span("ingest.item",
                Fields.of(Attr.ITEM_ID, itemId)
                        .with(Attr.FORMAT, pipeline.settings().format()));
        telemetry.itemStartedProcessing();
        try (Scope ignored = span.makeCurrent())
        {
            Outcome outcome = run(itemId, digest, opener);
            span.setAttribute(Attr.RESULT.key(), outcome.result().name());
            if (outcome.failure() != null)
            {
                span.setAttribute(Attr.STAGE.key(),
                        outcome.failure().stage());
                Telemetry.fail(span, outcome.failure().code());
            }
            return outcome;
        }
        catch (Exception | Error failure)
        {
            Telemetry.fail(span, failure);
            throw failure;
        }
        finally
        {
            span.end();
            telemetry.itemFinishedProcessing();
        }
    }

    /**
     * Gives up an item in progress that won't be processed, such as a file
     * from before landing (design §5.3.6). Records already stored stay.
     *
     * @param checkpoint the item's checkpoint
     * @return the outcome
     * @throws Exception if the checkpoint can't be committed
     */
    public Outcome abandon(Checkpoint checkpoint) throws Exception
    {
        pipeline.commitCheckpoint(
                checkpoint.withState(Checkpoint.State.ABANDONED));
        Telemetry.get().auditWarn("item.abandoned",
                Fields.of(Attr.ITEM_ID, checkpoint.itemId())
                        .with(Attr.STATE, checkpoint.state())
                        .with(Attr.SEQ, checkpoint.committedSeq()));
        return new Outcome(checkpoint.itemId(), Result.ABANDONED,
                checkpoint.recordCount(), null);
    }

    /**
     * Picks up the item where its checkpoint left it.
     *
     * @param itemId the item's ID
     * @param digest its digest
     * @param opener opens its bytes
     * @return the outcome
     * @throws Exception if storage or the broker fails
     */
    private Outcome run(UUID itemId, String digest, Opener opener)
            throws Exception
    {
        Optional<Checkpoint> stored = pipeline.checkpoint()
                .filter(current -> current.itemId().equals(itemId));
        if (stored.isPresent())
        {
            Checkpoint checkpoint = stored.get();
            switch (checkpoint.state())
            {
                case COMPLETED ->
                {
                    return new Outcome(itemId, Result.COMPLETED,
                            checkpoint.recordCount(), null);
                }
                case FAILED ->
                {
                    return new Outcome(itemId, Result.FAILED,
                            checkpoint.recordCount(), checkpoint.failure());
                }
                case ABANDONED ->
                {
                    return new Outcome(itemId, Result.ABANDONED,
                            checkpoint.recordCount(), null);
                }
                case FAILING ->
                {
                    return finishFailure(checkpoint);
                }
                case ARCHIVING ->
                {
                    return finishCompletion(checkpoint, digest);
                }
                default ->
                {
                    // SPLITTING: resume below.
                }
            }
        }
        IngestPipeline.SplitResult split;
        long size = 0;
        try (SeekableByteSource source = opener.open())
        {
            size = source.size();
            split = pipeline.split(source, itemId);
        }
        catch (Exception failure)
        {
            if (!permanent(failure))
            {
                throw failure;
            }
            return fail(failingCheckpoint(itemId, size, failure));
        }
        return finishCompletion(split.checkpoint(), digest);
    }

    /**
     * Tells whether an error is the item's own, so trying again won't help.
     *
     * @param failure the error
     * @return true for a split or limit error, or a damaged or missing item
     */
    static boolean permanent(Exception failure)
    {
        return failure instanceof SplitException
                || failure instanceof ChunkIntegrityException
                || failure instanceof LandedItemSource.NotLandedException;
    }

    /**
     * Publishes {@code file.completed} and commits {@code COMPLETED}.
     *
     * @param checkpoint the item's {@code ARCHIVING} checkpoint
     * @param digest     the item's digest
     * @return the outcome
     * @throws Exception if the broker or storage fails
     */
    private Outcome finishCompletion(Checkpoint checkpoint, String digest)
            throws Exception
    {
        UUID itemId = checkpoint.itemId();
        long failedRecords = pipeline.failedRecordCount(itemId);
        Instant now = pipeline.clock().instant();
        pipeline.publishNow(List.of(new FileCompleted(itemId,
                checkpoint.recordCount(), failedRecords,
                checkpoint.itemSize(), digest, checkpoint.startedAt(), now)));
        pipeline.commitCheckpoint(
                checkpoint.withState(Checkpoint.State.COMPLETED));
        Telemetry.get().audit("item.completed",
                Fields.of(Attr.ITEM_ID, itemId)
                        .with(Attr.COUNT, checkpoint.recordCount())
                        .with(Attr.BYTES, checkpoint.itemSize()));
        Telemetry.get().itemCompleted();
        return new Outcome(itemId, Result.COMPLETED,
                checkpoint.recordCount(), null);
    }

    /**
     * Builds the {@code FAILING} checkpoint for an item's error: on its own
     * checkpoint if it has one, otherwise a new one.
     *
     * @param itemId  the item's ID
     * @param size    its size, if it could be opened; otherwise 0
     * @param failure the error
     * @return the checkpoint
     */
    private Checkpoint failingCheckpoint(UUID itemId, long size,
            Exception failure)
    {
        Instant now = pipeline.clock().instant();
        Checkpoint base = pipeline.checkpoint()
                .filter(current -> current.itemId().equals(itemId)
                        && current.state().inProgress())
                .orElseGet(() -> Checkpoint.start(itemId,
                        IngestPipeline.itemFingerprint(itemId),
                        pipeline.settings().format(), size, now));
        ErrorCode code = ErrorCode.SOURCE_READ_FAILED;
        String stage = "landing";
        long offset = base.resumeAt().offset();
        if (failure instanceof SplitException split)
        {
            code = split.code();
            offset = split.offset();
            stage = code == ErrorCode.LIMIT_ITEM_SIZE ? "intake" : "split";
        }
        Telemetry.get().auditWarn("item.failing",
                Fields.of(Attr.ITEM_ID, itemId).with(Attr.STAGE, stage)
                        .with(Attr.ERROR_CODE, code).with(Attr.OFFSET, offset)
                        .with(Attr.REASON, failure.getClass()
                                .getSimpleName()));
        return base.failing(new Failure(stage, code, offset, now));
    }

    /**
     * Commits the decision to fail, then finishes failing.
     *
     * @param failing the {@code FAILING} checkpoint
     * @return the outcome
     * @throws Exception if storage fails; the item is tried again later
     */
    private Outcome fail(Checkpoint failing) throws Exception
    {
        pipeline.commitCheckpoint(failing);
        return finishFailure(failing);
    }

    /**
     * Announces records stored before the failure, then commits
     * {@code FAILED}.
     *
     * @param failing the {@code FAILING} checkpoint
     * @return the outcome
     * @throws Exception if storage or the broker fails
     */
    private Outcome finishFailure(Checkpoint failing) throws Exception
    {
        if (failing.committedSeq() > failing.publishedSeq())
        {
            pipeline.announceCommitted(failing);
        }
        Failure failure = failing.failure();
        pipeline.commitCheckpoint(failing.failed());
        Telemetry.get().auditError("item.failed",
                Fields.of(Attr.ITEM_ID, failing.itemId())
                        .with(Attr.STAGE, failure.stage())
                        .with(Attr.ERROR_CODE, failure.code())
                        .with(Attr.OFFSET, failure.offset())
                        .with(Attr.SEQ, failing.committedSeq()));
        Telemetry.get().itemFailed(failure.code(), failure.stage());
        return new Outcome(failing.itemId(), Result.FAILED,
                failing.recordCount(), failure);
    }
}
