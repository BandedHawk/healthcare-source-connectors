/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemSplit;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint.Failure;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.landing.LandedRefs;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
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
 * Takes one landed item through the split stage (design §5.3.6, STAGES.md
 * §8): SPLITTING → COMPLETED, or FAILING → FAILED. The caller holds the
 * item's lease.
 * <p>
 * <b>Completing:</b> once every record is stored and its
 * {@code record.split} acknowledged, the checkpoint is {@code ARCHIVING}
 * (the name is kept from when originals were archived; it now means
 * "split, finishing"); {@code item.split} is sent, with the item's digest,
 * and {@code COMPLETED} written.
 * <p>
 * <b>Failing:</b> only a problem with the item itself fails it: it can't be
 * split, is over a limit, has stopped workers too often, or its landed
 * chunks are missing or altered. The decision is written first
 * ({@code FAILING}, with stage, code and offset), so a worker that stops
 * finishes failing next time rather than trying again. Records already
 * stored stay, and are announced; {@code item.split} carries the failure.
 * The item stays in landing.
 * <p>
 * <b>Finished already:</b> an item delivered again after it finished has
 * its {@code item.split} sent again (its ID is fixed, so the next stage
 * drops a repeat), in case the first was lost.
 * <p>
 * Anything else (storage or the broker down, the lease lost) is thrown,
 * and the item is tried again later from its checkpoint: it is safe in
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
        /** Every record was stored and announced. */
        COMPLETED,

        /** The item couldn't be split to its end; see the failure. */
        FAILED
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
         * @throws IOException if they can't be read
         */
        SeekableByteSource open() throws IOException;
    }

    /**
     * What came with an item from landing.
     *
     * @param digest its digest, from its {@code item.landed}
     * @param refs   what landing kept about it for its result
     */
    private record Landing(String digest, LandedRefs refs)
    {
    }

    /** Splits, stores and announces. */
    private final IngestPipeline pipeline;

    /** The intake the items came in by, for {@code item.split}. */
    private final String intake;

    /**
     * Creates a processor.
     *
     * @param pipeline splits, stores and announces
     * @param intake   the intake the items came in by
     */
    public ItemProcessor(IngestPipeline pipeline, String intake)
    {
        this.pipeline = pipeline;
        this.intake = intake;
    }

    /**
     * Reads an item's checkpoint.
     *
     * @param itemId the item
     * @return the checkpoint; empty if the item was never started
     */
    public Optional<Checkpoint> checkpoint(UUID itemId)
    {
        return pipeline.checkpoint(itemId);
    }

    /**
     * Counts an item's stored records the splitter couldn't parse.
     *
     * @param itemId the item
     * @return how many there are
     */
    public long failedRecordCount(UUID itemId)
    {
        return pipeline.failedRecordCount(itemId);
    }

    /**
     * Reads an item's stored records that carry an error.
     *
     * @param itemId the item
     * @return them, in sequence order
     */
    public List<CommittedRecords.Failed> failedRecords(UUID itemId)
    {
        return pipeline.failedRecords(itemId);
    }

    /**
     * Splits an item with nothing kept by landing for its result.
     *
     * @param itemId the item's ID
     * @param digest the item's digest, from its {@code item.landed}
     * @param opener opens the item's bytes, if they are needed
     * @param lease  the item's lease, held by the caller
     * @return how it ended
     * @throws Exception if storage or the broker fails; the item is tried
     *                   again later
     */
    public Outcome process(UUID itemId, String digest, Opener opener,
            LeaseToken lease) throws Exception
    {
        return process(itemId, digest, opener, lease, LandedRefs.NONE);
    }

    /**
     * Splits an item, or finishes or announces again one already started.
     *
     * @param itemId the item's ID
     * @param digest the item's digest, from its {@code item.landed}
     * @param opener opens the item's bytes, if they are needed
     * @param lease  the item's lease, held by the caller
     * @param refs   what landing kept about the item for its result,
     *               passed on in {@code item.split} and each
     *               {@code record.split} (STAGES.md §9)
     * @return how it ended
     * @throws Exception if storage or the broker fails; the item is tried
     *                   again later
     */
    public Outcome process(UUID itemId, String digest, Opener opener,
            LeaseToken lease, LandedRefs refs) throws Exception
    {
        Telemetry telemetry = Telemetry.get();
        Span span = telemetry.span("ingest.item",
                Fields.of(Attr.ITEM_ID, itemId)
                        .with(Attr.FORMAT, pipeline.settings().format()));
        telemetry.itemStartedProcessing();
        try (Scope ignored = span.makeCurrent())
        {
            Outcome outcome = run(itemId, new Landing(digest, refs), opener,
                    lease);
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
     * Takes an item on from wherever its checkpoint is.
     *
     * @param itemId  the item's ID
     * @param landing what came with it from landing
     * @param opener  opens its bytes
     * @param lease   its lease
     * @return how it ended
     * @throws Exception if storage or the broker fails
     */
    private Outcome run(UUID itemId, Landing landing, Opener opener,
            LeaseToken lease) throws Exception
    {
        Optional<ItemCheckpoints.Stored> stored = pipeline.stored(itemId);
        if (stored.isPresent())
        {
            Checkpoint checkpoint = stored.get().checkpoint();
            Instant endedAt = stored.get().endedAt();
            switch (checkpoint.state())
            {
                case COMPLETED ->
                {
                    announce(checkpoint, landing, endedAt);
                    return new Outcome(itemId, Result.COMPLETED,
                            checkpoint.recordCount(), null);
                }
                case FAILED ->
                {
                    announce(checkpoint, landing, endedAt);
                    return new Outcome(itemId, Result.FAILED,
                            checkpoint.recordCount(), checkpoint.failure());
                }
                case FAILING ->
                {
                    return finishFailure(checkpoint, landing, lease);
                }
                case ARCHIVING ->
                {
                    return finishCompletion(checkpoint, landing, lease);
                }
                default ->
                {
                    // SPLITTING, or ABANDONED from before contract 3.0:
                    // split (again) below.
                }
            }
        }
        IngestPipeline.SplitResult split;
        long size = 0;
        try (SeekableByteSource source = opener.open())
        {
            size = source.size();
            split = pipeline.split(source, itemId, lease, landing.refs());
        }
        catch (Exception failure)
        {
            if (!permanent(failure))
            {
                throw failure;
            }
            return fail(failingCheckpoint(itemId, size, failure), landing,
                    lease);
        }
        return finishCompletion(split.checkpoint(), landing, lease);
    }

    /**
     * Tells whether an error is the item's own fault, so it is failed
     * rather than tried again.
     *
     * @param failure the error
     * @return true if it can't be split, or its landed copy is bad
     */
    static boolean permanent(Exception failure)
    {
        return failure instanceof SplitException
                || failure instanceof ChunkIntegrityException
                || failure instanceof LandedItemSource.NotLandedException;
    }

    /**
     * Sends {@code item.split} and writes {@code COMPLETED}.
     *
     * @param checkpoint the {@code ARCHIVING} checkpoint
     * @param landing    what came with the item from landing
     * @param lease      its lease
     * @return the outcome
     * @throws Exception if the broker or the store fails
     */
    private Outcome finishCompletion(Checkpoint checkpoint, Landing landing,
            LeaseToken lease) throws Exception
    {
        UUID itemId = checkpoint.itemId();
        announce(checkpoint, landing, endedAt(itemId));
        pipeline.commitCheckpoint(
                checkpoint.withState(Checkpoint.State.COMPLETED), lease);
        Telemetry.get().audit("item.completed",
                Fields.of(Attr.ITEM_ID, itemId)
                        .with(Attr.COUNT, checkpoint.recordCount())
                        .with(Attr.BYTES, checkpoint.itemSize()));
        Telemetry.get().itemCompleted();
        return new Outcome(itemId, Result.COMPLETED,
                checkpoint.recordCount(), null);
    }

    /**
     * When an item's splitting ended, as its stored checkpoint notes. Every
     * {@code item.split} for the item carries this time, so one sent again
     * is the same as the first.
     *
     * @param itemId the item, past {@code SPLITTING}
     * @return the time; null if none is noted
     */
    private Instant endedAt(UUID itemId)
    {
        return pipeline.stored(itemId).map(ItemCheckpoints.Stored::endedAt)
                .orElse(null);
    }

    /**
     * Sends an item's {@code item.split}: its totals, the producer's
     * references from landing, and its failure if it failed, placed in its
     * message for a batch.
     *
     * @param checkpoint its checkpoint
     * @param landing    what came with it from landing
     * @param endedAt    when its splitting ended, as stored
     * @throws Exception if the broker fails
     */
    private void announce(Checkpoint checkpoint, Landing landing,
            Instant endedAt) throws Exception
    {
        Failure failure = checkpoint.failure();
        LandedRefs refs = landing.refs();
        Problem problem = failure == null ? null
                : refs.place(new Problem(failure.code().name(),
                        failure.stage(), -1, failure.offset(), null, -1,
                        null, null));
        Instant finishedAt = endedAt != null ? endedAt
                : failure != null ? failure.failedAt()
                : pipeline.clock().instant();
        pipeline.publishNow(List.of(new ItemSplit(checkpoint.itemId(),
                intake, checkpoint.recordCount(),
                pipeline.failedRecordCount(checkpoint.itemId()),
                checkpoint.itemSize(), landing.digest(), refs.producerRef(),
                refs.replaces(), problem, checkpoint.startedAt(),
                finishedAt)));
    }

    /**
     * The {@code FAILING} checkpoint for an item that can't be split.
     *
     * @param itemId  the item
     * @param size    its size, if it was opened
     * @param failure why it can't be
     * @return the checkpoint
     */
    private Checkpoint failingCheckpoint(UUID itemId, long size,
            Exception failure)
    {
        Instant now = pipeline.clock().instant();
        Checkpoint base = pipeline.checkpoint(itemId)
                .filter(current -> current.state().inProgress())
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
     * Writes the decision to fail, then finishes failing.
     *
     * @param failing the {@code FAILING} checkpoint
     * @param landing what came with the item from landing
     * @param lease   its lease
     * @return the outcome
     * @throws Exception if the store or the broker fails
     */
    private Outcome fail(Checkpoint failing, Landing landing,
            LeaseToken lease) throws Exception
    {
        pipeline.commitCheckpoint(failing, lease);
        return finishFailure(failing, landing, lease);
    }

    /**
     * Announces the records already stored, sends {@code item.split} with
     * the failure, and writes {@code FAILED}.
     *
     * @param failing the {@code FAILING} checkpoint
     * @param landing what came with the item from landing
     * @param lease   its lease
     * @return the outcome
     * @throws Exception if the store or the broker fails
     */
    private Outcome finishFailure(Checkpoint failing, Landing landing,
            LeaseToken lease) throws Exception
    {
        if (failing.committedSeq() > failing.publishedSeq())
        {
            pipeline.announceCommitted(failing, landing.refs());
        }
        Failure failure = failing.failure();
        announce(failing, landing, endedAt(failing.itemId()));
        pipeline.commitCheckpoint(failing.failed(), lease);
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
