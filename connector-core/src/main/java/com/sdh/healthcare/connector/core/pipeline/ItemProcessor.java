/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.contract.crypto.ChunkIntegrityException;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.HoldReleases;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint.Failure;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /** The processor's log. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ItemProcessor.class);

    /** The stage's name in {@code stage.item_completed}, as for the others. */
    public static final String STAGE = "split";

    /** How an item ended. */
    public enum Result
    {
        /** Every record was stored and announced. */
        COMPLETED,

        /** The item couldn't be split to its end; see the failure. */
        FAILED,

        /**
         * The item is part way, begun under another version, and waits for
         * an operator (STAGES.md §14.3).
         */
        HELD,

        /**
         * The item was held, and an operator closed it, to land it again
         * (STAGES.md §15.6e): nothing more is done with it.
         */
        CLOSED
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

    /** The split stage's fail store; null without a database. */
    private final FailureStore failures;

    /** Operators' corrections of its failures; null for none. */
    private final CorrectionStore corrections;

    /**
     * Creates a processor with no fail store: each failure counts as open.
     *
     * @param pipeline splits, stores and announces
     * @param intake   the intake the items came in by
     */
    public ItemProcessor(IngestPipeline pipeline, String intake)
    {
        this(pipeline, intake, null);
    }

    /**
     * Creates a processor.
     *
     * @param pipeline splits, stores and announces
     * @param intake   the intake the items came in by
     * @param failures the split stage's fail store (STAGES.md §15.3); null
     *                 for none
     */
    public ItemProcessor(IngestPipeline pipeline, String intake,
            FailureStore failures)
    {
        this(pipeline, intake, failures, null);
    }

    /**
     * Creates a processor that also applies operators' corrections.
     *
     * @param pipeline    splits, stores and announces
     * @param intake      the intake the items came in by
     * @param failures    the split stage's fail store; null for none
     * @param corrections operators' corrections of its failures (STAGES.md
     *                    §15.6d); null for none
     */
    public ItemProcessor(IngestPipeline pipeline, String intake,
            FailureStore failures, CorrectionStore corrections)
    {
        this.pipeline = pipeline;
        this.intake = intake;
        this.failures = failures;
        this.corrections = corrections;
    }

    /**
     * Applies an operator's correction of one of an item's failures
     * (STAGES.md §15.6d): splits and stores it, sends its records, notes
     * its outcome in the fail store, then sends the item's end again, as
     * the store now counts it. A pod that stops part way leaves the
     * correction pending, or its end not sent; both are done again.
     *
     * @param pending the correction
     * @param digest  the item's digest, from landing
     * @param refs    what landing kept about the item
     * @param lease   the item's lease, held by the caller
     * @return what it came to
     * @throws IllegalStateException if the item's splitting hasn't ended,
     *                               or the processor takes no corrections
     * @throws Exception             if storage or the broker fails; it is
     *                               tried again later
     */
    public CorrectionStore.Outcome correct(CorrectionStore.Pending pending,
            String digest, LandedRefs refs, LeaseToken lease) throws Exception
    {
        if (corrections == null)
        {
            throw new IllegalStateException("No correction store");
        }
        Checkpoint checkpoint = ended(pending.itemId());
        IngestPipeline.CorrectionSplit split = pipeline.splitCorrection(
                checkpoint, pending, refs, lease);
        if (!split.messages().isEmpty())
        {
            pipeline.publishNow(split.messages());
        }
        corrections.finish(split.outcome());
        reannounce(pending.itemId(), digest, refs);
        return split.outcome();
    }

    /**
     * The split stage's version, as this processor splits under it.
     *
     * @return the version; null if none is set
     */
    public String version()
    {
        return pipeline.settings().version();
    }

    /**
     * Ends the hold on an item held across versions, as an operator asked
     * (STAGES.md §15.6e): see {@link IngestPipeline#release}.
     *
     * @param itemId   the item
     * @param action   what the operator asked
     * @param operator who asked
     * @param lease    the item's lease, held by the caller
     * @return the checkpoint as it was held; empty if it isn't held
     */
    public Optional<Checkpoint> release(UUID itemId,
            HoldReleases.Action action, String operator, LeaseToken lease)
    {
        return pipeline.release(itemId, action, operator, lease);
    }

    /**
     * Sends an item's end again after a change in its failures (a
     * correction, or an operator's decision), as the fail store now counts
     * them, and notes it sent for its corrections.
     *
     * @param itemId the item
     * @param digest its digest, from landing
     * @param refs   what landing kept about it
     * @throws IllegalStateException if its splitting hasn't ended
     * @throws Exception             if the store or the broker fails
     */
    public void reannounce(UUID itemId, String digest, LandedRefs refs)
            throws Exception
    {
        Checkpoint checkpoint = ended(itemId);
        Ended ended = announce(checkpoint, new Landing(digest, refs),
                endedAt(itemId));
        auditEnded(ended, checkpoint.state() == Checkpoint.State.COMPLETED
                || wholeItemCorrected(itemId) ? Result.COMPLETED
                        : Result.FAILED);
        if (corrections != null)
        {
            corrections.markNotified(itemId);
        }
    }

    /**
     * An item's checkpoint, its splitting ended.
     *
     * @param itemId the item
     * @return the checkpoint
     * @throws IllegalStateException if it hasn't ended
     */
    private Checkpoint ended(UUID itemId)
    {
        Checkpoint checkpoint = pipeline.checkpoint(itemId)
                .orElseThrow(() -> new IllegalStateException(
                        "The item was never split"));
        if (checkpoint.state() != Checkpoint.State.COMPLETED
                && checkpoint.state() != Checkpoint.State.FAILED)
        {
            throw new IllegalStateException("The item is "
                    + checkpoint.state() + ", not ended");
        }
        return checkpoint;
    }

    /**
     * Whether an operator's correction or retry of an item as a whole
     * passed.
     *
     * @param itemId the item
     * @return true if its failure as a whole is superseded, with no
     *         failure of a later revision open in its place (STAGES.md
     *         §15.8.5)
     */
    private boolean wholeItemCorrected(UUID itemId)
    {
        return failures != null && failures.failure(itemId,
                FailureStore.WHOLE_ITEM).map(stored -> stored.state()
                        == FailureStore.State.SUPERSEDED).orElse(false);
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
                case HELD ->
                {
                    LOG.info("split.item_held_waiting");
                    return new Outcome(itemId, Result.HELD,
                            checkpoint.recordCount(), null);
                }
                case CLOSED ->
                {
                    LOG.info("split.item_closed_skipped");
                    return new Outcome(itemId, Result.CLOSED,
                            checkpoint.recordCount(), null);
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
        catch (ItemHeldException held)
        {
            return new Outcome(itemId, Result.HELD, 0, null);
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
        Ended announced = announce(checkpoint, landing, endedAt(itemId));
        pipeline.commitCheckpoint(
                checkpoint.withState(Checkpoint.State.COMPLETED), lease);
        auditEnded(announced, Result.COMPLETED);
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
     * What ended an item at the split stage.
     *
     * @param itemId  the item
     * @param message {@code item.split}, or {@code item.progress} while a
     *                failure is open
     * @param count   the records it was cut into, failed ones included,
     *                with those corrections added
     * @param failed  the pieces the splitter couldn't parse, less those
     *                corrected
     */
    private record Ended(UUID itemId, ConnectorMessage message, long count,
            long failed)
    {
    }

    /**
     * Records an item's failures in the fail store, then sends the message
     * that ends it (STAGES.md §15.6): {@code item.split} when none is open,
     * otherwise {@code item.progress}. Failures aren't sent on (§15.1). Both
     * steps are safe to repeat, so after a crash it is done again.
     *
     * @param checkpoint its checkpoint
     * @param landing    what came with it from landing
     * @param endedAt    when its splitting ended, as stored
     * @return what was sent
     * @throws Exception if the store or the broker fails
     */
    private Ended announce(Checkpoint checkpoint, Landing landing,
            Instant endedAt) throws Exception
    {
        UUID itemId = checkpoint.itemId();
        List<CommittedRecords.Row> failed = pipeline.failedRows(itemId);
        FailureStore.Counts counts;
        if (failures == null)
        {
            counts = SplitMessages.unstored(checkpoint, failed.size());
        }
        else
        {
            failures.record(SplitMessages.failures(checkpoint, failed));
            counts = failures.counts(itemId);
        }
        SplitMessages.Corrected corrected = corrections == null
                ? SplitMessages.Corrected.NONE
                : SplitMessages.Corrected.of(checkpoint,
                        pipeline.correctionRows(itemId),
                        wholeItemCorrected(itemId));
        // Numbered before it is sent (STAGES.md §15.6e).
        int completion = pipeline.announced(itemId, counts.open() == 0);
        ConnectorMessage message = SplitMessages.itemEnd(checkpoint, intake,
                failed.size(), counts, landing.digest(), landing.refs(),
                endedAt, pipeline.clock().instant(), corrected, completion);
        pipeline.publishNow(List.of(message));
        if (message instanceof ItemProgressed progressed)
        {
            Telemetry.get().audit("item.progress",
                    Fields.of(Attr.STAGE, STAGE)
                            .with(Attr.ITEM_ID, itemId)
                            .with(Attr.COUNT, progressed.passedCount())
                            .with(Attr.OPEN_COUNT, progressed.openCount())
                            .with(Attr.DECLINED_COUNT,
                                    progressed.declinedCount()));
        }
        // With what operators' corrections added and resolved (§15.6d).
        return new Ended(itemId, message,
                checkpoint.recordCount() + corrected.added(),
                failed.size() - corrected.pieces());
    }

    /**
     * Audits the end of an item at the split stage as every stage does
     * ({@code stage.item_completed}, STAGES.md §13.2), so one query over
     * the stages finds an item that ended at one and not the next. An item
     * with a failure open ends its splitting, but not its time at the stage
     * ({@code INCOMPLETE}, §15.6).
     *
     * @param ended  what ended it
     * @param result how its splitting ended
     */
    private static void auditEnded(Ended ended, Result result)
    {
        Telemetry.get().audit("stage.item_completed",
                Fields.of(Attr.STAGE, STAGE)
                        .with(Attr.ITEM_ID, ended.itemId())
                        .with(Attr.RESULT,
                                ended.message() instanceof ItemProgressed
                                        ? "INCOMPLETE" : result.name())
                        .with(Attr.COUNT, ended.count())
                        .with(Attr.FAILED_COUNT, ended.failed()));
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
        Ended announced = announce(failing, landing,
                endedAt(failing.itemId()));
        pipeline.commitCheckpoint(failing.failed(), lease);
        auditEnded(announced, Result.FAILED);
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
